<?php

namespace Signaladoc\EventBus\Consumer;

use Illuminate\Database\UniqueConstraintViolationException;
use Illuminate\Support\Facades\DB;
use JsonException;
use Psr\Log\LoggerInterface;
use Signaladoc\EventBus\Consumer\Contracts\EventReceiptStore;
use Signaladoc\EventBus\Consumer\Contracts\StreamReader;
use Signaladoc\EventBus\Consumer\Exceptions\MalformedEnvelopeException;
use Signaladoc\EventBus\Consumer\Exceptions\SkipEventException;
use Throwable;

/**
 * Per-message orchestrator.
 *
 * Owns the canonical consumer transaction shape (microservice/ARCHITECTURE.md §13.5):
 *
 *     BEGIN TX
 *       INSERT INTO <source>_event_receipts (event_id, …)
 *         -- UNIQUE violation ⇒ already processed; ROLLBACK, XACK, continue
 *       <run handlers>
 *       UPDATE <source>_event_receipts SET status='processed', processed_at=NOW()
 *     COMMIT
 *     XACK <group> <message_id>          -- done by StreamWorker, ONLY after COMMIT
 *
 * The Dispatcher is producer-agnostic: it talks to an EventReceiptStore
 * (one impl per producer) and a HandlerRegistry (shared, keyed on the
 * fully-qualified event_type which is itself producer-prefixed). To consume
 * a new producer you build a new Dispatcher via DispatcherFactory — the
 * dispatch logic itself never changes.
 *
 * Failure routing:
 *
 *  - Malformed envelope             → DeadLettered  (DLQ + XACK; redeliver won't help)
 *  - UNIQUE violation on receipt    → Duplicate     (XACK; already processed)
 *  - delivery_count > max_attempts  → DeadLettered  (DLQ + XACK; poison message)
 *  - SkipEventException             → Skipped       (receipt=skipped, XACK)
 *  - Other Throwable                → TransientFailure (no XACK; Redis redelivers)
 *  - Happy path                     → Processed     (receipt=processed, XACK)
 *
 * Stateless — safe to share across worker threads if Laravel ever runs
 * multiple per process.
 */
final class Dispatcher
{
    public function __construct(
        private readonly EnvelopeParser $parser,
        private readonly HandlerRegistry $registry,
        private readonly StreamReader $reader,
        private readonly EventReceiptStore $receipts,
        private readonly LoggerInterface $logger,
        private readonly int $maxAttempts,
        private readonly int $dlqMaxLen,
    ) {}

    /**
     * @param  array<string, string>  $rawFields  Field map from XREADGROUP.
     * @param  int  $deliveryCount  Authoritative retry count from XPENDING.
     *                              (1 on first delivery; bumped by Redis on each redeliver.)
     */
    public function dispatch(
        string $stream,
        string $messageId,
        array $rawFields,
        int $deliveryCount,
        string $consumerGroup,
    ): DispatchOutcome {
        // ---------------------------------------------------------------
        // 1. Parse envelope. Malformed = DLQ (no point redelivering).
        // ---------------------------------------------------------------
        try {
            $envelope = $this->parser->parse($rawFields);
        } catch (MalformedEnvelopeException $e) {
            $this->logger->error('consumer.envelope.malformed', [
                'stream'     => $stream,
                'message_id' => $messageId,
                'group'      => $consumerGroup,
                'error'      => $e->getMessage(),
            ]);
            $this->writeDlqRaw($stream, $messageId, $rawFields, [
                'reason' => 'malformed_envelope',
                'error'  => $e->getMessage(),
            ]);

            return DispatchOutcome::DeadLettered;
        }

        $logContext = [
            'stream'         => $stream,
            'message_id'     => $messageId,
            'group'          => $consumerGroup,
            'event_id'       => $envelope->eventId,
            'event_type'     => $envelope->eventType,
            'delivery_count' => $deliveryCount,
        ];

        // ---------------------------------------------------------------
        // 2. Poison-message gate. delivery_count is from Redis (XPENDING),
        //    not from our receipts table — it's authoritative across
        //    consumer crashes.
        // ---------------------------------------------------------------
        if ($deliveryCount > $this->maxAttempts) {
            $this->logger->warning('consumer.event.max_attempts_exceeded', $logContext);
            $this->writeDlqEnvelope($stream, $envelope, [
                'reason'         => 'max_attempts_exceeded',
                'delivery_count' => $deliveryCount,
                'max_attempts'   => $this->maxAttempts,
            ]);
            $this->recordTerminalReceipt(
                envelope: $envelope,
                stream: $stream,
                messageId: $messageId,
                consumerGroup: $consumerGroup,
                status: ReceiptStatus::Failed,
                attempts: $deliveryCount,
                lastError: "max attempts exceeded ({$deliveryCount} > {$this->maxAttempts}); dead-lettered",
            );

            return DispatchOutcome::DeadLettered;
        }

        $handlers = $this->registry->handlersFor($envelope->eventType);

        // ---------------------------------------------------------------
        // 3. No handlers? Record skipped, XACK. This is the common case
        //    for events we don't care about (e.g. analytics-only groups
        //    on a stream that carries many event types).
        // ---------------------------------------------------------------
        if ($handlers === []) {
            $this->recordTerminalReceipt(
                envelope: $envelope,
                stream: $stream,
                messageId: $messageId,
                consumerGroup: $consumerGroup,
                status: ReceiptStatus::Skipped,
                attempts: $deliveryCount,
                lastError: 'no handlers registered for event_type',
            );

            return DispatchOutcome::Skipped;
        }

        // ---------------------------------------------------------------
        // 4. Canonical transactional dispatch — §13.5.
        // ---------------------------------------------------------------
        DB::beginTransaction();
        try {
            $this->receipts->createReceived(
                envelope: $envelope,
                streamName: $stream,
                consumerGroup: $consumerGroup,
                streamMessageId: $messageId,
                attempts: $deliveryCount,
            );

            foreach ($handlers as $handler) {
                $handler->handle($envelope);
            }

            $this->receipts->markProcessed($envelope->eventId);

            DB::commit();

            return DispatchOutcome::Processed;

        } catch (UniqueConstraintViolationException) {
            // Another consumer already inserted this event_id. Roll back —
            // their work is authoritative (or in flight). XACK either way:
            // if they're mid-flight and ultimately fail, their handler
            // will leave the receipt non-terminal and the message will
            // come back via reclaim with a higher delivery_count.
            DB::rollBack();
            $this->logger->info('consumer.event.duplicate', $logContext);

            return DispatchOutcome::Duplicate;

        } catch (SkipEventException $e) {
            DB::rollBack();
            $this->logger->info('consumer.event.skipped', $logContext + ['reason' => $e->getMessage()]);

            // Re-write the receipt as terminal `skipped` outside the rolled-back tx.
            $this->recordTerminalReceipt(
                envelope: $envelope,
                stream: $stream,
                messageId: $messageId,
                consumerGroup: $consumerGroup,
                status: ReceiptStatus::Skipped,
                attempts: $deliveryCount,
                lastError: $e->getMessage(),
            );

            return DispatchOutcome::Skipped;

        } catch (Throwable $e) {
            DB::rollBack();
            $this->logger->error('consumer.event.failed', $logContext + [
                'error' => $e->getMessage(),
                'class' => $e::class,
            ]);

            // Best-effort visibility row so Ops can see "this is being
            // retried". Separate write outside the rolled-back tx.
            $this->recordTerminalReceipt(
                envelope: $envelope,
                stream: $stream,
                messageId: $messageId,
                consumerGroup: $consumerGroup,
                status: ReceiptStatus::Failed,
                attempts: $deliveryCount,
                lastError: substr($e->getMessage(), 0, 1000),
            );

            return DispatchOutcome::TransientFailure;
        }
    }

    /**
     * Upsert a receipt in a terminal-or-pending state OUTSIDE any caller
     * transaction. Used for skipped/failed/dead-lettered paths where the
     * happy-path tx was rolled back (or never started).
     *
     * Failure here must never abort the consumer cycle — the worst-case
     * effect is missing ops visibility for one message.
     */
    private function recordTerminalReceipt(
        Envelope $envelope,
        string $stream,
        string $messageId,
        string $consumerGroup,
        ReceiptStatus $status,
        int $attempts,
        ?string $lastError,
    ): void {
        try {
            $this->receipts->upsertTerminal(
                envelope: $envelope,
                streamName: $stream,
                consumerGroup: $consumerGroup,
                streamMessageId: $messageId,
                status: $status,
                attempts: $attempts,
                lastError: $lastError,
            );
        } catch (Throwable $e) {
            $this->logger->warning('consumer.receipt.write_failed', [
                'event_id' => $envelope->eventId,
                'status'   => $status->value,
                'error'    => $e->getMessage(),
            ]);
        }
    }

    /**
     * Re-publish a parseable envelope into <stream>.dlq with an extra
     * `dlq` metadata block describing why it was sidelined.
     */
    private function writeDlqEnvelope(string $stream, Envelope $envelope, array $reason): void
    {
        $rebuilt = [
            'event_id'       => $envelope->eventId,
            'event_type'     => $envelope->eventType,
            'schema_version' => $envelope->schemaVersion,
            'occurred_at'    => $envelope->occurredAt,
            'producer'       => ['service' => $envelope->producerService, 'version' => $envelope->producerVersion],
            'aggregate'      => ['type' => $envelope->aggregateType, 'ref' => $envelope->aggregateRef],
            'partition_key'  => $envelope->partitionKey,
            'metadata'       => $envelope->metadata + ['dlq' => $reason + ['source_stream' => $stream]],
            'data'           => $envelope->data,
        ];

        $this->publishDlqJson("{$stream}.dlq", $rebuilt);
    }

    /**
     * DLQ a message we couldn't parse. Wrap the raw fields and the
     * Redis ID in a synthetic envelope so the DLQ stream entries are
     * uniformly shaped.
     *
     * @param  array<string, string>  $rawFields
     */
    private function writeDlqRaw(string $stream, string $messageId, array $rawFields, array $reason): void
    {
        $payload = [
            'event_id'       => 'malformed-'.$messageId,
            'event_type'     => 'dlq.malformed',
            'schema_version' => 1,
            'occurred_at'    => now()->toIso8601String(),
            'producer'       => ['service' => 'unknown', 'version' => 'unknown'],
            'aggregate'      => ['type' => 'unknown', 'ref' => $messageId],
            'partition_key'  => null,
            'metadata'       => ['dlq' => $reason + ['source_stream' => $stream, 'source_message_id' => $messageId]],
            'data'           => ['raw_fields' => $rawFields],
        ];

        $this->publishDlqJson("{$stream}.dlq", $payload);
    }

    private function publishDlqJson(string $dlqStream, array $payload): void
    {
        try {
            $json = json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
        } catch (JsonException $e) {
            // Should never happen — payload is constructed from primitives.
            $this->logger->error('consumer.dlq.encode_failed', [
                'dlq_stream' => $dlqStream,
                'error'      => $e->getMessage(),
            ]);

            return;
        }

        try {
            $id = $this->reader->publishDlq($dlqStream, $json, $this->dlqMaxLen);
            $this->logger->warning('consumer.event.dead_lettered', [
                'dlq_stream'     => $dlqStream,
                'dlq_message_id' => $id,
            ]);
        } catch (Throwable $e) {
            // DLQ write failure is alert-worthy but we still XACK so we
            // don't loop forever on the same message.
            $this->logger->critical('consumer.dlq.publish_failed', [
                'dlq_stream' => $dlqStream,
                'error'      => $e->getMessage(),
            ]);
        }
    }
}
