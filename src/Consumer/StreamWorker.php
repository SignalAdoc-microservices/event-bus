<?php

namespace Signaladoc\EventBus\Consumer;

use Psr\Log\LoggerInterface;
use Signaladoc\EventBus\Consumer\Contracts\StreamReader;
use Throwable;

/**
 * Long-running consumer loop for ONE consumer group across one or more streams.
 *
 * A "worker" here is:
 *   - one process,
 *   - one consumer group (e.g. `insurance.streak-engine`),
 *   - one consumer name (per process, defaulted to host+pid),
 *   - a list of source streams (typically a single producing service's streams).
 *
 * Per cycle, the worker:
 *
 *   1. Optionally reclaims stuck pending messages (XPENDING + XCLAIM)
 *      from dead consumers in the same group, then dispatches them.
 *   2. XREADGROUP a batch of new messages, dispatches each.
 *   3. XACK every message the Dispatcher tells us to ACK
 *      (everything except TransientFailure).
 *   4. Returns a ConsumerCycleResult so the calling Command can decide
 *      whether to sleep, log, or exit.
 *
 * The worker itself owns NO transaction — the Dispatcher does. The worker
 * owns the loop, the consumer-group bookkeeping (ensureGroup, XACK), and
 * the reclaim cadence.
 */
final class StreamWorker
{
    /** @var list<string> */
    private array $streams;

    private int $idleCyclesSinceLastReclaim = 0;

    /**
     * @param  list<string>  $streams  Source streams (e.g. ['billing.cycle', 'billing.subscription']).
     */
    public function __construct(
        private readonly StreamReader $reader,
        private readonly Dispatcher $dispatcher,
        private readonly LoggerInterface $logger,
        private readonly string $consumerGroup,
        private readonly string $consumerName,
        array $streams,
        private readonly int $batchSize,
        private readonly int $blockMs,
        private readonly int $reclaimMinIdleMs,
        private readonly int $reclaimBatchSize,
        private readonly int $reclaimEveryNIdleCycles,
    ) {
        $this->streams = array_values($streams);
    }

    /**
     * Idempotent setup. Call once at worker boot, before the run loop.
     */
    public function bootstrap(): void
    {
        foreach ($this->streams as $stream) {
            $this->reader->ensureGroup($stream, $this->consumerGroup);
        }
    }

    /**
     * Run exactly one cycle (one reclaim pass if due + one XREADGROUP batch).
     * The owning command calls this in a loop and decides when to stop
     * (signals, max cycles, etc.).
     */
    public function runOnce(): ConsumerCycleResult
    {
        $tally = [
            'read' => 0, 'processed' => 0, 'skipped' => 0,
            'duplicates' => 0, 'transient' => 0, 'dlq' => 0,
            'reclaimed' => 0,
        ];

        // 1. Reclaim before reading. Reclaimed messages are dispatched
        //    just like fresh ones — they reuse the existing message id
        //    and Redis-tracked delivery count.
        if ($this->shouldReclaim()) {
            $tally['reclaimed'] = $this->reclaimAndDispatch($tally);
            $this->idleCyclesSinceLastReclaim = 0;
        }

        // 2. Read a fresh batch.
        try {
            $messages = $this->reader->read(
                group: $this->consumerGroup,
                consumer: $this->consumerName,
                streams: $this->streams,
                count: $this->batchSize,
                blockMs: $this->blockMs,
            );
        } catch (Throwable $e) {
            $this->logger->error('consumer.xreadgroup.failed', [
                'group' => $this->consumerGroup,
                'error' => $e->getMessage(),
            ]);

            return ConsumerCycleResult::empty();
        }

        $tally['read'] = count($messages);

        foreach ($messages as $message) {
            $this->dispatchOne($message, deliveryCount: 1, tally: $tally);
        }

        if ($tally['read'] === 0 && $tally['reclaimed'] === 0) {
            $this->idleCyclesSinceLastReclaim++;
        }

        return new ConsumerCycleResult(
            read: $tally['read'],
            processed: $tally['processed'],
            skipped: $tally['skipped'],
            duplicates: $tally['duplicates'],
            transientFailures: $tally['transient'],
            deadLettered: $tally['dlq'],
            reclaimed: $tally['reclaimed'],
        );
    }

    private function shouldReclaim(): bool
    {
        if ($this->reclaimEveryNIdleCycles <= 0) {
            return false;
        }

        return $this->idleCyclesSinceLastReclaim >= $this->reclaimEveryNIdleCycles;
    }

    /**
     * For each source stream, find pending messages idle long enough that
     * their original owner is presumed dead, XCLAIM them to this consumer,
     * then dispatch each with the Redis-tracked delivery_count.
     */
    private function reclaimAndDispatch(array &$tally): int
    {
        $reclaimedCount = 0;

        foreach ($this->streams as $stream) {
            try {
                $pending = $this->reader->pending(
                    stream: $stream,
                    group: $this->consumerGroup,
                    minIdleMs: $this->reclaimMinIdleMs,
                    count: $this->reclaimBatchSize,
                );
            } catch (Throwable $e) {
                $this->logger->warning('consumer.xpending.failed', [
                    'stream' => $stream,
                    'error'  => $e->getMessage(),
                ]);

                continue;
            }

            if ($pending === []) {
                continue;
            }

            $ids = array_map(static fn ($p) => $p['id'], $pending);
            $deliveryCountById = [];
            foreach ($pending as $row) {
                $deliveryCountById[$row['id']] = $row['delivery_count'];
            }

            try {
                $claimed = $this->reader->claim(
                    stream: $stream,
                    group: $this->consumerGroup,
                    consumer: $this->consumerName,
                    minIdleMs: $this->reclaimMinIdleMs,
                    messageIds: $ids,
                );
            } catch (Throwable $e) {
                $this->logger->warning('consumer.xclaim.failed', [
                    'stream' => $stream,
                    'error'  => $e->getMessage(),
                ]);

                continue;
            }

            $this->logger->info('consumer.reclaim', [
                'stream'   => $stream,
                'group'    => $this->consumerGroup,
                'claimed'  => count($claimed),
                'pending'  => count($pending),
            ]);

            foreach ($claimed as $message) {
                $delivery = $deliveryCountById[$message['id']] ?? 1;
                $this->dispatchOne($message, deliveryCount: $delivery, tally: $tally);
                $reclaimedCount++;
            }
        }

        return $reclaimedCount;
    }

    /**
     * Dispatch one message and reconcile Redis state (XACK or leave pending).
     *
     * @param  array{stream: string, id: string, fields: array<string, string>}  $message
     */
    private function dispatchOne(array $message, int $deliveryCount, array &$tally): void
    {
        $outcome = $this->dispatcher->dispatch(
            stream: $message['stream'],
            messageId: $message['id'],
            rawFields: $message['fields'],
            deliveryCount: $deliveryCount,
            consumerGroup: $this->consumerGroup,
        );

        match ($outcome) {
            DispatchOutcome::Processed        => $tally['processed']++,
            DispatchOutcome::Skipped          => $tally['skipped']++,
            DispatchOutcome::Duplicate        => $tally['duplicates']++,
            DispatchOutcome::TransientFailure => $tally['transient']++,
            DispatchOutcome::DeadLettered     => $tally['dlq']++,
        };

        if ($outcome->shouldAck()) {
            try {
                $this->reader->ack($message['stream'], $this->consumerGroup, $message['id']);
            } catch (Throwable $e) {
                // Failing to XACK after a successful COMMIT is benign — the
                // message will redeliver, hit the receipt UNIQUE / business
                // UNIQUE, and we'll ACK on the next pass. Log so it shows
                // up if it becomes chronic.
                $this->logger->warning('consumer.xack.failed', [
                    'stream'     => $message['stream'],
                    'message_id' => $message['id'],
                    'outcome'    => $outcome->value,
                    'error'      => $e->getMessage(),
                ]);
            }
        }
    }
}
