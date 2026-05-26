<?php

namespace Signaladoc\EventBus\Producer;

use Carbon\CarbonInterface;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Http\Request;
use Illuminate\Support\Carbon;
use Illuminate\Support\Str;
use Signaladoc\EventBus\Producer\Contracts\EventTypeMap;
use Signaladoc\EventBus\Producer\Exceptions\OutboxEmissionOutsideTransactionException;

/**
 * Ergonomic helper for inserting a row into outbox_events.
 *
 * The emission hook lives in your service class, inside the same
 * `DB::transaction(...)` block as the business state change:
 *
 *     DB::transaction(function () use ($cycle) {
 *         $cycle->update(['status' => 'completed', ...]);
 *
 *         OutboxEmitter::record(
 *             eventType:    BillingEventTypeRegistry::CYCLE_COMPLETED,
 *             aggregateId:  $cycle->ref,  // store the full cross-service ref
 *             payload:      [...],
 *             partitionKey: $cycle->ref,
 *         );
 *     });
 *
 * Why this exists instead of `OutboxEvent::create([...])`:
 *
 *  1. Auto-fills `event_id` (ULID), `aggregate_type` and `stream_name`
 *     (from the injected {@see EventTypeMap}), `occurred_at`,
 *     `available_at`, `status`, `attempts`, and `metadata.correlation_id` —
 *     so the call site is four lines, not fifteen.
 *
 *  2. THROWS if called outside a DB transaction. The outbox pattern is
 *     worthless without atomicity — this guard catches the most common
 *     misuse at the first call instead of silently corrupting state.
 *
 *  3. Single chokepoint for every event emission in the service. Easy to
 *     add cross-cutting concerns later (metrics, audit log, tenant tag).
 *
 * The `$aggregateId` parameter accepts the aggregate's cross-service ref
 * (§12.1) — `<service>.<table>:<id>`. The legacy parameter name is kept
 * for backwards-compatibility; the EnvelopeBuilder surfaces the value as
 * `aggregate.ref` on the wire.
 *
 * @see microservice/ARCHITECTURE.md §13.1 (transactional outbox)
 * @see microservice/ARCHITECTURE.md §13.13 (locked event contracts)
 */
final class OutboxEmitter
{
    public function __construct(
        private readonly ConnectionInterface $db,
        private readonly EventTypeMap $eventTypes,
        private readonly ?Request $request = null,
    ) {}

    /**
     * Static convenience — resolves the emitter from the container so call
     * sites don't need to inject anything. Test code can either call
     * ->emit() directly on an instance, or swap the container binding.
     *
     * @param  array<string,mixed>  $payload
     * @param  array<string,mixed>  $metadata
     */
    public static function record(
        string $eventType,
        string $aggregateId,
        array $payload,
        ?string $partitionKey = null,
        array $metadata = [],
        ?CarbonInterface $occurredAt = null,
        ?CarbonInterface $availableAt = null,
    ): OutboxEvent {
        return app(self::class)->emit(
            eventType: $eventType,
            aggregateId: $aggregateId,
            payload: $payload,
            partitionKey: $partitionKey,
            metadata: $metadata,
            occurredAt: $occurredAt,
            availableAt: $availableAt,
        );
    }

    /**
     * Instance variant — same behaviour, easier to unit-test.
     *
     * @param  array<string,mixed>  $payload
     * @param  array<string,mixed>  $metadata
     */
    public function emit(
        string $eventType,
        string $aggregateId,
        array $payload,
        ?string $partitionKey = null,
        array $metadata = [],
        ?CarbonInterface $occurredAt = null,
        ?CarbonInterface $availableAt = null,
    ): OutboxEvent {
        $this->guardInsideTransaction($eventType);

        // EventTypeMap lookups throw on unknown event_type — fail fast at
        // the emission site instead of letting a bad event_type land in
        // the table and surface later in the forwarder.
        $aggregateType = $this->eventTypes->aggregateFor($eventType);
        $streamName    = $this->eventTypes->streamFor($eventType);

        $occurredAt ??= Carbon::now();
        $availableAt ??= $occurredAt;

        $mergedMetadata = $this->enrichMetadata($metadata);

        return OutboxEvent::create([
            'event_id'       => (string) Str::ulid(),
            'event_type'     => $eventType,
            'aggregate_type' => $aggregateType,
            'aggregate_id'   => $aggregateId,
            'stream_name'    => $streamName,
            'partition_key'  => $partitionKey,
            'payload'        => $payload,
            'metadata'       => $mergedMetadata ?: null,
            'occurred_at'    => $occurredAt,
            'available_at'   => $availableAt,
            'status'         => OutboxEvent::STATUS_PENDING,
            'attempts'       => 0,
        ]);
    }

    private function guardInsideTransaction(string $eventType): void
    {
        if ($this->db->transactionLevel() < 1) {
            throw new OutboxEmissionOutsideTransactionException($eventType);
        }
    }

    /**
     * Stamp the metadata blob with a correlation_id when one is available
     * from the current request, but only if the caller didn't provide one.
     * In CLI / queue worker contexts there's no Request, and we just leave
     * the metadata alone.
     *
     * @param  array<string,mixed>  $metadata
     * @return array<string,mixed>
     */
    private function enrichMetadata(array $metadata): array
    {
        if (array_key_exists('correlation_id', $metadata)) {
            return $metadata;
        }

        $correlationId = $this->request?->headers->get('X-Request-Id');
        if ($correlationId !== null && $correlationId !== '') {
            $metadata['correlation_id'] = $correlationId;
        }

        return $metadata;
    }
}
