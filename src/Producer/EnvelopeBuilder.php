<?php

namespace Signaladoc\EventBus\Producer;

/**
 * Builds the canonical Redis Streams envelope from an OutboxEvent row.
 *
 * Envelope shape is defined in microservice/ARCHITECTURE.md §13.2 and is
 * platform-wide — DO NOT add or remove top-level keys here without bumping
 * `event-bus.producer.envelope_schema_version` in config and coordinating
 * a platform-wide migration.
 *
 * The `aggregate.ref` field carries the producer's full cross-service
 * reference for the aggregate (`<service>.<table>:<id>`, see §12.1).
 * That value is stored verbatim in `outbox_events.aggregate_id` at emit
 * time — the column name predates the §12.1 ref convention.
 *
 * Pure function: no DB, no Redis, no time. Trivially unit-testable.
 */
final class EnvelopeBuilder
{
    public function __construct(
        private readonly string $producerService,
        private readonly string $producerVersion,
        private readonly int $envelopeSchemaVersion,
    ) {}

    /**
     * Returns the envelope as an associative array, ready to be json_encode'd
     * by the publisher.
     *
     * @return array<string, mixed>
     */
    public function build(OutboxEvent $row): array
    {
        return [
            'event_id'       => $row->event_id,
            'event_type'     => $row->event_type,
            'schema_version' => $this->envelopeSchemaVersion,
            // ISO-8601 with microsecond precision so consumers can sort
            // within the same wall-second.
            'occurred_at'    => $row->occurred_at->format('Y-m-d\TH:i:s.u\Z'),
            'producer'       => [
                'service' => $this->producerService,
                'version' => $this->producerVersion,
            ],
            'aggregate'      => [
                'type' => $row->aggregate_type,
                'ref'  => $row->aggregate_id,
            ],
            'partition_key'  => $row->partition_key,
            'metadata'       => $row->metadata ?? new \stdClass, // {} when null, never `null`
            'data'           => $row->payload,
        ];
    }
}
