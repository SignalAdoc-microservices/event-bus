<?php

namespace Signaladoc\EventBus\Consumer;

/**
 * Parsed, validated view of one Redis Streams envelope.
 *
 * Canonical envelope shape: microservice/ARCHITECTURE.md §13.2.
 *
 * Immutable value object. Construct via EnvelopeParser (never by hand from
 * raw fields), so the contract validation lives in exactly one place.
 *
 * `metadata` and `data` are kept as raw assoc arrays — handlers know what
 * shape their own `event_type` expects; this object stays generic.
 *
 * `aggregateRef` is the producer's full cross-service ref for the aggregate
 * (`<service>.<table>:<id>`, see ARCHITECTURE.md §12.1). Per the locked
 * envelope spec (§13.2), `aggregate.ref` replaces the legacy `aggregate.id`
 * on the wire.
 *
 * @phpstan-type EnvelopeMetadata array<string, mixed>
 * @phpstan-type EnvelopeData array<string, mixed>
 */
final readonly class Envelope
{
    /**
     * @param  EnvelopeMetadata  $metadata
     * @param  EnvelopeData  $data
     */
    public function __construct(
        public string $eventId,
        public string $eventType,
        public int $schemaVersion,
        public string $occurredAt,
        public string $producerService,
        public string $producerVersion,
        public string $aggregateType,
        public string $aggregateRef,
        public ?string $partitionKey,
        public array $metadata,
        public array $data,
    ) {}
}
