<?php

namespace Signaladoc\EventBus\Consumer;

use JsonException;
use Signaladoc\EventBus\Consumer\Exceptions\MalformedEnvelopeException;

/**
 * Parses a Redis Streams entry into an Envelope value object.
 *
 * Inputs are the raw `fields => values` map XREADGROUP returns. The producer
 * writes a single field named `envelope` whose value is the canonical JSON.
 *
 * Validation is strict on the **envelope shape** (per ARCHITECTURE.md §13.2)
 * but tolerant on `data` and `metadata` content — those evolve per
 * event_type and are owned by the producer. Forward-compatibility rule from
 * §13.2: consumers MUST tolerate unknown keys in `metadata` and `data`.
 *
 * The aggregate identifier on the wire is `aggregate.ref` per the locked
 * envelope spec (§13.2). For backwards compatibility with any pre-lock
 * payloads still in flight at deploy time, parsers accept either
 * `aggregate.ref` (preferred) or `aggregate.id` (legacy).
 */
final class EnvelopeParser
{
    private const REQUIRED_TOP_LEVEL = [
        'event_id',
        'event_type',
        'schema_version',
        'occurred_at',
        'producer',
        'aggregate',
        'metadata',
        'data',
    ];

    /**
     * @param  array<string, string>  $rawFields  As returned by XREADGROUP
     *                                            for one message (field map).
     *
     * @throws MalformedEnvelopeException
     */
    public function parse(array $rawFields): Envelope
    {
        if (! array_key_exists('envelope', $rawFields)) {
            throw new MalformedEnvelopeException("missing 'envelope' field on Redis Streams entry");
        }

        try {
            $envelope = json_decode($rawFields['envelope'], associative: true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new MalformedEnvelopeException('envelope is not valid JSON: '.$e->getMessage());
        }

        if (! is_array($envelope)) {
            throw new MalformedEnvelopeException('envelope JSON is not an object');
        }

        foreach (self::REQUIRED_TOP_LEVEL as $key) {
            if (! array_key_exists($key, $envelope)) {
                throw new MalformedEnvelopeException("missing top-level key '{$key}'");
            }
        }

        $producer = $envelope['producer'];
        if (! is_array($producer) || ! isset($producer['service'], $producer['version'])) {
            throw new MalformedEnvelopeException("'producer' must include service + version");
        }

        $aggregate = $envelope['aggregate'];
        if (! is_array($aggregate) || ! isset($aggregate['type'])) {
            throw new MalformedEnvelopeException("'aggregate' must include type");
        }
        // Accept `ref` (post-lock §13.2) or fall back to legacy `id`.
        $aggregateRef = $aggregate['ref'] ?? $aggregate['id'] ?? null;
        if ($aggregateRef === null || $aggregateRef === '') {
            throw new MalformedEnvelopeException("'aggregate' must include a non-empty 'ref' (or legacy 'id')");
        }

        $metadata = $envelope['metadata'];
        if ($metadata instanceof \stdClass) {
            $metadata = (array) $metadata;
        }
        if (! is_array($metadata)) {
            throw new MalformedEnvelopeException("'metadata' must be a JSON object");
        }

        if (! is_array($envelope['data'])) {
            throw new MalformedEnvelopeException("'data' must be a JSON object");
        }

        return new Envelope(
            eventId: (string) $envelope['event_id'],
            eventType: (string) $envelope['event_type'],
            schemaVersion: (int) $envelope['schema_version'],
            occurredAt: (string) $envelope['occurred_at'],
            producerService: (string) $producer['service'],
            producerVersion: (string) $producer['version'],
            aggregateType: (string) $aggregate['type'],
            aggregateRef: (string) $aggregateRef,
            partitionKey: isset($envelope['partition_key']) && $envelope['partition_key'] !== ''
                ? (string) $envelope['partition_key']
                : null,
            metadata: $metadata,
            data: $envelope['data'],
        );
    }
}
