<?php

namespace Signaladoc\EventBus\Tests\Unit\Producer;

use Illuminate\Support\Carbon;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Signaladoc\EventBus\Producer\EnvelopeBuilder;
use Signaladoc\EventBus\Producer\OutboxEvent;

/**
 * Pure-function tests for the envelope shape.
 *
 * These don't need Laravel or a DB — just construct an OutboxEvent in
 * memory, call build(), assert on the resulting array.
 *
 * If you change the envelope shape here without bumping
 * event-bus.producer.envelope_schema_version, every consumer in the
 * platform will silently start failing. Don't.
 */
class EnvelopeBuilderTest extends TestCase
{
    private EnvelopeBuilder $builder;

    protected function setUp(): void
    {
        parent::setUp();
        $this->builder = new EnvelopeBuilder(
            producerService: 'billing-service',
            producerVersion: 'v1.2.3',
            envelopeSchemaVersion: 1,
        );
    }

    #[Test]
    public function it_builds_the_canonical_envelope_shape(): void
    {
        $row = $this->makeRow([
            'event_id'       => '01HZX9K2YQ3R6JV4P5N7M8B0Z9',
            'event_type'     => 'cycle.completed',
            'aggregate_type' => 'cycle',
            'aggregate_id'   => '01HZX9K2Z0AAAAAAAAAAAAAA01',
            'partition_key'  => 'subscription:42',
            'occurred_at'    => Carbon::parse('2026-05-24T15:30:45.123456Z'),
            'payload'        => ['cycle_id' => 17, 'amount_minor' => 250000],
            'metadata'       => ['correlation_id' => 'req-abc-123'],
        ]);

        $envelope = $this->builder->build($row);

        $this->assertSame('01HZX9K2YQ3R6JV4P5N7M8B0Z9', $envelope['event_id']);
        $this->assertSame('cycle.completed', $envelope['event_type']);
        $this->assertSame(1, $envelope['schema_version']);
        $this->assertSame('2026-05-24T15:30:45.123456Z', $envelope['occurred_at']);
        $this->assertSame(['service' => 'billing-service', 'version' => 'v1.2.3'], $envelope['producer']);
        $this->assertSame(['type' => 'cycle', 'ref' => '01HZX9K2Z0AAAAAAAAAAAAAA01'], $envelope['aggregate']);
        $this->assertSame('subscription:42', $envelope['partition_key']);
        $this->assertSame(['correlation_id' => 'req-abc-123'], $envelope['metadata']);
        $this->assertSame(['cycle_id' => 17, 'amount_minor' => 250000], $envelope['data']);
    }

    #[Test]
    public function metadata_is_an_empty_object_when_null_so_json_encodes_to_curly_braces(): void
    {
        $row = $this->makeRow(['metadata' => null]);

        $envelope = $this->builder->build($row);

        $this->assertInstanceOf(\stdClass::class, $envelope['metadata']);
        $json = json_encode($envelope);
        $this->assertStringContainsString('"metadata":{}', $json);
    }

    #[Test]
    public function envelope_keys_are_in_the_documented_order(): void
    {
        $expected = [
            'event_id',
            'event_type',
            'schema_version',
            'occurred_at',
            'producer',
            'aggregate',
            'partition_key',
            'metadata',
            'data',
        ];

        $envelope = $this->builder->build($this->makeRow());

        $this->assertSame($expected, array_keys($envelope));
    }

    #[Test]
    public function partition_key_passes_through_null_when_unset(): void
    {
        $row = $this->makeRow(['partition_key' => null]);

        $envelope = $this->builder->build($row);

        $this->assertNull($envelope['partition_key']);
    }

    /** @param array<string, mixed> $overrides */
    private function makeRow(array $overrides = []): OutboxEvent
    {
        $merged = array_merge([
            'event_id'       => '01HZX9K2YQ3R6JV4P5N7M8B0Z9',
            'event_type'     => 'cycle.completed',
            'aggregate_type' => 'cycle',
            'aggregate_id'   => '01HZX9K2Z0AAAAAAAAAAAAAA01',
            'stream_name'    => 'billing.cycle',
            'partition_key'  => null,
            'payload'        => ['x' => 1],
            'metadata'       => null,
            'occurred_at'    => Carbon::parse('2026-05-24T15:30:45.123456Z'),
        ], $overrides);

        $raw = $merged;
        foreach (['payload', 'metadata'] as $jsonCol) {
            if ($raw[$jsonCol] !== null) {
                $raw[$jsonCol] = json_encode($raw[$jsonCol]);
            }
        }

        $row = new OutboxEvent;
        $row->setRawAttributes($raw);

        return $row;
    }
}
