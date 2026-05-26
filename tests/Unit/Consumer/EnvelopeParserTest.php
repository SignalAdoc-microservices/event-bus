<?php

namespace Signaladoc\EventBus\Tests\Unit\Consumer;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Signaladoc\EventBus\Consumer\EnvelopeParser;
use Signaladoc\EventBus\Consumer\Exceptions\MalformedEnvelopeException;

class EnvelopeParserTest extends TestCase
{
    private EnvelopeParser $parser;

    protected function setUp(): void
    {
        parent::setUp();
        $this->parser = new EnvelopeParser;
    }

    #[Test]
    public function it_parses_a_well_formed_envelope(): void
    {
        $envelope = $this->parser->parse([
            'envelope' => json_encode($this->validEnvelopeArray()),
        ]);

        $this->assertSame('01JC0000000000000000000001', $envelope->eventId);
        $this->assertSame('billing.cycle.completed', $envelope->eventType);
        $this->assertSame(1, $envelope->schemaVersion);
        $this->assertSame('billing-service', $envelope->producerService);
        $this->assertSame('1.4.0', $envelope->producerVersion);
        $this->assertSame('cycle', $envelope->aggregateType);
        $this->assertSame('billing-service.subscription_cycles:42', $envelope->aggregateRef);
        $this->assertSame('billing-service.subscription_cycles:42', $envelope->partitionKey);
        $this->assertSame(['trace_id' => 't-1'], $envelope->metadata);
        $this->assertSame(['amount_minor' => 50_000], $envelope->data);
    }

    #[Test]
    public function it_accepts_an_empty_metadata_object(): void
    {
        $env = $this->validEnvelopeArray();
        $env['metadata'] = new \stdClass;

        $envelope = $this->parser->parse(['envelope' => json_encode($env)]);

        $this->assertSame([], $envelope->metadata);
    }

    #[Test]
    public function it_treats_a_missing_partition_key_as_null(): void
    {
        $env = $this->validEnvelopeArray();
        unset($env['partition_key']);

        $envelope = $this->parser->parse(['envelope' => json_encode($env)]);

        $this->assertNull($envelope->partitionKey);
    }

    #[Test]
    public function it_throws_when_envelope_field_is_missing(): void
    {
        $this->expectException(MalformedEnvelopeException::class);
        $this->expectExceptionMessageMatches('/missing .envelope. field/');

        $this->parser->parse(['other' => 'x']);
    }

    #[Test]
    public function it_throws_when_envelope_is_not_json(): void
    {
        $this->expectException(MalformedEnvelopeException::class);
        $this->expectExceptionMessageMatches('/not valid JSON/');

        $this->parser->parse(['envelope' => 'not-json']);
    }

    #[Test]
    public function it_throws_when_top_level_key_is_missing(): void
    {
        $env = $this->validEnvelopeArray();
        unset($env['event_id']);

        $this->expectException(MalformedEnvelopeException::class);
        $this->expectExceptionMessageMatches("/missing top-level key 'event_id'/");

        $this->parser->parse(['envelope' => json_encode($env)]);
    }

    #[Test]
    public function it_throws_when_producer_is_incomplete(): void
    {
        $env = $this->validEnvelopeArray();
        $env['producer'] = ['service' => 'billing-service'];

        $this->expectException(MalformedEnvelopeException::class);
        $this->expectExceptionMessageMatches('/producer.*service.*version/');

        $this->parser->parse(['envelope' => json_encode($env)]);
    }

    #[Test]
    public function it_throws_when_aggregate_has_no_type(): void
    {
        $env = $this->validEnvelopeArray();
        $env['aggregate'] = ['ref' => 'billing-service.subscription_cycles:42'];

        $this->expectException(MalformedEnvelopeException::class);
        $this->expectExceptionMessageMatches('/aggregate.*type/');

        $this->parser->parse(['envelope' => json_encode($env)]);
    }

    #[Test]
    public function it_throws_when_aggregate_has_no_ref_or_id(): void
    {
        $env = $this->validEnvelopeArray();
        $env['aggregate'] = ['type' => 'cycle'];

        $this->expectException(MalformedEnvelopeException::class);
        $this->expectExceptionMessageMatches("/aggregate.*ref/");

        $this->parser->parse(['envelope' => json_encode($env)]);
    }

    #[Test]
    public function it_falls_back_to_legacy_aggregate_id_when_ref_is_absent(): void
    {
        // Forward-compatibility window: pre-§13.13 producers may still emit
        // `aggregate.id` instead of `aggregate.ref`. The parser accepts both.
        $env = $this->validEnvelopeArray();
        $env['aggregate'] = ['type' => 'cycle', 'id' => '42'];

        $envelope = $this->parser->parse(['envelope' => json_encode($env)]);

        $this->assertSame('42', $envelope->aggregateRef);
    }

    #[Test]
    public function it_tolerates_unknown_top_level_keys(): void
    {
        $env = $this->validEnvelopeArray();
        $env['future_field'] = ['new' => true];

        $envelope = $this->parser->parse(['envelope' => json_encode($env)]);
        $this->assertSame('billing.cycle.completed', $envelope->eventType);
    }

    /** @return array<string, mixed> */
    private function validEnvelopeArray(): array
    {
        return [
            'event_id'       => '01JC0000000000000000000001',
            'event_type'     => 'billing.cycle.completed',
            'schema_version' => 1,
            'occurred_at'    => '2026-05-24T12:00:00.000000Z',
            'producer'       => ['service' => 'billing-service', 'version' => '1.4.0'],
            'aggregate'      => ['type' => 'cycle', 'ref' => 'billing-service.subscription_cycles:42'],
            'partition_key'  => 'billing-service.subscription_cycles:42',
            'metadata'       => ['trace_id' => 't-1'],
            'data'           => ['amount_minor' => 50_000],
        ];
    }
}
