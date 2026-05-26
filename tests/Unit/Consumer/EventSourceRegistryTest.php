<?php

namespace Signaladoc\EventBus\Tests\Unit\Consumer;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Signaladoc\EventBus\Consumer\EventSource;
use Signaladoc\EventBus\Consumer\EventSourceRegistry;
use Signaladoc\EventBus\Tests\Support\TestEventReceipt;

/**
 * Guards the per-producer wiring contract enforced by the registry.
 *
 * The most important behaviour here is the duplicate-group check —
 * two sources sharing a consumer group would silently route different
 * messages through the same handlers, which is impossible to debug.
 * It must fail loudly at boot.
 */
class EventSourceRegistryTest extends TestCase
{
    #[Test]
    public function it_returns_the_owning_source_for_a_known_group(): void
    {
        $registry = new EventSourceRegistry;
        $billing = $this->billingSource();

        $registry->register($billing);

        $this->assertSame($billing, $registry->forGroup('insurance.streak-engine'));
        $this->assertSame($billing, $registry->get('billing'));
        $this->assertSame(['insurance.streak-engine'], $registry->knownGroups());
    }

    #[Test]
    public function it_returns_null_for_an_unknown_group(): void
    {
        $registry = new EventSourceRegistry;

        $this->assertNull($registry->forGroup('insurance.does-not-exist'));
    }

    #[Test]
    public function it_throws_when_registering_the_same_source_twice(): void
    {
        $registry = new EventSourceRegistry;
        $registry->register($this->billingSource());

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches("/'billing' is already registered/");

        $registry->register($this->billingSource());
    }

    #[Test]
    public function it_throws_when_two_sources_claim_the_same_group(): void
    {
        $registry = new EventSourceRegistry;
        $registry->register($this->billingSource());

        $colliding = new EventSource(
            name: 'telemedicine',
            receiptModel: TestEventReceipt::class,
            aggregateColumnPrefix: 'telemedicine',
            groupStreams: [
                'insurance.streak-engine' => ['telemedicine.consultation'],
            ],
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches(
            "/group 'insurance.streak-engine' is already claimed by source 'billing'/",
        );

        $registry->register($colliding);
    }

    #[Test]
    public function get_throws_a_descriptive_error_for_unknown_source(): void
    {
        $registry = new EventSourceRegistry;
        $registry->register($this->billingSource());

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches("/Unknown event source 'unknown'.*Registered: billing/");

        $registry->get('unknown');
    }

    #[Test]
    public function event_source_rejects_a_receipt_model_that_is_not_an_eloquent_model(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new EventSource(
            name: 'x',
            receiptModel: self::class,
            aggregateColumnPrefix: 'x',
            groupStreams: ['x.consumer' => ['x.stream']],
        );
    }

    #[Test]
    public function event_source_rejects_empty_group_streams(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new EventSource(
            name: 'x',
            receiptModel: TestEventReceipt::class,
            aggregateColumnPrefix: 'x',
            groupStreams: [],
        );
    }

    private function billingSource(): EventSource
    {
        return new EventSource(
            name: 'billing',
            receiptModel: TestEventReceipt::class,
            aggregateColumnPrefix: 'billing',
            groupStreams: [
                'insurance.streak-engine' => ['billing.cycle', 'billing.subscription'],
            ],
        );
    }
}
