<?php

namespace Signaladoc\EventBus\Tests\Support;

use InvalidArgumentException;
use Signaladoc\EventBus\Producer\Contracts\EventTypeMap;

/**
 * Minimal in-test EventTypeMap implementation.
 *
 * Concrete services ship their own (e.g. billing-service's EventTypeRegistry).
 * This one carries just the events the package's own tests need.
 */
final class TestEventTypeMap implements EventTypeMap
{
    public const CYCLE_COMPLETED = 'billing.cycle.completed';

    public const INVOICE_PAID = 'billing.invoice.paid';

    private const EVENT_TO_AGGREGATE = [
        self::CYCLE_COMPLETED => 'cycle',
        self::INVOICE_PAID    => 'invoice',
    ];

    private const AGGREGATE_TO_STREAM = [
        'cycle'   => 'billing.cycle',
        'invoice' => 'billing.invoice',
    ];

    public function aggregateFor(string $eventType): string
    {
        return self::EVENT_TO_AGGREGATE[$eventType]
            ?? throw new InvalidArgumentException("Unknown event_type '{$eventType}'.");
    }

    public function streamFor(string $eventType): string
    {
        return self::AGGREGATE_TO_STREAM[$this->aggregateFor($eventType)];
    }
}
