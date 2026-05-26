<?php

namespace Signaladoc\EventBus\Consumer;

use InvalidArgumentException;

/**
 * Process-wide registry of every upstream producer this service consumes from.
 *
 * Bound as a singleton by ConsumerServiceProvider. Each producer registers
 * itself via ->register() at boot:
 *
 *     $registry->register(new EventSource(
 *         name: 'billing',
 *         receiptModel: BillingEventReceipt::class,
 *         aggregateColumnPrefix: 'billing',
 *         groupStreams: [
 *             'insurance.streak-engine' => ['billing.cycle', 'billing.subscription'],
 *         ],
 *     ));
 *
 * The EventsConsumeCommand and DispatcherFactory both consult this
 * registry — they never enumerate sources themselves.
 *
 * Group names are globally unique across the service (a group identifies
 * one consumer-side responsibility); two sources may NOT share a group.
 */
final class EventSourceRegistry
{
    /** @var array<string, EventSource> */
    private array $byName = [];

    public function register(EventSource $source): void
    {
        if (isset($this->byName[$source->name])) {
            throw new InvalidArgumentException(
                "EventSource '{$source->name}' is already registered",
            );
        }

        foreach ($source->groupNames() as $group) {
            $owner = $this->forGroup($group);
            if ($owner !== null) {
                throw new InvalidArgumentException(
                    "Consumer group '{$group}' is already claimed by source '{$owner->name}'; "
                    ."cannot also register it under '{$source->name}'",
                );
            }
        }

        $this->byName[$source->name] = $source;
    }

    public function get(string $name): EventSource
    {
        if (! isset($this->byName[$name])) {
            throw new InvalidArgumentException(
                "Unknown event source '{$name}'. Registered: ".implode(', ', array_keys($this->byName)),
            );
        }

        return $this->byName[$name];
    }

    public function forGroup(string $group): ?EventSource
    {
        foreach ($this->byName as $source) {
            if ($source->hasGroup($group)) {
                return $source;
            }
        }

        return null;
    }

    /** @return list<EventSource> */
    public function all(): array
    {
        return array_values($this->byName);
    }

    /** @return list<string> */
    public function knownGroups(): array
    {
        $groups = [];
        foreach ($this->byName as $source) {
            foreach ($source->groupNames() as $g) {
                $groups[] = $g;
            }
        }

        return $groups;
    }
}
