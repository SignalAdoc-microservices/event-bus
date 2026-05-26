<?php

namespace Signaladoc\EventBus\Producer\Contracts;

use InvalidArgumentException;

/**
 * Producer-side `event_type` catalogue contract.
 *
 * Decouples the framework-level {@see \Signaladoc\EventBus\Producer\OutboxEmitter}
 * from any specific service's event types. Each producing service ships ONE
 * implementation listing its known event types and the routing rules
 * (event_type → aggregate_type → stream_name).
 *
 * Implementations MUST throw {@see InvalidArgumentException} for unknown
 * event types — fail fast at the emission site rather than letting bad
 * data land in the outbox table and surface in the forwarder.
 *
 * See ARCHITECTURE.md §13.2 / §13.3 for the event-type + stream-naming
 * conventions every implementation MUST follow.
 */
interface EventTypeMap
{
    /**
     * @throws InvalidArgumentException  When $eventType is not registered.
     */
    public function aggregateFor(string $eventType): string;

    /**
     * @throws InvalidArgumentException  When $eventType is not registered.
     */
    public function streamFor(string $eventType): string;
}
