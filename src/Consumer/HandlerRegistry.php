<?php

namespace Signaladoc\EventBus\Consumer;

use Signaladoc\EventBus\Consumer\Contracts\EventHandler;

/**
 * Maps `event_type` → list<EventHandler>.
 *
 * Registered at boot (service providers). The Dispatcher reads the registry
 * once per message; an unrecognised event_type is NOT an error — the
 * Dispatcher marks it `skipped` and XACKs. This matters because a producer's
 * stream carries every event for that aggregate, but consumers only care
 * about a subset.
 */
final class HandlerRegistry
{
    /** @var array<string, list<EventHandler>> */
    private array $handlers = [];

    public function register(string $eventType, EventHandler $handler): void
    {
        $this->handlers[$eventType][] = $handler;
    }

    /** @return list<EventHandler> */
    public function handlersFor(string $eventType): array
    {
        return $this->handlers[$eventType] ?? [];
    }

    /** @return list<string> */
    public function knownEventTypes(): array
    {
        return array_keys($this->handlers);
    }
}
