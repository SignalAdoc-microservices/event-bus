<?php

namespace Signaladoc\EventBus\Consumer;

/**
 * Tally of one StreamWorker loop iteration (one XREADGROUP + reclaim pass).
 *
 * Mirrors {@see \Signaladoc\EventBus\Producer\ForwarderCycleResult} — same
 * shape on purpose so the EventsConsumeCommand output, future Horizon-style
 * dashboards, and metrics emitters can treat producer + consumer cycles
 * uniformly.
 */
final readonly class ConsumerCycleResult
{
    public function __construct(
        public int $read,
        public int $processed,
        public int $skipped,
        public int $duplicates,
        public int $transientFailures,
        public int $deadLettered,
        public int $reclaimed,
    ) {}

    public static function empty(): self
    {
        return new self(0, 0, 0, 0, 0, 0, 0);
    }

    public function isIdle(): bool
    {
        return $this->read === 0 && $this->reclaimed === 0;
    }
}
