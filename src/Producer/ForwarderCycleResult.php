<?php

namespace Signaladoc\EventBus\Producer;

/**
 * Summary of one Forwarder::processOnce() cycle.
 *
 * Surfaced by the OutboxForwardCommand for logs/metrics. Also returned
 * from tests so they can assert on the cycle outcome.
 */
final readonly class ForwarderCycleResult
{
    public function __construct(
        public int $published,
        public int $failedTransient,
        public int $failedTerminal,
        public int $elapsedMs,
    ) {}

    public function processedCount(): int
    {
        return $this->published + $this->failedTransient + $this->failedTerminal;
    }

    public function isIdle(): bool
    {
        return $this->processedCount() === 0;
    }
}
