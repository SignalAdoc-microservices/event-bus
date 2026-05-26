<?php

namespace Signaladoc\EventBus\Tests\Support;

use Signaladoc\EventBus\Consumer\Contracts\EventHandler;
use Signaladoc\EventBus\Consumer\Envelope;
use Throwable;

/**
 * Test handler that records every envelope it sees and can be told to throw.
 *
 * Use ->throwOnNext(new SomeException) to fail the next handle() call —
 * lets tests exercise transient failure / skipped / dead-letter paths
 * without needing real business code.
 */
final class RecordingHandler implements EventHandler
{
    /** @var list<Envelope> */
    public array $seen = [];

    private ?Throwable $throwNext = null;

    public function handle(Envelope $envelope): void
    {
        $this->seen[] = $envelope;

        if ($this->throwNext !== null) {
            $e = $this->throwNext;
            $this->throwNext = null;
            throw $e;
        }
    }

    public function throwOnNext(Throwable $e): void
    {
        $this->throwNext = $e;
    }
}
