<?php

namespace Signaladoc\EventBus\Tests\Support;

use RuntimeException;
use Signaladoc\EventBus\Producer\Contracts\StreamPublisher;

/**
 * In-memory StreamPublisher for tests.
 *
 * Records every publish() call so assertions can inspect the wire-format
 * envelope and target stream. Optionally fails the next N calls to
 * simulate Redis flakes (used to test attempts + last_error logic).
 */
final class FakeStreamPublisher implements StreamPublisher
{
    /** @var list<array{stream: string, envelope: string, maxLen: int}> */
    public array $published = [];

    private int $failNext = 0;

    private string $failureMessage = 'simulated Redis failure';

    private int $messageCounter = 0;

    public function publish(string $stream, string $envelope, int $maxLen): string
    {
        if ($this->failNext > 0) {
            $this->failNext--;
            throw new RuntimeException($this->failureMessage);
        }

        $this->published[] = [
            'stream'   => $stream,
            'envelope' => $envelope,
            'maxLen'   => $maxLen,
        ];

        $this->messageCounter++;

        return sprintf('%d-%d', 1_700_000_000_000 + $this->messageCounter, 0);
    }

    public function failNext(int $count, string $message = 'simulated Redis failure'): void
    {
        $this->failNext = $count;
        $this->failureMessage = $message;
    }
}
