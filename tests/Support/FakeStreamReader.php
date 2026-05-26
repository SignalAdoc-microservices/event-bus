<?php

namespace Signaladoc\EventBus\Tests\Support;

use Signaladoc\EventBus\Consumer\Contracts\StreamReader;

/**
 * In-memory StreamReader for tests.
 *
 * Lets tests:
 *   - pre-load messages into per-stream FIFO queues, then assert ACKs / DLQ rows
 *   - simulate the pending-list / reclaim path
 *   - assert the worker correctly distinguishes "no messages" from "error"
 *
 * Not threadsafe (tests don't need it).
 */
final class FakeStreamReader implements StreamReader
{
    /** @var array<string, list<array{id: string, fields: array<string, string>}>> */
    private array $messages = [];

    /** @var list<array{stream: string, group: string, id: string}> */
    public array $acks = [];

    /** @var list<array{stream: string, group: string, startId: string}> */
    public array $groupsCreated = [];

    /** @var list<array{stream: string, envelope: string, maxLen: int}> */
    public array $dlqWrites = [];

    /** @var array<string, list<array{id: string, consumer: string, idle_ms: int, delivery_count: int}>> */
    public array $pendingByStream = [];

    /** @var array<string, list<array{id: string, fields: array<string, string>}>> */
    public array $claimableByStream = [];

    private int $dlqCounter = 0;

    /**
     * Helper for tests — queue a message that the next read() will return.
     *
     * @param  array<string, string>  $fields
     */
    public function queue(string $stream, string $id, array $fields): void
    {
        $this->messages[$stream][] = ['id' => $id, 'fields' => $fields];
    }

    public function ensureGroup(string $stream, string $group, string $startId = '$'): void
    {
        $this->groupsCreated[] = ['stream' => $stream, 'group' => $group, 'startId' => $startId];
    }

    public function read(string $group, string $consumer, array $streams, int $count, int $blockMs): array
    {
        $batch = [];
        foreach ($streams as $stream) {
            while ($count > 0 && ! empty($this->messages[$stream])) {
                $msg = array_shift($this->messages[$stream]);
                $batch[] = [
                    'stream' => $stream,
                    'id'     => $msg['id'],
                    'fields' => $msg['fields'],
                ];
                $count--;
            }
        }

        return $batch;
    }

    public function ack(string $stream, string $group, string $messageId): void
    {
        $this->acks[] = ['stream' => $stream, 'group' => $group, 'id' => $messageId];
    }

    public function pending(string $stream, string $group, int $minIdleMs, int $count): array
    {
        return array_slice($this->pendingByStream[$stream] ?? [], 0, $count);
    }

    public function claim(string $stream, string $group, string $consumer, int $minIdleMs, array $messageIds): array
    {
        $available = $this->claimableByStream[$stream] ?? [];
        $byId = [];
        foreach ($available as $m) {
            $byId[$m['id']] = $m;
        }

        $claimed = [];
        foreach ($messageIds as $id) {
            if (isset($byId[$id])) {
                $claimed[] = [
                    'stream' => $stream,
                    'id'     => $byId[$id]['id'],
                    'fields' => $byId[$id]['fields'],
                ];
            }
        }

        return $claimed;
    }

    public function publishDlq(string $dlqStream, string $envelope, int $maxLen): string
    {
        $this->dlqWrites[] = [
            'stream'   => $dlqStream,
            'envelope' => $envelope,
            'maxLen'   => $maxLen,
        ];
        $this->dlqCounter++;

        return sprintf('%d-%d', 1_700_000_000_000 + $this->dlqCounter, 0);
    }
}
