<?php

namespace Signaladoc\EventBus\Consumer;

use Illuminate\Redis\RedisManager;
use RuntimeException;
use Signaladoc\EventBus\Consumer\Contracts\StreamReader;
use Throwable;

/**
 * Concrete StreamReader using Laravel's Redis manager (phpredis under the hood).
 *
 * Every Redis call uses ->command(...) with the raw protocol arguments so we don't
 * depend on phpredis-vs-predis high-level signatures (they differ across
 * client versions, especially for XCLAIM and XPENDING IDLE).
 *
 * Designed to be quiet on benign edge cases:
 *  - ensureGroup() swallows "BUSYGROUP" (group exists) — that's the happy path.
 *  - read() / pending() / claim() return [] on "no messages" rather than throwing.
 */
final class PhpRedisStreamReader implements StreamReader
{
    public function __construct(
        private readonly RedisManager $redis,
        private readonly string $connection,
    ) {}

    public function ensureGroup(string $stream, string $group, string $startId = '$'): void
    {
        try {
            // MKSTREAM = create the stream itself if absent (we may boot before producer's first XADD).
            $this->conn()->command('XGROUP', ['CREATE', $stream, $group, $startId, 'MKSTREAM']);
        } catch (Throwable $e) {
            // BUSYGROUP is normal on every subsequent boot — only re-throw real errors.
            if (! str_contains($e->getMessage(), 'BUSYGROUP')) {
                throw new RuntimeException(
                    "XGROUP CREATE on stream '{$stream}' group '{$group}' failed: {$e->getMessage()}",
                    previous: $e,
                );
            }
        }
    }

    public function read(string $group, string $consumer, array $streams, int $count, int $blockMs): array
    {
        // XREADGROUP GROUP <g> <c> COUNT <n> BLOCK <ms> STREAMS <s1> <s2> > > ...
        $args = ['GROUP', $group, $consumer, 'COUNT', (string) $count, 'BLOCK', (string) $blockMs, 'STREAMS'];
        foreach ($streams as $s) {
            $args[] = $s;
        }
        foreach ($streams as $_) {
            $args[] = '>';
        }

        try {
            $raw = $this->conn()->command('XREADGROUP', $args);
        } catch (Throwable $e) {
            throw new RuntimeException("XREADGROUP failed: {$e->getMessage()}", previous: $e);
        }

        // Timeout / no messages: phpredis returns false; predis returns null.
        if ($raw === false || $raw === null) {
            return [];
        }

        return $this->normaliseReadResponse($raw);
    }

    public function ack(string $stream, string $group, string $messageId): void
    {
        try {
            $this->conn()->command('XACK', [$stream, $group, $messageId]);
        } catch (Throwable $e) {
            // XACK failing is bad (message will redeliver) but not fatal here —
            // the worker logs and continues; idempotency at handler level protects us.
            throw new RuntimeException("XACK failed for {$stream}/{$messageId}: {$e->getMessage()}", previous: $e);
        }
    }

    public function pending(string $stream, string $group, int $minIdleMs, int $count): array
    {
        // XPENDING <stream> <group> IDLE <min-idle> - + <count>
        try {
            $raw = $this->conn()->command('XPENDING', [
                $stream,
                $group,
                'IDLE', (string) $minIdleMs,
                '-', '+',
                (string) $count,
            ]);
        } catch (Throwable $e) {
            throw new RuntimeException("XPENDING failed on {$stream}: {$e->getMessage()}", previous: $e);
        }

        if (! is_array($raw)) {
            return [];
        }

        $out = [];
        foreach ($raw as $row) {
            // Each row: [id, consumer, idle_ms, delivery_count]
            if (! is_array($row) || count($row) < 4) {
                continue;
            }
            $out[] = [
                'id'             => (string) $row[0],
                'consumer'       => (string) $row[1],
                'idle_ms'        => (int) $row[2],
                'delivery_count' => (int) $row[3],
            ];
        }

        return $out;
    }

    public function claim(string $stream, string $group, string $consumer, int $minIdleMs, array $messageIds): array
    {
        if ($messageIds === []) {
            return [];
        }

        $args = [$stream, $group, $consumer, (string) $minIdleMs, ...$messageIds];

        try {
            $raw = $this->conn()->command('XCLAIM', $args);
        } catch (Throwable $e) {
            throw new RuntimeException("XCLAIM failed on {$stream}: {$e->getMessage()}", previous: $e);
        }

        if (! is_array($raw)) {
            return [];
        }

        // XCLAIM returns the same shape as a single-stream XREADGROUP body:
        // [[id, [field, value, ...]], ...]  — synthesise the stream label.
        $messages = [];
        foreach ($raw as $entry) {
            if (! is_array($entry) || count($entry) < 2) {
                continue;
            }
            [$id, $fieldArray] = $entry;
            $messages[] = [
                'stream' => $stream,
                'id'     => (string) $id,
                'fields' => $this->fieldArrayToMap($fieldArray),
            ];
        }

        return $messages;
    }

    public function publishDlq(string $dlqStream, string $envelope, int $maxLen): string
    {
        try {
            $id = $this->conn()->command('XADD', [
                $dlqStream,
                'MAXLEN', '~', (string) $maxLen,
                '*',
                'envelope', $envelope,
            ]);
        } catch (Throwable $e) {
            throw new RuntimeException("XADD to DLQ '{$dlqStream}' failed: {$e->getMessage()}", previous: $e);
        }

        if (! is_string($id) || $id === '') {
            throw new RuntimeException('XADD DLQ returned non-string id: '.var_export($id, true));
        }

        return $id;
    }

    /**
     * Flatten an XREADGROUP response (multi-stream) into a list of
     * {stream, id, fields} records, regardless of phpredis vs predis shape.
     *
     * Phpredis shape:   ['stream1' => [['id', ['f', 'v', ...]], ...], 'stream2' => ...]
     * Predis shape:     [['stream1', [['id', ['f', 'v', ...]], ...]], ...]
     *
     * @return list<array{stream: string, id: string, fields: array<string, string>}>
     */
    private function normaliseReadResponse(mixed $raw): array
    {
        $messages = [];
        if (! is_array($raw)) {
            return $messages;
        }

        foreach ($raw as $streamKey => $streamEntries) {
            // Predis style: $raw is a list of [streamName, entries].
            if (is_int($streamKey) && is_array($streamEntries) && count($streamEntries) === 2 && is_string($streamEntries[0])) {
                $streamName = $streamEntries[0];
                $entries = $streamEntries[1];
            } else {
                $streamName = (string) $streamKey;
                $entries = $streamEntries;
            }

            if (! is_array($entries)) {
                continue;
            }

            foreach ($entries as $entry) {
                if (! is_array($entry) || count($entry) < 2) {
                    continue;
                }
                [$id, $fieldArray] = $entry;
                $messages[] = [
                    'stream' => $streamName,
                    'id'     => (string) $id,
                    'fields' => $this->fieldArrayToMap($fieldArray),
                ];
            }
        }

        return $messages;
    }

    /**
     * Convert flat [field, value, field, value, ...] into [field => value, ...].
     * Phpredis already returns the map form for XREADGROUP in some versions,
     * so accept both shapes.
     *
     * @return array<string, string>
     */
    private function fieldArrayToMap(mixed $fieldArray): array
    {
        if (! is_array($fieldArray)) {
            return [];
        }

        $isAssoc = array_keys($fieldArray) !== range(0, count($fieldArray) - 1);
        if ($isAssoc) {
            $out = [];
            foreach ($fieldArray as $k => $v) {
                $out[(string) $k] = (string) $v;
            }

            return $out;
        }

        $out = [];
        $count = count($fieldArray);
        for ($i = 0; $i + 1 < $count; $i += 2) {
            $out[(string) $fieldArray[$i]] = (string) $fieldArray[$i + 1];
        }

        return $out;
    }

    private function conn(): \Illuminate\Redis\Connections\Connection
    {
        return $this->redis->connection($this->connection);
    }
}
