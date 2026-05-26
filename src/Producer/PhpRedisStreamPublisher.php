<?php

namespace Signaladoc\EventBus\Producer;

use Illuminate\Redis\RedisManager;
use RuntimeException;
use Signaladoc\EventBus\Producer\Contracts\StreamPublisher;
use Signaladoc\EventBus\Support\RawRedis;
use Throwable;

/**
 * Concrete StreamPublisher using Laravel's Redis manager (phpredis or Predis).
 *
 * Emits:
 *   XADD <stream> MAXLEN ~ <maxlen> * envelope <json>
 *
 * The single field name "envelope" is intentional — consumers know to read
 * one field and json_decode it. Multi-field XADD is harder for clients in
 * other languages to introspect; we keep the wire format dead simple.
 *
 * Uses {@see RawRedis} to send the command through the underlying client's
 * raw RESP entrypoint (phpredis `rawCommand`, Predis `executeRaw`). The
 * Laravel `Connection::command()` wrapper dispatches to typed methods like
 * `Redis::xadd($key, $id, $values, $maxlen, $approximate)` which can't
 * accept raw positional args — see RawRedis docblock.
 */
final class PhpRedisStreamPublisher implements StreamPublisher
{
    public function __construct(
        private readonly RedisManager $redis,
        private readonly string $connection,
    ) {}

    public function publish(string $stream, string $envelope, int $maxLen): string
    {
        try {
            $messageId = RawRedis::send($this->redis->connection($this->connection), 'XADD', [
                $stream,
                'MAXLEN',
                '~',
                (string) $maxLen,
                '*',
                'envelope',
                $envelope,
            ]);

            if (! is_string($messageId) || $messageId === '') {
                throw new RuntimeException('XADD returned non-string id: '.var_export($messageId, true));
            }

            return $messageId;
        } catch (Throwable $e) {
            // Surface as a uniform exception type so the Forwarder's catch
            // block doesn't need to know about phpredis internals.
            throw new RuntimeException(
                "XADD to stream '{$stream}' failed: {$e->getMessage()}",
                previous: $e,
            );
        }
    }
}
