<?php

namespace Signaladoc\EventBus\Producer;

use Illuminate\Redis\RedisManager;
use RuntimeException;
use Signaladoc\EventBus\Producer\Contracts\StreamPublisher;
use Throwable;

/**
 * Concrete StreamPublisher using Laravel's Redis manager (phpredis under the hood).
 *
 * Emits:
 *   XADD <stream> MAXLEN ~ <maxlen> * envelope <json>
 *
 * The single field name "envelope" is intentional — consumers know to read
 * one field and json_decode it. Multi-field XADD is harder for clients in
 * other languages to introspect; we keep the wire format dead simple.
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
            // Raw command for cross-client-version safety: phpredis vs predis differ on the high-level xAdd signature.
            $messageId = $this->redis->connection($this->connection)->command('XADD', [
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
