<?php

namespace Signaladoc\EventBus\Consumer\Contracts;

/**
 * Abstraction over the Redis Streams commands the consumer needs.
 *
 * Exists for two reasons:
 *
 *  - Tests can inject a fake (no real Redis required).
 *  - Transport swap (e.g. Kafka) is a localised change — the Dispatcher,
 *    StreamWorker, and EventsConsumeCommand stay the same.
 *
 * Implementations MUST:
 *
 *  - ensureGroup() be safe to call repeatedly — `XGROUP CREATE ... MKSTREAM`
 *    is idempotent under "BUSYGROUP" (already-exists) error.
 *  - read() block up to $blockMs and return an empty list on timeout
 *    rather than throwing. Caller treats empty == idle.
 *  - ack() / claim() be safe under retry — Redis treats both as idempotent.
 *  - publishDlq() use MAXLEN ~ N trim (approximate) on every XADD.
 *
 * @phpstan-type StreamMessage array{stream: string, id: string, fields: array<string, string>}
 */
interface StreamReader
{
    /**
     * Idempotent create-or-noop of the consumer group on the given stream.
     * Uses XGROUP CREATE ... MKSTREAM so the stream itself is created if
     * it doesn't exist yet (consumer launched before the first producer
     * emission).
     */
    public function ensureGroup(string $stream, string $group, string $startId = '$'): void;

    /**
     * XREADGROUP GROUP <group> <consumer> COUNT <count> BLOCK <blockMs> STREAMS <streams...> >
     *
     * @param  list<string>  $streams
     * @return list<StreamMessage>  Empty list on timeout / no messages.
     */
    public function read(string $group, string $consumer, array $streams, int $count, int $blockMs): array;

    /**
     * XACK <stream> <group> <id> — removes one message from the group's
     * pending list. Safe to call for unknown ids.
     */
    public function ack(string $stream, string $group, string $messageId): void;

    /**
     * XPENDING <stream> <group> IDLE <minIdleMs> - + <count>
     *
     * @return list<array{id: string, consumer: string, idle_ms: int, delivery_count: int}>
     */
    public function pending(string $stream, string $group, int $minIdleMs, int $count): array;

    /**
     * XCLAIM <stream> <group> <consumer> <minIdleMs> <ids...>
     *
     * @param  list<string>  $messageIds
     * @return list<StreamMessage>  Messages now owned by <consumer>.
     */
    public function claim(string $stream, string $group, string $consumer, int $minIdleMs, array $messageIds): array;

    /**
     * XADD <dlqStream> MAXLEN ~ <maxLen> * envelope <envelope>
     *
     * @return string  Redis-assigned message id (e.g. "1717195094124-0")
     */
    public function publishDlq(string $dlqStream, string $envelope, int $maxLen): string;
}
