<?php

namespace Signaladoc\EventBus\Producer\Contracts;

/**
 * Abstraction over the Redis Streams XADD call.
 *
 * Exists so we can:
 *  - inject a fake in tests (no real Redis required)
 *  - swap transports later (Kafka, NATS, etc.) without touching the Forwarder
 *
 * Implementations MUST:
 *  - call XADD with MAXLEN ~ <maxlen> trim policy on every publish
 *  - return the Redis-assigned message id (e.g. "1717195094124-0") on success
 *  - throw on any transport-level failure (the Forwarder catches and records)
 */
interface StreamPublisher
{
    /**
     * @param  string  $stream  Stream name, e.g. "billing.cycle"
     * @param  string  $envelope  The canonical envelope JSON (single field "envelope")
     * @param  int  $maxLen  MAXLEN ~ N trim threshold (approximate)
     * @return string Stream message id assigned by Redis (e.g. "1717195094124-0")
     *
     * @throws \RuntimeException on transport failure
     */
    public function publish(string $stream, string $envelope, int $maxLen): string;
}
