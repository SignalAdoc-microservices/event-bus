<?php

namespace Signaladoc\EventBus\Consumer\Exceptions;

use RuntimeException;

/**
 * Thrown by EnvelopeParser when a Redis message can't be parsed into a valid
 * Envelope — missing top-level keys, non-JSON payload, etc.
 *
 * Dispatcher treats this as a terminal failure (XACK + DLQ): redelivering a
 * malformed message will never help; only producer fix + replay will.
 */
class MalformedEnvelopeException extends RuntimeException
{
    public function __construct(string $reason)
    {
        parent::__construct("Malformed event envelope: {$reason}");
    }
}
