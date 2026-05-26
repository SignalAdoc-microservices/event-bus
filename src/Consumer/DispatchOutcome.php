<?php

namespace Signaladoc\EventBus\Consumer;

/**
 * Per-message result returned by Dispatcher to StreamWorker.
 *
 * Encodes the receipt status PLUS what the worker should do next w.r.t.
 * Redis: XACK on success/skip/duplicate/dead-letter; leave pending (no ACK)
 * on transient failure so the message will be reclaimed/redelivered.
 *
 * `deadLettered` is true when the dispatcher itself decided this is
 * unrecoverable (malformed envelope OR delivery_count >= max_attempts AND
 * the dispatcher already wrote to the DLQ). The worker still XACKs in that
 * case — Redis is done with the message.
 */
enum DispatchOutcome: string
{
    case Processed = 'processed';
    case Skipped = 'skipped';
    case Duplicate = 'duplicate';
    case TransientFailure = 'transient_failure';
    case DeadLettered = 'dead_lettered';

    /**
     * Should the worker call XACK on the originating message?
     *
     * No XACK on TransientFailure ⇒ message stays in the group's pending
     * list and will be re-delivered (either to this consumer's next
     * XREADGROUP call after its visibility timeout, or to whatever consumer
     * wins the next reclaim pass).
     */
    public function shouldAck(): bool
    {
        return match ($this) {
            self::Processed, self::Skipped, self::Duplicate, self::DeadLettered => true,
            self::TransientFailure => false,
        };
    }
}
