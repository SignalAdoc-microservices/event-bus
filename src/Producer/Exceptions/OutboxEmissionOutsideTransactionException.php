<?php

namespace Signaladoc\EventBus\Producer\Exceptions;

use RuntimeException;

/**
 * Thrown when OutboxEmitter::record() is called outside an active DB transaction.
 *
 * The whole point of the outbox pattern is that the row insert and the
 * business state change commit together. Calling the emitter outside a
 * transaction silently breaks that atomicity guarantee — you could end up
 * publishing events for state that was never persisted (or vice versa).
 *
 * Fix: wrap your call site in `DB::transaction(fn () => { ... })` (or use
 * Eloquent's `Model::transaction()` helper). See ARCHITECTURE.md §13.1.
 */
class OutboxEmissionOutsideTransactionException extends RuntimeException
{
    public function __construct(string $eventType)
    {
        parent::__construct(
            "OutboxEmitter::record() called for event '{$eventType}' outside an active DB transaction. "
            . 'Wrap the emission and the business state change in DB::transaction(...) so they commit atomically. '
            . 'See microservice/ARCHITECTURE.md §13.1.',
        );
    }
}
