<?php

namespace Signaladoc\EventBus\Consumer\Contracts;

use Signaladoc\EventBus\Consumer\Envelope;

/**
 * One business-logic handler for one (or more) event types.
 *
 * The Dispatcher invokes handle() INSIDE the per-message DB transaction.
 * Handlers MUST:
 *
 *  - Be idempotent at the business layer (rely on the relevant domain
 *    UNIQUE constraint — e.g. coverage_cycle_log.billing_cycle_ref — so
 *    a redelivered event that already updated state is a no-op).
 *
 *  - NOT call XACK / DB::commit() / DB::beginTransaction() themselves.
 *    The Dispatcher owns the transaction boundary and the ACK.
 *
 *  - Throw to fail. Any thrown exception rolls back the transaction (the
 *    receipt INSERT included) and prevents the XACK — the message will be
 *    redelivered.
 *
 *  - Throw {@see \Signaladoc\EventBus\Consumer\Exceptions\SkipEventException}
 *    if the event is well-formed but doesn't apply (e.g. the aggregate
 *    isn't tracked by this service). The receipt is recorded with
 *    status=`skipped` and the message is XACKed without business-layer changes.
 *
 * Handlers are registered with HandlerRegistry — typically in a service
 * provider — and looked up by `event_type`. Multiple handlers can register
 * for the same event_type; they run in registration order, all inside the
 * same transaction. Throwing in any handler aborts the whole batch entry.
 */
interface EventHandler
{
    public function handle(Envelope $envelope): void;
}
