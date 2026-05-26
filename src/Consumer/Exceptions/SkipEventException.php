<?php

namespace Signaladoc\EventBus\Consumer\Exceptions;

use RuntimeException;

/**
 * Thrown by an EventHandler when the event is well-formed but doesn't apply
 * to anything this service tracks (e.g. billing.cycle.completed for a
 * subscription with no insurance coverage).
 *
 * Dispatcher catches this, marks the receipt as `skipped`, and XACKs. The
 * distinction between `skipped` and `processed` matters for analytics.
 */
class SkipEventException extends RuntimeException
{
}
