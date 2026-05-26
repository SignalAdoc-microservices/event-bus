<?php

namespace Signaladoc\EventBus\Producer;

/**
 * Per-row outcome reported by Forwarder::publishRow().
 */
enum PublishOutcome
{
    case Published;
    case FailedTransient; // will be retried next cycle (attempts < max)
    case FailedTerminal;  // status flipped to 'failed'; needs Ops attention
}
