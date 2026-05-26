<?php

namespace Signaladoc\EventBus\Consumer;

/**
 * Lifecycle states for any `<source>_event_receipts` row.
 *
 * Same values across every producer→consumer pairing — defined once here
 * rather than re-declaring `STATUS_*` constants on every model.
 *
 * State machine:
 *   Received  ──ok──▶ Processed
 *             ──no-handler / SkipEventException──▶ Skipped
 *             ──exception──▶ Failed ──redeliver──▶ Received (loop) ──n×──▶ DLQ
 *
 * No `duplicate` state — duplicates fail the UNIQUE(event_id) constraint
 * at INSERT time; the existing row is already in its terminal state
 * (per microservice/ARCHITECTURE.md §13.7).
 */
enum ReceiptStatus: string
{
    case Received = 'received';

    case Processed = 'processed';

    case Failed = 'failed';

    case Skipped = 'skipped';

    public function isTerminal(): bool
    {
        return match ($this) {
            self::Processed, self::Skipped, self::Failed => true,
            self::Received => false,
        };
    }

    public function hasProcessedTimestamp(): bool
    {
        return $this === self::Processed || $this === self::Skipped;
    }
}
