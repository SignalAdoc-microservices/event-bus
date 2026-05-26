<?php

namespace Signaladoc\EventBus\Consumer\Contracts;

use Illuminate\Database\UniqueConstraintViolationException;
use Signaladoc\EventBus\Consumer\Envelope;
use Signaladoc\EventBus\Consumer\ReceiptStatus;

/**
 * Persistence abstraction for a `<source>_event_receipts` table.
 *
 * Exists so the Dispatcher doesn't bind to any specific Eloquent model.
 * One implementation (EloquentEventReceiptStore) is enough for every
 * producer — it's parameterised by the receipt model class and the
 * aggregate-column prefix.
 *
 * Three operations cover every Dispatcher branch:
 *
 *  - createReceived(): INSERT a fresh 'received' row inside the caller's
 *    DB transaction. MUST surface UNIQUE violations as
 *    UniqueConstraintViolationException so the Dispatcher's duplicate
 *    branch fires.
 *
 *  - markProcessed(): UPDATE the row to 'processed' inside the same tx.
 *    Idempotent w.r.t. event_id.
 *
 *  - upsertTerminal(): UPSERT a row to a terminal status OUTSIDE any tx.
 *    Used for skipped / failed / dead-lettered paths where the main tx
 *    was rolled back (or never opened). MUST be tolerant of races —
 *    if another consumer wrote first, prefer their row.
 */
interface EventReceiptStore
{
    /**
     * @throws UniqueConstraintViolationException  When event_id already exists.
     */
    public function createReceived(
        Envelope $envelope,
        string $streamName,
        string $consumerGroup,
        string $streamMessageId,
        int $attempts,
    ): void;

    public function markProcessed(string $eventId): void;

    public function upsertTerminal(
        Envelope $envelope,
        string $streamName,
        string $consumerGroup,
        string $streamMessageId,
        ReceiptStatus $status,
        int $attempts,
        ?string $lastError,
    ): void;
}
