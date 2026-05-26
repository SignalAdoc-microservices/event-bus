<?php

namespace Signaladoc\EventBus\Consumer;

use Illuminate\Database\Eloquent\Model;
use Signaladoc\EventBus\Consumer\Contracts\EventReceiptStore;

/**
 * Generic EventReceiptStore backed by any Eloquent model that follows the
 * §13.7 receipts shape.
 *
 * Per-producer differences are reduced to two constructor parameters:
 *
 *   - $modelClass             e.g. \App\Models\BillingEventReceipt::class
 *   - $aggregateColumnPrefix  e.g. 'billing' → writes 'billing_aggregate_type'
 *                             and 'billing_aggregate_id'
 *
 * All other columns (event_id, event_type, stream_name, consumer_group,
 * stream_message_id, received_at, processed_at, status, attempts,
 * last_error) are constant across producers per §13.7, so they're
 * hardcoded here.
 *
 * The `<prefix>_aggregate_id` column stores the aggregate's cross-service
 * ref string verbatim (the column name predates the §12.1 ref convention).
 *
 * One instance per producer — typically constructed by DispatcherFactory.
 */
final class EloquentEventReceiptStore implements EventReceiptStore
{
    /**
     * @param  class-string<Model>  $modelClass  Eloquent receipt model for this producer.
     * @param  string  $aggregateColumnPrefix    e.g. 'billing' (no trailing underscore).
     */
    public function __construct(
        private readonly string $modelClass,
        private readonly string $aggregateColumnPrefix,
    ) {}

    public function createReceived(
        Envelope $envelope,
        string $streamName,
        string $consumerGroup,
        string $streamMessageId,
        int $attempts,
    ): void {
        $this->modelClass::query()->create([
            'event_id'                   => $envelope->eventId,
            'event_type'                 => $envelope->eventType,
            $this->aggregateTypeColumn() => $envelope->aggregateType,
            $this->aggregateIdColumn()   => $envelope->aggregateRef,
            'stream_name'                => $streamName,
            'consumer_group'             => $consumerGroup,
            'stream_message_id'          => $streamMessageId,
            'received_at'                => now(),
            'status'                     => ReceiptStatus::Received->value,
            'attempts'                   => $attempts,
        ]);
    }

    public function markProcessed(string $eventId): void
    {
        $this->modelClass::query()
            ->where('event_id', $eventId)
            ->update([
                'status'       => ReceiptStatus::Processed->value,
                'processed_at' => now(),
                'last_error'   => null,
            ]);
    }

    public function upsertTerminal(
        Envelope $envelope,
        string $streamName,
        string $consumerGroup,
        string $streamMessageId,
        ReceiptStatus $status,
        int $attempts,
        ?string $lastError,
    ): void {
        $this->modelClass::query()->updateOrCreate(
            ['event_id' => $envelope->eventId],
            [
                'event_type'                 => $envelope->eventType,
                $this->aggregateTypeColumn() => $envelope->aggregateType,
                $this->aggregateIdColumn()   => $envelope->aggregateRef,
                'stream_name'                => $streamName,
                'consumer_group'             => $consumerGroup,
                'stream_message_id'          => $streamMessageId,
                'received_at'                => now(),
                'processed_at'               => $status->hasProcessedTimestamp() ? now() : null,
                'status'                     => $status->value,
                'attempts'                   => $attempts,
                'last_error'                 => $lastError,
            ],
        );
    }

    private function aggregateTypeColumn(): string
    {
        return "{$this->aggregateColumnPrefix}_aggregate_type";
    }

    private function aggregateIdColumn(): string
    {
        return "{$this->aggregateColumnPrefix}_aggregate_id";
    }
}
