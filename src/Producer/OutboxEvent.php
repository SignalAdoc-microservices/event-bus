<?php

namespace Signaladoc\EventBus\Producer;

use Carbon\CarbonInterface;
use Illuminate\Database\Eloquent\Model;

/**
 * Producer-side transactional outbox row.
 *
 * Written by the emission hook in the SAME DB transaction as the business
 * state change. Drained by the {@see Forwarder} process and XADD'd to
 * Redis Streams.
 *
 * The owning service depends on this class directly — it is the canonical
 * model for the `outbox_events` table shipped by this package.
 *
 * @see microservice/ARCHITECTURE.md §13.6 — canonical column shape
 * @see Forwarder — drainer
 * @see EnvelopeBuilder — row → envelope JSON
 *
 * @property int $id
 * @property string $event_id ULID
 * @property string $event_type
 * @property string $aggregate_type
 * @property string $aggregate_id    Stores the aggregate's cross-service ref (§12.1),
 *                                   e.g. `billing-service.subscription_cycles:42`.
 *                                   Column is named `aggregate_id` for historical
 *                                   reasons; the value is treated as a ref on the wire.
 * @property string $stream_name
 * @property string|null $partition_key
 * @property array $payload
 * @property array|null $metadata
 * @property CarbonInterface $occurred_at
 * @property CarbonInterface $available_at
 * @property string $status pending|published|failed
 * @property int $attempts
 * @property string|null $last_error
 * @property CarbonInterface|null $published_at
 * @property string|null $stream_message_id
 * @property CarbonInterface $created_at
 * @property CarbonInterface $updated_at
 */
class OutboxEvent extends Model
{
    public const STATUS_PENDING = 'pending';

    public const STATUS_PUBLISHED = 'published';

    public const STATUS_FAILED = 'failed';

    protected $table = 'outbox_events';

    /**
     * All columns are filled by the emission hook OR by the forwarder.
     * No user input ever reaches this model, so mass-assignment is safe.
     */
    protected $guarded = [];

    protected $casts = [
        'payload'      => 'array',
        'metadata'     => 'array',
        'occurred_at'  => 'datetime',
        'available_at' => 'datetime',
        'published_at' => 'datetime',
        'attempts'     => 'integer',
    ];
}
