<?php

namespace Signaladoc\EventBus\Tests\Support;

use Illuminate\Database\Eloquent\Model;
use Signaladoc\EventBus\Consumer\ReceiptStatus;

/**
 * Eloquent fixture mirroring the canonical receipt schema (ARCHITECTURE.md §13.7).
 *
 * Used in package tests as a stand-in for service-specific receipt models
 * like insurance-service's `BillingEventReceipt`. Same shape, same casts,
 * different table name + aggregate column prefix to keep the package tests
 * decoupled from any concrete service.
 */
class TestEventReceipt extends Model
{
    protected $table = 'test_event_receipts';

    protected $guarded = [];

    protected $casts = [
        'received_at'  => 'datetime',
        'processed_at' => 'datetime',
        'attempts'     => 'integer',
        'status'       => ReceiptStatus::class,
    ];
}
