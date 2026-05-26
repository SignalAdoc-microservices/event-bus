<?php

namespace Signaladoc\EventBus\Tests\Feature\Producer;

use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Str;
use PHPUnit\Framework\Attributes\Test;
use Psr\Log\NullLogger;
use Signaladoc\EventBus\Producer\EnvelopeBuilder;
use Signaladoc\EventBus\Producer\Forwarder;
use Signaladoc\EventBus\Producer\OutboxEvent;
use Signaladoc\EventBus\Tests\Support\FakeStreamPublisher;
use Signaladoc\EventBus\Tests\TestCase;

/**
 * Integration test for Forwarder against an in-memory SQLite DB and a fake publisher.
 *
 * Skips the rest of the migrations on purpose — we only need outbox_events
 * for these tests, and pulling in unrelated migrations would couple this
 * test to schema changes elsewhere.
 */
class ForwarderTest extends TestCase
{
    private FakeStreamPublisher $publisher;

    protected function setUp(): void
    {
        parent::setUp();
        $this->createOutboxEventsTable();
        $this->publisher = new FakeStreamPublisher;
    }

    #[Test]
    public function it_publishes_pending_rows_and_marks_them_published(): void
    {
        $this->seedPendingRow(eventType: 'cycle.completed', streamName: 'billing.cycle');
        $this->seedPendingRow(eventType: 'invoice.paid', streamName: 'billing.invoice');

        $result = $this->makeForwarder()->processOnce();

        $this->assertSame(2, $result->published);
        $this->assertSame(0, $result->failedTransient);
        $this->assertSame(0, $result->failedTerminal);

        $this->assertCount(2, $this->publisher->published);
        $this->assertSame('billing.cycle', $this->publisher->published[0]['stream']);
        $this->assertSame('billing.invoice', $this->publisher->published[1]['stream']);

        $rows = OutboxEvent::all();
        $this->assertCount(2, $rows);
        foreach ($rows as $row) {
            $this->assertSame(OutboxEvent::STATUS_PUBLISHED, $row->status);
            $this->assertNotNull($row->published_at);
            $this->assertNotNull($row->stream_message_id);
            $this->assertNull($row->last_error);
        }
    }

    #[Test]
    public function it_serializes_envelopes_with_the_expected_top_level_keys(): void
    {
        $this->seedPendingRow();

        $this->makeForwarder()->processOnce();

        $envelope = json_decode($this->publisher->published[0]['envelope'], true);
        $this->assertSame([
            'event_id', 'event_type', 'schema_version', 'occurred_at',
            'producer', 'aggregate', 'partition_key', 'metadata', 'data',
        ], array_keys($envelope));
    }

    #[Test]
    public function it_passes_the_configured_maxlen_on_every_publish(): void
    {
        $this->seedPendingRow();

        $this->makeForwarder(streamMaxLen: 42_000)->processOnce();

        $this->assertSame(42_000, $this->publisher->published[0]['maxLen']);
    }

    #[Test]
    public function it_skips_rows_whose_available_at_is_in_the_future(): void
    {
        $this->seedPendingRow(availableAt: Carbon::now()->addMinutes(5));

        $result = $this->makeForwarder()->processOnce();

        $this->assertSame(0, $result->processedCount());
        $this->assertCount(0, $this->publisher->published);
    }

    #[Test]
    public function transient_failure_leaves_row_pending_and_bumps_attempts(): void
    {
        $this->seedPendingRow();
        $this->publisher->failNext(1, 'connection reset');

        $result = $this->makeForwarder(maxAttempts: 5)->processOnce();

        $this->assertSame(0, $result->published);
        $this->assertSame(1, $result->failedTransient);
        $this->assertSame(0, $result->failedTerminal);

        $row = OutboxEvent::firstOrFail();
        $this->assertSame(OutboxEvent::STATUS_PENDING, $row->status);
        $this->assertSame(1, $row->attempts);
        $this->assertStringContainsString('connection reset', $row->last_error);
    }

    #[Test]
    public function reaching_max_attempts_marks_the_row_failed(): void
    {
        $this->seedPendingRow(attempts: 4);
        $this->publisher->failNext(1, 'still broken');

        $result = $this->makeForwarder(maxAttempts: 5)->processOnce();

        $this->assertSame(1, $result->failedTerminal);

        $row = OutboxEvent::firstOrFail();
        $this->assertSame(OutboxEvent::STATUS_FAILED, $row->status);
        $this->assertSame(5, $row->attempts);
    }

    #[Test]
    public function a_successful_publish_after_a_prior_failure_clears_last_error(): void
    {
        $this->seedPendingRow(attempts: 2, lastError: 'previous failure');

        $this->makeForwarder()->processOnce();

        $row = OutboxEvent::firstOrFail();
        $this->assertSame(OutboxEvent::STATUS_PUBLISHED, $row->status);
        $this->assertNull($row->last_error);
    }

    #[Test]
    public function it_ignores_rows_already_published_or_failed(): void
    {
        $this->seedRow(['status' => OutboxEvent::STATUS_PUBLISHED]);
        $this->seedRow(['status' => OutboxEvent::STATUS_FAILED]);
        $this->seedPendingRow();

        $result = $this->makeForwarder()->processOnce();

        $this->assertSame(1, $result->published);
        $this->assertCount(1, $this->publisher->published);
    }

    #[Test]
    public function batch_size_caps_the_number_of_rows_processed_per_cycle(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->seedPendingRow();
        }

        $result = $this->makeForwarder(batchSize: 2)->processOnce();

        $this->assertSame(2, $result->published);
        $this->assertSame(3, OutboxEvent::where('status', OutboxEvent::STATUS_PENDING)->count());
    }

    private function makeForwarder(
        int $batchSize = 100,
        int $maxAttempts = 5,
        int $streamMaxLen = 1_000_000,
    ): Forwarder {
        return new Forwarder(
            db: DB::connection(),
            publisher: $this->publisher,
            envelopeBuilder: new EnvelopeBuilder(
                producerService: 'billing-service',
                producerVersion: 'test',
                envelopeSchemaVersion: 1,
            ),
            logger: new NullLogger,
            batchSize: $batchSize,
            maxAttempts: $maxAttempts,
            streamMaxLen: $streamMaxLen,
        );
    }

    private function createOutboxEventsTable(): void
    {
        Schema::create('outbox_events', function ($table) {
            $table->bigIncrements('id');
            $table->char('event_id', 26);
            $table->string('event_type', 100);
            $table->string('aggregate_type', 50);
            $table->string('aggregate_id', 128);
            $table->string('stream_name', 100);
            $table->string('partition_key', 128)->nullable();
            $table->json('payload');
            $table->json('metadata')->nullable();
            $table->timestamp('occurred_at', 6);
            $table->timestamp('available_at', 6);
            $table->string('status', 20)->default('pending');
            $table->unsignedTinyInteger('attempts')->default(0);
            $table->text('last_error')->nullable();
            $table->timestamp('published_at', 6)->nullable();
            $table->string('stream_message_id', 50)->nullable();
            $table->timestamps(6);
        });
    }

    private function seedPendingRow(
        string $eventType = 'cycle.completed',
        string $streamName = 'billing.cycle',
        ?Carbon $availableAt = null,
        int $attempts = 0,
        ?string $lastError = null,
    ): OutboxEvent {
        return $this->seedRow([
            'event_type'   => $eventType,
            'stream_name'  => $streamName,
            'available_at' => $availableAt ?? Carbon::now(),
            'attempts'     => $attempts,
            'last_error'   => $lastError,
            'status'       => OutboxEvent::STATUS_PENDING,
        ]);
    }

    /** @param array<string, mixed> $overrides */
    private function seedRow(array $overrides = []): OutboxEvent
    {
        return OutboxEvent::create(array_merge([
            'event_id'       => (string) Str::ulid(),
            'event_type'     => 'cycle.completed',
            'aggregate_type' => 'cycle',
            'aggregate_id'   => (string) Str::ulid(),
            'stream_name'    => 'billing.cycle',
            'partition_key'  => null,
            'payload'        => ['k' => 'v'],
            'metadata'       => null,
            'occurred_at'    => Carbon::now(),
            'available_at'   => Carbon::now(),
            'status'         => OutboxEvent::STATUS_PENDING,
            'attempts'       => 0,
        ], $overrides));
    }
}
