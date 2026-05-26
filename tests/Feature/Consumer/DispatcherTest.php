<?php

namespace Signaladoc\EventBus\Tests\Feature\Consumer;

use Illuminate\Support\Facades\Schema;
use PHPUnit\Framework\Attributes\Test;
use Psr\Log\NullLogger;
use RuntimeException;
use Signaladoc\EventBus\Consumer\Dispatcher;
use Signaladoc\EventBus\Consumer\DispatchOutcome;
use Signaladoc\EventBus\Consumer\EloquentEventReceiptStore;
use Signaladoc\EventBus\Consumer\EnvelopeParser;
use Signaladoc\EventBus\Consumer\Exceptions\SkipEventException;
use Signaladoc\EventBus\Consumer\HandlerRegistry;
use Signaladoc\EventBus\Consumer\ReceiptStatus;
use Signaladoc\EventBus\Tests\Support\FakeStreamReader;
use Signaladoc\EventBus\Tests\Support\RecordingHandler;
use Signaladoc\EventBus\Tests\Support\TestEventReceipt;
use Signaladoc\EventBus\Tests\TestCase;

/**
 * Feature test for Dispatcher against in-memory SQLite + FakeStreamReader.
 *
 * Verifies the §13.5 canonical consumer flow against the TestEventReceipt
 * fixture — but the Dispatcher itself never references that model. It
 * talks to an EloquentEventReceiptStore. The same test shape works for
 * any concrete receipt model by swapping the model class + aggregate
 * prefix in the store constructor.
 *
 * Covered branches:
 *   - Processed   (happy path; receipt=processed; no DLQ)
 *   - Duplicate   (second delivery of same event_id; UNIQUE caught; no handler re-run)
 *   - Skipped     (handler throws SkipEventException; receipt=skipped; no DLQ)
 *   - TransientFailure (handler throws; receipt=failed; no XACK semantics on outcome)
 *   - DeadLettered (delivery_count > max_attempts; DLQ written; receipt=failed)
 *   - DeadLettered (malformed envelope; DLQ written under `dlq.malformed`)
 *   - Skipped     (no handlers registered for event_type)
 */
class DispatcherTest extends TestCase
{
    private const STREAM = 'billing.cycle';

    private const GROUP = 'test.streak-engine';

    private const AGGREGATE_PREFIX = 'test';

    private FakeStreamReader $reader;

    private HandlerRegistry $registry;

    protected function setUp(): void
    {
        parent::setUp();
        $this->createTestEventReceiptsTable();
        $this->reader = new FakeStreamReader;
        $this->registry = new HandlerRegistry;
    }

    #[Test]
    public function processed_path_writes_receipt_and_runs_all_registered_handlers(): void
    {
        $h1 = new RecordingHandler;
        $h2 = new RecordingHandler;
        $this->registry->register('billing.cycle.completed', $h1);
        $this->registry->register('billing.cycle.completed', $h2);

        $outcome = $this->dispatcher()->dispatch(
            stream: self::STREAM,
            messageId: '1700000000000-0',
            rawFields: $this->encodeEnvelope($this->validEnvelope()),
            deliveryCount: 1,
            consumerGroup: self::GROUP,
        );

        $this->assertSame(DispatchOutcome::Processed, $outcome);
        $this->assertCount(1, $h1->seen);
        $this->assertCount(1, $h2->seen);

        $receipt = TestEventReceipt::where('event_id', '01JC0000000000000000000001')->firstOrFail();
        $this->assertSame(ReceiptStatus::Processed, $receipt->status);
        $this->assertNotNull($receipt->processed_at);
        $this->assertNull($receipt->last_error);
        $this->assertSame(self::GROUP, $receipt->consumer_group);
        $this->assertSame(self::STREAM, $receipt->stream_name);
        $this->assertSame('1700000000000-0', $receipt->stream_message_id);
        $this->assertSame(1, $receipt->attempts);
        // Confirm the store mapped to the configured aggregate column prefix.
        // The aggregate_id column now stores the cross-service ref (§13.2 wire shape).
        $this->assertSame('cycle', $receipt->test_aggregate_type);
        $this->assertSame('billing-service.subscription_cycles:42', $receipt->test_aggregate_id);

        $this->assertSame([], $this->reader->dlqWrites);
    }

    #[Test]
    public function duplicate_delivery_returns_duplicate_without_re_running_handlers(): void
    {
        $h = new RecordingHandler;
        $this->registry->register('billing.cycle.completed', $h);
        $dispatcher = $this->dispatcher();

        $rawFields = $this->encodeEnvelope($this->validEnvelope());

        $first = $dispatcher->dispatch(self::STREAM, '1700000000000-0', $rawFields, 1, self::GROUP);
        $second = $dispatcher->dispatch(self::STREAM, '1700000000001-0', $rawFields, 2, self::GROUP);

        $this->assertSame(DispatchOutcome::Processed, $first);
        $this->assertSame(DispatchOutcome::Duplicate, $second);
        $this->assertCount(1, $h->seen);

        $this->assertSame(
            ReceiptStatus::Processed,
            TestEventReceipt::where('event_id', '01JC0000000000000000000001')->first()->status,
        );
    }

    #[Test]
    public function skip_event_exception_marks_receipt_skipped_and_does_not_dead_letter(): void
    {
        $h = new RecordingHandler;
        $h->throwOnNext(new SkipEventException('no coverage for this subscription'));
        $this->registry->register('billing.cycle.completed', $h);

        $outcome = $this->dispatcher()->dispatch(
            stream: self::STREAM,
            messageId: '1700000000000-0',
            rawFields: $this->encodeEnvelope($this->validEnvelope()),
            deliveryCount: 1,
            consumerGroup: self::GROUP,
        );

        $this->assertSame(DispatchOutcome::Skipped, $outcome);
        $this->assertCount(1, $h->seen);

        $receipt = TestEventReceipt::where('event_id', '01JC0000000000000000000001')->firstOrFail();
        $this->assertSame(ReceiptStatus::Skipped, $receipt->status);
        $this->assertNotNull($receipt->processed_at);
        $this->assertSame('no coverage for this subscription', $receipt->last_error);

        $this->assertSame([], $this->reader->dlqWrites);
    }

    #[Test]
    public function transient_failure_records_failed_receipt_and_does_not_dead_letter(): void
    {
        $h = new RecordingHandler;
        $h->throwOnNext(new RuntimeException('downstream HTTP 500'));
        $this->registry->register('billing.cycle.completed', $h);

        $outcome = $this->dispatcher()->dispatch(
            stream: self::STREAM,
            messageId: '1700000000000-0',
            rawFields: $this->encodeEnvelope($this->validEnvelope()),
            deliveryCount: 2,
            consumerGroup: self::GROUP,
        );

        $this->assertSame(DispatchOutcome::TransientFailure, $outcome);
        $this->assertFalse($outcome->shouldAck());

        $receipt = TestEventReceipt::where('event_id', '01JC0000000000000000000001')->firstOrFail();
        $this->assertSame(ReceiptStatus::Failed, $receipt->status);
        $this->assertSame(2, $receipt->attempts);
        $this->assertSame('downstream HTTP 500', $receipt->last_error);

        $this->assertSame([], $this->reader->dlqWrites);
    }

    #[Test]
    public function max_attempts_exceeded_dead_letters_and_xacks(): void
    {
        $h = new RecordingHandler;
        $this->registry->register('billing.cycle.completed', $h);

        $outcome = $this->dispatcher(maxAttempts: 3)->dispatch(
            stream: self::STREAM,
            messageId: '1700000000000-0',
            rawFields: $this->encodeEnvelope($this->validEnvelope()),
            deliveryCount: 4,
            consumerGroup: self::GROUP,
        );

        $this->assertSame(DispatchOutcome::DeadLettered, $outcome);
        $this->assertTrue($outcome->shouldAck());
        $this->assertCount(0, $h->seen, 'handler must not run when dead-lettering');

        $this->assertCount(1, $this->reader->dlqWrites);
        $this->assertSame(self::STREAM.'.dlq', $this->reader->dlqWrites[0]['stream']);

        $envelope = json_decode($this->reader->dlqWrites[0]['envelope'], true);
        $this->assertSame('billing.cycle.completed', $envelope['event_type']);
        $this->assertSame('max_attempts_exceeded', $envelope['metadata']['dlq']['reason']);
        $this->assertSame(4, $envelope['metadata']['dlq']['delivery_count']);

        $receipt = TestEventReceipt::where('event_id', '01JC0000000000000000000001')->firstOrFail();
        $this->assertSame(ReceiptStatus::Failed, $receipt->status);
        $this->assertSame(4, $receipt->attempts);
    }

    #[Test]
    public function malformed_envelope_is_dead_lettered_under_dlq_malformed(): void
    {
        $outcome = $this->dispatcher()->dispatch(
            stream: self::STREAM,
            messageId: '1700000000000-0',
            rawFields: ['envelope' => 'not-json'],
            deliveryCount: 1,
            consumerGroup: self::GROUP,
        );

        $this->assertSame(DispatchOutcome::DeadLettered, $outcome);
        $this->assertTrue($outcome->shouldAck());

        $this->assertCount(1, $this->reader->dlqWrites);
        $envelope = json_decode($this->reader->dlqWrites[0]['envelope'], true);
        $this->assertSame('dlq.malformed', $envelope['event_type']);
        $this->assertSame('malformed_envelope', $envelope['metadata']['dlq']['reason']);
        $this->assertSame('1700000000000-0', $envelope['metadata']['dlq']['source_message_id']);

        $this->assertSame(0, TestEventReceipt::count());
    }

    #[Test]
    public function unregistered_event_type_is_skipped_without_running_anything(): void
    {
        $outcome = $this->dispatcher()->dispatch(
            stream: self::STREAM,
            messageId: '1700000000000-0',
            rawFields: $this->encodeEnvelope($this->validEnvelope()),
            deliveryCount: 1,
            consumerGroup: self::GROUP,
        );

        $this->assertSame(DispatchOutcome::Skipped, $outcome);

        $receipt = TestEventReceipt::where('event_id', '01JC0000000000000000000001')->firstOrFail();
        $this->assertSame(ReceiptStatus::Skipped, $receipt->status);
        $this->assertSame('no handlers registered for event_type', $receipt->last_error);

        $this->assertSame([], $this->reader->dlqWrites);
    }

    private function dispatcher(int $maxAttempts = 5, int $dlqMaxLen = 100_000): Dispatcher
    {
        return new Dispatcher(
            parser: new EnvelopeParser,
            registry: $this->registry,
            reader: $this->reader,
            receipts: new EloquentEventReceiptStore(
                modelClass: TestEventReceipt::class,
                aggregateColumnPrefix: self::AGGREGATE_PREFIX,
            ),
            logger: new NullLogger,
            maxAttempts: $maxAttempts,
            dlqMaxLen: $dlqMaxLen,
        );
    }

    /** @return array<string, mixed> */
    private function validEnvelope(string $eventId = '01JC0000000000000000000001'): array
    {
        return [
            'event_id'       => $eventId,
            'event_type'     => 'billing.cycle.completed',
            'schema_version' => 1,
            'occurred_at'    => '2026-05-24T12:00:00.000000Z',
            'producer'       => ['service' => 'billing-service', 'version' => '1.4.0'],
            'aggregate'      => ['type' => 'cycle', 'ref' => 'billing-service.subscription_cycles:42'],
            'partition_key'  => 'billing-service.subscription_cycles:42',
            'metadata'       => ['trace_id' => 't-1'],
            'data'           => ['amount_minor' => 50_000],
        ];
    }

    /**
     * @param  array<string, mixed>  $envelope
     * @return array<string, string>
     */
    private function encodeEnvelope(array $envelope): array
    {
        return ['envelope' => json_encode($envelope)];
    }

    /**
     * Mirrors the canonical billing_event_receipts shape from
     * ARCHITECTURE.md §13.7 — just with a different table name and
     * aggregate column prefix, to demonstrate that the dispatcher / store
     * don't care which concrete receipt model a consuming service supplies.
     */
    private function createTestEventReceiptsTable(): void
    {
        Schema::create('test_event_receipts', function ($table) {
            $table->bigIncrements('id');
            $table->char('event_id', 26)->unique();
            $table->string('event_type', 100);
            $table->string('test_aggregate_type', 64);
            $table->string('test_aggregate_id', 128);
            $table->string('stream_name', 100);
            $table->string('consumer_group', 100);
            $table->string('stream_message_id', 32);
            $table->timestamp('received_at');
            $table->timestamp('processed_at')->nullable();
            $table->string('status', 20)->default('received');
            $table->unsignedTinyInteger('attempts')->default(0);
            $table->text('last_error')->nullable();
            $table->timestamps();
        });
    }
}
