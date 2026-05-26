<?php

namespace Signaladoc\EventBus\Tests\Feature\Producer;

use Illuminate\Http\Request;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use PHPUnit\Framework\Attributes\Test;
use Signaladoc\EventBus\Producer\Exceptions\OutboxEmissionOutsideTransactionException;
use Signaladoc\EventBus\Producer\OutboxEmitter;
use Signaladoc\EventBus\Producer\OutboxEvent;
use Signaladoc\EventBus\Tests\Support\TestEventTypeMap;
use Signaladoc\EventBus\Tests\TestCase;

class OutboxEmitterTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->createOutboxEventsTable();
    }

    #[Test]
    public function it_inserts_a_row_with_all_auto_fills(): void
    {
        $row = DB::transaction(fn () => $this->makeEmitter()->emit(
            eventType: TestEventTypeMap::CYCLE_COMPLETED,
            aggregateId: '17',
            payload: ['cycle_id' => 17, 'amount_minor' => 250000],
            partitionKey: 'subscription:42',
        ));

        $this->assertSame(TestEventTypeMap::CYCLE_COMPLETED, $row->event_type);
        $this->assertSame('cycle', $row->aggregate_type);
        $this->assertSame('billing.cycle', $row->stream_name);
        $this->assertSame('17', $row->aggregate_id);
        $this->assertSame('subscription:42', $row->partition_key);
        $this->assertSame(OutboxEvent::STATUS_PENDING, $row->status);
        $this->assertSame(0, $row->attempts);
        $this->assertNotEmpty($row->event_id);
        $this->assertSame(26, strlen($row->event_id));
        $this->assertNotNull($row->occurred_at);
        $this->assertNotNull($row->available_at);
        $this->assertSame(['cycle_id' => 17, 'amount_minor' => 250000], $row->payload);
    }

    #[Test]
    public function available_at_defaults_to_occurred_at_when_omitted(): void
    {
        $when = Carbon::parse('2026-05-24T15:30:45Z');

        $row = DB::transaction(fn () => $this->makeEmitter()->emit(
            eventType: TestEventTypeMap::CYCLE_COMPLETED,
            aggregateId: '17',
            payload: [],
            occurredAt: $when,
        ));

        $this->assertTrue($row->occurred_at->equalTo($when));
        $this->assertTrue($row->available_at->equalTo($when));
    }

    #[Test]
    public function it_honours_an_explicit_future_available_at_for_scheduled_events(): void
    {
        $occurred = Carbon::parse('2026-05-24T15:30:45Z');
        $available = $occurred->copy()->addHour();

        $row = DB::transaction(fn () => $this->makeEmitter()->emit(
            eventType: TestEventTypeMap::CYCLE_COMPLETED,
            aggregateId: '17',
            payload: [],
            occurredAt: $occurred,
            availableAt: $available,
        ));

        $this->assertTrue($row->occurred_at->equalTo($occurred));
        $this->assertTrue($row->available_at->equalTo($available));
    }

    #[Test]
    public function it_auto_attaches_correlation_id_from_the_current_request(): void
    {
        $request = Request::create('/whatever');
        $request->headers->set('X-Request-Id', 'req-abc-123');

        $row = DB::transaction(fn () => $this->makeEmitter($request)->emit(
            eventType: TestEventTypeMap::CYCLE_COMPLETED,
            aggregateId: '17',
            payload: [],
        ));

        $this->assertSame(['correlation_id' => 'req-abc-123'], $row->metadata);
    }

    #[Test]
    public function it_does_not_overwrite_a_caller_supplied_correlation_id(): void
    {
        $request = Request::create('/whatever');
        $request->headers->set('X-Request-Id', 'from-header');

        $row = DB::transaction(fn () => $this->makeEmitter($request)->emit(
            eventType: TestEventTypeMap::CYCLE_COMPLETED,
            aggregateId: '17',
            payload: [],
            metadata: ['correlation_id' => 'caller-wins'],
        ));

        $this->assertSame(['correlation_id' => 'caller-wins'], $row->metadata);
    }

    #[Test]
    public function metadata_is_null_when_no_request_and_no_caller_metadata(): void
    {
        $row = DB::transaction(fn () => $this->makeEmitter()->emit(
            eventType: TestEventTypeMap::CYCLE_COMPLETED,
            aggregateId: '17',
            payload: [],
        ));

        $this->assertNull($row->metadata);
    }

    #[Test]
    public function it_throws_when_called_outside_a_transaction(): void
    {
        $this->expectException(OutboxEmissionOutsideTransactionException::class);
        $this->expectExceptionMessageMatches('/billing\.cycle\.completed/');

        $this->makeEmitter()->emit(
            eventType: TestEventTypeMap::CYCLE_COMPLETED,
            aggregateId: '17',
            payload: [],
        );
    }

    #[Test]
    public function it_throws_when_event_type_is_not_registered(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        DB::transaction(fn () => $this->makeEmitter()->emit(
            eventType: 'billing.cycle.totally_made_up',
            aggregateId: '17',
            payload: [],
        ));
    }

    #[Test]
    public function the_static_facade_resolves_through_the_container(): void
    {
        // Rebind OutboxEmitter against the same connection but a deterministic
        // (no-Request) instance, then call through the static facade.
        $this->app->instance(OutboxEmitter::class, $this->makeEmitter());

        $row = DB::transaction(fn () => OutboxEmitter::record(
            eventType: TestEventTypeMap::INVOICE_PAID,
            aggregateId: '99',
            payload: ['invoice_id' => 99],
        ));

        $this->assertSame('billing.invoice', $row->stream_name);
        $this->assertSame('invoice', $row->aggregate_type);
        $this->assertSame('99', $row->aggregate_id);
    }

    private function makeEmitter(?Request $request = null): OutboxEmitter
    {
        return new OutboxEmitter(
            db: DB::connection(),
            eventTypes: new TestEventTypeMap,
            request: $request,
        );
    }

    /**
     * Mirrors the production migration columns the emitter writes to. If
     * you add a column the emitter populates, add it here too.
     */
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
}
