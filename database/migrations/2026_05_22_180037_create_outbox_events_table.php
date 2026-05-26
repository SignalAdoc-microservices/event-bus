<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Producer-side transactional outbox table (canonical shape per
     * microservice/ARCHITECTURE.md §13.6).
     *
     * State changes that emit domain events MUST write a row here IN THE
     * SAME DB TRANSACTION as the state change. A separate forwarder process
     * publishes pending rows to Redis Streams for downstream consumers.
     *
     * IDENTITY MODEL
     *   event_id   — server-generated ULID. The DURABLE, business-meaningful
     *                identity downstream consumers dedupe on. Time-sortable.
     *                NOT the Redis Streams message id (transport-local).
     *   stream_message_id — assigned by XADD at publish time. For debugging
     *                only; never used as a consumer dedupe key.
     *
     * STATE MACHINE
     *   pending   → forwarder hasn't picked it up yet (default on INSERT).
     *   published → forwarder successfully XADD'd; safe to archive after
     *               the retention window.
     *   failed    → exceeded retry budget; requires Ops attention (poison
     *               row). Forwarder does NOT auto-retry these.
     *   (No transient "publishing" state — FOR UPDATE SKIP LOCKED inside
     *    the forwarder's TX handles concurrency.)
     *
     * FORWARDER HOT QUERY (drives the (status, available_at, id) index):
     *   SELECT * FROM outbox_events
     *   WHERE status='pending' AND available_at <= NOW()
     *   ORDER BY id
     *   LIMIT 500
     *   FOR UPDATE SKIP LOCKED;
     *
     * RETENTION
     *   `published` rows are hard-deleted after the configured retention
     *   window by a nightly archive job.
     *   `failed`    rows are kept indefinitely until manually handled.
     */
    public function up(): void
    {
        Schema::create('outbox_events', function (Blueprint $table) {
            $table->id();

            // ULID — server-generated at emit time. Consumers dedupe on this.
            $table->char('event_id', 26);

            // Dot notation: <service-domain>.<aggregate>.<verb> (§13.2).
            // e.g. billing.cycle.completed | insurance.coverage.activated | ...
            $table->string('event_type', 100);

            // Aggregate the event is about: cycle | subscription | transaction |
            // policy | coverage_subscription | ...  Drives stream naming convention.
            $table->string('aggregate_type', 64);

            // Stores the aggregate's cross-service ref string (§12.1):
            // `<service>.<table>:<id>`. Column name is `aggregate_id` for
            // historical reasons; the value is the ref, and the wire envelope
            // surfaces it as `aggregate.ref` (§13.2). Bumped to 128 chars to
            // comfortably fit refs like `billing-service.subscription_cycles:18234`.
            $table->string('aggregate_id', 128);

            // Denormalized routing target: e.g. "billing.cycle", "billing.subscription".
            // The forwarder reads this directly to decide which Redis stream to XADD to.
            // By convention: "<service>.<aggregate_type>".
            $table->string('stream_name', 100);

            // "These events MUST stay in order with each other" hint. Usually
            // = aggregate ref. Not used today (Redis Streams are fully ordered
            // per-stream); reserved for future stream-sharding or Kafka migration.
            $table->string('partition_key', 128)->nullable();

            // Versioned business payload — MUST include a top-level `schema_version`
            // field. This becomes the envelope's `data` block at publish time.
            $table->json('payload');

            // Tracing / causation context — correlation_id, causation_id,
            // trace_id, user_ref, etc. Becomes the envelope's `metadata` block.
            $table->json('metadata')->nullable();

            // When the business event actually happened (set by emitting code,
            // NOT by the forwarder). Microsecond precision for sub-second
            // ordering within a single transaction.
            $table->timestamp('occurred_at', 6);

            // Earliest time the forwarder is allowed to publish this row.
            // Default = occurred_at. Set later only for scheduled emissions.
            $table->timestamp('available_at');

            // pending → published | failed. See state machine in docblock above.
            $table->enum('status', ['pending', 'published', 'failed'])->default('pending');

            // Forwarder retry counter. Bumped on every failed attempt.
            // attempts >= max → status flips to 'failed' (poison).
            $table->unsignedTinyInteger('attempts')->default(0);

            // Most recent error message from XADD or envelope build.
            // Cleared on success.
            $table->text('last_error')->nullable();

            // When XADD succeeded. Null until then.
            $table->timestamp('published_at')->nullable();

            // Redis Streams ID returned by XADD (e.g. "1717195094124-0").
            // For debugging + consumer-log correlation only.
            $table->string('stream_message_id', 32)->nullable();

            $table->timestamps();

            $table->unique('event_id');
            // Forwarder hot path — keep this index lean and selective.
            $table->index(['status', 'available_at', 'id']);
            // Per-aggregate replay/debug: "all events for subscription #42 in order".
            $table->index(['aggregate_type', 'aggregate_id', 'id']);
            // Analytics: "all cycle.completed events in date range".
            $table->index(['event_type', 'occurred_at']);
            // Nightly archive job: WHERE status='published' AND created_at < ...
            $table->index('created_at');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('outbox_events');
    }
};
