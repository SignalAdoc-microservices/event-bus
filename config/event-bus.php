<?php

/**
 * signaladoc/event-bus configuration.
 *
 * Both halves of the inter-service event contract:
 *   - `producer.*`  — transactional outbox forwarder knobs (services that emit events)
 *   - `consumer.*`  — Redis Streams worker knobs (services that read events)
 *
 * Producer-only services (e.g. billing-service) only need to populate the
 * `producer.*` block. Consumer-only services (e.g. insurance-service today)
 * only need the `consumer.*` block. Services that are both populate both.
 *
 * See:
 *   - microservice/ARCHITECTURE.md §13   (Inter-service events)
 *   - microservice/ARCHITECTURE.md §13.13 (Locked event contracts)
 */

return [

    /*
    |--------------------------------------------------------------------------
    | Producer side
    |--------------------------------------------------------------------------
    */
    'producer' => [

        /*
         * Producer identity (envelope)
         * Stamped on every envelope's `producer.{service,version}` field.
         * `version` should be the deployed release tag or git SHA — pass it
         * via env at deploy time.
         */
        'service' => env('EVENT_BUS_PRODUCER_SERVICE'),
        'version' => env('APP_VERSION', 'dev'),

        /*
         * Envelope schema version (§13.2).
         * Bumped ONLY for breaking envelope-shape changes. Per-event-type
         * payload evolution uses `data.schema_version` set by the emitter.
         */
        'envelope_schema_version' => 1,

        'forwarder' => [
            // Rows pulled per SELECT cycle. Larger = fewer round-trips, bigger lock window.
            'batch_size' => (int) env('EVENT_BUS_OUTBOX_BATCH_SIZE', 500),

            // Sleep between empty polls (ms). Non-empty polls re-poll immediately.
            'poll_idle_ms' => (int) env('EVENT_BUS_OUTBOX_POLL_IDLE_MS', 5_000),

            // After this many failed publish attempts, the row is marked `failed`
            // and the forwarder stops retrying it (Ops alert + manual reset).
            'max_attempts' => (int) env('EVENT_BUS_OUTBOX_MAX_ATTEMPTS', 5),

            // Per-cycle wall-clock budget for the SELECT+publish+UPDATE block.
            // Defensive timeout in case Redis hangs mid-batch.
            'cycle_timeout_seconds' => (int) env('EVENT_BUS_OUTBOX_CYCLE_TIMEOUT', 30),
        ],

        'redis' => [
            // Which Laravel redis connection to use for XADD. Separate from
            // cache/queue connections so producer traffic doesn't compete.
            'connection' => env('EVENT_BUS_PRODUCER_REDIS_CONNECTION', 'default'),

            // MAXLEN ~ N trim policy on every XADD. Approximate trim (the `~`)
            // is faster than exact trim and good enough for our purposes.
            'stream_maxlen' => (int) env('EVENT_BUS_STREAM_MAXLEN', 1_000_000),
        ],

        /*
         * Retention
         * Hard-delete `published` rows older than this many days, via a
         * nightly `outbox:archive` command (separate from the forwarder).
         * `failed` rows are kept indefinitely until Ops resolves.
         */
        'retention' => [
            'published_days' => (int) env('EVENT_BUS_OUTBOX_RETENTION_DAYS', 7),
        ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Consumer side
    |--------------------------------------------------------------------------
    */
    'consumer' => [

        'redis' => [
            // Which Laravel redis connection to use for XREADGROUP/XACK/XPENDING/XCLAIM.
            'connection' => env('EVENT_BUS_CONSUMER_REDIS_CONNECTION', 'default'),
        ],

        'worker' => [
            // Messages per XREADGROUP call.
            'batch_size' => (int) env('EVENT_BUS_CONSUMER_BATCH_SIZE', 32),

            // Block timeout for XREADGROUP (ms). 0 = non-blocking.
            'block_ms' => (int) env('EVENT_BUS_CONSUMER_BLOCK_MS', 5_000),

            // Sleep between empty polls (ms).
            'poll_idle_ms' => (int) env('EVENT_BUS_CONSUMER_POLL_IDLE_MS', 1_000),
        ],

        /*
         * Retry & dead-letter (§13.9)
         * After `max_attempts` redeliveries of the same event_id, the
         * consumer XACKs and writes to <source-stream>.dlq.
         */
        'retry' => [
            'max_attempts' => (int) env('EVENT_BUS_CONSUMER_MAX_ATTEMPTS', 5),
            'dlq_stream_maxlen' => (int) env('EVENT_BUS_CONSUMER_DLQ_MAXLEN', 100_000),
        ],

        /*
         * Pending-list reclaim (XPENDING + XCLAIM) — §13.8
         */
        'reclaim' => [
            'min_idle_ms' => (int) env('EVENT_BUS_CONSUMER_RECLAIM_MIN_IDLE_MS', 60_000),
            'batch_size' => (int) env('EVENT_BUS_CONSUMER_RECLAIM_BATCH', 64),
            'every_n_idle_cycles' => (int) env('EVENT_BUS_CONSUMER_RECLAIM_EVERY_N_IDLE', 10),
        ],
    ],

];
