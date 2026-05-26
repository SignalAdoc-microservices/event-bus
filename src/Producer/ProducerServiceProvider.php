<?php

namespace Signaladoc\EventBus\Producer;

use Illuminate\Database\DatabaseManager;
use Illuminate\Http\Request;
use Illuminate\Redis\RedisManager;
use Illuminate\Support\ServiceProvider;
use Psr\Log\LoggerInterface;
use Signaladoc\EventBus\Producer\Console\OutboxForwardCommand;
use Signaladoc\EventBus\Producer\Contracts\EventTypeMap;
use Signaladoc\EventBus\Producer\Contracts\StreamPublisher;

/**
 * Wires the producer-side of the event-bus into Laravel's container.
 *
 * Service-specific bindings (concrete EventTypeMap, registry of event-type
 * constants) live in the consuming application's own service provider —
 * this provider stays clean and service-agnostic.
 *
 * Registers the `outbox:forward` artisan command and publishes the
 * package's config + migrations.
 *
 * @see microservice/ARCHITECTURE.md §13 — transactional outbox
 */
final class ProducerServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../../config/event-bus.php', 'event-bus');

        $this->app->singleton(StreamPublisher::class, function ($app) {
            return new PhpRedisStreamPublisher(
                redis: $app->make(RedisManager::class),
                connection: (string) config('event-bus.producer.redis.connection', 'default'),
            );
        });

        $this->app->singleton(EnvelopeBuilder::class, function () {
            return new EnvelopeBuilder(
                producerService: (string) config('event-bus.producer.service'),
                producerVersion: (string) config('event-bus.producer.version', 'dev'),
                envelopeSchemaVersion: (int) config('event-bus.producer.envelope_schema_version', 1),
            );
        });

        // OutboxEmitter is bound as a singleton so the static facade
        // (OutboxEmitter::record) always resolves the same instance.
        // The Request is resolved lazily — null in CLI/queue contexts.
        $this->app->singleton(OutboxEmitter::class, function ($app) {
            return new OutboxEmitter(
                db: $app->make(DatabaseManager::class)->connection(),
                eventTypes: $app->make(EventTypeMap::class),
                request: $app->bound(Request::class) ? $app->make(Request::class) : null,
            );
        });

        $this->app->bind(Forwarder::class, function ($app) {
            return new Forwarder(
                db: $app->make(DatabaseManager::class)->connection(),
                publisher: $app->make(StreamPublisher::class),
                envelopeBuilder: $app->make(EnvelopeBuilder::class),
                logger: $app->make(LoggerInterface::class),
                batchSize: (int) config('event-bus.producer.forwarder.batch_size', 500),
                maxAttempts: (int) config('event-bus.producer.forwarder.max_attempts', 5),
                streamMaxLen: (int) config('event-bus.producer.redis.stream_maxlen', 1_000_000),
            );
        });
    }

    public function boot(): void
    {
        $this->loadMigrationsFrom(__DIR__.'/../../database/migrations');

        if (! $this->app->runningInConsole()) {
            return;
        }

        $this->commands([
            OutboxForwardCommand::class,
        ]);

        $this->publishes([
            __DIR__.'/../../config/event-bus.php' => config_path('event-bus.php'),
        ], 'event-bus-config');

        $this->publishesMigrations([
            __DIR__.'/../../database/migrations' => database_path('migrations'),
        ], 'event-bus-migrations');
    }
}
