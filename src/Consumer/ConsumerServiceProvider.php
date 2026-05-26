<?php

namespace Signaladoc\EventBus\Consumer;

use Illuminate\Contracts\Foundation\Application;
use Illuminate\Redis\RedisManager;
use Illuminate\Support\ServiceProvider;
use Psr\Log\LoggerInterface;
use Signaladoc\EventBus\Consumer\Console\EventsConsumeCommand;
use Signaladoc\EventBus\Consumer\Contracts\StreamReader;

/**
 * Wires the consumer-side of the event-bus into Laravel's container.
 *
 * Shared singletons (one per process, used by every source):
 *   - EnvelopeParser
 *   - HandlerRegistry      (handlers register against this from per-source
 *                           providers — see the consuming app's
 *                           App\Providers\<Source>SourceServiceProvider)
 *   - StreamReader         (concrete PhpRedisStreamReader)
 *   - EventSourceRegistry  (catalogue of producers we consume from)
 *   - DispatcherFactory    (builds a per-source Dispatcher on demand)
 *
 * Per-source registrations (EventSource + handler bindings) live in the
 * consuming app's own service providers. Adding a new upstream producer =
 * adding one new provider; this file does not change.
 */
final class ConsumerServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../../config/event-bus.php', 'event-bus');

        $this->app->singleton(EnvelopeParser::class);
        $this->app->singleton(HandlerRegistry::class);
        $this->app->singleton(EventSourceRegistry::class);

        $this->app->singleton(StreamReader::class, function (Application $app): StreamReader {
            return new PhpRedisStreamReader(
                $app->make(RedisManager::class),
                (string) $app['config']->get('event-bus.consumer.redis.connection', 'default'),
            );
        });

        $this->app->singleton(DispatcherFactory::class, function (Application $app): DispatcherFactory {
            $config = $app['config'];

            return new DispatcherFactory(
                parser: $app->make(EnvelopeParser::class),
                registry: $app->make(HandlerRegistry::class),
                reader: $app->make(StreamReader::class),
                logger: $app->make(LoggerInterface::class),
                maxAttempts: (int) $config->get('event-bus.consumer.retry.max_attempts', 5),
                dlqMaxLen: (int) $config->get('event-bus.consumer.retry.dlq_stream_maxlen', 100_000),
            );
        });
    }

    public function boot(): void
    {
        if (! $this->app->runningInConsole()) {
            return;
        }

        $this->commands([
            EventsConsumeCommand::class,
        ]);

        $this->publishes([
            __DIR__.'/../../config/event-bus.php' => config_path('event-bus.php'),
        ], 'event-bus-config');
    }
}
