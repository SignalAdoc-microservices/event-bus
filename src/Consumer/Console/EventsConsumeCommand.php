<?php

namespace Signaladoc\EventBus\Consumer\Console;

use Illuminate\Console\Command;
use Psr\Log\LoggerInterface;
use Signaladoc\EventBus\Consumer\Contracts\StreamReader;
use Signaladoc\EventBus\Consumer\DispatcherFactory;
use Signaladoc\EventBus\Consumer\EventSourceRegistry;
use Signaladoc\EventBus\Consumer\StreamWorker;

/**
 * Long-running consumer worker for ONE consumer group + N source streams.
 *
 * Run under supervisor / systemd / k8s Deployment, one process per group:
 *
 *   # Streak engine consumes billing.cycle + billing.subscription.
 *   php artisan events:consume insurance.streak-engine
 *
 *   # Subset of streams (testing / partial rollout).
 *   php artisan events:consume insurance.streak-engine --stream=billing.cycle
 *
 *   # One cycle then exit (cron-style or smoke test).
 *   php artisan events:consume insurance.streak-engine --once
 *
 *   # Override consumer name (otherwise host+pid).
 *   php artisan events:consume insurance.streak-engine --consumer=worker-a
 *
 * Group → streams + receipts model mapping comes from EventSourceRegistry
 * (populated by the consuming app's source service provider). This command
 * knows nothing about specific producers — to add one, register an
 * EventSource in the provider.
 *
 * Graceful shutdown: handles SIGTERM / SIGINT / SIGHUP via pcntl so deploys
 * can stop the process cleanly BETWEEN messages — never mid-transaction.
 *
 * @see microservice/ARCHITECTURE.md §13 (Inter-service events)
 */
class EventsConsumeCommand extends Command
{
    protected $signature = 'events:consume
                            {group : The consumer group to run (e.g. insurance.streak-engine)}
                            {--stream=* : Override the source streams for this group}
                            {--consumer= : Override the consumer name (default: hostname+pid)}
                            {--once : Run a single cycle then exit}
                            {--max-cycles=0 : Run at most this many cycles (0 = unlimited)}';

    protected $description = 'Consume events from upstream producers via Redis Streams';

    private bool $shouldStop = false;

    public function handle(
        EventSourceRegistry $sources,
        DispatcherFactory $factory,
        StreamReader $reader,
        LoggerInterface $logger,
    ): int {
        $this->installSignalHandlers();

        $group  = (string) $this->argument('group');
        $source = $sources->forGroup($group);

        if ($source === null) {
            $this->error(sprintf(
                "Unknown consumer group '%s'. Known groups: %s",
                $group,
                implode(', ', $sources->knownGroups()) ?: '(none registered)',
            ));

            return self::FAILURE;
        }

        $streams = $this->resolveStreams($source->streamsFor($group));
        if ($streams === []) {
            $this->error("No streams resolved for group '{$group}' under source '{$source->name}'.");

            return self::FAILURE;
        }

        $consumer  = (string) ($this->option('consumer') ?: $this->autoConsumerName());
        $idleMs    = (int) config('event-bus.consumer.worker.poll_idle_ms', 1_000);
        $once      = (bool) $this->option('once');
        $maxCycles = (int) $this->option('max-cycles');

        $worker = new StreamWorker(
            reader: $reader,
            dispatcher: $factory->make($source),
            logger: $logger,
            consumerGroup: $group,
            consumerName: $consumer,
            streams: $streams,
            batchSize: (int) config('event-bus.consumer.worker.batch_size', 32),
            blockMs: (int) config('event-bus.consumer.worker.block_ms', 5_000),
            reclaimMinIdleMs: (int) config('event-bus.consumer.reclaim.min_idle_ms', 60_000),
            reclaimBatchSize: (int) config('event-bus.consumer.reclaim.batch_size', 64),
            reclaimEveryNIdleCycles: (int) config('event-bus.consumer.reclaim.every_n_idle_cycles', 10),
        );

        $worker->bootstrap();

        $this->line(json_encode([
            'event'    => 'consumer.started',
            'group'    => $group,
            'source'   => $source->name,
            'consumer' => $consumer,
            'streams'  => $streams,
        ], JSON_UNESCAPED_SLASHES));

        $cycle = 0;

        while (! $this->shouldStop) {
            $cycle++;

            $result = $worker->runOnce();

            $this->line(json_encode([
                'event'              => 'consumer.cycle',
                'cycle'              => $cycle,
                'group'              => $group,
                'source'             => $source->name,
                'read'               => $result->read,
                'processed'          => $result->processed,
                'skipped'            => $result->skipped,
                'duplicates'         => $result->duplicates,
                'transient_failures' => $result->transientFailures,
                'dead_lettered'      => $result->deadLettered,
                'reclaimed'          => $result->reclaimed,
                'idle'               => $result->isIdle(),
            ], JSON_UNESCAPED_SLASHES));

            if ($once || ($maxCycles > 0 && $cycle >= $maxCycles) || $this->shouldStop) {
                break;
            }

            // Only sleep on idle. Active cycles re-poll immediately so we
            // don't introduce artificial latency during a burst.
            if ($result->isIdle()) {
                $this->interruptibleSleepMs($idleMs);
            }
        }

        $this->line(json_encode([
            'event'  => 'consumer.stopped',
            'group'  => $group,
            'source' => $source->name,
            'cycles' => $cycle,
        ], JSON_UNESCAPED_SLASHES));

        return self::SUCCESS;
    }

    /**
     * --stream CLI overrides win over the EventSource default. Used for
     * partial rollouts where we want to consume one stream of a multi-stream
     * source in isolation.
     *
     * @param  list<string>  $defaults
     * @return list<string>
     */
    private function resolveStreams(array $defaults): array
    {
        $overrides = (array) $this->option('stream');
        if ($overrides !== []) {
            return array_values(array_map('strval', $overrides));
        }

        return $defaults;
    }

    private function autoConsumerName(): string
    {
        $host = gethostname() ?: 'unknown';

        return sprintf('%s-%d', $host, getmypid() ?: 0);
    }

    private function installSignalHandlers(): void
    {
        if (! function_exists('pcntl_signal')) {
            return;
        }

        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, fn () => $this->shouldStop = true);
        pcntl_signal(SIGINT, fn () => $this->shouldStop = true);
        pcntl_signal(SIGHUP, fn () => $this->shouldStop = true);
    }

    private function interruptibleSleepMs(int $ms): void
    {
        $tick      = 100;
        $remaining = $ms;

        while ($remaining > 0 && ! $this->shouldStop) {
            usleep(min($tick, $remaining) * 1000);
            $remaining -= $tick;
        }
    }
}
