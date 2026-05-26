<?php

namespace Signaladoc\EventBus\Producer\Console;

use Illuminate\Console\Command;
use Signaladoc\EventBus\Producer\Forwarder;

/**
 * Long-running producer-side outbox forwarder.
 *
 * Run under supervisor / systemd / k8s Deployment:
 *
 *   php artisan outbox:forward            # loop forever
 *   php artisan outbox:forward --once     # one cycle then exit (for cron / debug)
 *   php artisan outbox:forward --max-cycles=100   # bounded loop (deploy churn)
 *
 * Graceful shutdown: handles SIGTERM / SIGINT / SIGHUP (via pcntl) so deploys
 * can stop the process cleanly between cycles instead of mid-transaction.
 *
 * @see microservice/ARCHITECTURE.md §13
 */
class OutboxForwardCommand extends Command
{
    protected $signature = 'outbox:forward
                            {--once : Run a single cycle then exit}
                            {--max-cycles=0 : Run at most this many cycles (0 = unlimited)}';

    protected $description = 'Drain outbox_events to Redis Streams (the producer-side forwarder)';

    private bool $shouldStop = false;

    public function handle(Forwarder $forwarder): int
    {
        $this->installSignalHandlers();

        $once      = (bool) $this->option('once');
        $maxCycles = (int) $this->option('max-cycles');
        $idleMs    = (int) config('event-bus.producer.forwarder.poll_idle_ms', 5000);

        $cycle = 0;

        while (! $this->shouldStop) {
            $cycle++;

            $result = $forwarder->processOnce();

            // Structured log line per cycle — easy to chart in Datadog/ELK.
            $this->line(json_encode([
                'event'            => 'outbox.forwarder.cycle',
                'cycle'            => $cycle,
                'published'        => $result->published,
                'failed_transient' => $result->failedTransient,
                'failed_terminal'  => $result->failedTerminal,
                'elapsed_ms'       => $result->elapsedMs,
                'idle'             => $result->isIdle(),
            ], JSON_UNESCAPED_SLASHES));

            if ($once) {
                break;
            }
            if ($maxCycles > 0 && $cycle >= $maxCycles) {
                break;
            }
            if ($this->shouldStop) {
                break;
            }

            // Sleep ONLY when the last cycle was idle. A non-empty cycle
            // means there may be more pending rows — re-poll immediately.
            if ($result->isIdle()) {
                $this->interruptibleSleepMs($idleMs);
            }
        }

        $this->line(json_encode([
            'event'  => 'outbox.forwarder.stopped',
            'cycles' => $cycle,
        ], JSON_UNESCAPED_SLASHES));

        return self::SUCCESS;
    }

    private function installSignalHandlers(): void
    {
        if (! function_exists('pcntl_signal')) {
            // pcntl not installed (e.g. on Windows or some FPM builds) — skip
            // gracefully; supervisor will SIGKILL eventually if needed.
            return;
        }

        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, fn () => $this->shouldStop = true);
        pcntl_signal(SIGINT, fn () => $this->shouldStop = true);
        pcntl_signal(SIGHUP, fn () => $this->shouldStop = true);
    }

    /**
     * Sleep for $ms milliseconds but wake early on shutdown signal.
     */
    private function interruptibleSleepMs(int $ms): void
    {
        $tick      = 100; // ms; check shutdown flag every 100ms
        $remaining = $ms;

        while ($remaining > 0 && ! $this->shouldStop) {
            usleep(min($tick, $remaining) * 1000);
            $remaining -= $tick;
        }
    }
}
