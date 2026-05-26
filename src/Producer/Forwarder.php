<?php

namespace Signaladoc\EventBus\Producer;

use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Support\Carbon;
use Psr\Log\LoggerInterface;
use Signaladoc\EventBus\Producer\Contracts\StreamPublisher;
use Throwable;

/**
 * Producer-side outbox forwarder.
 *
 * One processOnce() call = one full cycle:
 *   1. SELECT a batch of pending rows, FOR UPDATE SKIP LOCKED (MySQL/PG)
 *   2. For each row, build the envelope and XADD to Redis
 *   3. On success: mark row published, store stream_message_id
 *      On failure: bump attempts, record last_error; if attempts >= max,
 *                  mark row failed (no further auto-retry)
 *   4. COMMIT
 *
 * The OutboxForwardCommand wraps this in a `while (! shutdown)` loop with
 * sleep(poll_idle_ms) between empty cycles.
 *
 * Crash semantics: if the process dies between XADD and UPDATE, the row
 * STAYS pending (the transaction rolled back) and gets re-published next
 * cycle. Consumers dedupe on event_id (see ARCHITECTURE.md §13.5).
 *
 * Concurrency: multiple Forwarder processes can run safely thanks to
 * FOR UPDATE SKIP LOCKED — each picks a disjoint subset. On SQLite (tests),
 * skip-locked is not supported and we fall back to a plain SELECT.
 *
 * @see microservice/ARCHITECTURE.md §13
 */
final class Forwarder
{
    public function __construct(
        private readonly ConnectionInterface $db,
        private readonly StreamPublisher $publisher,
        private readonly EnvelopeBuilder $envelopeBuilder,
        private readonly LoggerInterface $logger,
        private readonly int $batchSize,
        private readonly int $maxAttempts,
        private readonly int $streamMaxLen,
    ) {}

    /**
     * Run one cycle of the forwarder.
     *
     * @return ForwarderCycleResult Summary of what happened (counts + duration).
     */
    public function processOnce(): ForwarderCycleResult
    {
        $startedAt = microtime(true);
        $published = 0;
        $failedTransient = 0;
        $failedTerminal = 0;

        $this->db->transaction(function () use (&$published, &$failedTransient, &$failedTerminal) {
            $rows = $this->lockBatch();

            foreach ($rows as $row) {
                $outcome = $this->publishRow($row);

                match ($outcome) {
                    PublishOutcome::Published => $published++,
                    PublishOutcome::FailedTransient => $failedTransient++,
                    PublishOutcome::FailedTerminal => $failedTerminal++,
                };
            }
        });

        return new ForwarderCycleResult(
            published: $published,
            failedTransient: $failedTransient,
            failedTerminal: $failedTerminal,
            elapsedMs: (int) round((microtime(true) - $startedAt) * 1000),
        );
    }

    /**
     * @return Collection<int, OutboxEvent>
     */
    private function lockBatch(): Collection
    {
        $query = OutboxEvent::query()
            ->where('status', OutboxEvent::STATUS_PENDING)
            ->where('available_at', '<=', Carbon::now())
            ->orderBy('id')
            ->limit($this->batchSize);

        // MySQL / Postgres support SKIP LOCKED. SQLite (tests) doesn't —
        // and doesn't need it because it's single-writer.
        $driver = $this->db->getDriverName();
        if (in_array($driver, ['mysql', 'mariadb', 'pgsql'], true)) {
            $query->lock('FOR UPDATE SKIP LOCKED');
        }

        return $query->get();
    }

    private function publishRow(OutboxEvent $row): PublishOutcome
    {
        $envelope = $this->envelopeBuilder->build($row);
        $json = json_encode($envelope, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);

        try {
            $messageId = $this->publisher->publish($row->stream_name, $json, $this->streamMaxLen);

            $row->update([
                'status' => OutboxEvent::STATUS_PUBLISHED,
                'published_at' => Carbon::now(),
                'stream_message_id' => $messageId,
                'last_error' => null,
            ]);

            return PublishOutcome::Published;
        } catch (Throwable $e) {
            $nextAttempts = $row->attempts + 1;
            $hitMaxAttempts = $nextAttempts >= $this->maxAttempts;

            $row->update([
                'attempts' => $nextAttempts,
                'last_error' => mb_substr($e->getMessage(), 0, 65_535),
                'status' => $hitMaxAttempts ? OutboxEvent::STATUS_FAILED : OutboxEvent::STATUS_PENDING,
            ]);

            $this->logger->warning('outbox.forwarder.publish_failed', [
                'event_id' => $row->event_id,
                'event_type' => $row->event_type,
                'stream' => $row->stream_name,
                'attempts' => $nextAttempts,
                'max_attempts' => $this->maxAttempts,
                'terminal' => $hitMaxAttempts,
                'error' => $e->getMessage(),
            ]);

            return $hitMaxAttempts ? PublishOutcome::FailedTerminal : PublishOutcome::FailedTransient;
        }
    }
}
