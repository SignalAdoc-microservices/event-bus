<?php

namespace Signaladoc\EventBus\Consumer;

use Illuminate\Database\Eloquent\Model;
use InvalidArgumentException;

/**
 * Declarative description of one upstream producer this service consumes from.
 *
 * Per microservice/ARCHITECTURE.md §13.7, every producer→consumer pairing
 * is an explicit contract. This value object IS that contract on the
 * consumer side:
 *
 *  - which Eloquent model holds the receipts,
 *  - which column prefix the receipts use for the producer's aggregate,
 *  - which consumer groups this service runs against the producer,
 *  - which streams each group reads.
 *
 * Adding a new producer = constructing one of these in a service provider
 * and registering it with EventSourceRegistry. The Dispatcher, StreamWorker,
 * EventsConsumeCommand, etc. don't need to change.
 *
 * Example:
 *
 *   new EventSource(
 *       name: 'billing',
 *       receiptModel: \App\Models\BillingEventReceipt::class,
 *       aggregateColumnPrefix: 'billing',
 *       groupStreams: [
 *           'insurance.streak-engine' => ['billing.cycle', 'billing.subscription'],
 *       ],
 *   );
 */
final readonly class EventSource
{
    /**
     * @param  string  $name  Short identifier, matching the producer service domain
     *                        (e.g. 'billing', 'telemedicine'). Used in logs and
     *                        as the registry key.
     * @param  class-string<Model>  $receiptModel  Eloquent model for the
     *                        `<source>_event_receipts` table.
     * @param  string  $aggregateColumnPrefix  e.g. 'billing' → store writes
     *                        `billing_aggregate_type` / `billing_aggregate_id`.
     *                        Usually equals $name but kept separate so a
     *                        producer rename doesn't force a column rename.
     * @param  array<string, list<string>>  $groupStreams  Map of consumer
     *                        group → source streams to XREADGROUP from.
     *                        E.g. ['insurance.streak-engine' => ['billing.cycle']].
     */
    public function __construct(
        public string $name,
        public string $receiptModel,
        public string $aggregateColumnPrefix,
        public array $groupStreams,
    ) {
        if ($name === '') {
            throw new InvalidArgumentException('EventSource name must be non-empty');
        }
        if (! is_subclass_of($receiptModel, Model::class)) {
            throw new InvalidArgumentException(
                "EventSource receiptModel must extend Illuminate\\Database\\Eloquent\\Model; got '{$receiptModel}'",
            );
        }
        if ($groupStreams === []) {
            throw new InvalidArgumentException(
                "EventSource '{$name}' must declare at least one consumer group",
            );
        }
    }

    public function hasGroup(string $group): bool
    {
        return array_key_exists($group, $this->groupStreams);
    }

    /** @return list<string> */
    public function streamsFor(string $group): array
    {
        return $this->groupStreams[$group] ?? [];
    }

    /** @return list<string> */
    public function groupNames(): array
    {
        return array_keys($this->groupStreams);
    }
}
