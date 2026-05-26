<?php

namespace Signaladoc\EventBus\Consumer;

use Psr\Log\LoggerInterface;
use Signaladoc\EventBus\Consumer\Contracts\StreamReader;

/**
 * Builds a Dispatcher pre-wired for a specific EventSource.
 *
 * Why a factory and not just a Dispatcher singleton: each producer has its
 * own receipts table (per ARCHITECTURE.md §13.7), so each Dispatcher
 * instance needs its own EventReceiptStore. The shared dependencies
 * (parser, handler registry, stream reader, logger, retry knobs) come in
 * via the factory constructor; the per-source piece (the store) is
 * constructed on demand from the EventSource passed to ->make().
 *
 * Registered as a singleton by ConsumerServiceProvider. Called by
 * EventsConsumeCommand once per worker boot.
 */
final class DispatcherFactory
{
    public function __construct(
        private readonly EnvelopeParser $parser,
        private readonly HandlerRegistry $registry,
        private readonly StreamReader $reader,
        private readonly LoggerInterface $logger,
        private readonly int $maxAttempts,
        private readonly int $dlqMaxLen,
    ) {}

    public function make(EventSource $source): Dispatcher
    {
        return new Dispatcher(
            parser: $this->parser,
            registry: $this->registry,
            reader: $this->reader,
            receipts: new EloquentEventReceiptStore(
                modelClass: $source->receiptModel,
                aggregateColumnPrefix: $source->aggregateColumnPrefix,
            ),
            logger: $this->logger,
            maxAttempts: $this->maxAttempts,
            dlqMaxLen: $this->dlqMaxLen,
        );
    }
}
