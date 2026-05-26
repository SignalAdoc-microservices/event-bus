<?php

declare(strict_types=1);

namespace Signaladoc\EventBus\Support;

use InvalidArgumentException;

/**
 * Opaque cross-service reference helpers.
 *
 * Format: `<service>.<table>:<id>` — see microservice/ARCHITECTURE.md §12.1.
 *
 * Examples:
 *   billing-service.plans:42
 *   billing-service.subscriptions:1024
 *   insurance-service.insurance_products:7
 *   partner-service.partners:99
 *   telemedicine-backend.users:4471          (legacy/source-system ref on customer_ref)
 *   chat-backend.whats_app_users:abc-123     (legacy/source-system ref on customer_ref)
 *
 * Refs are OPAQUE: callers store, log, and pass them around as strings.
 * Only the owning service should ever parse them, and even then only at the
 * service boundary (controller / handler). Business logic should treat them
 * as opaque tokens.
 *
 * On the owning side, models expose `$model->ref` via {@see TracksAsRef}.
 * On the consuming side, refs are stored verbatim in `*_ref` columns and
 * never re-formatted.
 *
 * The event-bus wire envelope (§13.2) uses this same format for
 * `aggregate.ref` and `partition_key`, which is why this helper ships with
 * the event-bus package — it's the canonical addressing primitive shared
 * by every service that participates in the event bus.
 */
final class CrossServiceRef
{
    /**
     * Format: `<service>.<table>:<id>`. Whole string captured between
     * service.table and id by a single colon. `<service>` may itself contain
     * dots (e.g. `partner-service`, `telemedicine-backend`).
     */
    private const PATTERN = '/^(?P<service>[a-z0-9][a-z0-9\-]*(?:\.[a-z0-9_]+)*)\.(?P<table>[a-z][a-z0-9_]*):(?P<id>[A-Za-z0-9_\-]+)$/';

    /**
     * Build a ref from its parts.
     */
    public static function format(string $service, string $table, int|string $id): string
    {
        if ($service === '' || $table === '' || (string) $id === '') {
            throw new InvalidArgumentException('CrossServiceRef::format requires non-empty service, table, and id.');
        }

        return sprintf('%s.%s:%s', $service, $table, $id);
    }

    /**
     * Parse a ref string into ['service' => …, 'table' => …, 'id' => …].
     *
     * Throws InvalidArgumentException on malformed input — callers should
     * validate at the API boundary and treat as 4xx, not 5xx.
     *
     * @return array{service: string, table: string, id: string}
     */
    public static function parse(string $ref): array
    {
        if (! preg_match(self::PATTERN, $ref, $m)) {
            throw new InvalidArgumentException(sprintf(
                'Malformed cross-service ref %s — expected "<service>.<table>:<id>".',
                json_encode($ref),
            ));
        }

        return [
            'service' => $m['service'],
            'table'   => $m['table'],
            'id'      => $m['id'],
        ];
    }

    /**
     * Type-safe ref → id extraction for refs owned by THIS service.
     *
     * Asserts the ref is for the expected (service, table), then returns the
     * id portion. Use this when you already know you're looking at one of
     * your own refs and want the local PK.
     */
    public static function idFor(string $ref, string $expectedService, string $expectedTable): string
    {
        $parsed = self::parse($ref);

        if ($parsed['service'] !== $expectedService || $parsed['table'] !== $expectedTable) {
            throw new InvalidArgumentException(sprintf(
                'Ref %s belongs to %s.%s, expected %s.%s.',
                json_encode($ref),
                $parsed['service'],
                $parsed['table'],
                $expectedService,
                $expectedTable,
            ));
        }

        return $parsed['id'];
    }

    public static function isValid(string $ref): bool
    {
        return (bool) preg_match(self::PATTERN, $ref);
    }
}
