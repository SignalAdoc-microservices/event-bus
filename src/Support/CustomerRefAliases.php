<?php

declare(strict_types=1);

namespace Signaladoc\EventBus\Support;

/**
 * Candidate spellings for a telemedicine customer identity ref.
 *
 * Identity / B2C gateway emit `telemedicine-backend.users:{id}` (canonical
 * per billing SCHEMA invariant #22). Some writers (notably insurance enroll
 * today) still persist `telemedicine.users:{id}`. When scoping B2C `/user`
 * reads from gateway `X-Identity-Source-Ref`, try both forms so either
 * spelling matches.
 *
 * Prefer converging writers on `telemedicine-backend.users:{id}` and then
 * retiring this helper. Until then, this is the single shared bridge —
 * do not re-copy into service `app/Support/`.
 *
 * @see CrossServiceRef
 * @see microservice/ARCHITECTURE.md §12.1
 */
final class CustomerRefAliases
{
    /**
     * @return list<string>
     */
    public static function candidates(?string $sourceRef): array
    {
        if ($sourceRef === null) {
            return [];
        }

        $ref = trim($sourceRef);
        if ($ref === '') {
            return [];
        }

        $candidates = [$ref];

        if (preg_match('/^telemedicine-backend\.users:(.+)$/', $ref, $m) === 1) {
            $candidates[] = 'telemedicine.users:' . $m[1];
        } elseif (preg_match('/^telemedicine\.users:(.+)$/', $ref, $m) === 1) {
            $candidates[] = 'telemedicine-backend.users:' . $m[1];
        }

        return array_values(array_unique($candidates));
    }
}
