<?php

declare(strict_types=1);

namespace Signaladoc\EventBus\Support;

/**
 * Adds a computed `ref` attribute to an Eloquent model so the model knows
 * how to identify itself across service boundaries without storing a
 * duplicated column.
 *
 * Usage (billing-service):
 *
 *   class Plan extends Model
 *   {
 *       use TracksAsRef;
 *
 *       protected static string $refService = 'billing-service';
 *       protected static string $refTable   = 'plans';
 *   }
 *
 *   $plan->ref;  // "billing-service.plans:42"
 *
 * Usage (insurance-service):
 *
 *   class InsuranceProduct extends Model
 *   {
 *       use TracksAsRef;
 *
 *       protected static string $refService = 'insurance-service';
 *       protected static string $refTable   = 'insurance_products';
 *   }
 *
 *   $product->ref;  // "insurance-service.insurance_products:7"
 *
 * The {@see CrossServiceRef} helper builds and parses these strings.
 *
 * Convention:
 *   - Models that ARE the canonical owner of an entity use this trait.
 *     The ref is computed from the local id; nothing is stored.
 *   - Models that REFERENCE another service's entity store the ref string
 *     verbatim in a `*_ref` column and never re-format it.
 *
 * See microservice/ARCHITECTURE.md §12.1.
 */
trait TracksAsRef
{
    public function getRefAttribute(): string
    {
        return CrossServiceRef::format(
            static::$refService,
            static::$refTable,
            $this->getKey(),
        );
    }
}
