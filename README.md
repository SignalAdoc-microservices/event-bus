# signaladoc/event-bus

Transactional outbox **producer** + Redis Streams **consumer** for the Signaladoc microservice platform.

Reference implementation of:

- `microservice/ARCHITECTURE.md` §13 — Inter-service events
- `microservice/ARCHITECTURE.md` §13.13 — Locked event contracts

This package travels with the platform. The shape of the wire envelope, the
outbox row, and the consumer-receipts row are all platform-wide invariants
governed by the architecture doc above — **do not mutate them without an
architecture-change PR**.

---

## What's in the box

| Side | What you get | Service provider |
|---|---|---|
| Producer (anyone who emits events) | `OutboxEmitter`, `OutboxEvent` Eloquent model, `Forwarder` (drainer) loop, `PhpRedisStreamPublisher`, `EnvelopeBuilder`, `outbox:forward` artisan command, `outbox_events` migration | `Signaladoc\EventBus\Producer\ProducerServiceProvider` |
| Consumer (anyone who reads events) | `StreamWorker`, `Dispatcher`, `EnvelopeParser`, `HandlerRegistry`, `EventSourceRegistry`, `PhpRedisStreamReader`, `EloquentEventReceiptStore`, `events:consume` artisan command | `Signaladoc\EventBus\Consumer\ConsumerServiceProvider` |
| Shared | `Envelope` value object, `EventTypeMap` contract, `MalformedEnvelopeException` | n/a (loaded by both) |

Service providers are **NOT** auto-discovered. Add the ones you need to your
app's `bootstrap/providers.php` (Laravel 11+) or `config/app.php`.

A service can be producer-only (billing-service), consumer-only
(insurance-service today), or both — it just registers the right provider.

---

## Installation

### From a path repository (in-monorepo development)

In the consuming app's `composer.json`:

```json
{
    "repositories": [
        { "type": "path", "url": "../packages/event-bus" }
    ],
    "require": {
        "signaladoc/event-bus": "*"
    }
}
```

### From the GitHub VCS repository (production)

```json
{
    "repositories": [
        { "type": "vcs", "url": "https://github.com/signaladoc/event-bus" }
    ],
    "require": {
        "signaladoc/event-bus": "^0.1"
    }
}
```

Then:

```bash
composer update signaladoc/event-bus
php artisan vendor:publish --tag=event-bus-config
php artisan vendor:publish --tag=event-bus-migrations
php artisan migrate
```

---

## Wiring

### Producer-only service (e.g. billing-service)

`bootstrap/providers.php`:

```php
return [
    App\Providers\AppServiceProvider::class,
    Signaladoc\EventBus\Producer\ProducerServiceProvider::class,
    App\Providers\BillingOutboxServiceProvider::class, // binds your concrete EventTypeMap
];
```

You provide a concrete `EventTypeMap` (a final class that lists the event
types your service emits) and bind it in your own service provider:

```php
$this->app->singleton(
    Signaladoc\EventBus\Producer\Contracts\EventTypeMap::class,
    App\Catalogs\EventBusCatalog::class,
);
```

Set `config/event-bus.php` keys (or env):

| Key | Purpose |
|---|---|
| `producer.service` | This service's canonical name (`billing-service`). Stamped into every envelope's `producer.service`. |
| `producer.version` | Release id (git sha or tag). |
| `producer.envelope_schema_version` | Stays at `1` per §13.2 unless a platform-wide envelope migration happens. |
| `producer.forwarder.batch_size` | Rows pulled per cycle. |
| `producer.forwarder.max_attempts` | Before `failed` terminal state. |
| `producer.redis.stream_maxlen` | `MAXLEN ~ N` for `XADD`. |

### Consumer-only service (e.g. insurance-service)

`bootstrap/providers.php`:

```php
return [
    App\Providers\AppServiceProvider::class,
    Signaladoc\EventBus\Consumer\ConsumerServiceProvider::class,
    App\Providers\BillingSourceServiceProvider::class, // registers EventSource + handlers for `billing.*`
];
```

You declare each upstream producer as an `EventSource` (which model you
write receipts into, which streams you consume from, which consumer group
you join). The package's `events:consume <source>` command does the rest.

See `microservice/ARCHITECTURE.md` §13.4 / §13.7 / `EVENT-BUS-WALKTHROUGH.md`
for the per-source pattern.

---

## What this package does NOT do

- It does NOT define your event types. That's your service's concrete
  `EventTypeMap` implementation.
- It does NOT define your handlers. That's your service's per-source
  service provider that calls `HandlerRegistry::register()`.
- It does NOT define your receipts table. Each consuming service has its
  own per-source receipts table (`billing_event_receipts`,
  `insurance_event_receipts`, …) — per ARCHITECTURE.md §13.7.

These boundaries are deliberate — they're what lets `event-bus` stay
service-agnostic.

---

## Tests

```bash
composer install
vendor/bin/phpunit
```

The package's tests cover envelope building/parsing, the forwarder loop
against an in-memory publisher, the dispatcher's idempotency layer, and
the receipt-store contract. Integration with a real Redis is exercised
in each consuming service's own test suite.

---

## Versioning

The package uses **semver** per Composer convention. Breaking changes to
public PHP APIs are tracked in `CHANGELOG.md`. The wire envelope itself
is governed by §13 of `microservice/ARCHITECTURE.md` and evolves on a
slower, platform-coordinated cadence.
