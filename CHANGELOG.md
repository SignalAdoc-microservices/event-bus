# Changelog

All notable changes to `signaladoc/event-bus` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial extraction from the in-monorepo `billing-service` (producer) and
  `insurance-service` (consumer) implementations.
- `Signaladoc\EventBus\Producer\ProducerServiceProvider` — transactional outbox
  + Redis Streams forwarder + `outbox:forward` artisan command +
  `outbox_events` migration.
- `Signaladoc\EventBus\Consumer\ConsumerServiceProvider` — Redis Streams
  worker + dispatcher + handler registry + receipt store + `events:consume`
  artisan command.
- Wire envelope conforms to `microservice/ARCHITECTURE.md` §13.2 (locked):
  `aggregate.ref` carries the cross-service ref (`<service>.<table>:<id>`).
- `EnvelopeParser` accepts both `aggregate.ref` (preferred) and
  `aggregate.id` (legacy) for a forward-compatibility window.

### Contract notes
- Wire envelope shape is governed by ARCHITECTURE.md §13.2 — bump
  `event-bus.producer.envelope_schema_version` and coordinate a
  platform-wide migration before changing top-level envelope keys.
- Per-event-type payload evolution uses `data.schema_version` — see §13.13
  for the additive-vs-breaking rule and dual-emit deprecation policy.
