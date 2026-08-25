# Issue #50: Reliable Reply Delivery

Issue #48 provides the tenant-scoped reply_outbox facts and fenced storage
operations. Issue #50 defines the delivery loop that consumes those facts
without coupling queue logic to a Telegram SDK.

## Contract and Boundaries

The delivery path is:

    Runner event -> materialize reply segments -> claim with lease/fence
      -> Provider.Deliver(stable key) -> sent/provider receipt
      -> all segments sent -> message_event replied

Provider is an injectable protocol-neutral interface. Its delivery identity is
(tenant_id, reply_id, segment_index) and implementations must pass the stable
reply_id plus segment index to provider-level idempotency when the provider
supports it. Database fencing protects the commit race; it does not promise
external exactly-once delivery.

The worker owns no Runner, Telegram SDK, request body, secret, or provider raw
error. It receives a tenant-scoped RuntimeStore, a Provider, a context, and
bounded retry/shutdown configuration. A provider may be Telegram, a test fake,
or a future channel implementation.

## Lifecycle

Pending rows are eligible immediately; retryable rows are eligible when the
exponential delay derived from attempts and updated_at is due. A worker
claims one row, increments the attempt and receives a lease/fencing token. Only
that owner and fence can commit sent, retryable, or dead_letter.

An expired sending lease is first reconciled with the provider using the same
stable key. accepted becomes sent, rejected becomes retryable, and unknown or
reconciliation errors remain unresolved and are never automatically
redelivered. This avoids duplicating a provider side effect whose receipt was
lost.

Retryable errors use bounded exponential backoff with jitter and a configurable
maximum attempt count. Permanent errors go directly to dead_letter. Only a
stable error class is stored in last_error_class; raw provider errors never
enter storage, logs, traces, metrics, or client responses.

When a reply contains multiple segments, successful segments remain sent and
only remaining segments are retried. The event advances to replied only after
all segments are sent. Duplicate materialization uses the existing outbox key
and does not create a second row.

## Worker Lifecycle and Telemetry

Run(ctx) stops claiming after cancellation, cancels in-flight provider calls
through the same context, waits for bounded in-flight work, and returns without
leaking goroutines or leases. Close is idempotent. Shutdown is observable
through low-cardinality metrics: claims, send success/failure, retries,
dead-letters, lease recovery, and delivery latency. Traces and logs carry only
request_id, trace_id, component, operation, provider/channel, status, and
stable error class. Tenant/session/user/message bodies, tokens, DSNs, and raw
provider errors are redacted.

## Issue Ledger

- [x] Injectable Provider/Delivery contract and tenant-scoped worker lifecycle.
- [x] Runner reply materialization into idempotent outbox segments.
- [ ] Fenced concurrent claims with one valid winner.
- [ ] Exponential backoff, bounded retries, permanent-error DLQ, and stable
      error classes.
- [ ] Expired-lease reconciliation, restart recovery, and stale-fence rejection.
- [ ] Multi-segment completion and partial-failure recovery without duplicate rows.
- [ ] Cross-tenant claim/read/transition rejection.
- [ ] Context cancellation and graceful shutdown leak tests.
- [ ] Low-cardinality metrics, trace correlation, and secret/message redaction.
- [ ] Telegram provider integration test and opt-in real-provider E2E evidence.
- [ ] InMemory tests, live PostgreSQL/restart tests, race tests, and full CI.
- [ ] Operational documentation for delivery semantics, retry/DLQ, recovery,
      provider limitations, and capacity estimates.

## Acceptance Evidence

The deterministic tests run in every CI build. PostgreSQL restart and real
Telegram E2E require explicitly provisioned test infrastructure and are never
represented as passing when their DSN/token is absent. The PR description keeps
those external prerequisites separate from local evidence.
