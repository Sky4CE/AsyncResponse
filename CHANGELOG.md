# Changelog

Notable changes to AsyncResponse are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**[GitHub Releases](https://github.com/Sky4CE/AsyncResponse/releases) are the canonical
release-notes location** — each release carries the full notes for its version. This file tracks
work that has landed on `main` but not yet shipped. Security reporters credited under the
[security policy](SECURITY.md) are named in the GitHub Release notes for the fixed version.

## [Unreleased]

### Added

- `HostShutdownTimeout` on the NATS, PostgreSQL, SQL Server, and MongoDB transports, matching the
  broker transports that already had it, so early-ACK drain budgets can be validated against host
  shutdown on every transport.
- Lock/visibility renewal: the Azure Service Bus subscriber renews the peek-lock of unsettled
  batch messages (`LockRenewalInterval`, default 30 s), SQS gains an opt-in visibility heartbeat
  (`VisibilityRenewalInterval`), and the PostgreSQL, SQL Server, and MongoDB transports renew a
  claimed row's lease automatically (fenced by `lock_id`) while its handler runs.
- `MaxStateBytes` flow-store option — an explicit cap on persisted flow-state size, replacing
  silent provider-specific limits.
- Weekly scheduled CodeQL run alongside the existing per-push analysis.

### Changed

- The SQL Server transport's receive spans are renamed to `asyncresponse.sqlserver.receive`
  (previously `asyncresponse.worker.receive` / `asyncresponse.response.receive`), matching every
  other transport's naming; the role still travels as a span tag.
- NuGet packages now ship a dedicated package README instead of the repository README.

### Fixed

- Durable-flow wake delivery is retried through a crashed executor's lease window, fixing flows
  that could stay `Running` forever when their only wake arrived while the dead holder's lease was
  still unexpired and was silently dropped.
- Durable channels rethrow on waiter-registration failure instead of continuing, so a
  subscribe-before-send race cannot silently arm a waiter with no recovery state.
- SQS early-ACK dispatch gates receive-loop saturation, preventing a redrive burn where messages
  were received, released, and re-received in a tight loop while the background queue was full.
- The Azure Service Bus transport assigns a unique `MessageId` per publish, so broker
  duplicate-detection can no longer drop distinct jobs that shared an id.
- Unified early-ACK hard-stop drain across transports: shutdown drains the bounded background
  queue within its budget, and failures during drain surface through `OnBackgroundFailure`
  instead of disappearing.
- Database-channel subscriber heartbeats upsert their row/document, so a pruned registration is
  resurrected instead of leaving a live waiter invisible to delivery confirmation.
- Durable channels now register the subscription before saving recovery state, and cleanup deletes
  recovery state before tearing down the subscription — closing two race windows where a
  concurrent publisher could consume a registering waiter's recovery state, or resume a wait that
  had already completed. Lost-subscriber dispatch also re-checks for a waiter that went live
  mid-dispatch (responses and exceptions alike) instead of consuming its registration.
- Early-ACK backpressure on Azure Service Bus, Google Pub/Sub, and NATS pauses receiving while the
  background queue is full instead of abandoning/NACKing, so saturation no longer burns broker
  delivery attempts or a subscription `DeadLetterPolicy`.
- The PostgreSQL, SQL Server, MySQL, Oracle, and MongoDB flow stores compute lease and expiry math
  on the database server's clock, removing client clock-skew from lease takeover.
- A shared raw-JSON response could be deserialized concurrently by racing dispatches of duplicate
  deliveries, corrupting its memoization; the cache is now synchronized.
- MongoDB transport publishes stamped `available_at` with the client clock while the claim filter
  compares it against the server clock (`$$NOW`), so a client running ahead of the server briefly
  hid fresh messages from consumers; inserts now mark messages available immediately on arrival,
  matching the SQL transports' server-side default.
- The in-memory transport drains already-accepted jobs on graceful shutdown instead of dropping
  them, so a clean restart cannot strand a `Running` durable flow.
- Oversized flow state now fails with a diagnosable error naming the flow, the size, and the
  limit, instead of a provider-specific write failure.
