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

- **Durable timers inside flows** — `IDurableFlowContext.DelayAsync(name, delay)` and
  `DelayUntilAsync(name, instant)`: checkpointed sleeps whose due time anchors at first
  execution, so crashes/redeploys resume the remainder. On transports with native delayed
  delivery a sleeping run *suspends* (the child-flow mechanism) and is woken by a delayed
  worker job — no worker, execution lease, or memory held while sleeping; the ledger TTL is
  extended to cover the sleep. Sub-`TimerInProcessThreshold` (new `DurableFlowOptions` knob,
  default 10 s) remainders — and all timers on non-delayed transports — wait in process under
  the lease, the same footprint as an awaited step. See
  [docs/timers-and-scheduling.md](docs/timers-and-scheduling.md).
- **Delayed worker jobs** — `EnqueueWorkerAsync(..., TimeSpan delay)` overloads on the fluent
  builder, backed by the new optional `IDelayedWorkerTransport` capability. Native
  implementations: in-memory (TimeProvider timer wheel), Azure Service Bus (scheduled
  messages), SQS (`DelaySeconds`; 15-minute hops, standard queues only), PostgreSQL /
  SQL Server / MongoDB (`available_at` on the queue with database-clock arithmetic).
  `WorkerJobEnvelope` gains additive `NotBeforeUtc` (due-time stamp) and `LastRedelayRemaining`
  (re-publish progress baseline) properties; the shared worker-job executor re-publishes early
  deliveries for the remainder, chunking longer-than-cap delays uniformly, and executes instead
  of re-publishing when consecutive hops stop shrinking the remainder (persistent clock skew
  between the stamping and delivery-gating clocks would otherwise loop forever with fresh
  message ids that never dead-letter). Non-capable transports reject delayed enqueue with
  guidance at the call site; a transport whose configuration cannot honor the capability
  advertises `MaxPublishDelay` = zero and counts as non-capable (an SQS FIFO worker queue), so
  flow timers fall back in process rather than suspending toward a publish that would throw.
- **Cron-scheduled flows** — `WithScheduledFlow<TFlow, TInput>(name, cron, inputFactory,
  options)` starts a flow per occurrence with a deterministic run id
  (`sched:{name}:{occurrenceUtc}`), deduplicated across replicas by the flow store's atomic
  create — no leader election. Public `CronSchedule` parser (five-field Vixie semantics:
  lists/ranges/steps/names, the dom/dow rule with Vixie's star flags — OR only when both day
  fields are explicitly restricted, AND when either is star-shaped like `*/2` — and optional
  `TimeZoneInfo` with honest DST behavior), validated at registration (duplicate schedule
  names too). Occurrences missed while
  no replica ran are skipped by policy.
- **`AsyncResponse.Testing` package** — deterministic testing kit
  ([docs/testing.md](docs/testing.md)): `VirtualTimeProvider` (stepwise, due-order virtual
  clock), `AsyncResponseTestHarness` (full in-memory engine + hosted services on virtual time,
  `AdvanceAsync`, `SimulateRestartAsync` preserving recovery registrations / ledgers / scheduled
  jobs — with a `whileDown` hook for outage simulation), `FlowTestHarness` (script replies to
  awaited steps, observe timers/steps, `StepExecutions` exactly-once assertions,
  `CrashBeforeStep`/`CrashAfterStep` one-shot `SimulatedCrashException` injection) — all with
  zero instrumentation in the flow classes under test.
- **`IDurableFlowExecutionObserver`** — public execution-observation seam (step
  starting/waiting/completed, run finished) invoked on the executor path; the Testing harness is
  built on it and it doubles as a lightweight production telemetry hook. Observer exceptions
  fail the current execution attempt like a step failure (that contract is the crash-injection
  mechanism).
- **Engine-wide `TimeProvider` seam** — `AddAsyncResponse()` registers `TimeProvider.System`
  (TryAdd; a host or the test harness can pre-register its own), and every time-driven Core
  component resolves it: waiter timeouts, execution leases and their renewal, the recovery
  watchdog, in-memory retry backoff, timers, and schedules.

### Changed

- **The in-memory channel now implements the full recoverable contract**
  (`IRecoverableAsyncResponseSubscriber` + `IRecoverableAsyncResponseBuilder`, with the same
  `OnRecovery()`-override guard as the durable channels). Durable flows on the in-memory channel
  now register their lost-subscriber callbacks, and `IAsyncResponseBuilder` resolves to the
  recoverable builder — what passes in tests passes on Redis. Recovery spans waiter loss within
  one process lifetime (and the Testing harness's simulated restarts); a real process exit still
  loses the process-local store.
  > **Breaking for in-memory durable flows.** Every payload a flow awaits must now override
  > `IAsyncResponsePayload.OnRecovery()` on the in-memory channel too — previously only durable
  > channels enforced it, so a flow that ran in-memory could fail at its first awaited step after
  > moving to Redis. Waiter creation now fails fast with the payload type and the required
  > override named. Typical mapping: terminal success → `Resume`, terminal failure → `Fail`,
  > progress/checkpoint payloads → `KeepWaiting` (see [recovery.md](docs/recovery.md)).
- `FlowStepState` gains an additive `WakeAtUtc` property (the timer breadcrumb);
  `FlowStateSchema`/`RecoveryStateSchema`/`WorkerJobEnvelopeSchema` stay at version 1.
- The flow executor's host-lifetime hookup is now actually wired through DI (the lease-poll
  abandon-on-shutdown path previously never saw `IHostApplicationLifetime`).

- **Provider cross-product test coverage.** Channels, transports, and durable-flow stores are
  selected independently, so the suite now enumerates the whole product rather than testing each
  provider alone: **6 channels × 11 transports × 10 stores = 660 combinations**, three scenarios
  each (a durable flow end to end, a terminal domain failure, a worker job with restored context).
  A cell builds a real DI provider in the test process against real servers; the cells are sharded
  into nine CI legs by container footprint, because the whole fleet at once is ~9 GiB and Oracle and
  Cosmos cannot share a runner. `MatrixCompletenessTests` reflects over the shipped `With…Channel` /
  `With…Transport` / `With…DurableFlows` registrations and fails when a provider has no matrix axis
  member, so a new package cannot ship without cross-product coverage. Reproduce one combination
  with `ASYNCRESPONSE_MATRIX_FILTER=PostgreSql+Kafka+MongoDb`. See
  [operations](docs/operations.md#the-provider-cross-product).
- **`TransportConformanceSuite`** — the worker-transport behavioral contract, the counterpart to the
  channel contract that already existed. Ten `Contract_*` facts run against all eleven transports:
  exactly-once delivery of a successful job, ambient-context restoration, responses published from a
  worker, concurrency, large payloads, redelivery after a transient failure, poison-message bounds,
  early-ACK execution, durability across a consumer outage, and idle-shutdown latency. Previously
  each transport had its own ad-hoc list and none of them covered dead-lettering, redelivery,
  drain, or payload limits. `TransportCapabilities` records where the guarantees come from rather
  than letting the difference become an untested gap: every transport bounds redelivery, but through
  a subscriber knob on six, the in-process retry budget on the in-memory queue, the queue's redrive
  policy on SQS, and the subscription's `DeadLetterPolicy` on Google Pub/Sub. Two constrain the bound
  itself — RabbitMQ cannot count past two without an application-owned TTL-retry cycle, and a Pub/Sub
  `DeadLetterPolicy` rejects anything under five — and payload ceilings differ by two orders of
  magnitude, so the payload fact is sized per transport. Where a capability is genuinely absent (the
  in-memory queue has no early-ACK mode and no life beyond its host) the contract asserts the absence
  instead of skipping, so adding one later fails the test.
- **Expanded durable-flow store contract.** Every store now additionally proves lease expiry and
  steal (the crash-recovery path: a worker that dies never releases its lease), renewal and release
  by a non-owner, the full unknown-flow-id surface, a ~64 KiB state round trip, and rejection of a
  state written by a newer schema version.

- `RecoveryAction` and `IAsyncResponsePayload.OnRecovery()` — the tri-state lost-subscriber
  classification that **replaces** the binary `ShouldResumeOnRecovery()`: `Resume`, `Fail`, or
  `KeepWaiting` (default `Fail` — nothing resumes by omission). `KeepWaiting` is the recovery-side
  mirror of an `Until` predicate skipping a progress message: a **non-terminal checkpoint**
  arriving with no live waiter invokes no callback and **retains the recovery registration**, so
  the terminal response that follows still routes. Previously a checkpoint had to classify as
  resume (spawning one resumed worker per checkpoint and consuming the registration out from under
  the terminal response, which was then dropped) or fail (failing a flow that was still running).
  A bool cannot carry three outcomes, and the library deliberately supports response streams with
  progress messages before the terminal result — so the payload contract is now the one method that
  can. Migration: `ShouldResumeOnRecovery() => X` becomes
  `OnRecovery() => X ? RecoveryAction.Resume : RecoveryAction.Fail`, plus `KeepWaiting` for
  checkpoint states. The durable-channel fail-fast guard is
  `AsyncResponsePayloadReflection.OverridesOnRecovery`. The lost-subscriber route tag/metric gains
  a `keep_waiting` value.

- Startup validation vetoes early ACK (`AckAfterEnqueue`) on the worker subscriber: durable-flow
  wake-ups ride the worker queue and rely on broker redelivery for crash recovery, so a crash
  after an early ACK stranded the run as `Running` with no lease, no queued job, and no discovery
  API. `DurableFlowOptions.AllowEarlyAckWorkerSubscriber = true` explicitly accepts the risk;
  early ACK on the response subscriber logs a startup warning instead (at-most-once response
  delivery: the waiter times out and a durable flow restarts the step, re-sending its idempotent
  trigger). **This changes startup behavior** for configurations written against earlier
  pre-release commits — migration is one line: drop `UseAckAfterEnqueue` from the worker
  subscriber (the safe default), or set `AllowEarlyAckWorkerSubscriber = true` on the flow-store
  registration.
- Startup schema validation for manually managed database schemas: with
  `AutoCreateSchema = false`, the PostgreSQL and SQL Server channels verify the 1.0 ack-sequence
  objects once at first use and fail with an error carrying the exact migration SQL, instead of a
  raw "column does not exist" mid-operation. `docs/postgresql.md` and `docs/sqlserver.md` gain
  "Upgrading a manually managed schema" recipes (the integration suite executes the SQL Server
  recipe straight out of the error message).
- `asyncresponse.ingress.unroutable_responses` counter (plus an Error-level log): inbound
  responses with no correlation id are acknowledged by design — redelivery could never route
  them — and each occurrence is now loud instead of a Warning-level whisper.
- Channel-conformance facts for the waiter-disposal contract (see Fixed): disposing before any
  terminal signal cancels the public `ResponseTask`; disposing during an in-flight delivery
  drains it and settles as delivered; a drain outliving `DisposalDrainTimeout` faults as
  indeterminate.
- `AsyncResponseChannelOptions.DisposalDrainTimeout` (default 30 s) — the bound on how long
  waiter disposal drains an in-flight delivery — and `AsyncResponseIndeterminateDeliveryException`,
  the explicit contract for a delivery abandoned mid-flight whose outcome is unknowable.
- `FlowRunStatus.Suspended` — operator parking for durable flow runs. A suspended run ignores
  wake-ups, resumes, and failure signals (a parent awaiting a suspended child keeps waiting), so
  a dead-lettered `Running` run can be taken under manual control without a late response
  resurrecting it; a recovered terminal response is checkpointed WITHOUT waking the run (see
  Fixed). Set the status back to `Running` and call `ResumeAsync` to replay.
- `AsyncResponseCallbackAllowList.AllowDurableFlowExecutor` (default `true`): the allowlist
  authorizer now covers `IDurableFlowExecutor` explicitly instead of the reflection layer exempting
  it from authorization. Custom `IAsyncResponseCallbackAuthorizer` implementations must allow
  `IDurableFlowExecutor` when durable flows are enabled.
- Channel-contract conformance suite: one shared behavioral suite (live delivery, `Until`
  predicates, remote exceptions, timeouts, correlation isolation, lost-subscriber resume/failure
  routing, raw-JSON ingress, correlation-id reuse, subscriber counts, late-response recovery) runs
  against the in-memory reference in unit tests and against real Redis, NATS, PostgreSQL,
  SQL Server, and MongoDB in the integration suite — the in-memory channel's behavior is now an
  enforced contract rather than a de-facto spec. Multi-response streams — the library's core
  scenario — are pinned end to end: a live waiter riding out a whole checkpoint stream, the
  raw-ingress lost path (materialized payloads, checkpoint retention, the full incident replay),
  the crash-mid-stream interleaving (live checkpoint → crash → lost checkpoint → lost terminal),
  and stragglers (duplicate terminals, late checkpoints) after completion on either path, with
  correlation-id reuse intact.
- `docs/transport-semantics.md`: a per-transport semantics matrix — ack modes, delivery-attempt
  counting, dead-letter destinations, early-ACK failure handling, shutdown-drain budgets, and
  lock/lease renewal — replacing prose scattered across `configuration.md`.
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

- `DurableFlowOptions.StateExpiry` default raised from 7 to 14 days — deliberately double the
  7-day default step-timeout chain (`DefaultStepTimeout` → channel `DefaultTimeout` →
  `RecoveryStateExpiry`), so a step that silently waits out the full default timeout faults and
  checkpoints before its ledger can expire instead of racing it.
- Callback authorization now runs **before** type resolution: an unauthorized
  service/method name is rejected string-first, without spending a full assembly scan on
  attacker-supplied input. Unresolvable type names are additionally negative-cached (bounded;
  entries are generation-stamped and self-invalidate when an assembly loads or a custom resolver
  registers, so an in-flight miss racing a registration can never poison the cache), and a
  poisoned recovery row no longer re-walks every loaded assembly on every delivery.
- The recovery watchdog's scan now feeds the same pure `AsyncResponseWatchdogReport.Evaluate`
  classifier it exposes publicly (the two had drifted into duplicate logic), and duplicate
  registrations for one correlation id keep the **oldest** — the scanner contract promises no
  ordering, so a young sibling yielded first can no longer mask an older stale one.
- Rejected durable-flow checkpoints are diagnosed before being reported: a ledger revision
  advanced by a concurrent lease-bypassing writer (`RecoverAsync`, `FailAsync`, operator parking)
  is reported as a concurrent write instead of a phantom "lost its execution lease", and the
  failure the checkpoint was recording travels as the inner exception instead of being discarded.
- Lost-subscriber **failure** callbacks are retried in-process (bounded, jittered — same policy as
  ingress) before the deliberate swallow, so a transient dependency blip no longer silently drops
  the only delivery of a domain-failure signal.
- The PostgreSQL, SQL Server, and MongoDB transport message dispatchers and correlation-id
  extractors now share one source-included implementation (previously three ~280-line and three
  ~106-line near-verbatim copies). Internal only: rendered log text and telemetry are unchanged,
  but structured log **message templates** did change (the provider name and queue-item noun now
  arrive as `{Provider}`/`{Unit}` properties on the same rendered text) — re-pin any
  template-matched alerts or Serilog/Seq groupings.
- Shutdown-budget defaults now fit the .NET host's 30 s `HostOptions.ShutdownTimeout` out of the
  box: `BackgroundDrainTimeout` defaults dropped from 30 s to 20 s on all transports, transport
  `ShutdownTimeout` defaults dropped from 15 s to 5 s where the value is actually consumed (Azure
  Service Bus, RabbitMQ, Google Pub/Sub, PostgreSQL, MongoDB), and the property was removed on the
  five transports where nothing consumed it (SQL Server, Kafka, Redis, NATS, SQS). The documented
  early-ACK opt-in (`UseAckAfterEnqueue(workers, capacity)` with stock defaults) previously failed
  the startup budget validation on every transport. The check itself is now one shared
  implementation that sums only the components a transport actually spends during shutdown.
- The early-ACK mode is named `AckAfterEnqueue` on every transport: Azure Service Bus, NATS, and
  PostgreSQL renamed `AckAfterReceive` → `AckAfterEnqueue` (and `UseAckAfterReceive()` →
  `UseAckAfterEnqueue()`) to match the other seven transports — the semantics were already
  identical (acknowledge after acceptance into the bounded in-process background queue).
- Response ingress retries transient infrastructure failures in-process (jittered backoff) before
  finalizing the waiter through `SetException`, and propagates when even that escalation fails so
  the transport's redelivery/dead-letter policy gets to retry the delivery; unparseable messages
  still finalize immediately.
- Retry backoff (subscriber reconnect loops, ingress retries) now applies half-jitter so replicas
  don't reconnect in lockstep waves after a broker blip.
- The PostgreSQL, SQL Server, and MongoDB response channels now share one source-included
  implementation of the provider-agnostic machinery (dispatch, heartbeat, delivery confirmation,
  cleanup, subscriptions) — previously three ~1,150-line near-copies, now thin provider classes
  over a single shared base. Internal only: no public API or behavior change, and per-message
  paths keep direct (non-virtual) store calls.
- The SQL Server transport's receive spans are renamed to `asyncresponse.sqlserver.receive`
  (previously `asyncresponse.worker.receive` / `asyncresponse.response.receive`), matching every
  other transport's naming; the role still travels as a span tag.
- NuGet packages now ship a dedicated package README instead of the repository README.

### Fixed

- **A maximum-duration durable timer can no longer expire its own ledger.** `DelayAsync` /
  `DelayUntilAsync` accepted sleeps up to the 3650-day persistence ceiling itself, while the
  sleeping ledger's TTL (`sleep + StateExpiry`) saturated at that same ceiling — a
  ceiling-length sleep stamped a TTL that expired exactly at the due instant, so the wake-up
  found the flow state gone and the run hung unfinished forever. Sleeps are now capped at
  ceiling − `StateExpiry` (default 14 days → 3636 days), keeping the full idle margin between
  due time and expiry, and the span is validated **before** the due-time arithmetic —
  `TimeSpan.MaxValue` previously overflowed `UtcNow.Add` with a retriable
  `ArgumentOutOfRangeException` that burned the delivery budget; both now fail terminally with
  the budget in the message.
- **Scheduled flows no longer report an occurrence as "ran exactly once" when nothing ran.**
  The scheduler classified ANY `InvalidOperationException` from the start delegate — including
  one thrown by the user's input factory, before any store call — as the benign
  deterministic-id duplicate. The idempotent-start conflict now throws the dedicated
  `DurableFlowIdConflictException` (public, derived from `InvalidOperationException` for
  compatibility), the scheduler keys the duplicate reading off exactly that type, and every
  other exception is logged as the failed start it is.
- **Three cron correctness gaps.** (1) The next-occurrence horizon was 8 years — sparse-but-valid
  schedules (`0 0 29 2 */7`, Feb 29 on a Sunday: gaps up to 40 years around skipped century leap
  days) fired once and then permanently stopped, indistinguishable from "Feb 30". The scan now
  covers 400 years — a full Gregorian cycle (146 097 days, exactly 20 871 weeks), so a miss is a
  completeness *proof* of unsatisfiability, and misses skip by month/day so the far scan stays
  cheap. (2) A spring-forward-gapped wall time mapped through the pre-transition offset to a
  point PAST the jump (02:30 in a 02:00→03:00 gap fired at 03:30, not the documented gap end),
  and that phantom-late instant also broke next-occurrence ordering — a re-query from inside the
  half-open window skipped it entirely. Gapped times now fire at the transition instant itself,
  with all gapped minutes collapsing onto one fire. (3) Step masks were built with `int`
  arithmetic: a step near `int.MaxValue` overflowed `value += step` and the six-bit shift
  masking minted phantom low values (`1/2147483647` gained minute 0). The accumulators are now
  `long`, preserving Vixie's "oversized step → start value only" semantics.
- **Flow-id length is now a portable contract, enforced centrally.** SQL Server, MySQL, Oracle,
  and EF Core declare `flow_id` as a 400-character column while the other stores are unbounded,
  so a long id worked on some providers and failed on others — or worked as a root and started
  failing the day a child (`:{stepName}`) or scheduled (`sched:{name}:{timestamp}`) suffix was
  appended. Every final id is validated at creation against the new
  `DurableFlowOptions.MaxFlowIdLength` (400): over-long root ids are rejected at `StartAsync`,
  an over-long composed child id fails the parent terminally with the budget in the message
  (deterministic on every replay — retrying a store rejection would be waste), and
  `WithScheduledFlow` validates the final occurrence-id length at registration.
- **Lost-subscriber recovery now raises the step-completed observer event.** `RecoverAsync`
  checkpoints the recovered terminal payload, and the replayed execution short-circuits the
  memoized step — so observers (including the Testing probe's step waiters) never saw that
  completion. The executor now notifies `OnStepCompletedAsync` for the settled step before
  waking the run. Best-effort by necessity: once the checkpoint settles the pending correlation
  id is gone, so a redelivered `RecoverAsync` can no longer attribute the completion — unlike
  run-finished, which re-derives from persisted status.
- **Timed-out `FlowTestHarness` waiters no longer accumulate.** A waiter that hit its real-time
  guard stayed registered forever: every future probe event re-evaluated its predicate and its
  completion source pinned the captured closures. Waits now remove their waiter in a race-safe
  `finally`.
- **The database transports' per-delivery dispatch cost dropped ~3.4× (PostgreSQL, SQL Server,
  MongoDB) — without weakening lease protection.** The claim-lease heartbeat introduced with
  the July hardening tore down its renewal machinery with a thrown-and-caught
  `TaskCanceledException` per message — ~6.5 µs and 1.3 KB on a dispatch path that otherwise
  costs well under a microsecond. The beat now observes cancellation exception-free
  (`ConfigureAwaitOptions.SuppressThrowing`). The heartbeat is still armed BEFORE any user code
  runs: a handler can burn its entire lease synchronously (CPU work or blocking I/O before its
  first await), and only an already-armed beat — firing on a timer thread — renews under a
  blocked handler thread. (A briefly considered lazily-armed variant was withdrawn as unsound
  for exactly that case and is pinned against by a fact whose handler blocks its thread until
  it OBSERVES a renewal.) Renewal cadence, fencing, and lease-lost semantics are unchanged.
  Locally measured ~1.0 µs / 888 B per dispatch, versus 3,465 ns / 1,392 B before the fix and
  217 ns / 88 B at the pre-heartbeat baseline — the residual is the price of arming sound lease
  protection per delivery, negligible beside the database claim round-trip itself.
- **The in-memory channel's wire-parity materialization dropped its string detour.** The
  per-waiter materialization introduced with the aliasing fix serialized to a UTF-16 string and
  deserialized back through the reflection conversion path; it now serializes once per publish
  straight to UTF-8 bytes and deserializes each waiter's instance from those bytes with the
  same case-insensitive matching — byte-identical semantics (the conformance wire-parity and
  fan-out facts are the regression guard), fewer allocations, no transcodes. The remaining
  ~200 ns/delivery over the pre-fix baseline is the price of the correctness contract itself:
  every waiter gets its own wire-true instance instead of an alias of the publisher's live
  object. The watchdog/health-check evaluation cost added by the July observability work
  (order-independent dedupe, richer health data) is retained deliberately — those run per scan
  interval and per health probe, not per message.
- **PostgreSQL schema creation now verifies, against the catalog, that every relation it
  ensured actually IS what it intended (channel, transport, and durable-flow stores).**
  Per-component name-plan validation cannot see the OTHER packages sharing a schema: a channel
  table occupying the transport's derived claim-index name (or vice versa) let
  `CREATE ... IF NOT EXISTS` — which matches ANY relation in the shared namespace — silently
  skip the DDL, leaving a missing index or a "table" that was really someone else's index.
  After its DDL, each store now checks `pg_class`/`pg_index`/`pg_attribute`/`pg_sequence` (in
  the same transaction, under the shared advisory DDL lock) that every expected name resolves
  to the expected relation kind **and shape** — tables verified column by column (name, type,
  nullability, and for runtime-relied defaults the exact `pg_get_expr` rendering: a same-named
  default computing something else — `created_at DEFAULT now() + interval '1 year'` — silently
  shifts every timestamp the store compares, and a future `available_at` default would strand
  transport jobs), rejecting extra columns that are NOT NULL without a default (every normal
  insert would fail with 23502), with the expected primary key; indexes verified to sit on
  the expected table as plain, non-unique, non-partial, valid-and-ready btrees over exactly the
  expected key columns in order; the ack sequence verified as the required cross-process
  monotonic clock (`bigint`, `INCREMENT 1`, `CACHE 1` — a larger cache hands sessions private
  blocks and breaks cross-session ordering — `NO CYCLE`, full positive range); everything
  verified permanent (not UNLOGGED/temporary). `CREATE ... IF NOT EXISTS` explicitly guarantees
  none of this about an existing object. A same-KIND collision that breaks the dependent index
  DDL itself (`column ... does not exist`, SQLSTATE 42703) is translated into the same
  actionable collision error instead of a raw column error. Failing is actionable on whichever
  component starts second, regardless of startup order. The verifier is one source-linked file
  compiled into all three packages. Pinned by integration tests running cross-component
  collisions (different-kind and same-kind, both orders), a crafted table that satisfies the
  index DDL but lacks operational columns, the wrong-index-definition case, and the
  integer/descending-sequence cases on a real server.
- **RabbitMQ rejects non-positive `NetworkRecoveryInterval` instead of breaking automatic
  recovery.** The value is copied verbatim into the client's `ConnectionFactory`, whose
  recovery loop uses it directly as a `Task.Delay`: a negative interval faulted — and
  TERMINATED — that loop (the connection never recovered), and zero spun it. The old
  "fall back to 5 seconds" behavior only ever applied to the subscriber's start-retry delay,
  not the client's recovery loop, so the interval is now strictly positive (and
  timer-ceiling-bounded) with the subscriber fallback removed.
- **MongoDB stores validate effective namespaces — including the derived counters collection —
  against the injected database name at construction.** MongoDB limits a namespace
  (`database.collection`) to 255 UTF-8 bytes (235 sharded); only the store knows the actual
  database name, and the derived `{MessageCollection}_counters` namespace is 9 bytes longer
  than anything static validation sees, so a near-limit configuration passed every check and
  failed at the first ack-sequence draw. The channel (all four effective namespaces), transport,
  and durable-flow stores now fail construction with the computed byte count; collection-name
  validation additionally rejects the reserved system namespace appearing anywhere in a dotted
  name, and the durable-flow options gain the same character rules as the channel/transport.
  Ownership-ledger claims are independent of `AutoCreateIndexes`: disabling index DDL no
  longer silently disables cross-component collision protection (the new
  `UseOwnershipLedger` option — default `true` — is the distinct, explicit opt-out for
  least-privilege deployments that cannot write the ledger collection). DI-hosted Mongo
  components additionally claim their effective collections — derived ones
  included — in a container-scoped ownership ledger keyed by cluster + database: a durable-flow
  store configured onto the channel's derived `{MessageCollection}_counters` collection (whose
  TTL index would silently delete the ack-sequence counter) now fails startup in either
  construction order, naming both claimants. Because that in-memory ledger ends at the
  container boundary, every store also claims its collections in a PERSISTED ledger — one
  atomic upsert per collection into the reserved `asyncresponse_ownership` collection at first
  use — so two independent hosts (or a directly constructed store) sharing a database fail the
  same way, in either startup order; restarts re-claim idempotently, deployments that disable
  auto-creation own their provisioning and skip the ledger, and the error text covers removing
  a stale claim after a deliberate reconfiguration. The namespace byte limit is now enforced at
  MongoDB's SHARDED bound (235 bytes, was 255): a 236–255-byte namespace is only valid while
  the collection stays unsharded, and a later shard-enable would strand it.
- **Derived index names now reserve suffix space at the identifier caps, and the full
  object-name plan is validated (PostgreSQL, SQL Server — channels and transports).** The
  generated index names truncated as a whole, so a maximum-length table name derived its own
  name: on PostgreSQL `CREATE INDEX IF NOT EXISTS` matched the table relation and created ZERO
  indexes; on SQL Server both indexes shared one name and only the first was created — silent
  full scans either way. Index names now truncate the table STEM (as the ack-sequence name
  already did), identifier length is enforced at validation (63/128 — PostgreSQL silently
  truncates longer names server-side; SQL Server errors at DDL), and the complete effective name
  plan (tables + derived sequence + derived indexes) is checked for pairwise distinctness, which
  also catches a table occupying a derived name outright and two long tables whose reserved
  stems truncate identically. The MongoDB channel likewise reserves the derived
  `{MessageCollection}_counters` name — a recovery collection occupying it would have let the
  TTL reaper silently delete the ack counter. The same rule is applied to the durable-flow
  stores' derived `{TableName}_expires_idx` (PostgreSQL, SQL Server, MySQL, Oracle): stem
  truncation at each provider's cap, identifier-length validation, and a PostgreSQL
  relation-namespace collision guard. Boundary integration tests now assert the expected
  catalog indexes on real servers.
- **The timer-ceiling/persistence-bound classification now covers every timeout knob in every
  package** — completing the "passes validation, throws (or hangs) mid-operation" family at both
  ends. Timer-armed knobs (retry delays, poll/drain/shutdown timeouts, lock and visibility
  renewal intervals, the watchdog interval and startup delay, the NATS channel's confirmation
  and probe timeouts that feed `NatsSubOpts.Timeout`) are bounded by the ~49.7-day .NET timer
  ceiling; values that become persisted or server-side "now + value" stamps (DB `LockTimeout`
  renewal aside, redelivery-delay stamps, dead-letter retention, NATS `AckWait`/NAK delays,
  Redis XAUTOCLAIM min-idle) carry the 3650-day persistence bound instead — and deliberately
  accept beyond-timer-ceiling values. Client-specific domains are tighter still and enforced
  with their own reasons: the Kafka client passes timeouts as 32-bit milliseconds
  (`OperationTimeout`, `PollTimeout` ≤ ~24.8 days) and librdkafka caps
  `auto.commit.interval.ms` at one day; AMQP heartbeats are 16-bit seconds
  (`RequestedHeartbeat` ≤ 65535 s, zero = disabled). Google Pub/Sub's retry delays and shutdown
  timeout previously had NO validation at all. Zero remains valid where it means "skip the
  wait" (`Watchdog.StartupDelay`).
- **Durable-flow option bounds re-classified to their actual sinks — a valid 60-day lease or
  progress throttle no longer fails startup.** The previous release's upper bounds over-reached:
  `ExecutionLeaseDuration` is a persisted lease deadline (the timers are the renew interval and
  a capped poll) and now carries the persistence bound rather than the timer ceiling, the
  explicitly timer-armed `ExecutionLeaseRenewInterval` carries the timer ceiling, and
  `ProgressPersistenceInterval` — only ever compared against elapsed time — is merely
  non-negative again. The defensive lease-release-on-construction-failure path (unreachable
  after validation, and mislabeled its own failure) is removed; lease expiry remains the
  backstop.
- **A terminal response racing the waiter's registration can no longer orphan a callback-armed
  recovery registration (Redis, NATS).** Both channels go live before the recovery state is
  saved; a response completing the waiter in that window ran cleanup whose delete preceded the
  save — the save then committed a registration nothing would ever delete, and any later publish
  on that correlation id resurrected the resume/failure callback for a completed wait. Both
  channels now compensate after the save when cleanup already started (the in-memory channel's
  existing post-save check, ported). On Redis, a message pumped INSIDE SubscribeAsync could
  additionally complete cleanup before the subscription handle existed, skipping the unsubscribe
  — a zombie server-side subscription that made every probe/publish count a live waiter and
  suppressed recovery for that correlation id until process exit; the creator now unsubscribes
  post-assignment when cleanup already ran.
- **The database channels' same-tick delivery-vs-history tie is now arbitrated by a monotonic
  ack sequence — exact except for a narrow conservative residual.** A
  message acked in the same server-clock tick a waiter registered in was indistinguishable
  between "history a reused correlation id must not replay" and "a cross-process fan-out
  delivery this waiter is part of"; the watermark resolved it in the safe at-most-once
  direction, costing the fan-out waiter its response (a stall to its step timeout). Delivery
  claims now stamp `acked_seq` from a store-side monotonic sequence (PostgreSQL/SQL Server: a
  `SEQUENCE`; MongoDB: a counter document) and every waiter registration draws its position from
  the same sequence. Timestamps remain the primary comparison — a sequence value is drawn before
  its claim becomes visible, so it must not outrank truthful unequal timestamps (a claim can
  draw, stall past a registration, and truthfully land in a later tick) — and the sequence
  breaks only the tie, where a claim visible before a same-tick registration necessarily drew
  earlier and so can never replay history. The one conservative residual (a draw stalled from an
  earlier tick into the exact registration tick) resolves as the old rule always did. Additive
  schema — the column/sequence are auto-created when schema creation is enabled, and rows acked
  by an older build fall back to the previous timestamp rule **permanently**: the sequence is
  stamped only when the same claim transitions `acked_at` from null, never back-filled onto a
  legacy-acked row (a fresh stamp against an old `acked_at` would let a tick-tied waiter replay
  its predecessor's response during a rolling upgrade).
- **Maximum-length message-table names no longer collide with their generated ack-sequence
  name.** `{table}_ack_seq` was truncated as a whole, so a 63-character PostgreSQL (or
  128-character SQL Server) table name produced the table's own name: PostgreSQL silently skipped
  sequence creation (tables and sequences share a namespace) and failed at the first `nextval`,
  SQL Server failed at `CREATE SEQUENCE`. Suffix space is now reserved before capping, and the
  PostgreSQL managed-schema validation checks `relkind = 'S'` so a table can never satisfy the
  sequence check. Pinned end-to-end with maximum-length table names on real PostgreSQL and
  SQL Server.
- **Interval and TTL options are bounded by what the runtime can represent.** Timer-armed knobs
  (poll/heartbeat intervals, retry delays, step timeouts, lease durations) are validated against
  the ~49.7-day .NET timer ceiling, and persisted-TTL/deadline knobs (retentions, expiries,
  confirmation timeouts, `DurableFlowOptions.StateExpiry`) against a 10-year stamp-arithmetic
  bound — closing the "passes validation, throws mid-operation" family: a `TimeSpan.MaxValue`
  confirmation timeout overflowed AFTER the publisher's insert (reporting failure for a possibly
  delivered response), and an over-ceiling poll interval killed its background loop. The
  in-memory flow store additionally saturates caller-supplied TTL stamps like every external
  store, and a durable-flow execution lease acquired in the store is released if local lease
  construction fails.
- **A NATS consume-loop failure during waiter registration now throws instead of returning a
  faulted waiter.** The loop's death faults the response task with a TRANSPORT error — nothing
  was delivered and nothing ever will be — but registration treated any settled task as a
  delivered response, returned the waiter, and the builder then fired the remote trigger with no
  live subscription and no recovery state left to route its response. Registration now
  distinguishes loop deaths from delivered failures and throws, so the operation never starts;
  a delivered failure envelope still settles the wait as before. The distinction is carried BY
  the settlement itself (an internal marker on the fault, unwrapped before the public
  `ResponseTask`), not a side-band flag: only a loop fault that actually WINS the settlement
  aborts registration, so a loop dying just after a terminal payload landed can never discard
  the delivered response.
- **In-memory same-type fan-out is now wire-true.** The exact-type fast path handed the
  publisher's live payload instance to every same-type waiter — one shared mutable reference
  across the fan-out, with `[JsonIgnore]` in-process state visible that no broker-backed channel
  can deliver, contradicting the conformance materialization contract. The publish now
  serializes the declared payload once and every waiter (same-type included) materializes its
  own instance, byte-equivalent to what Redis/NATS/database waiters receive; pinned by a
  same-type fan-out conformance fact on all six channel derivations.
- **Completed durable-flow steps no longer retain a stale `PendingPayloadTypeFullName`.** The
  response-winning cancellation settlement cleared only the correlation-id breadcrumb; the
  declared-type breadcrumb is now cleared centrally on every settlement path, per the
  `FlowState` contract.
- **The in-memory worker transport now honors the redelivery contract.** A failing job was
  dropped on its first failure — silently voiding durable-flow crash/contention recovery (the
  revision conflict's designed "abandon and let the delivery retry") on the dev transport. Jobs
  now retry in place with exponential backoff up to
  `InMemoryWorkerTransportOptions.MaxDeliveryAttempts` (default 5, `0` = unlimited;
  `RetryBaseDelay`/`RetryMaxDelay` pace it, validated against the ~49.7-day .NET timer ceiling so
  a delay `Task.Delay` rejects can never silently void the retry contract), then drop loudly: an
  error log plus a `dropped` outcome on `asyncresponse.worker.jobs` (broker transports
  dead-letter at this point instead).
- **A DB subscriber heartbeat can no longer resurrect a just-deleted subscriber row.** A cleanup
  landing between the heartbeat's snapshot and its upsert had its row re-created for up to one
  heartbeat-timeout window, during which every publisher counted a phantom live waiter and
  lost-subscriber recovery was suppressed for that correlation id. The heartbeat round now
  re-checks its snapshot afterwards and issues a compensating delete for any registration
  dropped mid-round — including after a FAILED round: SQL Server commits per-batch and MongoDB
  bulk-writes unordered, so a round that throws may still have landed the resurrecting upsert
  (PostgreSQL, SQL Server, MongoDB).
- **Shared-correlation dispatches whose registrations legitimately take different recovery routes
  now report `route=mixed`** on the lost-subscriber metric and publish span instead of
  `unclassified`, which was indistinguishable from a poisoned payload; each registration's own
  dispatch span carries its true route as before.
- **Unresolvable payload type names no longer re-scan every loaded assembly per redelivery.** The
  recovery classifier now shares the callback resolver's bounded, generation-stamped negative
  cache — invalidated automatically when an assembly loads or a resolver registers, so a
  late-loading plugin is picked up immediately while a poisoned/renamed type name costs a
  dictionary hit (its diagnostic still fires per attempt).
- **The library's type caches no longer pin collectible (plugin) AssemblyLoadContexts.** Resolved
  types, conversion/invocation plans, and `OnRecovery` override detection all skip strong caching
  for types from collectible assemblies, so unloading a plugin context actually reclaims it —
  verified by an unload test exercising every cache. Note the runtime boundary that remains:
  `System.Text.Json` pins any collectible type it serializes via runtime-internal caches, so for
  unloadable plugins keep payload/service contract types in a non-collectible contracts assembly
  (documented in `docs/security.md`).
- **A registration step failing after a response already settled the wait no longer discards the
  response (InMemory, Redis, NATS).** When a delivery completed the waiter while the
  recovery-state save was still in flight and the save (or, on NATS, the post-save server flush
  reading its token from the lifetime source the settled wait's cleanup had disposed) then
  failed, `CreateResponseWaiter` rethrew — throwing away a response the waiter already held, the
  exact loss the library exists to prevent, while the same interleaving with a healthy store
  returns the completed waiter. All three creators now return the completed waiter on a
  post-settlement failure (an unsettled registration failure still throws, unchanged); NATS skips
  the flush outright once cleanup started, and the in-memory channel's post-save compensation
  delete is now best-effort like the broker channels' instead of failing the create.
- **A resumable response with only a failure callback registered now engages it.** A registration
  armed with just `OnLostSubscriberFailure` ("tell me if my flow dies") dropped resumable terminal
  responses with a warning on every redelivery until the registration's TTL; the flow cannot
  proceed without a resume callback, so the failure route now fires with the materialized payload
  — mirroring the unclassifiable route's conservatism. Registrations with neither callback keep
  the warn-and-retain behavior.
- **Recovered step checkpoints serialize as the step's DECLARED response type.** Lost-subscriber
  recovery checkpointed the materialized payload by its runtime type; for `[JsonPolymorphic]`
  contracts the runtime type is the derived payload, whose serialization omits the discriminator —
  breaking every replay against an abstract declared base or silently truncating against a
  concrete one. The awaited step now records its declared response type next to the re-attach
  breadcrumb (`FlowStepState.PendingPayloadTypeFullName`, additive), and recovery serializes the
  checkpoint as that type, falling back to the runtime type for ledgers written before the field
  existed.
- **A re-attaching execution takes a recovery checkpoint that landed between its state load and
  its waiter registration.** Recovery's wake-up delivery finds the re-attached execution's own
  lease alive and acks as a duplicate, so nothing would ever wake the parked wait — it burned the
  full step timeout (and could strand outright once the transport's redelivery window lapsed).
  After registering its waiter, a re-attach now re-reads the persisted checkpoint and
  short-circuits with the recovered result; recovery always checkpoints before enqueueing its
  wake-up, so the re-read is authoritative.
- **A Suspended run checkpoints a recovered terminal response without being woken.** Suspension
  still means operator control — no wake-up fires — but the recovered payload (which exists
  nowhere else once the callback returns) used to be discarded with the registration consumed,
  so an un-suspended run re-attached to a correlation id nothing could answer. `ResumeAsync` now
  continues from the recovered checkpoint.
- **Lost-subscriber recovery callbacks now receive the materialized payload, never raw broker
  JSON.** A response arriving through a broker ingress after the waiter died was classified by
  materializing it as the registered payload type — and the materialized instance was then
  discarded: the callback invocation bound the raw `JsonElement` to the callback's *declared*
  parameter type. An `object`-typed parameter (the natural shape for one callback shared across
  payload types) received a `JsonElement`, so every `payload is …` guard in the consuming flow
  silently failed; an interface-typed parameter could not be invoked at all; a base-class parameter
  silently sliced off derived state. This reproduced a production flow deadlock: duplicate resumed
  workers, an unpersisted terminal success, and a re-attached wait on a consumed correlation id.
  Both resume and failure callbacks now receive the instance the classifier materialized (the
  conservative unclassifiable path still attaches the raw payload). Classification is
  **per-registration**: each recovery registration is classified as the payload type IT registered
  — for typed in-process publishes too (normalized to their declared-type wire representation, see
  the serialization-boundary entry below), so shared-correlation registrations with different
  payload types route independently instead of all inheriting the published instance's verdict.
  Pinned by channel-conformance facts on all six channel derivations plus ingress-level regression
  tests.
- **Lost-subscriber dispatch after a raced live retry re-checks liveness before consuming
  recovery state.** On NATS, Redis, and the in-memory channel, when the first lost dispatch found a
  live subscriber (the miss had raced a fresh registration) and the re-delivery ALSO missed, the
  second dispatch ran without the liveness re-check — a delivery/probe contradiction (subscription
  interest not yet visible server-side, a stale heartbeat, subscriber churn) then consumed a live
  waiter's recovery registration. Every lost dispatch now carries the liveness probe; the retry is
  bounded, and a persistent contradiction leaves all recovery state intact (warning +
  `asyncresponse.recovery.liveness_contradiction` trace tag) instead of consuming it. The database
  channels are unchanged by design: their post-claim dispatch runs only after atomically winning
  the message for recovery, which already excludes live delivery.
- **A post-callback cleanup fault is never reinterpreted as a failed response.** The registration
  delete that follows a successfully invoked recovery callback ran inside the dispatch's
  exception scope: a store fault on that delete rethrew, the ingress retried the whole delivery
  (re-invoking the resume callback each attempt) and then published the CLEANUP exception through
  `SetException` — invoking the failure callback for a flow whose resume had already succeeded.
  The delete is now best-effort bookkeeping: a fault is logged, the registration stays until its
  TTL or the next delivery (at-least-once, watchdog-visible), and the callback's outcome stands.
  Same rule on the exception-dispatch route.
- **Recovery classification cannot diverge across the serialization boundary.** Typed lost
  dispatches are now normalized to their **wire representation** — the payload serialized as the
  publisher's DECLARED `T`, exactly as `AsyncResponseEnvelope<T>` writes it — before
  classification, and the classifier consumes only wire JSON. Neither instance reuse (which let
  `[JsonIgnore]`d in-process state and derived `OnRecovery()` overrides decide routing) nor
  runtime-type serialization (which dropped the `[JsonPolymorphic]` discriminators only the
  declared-type contract emits) can make an in-process publish route differently from the same
  response delivered by a broker. Unserializable payloads (cycles, unregistered AOT metadata)
  stay conservatively unclassifiable and take the failure route with the instance attached.
- **In-memory live fan-out matches broker channels for mixed payload types.** Two waiters with
  different declared payload types sharing one correlation id: broker channels give each waiter
  its own JSON materialization of the envelope, but the in-memory channel cast the publisher's
  CLR instance (`Convert.ChangeType`), faulting the second waiter with an `InvalidCastException`.
  Foreign CLR payloads are now re-materialized from the publisher's **declared-type** wire JSON —
  the exact envelope representation, polymorphic discriminators included (runtime-type
  serialization dropped them, faulting waiters bound to a compatible `[JsonPolymorphic]`
  contract). Pinned by concrete and polymorphic mixed-fan-out conformance facts on all six
  channel derivations. (Same-type deliveries initially kept the live-instance fast path; a later
  entry above makes same-type fan-out wire-true as well.)

- Disposing a waiter before any terminal signal now cancels its public `ResponseTask` on the
  Redis, NATS, PostgreSQL, SQL Server, and MongoDB channels, matching the in-memory reference —
  it used to stay pending forever (the disposal also destroyed the timeout, so nothing could ever
  complete it). On the database channels this also covers channel `DisposeAsync` at host
  shutdown, which runs the same cleanup over every in-flight subscription and used to hang
  still-awaiting `WaitAsync` callers. Disposal first **drains** a delivery already in flight
  (every channel implementation): a response mid-`Until`-predicate has already been claimed from
  the channel, so it settles the task as *delivered* — never a cancellation stealing a consumed
  response — and if the drain cannot PROVE settlement (the `DisposalDrainTimeout` budget lapses
  with the delivery still running, or a broker teardown failure leaves the delivery loop's fate
  unknown), the task faults with `AsyncResponseIndeterminateDeliveryException` instead of
  cancelling, because "canceled" would promise nothing was consumed and invite a durable flow to
  re-attach to a correlation id nothing can answer. Durable flows route that fault to a fresh
  restart of the idempotent step.
- A durable-flow step whose wait is cancelled — the channel disposing its waiter at host
  shutdown, or the execution's own cancellation token — no longer marks the step faulted: the
  remote operation is still in flight, so the persisted breadcrumb survives and the redelivered
  execution **re-attaches to the same correlation id** instead of restarting the step and
  re-sending the remote request on every graceful shutdown. Timeouts and real faults still
  restart the step fresh.
- The database channels' same-process fast path now uses the server-stamped `created_at` returned
  by the insert instead of the app clock; an app clock more than 1 s behind the database used to
  silently disable the fast path on every publish, degrading same-process delivery to sweep
  latency.
- The database channels' dispatch loop no longer accumulates an abandoned channel-read waiter per
  poll interval on idle channels.
- Database channels (PostgreSQL, SQL Server, MongoDB) no longer redeliver an already-delivered
  response to a new waiter that reuses the correlation id within the delivery watermark's 1 s
  clock-skew tolerance — the new waiter used to complete instantly with the previous waiter's
  stale payload. A message acked before a subscription existed is now excluded from its
  deliveries, while cross-process fan-out (waiters registered before the ack) is preserved.
  Found by the new channel-contract conformance suite; in-memory, Redis, and NATS already
  behaved correctly. The watermark's ack comparison is strict (`>`), which is load-bearing: server
  clocks are far coarser than their column precision — SQL Server's `SYSUTCDATETIME()` is
  `datetime2(7)` but advances in ~5 ms ticks, and MongoDB's `$$NOW` is millisecond-resolution — so
  a waiter re-registering within one tick of the previous ack lands on exactly `acked_at`, and a
  non-strict comparison let the stale response through on roughly 1 in 8 reuses against SQL Server.
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
- The Cosmos DB flow store's expired-slot reclaim no longer concedes on a moved ETag — the TTL
  purge itself can bump it mid-reclaim — and backs off briefly between attempts, so a purge race
  heals instead of spuriously reporting the flow id as taken (markedly more likely on the Linux
  emulator, but real on the service too).
- MongoDB transport publishes stamped `available_at` with the client clock while the claim filter
  compares it against the server clock (`$$NOW`), so a client running ahead of the server briefly
  hid fresh messages from consumers; inserts now mark messages available immediately on arrival,
  matching the SQL transports' server-side default.
- The in-memory transport drains already-accepted jobs on graceful shutdown instead of dropping
  them, so a clean restart cannot strand a `Running` durable flow.
- Oversized flow state now fails with a diagnosable error naming the flow, the size, and the
  limit, instead of a provider-specific write failure.
