# Changelog

Notable changes to AsyncResponse are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**[GitHub Releases](https://github.com/Sky4CE/AsyncResponse/releases) are the canonical
release-notes location** — each release carries the full notes for its version. This file tracks
work that has landed on `main` but not yet shipped. Security reporters credited under the
[security policy](SECURITY.md) are named in the GitHub Release notes for the fixed version.

## [Unreleased]

### Fixed

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
  channel derivations. The same-type fast path still hands the live instance through, unchanged.

### Added

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
  wake-ups, recoveries, resumes, and failure signals (a parent awaiting a suspended child keeps
  waiting), so a dead-lettered `Running` run can be taken under manual control without a late
  response resurrecting it. Set the status back to `Running` and call `ResumeAsync` to replay.
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
