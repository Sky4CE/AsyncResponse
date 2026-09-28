# Lost-subscriber recovery

[← Back to README](../README.md)

Every wait records *recovery state* for cleanup and watchdog visibility; durable channels (Redis,
NATS, PostgreSQL, SQL Server, MongoDB) persist it beyond the process. When a response arrives after
the waiter died (e.g. a redeploy), it is **classified by its domain outcome** and routed: resume the
flow, fail it — never resume a failure — or, for a non-terminal checkpoint, keep the registration
armed for the terminal response.

Two independent decisions govern a response:

- **`Until` (live)** — an *active* waiter's predicate decides whether this message completes the
  wait. A failed payload is a valid response here.
- **`OnRecovery()` (recovery)** — the payload decides what a late response does to the flow when
  *nobody is listening*: `Resume`, `Fail`, or `KeepWaiting`.

Why they stay separate: [two axes](#why-recovery-routing-and-until-stay-separate).

Inject `IRecoverableAsyncResponseBuilder` to reach the `OnLostSubscriber*` methods (on the builders
its `For<T>()` returns). Every bundled channel registers it, `.WithInMemoryChannel()` included — but
in-memory recovery is process-local: it spans waiter loss within one process lifetime (and the
simulated restarts of [AsyncResponse.Testing](testing.md)), not a process exit. For recovery that
survives a redeploy use a durable channel (`.WithRedisChannel()`, `.WithNatsChannel()`,
`.WithPostgreSqlChannel(...)`, `.WithSqlServerChannel(...)`, `.WithMongoDbChannel(...)`).

**On this page**

- [`OnRecovery()` — classifying recovered responses](#onrecovery--classifying-recovered-responses)
- [Non-terminal checkpoints: `KeepWaiting`](#non-terminal-checkpoints-keepwaiting)
- [Callbacks receive the materialized payload](#callbacks-receive-the-materialized-payload)
- [Registering recovery callbacks](#registering-recovery-callbacks)
- [Why recovery routing and `Until` stay separate](#why-recovery-routing-and-until-stay-separate)
- [The recovery watchdog + health check](#the-recovery-watchdog--health-check)
- [Recovery-state durability](#recovery-state-durability)
- [Wire/schema versioning](#wireschema-versioning)
- [Shared-correlation recovery](#shared-correlation-recovery)

## `OnRecovery()` — classifying recovered responses

Override `OnRecovery()` on **every payload type you use with lost-subscriber recovery
callbacks** — every channel, in-memory included, fails fast at waiter creation without it, so a
payload that passes in tests passes unchanged on Redis. That includes payloads that can never fail
(a success-only notification still needs `=> RecoveryAction.Resume`) and progress-only checkpoints
(`=> RecoveryAction.KeepWaiting`). It is consulted **only** on the recovery path, never for live
completion:

```csharp
public sealed class OrderResult : IAsyncResponsePayload
{
    public OrderStatus Status { get; set; }
    public string? Message { get; set; }

    // Recovery routing only: resume on the terminal success, keep the registration armed on a
    // progress checkpoint, fail otherwise.
    public RecoveryAction OnRecovery() => Status switch
    {
        OrderStatus.Completed => RecoveryAction.Resume,
        OrderStatus.Processing => RecoveryAction.KeepWaiting,
        _ => RecoveryAction.Fail,
    };
}
```

A payload whose response stream is strictly terminal (every message ends the operation) simply never
returns `KeepWaiting` — e.g. `public RecoveryAction OnRecovery() => Status == OrderStatus.Failed ?
RecoveryAction.Fail : RecoveryAction.Resume;`.

The interface default is `Fail`, so a response can never resume the happy path by omission.
`Resume` and `Fail` consume the registration; `KeepWaiting` invokes nothing and keeps it.

## Non-terminal checkpoints: `KeepWaiting`

Some remote systems report progress on the same correlation id before the terminal result — "still
running" heartbeats, staged sub-results. A live waiter simply lets its `Until` predicate observe and
skip them. On the lost-subscriber path the same message needs an explicit third route:
`RecoveryAction.KeepWaiting` invokes **no callback** and **retains the recovery registration**, so
the terminal response that follows still routes.

Both alternatives corrupt the flow:

- `Resume` on a checkpoint spawns one resumed worker *per checkpoint* and consumes the
  registration; the terminal response then has nothing to route against and is dropped, and the
  resumed workers re-attach to a correlation id nothing will answer, hanging to their step timeout.
- `Fail` fails a flow that is still running remotely, and equally consumes the registration out
  from under the real result.

A retained registration stays bounded by `RecoveryStateExpiry` and visible to the
[watchdog](#the-recovery-watchdog--health-check); the lost-subscriber metric and span report route
`keep_waiting` for these dispatches.

## Callbacks receive the materialized payload

A response that arrives through a broker ingress is raw JSON. The recovery path materializes it as
the payload type recorded in the registration *before* classifying it, and the chosen callback
receives **that materialized instance** — regardless of the callback parameter's declared type.
Declaring the parameter as `object`, `IAsyncResponsePayload`, a base class, or the concrete type all
work; guards like `payload is OrderResult` behave identically to the live path.

So one `object`-typed callback can serve several payload types. If the recorded payload type
cannot be resolved or materialized (renamed or removed across a deploy, or a generic argument from
an assembly this process has not loaded — resolution never loads one), the response goes to the
failure callback with the raw payload attached; an unresolvable name is counted on
`asyncresponse.type_resolution.unresolved` (see [security.md](security.md#type-resolution-for-plugins--assemblyloadcontext)).

## Registering recovery callbacks

Register what should happen if the response arrives after your process died. Callbacks are
serializable method descriptors (persisted in the store, invoked through DI by the process that
receives the late response — which may be a *different deployment*):

```csharp
public sealed class OrderController(
    IRecoverableAsyncResponseBuilder _asyncResponse,
    IRemoteSystem _remoteSystem)
{
    public async Task<OrderResult> SubmitAsync(string orderId)
    {
        return await _asyncResponse
            .For<OrderResult>()
            .Until(r => r.Status != OrderStatus.Processing)
            .OnLostSubscriberResume<IOrderFlow>(flow =>
                flow.ResumeAsync(orderId, Placeholder.Payload<OrderResult>(), Placeholder.CorrelationId()))
            .OnLostSubscriberFailure<IOrderFlow>(flow =>
                flow.FailAsync(Placeholder.Exception(), Placeholder.CorrelationId()))
            .WaitAsync(context => _remoteSystem.SubmitAsync(orderId, context.CorrelationId));
    }
}
```

`Placeholder.Payload<T>()`, `Placeholder.Exception()`, and `Placeholder.CorrelationId()` are
compile-time markers substituted with the real values when the callback fires. Literal arguments
(`orderId`) are captured by value.

The failure callback receives an `AsyncResponseDomainFailureException` for domain failures (carrying
the payload JSON, outcome, and correlation id) and the original exception for technical ones —
pattern-match to tell them apart:

```csharp
public sealed class OrderFlow(ILogger<OrderFlow> _logger, IOrderStore _orders) : IOrderFlow
{
    public Task FailAsync(Exception ex, string correlationId)
    {
        if (ex is AsyncResponseDomainFailureException domain)
            _logger.LogError("Order flow {CorrelationId} failed remotely: {Payload}", correlationId, domain.PayloadJson);

        return _orders.MarkFailedRetriableAsync(correlationId, ex.Message);
    }
}
```

> The domain payload JSON is carried on `AsyncResponseDomainFailureException.PayloadJson`, **not** in
> the exception `Message`, so the payload (which may contain PII) does not leak into generic
> exception logs. Log `PayloadJson` deliberately, where you mean to.

> ⚠️ **Naming contract:** callback targets are persisted as interface/method *name strings* and live
> in the store for up to `RecoveryStateExpiry`. Renaming a registered callback method is a breaking
> change for in-flight recovery state — deploy renames with care (keep a forwarding method for one
> expiry window).

> ⚠️ **Binding contract:** the persisted descriptor is *name + parameter count*, so the target
> must be the only public method on the interface (base interfaces included) with that name and
> arity — an overload set such as `Run(int)` / `Run(string)` cannot be told apart on the wire.
> The expression overloads (`OnLostSubscriberResume<T>(...)`, `EnqueueWorkerAsync<T>(...)`)
> validate this **at registration**, in your stack, and throw for an ambiguous, by-ref, or
> open-generic target; the same check runs at dispatch, so the two never disagree. Targets must
> return `Task`, `ValueTask`, or `void` **synchronously** — an `async void` implementation is
> refused before it is invoked (its body would still be running when the job is acknowledged and
> its DI scope disposed, and any later exception would escape to the thread pool). The refusal
> applies to the *implementation* the interface resolves to, so it is checked on the first
> dispatch and cached per implementation type.

### Make resume callbacks re-entrant

A resume may re-trigger a flow whose step is still running remotely, so resume should *re-attach*
(subscribe to the same correlation id) rather than re-execute side effects; persist enough state to
tell the difference. **Register both callbacks** for any flow that must survive redeploys: a failed
payload with no failure callback is logged and dropped (never resumed) — still a stuck flow. A
resumable payload for a registration with only a failure callback takes the failure route rather
than being discarded.

Recovery callbacks are **at-least-once**. Two publishers racing on one orphaned correlation id can
each load the registration before either deletes it, and a crash between "callback invoked" and
"registration deleted" re-invokes the callback on the next publish. On Redis and NATS a fan-out's
consumed registrations are removed together after every registration has been dispatched (see
[shared-correlation recovery](#shared-correlation-recovery)), so a crash, a concurrent redelivery,
or a racing publisher mid-fan-out can re-invoke every callback that fan-out already ran. There is
deliberately no distributed claim step in front of the callback: resume must be re-attach-safe
anyway. Treat both callbacks as idempotent — key side effects on the correlation id, not on the
invocation.

### When the failure callback cannot be invoked

On both failure routes (a response that declined to resume, and `SetException`), the dispatcher
makes up to four in-process attempts at the failure callback for transient faults, with jittered
backoff of up to 250 ms, 500 ms, then 1 s. If every attempt fails, the publish throws
`RecoveryCallbackFailedException` (correlation id + attempt count): the registration stays armed,
the broker ingress passes the exception through untouched (no further retry, no `SetException`
escalation that would call the same failing callback again), and the **transport redelivers the
message** under its own `MaxDeliveryAttempts`/dead-letter policy. A terminal signal is never
acknowledged while the flow stays stuck; it waits in the broker until the dependency recovers or an
operator replays it from the dead-letter destination. On RabbitMQ's default
`MaxDeliveryAttempts = 0` that means unlimited requeue — configure a cap and a `DeadLetterExchange`
as you would for worker jobs. A direct caller of `SetResponse`/`SetException` (an HTTP callback
endpoint) sees the same exception and should answer the remote system with a retriable status.

**Deterministic faults** are logged and acknowledged, because redelivery cannot fix them: an
unauthorized or unresolvable target, a malformed persisted descriptor (null parameter list, null
entry, null or blank name), a method that no longer binds, or a persisted argument that no longer
converts to its parameter type (e.g. a payload that could not be materialized as the registered
type). The kept registration is what the watchdog surfaces. Through the broker ingress, a
deterministic *resume* fault escalates to the failure callback at once, skipping the ingress's retry
ladder.

### When the stored registrations cannot be read

If any live registration for the correlation id cannot be interpreted by this build — malformed, an
incomplete identity, or a newer schema version — the store throws
`RecoveryStateUnreadableException` instead of reporting "no registration", and **no registration is
dispatched, the readable ones included**. Dispatching the readable subset would consume them and let
the transport acknowledge the response, leaving the unreadable one armed with no payload left to
deliver. Refusing up front also means nothing runs twice: the redelivery that reaches a build able
to read every registration (the newer one a rolling upgrade is bringing up) settles them all at once.

- On the database channels and NATS, a registration carrying *another* correlation id (a legacy
  case-insensitive collation's match) is readable but belongs elsewhere, so it counts as absent. Redis
  keys are exact, so there such an entry is corrupt and counts as unreadable.
- A refused lookup still yields to a live waiter that subscribed in the snapshot race: the response
  is handed back for live delivery rather than failed.
- The broker ingress propagates the exception without `SetException` escalation (that dispatch would
  read the same rows and fail identically), so the transport redelivers or dead-letters the message.
  It still runs the ingress retry ladder first (4 attempts, about 1.75 s), which paces each
  redelivery. An uncapped broker (RabbitMQ's default `MaxDeliveryAttempts = 0`) keeps redelivering at
  that pace until a build that can read the registrations consumes it or an operator removes them;
  a capped broker dead-letters it once its delivery count is spent.

The complete multi-step recipe built on these rules — a persisted step ledger, re-attach via the
pending correlation id, subset runs, and compensation — is in [durable-flows.md](durable-flows.md).

## Why recovery routing and `Until` stay separate

| | `Until` (live) | `OnRecovery()` (recovery) |
|---|---|---|
| **When it runs** | A waiter is alive and receives the message | The message arrives with *no waiter alive* |
| **Question** | "Is the operation done?" | "What should this late result do to the flow?" |
| **Who answers** | The waiter's predicate, with the calling code's full context | The payload alone — the recovering process has only the persisted registration and the message |
| **Failed payload** | A valid response: persist details, retry, or throw a rich domain error | Routed to the failure callback, never resumed |
| **Progress message** | Observed and skipped | `KeepWaiting` — the recovery-side mirror of that skip |

They can't be merged: they answer different questions at different times with different information
available.

## The recovery watchdog + health check

The recovery watchdog is part of the engine: `AddAsyncResponse()` starts it by default for whichever
channel you registered (in-memory, Redis, NATS, PostgreSQL, SQL Server, or MongoDB). It
periodically scans persisted recovery state and warns about entries that are old and have no live
waiter — flows that are probably stuck. By default it first scans 5 minutes after start, then every
6 hours, and flags entries older than 24 hours; tune it through `AsyncResponseOptions.Watchdog`
([configuration.md](configuration.md#engine-options-asyncresponseoptions)). When several hosts share
one store, set `Watchdog.Enabled = false` in all but one so scans and warnings aren't duplicated.

`AddAsyncResponseRecoveryCheck()` surfaces the cached findings on your health endpoint, with stats
and the offending correlation ids:

```csharp
builder.Services.AddHealthChecks()
    .AddAsyncResponseRecoveryCheck();              // surface the watchdog on /readyz
```

The check reports at most **`Degraded`** — a stuck *business flow* must never pull a healthy
*process* out of rotation, so keep `Degraded → 200` on readiness endpoints (the ASP.NET Core
default). Alert on its warnings or the `Degraded` status; the watchdog also feeds the
`asyncresponse.recovery.*` gauges ([observability.md](observability.md#instruments)).

**Before the first scan completes**, the check reports one of three states:

- **idle** — this host deliberately does not scan (`Watchdog.Enabled = false`, or no
  `IRecoveryStateScanner` is available): `Healthy`, with `scanning: false` and a `reason`, since
  another host attests staleness. With the in-memory channel the scanner is whatever
  `IRecoveryStateStore` resolves, so a custom store or decorator that does not implement or forward
  `IRecoveryStateScanner` idles the watchdog.
- **armed** — the scan loop runs but is still inside its first-scan budget (`StartupDelay` + 2 ×
  `Interval`): `Healthy`, with `scanning: true` and `firstScanDueByUtc`.
- **overdue** — armed but past that budget with nothing published: `Degraded`, the same as a loop
  that stopped publishing (last scan older than 2 × `Interval` plus the interval jitter).

**Once scanning**, the check is `Degraded` when any of these hold:

- stale entries exist;
- the scan could not probe waiter liveness for some entries (a probe outage, or no
  `IActiveSubscriberProbe` registered) — their staleness is unknown and never flagged stale by
  itself, so the check refuses to attest a clean pass (`unprobeable`);
- the scan found registrations this build **cannot read** — malformed, an incomplete identity, or a
  newer schema version (expected briefly during a rolling upgrade). A response for such a
  correlation id is refused and redelivered with its readable siblings undispatched (see
  [above](#when-the-stored-registrations-cannot-be-read)). The count is in the `unreadable` stat and
  gauge; the store logs a warning for each, with its key or correlation id wherever the record still
  yields one. Deploy a build that can read them, or remove them;
- the scan stopped at `MaxScanEntries` (truncated);
- the scan **failed**.

"Could not read part of the store" is a failure, not an empty result. That is a contract on
`IRecoveryStateScanner.ScanAsync`: a scanner that cannot inspect some of its storage must throw,
never complete with the reachable subset. Registrations it can find but not interpret are not
absence either: every built-in scanner (Redis, NATS, PostgreSQL, SQL Server, MongoDB) yields
everything it can read, then throws `RecoveryStateScanUnreadableException` with the count of
unexpired unreadable registrations; the watchdog keeps the readable results (one corrupt record
never hides the staleness of the rest) and reports the count. A custom scanner should follow the
same contract; any other caller of `ScanAsync` sees such a scan fail.

**Redis scan topology.** The scanner reads connected primaries only and fails the scan when **no
primary is connected**, or, in a **cluster**, when a slot owner is unreachable — expect
`Async-response watchdog scan failed: …` during a Redis outage. Outside a cluster one connected
primary is the whole keyspace, so a failed-over deployment that still lists its old primary as
disconnected scans normally, and a disconnected replica never matters. In a cluster every scan asks a
connected primary for `CLUSTER NODES` (the ACL must allow it) and uses that table, not the client's
possibly stale view of node roles:

- an unreachable node the table lists as a **replica** or as **owning no slots** is ignored;
- every slot owner the table lists must be scanned — a promoted node still flagged "replica" by the
  client is scanned as the owner it is, and a slot owner with no connected server fails the scan;
- a failed-over cluster scans normally as soon as the old primary's slots have moved;
- an unreachable endpoint the table does not list (a DNS name the cluster does not announce) is
  excused only once every listed slot owner has been scanned;
- a table that cannot be read while a primary-flagged endpoint is unreachable fails the scan.

**Scan throughput.** The Redis scan streams keys from `SCAN` and reads registration blobs in
pipelined batches of 128 (individual `GET`s, so a cluster routes each to its shard). The NATS scan
reads key-value entries in concurrent batches of 128 while the key listing streams; a read that
fails fails the scan. `Watchdog.ProbeConcurrency` governs the liveness-probe phase that follows.

## Recovery-state durability

Recovery state lives in the durable channel's store and survives a redeploy:

- **Redis** — keys under `KeyPrefix`, one per correlation id holding its registrations. Each
  registration carries its own `RecoveryStateExpiry` (7 days default) stamped at save time, and the
  key's TTL tracks the longest-remaining one, so a fresh registration can neither keep a dead sibling
  recoverable nor truncate a longer-lived one. The TTL is rounded **up** to whole milliseconds
  (Redis's expiry precision). Updates are optimistic (transaction-conditioned compare-and-set with
  retries), so concurrent registrations for one correlation id all survive, and each queued
  command's own result is checked: a save or delete Redis rejects fails rather than reporting
  success.

  **Waiter-liveness probing** asks every endpoint `PUBSUB NUMSUB` concurrently. A positive count
  anywhere proves a live waiter; a zero is conclusive only once every endpoint that could hold the
  subscription has answered:

  - Every primary-flagged endpoint must answer, except one this process saw disconnect **at least
    90 seconds** ago. The grace covers a failover whose waiters have not yet re-subscribed on the
    promoted node (StackExchange.Redis moves subscriptions once it learns the new topology — at the
    latest on its `ConfigCheckSeconds` check, 60 s by default); raise `ConfigCheckSeconds` and a
    waiter can take longer than the grace to follow. The clock starts at the multiplexer's
    `ConnectionFailed` and resets when the endpoint is seen connected again. An endpoint this
    process never saw connected gets no grace.
  - Past the grace, outside a cluster one answering primary is the whole answer. In a cluster, with
    a primary-flagged endpoint disconnected, the `CLUSTER NODES` table is read and disconnected
    endpoints are excused only when every slot owner it lists answered. Without the table, an
    endpoint still flagged as a replica is not treated as a possible owner.
  - Anything less is **unprobeable**: a lost-subscriber publish throws for its caller to retry (the
    transport redelivers it) instead of consuming a possibly live waiter's registration. Inside the
    grace a lost response therefore survives only through the response transport's redelivery,
    whose budget should outlast 90 s — the NATS, PostgreSQL, SQL Server and MongoDB transport
    defaults (5 attempts × 5 s) do not, nor do Kafka's in-process retries or Service Bus's immediate
    redeliveries; a response still unrecovered when the budget runs out is dead-lettered.
- **NATS** — a JetStream key-value bucket (`RecoveryBucket`), with a per-entry expiry layered over
  the bucket's `MaxAge`. Updates are revision-conditioned (KV compare-and-set with retries), so
  concurrent registrations for one correlation id all survive. Registrations are keyed by
  correlation id only, not by `SubjectPrefix`, so deployments sharing one NATS system each need their
  own `RecoveryBucket`. The bucket is created on first use only if it does not exist; an existing one
  is used as is, with drift reported when first opened. Every read goes to the bucket stream's
  **leader** (JetStream's message-get API), not NATS.Net's Direct Get: the client creates KV buckets
  with Direct Get enabled, and on a replicated bucket any replica may answer one — including a
  follower that has not yet applied a registration the leader already acknowledged, whose "not
  found" would let the response be acknowledged with no callback run. A read that cannot reach the
  leader fails, and the delivery is retried. Each completed waiter leaves a KV delete
  marker, which the watchdog scan purges once it is 30 minutes old (bounded by the marker's own
  sequence, so it never removes a registration written after it).

  **Waiter-liveness probing** over NATS Core reports presence, not counts: only a "no responders"
  answer is a definitive zero (the channel pins `ThrowIfNoResponders` on every request, so a
  connection configured with `RequestReplyMode = Direct` cannot turn it into an ordinary reply). A
  probe delivered but not answered within `PresenceProbeTimeout` (2 s default) is **unprobeable**,
  not dead: the consume loop answers probes serially, behind the previous message's `Until`
  predicate, so a slow predicate must not make a healthy waiter look gone. The mirror case on
  publish: a response a subscriber received but did not acknowledge within
  `DeliveryConfirmationTimeout` counts as delivered and never reaches recovery — including one sent
  to a waiter whose host died without closing its connection, until the server drops that stale
  subscription at its ping timeout (see [configuration.md](configuration.md#channel-options)).
- **PostgreSQL** — `RecoveryStateTable` (default `asyncresponse_recovery_state`), one row per waiter
  registration; rows expire by `expires_at` and are pruned opportunistically during channel
  operations.
- **SQL Server** — `RecoveryStateTable` (default `asyncresponse_recovery_state`), one row per waiter
  registration; rows expire by database clock (`SYSUTCDATETIME()`) and are pruned opportunistically
  during channel operations.
- **MongoDB** — `RecoveryStateCollection` (default `asyncresponse_recovery_state`), one document per
  waiter registration; documents expire natively via a TTL index.

Propagated ambient context (trace id, principal, tenant) is persisted alongside the recovery state
as a `string`→`string` bag, so it survives the redeploy too. Don't set `RecoveryStateExpiry` below
your longest flow duration — once recovery state is gone, a late response has nothing to route
against.

### Database channels: the delivery claim

On PostgreSQL, SQL Server and MongoDB the channel message table (collection) carries two claim
markers, updated atomically: `acked_at` (a live waiter claimed the response) and
`recovery_claimed` (the publisher won the lost-subscriber path after `DeliveryConfirmationTimeout`).
A slow live waiter and a recovery callback therefore never both process one response.

Each claim also stamps `acked_seq` from a store-side monotonic sequence (PostgreSQL/SQL Server: a
`SEQUENCE` next to the message table; MongoDB: a counter document in `{messages}_counters`), and
every waiter registration draws its position from the same sequence. That order separates "acked
before this waiter registered" (history — a reused correlation id must not replay its
predecessor's response) from "acked to a fan-out group including this waiter" (delivered), even
within one server-clock tick. It is exact unless the claim's sequence draw stalled across ticks; a
stalled draw resolves as history — a missed delivery that recovers through the step timeout, never
a replayed response. Rows with no `acked_seq` (acked by an older build) fall back to the strict
server-clock comparison.

## Wire/schema versioning

`RecoveryState`, `WorkerJobEnvelope`, and the response envelope each carry a mandatory
**`SchemaVersion`**. A reader accepts only the versions its build explicitly supports — today, the
current one; a missing, null, or unsupported value is rejected, never inferred. Add historical
versions only alongside a tested migration path.

The response envelope is validated just as strictly:

- `Success` is mandatory (`Success is required.`) — an envelope without it is not read as a
  message-less failure, so a foreign producer must always send it.
- On a `Success: true` envelope, an absent `Payload` is rejected exactly like `"Payload": null`
  (a permanent `JsonException`: the delivery faults instead of completing a waiter with `null`). A
  duplicated `Payload` key binds last-wins.

Keep all hosts that share recovery or worker storage on the same wire schema during deployment. An
incompatible writer fails safe: the reader refuses the payload instead of invoking a callback or
worker with a shape it does not understand.

**Unreadable is not missing.** On Redis and NATS a correlation id's registrations share one stored
value, and updating one rewrites the whole value. A registration this build cannot interpret (e.g.
written by a newer host) is carried through those rewrites untouched — pruning it would silently
delete a live sibling's recovery callback mid-rolling-upgrade. A read that meets one, or a whole
value this build cannot parse, fails rather than reporting "no registration" or the readable rest, so
the terminal response is redelivered to a host that can read every registration (see
[When the stored registrations cannot be read](#when-the-stored-registrations-cannot-be-read)). The
durable-flow ledger applies the same rule (`FlowStateUnreadableException`).

## Shared-correlation recovery

Multiple recoverable waiters may share one correlation id. Live delivery fans out to every active
waiter. If all waiters are lost, the store keeps one registration per waiter and a late
response/exception dispatches to every stored callback for that correlation id. A waiter that
completes normally removes only its own registration, so a still-active sibling stays recoverable.

On the database channels a registration and another process's delivery claim can land in the same
server-clock tick, indistinguishable by timestamp from a finished predecessor reusing the id. The
monotonic ack sequence ([above](#database-channels-the-delivery-claim)) separates them by integer
order. The one conservative residual: a claim whose sequence draw stalled into that exact tick
resolves as history, so the waiter misses that delivery (a durable-flow step faults at its step
timeout and restarts the idempotent step; a plain waiter surfaces a `TimeoutException`) — it can
never replay a consumed response. Rows with no sequence (acked by an older build) keep the
at-most-once server-clock tie resolution. The full reasoning is on `IsWithinWatermark` in the DB
channel source.

Each registration keeps its own delivery guarantee: a successful callback consumes only its own
registration.

- **Redis and NATS** keep a correlation id's registrations in one stored value, so the registrations
  one fan-out consumed are removed **together, in one conditional rewrite**, after every
  registration has been dispatched and before the dispatch settles. It removes exactly the consumed
  registrations — one added concurrently, one this build cannot read, and one whose callback failed
  all stay, each with its own expiry.
- **Database and in-memory stores** delete each registration as its callback succeeds. A custom
  `IRecoveryStateStore` needs no batch support.

**Any transient callback failure** throws `RecoveryCallbackFailedException` — for a single
registration too, and for a fan-out in which no callback succeeded. The ingress propagates it
without `SetException` escalation, so broker redelivery and the dead-letter policy own the retry;
direct publishers must retry it as well. This matters when `RecoverAsync` has saved a successful
response but cannot publish its resume job: the registration survives, and redelivery publishes the
missing wake-up without repeating the completed remote step.

Classification examines every failed sibling: a transient failure preserves redelivery even when a
deterministic failure came first. Deterministic binding/authorization failures are not wrapped —
after partial success they are logged and the registration is retained for the watchdog; if every
callback fails deterministically, the original exception follows the ingress's exception routing. A
failure callback that exhausted its own retry ladder keeps its `RecoveryCallbackFailedException` and
attempt count.

The watchdog reports shared-correlation recovery state once per correlation id, not once per
registration.
