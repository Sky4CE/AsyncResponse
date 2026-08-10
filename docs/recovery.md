# Lost-subscriber recovery

[← Back to README](../README.md)

Every wait records *recovery state* for cleanup and watchdog visibility; durable channels (Redis,
NATS, PostgreSQL, SQL Server, MongoDB) persist it beyond the process. When a response arrives after the waiter died
(e.g. a redeploy), it is **classified by its domain outcome** and routed to the right callback:
resume the flow, fail it — never resume a failure — or, for a non-terminal checkpoint, keep the
registration armed and wait for the terminal response.

The `OnLostSubscriber*` methods intentionally live only on `IRecoverableAsyncResponseBuilder` and
its fluent builders. If an app is configured with `.WithInMemoryChannel()`, those methods are absent
at compile time; switch to a durable channel (`.WithRedisChannel()` / `.WithNatsChannel()` /
`.WithPostgreSqlChannel(...)` / `.WithSqlServerChannel(...)` / `.WithMongoDbChannel(...)`) and
inject `IRecoverableAsyncResponseBuilder` for durable recovery flows.

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
callbacks** — durable channels fail fast at waiter creation without it. That includes payloads that
can never fail (a success-only notification still needs `=> RecoveryAction.Resume`) and
progress-only checkpoints (`=> RecoveryAction.KeepWaiting`), not just payloads that can carry a
domain failure. It answers one question — *what should a late response of this type do to the flow:
resume it, fail it, or keep waiting for the terminal response?* — and is consulted **only** on the
recovery path, never for live completion (which your `Until` predicate owns):

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

The default (no override) is `Fail`, so a response can never resume the happy path by omission.
Every channel requires the override when you register recovery callbacks — waiter creation fails
fast if it is missing. That includes the in-memory channel: it implements the same recoverable
contract against its process-local store (recovery there spans waiter loss within one process
lifetime — and the simulated restarts of [AsyncResponse.Testing](testing.md) — rather than a real
process exit), so a payload that passes in tests passes unchanged on Redis.

Recovery classification is **independent of your `Until` predicate** (which owns live completion).
They answer different questions: "what does this result do to the flow?" versus "is the operation
done?". A failed payload is *still a valid response* for an active waiter — your `Until` and flow
code want to see it. This is the
[two-axes recovery model](#why-recovery-routing-and-until-stay-separate).

## Non-terminal checkpoints: `KeepWaiting`

Some remote systems report progress on the same correlation id before the terminal result — "still
running" heartbeats, staged sub-results. A live waiter simply lets its `Until` predicate observe and
skip them. On the lost-subscriber path the same message needs an explicit third route:
`RecoveryAction.KeepWaiting` invokes **no callback** and **retains the recovery registration**, so
the terminal response that follows still routes.

This lane exists because both alternatives corrupt the flow (this is the production incident that
motivated it — a deploy killed a waiter mid-step, and the remote side then published two
"in progress" messages followed by a terminal success two seconds later):

- Classifying a checkpoint as `Resume` spawns one resumed worker *per checkpoint*, consumes the
  registration, and the terminal response then finds nothing to route against and is dropped — the
  resumed workers re-attach to a correlation id nothing can answer and hang to their step timeout.
- Classifying it as `Fail` fails a flow that is still running remotely, and equally consumes the
  registration out from under the real result.

A retained registration stays bounded by `RecoveryStateExpiry` and visible to the
[watchdog](#the-recovery-watchdog--health-check); the lost-subscriber route metric/trace tag reports
`keep_waiting` for these dispatches.

## Callbacks receive the materialized payload

A response that arrives through a broker ingress is raw JSON. The recovery path materializes it as
the payload type recorded in the registration *before* classifying it, and the chosen callback
receives **that materialized instance** — regardless of the callback parameter's declared type.
Declaring the parameter as `object`, `IAsyncResponsePayload`, a base class, or the concrete type all
work; guards like `payload is OrderResult` behave identically to the live path.

This matters most for services that register **one callback for several payload types** (an
`object`-typed parameter): binding the raw JSON to the *declared* parameter type used to hand such a
callback a `JsonElement`, silently failing every type guard inside it. If the recorded payload type
cannot be resolved or materialized (e.g. renamed/removed across a deploy), the response is routed
conservatively to the failure callback with the raw payload attached, and the type-resolution
failure is surfaced through diagnostics.

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

### Make resume callbacks re-entrant

A resume may re-trigger a flow whose step is still running remotely; resume should *re-attach*
(subscribe to the same correlation id) rather than re-execute side effects. Persist enough state to
tell the difference. And **register both callbacks** for any flow that must survive redeploys — a
failed payload with no failure callback is logged and dropped (never resumed), but dropped is still
a stuck flow. The inverse also routes conservatively: a resumable payload arriving for a
registration with only a failure callback takes the failure route (the flow cannot proceed without
a resume callback) instead of being discarded.

Recovery callbacks are **at-least-once**. Two publishers racing on the same orphaned correlation id
can each load the registration before either deletes it, and a crash between "callback invoked" and
"registration deleted" re-invokes the callback on the next publish. There is deliberately no
distributed claim step in front of the callback — resume must already be re-attach-safe, so the
extra store round-trip per recovery would buy nothing. Treat both callbacks as idempotent: key side
effects on the correlation id, not on the invocation.

The complete multi-step recipe built on these rules — a persisted step ledger, re-attach via the
pending correlation id, subset runs, and compensation — is documented in
[durable-flows.md](durable-flows.md).

## Why recovery routing and `Until` stay separate

Recovery classification is consulted only when nobody is listening — which is exactly when
somebody has to make the call. The two are deliberately two axes:

- **`Until` (live)** — owns completion for an *active* waiter. A failed payload is a valid response
  here: your predicate and flow code can persist details, decide to retry, or throw a rich domain
  error.
- **`OnRecovery()` (recovery)** — owns routing for a response that arrives with *no waiter
  alive*. It picks the resume callback, the failure callback, or the keep-waiting lane — the
  recovery-side mirror of `Until` skipping a non-terminal message.

They can't be merged because they answer different questions at different times with different
information available.

## The recovery watchdog + health check

The recovery watchdog is part of the engine: `AddAsyncResponse()` starts it by default, and it works
for whichever channel you registered (in-memory, Redis, NATS, PostgreSQL, SQL Server, or
MongoDB). It periodically scans the
persisted recovery state and warns about entries that are old and have no live waiter — flows that
are probably stuck. `AddAsyncResponseRecoveryCheck()` surfaces the cached findings on your health
endpoint with stats and the offending correlation ids.

```csharp
builder.Services.AddHealthChecks()
    .AddAsyncResponseRecoveryCheck();              // surface the watchdog on /readyz
```

Tune or disable it through `AsyncResponseOptions.Watchdog` — e.g. set `Watchdog.Enabled = false` in
all but one host when several hosts share one store, so its scan and warnings aren't duplicated.

The check reports at most **`Degraded`** — a stuck *business flow* must never pull a healthy
*process* out of rotation, so keep `Degraded → 200` on readiness endpoints (the ASP.NET Core
default). Alert on its warnings or the `Degraded` status: stale recovery state is your earliest
signal of stuck flows. The watchdog also feeds the `asyncresponse.recovery.*` gauges (see
[observability.md](observability.md)).

## Recovery-state durability

Recovery state lives in the durable channel's store and survives a redeploy:

- **Redis** — recovery state is stored in Redis keys under `KeyPrefix`, expiring after
  `RecoveryStateExpiry` (7 days default). Registration-list updates are optimistic
  (transaction-conditioned compare-and-set with retries), so concurrent registrations for one
  correlation id all survive.
- **NATS** — recovery state lives in a JetStream Key-Value bucket (`RecoveryBucket`), with a
  per-entry expiry layered over the bucket's `MaxAge`. Registration-list updates are
  revision-conditioned (KV compare-and-set with retries), so concurrent registrations for one
  correlation id all survive.
- **PostgreSQL** — recovery state lives in `RecoveryStateTable` (default
  `asyncresponse_recovery_state`) as one row per waiter registration. Rows expire by `expires_at`
  and are pruned opportunistically during channel operations.
- **SQL Server** — recovery state lives in `RecoveryStateTable` (default
  `asyncresponse_recovery_state`) as one row per waiter registration. Rows expire by DB clock
  (`SYSUTCDATETIME()`) and are pruned opportunistically during channel operations.
- **MongoDB** — recovery state lives in `RecoveryStateCollection` (default
  `asyncresponse_recovery_state`) as one document per waiter registration. Documents expire
  natively via a TTL index.

The carrier for propagated ambient context (trace id, principal, tenant) is persisted alongside the
recovery state as a `string`→`string` bag, so it survives the redeploy too. Don't set
`RecoveryStateExpiry` below your longest flow duration — once recovery state is gone, a late response
has nothing to route against.

On PostgreSQL, late-delivery routing is protected by two columns on the channel message table:
`acked_at` means a live waiter claimed the response, while `recovery_claimed` means the publisher
won the lost-subscriber path after the confirmation timeout. The channel updates those columns
atomically so a slow live waiter and a recovery callback cannot both process the same response.
The SQL Server and MongoDB channels implement the same claim protocol — the same pair of
columns/fields on the message table/collection, updated atomically — so the guarantee holds
across all three database channels.

Each delivery claim additionally stamps `acked_seq` from a store-side monotonic sequence
(PostgreSQL/SQL Server: a `SEQUENCE` next to the message table; MongoDB: a counter document in
`{messages}_counters`), and every waiter registration draws its own position from the same
sequence. That order separates "acked before this waiter registered" (history — a reused
correlation id must not replay its predecessor's response) from "acked to a fan-out group
including this waiter" (delivered) even when both events land on the same server-clock tick — a
tie timestamps cannot arbitrate. The arbitration is conservative-exact: exact whenever the
claim's sequence draw was not stalled across ticks, and on a stalled draw it resolves as history
(a missed delivery that recovers through the step timeout — never a replayed response; see the
shared-correlation section below). Rows acked by a build predating the column fall back to the
strict server-clock comparison.

## Wire/schema versioning

`RecoveryState`, `WorkerJobEnvelope`, and the response envelope each carry a **`SchemaVersion`**. A
reader accepts only versions explicitly supported by that build. Today that is the current version;
arbitrary lower numbers are not guessed to be compatible. The schema property is mandatory;
missing, null, and unsupported values are rejected rather than inferred. Add historical versions
only alongside a tested migration path.

Keep all hosts that share recovery or worker storage on the same wire schema during deployment.
An incompatible writer fails safe—the reader refuses the payload instead of invoking a callback or
worker with a shape it does not understand.

## Shared-correlation recovery

Multiple recoverable waiters may share one correlation id. Live delivery fans out to every active
waiter. On the database channels, a registration and another process's delivery claim can land in
the **same server-clock tick** — indistinguishable by timestamps from a finished predecessor
reusing the correlation id. The monotonic ack sequence breaks exactly that tie (see the delivery
protocol above): claims and registrations draw from one sequence, so tick-tied history and
fan-out separate by integer order. The one conservative residual: a claim whose sequence draw
stalled from an earlier tick into that exact tick resolves as history — the waiter misses that
delivery (a durable-flow step faults at its step timeout and restarts the idempotent step fresh;
a plain waiter surfaces a `TimeoutException`), which is the same verdict the previous
timestamp-only rule gave every tie and can never replay a consumed response. Rows acked by a
pre-1.0 build carry no sequence and keep that older at-most-once tie resolution. See the
`IsWithinWatermark` documentation in the DB channel source for the full reasoning. If all
waiters are lost, the recovery store keeps one registration per waiter and a
late response/exception dispatches to every stored callback for that correlation id. A waiter that
completes normally removes only its own registration, so a still-active sibling remains recoverable.

The watchdog reports shared-correlation recovery state once per correlation id, not once per stored
waiter registration.
