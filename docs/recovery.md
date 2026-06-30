# Lost-subscriber recovery

[← Back to README](../README.md)

Every wait records *recovery state* for cleanup and watchdog visibility; durable channels (Redis,
NATS JetStream KV, PostgreSQL) persist it beyond the process. When a response arrives after the waiter died
(e.g. a redeploy), it is **classified by its domain outcome** and routed to the right callback:
resume the flow, or fail it — never resume a failure.

The `OnLostSubscriber*` methods intentionally live only on `IRecoverableAsyncResponseBuilder` and
its fluent builders. If an app is configured with `.WithInMemoryChannel()`, those methods are absent
at compile time; switch to a durable channel (`.WithRedisChannel()` / `.WithNatsChannel()` /
`.WithPostgreSqlChannel(...)`) and inject `IRecoverableAsyncResponseBuilder` for durable recovery
flows.

## `ShouldResumeOnRecovery()`

Override `ShouldResumeOnRecovery()` only when a payload can carry a *domain failure* and you use
lost-subscriber recovery. It answers one question — *should a late response of this type resume the
flow, or fail it?* — and is consulted **only** on the recovery path, never for live completion
(which your `Until` predicate owns):

```csharp
public sealed class OrderResult : IAsyncResponsePayload
{
    public OrderStatus Status { get; set; }
    public string? Message { get; set; }

    // recovery routing only: fail on a failed result, resume otherwise.
    public bool ShouldResumeOnRecovery() => Status != OrderStatus.Failed;
}
```

The default returns `false` (don't resume), so a failed response can never resume the happy path by
omission. Durable channels require this override when you register recovery callbacks — they fail
fast if it's missing; the in-memory channel, which can't survive a redeploy, doesn't.

`ShouldResumeOnRecovery()` is **independent of your `Until` predicate** (which owns live completion).
They answer different questions: "is it a failure?" versus "is the operation done?". A failed payload
is *still a valid response* for an active waiter — your `Until` and flow code want to see it. This is
the [two-axes recovery model](#why-recovery-routing-and-until-stay-separate).

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
a stuck flow.

## Why recovery routing and `Until` stay separate

`ShouldResumeOnRecovery()` is consulted only when nobody is listening — which is exactly when
somebody has to make the call. The two are deliberately two axes:

- **`Until` (live)** — owns completion for an *active* waiter. A failed payload is a valid response
  here: your predicate and flow code can persist details, decide to retry, or throw a rich domain
  error.
- **`ShouldResumeOnRecovery()` (recovery)** — owns routing for a response that arrives with *no
  waiter alive*. It picks the resume callback or the failure callback.

They can't be merged because they answer different questions at different times with different
information available.

## The recovery watchdog + health check

The recovery watchdog is part of the engine: `AddAsyncResponse()` starts it by default, and it works
for whichever channel you registered (in-memory, Redis, NATS, or PostgreSQL). It periodically scans the
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
  `RecoveryStateExpiry` (7 days default).
- **NATS** — recovery state lives in a JetStream Key-Value bucket (`RecoveryBucket`), with a
  per-entry expiry layered over the bucket's `MaxAge`.
- **PostgreSQL** — recovery state lives in `RecoveryStateTable` (default
  `asyncresponse_recovery_state`) as one row per waiter registration. Rows expire by `expires_at`
  and are pruned opportunistically during channel operations.

The carrier for propagated ambient context (trace id, principal, tenant) is persisted alongside the
recovery state as a `string`→`string` bag, so it survives the redeploy too. Don't set
`RecoveryStateExpiry` below your longest flow duration — once recovery state is gone, a late response
has nothing to route against.

On PostgreSQL, late-delivery routing is protected by two columns on the channel message table:
`acked_at` means a live waiter claimed the response, while `recovery_claimed` means the publisher
won the lost-subscriber path after the confirmation timeout. The channel updates those columns
atomically so a slow live waiter and a recovery callback cannot both process the same response.

## Wire/schema versioning

`RecoveryState`, `WorkerJobEnvelope`, and the response envelope each carry a **`SchemaVersion`**. A
reader **rejects anything stamped with a version newer than it understands**, so a mixed-version
deploy (an old host reading data written by a newer one) fails safe instead of misinterpreting
fields. Data written *without* the field — produced by an older build before versioning existed — is
read as the current version and accepted.

Practically: rolling a newer build forward is fine (newer readers understand older data); rolling a
*newer* writer's data back into an *older* reader is the case versioning protects against — the older
reader refuses the payload rather than guessing.

## Shared-correlation recovery

Multiple recoverable waiters may share one correlation id. Live delivery still fans out to every
active waiter; if all waiters are lost, the recovery store keeps one registration per waiter and a
late response/exception dispatches to every stored callback for that correlation id. A waiter that
completes normally removes only its own registration, so a still-active sibling remains recoverable.

The watchdog reports shared-correlation recovery state once per correlation id, not once per stored
waiter registration.
