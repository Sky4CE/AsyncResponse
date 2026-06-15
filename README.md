# AsyncResponse

**Await responses from message brokers and background workers as if they were local async calls — with durable, domain-aware recovery when your process dies mid-wait.**

[![CI](https://github.com/vitaliy-opti/AsyncResponse/actions/workflows/ci.yml/badge.svg)](https://github.com/vitaliy-opti/AsyncResponse/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/AsyncResponse.Redis.svg)](https://www.nuget.org/packages/AsyncResponse.Redis)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

```csharp
OrderResult result = await asyncResponse
    .For<OrderResult>()                                       // correlation id generated for you
    .WithTimeout(TimeSpan.FromMinutes(10))
    .Until(r => r.Status != OrderStatus.Processing)           // consume progress messages
    .WaitAsync(correlationId =>                               // looks sync, is fully async
        paymentGateway.StartAsync(orderId, correlationId));   // sent only AFTER subscribing
```

---

## The problem

You call a remote system (another service, an Airflow DAG, a payment gateway, a long-running
job) and the answer comes back **later, on a different channel** — a message broker topic, a
webhook, a callback queue. Correlating that answer back to the code that asked for it usually
means hand-rolled `TaskCompletionSource` registries, polling loops, or callback spaghetti.

And then the hard part: **your service redeploys while it's waiting.** The in-memory waiter is
gone. The response arrives anyway. Now what?

- If you drop it, the flow hangs "in progress" forever.
- If you blindly resume the flow, you just resumed the **happy path on a failed response** —
  the payload said `"status": "failed"`, but nobody looked.

AsyncResponse solves both halves:

1. **Correlation** — `await` a response by correlation id over Redis pub/sub, with fluent
   timeouts, progress predicates, and race-free triggering.
2. **Recovery** — every wait persists durable *recovery state* (7 days by default). A response
   that arrives after the waiter died is **classified by its domain outcome** and routed to the
   right callback: resume the flow, or fail it — never resume a failure.

## How it works

```
        you                       AsyncResponse                         remote system
         │                              │                                     │
         │  For<T>(cid).WaitAsync(send) │                                     │
         ├─────────────────────────────►│ 1. subscribe cid + persist          │
         │                              │    RecoveryState (Redis)            │
         │                              │ 2. run trigger ────────────────────►│  (request sent
         │        await response        │                                     │   AFTER subscribe)
         │                              │◄──────── progress message ──────────┤
         │                              │   Until(…) → keep waiting           │
         │                              │◄──────── terminal message ──────────┤
         │◄────── payload / exception ──┤ 3. complete + clean up              │
         │                              │                                     │

   …and when a redeploy killed the waiter before step 3:

         │                              │◄──────── terminal message ──────────┤
         │                              │ nobody is listening →               │
         │                              │ classify payload.ClassifyOutcome()  │
         │   ResumeCallback(payload)  ◄─┤   Succeeded / InProgress            │
         │   FailureCallback(exception)◄┤   Failed / Unknown                  │
```

Three layers, one decision each, made exactly where its deciding fact is knowable:

| Layer | Knowable fact | Decision |
|---|---|---|
| **Ingress** (`IAsyncResponseIngress`) | "Does the message parse?" | Parses → deliver as payload, untyped and uninterpreted. Doesn't parse → report as exception. |
| **Transport** (`SetResponse`/`SetException`) | "Did any subscriber receive it?" | Delivered → the active waiter's `Until` and flow code interpret it. Nobody listening → hand to the dispatcher. |
| **Lost-subscriber dispatcher** | "What domain state does the payload carry?" | `Succeeded`/`InProgress` → resume callback. `Failed`/`Unknown` → failure callback. |

A failed payload is **still a valid response** for an active waiter (your `Until` predicate and
flow code want to see it — persist details, decide to retry, throw a rich domain error).
Classification applies only when nobody is listening — which is exactly when somebody has to
make the call.

## Packages

| Package | What's inside |
|---|---|
| `AsyncResponse.Redis` | The Redis transport, recovery watchdog, readiness health check. **Reference this from your host.** |
| `AsyncResponse.Core` | Transport-agnostic core: fluent builder, outcome classifier, lost-subscriber dispatcher, expression-based callbacks, in-process worker transport. |
| `AsyncResponse.Abstractions` | Contracts only — reference from class libraries that define payloads or flows. |

## Installation

```bash
dotnet add package AsyncResponse.Redis
```

## Quick start

```csharp
using AsyncResponse;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect("localhost:6379"));

builder.Services.AddRedisAsyncResponse();          // transport + fluent builder
builder.Services.AddInProcessWorkerTransport();    // optional: background worker jobs
builder.Services.AddAsyncResponseWatchdog();       // optional: stale-flow detection
builder.Services.AddHealthChecks()
    .AddAsyncResponseRecoveryCheck();              // optional: surface it on /readyz
```

### 1. Define a payload — and its domain semantics

Every payload implements `IAsyncResponsePayload`. The `ClassifyOutcome()` member is
**deliberately required**: the "what does a failed response mean" decision must be made
explicitly by every payload author, so it can never be forgotten — that omission is precisely
the bug class this library exists to prevent.

```csharp
public sealed class OrderResult : IAsyncResponsePayload
{
    public OrderStatus Status { get; set; }
    public string? Message { get; set; }

    public AsyncResponseOutcome ClassifyOutcome() => Status switch
    {
        OrderStatus.Completed  => AsyncResponseOutcome.Succeeded,
        OrderStatus.Processing => AsyncResponseOutcome.InProgress,
        OrderStatus.Failed     => AsyncResponseOutcome.Failed,
        _                      => AsyncResponseOutcome.Unknown   // fail conservatively
    };
}
```

Mirror the semantics your `Until` predicate applies. A payload only ever published on success
paths simply returns `Succeeded`.

### 2. Request/response correlation

```csharp
public async Task<OrderResult> PlaceOrderAsync(int orderId)
{
    var correlationId = AsyncResponseContext.CreateCorrelationId();

    return await _asyncResponse
        .For<OrderResult>(correlationId)
        .WithTimeout(TimeSpan.FromMinutes(10))
        .Until(r => r.Status != OrderStatus.Processing)
        .WaitAsync(() => _remoteSystem.SubmitAsync(orderId, correlationId));
}
```

The `WaitAsync` trigger is the race-killer: the request is sent **after** the subscription and
recovery state exist, so the first response can never arrive before anyone is listening, and a
failing trigger tears the registration down (the operation never started). Rule of thumb:
*never send the request yourself — pass the send as the trigger.*

Two more shapes of the same terminal:

- `WaitAsync()` with no trigger — the request was already sent: a resumed step re-attaching to
  its in-flight correlation id, or a different system owning the send. Only your flow can know
  this (from its persisted state), so it's an explicit argument, never auto-detected.
- `For<T>().WaitAsync(correlationId => …)` — the builder generates the correlation id (also
  placing it in the ambient `AsyncResponseContext`) and hands it to the trigger, so simple flows
  never touch correlation ids at all.

Need the waiter's lifetime under your control? `BuildWaiterAsync()` returns an
`IAsyncResponseWaiter<T>`; `await waiter.ResponseTask` when ready, dispose to cancel.

### 3. Deliver responses (your broker → the ingress)

Wherever responses physically arrive — Google Pub/Sub, RabbitMQ, Kafka, an HTTP webhook — feed
them into the ingress:

```csharp
// e.g. inside your broker consumer:
await ingress.HandleResponseMessageAsync(messageBodyJson, correlationIdFromHeaders);
```

In-process publishers can skip the ingress and call `IAsyncResponsePublisher` directly:

```csharp
await publisher.SetResponse(new OrderResult { Status = OrderStatus.Completed }, correlationId);
await publisher.SetException(new Exception("downstream unavailable"), correlationId); // technical failure
```

### 4. Survive redeploys: lost-subscriber recovery

Register what should happen if the response arrives after your process died. Callbacks are
serializable method descriptors (persisted in Redis, invoked through DI by the process that
receives the late response — which may be a *different deployment*):

```csharp
var result = await _asyncResponse
    .For<OrderResult>(correlationId)
    .Until(r => r.Status != OrderStatus.Processing)
    .OnLostSubscriberResume<IOrderFlow>(flow =>
        flow.ResumeAsync(orderId, Placeholder.Payload<OrderResult>(), Placeholder.CorrelationId()))
    .OnLostSubscriberFailure<IOrderFlow>(flow =>
        flow.FailAsync(Placeholder.Exception(), Placeholder.CorrelationId()))
    .WaitAsync(() => _remoteSystem.SubmitAsync(orderId, correlationId));
```

`Placeholder.Payload<T>()`, `Placeholder.Exception()`, and `Placeholder.CorrelationId()` are
compile-time markers substituted with the real values when the callback fires. Literal arguments
(`orderId`) are captured by value.

The failure callback receives an `AsyncResponseDomainFailureException` for domain failures
(carrying the payload JSON, outcome, and correlation id) and the original exception for
technical ones — pattern-match to tell them apart:

```csharp
public Task FailAsync(Exception ex, string correlationId)
{
    if (ex is AsyncResponseDomainFailureException domain)
        _logger.LogError("Order flow {Cid} failed remotely: {Payload}", correlationId, domain.PayloadJson);

    return _orders.MarkFailedRetriableAsync(correlationId, ex.Message);
}
```

> ⚠️ **Naming contract:** callback targets are persisted as interface/method *name strings* and
> live in Redis for up to `RecoveryStateExpiry`. Renaming a registered callback method is a
> breaking change for in-flight recovery state — deploy renames with care (keep a forwarding
> method for one expiry window).

### 5. Fire-and-forget background workers

```csharp
await _asyncResponse.EnqueueWorkerAsync<IOrderFlow>(flow => flow.ProcessOrderAsync(orderId));
```

The ambient correlation id is captured with the job and restored before execution, so anything
the job publishes correlates automatically. `AddInProcessWorkerTransport()` executes jobs in the
current process; for distributed execution implement `IWorkerTransport` against your broker and
have the consumer call `ingress.HandleWorkerMessageAsync(json)`.

### 6. Timeouts, errors, and cancellation

```csharp
try
{
    var result = await _asyncResponse.For<OrderResult>(correlationId)
        .WithTimeout(TimeSpan.FromSeconds(30))
        .WaitAsync();
}
catch (TimeoutException)   { /* no response in time — flow fails visibly, never hangs */ }
catch (Exception ex)       { /* SetException from the remote side, with remote stack in ex.Data */ }
```

- **Waits are never infinite.** Without `WithTimeout`, the default is the recovery-state expiry
  (7 days) — once recovery state is gone, waiting longer is meaningless.
- A trigger that throws tears the waiter down (subscription + recovery state) and rethrows: the
  operation never started, so nothing is left armed.
- Disposing a waiter cancels the subscription and deletes its recovery state.

### 7. Operations: watchdog + health check

`AddAsyncResponseWatchdog()` (register in **one** host per Redis) periodically scans the
persisted recovery state and warns about entries that are old and have no live waiter — flows
that are probably stuck. `AddAsyncResponseRecoveryCheck()` surfaces the cached findings on your
health endpoint with stats and the offending correlation ids.

The check reports at most **`Degraded`** — a stuck *business flow* must never pull a healthy
*process* out of rotation, so keep `Degraded → 200` on readiness endpoints (the ASP.NET Core
default).

## Configuration

```csharp
builder.Services.AddRedisAsyncResponse(options =>
{
    options.KeyPrefix = "myapp";                            // isolate apps/environments
    options.RecoveryStateExpiry = TimeSpan.FromDays(7);     // how long recovery survives
    options.DefaultTimeout = TimeSpan.FromHours(12);        // default per-waiter timeout
});

builder.Services.AddAsyncResponseWatchdog(options =>
{
    options.Interval = TimeSpan.FromHours(6);
    options.StaleAfter = TimeSpan.FromHours(24);
    options.StartupDelay = TimeSpan.FromMinutes(5);
});
```

Tracing: all operations emit `System.Diagnostics.Activity` spans from the `"AsyncResponse"`
`ActivitySource` (`asyncresponse.wait`, `asyncresponse.set_response`, …) — subscribe to it from
OpenTelemetry with `.AddSource("AsyncResponse")`.

## The sample app

A complete testbed lives in [`samples/AsyncResponse.Sample`](samples/AsyncResponse.Sample):

```bash
docker compose up -d          # local Redis
dotnet run --project samples/AsyncResponse.Sample
```

Then walk the scenarios:

```bash
curl -X POST 'localhost:5000/demo/request-response?behavior=Succeed'     # happy path with progress messages
curl -X POST 'localhost:5000/demo/request-response?behavior=FailDomain'  # domain failure seen by the active waiter
curl -X POST 'localhost:5000/demo/timeout'                               # 2s timeout vs 15s remote
curl -X POST 'localhost:5000/demo/worker?orderId=42'                     # background worker job

# The headline feature — recovery after a "redeploy":
curl -X POST 'localhost:5000/demo/lost-subscriber/arm'                   # → returns a correlationId
curl -X POST 'localhost:5000/demo/lost-subscriber/crash'                 # kills every subscription
curl -X POST 'localhost:5000/demo/lost-subscriber/respond?correlationId=<id>&status=Completed'  # → resume callback
curl -X POST 'localhost:5000/demo/lost-subscriber/respond?correlationId=<id>&status=Failed'     # → failure callback
curl 'localhost:5000/healthz'                                            # watchdog findings
```

## Best practices

1. **Always make the send the trigger** (the `WaitAsync` argument). Sending before subscribing
   is a race: a fast first response finds nobody listening and, on first registration, no
   recovery state either.
2. **Classify honestly.** `ClassifyOutcome()` must mirror your active waiter's `Until`
   semantics. Map unrecognized states to `Unknown` (fails conservatively) unless your active
   path deliberately keeps waiting on them — then map them to `InProgress`.
3. **Register both recovery callbacks** for any flow that must survive redeploys. A failed
   payload with no failure callback is logged and dropped — never resumed — but dropped is
   still a stuck flow.
4. **Make resume callbacks re-entrant.** A resume may re-trigger a flow whose step is still
   running remotely; resume should *re-attach* (subscribe to the same correlation id) rather
   than re-execute side effects. Persist enough state to tell the difference.
5. **Treat callback method names and the `KeyPrefix` as deployment contracts.** They are
   persisted; rename with a migration window.
6. **Set timeouts per flow.** The 7-day default is a backstop, not a recommendation; a payment
   flow should fail in minutes.
7. **Run the watchdog in exactly one host per Redis** and alert on its warnings or the
   `Degraded` health status — stale recovery state is your earliest signal of stuck flows.
8. **Mind Redis pub/sub semantics.** Delivery is at-most-once to live subscribers; the recovery
   state is what makes the system safe across gaps. Don't disable it (`RecoveryStateExpiry`)
   below your longest flow duration.
9. **One `IConnectionMultiplexer`.** Reuse your application's existing multiplexer; don't create
   a second connection for AsyncResponse.

## License

[MIT](LICENSE) — © Vitalii Tiunisov
