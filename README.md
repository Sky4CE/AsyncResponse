# AsyncResponse

**Await responses from message brokers and background workers as if they were local async calls — with optional durable, domain-aware recovery when your process dies mid-wait.**

[![CI](https://github.com/Sky4CE/AsyncResponse/actions/workflows/ci.yml/badge.svg)](https://github.com/Sky4CE/AsyncResponse/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/AsyncResponse.Channels.Redis.svg)](https://www.nuget.org/packages/AsyncResponse.Channels.Redis)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

```csharp
OrderResult result = await asyncResponse
    .For<OrderResult>()                                       // correlation id generated for you
    .WithTimeout(TimeSpan.FromMinutes(10))
    .Until(r => r.Status != OrderStatus.Processing)           // consume progress messages
    .WaitAsync(context =>                                     // looks sync, is fully async
        paymentGateway.StartAsync(orderId, context.CorrelationId)); // sent only AFTER subscribing
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

AsyncResponse solves the core correlation problem without requiring infrastructure, and lets you
add durable recovery when the flow needs it:

1. **Correlation** — `await` a response by correlation id, with fluent timeouts, progress
   predicates, and race-free triggering. `AsyncResponse.Core` ships a process-local response
   channel for simple apps and tests.
2. **Recovery** — response channels persist *recovery state* through a pluggable
   `IRecoveryStateStore`. The default store is in-memory; `AsyncResponse.Channels.Redis` adds durable
   recovery. A response that arrives after the waiter died is **classified by its domain
   outcome** and routed to the right callback: resume the flow, or fail it — never resume a
   failure.

## How it works

```
        you                       AsyncResponse                         remote system
         │                              │                                     │
         │  For<T>().WaitAsync(send)    │                                     │
         ├─────────────────────────────►│ 1. subscribe cid + save             │
         │                              │    RecoveryState (store)            │
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
| **Response channel** (`SetResponse`/`SetException`) | "Did any subscriber receive it?" | Delivered → the active waiter's `Until` and flow code interpret it. Nobody listening → hand to the dispatcher. |
| **Lost-subscriber dispatcher** | "What domain state does the payload carry?" | `Succeeded`/`InProgress` → resume callback. `Failed`/`Unknown` → failure callback. |

A failed payload is **still a valid response** for an active waiter (your `Until` predicate and
flow code want to see it — persist details, decide to retry, throw a rich domain error).
Classification applies only when nobody is listening — which is exactly when somebody has to
make the call.

## Packages

| Package | What's inside |
|---|---|
| `AsyncResponse.Core` | Fluent registration + waiter builder, process-local response channel and recovery store, transport-neutral ingress, outcome classifier, expression-based callbacks, in-memory worker queue, and the recovery watchdog + readiness health check. |
| `AsyncResponse.Channels.Redis` | Optional durable Redis response channel and recovery-state store; the Core watchdog and health check work against it automatically. |
| `AsyncResponse.Transports.GooglePubSub` | Optional Google Pub/Sub worker transport and hosted subscribers for worker jobs and response ingress. |
| `AsyncResponse.Abstractions` | Contracts only — reference from class libraries that define payloads or flows. |

Package naming follows the extension point: `AsyncResponse.Channels.*` packages provide
response/recovery channels; `AsyncResponse.Transports.*` packages provide broker transports for
workers and ingress.

## Installation

```bash
dotnet add package AsyncResponse.Core

# Optional durable Redis channel:
dotnet add package AsyncResponse.Channels.Redis

# Optional Google Pub/Sub transport:
dotnet add package AsyncResponse.Transports.GooglePubSub
```

## Quick start

### Core-only, no Redis

Use this when you want the async-response pattern without durable recovery. Waiters and recovery
state live in the current process.

```csharp
using AsyncResponse;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAsyncResponse()   // engine: fluent builder, ingress, recovery watchdog
    .WithInMemoryChannel()            // process-local response channel + recovery store (required)
    .WithInMemoryTransport();         // optional: in-process background worker jobs
```

### Redis-backed response channel and recovery

Use this when waiters may die during redeploys and late responses must resume/fail the owning
flow durably.

```csharp
using AsyncResponse;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect("localhost:6379"));

builder.Services.AddAsyncResponse()                // engine + recovery watchdog (on by default)
    .WithRedisChannel()                            // Redis response channel + Redis recovery store
    .WithInMemoryTransport();                      // optional: background worker jobs
builder.Services.AddHealthChecks()
    .AddAsyncResponseRecoveryCheck();              // optional: surface the watchdog on /readyz
```

`AddAsyncResponse()` registers the channel-agnostic engine but **no channel** — chain exactly one
(`.WithInMemoryChannel()` or `.WithRedisChannel()`). An app that starts without a channel fails
fast at host startup, so a misconfiguration can never silently hang every waiter. The recovery
watchdog is part of the engine and runs by default for whichever channel you choose.

### Google Pub/Sub transport

Pub/Sub is a message transport, not a recovery store. Compose it with Core-only waiting for
simple apps, or with Redis/Postgres-style recovery when late responses must survive redeploys.

```csharp
builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()   // or .WithRedisChannel() for durable recovery
    .WithGooglePubSubTransport(options =>
    {
        options.ProjectId = "my-gcp-project";
        options.WorkerTopicId = "asyncresponse-workers";
        options.WorkerSubscriptionId = "asyncresponse-workers-sub";
        options.ResponseTopicId = "asyncresponse-responses";
        options.ResponseSubscriptionId = "asyncresponse-responses-sub";
        options.CorrelationIdAttribute = "correlationId";
    });
```

One `.WithGooglePubSubTransport(...)` wires the worker publisher, the worker-job subscriber, and
the response-ingress subscriber. Pub/Sub is a transport, not a recovery store, so pair it with a
channel: `.WithInMemoryChannel()` for simple apps, or `.WithRedisChannel()` when late responses
must survive redeploys.

The response topic is also exposed as a transport-neutral reply target. Use this when the
remote request needs to know where to publish its eventual response:

```csharp
OrderResult result = await _asyncResponse
    .For<OrderResult>()
    .WithReplyTarget() // default: options.ResponseTopicId
    .WaitAsync(context => _remoteSystem.SubmitAsync(
        orderId,
        correlationId: context.CorrelationId,
        replyProjectId: context.ReplyTarget!.Properties["projectId"],
        replyTopicId: context.ReplyTarget.Properties["topicId"]));
```

For multi-region or multi-tenant routing, register named targets and select one per flow:

```csharp
builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithGooglePubSubTransport(options =>
    {
        options.ProjectId = "my-gcp-project";
        options.WorkerTopicId = "asyncresponse-workers";
        options.WorkerSubscriptionId = "asyncresponse-workers-sub";
        options.ResponseSubscriptionId = "asyncresponse-responses-sub";
        options.AddReplyTarget("regional-us", "my-gcp-project-us", "asyncresponse-responses-us");
    });

var result = await _asyncResponse
    .For<OrderResult>()
    .WithReplyTarget("regional-us")
    .WaitAsync(context => _remoteSystem.SubmitAsync(orderId, context));
```

The hosted response subscriber feeds Pub/Sub messages into the same transport-neutral
`IAsyncResponseIngress` you would call from any broker. It resolves the correlation id from the
configured message attribute first (`correlationId` by default), then falls back to JSON body
paths such as `CorrelationId`, `CustomParameters`, `CustomParameters.CorrelationId`,
`PubSubParams.CustomParameters.CorrelationId`, and `DagJsonParameters.CorrelationId`.

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
    return await _asyncResponse
        .For<OrderResult>()
        .WithTimeout(TimeSpan.FromMinutes(10))
        .Until(r => r.Status != OrderStatus.Processing)
        .WaitAsync(context => _remoteSystem.SubmitAsync(orderId, context.CorrelationId));
}
```

The `WaitAsync` trigger is the race-killer: the request is sent **after** the subscription and
recovery state exist, so the first response can never arrive before anyone is listening, and a
failing trigger tears the registration down (the operation never started). Rule of thumb:
*never send the request yourself — pass the send as the trigger.*

The two builder shapes are **typed for safety** — each terminal offers exactly the actions
that make sense for where the correlation id came from:

- `For<T>()` → `IAsyncResponseTriggeredBuilder<T>` — the builder generates the correlation id
  (also placing it in the ambient `AsyncResponseContext`) and `WaitAsync` *requires* the
  trigger: a generated id is known to nobody else, so waiting without sending could never
  complete, and the type system makes that mistake unrepresentable. The trigger receives an
  `AsyncResponseRequestContext` (correlation id + reply target) — persist `context.CorrelationId`
  into your flow state there (the subscription already exists), then send. Ignore the argument
  (`_ => …`) when the ambient `AsyncResponseContext` is enough.
- `For<T>(correlationId)` → `IAsyncResponseAttachedBuilder<T>` — *attaches* to an operation
  already started elsewhere: a resumed step re-attaching to its in-flight correlation id, or a
  different system owning the send. Its `WaitAsync()` takes *no* trigger — re-sending an
  in-flight operation would double-fire it, so that mistake is unrepresentable too.

Flows that decide fresh-vs-resume at runtime branch between the two chains on their persisted
state — only the flow can know which case applies; the transport cannot detect it.

Need to arm recovery without awaiting in place? Start the wait as a background task
(`_ = builder…WaitAsync(trigger)`): the subscription and the persisted recovery state stay
alive while your code moves on (see the sample's `/demo/lost-subscriber/arm` endpoint).

### 3. Deliver responses (your broker → the ingress)

Wherever responses physically arrive — Google Pub/Sub, RabbitMQ, Kafka, an HTTP webhook — feed
them into the ingress:

```csharp
// e.g. inside your broker consumer:
await ingress.HandleResponseMessageAsync(messageBodyJson, correlationIdFromHeaders);
```

When you use `AsyncResponse.Transports.GooglePubSub`, the hosted response subscriber does this
for you and can extract the correlation id from Pub/Sub attributes or the configured JSON paths.

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
    .For<OrderResult>()
    .Until(r => r.Status != OrderStatus.Processing)
    .OnLostSubscriberResume<IOrderFlow>(flow =>
        flow.ResumeAsync(orderId, Placeholder.Payload<OrderResult>(), Placeholder.CorrelationId()))
    .OnLostSubscriberFailure<IOrderFlow>(flow =>
        flow.FailAsync(Placeholder.Exception(), Placeholder.CorrelationId()))
    .WaitAsync(context => _remoteSystem.SubmitAsync(orderId, context.CorrelationId));
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
the job publishes correlates automatically. `.WithInMemoryTransport()` executes jobs in the
current process; for distributed execution use `.WithGooglePubSubTransport(...)` or implement
`IWorkerTransport` against your broker and have the consumer call
`ingress.HandleWorkerMessageAsync(json)`.

### 6. Propagating ambient context (trace, principal, tenant)

Anything your app keeps in `AsyncLocal` — a trace id, the current principal, a tenant, a logging
scope — is lost when AsyncResponse hands work to a foreign execution context. AsyncResponse carries
it back, choosing the mechanism automatically per boundary:

| Where work resumes | Boundary | How context is carried |
|---|---|---|
| Response handler (Redis & in-memory) — your `Until` predicate, progress handling | in-process thread hop | captured `ExecutionContext` (automatic) |
| In-memory worker job | in-process thread hop | captured `ExecutionContext` (automatic) |
| Broker-backed worker job (e.g. Google Pub/Sub) | serialized, another process | `IAsyncResponseContextPropagator` baggage |
| Lost-subscriber recovery callback (after a redeploy) | serialized, maybe another deployment | `IAsyncResponseContextPropagator` baggage |

**In-process hops need no configuration.** The response handler and the in-memory worker run under
the `ExecutionContext` captured when the wait was armed / the job was enqueued, so ambient
`AsyncLocal` state — `Activity.Current`, the principal, an open logging scope — flows automatically.
This is the classic "restore the trace/principal *inside* the broker callback" chore, handled for you.

**Serialized hops can't carry `AsyncLocal`s**, so register an `IAsyncResponseContextPropagator` to
capture your context into a `string`→`string` bag (persisted with the worker job / recovery state)
and restore it on the far side:

```csharp
public sealed class TracePropagator(ILogger<TracePropagator> logger) : IAsyncResponseContextPropagator
{
    public void Capture(IDictionary<string, string> carrier)
    {
        if (Activity.Current is { } activity) carrier["trace.id"] = activity.TraceId.ToString();
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> carrier)
        => carrier.TryGetValue("trace.id", out var traceId)
            ? logger.BeginScope("traceId:{TraceId}", traceId)!   // the IDisposable restores a logging scope
            : NullScope.Instance;
}

builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithContextPropagator<TracePropagator>()       // register one per concern…
    .WithContextPropagator<PrincipalPropagator>()   // …they compose (each namespaces its own keys)
    .WithInMemoryTransport();
```

The carrier is `string`→`string`, so it survives JSON serialization and a redeploy; the `Restore`
return value is an `IDisposable`, so a propagator can re-establish *behavior* like a logging scope,
not just data — and the library disposes the scopes (in reverse) once processing finishes. With no
propagator registered the feature is a zero-cost no-op and the wire payload is unchanged. See
`SampleTracePropagator`/`SampleTenantPropagator` in the sample for a runnable example spanning all
four boundaries.

### 7. Timeouts, errors, and cancellation

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

### 8. Operations: watchdog + health check

The recovery watchdog is part of the engine: `AddAsyncResponse()` starts it by default, and it
works for whichever channel you registered (in-memory or Redis). It periodically scans the
persisted recovery state and warns about entries that are old and have no live waiter — flows
that are probably stuck. `AddAsyncResponseRecoveryCheck()` surfaces the cached findings on your
health endpoint with stats and the offending correlation ids.

Tune or disable it through `AsyncResponseOptions.Watchdog` — e.g. set `Watchdog.Enabled = false`
in all but one host when several hosts share one Redis, so its scan and warnings aren't
duplicated.

The check reports at most **`Degraded`** — a stuck *business flow* must never pull a healthy
*process* out of rotation, so keep `Degraded → 200` on readiness endpoints (the ASP.NET Core
default).

## Configuration

```csharp
builder.Services.AddAsyncResponse(options =>
{
    options.Watchdog.Interval = TimeSpan.FromHours(6);
    options.Watchdog.StaleAfter = TimeSpan.FromHours(24);
    options.Watchdog.StartupDelay = TimeSpan.FromMinutes(5);
    // options.Watchdog.Enabled = false;                   // e.g. all but one host per Redis
})
.WithRedisChannel(options =>
{
    options.KeyPrefix = "myapp";                            // isolate apps/environments
    options.RecoveryStateExpiry = TimeSpan.FromDays(7);     // how long recovery survives
    options.DefaultTimeout = TimeSpan.FromHours(12);        // default per-waiter timeout
});
```

Tracing: all operations emit `System.Diagnostics.Activity` spans from the `"AsyncResponse"`
`ActivitySource` (`asyncresponse.wait`, `asyncresponse.set_response`, …) — subscribe to it from
OpenTelemetry with `.AddSource("AsyncResponse")`.

## The sample app

A complete testbed lives in [`samples/AsyncResponse.Sample`](samples/AsyncResponse.Sample):

Run it as an Aspire playground when you want the dashboard, managed Redis, logs, traces, metrics,
resource environment, and health checks in one place:

```bash
dotnet run --project samples/AsyncResponse.AppHost
```

In Rider, use the shared run configuration `AsyncResponse.Playground`. It is a small launcher
project that starts the Aspire AppHost without relying on Rider's Aspire/launch-profile project
pickers.

The Aspire AppHost starts a Redis container and the sample API, then opens the Aspire dashboard.
Use the dashboard's `playground` resource to open the API endpoint and inspect `AsyncResponse`
logs/traces. The local playground pins the dashboard to `http://localhost:18888` and uses HTTP
resource/OTLP endpoints to avoid local HTTPS certificate issues. The sample also exposes Aspire
service-default endpoints at `/health` and `/alive`.

Prerequisites: .NET 10 SDK, `dotnet` available on `PATH`, and a supported container runtime
such as Docker or Podman for the Redis resource.

If you want to run the sample without Aspire, start Redis yourself:

```bash
docker compose up -d          # local Redis
dotnet run --project samples/AsyncResponse.Sample
```

Then walk the scenarios:

```bash
curl -X POST 'http://localhost:5000/demo/request-response?behavior=Succeed'              # happy path with progress messages
curl -X POST 'http://localhost:5000/demo/request-response?behavior=FailDomain'           # domain failure seen by the active waiter
curl -X POST 'http://localhost:5000/demo/request-response/reply-target?behavior=Succeed' # same flow, passing explicit reply-to metadata
curl -X POST 'http://localhost:5000/demo/timeout'                                        # 2s timeout vs 15s remote
curl -X POST 'http://localhost:5000/demo/worker?orderId=42'                              # background worker job

# The headline feature — recovery after a "redeploy":
curl -X POST 'http://localhost:5000/demo/lost-subscriber/arm'                            # returns a correlationId
curl -X POST 'http://localhost:5000/demo/lost-subscriber/crash'                          # kills every subscription
curl -X POST 'http://localhost:5000/demo/lost-subscriber/respond?correlationId=<id>&status=Completed' # resume callback
curl -X POST 'http://localhost:5000/demo/lost-subscriber/respond?correlationId=<id>&status=Failed'    # failure callback
curl 'http://localhost:5000/healthz'                                                     # watchdog findings
curl 'http://localhost:5000/health'                                                      # Aspire readiness check
curl 'http://localhost:5000/alive'                                                       # Aspire liveness check
```

For the lost-subscriber flow, copy the `correlationId` returned by `/arm` and replace `<id>` in
one of the `/respond` requests. `Completed` exercises the resume callback; `Failed` exercises
the failure callback with an `AsyncResponseDomainFailureException`.

The sample also wires two context propagators (`SampleTracePropagator`, `SampleTenantPropagator`) —
watch the `traceId`/`tenant` fields in the logs: `/demo/request-response` shows them on `HANDLER:`
lines (flowing into the Redis response handler via `ExecutionContext`), `/demo/worker` shows them on
the `WORKER:` line (in-memory worker, also `ExecutionContext`), and the `/demo/lost-subscriber` flow
shows them on the `RECOVERY:` line — there they survived the simulated crash as serialized baggage
persisted in the recovery state.

## Best practices

1. **Always make the send the trigger** (the `WaitAsync` argument). Sending before subscribing
   is a race: a fast first response finds nobody listening and, on first registration, no
   recovery state either.
2. **Use reply targets for generic response topics.** If the remote system needs reply-to
   metadata, call `.WithReplyTarget()` and pass the `AsyncResponseRequestContext` into the
   trigger. Transport packages own how native destinations become reply targets.
3. **Classify honestly.** `ClassifyOutcome()` must mirror your active waiter's `Until`
   semantics. Map unrecognized states to `Unknown` (fails conservatively) unless your active
   path deliberately keeps waiting on them — then map them to `InProgress`.
4. **Register both recovery callbacks** for any flow that must survive redeploys. A failed
   payload with no failure callback is logged and dropped — never resumed — but dropped is
   still a stuck flow.
5. **Make resume callbacks re-entrant.** A resume may re-trigger a flow whose step is still
   running remotely; resume should *re-attach* (subscribe to the same correlation id) rather
   than re-execute side effects. Persist enough state to tell the difference.
6. **Treat callback method names and the `KeyPrefix` as deployment contracts.** They are
   persisted; rename with a migration window.
7. **Set timeouts per flow.** The 7-day default is a backstop, not a recommendation; a payment
   flow should fail in minutes.
8. **Run the watchdog in exactly one host per Redis** and alert on its warnings or the
   `Degraded` health status — stale recovery state is your earliest signal of stuck flows.
9. **Mind Redis pub/sub semantics.** Delivery is at-most-once to live subscribers; the recovery
   state is what makes the system safe across gaps. Don't disable it (`RecoveryStateExpiry`)
   below your longest flow duration.
10. **One `IConnectionMultiplexer`.** Reuse your application's existing multiplexer; don't create
   a second connection for AsyncResponse.

## Building and testing

```bash
dotnet build
dotnet test            # runs on Microsoft.Testing.Platform (xUnit.net v3)
```

The test project is a Microsoft.Testing.Platform application, so you can also run it directly and
use MTP options — test filtering, a TRX report, and code coverage:

```bash
dotnet run --project tests/AsyncResponse.Tests -f net10.0 -- \
    --filter-namespace AsyncResponse.Tests \
    --report-trx --coverage --results-directory ./TestResults
```

The integration tests in [`tests/AsyncResponse.IntegrationTests`](tests/AsyncResponse.IntegrationTests)
exercise the library end-to-end against **real infrastructure**, orchestrated by **.NET Aspire**: the
test boots the [AppHost](samples/AsyncResponse.AppHost) via `Aspire.Hosting.Testing`, which starts real
Redis and a Google Pub/Sub emulator (containers) plus the system-under-test app (`itest-app`), then
drives every scenario over HTTP. They need a running Docker daemon (and pull a Pub/Sub emulator image
on first run), so they run locally / on demand rather than in CI:

```bash
dotnet run --project tests/AsyncResponse.IntegrationTests
```

The same AppHost doubles as a dashboard playground — `aspire run` (or the command below) shows `redis`,
`pubsub` and `itest-app` with their live logs and traces, so you can poke the SUT's endpoints by hand:

```bash
dotnet run --project samples/AsyncResponse.AppHost
```

## License

[MIT](LICENSE) — © Vitalii Tiunisov
