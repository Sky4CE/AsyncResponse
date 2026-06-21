<h1 align="center">AsyncResponse</h1>

<p align="center">
  <img src="icon.png" alt="AsyncResponse Icon" width="128" />
</p>

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
         │                              │ payload.ShouldResumeOnRecovery()    │
         │   ResumeCallback(payload)  ◄─┤   true → resume                     │
         │   FailureCallback(exception)◄┤   false → fail                      │
```

Three layers, one decision each, made exactly where its deciding fact is knowable:

| Layer | Knowable fact | Decision |
|---|---|---|
| **Ingress** (`IAsyncResponseIngress`) | "Does the message parse?" | Parses → deliver as payload, untyped and uninterpreted. Doesn't parse → report as exception. |
| **Response channel** (`SetResponse`/`SetException`) | "Did any subscriber receive it?" | Delivered → the active waiter's `Until` and flow code interpret it. Nobody listening → hand to the dispatcher. |
| **Lost-subscriber dispatcher** | "Should this late response resume the flow?" | `ShouldResumeOnRecovery()` true → resume callback. false (or unclassifiable) → failure callback. |

A failed payload is **still a valid response** for an active waiter (your `Until` predicate and
flow code want to see it — persist details, decide to retry, throw a rich domain error).
`ShouldResumeOnRecovery()` is consulted only when nobody is listening — which is exactly when
somebody has to make the call.

## Packages

| Package | What's inside |
|---|---|
| `AsyncResponse.Core` | Fluent registration + waiter builder, process-local response channel and recovery store, transport-neutral ingress, outcome classifier, expression-based callbacks, in-memory worker queue, and the recovery watchdog + readiness health check. |
| `AsyncResponse.Channels.Redis` | Optional durable Redis response channel and recovery-state store; the Core watchdog and health check work against it automatically. |
| `AsyncResponse.Transports.GooglePubSub` | Optional Google Pub/Sub worker transport and hosted subscribers for worker jobs and response ingress. |
| `AsyncResponse.Transports.RabbitMQ` | Optional RabbitMQ worker transport and hosted subscribers for worker jobs and response ingress. |
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

# Optional RabbitMQ transport:
dotnet add package AsyncResponse.Transports.RabbitMQ
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
    .WithInMemoryTransport();         // in-process worker transport (required)
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
    .WithInMemoryTransport();                      // in-process worker transport
builder.Services.AddHealthChecks()
    .AddAsyncResponseRecoveryCheck();              // optional: surface the watchdog on /readyz
```

`AddAsyncResponse()` registers the channel-agnostic engine but **no channel or transport** — chain
exactly one channel (`.WithInMemoryChannel()` or `.WithRedisChannel()`) and exactly one transport
(`.WithInMemoryTransport()`, `.WithGooglePubSubTransport(...)`, `.WithRabbitMqTransport(...)`, or another full AsyncResponse
transport package). An app that starts without either one fails fast at host startup with setup
guidance, so a misconfiguration can never silently hang every waiter or drop worker dispatch. The
recovery watchdog is part of the engine and runs by default for whichever channel you choose.

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

By default, both hosted subscribers ACK only after their handler completes. That preserves Pub/Sub
redelivery when the handler throws. For worker jobs that can run longer than the Pub/Sub ack window,
you can explicitly opt the worker subscription into ACK-after-enqueue behavior:

```csharp
builder.Services.Configure<HostOptions>(host =>
{
    host.ShutdownTimeout = TimeSpan.FromMinutes(3);
});

builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithGooglePubSubTransport(options =>
    {
        options.ProjectId = "my-gcp-project";
        options.WorkerTopicId = "asyncresponse-workers";
        options.WorkerSubscriptionId = "asyncresponse-workers-sub";
        options.ResponseTopicId = "asyncresponse-responses";
        options.ResponseSubscriptionId = "asyncresponse-responses-sub";

        options.HostShutdownTimeout = TimeSpan.FromMinutes(3);
        options.WorkerSubscriber.UseAckAfterEnqueue(
            backgroundWorkerCount: 64,
            backgroundQueueCapacity: 10_000,
            backgroundDrainTimeout: TimeSpan.FromMinutes(2));
        options.WorkerSubscriber.OnBackgroundFailure = context =>
        {
            // Alert, increment a metric, or publish context.Message to a durable failure path.
            return ValueTask.CompletedTask;
        };
    });
```

This is intentionally opt-in. With `AckAfterEnqueue`, Pub/Sub is ACKed once the message is accepted
into a bounded in-process queue; if the process dies after that point, Pub/Sub will not redeliver
that message. If the queue is full or draining, the subscriber returns NACK so Pub/Sub can retry.
If a background handler fails after ACK, the failure is logged and reported through
`OnBackgroundFailure`; use that hook for operator-visible metrics, alerting, or a durable
dead-letter path. The transport validates that `ShutdownTimeout + BackgroundDrainTimeout` fits
inside `HostShutdownTimeout` (30 seconds by default, matching the Generic Host default), so mirror
any custom `HostOptions.ShutdownTimeout` value in the transport options. With more than one
background worker, handling is concurrent and message ordering is not preserved; use one worker for
order-sensitive subscriptions or Pub/Sub ordering-key workflows.
Keep response ingress on the default `AckAfterHandlerCompletes` unless response processing is also
durable enough to tolerate early ACK.

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

### RabbitMQ transport

RabbitMQ is also a message transport, not a recovery store. Compose it with Core-only waiting for
simple apps, or with Redis/Postgres-style recovery when late responses must survive redeploys.

```csharp
builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(options =>
    {
        options.ConnectionString = "amqp://guest:guest@localhost:5672/";
        options.WorkerExchange = "asyncresponse.worker";
        options.WorkerQueue = "asyncresponse.worker";
        options.WorkerRoutingKey = "asyncresponse.worker";
        options.ResponseExchange = "asyncresponse.response";
        options.ResponseQueue = "asyncresponse.response";
        options.ResponseRoutingKey = "asyncresponse.response";
        options.CorrelationIdHeader = "correlationId";
    });
```

One `.WithRabbitMqTransport(...)` wires the worker publisher, the worker-job subscriber, the
response-ingress subscriber, RabbitMQ topology declaration, reply-target support, and shutdown
validation. By default it declares durable direct exchanges, durable queues, and bindings for the
configured worker and response paths before publishing or consuming. Set
`DeclareTopology = false` if your infrastructure team owns topology creation.

RabbitMQ ACK behavior mirrors the Pub/Sub transport. The default is
`AckAfterHandlerCompletes`, which ACKs only after AsyncResponse handling succeeds and NACKs with
requeue if the handler throws. For worker jobs that must be accepted quickly into the process, opt
in explicitly:

```csharp
builder.Services.Configure<HostOptions>(host =>
{
    host.ShutdownTimeout = TimeSpan.FromMinutes(3);
});

builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(options =>
    {
        options.ConnectionString = "amqp://guest:guest@localhost:5672/";
        options.HostShutdownTimeout = TimeSpan.FromMinutes(3);
        options.WorkerSubscriber.UseAckAfterEnqueue(
            backgroundWorkerCount: 64,
            backgroundQueueCapacity: 10_000,
            backgroundDrainTimeout: TimeSpan.FromMinutes(2));
        options.WorkerSubscriber.OnBackgroundFailure = context =>
        {
            // Alert, increment a metric, or publish to a durable failure path.
            return ValueTask.CompletedTask;
        };
    });
```

With `AckAfterEnqueue`, RabbitMQ is ACKed once the delivery is accepted into a bounded in-process
queue. If the process dies after that point, RabbitMQ will not redeliver that message. If the
queue is full, the subscriber NACKs with requeue. If a background handler fails after ACK, the
failure is logged and reported through `OnBackgroundFailure`. The transport validates that
`ShutdownTimeout + BackgroundDrainTimeout` fits inside `HostShutdownTimeout`, so mirror any custom
`HostOptions.ShutdownTimeout` value in the transport options. Keep response ingress on the default
`AckAfterHandlerCompletes` unless response processing is durable enough to tolerate early ACK.

The RabbitMQ response exchange/routing key are exposed as a transport-neutral reply target:

```csharp
OrderResult result = await _asyncResponse
    .For<OrderResult>()
    .WithReplyTarget()
    .WaitAsync(context => _remoteSystem.SubmitAsync(
        orderId,
        correlationId: context.CorrelationId,
        replyExchange: context.ReplyTarget!.Properties["exchange"],
        replyRoutingKey: context.ReplyTarget.Properties["routingKey"]));
```

The hosted response subscriber feeds RabbitMQ deliveries into `IAsyncResponseIngress`. It resolves
the correlation id from AMQP `CorrelationId` first, then from the configured message header
(`correlationId` by default), then from the same JSON body paths used by the Pub/Sub transport.

### 1. Define a payload

Every payload implements `IAsyncResponsePayload` — a marker that also keeps scalars (`string`,
`int`, …) out of `For<T>()`. Most payloads need nothing more:

```csharp
public sealed class OrderResult : IAsyncResponsePayload
{
    public OrderStatus Status { get; set; }
    public string? Message { get; set; }
}
```

Override `ShouldResumeOnRecovery()` only when a payload can carry a *domain failure* and you use
lost-subscriber recovery. It answers one question — *should a late response of this type resume
the flow, or fail it?* — and is consulted **only** on the recovery path, never for live
completion (which your `Until` predicate owns):

```csharp
public sealed class OrderResult : IAsyncResponsePayload
{
    public OrderStatus Status { get; set; }
    public string? Message { get; set; }

    // recovery routing only: fail on a failed result, resume otherwise.
    public bool ShouldResumeOnRecovery() => Status != OrderStatus.Failed;
}
```

The default returns `false` (don't resume), so a failed response can never resume the happy path
by omission. Durable channels (Redis) require this override when you register recovery callbacks —
they fail fast if it's missing; the in-memory channel, which can't survive a redeploy, doesn't.

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

```csharp
// A resumed step re-attaching to its in-flight correlation id — no trigger, nothing to re-send:
OrderResult result = await _asyncResponse
    .For<OrderResult>(persistedCorrelationId)     // attach mode (IAsyncResponseAttachedBuilder<T>)
    .WithTimeout(TimeSpan.FromMinutes(10))
    .Until(r => r.Status != OrderStatus.Processing)
    .WaitAsync();                                  // wait-only terminal
```

Need to arm recovery without awaiting in place? Start the wait as a background task
(`_ = builder…WaitAsync(trigger)`): the subscription and the persisted recovery state stay
alive while your code moves on (see the sample's `/arm` endpoint).

### 3. Deliver responses (your broker → the ingress)

Wherever responses physically arrive — Google Pub/Sub, RabbitMQ, Kafka, an HTTP webhook — feed
them into the ingress:

```csharp
// e.g. inside your broker consumer:
await ingress.HandleResponseMessageAsync(messageBodyJson, correlationIdFromHeaders);
```

When you use `AsyncResponse.Transports.GooglePubSub` or `AsyncResponse.Transports.RabbitMQ`, the
hosted response subscriber does this for you and can extract the correlation id from broker
metadata or the configured JSON paths.

In-process publishers can skip the ingress and call `IAsyncResponsePublisher` directly with
payload types that implement `IAsyncResponsePayload`. Raw broker/webhook JSON should stay on
`IAsyncResponseIngress`, which keeps it untyped until the channel can route it:

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

> ⚠️ **Naming contract:** callback targets are persisted as interface/method *name strings* and
> live in Redis for up to `RecoveryStateExpiry`. Renaming a registered callback method is a
> breaking change for in-flight recovery state — deploy renames with care (keep a forwarding
> method for one expiry window).

### 5. Fire-and-forget background workers

```csharp
await _asyncResponse.EnqueueWorkerAsync<IOrderFlow>(flow => flow.ProcessOrderAsync(orderId));
```

The ambient correlation id is captured with the job and restored before execution, so anything
the job publishes correlates automatically. Transport is an explicit host-level choice:
`.WithInMemoryTransport()` executes jobs in the current process; for distributed execution use
`.WithGooglePubSubTransport(...)`, `.WithRabbitMqTransport(...)`, or another full AsyncResponse
transport package.

A transport package is more than a publish-side adapter: it owns the worker publisher, any hosted
subscribers, response ingress, reply-target support, options validation, and shutdown behavior that
the broker needs. New transports should expose a complete fluent extension such as
`.WithRabbitMqTransport(...)` rather than asking application hosts to raw-register
`IWorkerTransport`.

```csharp
builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(options =>
    {
        options.ConnectionString = "...";
        options.WorkerQueue = "asyncresponse-workers";
        options.ResponseQueue = "asyncresponse-responses";
    });
```

### 6. Propagating ambient context (trace, principal, tenant)

Anything your app keeps in `AsyncLocal` — a trace id, the current principal, a tenant, a logging
scope — is lost when AsyncResponse hands work to a foreign execution context. AsyncResponse carries
it back, choosing the mechanism automatically per boundary:

| Where work resumes | Boundary | How context is carried |
|---|---|---|
| Response handler (Redis & in-memory) — your `Until` predicate, progress handling | in-process thread hop | captured `ExecutionContext` (automatic) |
| In-memory worker job | in-process thread hop | captured `ExecutionContext` (automatic) |
| Broker-backed worker job (e.g. Google Pub/Sub or RabbitMQ) | serialized, another process | `IAsyncResponseContextPropagator` baggage |
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
})
.WithInMemoryTransport();                                   // or .WithGooglePubSubTransport(...) / .WithRabbitMqTransport(...)
```

Tracing: AsyncResponse emits `System.Diagnostics.Activity` spans from one source,
`AsyncResponseDiagnostics.ActivitySourceName` (`"AsyncResponse"`). The library does not take an
OpenTelemetry dependency; your host connects that source to OpenTelemetry, Datadog, or any other
`ActivitySource` listener:

```csharp
using AsyncResponse;
using OpenTelemetry.Trace;

builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddSource(AsyncResponseDiagnostics.ActivitySourceName)
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation());
```

Spans cover the whole library path, not only Redis:

| Span | What it represents |
|---|---|
| `asyncresponse.wait` | active waiter lifetime, including timeout/fault status |
| `asyncresponse.set_response`, `asyncresponse.set_exception` | publishing a response or exception through the configured channel |
| `asyncresponse.ingress.response`, `asyncresponse.ingress.worker` | transport-neutral response and worker message ingress |
| `asyncresponse.enqueue_worker`, `asyncresponse.worker.publish`, `asyncresponse.worker.execute` | worker enqueue, transport publish, and execution |
| `asyncresponse.pubsub.receive` | Google Pub/Sub subscriber message handling |
| `asyncresponse.rabbitmq.receive` | RabbitMQ subscriber message handling |
| `asyncresponse.lost_subscriber.dispatch` | recovery callback routing when no waiter is alive |
| `asyncresponse.watchdog.scan` | recovery watchdog scans |

Common tags include `asyncresponse.correlation_id`, `asyncresponse.channel`,
`asyncresponse.transport`, `asyncresponse.payload_type`, `asyncresponse.subscribers`,
`asyncresponse.lost_subscriber_route`, and worker/reply-target details.

## The sample app

A complete testbed lives in [`samples/AsyncResponse.Sample`](samples/AsyncResponse.Sample):

Run it as an Aspire playground when you want the dashboard, managed Redis, logs, traces, metrics,
resource environment, and health checks in one place:

```bash
dotnet run --project samples/AsyncResponse.AppHost
```

The sample AppHost starts Redis and RabbitMQ containers plus the sample API, then opens the Aspire dashboard.
Use the dashboard's `playground` resource to open the API endpoint and inspect `AsyncResponse`
logs/traces. The local playground pins the dashboard to `http://localhost:18888` and uses HTTP
resource/OTLP endpoints to avoid local HTTPS certificate issues. The sample also exposes Aspire
service-default endpoints at `/health` and `/alive`.

Prerequisites: .NET 10 SDK, `dotnet` available on `PATH`, and a supported container runtime
such as Docker or Podman for the Redis and RabbitMQ resources.

The sample is **configuration-driven**: `AsyncResponse:Channel` (`InMemory` | `Redis`) and
`AsyncResponse:Transport` (`InMemory` | `GooglePubSub` | `RabbitMQ`) select the providers,
defaulting to fully in-memory — so it runs standalone with **no external dependencies**:

```bash
dotnet run --project samples/AsyncResponse.Sample      # in-memory channel + in-memory worker transport
```

The durable lost-subscriber recovery flow needs a real channel — point the sample at Redis for it:

```bash
docker compose up -d                                                  # local Redis
AsyncResponse__Channel=Redis dotnet run --project samples/AsyncResponse.Sample
```

To run the standalone sample against a local RabbitMQ broker, point the transport at an AMQP
connection string:

```bash
docker compose up -d                                                  # local Redis
docker run -d --rm --name asyncresponse-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management
AsyncResponse__Channel=Redis \
AsyncResponse__Transport=RabbitMQ \
RabbitMQ__ConnectionString=amqp://guest:guest@localhost:5672/ \
dotnet run --project samples/AsyncResponse.Sample
```

Then walk the scenarios (the same HTTP endpoints the integration tests drive):

```bash
curl -X POST 'http://localhost:5000/request-response?behavior=Succeed'      # happy path with progress messages
curl -X POST 'http://localhost:5000/request-response?behavior=FailDomain'   # domain failure seen by the active waiter
curl -X POST 'http://localhost:5000/request-response?behavior=Fail'         # technical failure (SetException)
curl -X POST 'http://localhost:5000/request-response?behavior=Timeout'      # 2s timeout vs a slow remote
curl -X POST 'http://localhost:5000/attach'                                 # attach to an in-flight op by correlation id
curl -X POST 'http://localhost:5000/multi-step?first=Succeed&second=Succeed' # sequential two-step flow
curl -X POST 'http://localhost:5000/multi-step?first=Succeed&second=Fail'    # step 2 fails through SetException
curl -X POST 'http://localhost:5000/ambient-exception?message=boom'          # SetException uses ambient correlation id
curl -X POST 'http://localhost:5000/shared-correlation-exception?message=boom' # one exception faults two waiters
curl -X POST 'http://localhost:5000/worker?token=order-42'                  # fire-and-forget background worker job
curl -X POST 'http://localhost:5000/emit-response?correlationId=<id>&status=Completed&useAttribute=true' # broker response ingress
curl      'http://localhost:5000/healthz'                                   # recovery watchdog findings
curl      'http://localhost:5000/alive'                                     # liveness check

# Recovery after a "redeploy" (needs the Redis channel):
curl -X POST 'http://localhost:5000/arm'                                          # returns a correlationId
curl -X POST 'http://localhost:5000/crash'                                        # drops every subscription
curl -X POST 'http://localhost:5000/publish?correlationId=<id>&status=Completed'  # → resume callback
curl -X POST 'http://localhost:5000/publish?correlationId=<id>&status=Failed'     # → failure callback
curl -X POST 'http://localhost:5000/publish?correlationId=<id>&exception=boom'    # → failure callback via SetException

# Same recovery flow, composed into one endpoint:
curl -X POST 'http://localhost:5000/lost-subscriber-flow?outcome=Completed'        # arm + drop this channel + late success → resume
curl -X POST 'http://localhost:5000/lost-subscriber-flow?outcome=Failed'           # arm + drop this channel + late failed payload → fail
curl -X POST 'http://localhost:5000/lost-subscriber-flow?outcome=Exception'        # arm + drop this channel + late SetException → fail
```

For the lost-subscriber flow, copy the `correlationId` returned by `/arm` and replace `<id>` in a
`/publish` request. `Completed` exercises the resume callback; `Failed` exercises the failure
callback with an `AsyncResponseDomainFailureException`; `exception=...` exercises the technical
failure path through `IAsyncResponsePublisher.SetException`. (`/arm`, `/crash`, `/publish`, and
`/lost-subscriber-flow` require the Redis channel — run with `AsyncResponse__Channel=Redis`.)
`/crash` is intentionally a blunt manual demo that drops all Redis subscriptions, while
`/lost-subscriber-flow` drops only the correlation id it just armed so load tests can run many
recovery flows concurrently without disturbing each other.

`/shared-correlation-exception` demonstrates fan-out: two waiters attach to the same correlation
id, then one `SetException` faults both. This works with the in-memory and Redis channels; Redis may
multiplex local handlers through one server-side subscription, so the sample waits for both waiter
registrations directly rather than relying on Redis subscriber counts.

The sample also wires two context propagators (`SampleTracePropagator`, `SampleTenantPropagator`) —
watch the `traceId`/`tenant` fields in the logs: `/request-response` shows them on `HANDLER:` lines
(flowing into the response handler via `ExecutionContext`), `/worker` shows them on the `WORKER:`
line (the in-memory worker, also `ExecutionContext`), and the `/arm`→`/crash`→`/publish` flow shows
them on the `RECOVERY:` line — there they survived the simulated crash as serialized baggage
persisted in the recovery state.

## Best practices

1. **Always make the send the trigger** (the `WaitAsync` argument). Sending before subscribing
   is a race: a fast first response finds nobody listening and, on first registration, no
   recovery state either.
2. **Use reply targets for generic response topics.** If the remote system needs reply-to
   metadata, call `.WithReplyTarget()` and pass the `AsyncResponseRequestContext` into the
   trigger. Transport packages own how native destinations become reply targets.
3. **Decide recovery routing honestly.** Override `ShouldResumeOnRecovery()` for any payload that
   can carry a domain failure on a durable channel, returning `false` for the states that must not
   resume. It's independent of your `Until` predicate (which owns live completion) — they answer
   different questions: "is it a failure?" versus "is the operation done?".
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
11. **Measure hot paths in isolation before comparing profiles.** The sample's remote simulator
    deliberately waits before progress and terminal messages, so broad HTTP load-test latency mostly
    reflects sample workflow timing. Use the micro-benchmarks, stress harness, and NBomber
    `--scenario` filter to separate library overhead from demo behavior.

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
exercise the library end-to-end, driving the **sample app itself** as the system under test (one app —
no separate fixture app to keep in sync). They run at two levels:

- **In-process, no Docker** — `WebApplicationFactory` boots the sample on the fully in-memory channel
  and transport, covering the core request/response, attach, worker, and concurrency paths. They need
  no containers, so they stay fast and reliable even where Docker is unavailable.
- **Aspire-orchestrated, Docker** — `Aspire.Hosting.Testing` boots an AppHost that starts real Redis,
  a Google Pub/Sub emulator container, RabbitMQ, and four sample-app SUTs: Pub/Sub default ACK,
  Pub/Sub worker `AckAfterEnqueue`, RabbitMQ default ACK, and RabbitMQ worker `AckAfterEnqueue`.
  Tests drive the Redis-channel, Pub/Sub, and RabbitMQ scenarios over HTTP. They need a running
  Docker daemon (and pull broker images on first run), so CI runs them in a separate Docker-backed
  `integration-tests` job:

```bash
dotnet run --project tests/AsyncResponse.IntegrationTests
```

In Rider, use the Unit Tests window or gutter icons to run/debug individual unit or integration
tests. Aspire is not a test explorer here; it is only the infrastructure harness that the integration
fixture starts for you.

## Benchmarking and load testing

[`benchmarks/AsyncResponse.Benchmarks`](benchmarks/AsyncResponse.Benchmarks) is a console app with two
modes — micro-benchmarks (BenchmarkDotNet) and an in-process load/stress harness. Run both from a
**Release** build.

**Benchmarks** — per-operation latency, allocations, and GC for the hot paths (in-memory
request/response round-trip, raw broker ingress, shared-correlation fanout, exception fanout,
recovery-state save/scan, watchdog/health evaluation, context propagation, envelope
(de)serialization, payload classification, expression→callback conversion, reflection invoke, and
Google Pub/Sub/RabbitMQ subscriber ACK dispatch modes).
`[MemoryDiagnoser]` reports allocated bytes and Gen0/1/2 collections per op alongside
mean/median/percentile timings:

```bash
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks                 # all benchmarks
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*Channel*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*Ingress*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*GooglePubSubAckDispatch*'
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- --filter '*RabbitMqAckDispatch*'
```

**Load / stress** — high-concurrency scenarios that *assert* correctness under contention (no
lost/crossed responses, no duplicate worker executions, no cleanup leaks, no context bleed, no hangs)
and report throughput, latency percentiles, allocations, GC counts, and working set. The process exits
non-zero if any correctness check fails, so it doubles as a soak gate:

```bash
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- stress
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- stress --concurrency 512 --count 200000 --progress 5
dotnet run -c Release --project benchmarks/AsyncResponse.Benchmarks -- stress --fanout 8 --timeout-count 5000 --timeout-ms 50
```

The stress harness now checks the system from multiple angles: **waiter-storm** (N concurrent waiters,
each must receive exactly its own response — no cross-correlation leakage), **progress-storm** (a burst
of progress messages then a terminal per flow), **worker-storm** (N fire-and-forget jobs, each executed
exactly once), **google-pubsub-ack-after-enqueue-dispatch-storm** (bounded early-ACK dispatcher:
every ACKed message must be processed once), **race-burst** (subscribe-before-send under contention),
**raw-ingress-storm** (broker JSON into typed waiters), **shared-response-fanout** and
**exception-fanout** (many waiters on one correlation id), **timeout-storm** and
**dispose-cleanup-storm** (subscription/recovery cleanup), **context-isolation-storm** (captured
`ExecutionContext` under foreign publishers), and
**watchdog-scan-storm** (scanner + active-subscriber probe + stale evaluation). The same invariants are
gated on every CI run, at smaller scale, by
[`ConcurrencyTests`](tests/AsyncResponse.Tests/ConcurrencyTests.cs) in the unit suite. Both tiers run
the in-memory channel and transport in-process.

**End-to-end load (NBomber).** [`benchmarks/AsyncResponse.LoadTests`](benchmarks/AsyncResponse.LoadTests)
drives the sample app's HTTP endpoints with [NBomber v4](https://nbomber.com) over the **real** stack —
Redis channel + broker transports — reporting throughput, latency percentiles, and failures per
scenario. By default it boots Redis + a Pub/Sub emulator + RabbitMQ + four SUTs via Aspire (Docker
required): default/early-ACK Pub/Sub apps and default/early-ACK RabbitMQ apps. Pass `--url` to load an
already-running default instance, `--early-ack-url` for the Pub/Sub early-ACK target, and
`--rabbitmq-url` / `--rabbitmq-early-ack-url` for RabbitMQ targets. Profiles let you choose the scenario set:
`broad` (default, non-destructive request/response, attach, observed worker, multi-step, ambient
exception, shared exception, reply target), `pubsub` (default worker dispatch, response-topic ingress
with attribute/body correlation ids, and early-ACK worker dispatch when an early target is available),
`rabbitmq` (default worker dispatch, response-queue ingress with header/body correlation ids, reply
target, and early-ACK worker dispatch when an early target is available), or `recovery`
(lost-subscriber resume/failure/exception and stale health). Run the
recovery profile separately because it intentionally simulates subscriber loss:

```bash
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile broad --rate 20 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile pubsub --rate 10 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile rabbitmq --rate 10 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile recovery --rate 5 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile broad --scenario request_response_success_redis --rate 20 --duration 60
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --url http://localhost:5000
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --url http://localhost:5000 --early-ack-url http://localhost:5001 --profile pubsub
dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --rabbitmq-url http://localhost:5002 --rabbitmq-early-ack-url http://localhost:5003 --profile rabbitmq
```

Use `--scenario name` (or a comma-separated list) when you want a cleaner single-scenario baseline;
the mixed profiles are better at finding interference between flows. The sample Pub/Sub emit endpoint
reuses its publisher client, while RabbitMQ response emits open a short-lived AMQP channel per request
to model an external producer. It writes an HTML/CSV/Markdown report to `nbomber-report/`.
The [load-test workflow](.github/workflows/loadtest.yml) runs it on every push to `main` (and on demand),
publishing per-scenario throughput and latency to the **same dashboard** as the benchmarks and
uploading the full report as an artifact. Manual workflow runs can switch `profile`, `rate`, and
`duration`; the pushed JSON still uses github-action-benchmark's `customBiggerIsBetter` and
`customSmallerIsBetter` formats, so new scenario series appear automatically under `dev/bench` on
`gh-pages`.

**Performance over time.** Every push to `main` runs the micro-benchmarks and the stress harness
([`benchmarks.yml`](.github/workflows/benchmarks.yml)) and publishes them with
[github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark) as
interactive, per-commit charts: micro-benchmark timings & allocations, the in-process stress suites,
and — from the load-test workflow — end-to-end throughput & latency over the real Redis/broker stack:

**📈 [Benchmark dashboard](https://sky4ce.github.io/AsyncResponse/dev/bench/)**

A change that moves a number stands out immediately; a regression beyond the alert threshold is posted
as a comment on the offending commit, and every run prints a results table to its
[workflow summary](https://github.com/Sky4CE/AsyncResponse/actions/workflows/benchmarks.yml). The
numbers come from shared CI runners, so read them as **trends** rather than absolute hardware figures —
run the benchmarks locally (above) for stable measurements.

> The dashboard goes live after the workflow's first run on `main`, once GitHub Pages is enabled for
> the `gh-pages` branch (Settings → Pages → Branch: `gh-pages`).

## License

[MIT](LICENSE) — © Vitalii Tiunisov
