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

You call a remote system (another service, an Airflow DAG, a payment gateway, a long-running job)
and the answer comes back **later, on a different channel** — a broker topic, a webhook, a callback
queue. Correlating that answer back to the code that asked for it usually means hand-rolled
`TaskCompletionSource` registries, polling loops, or callback spaghetti.

And then the hard part: **your service redeploys while it's waiting.** The in-memory waiter is gone.
The response arrives anyway. If you drop it, the flow hangs "in progress" forever; if you blindly
resume it, you just resumed the **happy path on a failed response**.

AsyncResponse solves the core correlation problem without requiring infrastructure, and lets you add
durable recovery when the flow needs it:

1. **Correlation** — `await` a response by correlation id, with fluent timeouts, progress predicates,
   and race-free triggering. `AsyncResponse.Core` ships a process-local response channel for simple
   apps and tests.
2. **Recovery** — every wait records *recovery state* for cleanup and watchdog visibility; durable
   channels persist it beyond the process. A response that arrives after the waiter died is
   **classified by its domain outcome** and routed to the right callback: resume the flow, or fail it
   — never resume a failure.

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
         │   …and when a redeploy killed the waiter before step 3:            │
         │                              │◄──────── terminal message ──────────┤
         │   ResumeCallback(payload)  ◄─┤ nobody listening →                  │
         │   FailureCallback(exception)◄┤ payload.ShouldResumeOnRecovery()    │
```

Three layers, one decision each, made exactly where its deciding fact is knowable:

| Layer | Knowable fact | Decision |
|---|---|---|
| **Ingress** (`IAsyncResponseIngress`) | "Does the message parse?" | Parses → deliver as payload, untyped and uninterpreted. Doesn't parse → report as exception. |
| **Response channel** (`SetResponse`/`SetException`) | "Did any subscriber receive it?" | Delivered → the active waiter's `Until` and flow code interpret it. Nobody listening → hand to the dispatcher. |
| **Lost-subscriber dispatcher** | "Should this late response resume the flow?" | `ShouldResumeOnRecovery()` true → resume callback. false (or unclassifiable) → failure callback. |

A failed payload is **still a valid response** for an active waiter (your `Until` predicate and flow
code want to see it — persist details, decide to retry, throw a rich domain error).
`ShouldResumeOnRecovery()` is consulted only when nobody is listening — which is exactly when
somebody has to make the call. See [docs/recovery.md](docs/recovery.md) for the full model.

## Packages

| Package | What's inside |
|---|---|
| `AsyncResponse.Core` | Fluent registration + waiter builder, process-local response channel and recovery store, transport-neutral ingress, outcome classifier, expression-based callbacks, in-memory worker queue, and the recovery watchdog + readiness health check. |
| `AsyncResponse.Channels.Redis` | Optional durable Redis response channel and recovery-state store; the Core watchdog and health check work against it automatically. |
| `AsyncResponse.Channels.NATS` | Optional NATS response channel (Core request/reply) and durable JetStream Key-Value recovery-state store; the Core watchdog and health check work against it automatically. |
| `AsyncResponse.Channels.PostgreSQL` | Optional PostgreSQL response channel using `LISTEN/NOTIFY` plus durable recovery-state tables; the Core watchdog and health check work against it automatically. |
| `AsyncResponse.Transports.Redis` | Optional Redis Streams worker transport and hosted subscribers for worker jobs and response ingress, with consumer-group ACKs, pending-entry retry, and dead-lettering. |
| `AsyncResponse.Transports.GooglePubSub` | Optional Google Pub/Sub worker transport and hosted subscribers for worker jobs and response ingress. |
| `AsyncResponse.Transports.RabbitMQ` | Optional RabbitMQ worker transport and hosted subscribers for worker jobs and response ingress. |
| `AsyncResponse.Transports.NATS` | Optional NATS JetStream worker transport and hosted subscribers for worker jobs and response ingress, with explicit ACKs, bounded redelivery, and dead-lettering. |
| `AsyncResponse.Transports.PostgreSQL` | Optional PostgreSQL queue-table worker transport and hosted response-ingress subscribers, with `FOR UPDATE SKIP LOCKED`, bounded redelivery, and dead-lettering. |
| `AsyncResponse.Abstractions` | Contracts only — reference from class libraries that define payloads or flows. |

Package naming follows the extension point: `AsyncResponse.Channels.*` packages provide
response/recovery channels; `AsyncResponse.Transports.*` packages provide broker transports for
workers and ingress.

## Installation

```bash
dotnet add package AsyncResponse.Core

# Optional durable channels:
dotnet add package AsyncResponse.Channels.Redis
dotnet add package AsyncResponse.Channels.NATS
dotnet add package AsyncResponse.Channels.PostgreSQL

# Optional transports:
dotnet add package AsyncResponse.Transports.Redis
dotnet add package AsyncResponse.Transports.GooglePubSub
dotnet add package AsyncResponse.Transports.RabbitMQ
dotnet add package AsyncResponse.Transports.NATS
dotnet add package AsyncResponse.Transports.PostgreSQL
```

## Quick start

`AddAsyncResponse()` registers the channel-agnostic engine but **no channel or transport** — chain
exactly one channel and exactly one transport. An app that starts without either fails fast at host
startup, so a misconfiguration can never silently hang every waiter or drop worker dispatch.

### Core-only, no external dependencies

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

Use this when waiters may die during redeploys and late responses must resume/fail the owning flow
durably.

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

`.WithRedisChannel()` also registers `IRecoverableAsyncResponseBuilder`. Use ordinary
`IAsyncResponseBuilder` for normal request/response waits; inject `IRecoverableAsyncResponseBuilder`
only in flows that register lost-subscriber callbacks (the `OnLostSubscriber*` methods are absent on
the in-memory channel at compile time). See [docs/recovery.md](docs/recovery.md).

### PostgreSQL-backed channel and transport

Use this when PostgreSQL is already your durable infrastructure and you do not want to add Redis just
for async-response recovery. The channel stores response envelopes in a PostgreSQL table and wakes
waiters with `LISTEN/NOTIFY`; notifications carry only a message id, so large payloads are not limited
by PostgreSQL's NOTIFY payload cap. Recovery state is row-per-waiter registration. The transport uses
one queue table for worker, response-ingress, and dead-letter rows, claiming work with
`FOR UPDATE SKIP LOCKED`.

```csharp
using AsyncResponse;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

// The channel/transport are table-backed and chatty, so tune the pooled connection string:
//   No Reset On Close=true  — skip the per-checkin DISCARD ALL (otherwise the single most-executed
//                             statement under load) and keep auto-prepared statements alive across reuse.
//   Max Auto Prepare=20     — auto-prepare the recurring queries, cutting parse/plan CPU on the server.
// Both roughly halve server-side work under load. (LISTEN runs only on dedicated long-lived
// connections, so skipping reset on the pooled query connections is safe.)
builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(
    builder.Configuration.GetConnectionString("PostgreSQL")! + ";No Reset On Close=true;Max Auto Prepare=20"));

builder.Services.AddAsyncResponse()
    .WithPostgreSqlChannel(options =>
    {
        options.SchemaName = "public";
        options.RecoveryStateTable = "asyncresponse_recovery_state";
        options.MessageTable = "asyncresponse_channel_messages";
        options.SubscriberTable = "asyncresponse_channel_subscribers";
        options.NotificationChannel = "asyncresponse_channel_notify";
    })
    .WithPostgreSqlTransport(options =>
    {
        options.MessageTable = "asyncresponse_transport_messages";
        options.WorkerQueue = "worker";
        options.ResponseQueue = "response";
        options.DeadLetterQueue = "deadletter";
        options.WorkerSubscriber.UseAckAfterReceive(
            backgroundWorkerCount: 4,
            backgroundQueueCapacity: 256);
    });
```

The two packages share the same `NpgsqlDataSource` but use separate table sets:

| Area | Storage | Runtime behavior |
|---|---|---|
| Channel messages | `MessageTable` (`asyncresponse_channel_messages`) | Inserts a response envelope, sends a small `NOTIFY`, then waits for live delivery confirmation. If no waiter confirms before `DeliveryConfirmationTimeout`, the row is atomically claimed for lost-subscriber recovery. |
| Recovery state | `RecoveryStateTable` (`asyncresponse_recovery_state`) | One row per waiter registration, so shared correlation ids recover every registered callback instead of only one. |
| Live subscribers | `SubscriberTable` (`asyncresponse_channel_subscribers`) | Short-lived heartbeat rows let publishers distinguish "no subscribers" from "subscriber should confirm delivery". |
| Transport queue | `MessageTable` (`asyncresponse_transport_messages`) | Worker and response-ingress rows are claimed with `FOR UPDATE SKIP LOCKED`; failed rows are redelivered or moved to `DeadLetterQueue`. |

Set `AutoCreateSchema = false` on either options object when migrations provision the schema. Channel
and transport schema creation take the same PostgreSQL advisory lock for a shared schema, avoiding the
`CREATE SCHEMA IF NOT EXISTS` race that can otherwise appear when multiple app instances start at
once.

Key channel options: `SchemaName`, `RecoveryStateTable`, `MessageTable`, `SubscriberTable`,
`NotificationChannel`, `RecoveryStateExpiry`, `DefaultTimeout`, `MessageRetention`,
`DeliveryConfirmationTimeout`, `DeliveryConfirmationPollInterval`, `ListenerPollInterval`,
`PendingMessageBatchSize`, `SubscriberHeartbeatInterval`, `SubscriberHeartbeatTimeout`,
`PublishMaxAttempts`, and `PruneInterval`.

Key transport options: `SchemaName`, `MessageTable`, `NotificationChannel`, `WorkerQueue`,
`ResponseQueue`, `DeadLetterQueue`, `LockTimeout`, `WorkerSubscriber`, `ResponseSubscriber`,
`DeadLetterEnabled`, `DeadLetterRetention`, `CorrelationIdHeader`, `CorrelationIdJsonPaths`,
`PublishMaxAttempts`, and subscriber `AckMode`/`MaxDeliveryAttempts`/`RedeliveryDelay`.
`AckAfterReceive` deletes a row as soon as it is accepted into a bounded in-process queue; handler
failures after that point are logged, reported through `OnBackgroundFailure`, and dead-lettered when
enabled.

### Define a payload and await a response

Every payload implements `IAsyncResponsePayload` — a marker that also keeps scalars out of `For<T>()`:

```csharp
public sealed class OrderResult : IAsyncResponsePayload
{
    public OrderStatus Status { get; set; }
    public string? Message { get; set; }
}

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
failing trigger tears the registration down. Rule of thumb: *never send the request yourself — pass
the send as the trigger.* `For<T>(correlationId)` instead *attaches* to an operation already started
elsewhere, and its `WaitAsync()` takes no trigger.

`IAsyncResponseWaiter<T>` is `IAsyncDisposable` — use `await using` if you hold a waiter directly.
`IAsyncResponsePublisher.SetResponse`/`SetException` accept an optional `CancellationToken`.

### Deliver responses, recover, enqueue workers

```csharp
// Feed raw broker/webhook JSON into the transport-neutral ingress:
await ingress.HandleResponseMessageAsync(messageBodyJson, correlationIdFromHeaders);

// In-process publishers can call the publisher directly with typed payloads:
await publisher.SetResponse(new OrderResult { Status = OrderStatus.Completed }, correlationId);

// Fire-and-forget background work (ambient correlation id is captured and restored):
await _asyncResponse.EnqueueWorkerAsync<IOrderFlow>(flow => flow.ProcessOrderAsync(orderId));
```

For durable lost-subscriber recovery callbacks, broker transports (Redis Streams, Google Pub/Sub,
RabbitMQ, NATS JetStream, PostgreSQL), reply targets, ambient-context propagation,
timeouts/cancellation, and the watchdog, see the docs below.

## Documentation

- **[Configuration](docs/configuration.md)** — `AddAsyncResponse` wiring and a consolidated options
  reference (engine, channel, and transport options).
- **[Recovery](docs/recovery.md)** — lost-subscriber recovery, `ShouldResumeOnRecovery`, the
  watchdog and health check, recovery-state durability, wire/schema versioning, and the
  shared-correlation recovery limitation.
- **[Observability](docs/observability.md)** — tracing (`ActivitySource`) and metrics
  (`System.Diagnostics.Metrics`) for the `"AsyncResponse"` source/meter.
- **[Security & hardening](docs/security.md)** — callback authorization allowlist, securing the
  store/transport, the remote stack-trace policy, strict correlation id, and type resolution for
  plugin/ALC scenarios.
- **[Operations](docs/operations.md)** — best practices, building and testing, and benchmarking and
  load testing.
- **[PostgreSQL](docs/postgresql.md)** — channel/transport architecture, schema, delivery
  confirmation, ACK modes, and operational tuning.
- **[Sample app](docs/sample.md)** — the runnable testbed and curl walkthroughs for every scenario.

Transports, reply targets, and ambient-context propagation are covered alongside their configuration
in [docs/configuration.md](docs/configuration.md) and the per-transport Quick start examples.

## License

[MIT](LICENSE) — © Vitalii Tiunisov
