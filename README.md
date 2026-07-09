<h1 align="center">AsyncResponse</h1>

<p align="center">
  <img src="icon.png" alt="AsyncResponse Icon" width="128" />
</p>

<p align="center"><b>
Await responses from message brokers, webhooks, and background workers as if they were local async calls —
with optional durable, domain-aware recovery when your process dies mid-wait.
</b></p>

<p align="center">
  <a href="https://github.com/Sky4CE/AsyncResponse/actions/workflows/ci.yml"><img src="https://github.com/Sky4CE/AsyncResponse/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://www.nuget.org/packages/AsyncResponse.Core"><img src="https://img.shields.io/nuget/v/AsyncResponse.Core.svg" alt="NuGet"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT"></a>
</p>

```csharp
OrderResult result = await asyncResponse
    .For<OrderResult>()                                       // correlation id generated for you
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
The response arrives anyway. Drop it and the flow hangs "in progress" forever; blindly resume it and
you just resumed the **happy path on a failed response**.

AsyncResponse solves the correlation problem with zero infrastructure, and lets you add durable
recovery when a flow needs it.

## Why AsyncResponse

- **Feels local, runs distributed.** One fluent expression replaces the `TaskCompletionSource`
  registry, the timeout plumbing, and the correlation bookkeeping — and with **durable flows**
  a multi-step process over a dozen remote services reads as a dozen sequential `await`s.
  Steps checkpoint automatically, in-flight waits re-attach after a redeploy, and inserting or
  reordering a step is an ordinary code edit ([durable flows](docs/durable-flows.md)).
- **Race-free by construction.** The request is sent by a *trigger* that runs only after the
  subscription and recovery state exist, so the first response can never beat its waiter — and a
  failing trigger tears the registration down. `For<T>()` **requires** a trigger;
  `For<T>(correlationId)` (attaching to an operation started elsewhere) **forbids** one. The
  compiler enforces the difference.
- **Progress-aware waits.** `Until(...)` consumes progress messages and completes only on the
  terminal one — no extra queues, no hand-rolled state machine.
- **Recovery that understands your domain.** A response that arrives after its waiter died is
  classified by the payload's `ShouldResumeOnRecovery()` and routed to a durable resume or failure
  callback. A failed response is **never** blindly resumed.
- **Any channel × any transport.** Response channels: in-memory, Redis, NATS, PostgreSQL,
  SQL Server. Worker transports: in-memory, Redis Streams, RabbitMQ, Azure Service Bus, Google
  Pub/Sub, AWS SQS, Kafka, NATS JetStream, PostgreSQL, SQL Server. The Redis channel and transport
  also run on Valkey and Dragonfly (Garnet as a channel). Every combination works; your flow code
  never changes.
- **One contract everywhere.** Schema-versioned wire envelopes, capped remote stack traces,
  ambient-context restoration into foreign callback threads, and identical `Until`/recovery
  semantics on every channel — switching infrastructure is a DI change, not a rewrite.
- **Built to operate.** Recovery watchdog + readiness health check, `ActivitySource` tracing and
  `System.Diagnostics.Metrics` under the `"AsyncResponse"` source/meter, OpenTelemetry messaging
  attributes on the transports, and an opt-in callback authorization allowlist.
- **Fast where it counts.** Benchmark-tuned hot path — a complete in-memory round trip
  (subscribe → publish → complete) runs in well under a microsecond; broker channels deliver by
  push, not polling. See [Performance](#performance).

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

A failed payload is **still a valid response** for an active waiter — your `Until` predicate and
flow code want to see it (persist details, decide to retry, throw a rich domain error).
`ShouldResumeOnRecovery()` is consulted only when nobody is listening — which is exactly when
somebody has to make the call. Full model: [docs/recovery.md](docs/recovery.md).

## Durable flows

Compose those waits into whole processes. A **durable flow** is a multi-step orchestration written
as plain sequential C# — the library checkpoints every step, so the flow survives crashes,
redeploys, and redeliveries mid-step and resumes exactly where it left off:

```csharp
public sealed class TenantProvisioningFlow(IMigrationService _migrations, INotifier _notifier)
    : IDurableFlow<ProvisioningInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ProvisioningInput input)
    {
        var ws = await flow.StepAsync("create-workspace",          // local step: runs once,
            () => _workspaces.CreateAsync(input.TenantId));        // result memoized

        var migration = await flow.AwaitStepAsync<MigrationResult>("run-migration",
            trigger: cid => _migrations.StartAsync(input.TenantId, cid),   // remote step: durably
            until: r => r.Status != MigrationStatus.Running);              // awaited, progress-aware

        if (migration.Status == MigrationStatus.Failed)
            throw new DurableFlowFailedException(migration.Message!);      // terminal, no retry

        await flow.StepAsync("notify", () => _notifier.SendAsync(input.TenantId));
    }
}

var flowId = await _flows.StartAsync<TenantProvisioningFlow, ProvisioningInput>(new(tenantId));
```

- **Crash-safe by construction** — completed steps skip, the in-flight wait *re-attaches* (the
  request is never re-sent), and lost-subscriber recovery callbacks are wired automatically.
- **Edit flows like code** — insert, reorder, or branch steps with ordinary C#; in-flight runs
  pick up the changes on resume. Hotfix a bug and resume — no replay rules, no version patching.
- **Storage is explicit when it matters** — the default flow-state store rides in your channel's
  recovery store for tests/dev/migration; production flows should use an
  `AsyncResponse.DurableFlows.*` package such as `.WithSqlServerDurableFlows(...)`, or
  `.WithCustomDurableFlows<MyFlowStateStore>()` when your storage model is custom.
- **Tested like the rest of the library** — a crash-at-every-checkpoint unit matrix, end-to-end
  integration runs against every durable channel, and a concurrent-flow stress scenario gating CI.

The full guide — rules, failure modes, compensation, testing your flows, and app-owned state
stores — is [docs/durable-flows.md](docs/durable-flows.md).

**Durable-flow state stores** (`AsyncResponse.DurableFlows.*`) — optional but recommended for
production durable flows:

| Store package | Registration |
|---|---|
| SQL Server | `.WithSqlServerDurableFlows(...)` |
| PostgreSQL | `.WithPostgreSqlDurableFlows(...)` |
| MySQL / MariaDB | `.WithMySqlDurableFlows(...)` |
| SQLite | `.WithSqliteDurableFlows(...)` |
| Oracle | `.WithOracleDurableFlows(...)` |
| MongoDB | `.WithMongoDbDurableFlows(...)` |
| Azure Cosmos DB | `.WithCosmosDurableFlows(...)` |
| DynamoDB | `.WithDynamoDbDurableFlows(...)` |

See [durable-flow state stores](docs/durable-flow-state-stores.md) for options, schema ownership,
and custom `IFlowStateStore` examples.

## Pick your channel and transport

A **channel** delivers responses to waiters and persists recovery state. A **transport** moves
worker jobs and inbound responses through a broker. They are independent axes — pair any channel
with any transport.

**Channels** (`AsyncResponse.Channels.*`) — exactly one required:

| Channel | Delivery | Recovery durability |
|---|---|---|
| In-memory (in `Core`) | in-process | process lifetime |
| Redis | pub/sub push, zero polling | TTL'd Redis keys |
| NATS | core request/reply — "no responders" is a positive lost-waiter signal | JetStream Key-Value |
| PostgreSQL | `LISTEN/NOTIFY` wake + table rows — notifications carry only ids, so payload size is unbounded | row per waiter registration, database-clock TTLs |
| SQL Server | adaptive polling sweep (tight while waiters exist, backed off while idle) + table rows — same-process deliveries skip the sweep entirely | row per waiter registration, database-clock TTLs |

**Transports** (`AsyncResponse.Transports.*`) — exactly one required:

| Transport | Broker mechanics |
|---|---|
| In-memory (in `Core`) | in-process queue |
| Redis | Redis Streams consumer groups, pending-entry retry, poison-entry discard, dead-lettering |
| RabbitMQ | publisher confirms + mandatory routing, dead-letter exchange |
| Azure Service Bus | peek-lock ACKs; reuses your own `ServiceBusClient` (e.g. Azure Identity) if registered |
| Google Pub/Sub | streaming pull; redelivery bounds via the subscription's DeadLetterPolicy |
| AWS SQS | long-poll `ReceiveMessage` (up to 10/batch), visibility-timeout redelivery, native dead-letter via redrive policies (provisionable with `CreateQueues`), opt-in FIFO ordering per flow; reuses your own `IAmazonSQS` if registered |
| Kafka | classic consumer groups, manual offset management, in-process bounded retry, `{topic}.deadletter` topics; also covers Redpanda / Amazon MSK / WarpStream / Aiven / Confluent Cloud |
| NATS | JetStream explicit ACKs, NAK-with-delay redelivery, dead-lettering |
| PostgreSQL | queue table claimed with `FOR UPDATE SKIP LOCKED`, idempotent publish, dead-lettering |
| SQL Server | queue table claimed with `UPDLOCK, ROWLOCK, READPAST` (the `SKIP LOCKED` equivalent), idempotent publish, dead-lettering |

Every transport ships hosted subscribers for worker jobs and response ingress with two ACK modes:
the default acknowledges only after your handler completes; opt-in **early ACK** trades that
guarantee for throughput, with an explicitly bounded in-process queue, a drain budget validated
against host shutdown, and post-ACK failures surfaced through `OnBackgroundFailure`. Per-transport
semantics: [docs/configuration.md](docs/configuration.md).

**Redis-compatible servers.** The Redis channel and transport speak RESP through
`StackExchange.Redis`, so they run unchanged on Redis-compatible servers. **Valkey** and
**Dragonfly** are validated end-to-end as both channel and transport; **Garnet** implements the
pub/sub + string + `SCAN` surface the channel needs but has no stream commands, so it works as a
channel but not as this transport. That covers the managed options too — Amazon ElastiCache /
MemoryDB and Azure Managed Redis. Details in [docs/configuration.md](docs/configuration.md#redis-compatible-servers).

`AsyncResponse.Abstractions` holds contracts only — reference it from class libraries that define
payloads or flows.

## Installation

```bash
dotnet add package AsyncResponse.Core

# exactly one channel (skip for in-memory):
dotnet add package AsyncResponse.Channels.Redis        # or .NATS / .PostgreSQL / .SqlServer

# exactly one transport (skip for in-memory):
dotnet add package AsyncResponse.Transports.RabbitMQ   # or .Kafka / .Redis / .AzureServiceBus / .GooglePubSub / .SQS / .NATS / .PostgreSQL / .SqlServer
```

Targets .NET 8 and .NET 10.

## Quick start

`AddAsyncResponse()` registers the channel-agnostic engine but **no channel or transport** — chain
exactly one of each. An app that starts without either fails fast at host startup, so a
misconfiguration can never silently hang every waiter or drop worker dispatch.

### In-memory — no external dependencies

```csharp
using AsyncResponse;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAsyncResponse()   // engine: fluent builder, ingress, recovery watchdog
    .WithInMemoryChannel()            // process-local response channel + recovery store
    .WithInMemoryTransport();         // in-process worker transport
```

The full programming model, process-local: ideal for single-node apps, prototypes, and tests.
Waiters and recovery state disappear with the process.

### Durable recovery — Redis channel

```csharp
using AsyncResponse;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect("localhost:6379"));

builder.Services.AddAsyncResponse()
    .WithRedisChannel()                    // Redis response channel + Redis recovery store
    .WithInMemoryTransport();

builder.Services.AddHealthChecks()
    .AddAsyncResponseRecoveryCheck();      // optional: surface the watchdog on /readyz
```

A durable channel also registers `IRecoverableAsyncResponseBuilder`, which adds the
`OnLostSubscriber*` callback methods. Keep injecting plain `IAsyncResponseBuilder` for ordinary
waits — the recovery API doesn't exist on it, so flows that don't opt in can't misuse it at compile
time. See [docs/recovery.md](docs/recovery.md).

### Broker transport — Azure Service Bus

Transports pair with any channel; here Redis holds waiters/recovery while Service Bus queues move
worker jobs and inbound responses.

```csharp
builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithAzureServiceBusTransport(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("AzureServiceBus");
        options.WorkerQueue = "orders-worker";
        options.ResponseQueue = "orders-response";
        options.CorrelationIdProperty = "correlationId";
    });
```

Need more throughput than ack-per-handler? Opt into early ACK with
`options.WorkerSubscriber.UseAckAfterReceive(backgroundWorkerCount: 4, backgroundQueueCapacity: 256)`
— messages are completed after bounded enqueue and processed by background workers, with failures
reported through `OnBackgroundFailure`.

### Broker transport — Kafka

One package covers everything that speaks the Kafka protocol: Apache Kafka, Redpanda, Amazon MSK,
WarpStream, Aiven, Confluent Cloud. Kafka is a transport only, by design — its partitioned log has
no targeted waiter wake and no per-key TTL store, so pair it with a durable channel.

```csharp
builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithKafkaTransport(options =>
    {
        options.BootstrapServers = builder.Configuration["Kafka:BootstrapServers"];
        options.WorkerTopic = "orders-worker";
        options.ResponseTopic = "orders-response";
        options.CorrelationIdHeader = "correlationId";
    });
```

The correlation id travels as a message header and doubles as the partition key, so one flow's jobs
stay ordered within their partition. Honest caveats: consumer parallelism equals the partition
count (size `TopicNumPartitions` accordingly), and a slow message delays its partition — retries run
in-process with backoff because offsets cannot NACK a single message; after
`WorkerSubscriber.MaxDeliveryAttempts` the message is produced to `{topic}.deadletter` with
failure-detail headers and its offset committed so the partition keeps moving. These trade-offs are
inherent to classic consumer groups; a KIP-932 share-group consumption mode can slot in behind the
same options once librdkafka supports it.

### Everything on PostgreSQL

Use this when PostgreSQL is already your durable infrastructure and you don't want to add a broker
just for async-response recovery.

```csharp
using AsyncResponse;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(
    builder.Configuration.GetConnectionString("PostgreSQL")!));

builder.Services.AddAsyncResponse()
    .WithPostgreSqlChannel()               // LISTEN/NOTIFY channel + row-per-waiter recovery
    .WithPostgreSqlTransport(options =>    // queue table, FOR UPDATE SKIP LOCKED
    {
        options.WorkerSubscriber.UseAckAfterReceive(
            backgroundWorkerCount: 4,
            backgroundQueueCapacity: 256);
    });
```

The channel and transport share the `NpgsqlDataSource` but use separate table sets, and schema
creation is serialized across app instances with an advisory lock (set `AutoCreateSchema = false`
when migrations own the schema). Table names, delivery-confirmation mechanics, and the
connection-string settings worth tuning under load (`No Reset On Close`, `Max Auto Prepare`) are in
[docs/postgresql.md](docs/postgresql.md).

### Everything on SQL Server

The same recipe for SQL Server shops: durable waits, recovery, and a worker queue on the database
you already run, with no broker to add.

```csharp
builder.Services.AddAsyncResponse()
    .WithSqlServerChannel(options =>          // adaptive-polling channel + row-per-waiter recovery
        options.ConnectionString = builder.Configuration.GetConnectionString("SqlServer"))
    .WithSqlServerTransport(options =>        // queue table, UPDLOCK/ROWLOCK/READPAST claims
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("SqlServer");
        options.WorkerSubscriber.UseAckAfterEnqueue(
            backgroundWorkerCount: 4,
            backgroundQueueCapacity: 256);
    });
```

SQL Server has no `LISTEN/NOTIFY`, so the channel wakes waiters with an adaptive polling sweep:
tight (250 ms) while waiters are subscribed, backed off (2 s) while idle — and same-process
deliveries skip the sweep entirely, so the common path never polls. Schema creation is serialized
across instances with `sp_getapplock`; the packages create their schema and tables but never the
database itself. Details in [docs/sqlserver.md](docs/sqlserver.md).

### AWS-native stack — SQS transport + Redis or PostgreSQL channel

The full AWS recipe with zero self-managed brokers: SQS carries worker jobs and response ingress,
and the channel rides ElastiCache/MemoryDB (Redis) or RDS/Aurora (PostgreSQL) for the waiter side
and recovery state. Redelivery and dead-lettering stay native to SQS via queue redrive policies.

```csharp
builder.Services
    .AddAsyncResponse()
    .WithRedisChannel(options => options.KeyPrefix = "orders")   // ElastiCache / MemoryDB
    // …or .WithPostgreSqlChannel(...) on RDS / Aurora
    .WithSqsTransport(options =>
    {
        options.Region = "us-east-1";                 // omit to use the SDK default chain
        options.WorkerQueue = "orders-worker";        // queue name or full queue URL
        options.ResponseQueue = "orders-response";
        options.CreateQueues = true;                  // + redrive-policy DLQs (dev/test; own your
        options.MaxReceiveCount = 5;                  //   queues via infra code in production)
    });
```

Name the queues `*.fifo` to opt into FIFO ordering — the correlation id becomes the
`MessageGroupId`, so one flow's jobs stay ordered while distinct flows fan out. An
application-registered `IAmazonSQS` (for example from `AWSSDK.Extensions.NETCore.Setup` with IAM
roles) is reused automatically.

### The other combinations

Same pattern everywhere: `.WithNatsChannel()`, `.WithPostgreSqlChannel()`, `.WithSqlServerChannel()`,
`.WithRedisTransport()`, `.WithRabbitMqTransport()`, `.WithGooglePubSubTransport()`,
`.WithKafkaTransport()`, `.WithNatsTransport()`, `.WithSqlServerTransport()`, `.WithSqsTransport()` —
every channel, transport, and option is documented in [docs/configuration.md](docs/configuration.md).

## Define a payload and await it

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

Rule of thumb: **never send the request yourself — pass the send as the trigger.** That is what
makes the subscribe-before-send guarantee hold. Use `For<T>(correlationId)` to *attach* to an
operation already started elsewhere; its `WaitAsync()` takes no trigger.

`IAsyncResponseWaiter<T>` is `IAsyncDisposable` — use `await using` if you hold a waiter directly.
`IAsyncResponsePublisher.SetResponse`/`SetException` accept an optional `CancellationToken`.

## Deliver responses, recover, enqueue workers

```csharp
// Feed raw broker/webhook JSON into the transport-neutral ingress:
await ingress.HandleResponseMessageAsync(messageBodyJson, correlationIdFromHeaders);

// In-process publishers can call the publisher directly with typed payloads:
await publisher.SetResponse(new OrderResult { Status = OrderStatus.Completed }, correlationId);

// Fire-and-forget background work (ambient correlation id is captured and restored):
await _asyncResponse.EnqueueWorkerAsync<IOrderFlow>(flow => flow.ProcessOrderAsync(orderId));
```

Durable lost-subscriber callbacks, reply targets, ambient-context propagation, timeouts and
cancellation, and the watchdog are covered in the [docs](#documentation).

## Performance

- A complete in-memory round trip — create waiter, publish, complete, clean up — benchmarks at
  **≈0.7–0.8 µs with ≈1.3–1.6 KB allocated** end-to-end (Apple M4 Pro, .NET 10).
- Hot paths are allocation-conscious by design: single-subscriber fast paths, cached
  `JsonEncodedText` envelope fields with a hand-rolled `Utf8JsonReader` converter, memoized raw-JSON
  materialization shared across waiters, and log/trace/metric gating so observability costs nothing
  when disabled.
- Broker channels deliver by push (Redis pub/sub, NATS request/reply, PostgreSQL `LISTEN/NOTIFY`) —
  no polling on the response hot path.
- Every wait has a timeout (defaulted when unset) and a single-winner terminal state, so abandoned
  waiters clean themselves up — no leaked registrations under load.

Benchmarks, a 17-scenario stress harness, and NBomber load tests run in CI on every push;
per-commit trends with regression alerting are published to the
[live benchmark dashboard](https://sky4ce.github.io/AsyncResponse/dev/bench/). Methodology:
[docs/operations.md](docs/operations.md).

## How it's tested

- **2200+ unit tests** across .NET 8 and .NET 10, including real concurrency suites (hundreds of
  parallel waiters with cross-correlation leak detection, duplicate-execution detection).
- **140+ integration tests** drive the shipped sample app black-box over HTTP against **real
  brokers** — Redis, NATS, PostgreSQL, SQL Server, RabbitMQ, Kafka containers plus the official
  Azure Service Bus and Google Pub/Sub emulators and LocalStack for AWS SQS — orchestrated by .NET
  Aspire, with a dedicated early-ACK app instance per transport. A scheduled CI matrix reruns the
  Redis-backed suite against Valkey to hold the Redis-compatible-server claim (Dragonfly is validated
  by running the real channel + transport against a live server).
- A **stress harness** asserts correctness invariants under storm load (zero lost, crossed,
  duplicated, or leaked responses) and fails CI on violation; NBomber load profiles include a
  destructive recovery scenario.
- Docs are kept code-true: option defaults, metric/span names, and behavior claims are verified
  against the source.

## When to use it — and when not

**Reach for AsyncResponse when**

- a flow needs the *answer* to a specific request that arrives asynchronously — job results,
  payment confirmations, DAG completions, webhook callbacks;
- you're **orchestrating a multi-step flow across async services** — implement
  `IDurableFlow<TInput>` and write the steps as plain sequential `await`s
  (`flow.StepAsync(...)`, `flow.AwaitStepAsync<T>(...)`). The library checkpoints every step,
  re-attaches in-flight waits after a crash or redeploy, and wires the recovery callbacks —
  [durable flows](docs/durable-flows.md);
- you're maintaining a hand-rolled `TaskCompletionSource` registry or a polling loop today;
- waits must survive redeploys, and a late **failure** must never be resumed as a success.

**Reach for something else when**

- you need **engine-owned** flow semantics: automatic compensation graphs, durable timers
  measured in weeks, human-approval tasks, replayable audit histories of every decision. With
  idempotent steps and a persisted ledger, AsyncResponse runs crash-resumable multi-step flows
  end to end — including explicit compensation — as
  [durable flows](docs/durable-flows.md) shows, and it does so without replay-determinism rules
  or workflow-version patching. Choose Temporal/Durable Task when you want the *engine* to own
  the ledger and derive compensation automatically, and accept those constraints in exchange.
- you need fire-and-forget pub/sub fan-out with nobody waiting — that's your message bus, and
  AsyncResponse coexists with it happily.

## Documentation

- **[Configuration](docs/configuration.md)** — `AddAsyncResponse` wiring and a consolidated options
  reference (engine, channel, and transport options).
- **[Recovery](docs/recovery.md)** — lost-subscriber recovery, `ShouldResumeOnRecovery`, the
  watchdog and health check, recovery-state durability, wire/schema versioning, and the
  shared-correlation recovery limitation.
- **[Durable flows](docs/durable-flows.md)** — first-class multi-step orchestration:
  `IDurableFlow<TInput>` with automatically checkpointed steps, crash-resume and re-attach,
  progress streaming, pluggable flow-state storage, explicit compensation, and an honest
  comparison with workflow engines.
- **[Observability](docs/observability.md)** — tracing (`ActivitySource`) and metrics
  (`System.Diagnostics.Metrics`) for the `"AsyncResponse"` source/meter.
- **[Security & hardening](docs/security.md)** — callback authorization allowlist, securing the
  store/transport, the remote stack-trace policy, strict correlation id, and type resolution for
  plugin/ALC scenarios.
- **[Operations](docs/operations.md)** — best practices, building and testing, and benchmarking and
  load testing.
- **[PostgreSQL](docs/postgresql.md)** — channel/transport architecture, schema, delivery
  confirmation, ACK modes, and operational tuning.
- **[SQL Server](docs/sqlserver.md)** — channel/transport architecture, adaptive polling wake,
  `UPDLOCK/READPAST` claims, schema, ACK modes, and operational tuning.
- **[Sample app](docs/sample.md)** — the runnable Aspire testbed and curl walkthroughs for every
  scenario.
- **[Roadmap](docs/roadmap.md)** — which channels and transports are next (Hangfire, …),
  with priorities and design sketches.

## License

[MIT](LICENSE) — © Vitalii Tiunisov
