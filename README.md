<h1 align="center">AsyncResponse</h1>

<p align="center">
  <img src="icon.png" alt="AsyncResponse Icon" width="128" />
</p>

<p align="center"><b>
Turn correlated messages into ordinary .NET <code>await</code>s — with progress handling,
subscribe-before-send safety, and optional crash recovery and checkpointed flows.
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

AsyncResponse is the correlation and recovery layer between your application and asynchronous
infrastructure. It does not replace your broker, webhook, or worker system; it removes the waiter
registry, polling loop, timeout plumbing, and recovery routing that applications otherwise build
around them.

---

## Why teams choose AsyncResponse

- **The API closes the response race.** `For<T>()` requires a trigger and runs it only after the
  waiter is registered. `For<T>(correlationId)` attaches to work started elsewhere and forbids a
  trigger. The compiler keeps those two cases separate.
- **Progress is part of the wait.** `Until(...)` consumes intermediate messages and completes on
  the terminal payload without another queue or state machine.
- **Late responses are domain-aware.** When a waiter disappeared during a restart,
  `ShouldResumeOnRecovery()` routes the payload to a resume or failure callback instead of blindly
  treating every response as success.
- **Infrastructure is replaceable.** Choose one response channel, one worker transport, and one
  flow-state store independently. Move any axis from in-memory to Redis, NATS, a database, Kafka,
  RabbitMQ, or a cloud service through DI while application and flow code stay the same.
- **Multi-step work can stay plain C#.** Durable flows checkpoint named steps, re-attach
  in-flight waits, and preserve terminal payloads received during a restart—without
  replay-determinism rules or a generated workflow DSL.
- **Duplicate work is fenced across replicas.** Built-in flow stores combine atomic idempotent
  start, optimistic revisions, and renewable execution leases, so duplicate worker deliveries do
  not run the same flow concurrently.
- **It is built to be operated.** OpenTelemetry-compatible traces and metrics, readiness health
  checks, recovery scans, bounded early-ACK queues, dead-letter support, and callback authorization
  are first-class features.
- **The local hot path is small.** The checked-in BenchmarkDotNet suite measures the complete
  subscribe → publish → complete cycle, while the stress harness checks isolation, cleanup,
  fan-out, timeouts, context propagation, transport dispatch, and durable flows.

## Pick the smallest setup that fits

Channel, transport, and flow-state storage are independent choices, and every app selects one of
each:

| You need | Response channel | Worker transport | Durable-flow store |
|---|---|---|---|
| Local development, tests, or one process | In-memory | In-memory | In-memory |
| Waiter recovery across restarts | Redis, NATS, PostgreSQL, SQL Server, or MongoDB | Any | In-memory, unless flow ledgers must also survive |
| Jobs on an existing broker or cloud queue | Any | Redis, RabbitMQ, Azure Service Bus, Google Pub/Sub, SQS, Kafka, NATS, PostgreSQL, SQL Server, or MongoDB | In-memory or a provider-backed store |
| Checkpointed multi-step orchestration | Prefer a durable channel | Any | SQL Server, PostgreSQL, MySQL, SQLite, Oracle, MongoDB, Cosmos DB, DynamoDB, EF Core, or custom |

Use in-memory for the shortest path. Add a durable channel when waiter recovery state must outlive
the process, and choose a provider-backed flow store when flow ledgers must outlive it. Selecting a
store completes registration; no ledger or flow execution is created until the application calls
`IDurableFlows`.

## Install and run

```bash
dotnet add package AsyncResponse.Core

# Add exactly one channel when in-memory is not enough:
dotnet add package AsyncResponse.Channels.Redis

# Add exactly one transport when jobs use a broker or queue:
dotnet add package AsyncResponse.Transports.RabbitMQ

# Add exactly one provider-backed flow store when ledgers must survive restarts:
dotnet add package AsyncResponse.DurableFlows.PostgreSQL
```

Packages target .NET 8 and .NET 10. `AsyncResponse.Abstractions` contains contracts only and is the
package to reference from class libraries that define payloads or flows.
The [provider examples](docs/provider-examples.md) page lists the exact package and a copy/paste
registration for every channel, transport, and flow store.

The complete process-local setup selects all three components in one chain:

```csharp
using AsyncResponse;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport(options =>
    {
        options.QueueCapacity = 1_024; // publishers wait asynchronously when full
        options.WorkerCount = 1;       // raise for independent jobs that may run concurrently
    })
    .WithInMemoryDurableFlows();
```

`AddAsyncResponse()` deliberately selects no channel, transport, or durable-flow store. Startup
validation fails fast if the application omits any one of them, so incomplete wiring cannot silently
strand waiters, worker jobs, or flow state. `.WithInMemoryDurableFlows()` is the zero-infrastructure
choice when durable flows are unused or process-local state is intentional. The values above are the
in-memory transport defaults; the bounded queue keeps a local load spike from becoming an unbounded
allocation spike. For production combinations, jump to
[Pick your channel, transport, and flow store](#pick-your-channel-transport-and-flow-store)
and [Production setup](#production-setup).

## The problem it solves

You call a remote system (another service, an Airflow DAG, a payment gateway, a long-running job)
and the answer comes back **later, on a different channel** — a broker topic, a webhook, a callback
queue. Correlating that answer back to the code that asked for it usually means hand-rolled
`TaskCompletionSource` registries, polling loops, or callback spaghetti.

And then the hard part: **your service redeploys while it's waiting.** The in-memory waiter is gone.
The response arrives anyway. Drop it and the flow hangs "in progress" forever; blindly resume it and
you just resumed the **happy path on a failed response**.

AsyncResponse makes the simple case process-local and adds infrastructure only when the required
durability or transport semantics demand it.

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

### Guarantees and boundaries

- For a generated correlation id, the waiter and its recovery state exist before the trigger runs.
- One terminal outcome wins a waiter; timeout, disposal, trigger failure, and completion all clean
  up the registration.
- Completion predicates for one waiter never run concurrently. Internal overload is backpressured through
  bounded in-process queues instead of growing an unbounded delegate or job backlog.
- PostgreSQL, SQL Server, and MongoDB treat notifications and change streams as wake hints:
  retained messages are keyset-paged to exhaustion, so a terminal response cannot be stranded
  behind one full progress batch. Subscriber heartbeats are batched by channel instance and
  interval instead of scheduling one timer and write per waiter.
- A durable channel persists waiter recovery metadata. It does **not** make every response path
  exactly-once: Redis pub/sub is at-most-once, while broker and queue transports can redeliver.
- Handlers, worker jobs, durable-flow steps, and outbound triggers should therefore be idempotent.
  Provider-specific ACK, retry, ordering, and dead-letter behavior is documented in
  [configuration](docs/configuration.md) and [operations](docs/operations.md).

## Durable flows

Compose those waits into whole processes. A **durable flow** is a multi-step orchestration written
as plain sequential C#. The library checkpoints named step results and pending waits so completed
work can be skipped and an interrupted flow can continue after a crash or redeploy:

```csharp
public sealed class TenantProvisioningFlow(
    IWorkspaceService _workspaces,
    IMigrationService _migrations,
    INotifier _notifier)
    : IDurableFlow<ProvisioningInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ProvisioningInput input)
    {
        var ws = await flow.StepAsync("create-workspace",          // local step: result is
            () => _workspaces.CreateAsync(input.TenantId));        // checkpointed after success

        var migration = await flow.AwaitStepAsync<MigrationResult>("run-migration",
            trigger: cid => _migrations.StartAsync(input.TenantId, cid),   // remote step: durably
            until: r => r.Status != MigrationStatus.Running);              // awaited, progress-aware

        if (migration.Status == MigrationStatus.Failed)
            throw new DurableFlowFailedException(migration.Message!);      // terminal, no retry

        await flow.StepAsync("notify", () => _notifier.SendAsync(input.TenantId));
    }
}

// Durable flows are explicit: register the flow class and exactly one atomic state store.
var sqlServerConnectionString = builder.Configuration.GetConnectionString("SqlServer")
    ?? throw new InvalidOperationException("ConnectionStrings:SqlServer is required.");

builder.Services.AddScoped<TenantProvisioningFlow>();
builder.Services.AddAsyncResponse()
    .WithSqlServerChannel(options =>
        options.ConnectionString = sqlServerConnectionString)
    .WithSqlServerTransport(options =>
        options.ConnectionString = sqlServerConnectionString)
    .WithSqlServerDurableFlows(options =>
        options.ConnectionString = sqlServerConnectionString);

var flowId = await _flows.StartAsync<TenantProvisioningFlow, ProvisioningInput>(new(tenantId));
```

- **Checkpointed resume** — completed steps are skipped, pending waits re-attach before retry
  rules run, and lost-subscriber recovery callbacks are wired automatically. A terminal payload
  received while the process is down is checkpointed directly into its pending step before the run
  resumes; it is not discarded and then waited for again.
- **Replica-safe execution** — a caller-supplied flow id is created atomically, every checkpoint is
  compare-and-swap protected, and one renewable lease owns execution. Duplicate deliveries become
  cheap no-ops while a worker is active; an expired lease lets another replica take over. Retrying
  `StartAsync` is idempotent only for the same flow and input; conflicting id reuse fails fast.
- **Edit flows like code** — insert, reorder, or branch steps with ordinary C#; in-flight runs
  pick up compatible changes on resume. Stable step keys preserve existing checkpoints; changing
  a key intentionally creates a new step.
- **Storage is explicit** — `AddAsyncResponse()` never hides flow ledgers in the channel cache.
  Complete every registration with `.WithInMemoryDurableFlows()` for one process, an
  `AsyncResponse.DurableFlows.*` provider such as `.WithSqlServerDurableFlows(...)`, or
  `.WithDurableFlows<MyFlowStateStore>()` for an application-owned implementation.
- **Tested like the rest of the library** — a crash-at-every-checkpoint unit matrix, end-to-end
  integration runs against every durable channel, and a concurrent-flow stress scenario gating CI.

> [!IMPORTANT]
> Durable flows provide **checkpointed, at-least-once execution**, not distributed exactly-once
> side effects. A crash after an external side effect but before its checkpoint can repeat that
> side effect, so step operations and triggers must be idempotent. Built-in state-store packages
> prevent concurrent execution with atomic creation, optimistic revisions, and renewable leases;
> those fences cannot make an external API call and the following state write one transaction.

Common flow-engine settings live beside the selected store's settings in the same callback:

```csharp
builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithInMemoryDurableFlows(options =>
    {
        options.StateExpiry = TimeSpan.FromDays(14);
        options.ExecutionLeaseDuration = TimeSpan.FromMinutes(1);
        options.ExecutionLeaseRenewInterval = TimeSpan.FromSeconds(20);
        options.ProgressPersistenceInterval = TimeSpan.FromSeconds(1);
    });
```

Rapid `ReportProgressAsync` calls are coalesced until the next checkpoint or terminal outcome to avoid
rewriting the whole flow ledger for every progress tick; set
`ProgressPersistenceInterval = TimeSpan.Zero` when every report must be written immediately.

There is one atomic `IFlowStateStore` contract for every store: insert-if-absent start,
revision-checked checkpoints, and acquire/renew/release execution leases. Custom stores do not get
an unsafe local-lock fallback. This keeps the correctness model identical from development through
multi-replica production; only the explicit in-memory store is process-local.

The full guide — rules, failure modes, compensation, testing your flows, and app-owned state
stores — is [docs/durable-flows.md](docs/durable-flows.md).

**Durable-flow state stores** — exactly one required; use the in-memory store from
`AsyncResponse.Core` or a provider package for restart-safe ledgers:

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
| Entity Framework Core (any relational provider) | `.WithEFCoreDurableFlows<TDbContext>(...)` |

See [durable-flow state stores](docs/durable-flow-state-stores.md) for a copy/paste registration for
every store, plus schema ownership, fail-fast provisioning rules, and the atomic custom-store
contract.

## Pick your channel, transport, and flow store

A **channel** delivers responses to waiters and persists recovery state. A **transport** moves
worker jobs and inbound responses through a broker. A **flow store** owns checkpoint ledgers and
execution leases. They are independent axes — combine any one of each.

**Channels** (`AsyncResponse.Channels.*`) — exactly one required:

| Channel | Delivery | Recovery durability |
|---|---|---|
| In-memory (in `Core`) | in-process | process lifetime |
| Redis | pub/sub push, zero polling | TTL'd Redis keys |
| NATS | core request/reply — "no responders" is a positive lost-waiter signal | JetStream Key-Value |
| PostgreSQL | `LISTEN/NOTIFY` wake + keyset-paged table scan—notifications carry only ids, so response size is not constrained by `NOTIFY` | row per waiter registration, database-clock TTLs |
| SQL Server | adaptive keyset-paged sweep (tight while waiters exist, backed off while idle); same-process deliveries skip the sweep entirely | row per waiter registration, database-clock TTLs |
| MongoDB | change-stream wake + keyset-paged collection scan—requires a replica set (single-node is enough); degrades to polling on standalone servers | document per waiter registration, native TTL indexes |

**Transports** (`AsyncResponse.Transports.*`) — exactly one required:

| Transport | Broker mechanics |
|---|---|
| In-memory (in `Core`) | bounded in-process queue; configurable capacity and worker concurrency |
| Redis | Redis Streams consumer groups, pending-entry retry, poison-entry discard, dead-lettering |
| RabbitMQ | publisher confirms + mandatory routing, dead-letter exchange |
| Azure Service Bus | peek-lock ACKs; reuses your own `ServiceBusClient` (e.g. Azure Identity) if registered |
| Google Pub/Sub | streaming pull; redelivery bounds via the subscription's DeadLetterPolicy |
| AWS SQS | long-poll `ReceiveMessage` (up to 10/batch), visibility-timeout redelivery, native dead-letter via redrive policies (provisionable with `CreateQueues`), opt-in FIFO ordering per flow; reuses your own `IAmazonSQS` if registered |
| Kafka | classic consumer groups, manual offset management, in-process bounded retry, `{topic}.deadletter` topics; also covers Redpanda / Amazon MSK / WarpStream / Aiven / Confluent Cloud |
| NATS | JetStream explicit ACKs, NAK-with-delay redelivery, dead-lettering |
| PostgreSQL | queue table claimed with `FOR UPDATE SKIP LOCKED`, idempotent publish, dead-lettering |
| SQL Server | queue table claimed with `UPDLOCK, ROWLOCK, READPAST` (the `SKIP LOCKED` equivalent), idempotent publish, dead-lettering |
| MongoDB | queue collection claimed atomically with `findOneAndUpdate` (server-clock leases, `lock_id` fences), idempotent publish, deterministic dead-letter ids; change-stream wake on replica sets |

Every transport ships hosted subscribers for worker jobs and response ingress with two ACK modes:
the default acknowledges only after your handler completes; opt-in **early ACK** trades that
guarantee for throughput, with an explicitly bounded in-process queue, a drain budget validated
against host shutdown, and post-ACK failures surfaced through `OnBackgroundFailure`. Per-transport
semantics: [docs/configuration.md](docs/configuration.md). Copy/paste registration for every
channel and transport: [provider examples](docs/provider-examples.md).

**Redis-compatible servers.** The Redis channel and transport speak RESP through
`StackExchange.Redis`, so they run unchanged on Redis-compatible servers. **Valkey** is validated
end-to-end as both channel and transport, rechecked by a weekly CI matrix; **Dragonfly** is
validated as both against a live server (its container entrypoint differs from the redis image, so
it runs outside the Aspire CI harness); **Garnet** implements the pub/sub + string + `SCAN` surface
the channel needs but has no stream commands, so it works as a channel but not as this transport. That covers the managed options too — Amazon ElastiCache /
MemoryDB and Azure Managed Redis. Details in [docs/configuration.md](docs/configuration.md#redis-compatible-servers).

## Production setup

The registration shape is always the same: engine + one channel + one transport. The examples
below show complete, representative combinations. The
[provider examples](docs/provider-examples.md) page has one registration for every channel and
transport; the [configuration guide](docs/configuration.md) covers every option and default.

### Durable recovery — Redis channel

```csharp
using AsyncResponse;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect("localhost:6379"));

builder.Services.AddAsyncResponse()
    .WithRedisChannel()                    // Redis response channel + Redis recovery store
    .WithInMemoryTransport()
    .WithInMemoryDurableFlows();           // required zero-infrastructure flow-store choice

builder.Services.AddHealthChecks()
    .AddAsyncResponseRecoveryCheck();      // optional: surface the watchdog on /readyz
```

A durable channel also registers `IRecoverableAsyncResponseBuilder`, which adds the
`OnLostSubscriber*` callback methods. Keep injecting plain `IAsyncResponseBuilder` for ordinary
waits — the recovery API doesn't exist on it, so flows that don't opt in can't misuse it at compile
time. See [docs/recovery.md](docs/recovery.md).

### Broker transport — Azure Service Bus

Transports pair with any channel; here Redis holds waiters/recovery while Service Bus queues move
worker jobs and inbound responses. This reuses the Redis connection registration from the previous
example.

```csharp
var serviceBusConnectionString = builder.Configuration.GetConnectionString("AzureServiceBus")
    ?? throw new InvalidOperationException("ConnectionStrings:AzureServiceBus is required.");

builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithAzureServiceBusTransport(options =>
    {
        options.ConnectionString = serviceBusConnectionString;
        options.WorkerQueue = "orders-worker";
        options.ResponseQueue = "orders-response";
        options.CorrelationIdProperty = "correlationId";
    })
    .WithInMemoryDurableFlows();
```

Need more throughput than ack-per-handler? Opt into early ACK with
`options.WorkerSubscriber.UseAckAfterReceive(backgroundWorkerCount: 4, backgroundQueueCapacity: 256)`
— messages are completed after bounded enqueue and processed by background workers, with failures
reported through `OnBackgroundFailure`.

### Broker transport — Kafka

One package covers Apache Kafka, Redpanda, Amazon MSK, WarpStream, Aiven, and Confluent Cloud.
Kafka is a transport only, so this example reuses the durable Redis channel registration above.

```csharp
var bootstrapServers = builder.Configuration["Kafka:BootstrapServers"]
    ?? throw new InvalidOperationException("Kafka:BootstrapServers is required.");

builder.Services.AddAsyncResponse()
    .WithRedisChannel(options => options.KeyPrefix = "orders")
    .WithKafkaTransport(options =>
    {
        options.BootstrapServers = bootstrapServers;
        options.TopicPrefix = "orders";
        options.TopicNumPartitions = 12;
        options.CreateTopics = true;
    })
    .WithInMemoryDurableFlows();
```

The correlation id is the message key, so one flow's jobs stay ordered within a partition.
Consumer parallelism is bounded by the partition count, and a slow or retrying message delays its
partition. After the configured attempts, the message moves to the dead-letter topic and its
offset is committed so the partition can continue.

### Everything on PostgreSQL

Use this when PostgreSQL is already your durable infrastructure and you do not want a separate
broker for response recovery or worker dispatch.

```csharp
using Npgsql;

var connectionString = builder.Configuration.GetConnectionString("PostgreSQL")
    ?? throw new InvalidOperationException("ConnectionStrings:PostgreSQL is required.");

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));

builder.Services.AddAsyncResponse()
    .WithPostgreSqlChannel(options =>
        options.SchemaName = "public")
    .WithPostgreSqlTransport(options =>
    {
        options.SchemaName = "public";
        options.WorkerSubscriber.UseAckAfterReceive(
            backgroundWorkerCount: 4,
            backgroundQueueCapacity: 256);
    })
    .WithPostgreSqlDurableFlows(options =>
        options.SchemaName = "public");
```

The channel, transport, and flow store share one connection pool but use separate tables.
`LISTEN/NOTIFY` wakes response readers, while workers claim queue rows with
`FOR UPDATE SKIP LOCKED`. Schema details and connection-pool tuning are in
[docs/postgresql.md](docs/postgresql.md).

### Everything on SQL Server

The SQL Server providers keep durable waits, worker messages, and flow ledgers in one existing
database.

```csharp
var connectionString = builder.Configuration.GetConnectionString("SqlServer")
    ?? throw new InvalidOperationException("ConnectionStrings:SqlServer is required.");

builder.Services.AddAsyncResponse()
    .WithSqlServerChannel(options =>
        options.ConnectionString = connectionString)
    .WithSqlServerTransport(options =>
    {
        options.ConnectionString = connectionString;
        options.WorkerSubscriber.UseAckAfterEnqueue(
            backgroundWorkerCount: 4,
            backgroundQueueCapacity: 256);
    })
    .WithSqlServerDurableFlows(options =>
        options.ConnectionString = connectionString);
```

SQL Server has no `LISTEN/NOTIFY`, so its channel polls adaptively and skips the sweep for
same-process delivery. Workers claim rows with `UPDLOCK`, `ROWLOCK`, and `READPAST`. The packages
create their schema and tables, but the database must already exist. Details:
[docs/sqlserver.md](docs/sqlserver.md).

### AWS-native stack — SQS transport + durable channel

SQS carries worker jobs and response ingress; Redis on ElastiCache/MemoryDB or PostgreSQL on
RDS/Aurora owns waiter recovery.

```csharp
builder.Services.AddAsyncResponse()
    .WithRedisChannel(options => options.KeyPrefix = "orders")
    .WithSqsTransport(options =>
    {
        options.Region = "eu-central-1";
        options.WorkerQueue = "orders-worker";
        options.ResponseQueue = "orders-response";
        options.CreateQueues = true; // development; provision queues and DLQs with IaC in production
        options.MaxReceiveCount = 5;
    })
    .WithDynamoDbDurableFlows(options =>
        options.TableName = "orders-flow-state");
```

SQS owns visibility-timeout redelivery and redrive-policy dead letters. Name both queues with a
`.fifo` suffix to keep each correlation id ordered as one message group. A registered `IAmazonSQS`
is reused automatically; otherwise the AWS SDK credential and region chain is used.

### More deployment combinations

| Existing infrastructure | Typical registration | Important behavior |
|---|---|---|
| Kafka / Redpanda / MSK / Confluent | durable channel + `.WithKafkaTransport(...)` + one flow store | Correlation id is the partition key; partition count bounds consumer parallelism and a retry delays that partition. |
| PostgreSQL | `.WithPostgreSqlChannel()` + `.WithPostgreSqlTransport(...)` + `.WithPostgreSqlDurableFlows(...)` | `LISTEN/NOTIFY` wakes response readers; workers claim queue rows with `FOR UPDATE SKIP LOCKED`. |
| SQL Server | `.WithSqlServerChannel(...)` + `.WithSqlServerTransport(...)` + `.WithSqlServerDurableFlows(...)` | Adaptive response polling; workers claim rows with `UPDLOCK, ROWLOCK, READPAST`. |
| AWS | Redis/PostgreSQL channel + `.WithSqsTransport(...)` + `.WithDynamoDbDurableFlows(...)` | Native visibility-timeout redelivery and redrive-policy dead letters; FIFO queues order by correlation id. |
| NATS | `.WithNatsChannel(...)` + `.WithNatsTransport(...)` + one flow store | Core request/reply for responses and JetStream explicit ACKs for worker jobs. |

See [configuration](docs/configuration.md) for every registration and option,
[PostgreSQL](docs/postgresql.md) and [SQL Server](docs/sqlserver.md) for database-specific tuning,
and [operations](docs/operations.md) for ACK-mode and delivery trade-offs.

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

- A representative .NET 10 short run on an Apple M4 Pro measured the complete in-memory round trip
  at **0.83 µs / 1.63 KB** through the fluent builder and **0.76 µs / 1.27 KB** through the lower-level
  subscriber API. That is library overhead only; broker, network, serialization, and store latency
  depend on the selected providers and environment.
- Hot paths are allocation-conscious by design: single-subscriber fast paths, cached
  `JsonEncodedText` envelope fields with a hand-rolled `Utf8JsonReader` converter, memoized raw-JSON
  materialization shared across waiters, cached reflection invocation plans, and listener-gated
  traces and metrics.
- Per-waiter predicates are serialized without allocating on the uncontended synchronous path.
  Internal work queues are bounded, database wake signals are coalesced, and database-channel
  heartbeats are batched per process rather than scheduled per waiter.
- Redis and NATS push responses directly; PostgreSQL uses `LISTEN/NOTIFY`; MongoDB uses change
  streams when available; SQL Server uses an adaptive polling sweep. Database rows/documents remain
  the source of truth when wake signals are coalesced or missed, and each scan keyset-pages through
  every retained message instead of stopping at the first batch.
- Every wait has a timeout (defaulted when unset) and a single-winner terminal state, so abandoned
  waiters clean themselves up — no leaked registrations under load.

BenchmarkDotNet, a **22-scenario correctness stress harness**, and NBomber load tests run in CI on
code pushes to `main`; per-commit trends with regression alerting are published to the
[live benchmark dashboard](https://sky4ce.github.io/AsyncResponse/dev/bench/). Methodology:
[docs/operations.md](docs/operations.md).

## How it's tested

- **2500+ unit tests on each target framework** (.NET 8 + .NET 10 executions), including
  concurrency suites with hundreds of parallel waiters, cross-correlation leak detection, and
  duplicate-execution detection.
- **165 integration test cases** drive the shipped sample app black-box over HTTP against **real
  brokers** — Redis, NATS, PostgreSQL, SQL Server, MongoDB (single-node replica set), RabbitMQ,
  Kafka containers plus the official Azure Service Bus and Google Pub/Sub emulators and LocalStack
  for AWS SQS — orchestrated by .NET Aspire, with a dedicated early-ACK app instance per transport.
  The same run verifies the atomic durable-flow store contract against SQL Server, PostgreSQL,
  MySQL, SQLite, Oracle, MongoDB, Cosmos DB, DynamoDB, and EF Core.
  A scheduled CI matrix reruns the Redis-backed suite against Valkey; Dragonfly is validated by
  running the real channel and transport against a live server.
- A **stress harness** asserts correctness invariants under storm load (zero lost, crossed,
  duplicated, or leaked responses) and fails CI on violation; NBomber load profiles include a
  destructive recovery scenario.
- Focused tests also cover option validation, ACK-mode dispatch, metric/span emission, callback
  authorization, unsupported-schema rejection, and recovery cleanup.

## When to use it — and when not

**Reach for AsyncResponse when**

- a flow needs the *answer* to a specific request that arrives asynchronously — job results,
  payment confirmations, DAG completions, webhook callbacks;
- you're **orchestrating a multi-step flow across async services** — implement
  `IDurableFlow<TInput>` and write the steps as plain sequential `await`s
  (`flow.StepAsync(...)`, `flow.AwaitStepAsync<T>(...)`). The library checkpoints successful steps,
  re-attaches in-flight waits after a crash or redeploy, and wires the recovery callbacks; external
  side effects remain at-least-once and must be idempotent —
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

Looking for something specific? The **[docs index](docs/README.md)** maps "I want to…" tasks to
the right page. The pages:

- **[Configuration](docs/configuration.md)** — `AddAsyncResponse` wiring and a consolidated options
  reference (engine, channel, and transport options).
- **[Provider examples](docs/provider-examples.md)** — copy/paste registration for every channel and
  every worker transport, plus links to every durable-flow store example.
- **[Recovery](docs/recovery.md)** — lost-subscriber recovery, `ShouldResumeOnRecovery`, the
  watchdog and health check, recovery-state durability, wire/schema versioning, and the
  shared-correlation recovery limitation.
- **[Durable flows](docs/durable-flows.md)** — first-class multi-step orchestration:
  `IDurableFlow<TInput>` with automatically checkpointed steps, crash-resume and re-attach,
  progress streaming, pluggable flow-state storage, explicit compensation, and an honest
  comparison with workflow engines.
- **[Durable-flow state stores](docs/durable-flow-state-stores.md)** — a complete registration for
  every store provider, the atomic contract, schema ownership, client lifetimes, and expiry.
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
