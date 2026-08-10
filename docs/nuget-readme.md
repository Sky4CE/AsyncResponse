# AsyncResponse

Turn correlated messages into ordinary .NET `await`s — with progress handling,
subscribe-before-send safety, and optional crash recovery and checkpointed flows with durable
timers and cron scheduling.

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
around them. This package is part of the AsyncResponse family; the full provider matrix and
documentation live in the [GitHub repository](https://github.com/Sky4CE/AsyncResponse).

## Three independent axes

Every application selects one response channel, one worker transport, and one durable-flow store.
Move any axis from in-memory to a provider through DI while application and flow code stay the
same:

- **Response channel** — delivers correlated responses to waiters; durable channels also keep
  recovery state across restarts. In-memory, Redis, NATS, PostgreSQL, SQL Server, or MongoDB.
- **Worker transport** — dispatches background jobs on your existing broker or queue. In-memory,
  Redis, RabbitMQ, Azure Service Bus, Google Pub/Sub, SQS, Kafka, NATS, PostgreSQL, SQL Server,
  or MongoDB.
- **Durable-flow store** — persists checkpointed multi-step flow ledgers. In-memory, SQL Server,
  PostgreSQL, MySQL, SQLite, Oracle, MongoDB, Cosmos DB, DynamoDB, or any EF Core relational
  provider.

## Quick start

```bash
dotnet add package AsyncResponse.Core
```

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

`AddAsyncResponse()` deliberately selects no channel, transport, or durable-flow store; startup
validation fails fast if the application omits any one of them. Swap an axis by replacing one
registration with a provider package — for example `AsyncResponse.Channels.Redis`,
`AsyncResponse.Transports.RabbitMQ`, or `AsyncResponse.DurableFlows.PostgreSQL`.

Flows sleep durably (`await flow.DelayAsync("cool-down", TimeSpan.FromDays(3))` suspends the run
— no worker or memory held; crashes resume the remainder) and start on cron schedules
(`.WithScheduledFlow<TFlow, TInput>("nightly", "0 6 * * *", …)`) with exactly-once occurrences
across replicas. In test projects, `AsyncResponse.Testing` runs the complete engine on a virtual
clock: script replies to awaited steps, skip production-sized timeouts and multi-day timers
instantly, inject crashes at exact checkpoints, and simulate restarts with real lost-subscriber
recovery.

Packages target .NET 8 and .NET 10 and are trim- and Native AOT-compatible.
`AsyncResponse.Abstractions` contains contracts only and is the package to reference from class
libraries that define payloads or flows.

## Documentation

- [Repository and full README](https://github.com/Sky4CE/AsyncResponse)
- [Provider examples — exact package and registration per channel, transport, and flow store](https://github.com/Sky4CE/AsyncResponse/blob/main/docs/provider-examples.md)
- [Configuration](https://github.com/Sky4CE/AsyncResponse/blob/main/docs/configuration.md)
- [Durable flows — checkpointed multi-step orchestration](https://github.com/Sky4CE/AsyncResponse/blob/main/docs/durable-flows.md)
- [Durable timers, delayed jobs, and cron-scheduled flows](https://github.com/Sky4CE/AsyncResponse/blob/main/docs/timers-and-scheduling.md)
- [Testing — the virtual-clock engine harness](https://github.com/Sky4CE/AsyncResponse/blob/main/docs/testing.md)
- [Recovery model](https://github.com/Sky4CE/AsyncResponse/blob/main/docs/recovery.md)
- [Observability](https://github.com/Sky4CE/AsyncResponse/blob/main/docs/observability.md)
- [Trimming and Native AOT](https://github.com/Sky4CE/AsyncResponse/blob/main/docs/aot.md)
- [License (MIT)](https://github.com/Sky4CE/AsyncResponse/blob/main/LICENSE)
