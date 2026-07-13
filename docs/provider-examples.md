# Channel, transport, and flow-store examples

[← Back to the documentation index](README.md)

AsyncResponse has three independent infrastructure choices:

1. one **channel** delivers responses to active waiters and stores lost-waiter recovery metadata;
2. one **transport** moves worker jobs and response-ingress messages;
3. one **durable-flow store** owns flow ledgers and completes the registration.

Choose exactly one of each. Even applications that do not yet start durable flows select a store;
`.WithInMemoryDurableFlows()` is the zero-infrastructure choice. The provider snippets below are
deliberately small and copyable; option defaults and delivery semantics remain in
[configuration.md](configuration.md).

**On this page**

- [Complete local setup](#complete-local-setup)
- [Channel examples](#channel-examples)
- [Transport examples](#transport-examples)
- [Durable-flow store examples](#durable-flow-store-examples)
- [Complete production composition](#complete-production-composition)

## Complete local setup

No external infrastructure and no additional package:

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport(options =>
    {
        options.QueueCapacity = 1_024;
        options.WorkerCount = 1;
    })
    .WithInMemoryDurableFlows();
```

The in-memory store satisfies the same atomic flow-state contract as provider stores, but its state
exists only for this process's lifetime.

## Channel examples

Each channel example uses the in-memory transport and flow store to isolate the channel choice.
Replace either one independently when jobs or flow ledgers must leave the process.

| Channel | NuGet package | Required infrastructure |
|---|---|---|
| [In-memory](#in-memory-channel) | `AsyncResponse.Core` | None |
| [Redis](#redis-channel) | `AsyncResponse.Channels.Redis` | Shared `IConnectionMultiplexer` |
| [NATS](#nats-channel) | `AsyncResponse.Channels.NATS` | Shared `INatsConnection`; JetStream enabled |
| [PostgreSQL](#postgresql-channel) | `AsyncResponse.Channels.PostgreSQL` | Shared `NpgsqlDataSource` |
| [SQL Server](#sql-server-channel) | `AsyncResponse.Channels.SqlServer` | Existing database + connection string |
| [MongoDB](#mongodb-channel) | `AsyncResponse.Channels.MongoDB` | Connection string or shared Mongo client/database |

### In-memory channel

```csharp
builder.Services.AddAsyncResponse()
    .WithInMemoryChannel(options =>
        options.DefaultTimeout = TimeSpan.FromMinutes(10))
    .WithInMemoryTransport()
    .WithInMemoryDurableFlows();
```

Waiters and recovery metadata exist only in this process.

### Redis channel

```csharp
using StackExchange.Redis;

var connectionString = builder.Configuration.GetConnectionString("Redis")
    ?? throw new InvalidOperationException("ConnectionStrings:Redis is required.");

builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect(connectionString));

builder.Services.AddAsyncResponse()
    .WithRedisChannel(options =>
    {
        options.KeyPrefix = "orders";
        options.RecoveryStateExpiry = TimeSpan.FromDays(7);
        options.DefaultTimeout = TimeSpan.FromHours(12);
    })
    .WithInMemoryTransport()
    .WithInMemoryDurableFlows();
```

Reuse the same multiplexer when the Redis Streams transport is also selected. `KeyPrefix` is a
deployment contract: every publisher and waiter must use the same value.

### NATS channel

```csharp
using NATS.Client.Core;

var natsUrl = builder.Configuration["Nats:Url"] ?? "nats://localhost:4222";

builder.Services.AddSingleton<INatsConnection>(
    _ => new NatsConnection(new NatsOpts { Url = natsUrl }));

builder.Services.AddAsyncResponse()
    .WithNatsChannel(options =>
    {
        options.SubjectPrefix = "orders";
        options.RecoveryBucket = "orders-recovery";
        options.RecoveryBucketReplicas = 1; // raise on a multi-node production cluster
    })
    .WithInMemoryTransport()
    .WithInMemoryDurableFlows();
```

The response path uses NATS Core request/reply. Lost-waiter recovery uses a JetStream Key-Value
bucket, so JetStream must be enabled. Use one replica for a single-node development server.

### PostgreSQL channel

```csharp
using Npgsql;

var connectionString = builder.Configuration.GetConnectionString("PostgreSQL")
    ?? throw new InvalidOperationException("ConnectionStrings:PostgreSQL is required.");

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));

builder.Services.AddAsyncResponse()
    .WithPostgreSqlChannel(options =>
    {
        options.SchemaName = "public";
        options.NotificationChannel = "orders_asyncresponse";
        options.AutoCreateSchema = true;
    })
    .WithInMemoryTransport()
    .WithInMemoryDurableFlows();
```

`LISTEN/NOTIFY` is only a wake signal; retained table rows remain the source of truth. Reuse the same
data source with the PostgreSQL transport and flow store when they are selected.

### SQL Server channel

```csharp
var connectionString = builder.Configuration.GetConnectionString("SqlServer")
    ?? throw new InvalidOperationException("ConnectionStrings:SqlServer is required.");

builder.Services.AddAsyncResponse()
    .WithSqlServerChannel(options =>
    {
        options.ConnectionString = connectionString;
        options.SchemaName = "dbo";
        options.AutoCreateSchema = true;
    })
    .WithInMemoryTransport()
    .WithInMemoryDurableFlows();
```

The database must already exist. The package creates its schema, tables, and indexes when
`AutoCreateSchema` is enabled.

### MongoDB channel

```csharp
var connectionString = builder.Configuration.GetConnectionString("MongoDB")
    ?? throw new InvalidOperationException("ConnectionStrings:MongoDB is required.");

builder.Services.AddAsyncResponse()
    .WithMongoDbChannel(options =>
    {
        options.ConnectionString = connectionString;
        options.DatabaseName = "orders";
        options.AutoCreateIndexes = true;
        options.UseChangeStreams = true;
    })
    .WithInMemoryTransport()
    .WithInMemoryDurableFlows();
```

Change-stream wake requires a replica set; a single-node replica set is enough. On a standalone
server the channel falls back to `ListenerPollInterval` polling. A registered `IMongoDatabase`, or
an `IMongoClient` plus `DatabaseName`, is reused automatically.

## Transport examples

Each transport example uses the in-memory channel and flow store to isolate worker delivery.
Replace either one independently when waiter recovery or flow ledgers must survive a restart.

| Transport | NuGet package | Required infrastructure |
|---|---|---|
| [In-memory](#in-memory-transport) | `AsyncResponse.Core` | None |
| [Redis Streams](#redis-streams-transport) | `AsyncResponse.Transports.Redis` | Shared `IConnectionMultiplexer` |
| [RabbitMQ](#rabbitmq-transport) | `AsyncResponse.Transports.RabbitMQ` | RabbitMQ connection |
| [Azure Service Bus](#azure-service-bus-transport) | `AsyncResponse.Transports.AzureServiceBus` | Connection string or shared `ServiceBusClient` |
| [Google Pub/Sub](#google-pubsub-transport) | `AsyncResponse.Transports.GooglePubSub` | Project, topics, subscriptions, credentials |
| [AWS SQS](#aws-sqs-transport) | `AsyncResponse.Transports.SQS` | AWS SDK credentials/region or shared `IAmazonSQS` |
| [Kafka](#kafka-transport) | `AsyncResponse.Transports.Kafka` | Kafka-compatible bootstrap servers |
| [NATS JetStream](#nats-jetstream-transport) | `AsyncResponse.Transports.NATS` | Shared `INatsConnection`; JetStream enabled |
| [PostgreSQL](#postgresql-transport) | `AsyncResponse.Transports.PostgreSQL` | Shared `NpgsqlDataSource` |
| [SQL Server](#sql-server-transport) | `AsyncResponse.Transports.SqlServer` | Existing database + connection string |
| [MongoDB](#mongodb-transport) | `AsyncResponse.Transports.MongoDB` | Connection string or shared Mongo client/database |

### In-memory transport

```csharp
builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport(options =>
    {
        options.QueueCapacity = 1_024; // PublishAsync waits asynchronously when full
        options.WorkerCount = 4;       // independent jobs may run concurrently
    })
    .WithInMemoryDurableFlows();
```

### Redis Streams transport

```csharp
using StackExchange.Redis;

var connectionString = builder.Configuration.GetConnectionString("Redis")
    ?? throw new InvalidOperationException("ConnectionStrings:Redis is required.");

builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect(connectionString));

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithRedisTransport(options =>
    {
        options.KeyPrefix = "orders";
        options.WorkerConsumerGroup = "orders-workers";
        options.ResponseConsumerGroup = "orders-responses";
        options.StreamMaxLength = 100_000;
    })
    .WithInMemoryDurableFlows();
```

The transport uses consumer groups, pending-entry reclaim, and a dead-letter stream. Redis Streams
requires Redis 5 or a compatible server that implements the Streams command set.

### RabbitMQ transport

```csharp
var connectionString = builder.Configuration.GetConnectionString("RabbitMQ")
    ?? throw new InvalidOperationException("ConnectionStrings:RabbitMQ is required.");

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithRabbitMqTransport(options =>
    {
        options.ConnectionString = connectionString;
        options.WorkerExchange = "orders.worker";
        options.WorkerQueue = "orders.worker";
        options.WorkerRoutingKey = "orders.worker";
        options.ResponseExchange = "orders.response";
        options.ResponseQueue = "orders.response";
        options.ResponseRoutingKey = "orders.response";
        options.DeclareTopology = true;
    })
    .WithInMemoryDurableFlows();
```

`DeclareTopology = true` creates durable exchanges, queues, bindings, and the configured retry/DLQ
topology. Set it to `false` when infrastructure tooling owns those resources.

### Azure Service Bus transport

```csharp
var connectionString = builder.Configuration.GetConnectionString("AzureServiceBus")
    ?? throw new InvalidOperationException("ConnectionStrings:AzureServiceBus is required.");

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithAzureServiceBusTransport(options =>
    {
        options.ConnectionString = connectionString;
        options.WorkerQueue = "orders-worker";
        options.ResponseQueue = "orders-response";
        options.CorrelationIdProperty = "correlationId";
    })
    .WithInMemoryDurableFlows();
```

Register a singleton `ServiceBusClient` instead of setting `ConnectionString` when the application
uses Azure Identity or custom client options. Worker and response queues must be distinct.

### Google Pub/Sub transport

```csharp
builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithGooglePubSubTransport(options =>
    {
        options.ProjectId = "my-gcp-project";
        options.WorkerTopicId = "orders-worker";
        options.WorkerSubscriptionId = "orders-worker-sub";
        options.ResponseTopicId = "orders-response";
        options.ResponseSubscriptionId = "orders-response-sub";
    })
    .WithInMemoryDurableFlows();
```

Provision the topics and subscriptions separately. Google application-default credentials are used
by the client libraries. Configure redelivery limits with each subscription's `DeadLetterPolicy`.

### AWS SQS transport

```csharp
builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithSqsTransport(options =>
    {
        options.Region = "eu-central-1";
        options.WorkerQueue = "orders-worker";
        options.ResponseQueue = "orders-response";
        options.CreateQueues = true;  // convenient for development; prefer IaC in production
        options.MaxReceiveCount = 5;  // redrive to the provisioned DLQs after five receives
    })
    .WithInMemoryDurableFlows();
```

Omit endpoint and credentials to use the AWS SDK default chain, or register one `IAmazonSQS` for the
application. Queue names ending in `.fifo` opt into FIFO publishing; the correlation id becomes the
message-group id.

### Kafka transport

```csharp
var bootstrapServers = builder.Configuration["Kafka:BootstrapServers"]
    ?? throw new InvalidOperationException("Kafka:BootstrapServers is required.");

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithKafkaTransport(options =>
    {
        options.BootstrapServers = bootstrapServers;
        options.TopicPrefix = "orders";
        options.TopicNumPartitions = 12;
        options.CreateTopics = true;
    })
    .WithInMemoryDurableFlows();
```

The package speaks the Kafka protocol and also works with Redpanda, Amazon MSK, WarpStream, Aiven,
and Confluent Cloud. Correlation ids are message keys, so ordering and head-of-line blocking are
per partition.

### NATS JetStream transport

```csharp
using NATS.Client.Core;

var natsUrl = builder.Configuration["Nats:Url"] ?? "nats://localhost:4222";

builder.Services.AddSingleton<INatsConnection>(
    _ => new NatsConnection(new NatsOpts { Url = natsUrl }));

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithNatsTransport(options =>
    {
        options.SubjectPrefix = "orders";
        options.CreateStreams = true;
        options.StreamMaxMessages = 100_000;
    })
    .WithInMemoryDurableFlows();
```

Reuse the same `INatsConnection` when the NATS channel is selected. The transport uses durable
JetStream consumers with explicit ACK and delayed NAK redelivery.

### PostgreSQL transport

```csharp
using Npgsql;

var connectionString = builder.Configuration.GetConnectionString("PostgreSQL")
    ?? throw new InvalidOperationException("ConnectionStrings:PostgreSQL is required.");

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithPostgreSqlTransport(options =>
    {
        options.SchemaName = "public";
        options.MessageTable = "asyncresponse_transport_messages";
        options.NotificationChannel = "orders_transport";
        options.AutoCreateSchema = true;
    })
    .WithInMemoryDurableFlows();
```

Competing workers claim rows with `FOR UPDATE SKIP LOCKED`; `LISTEN/NOTIFY` wakes idle subscribers.

### SQL Server transport

```csharp
var connectionString = builder.Configuration.GetConnectionString("SqlServer")
    ?? throw new InvalidOperationException("ConnectionStrings:SqlServer is required.");

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithSqlServerTransport(options =>
    {
        options.ConnectionString = connectionString;
        options.SchemaName = "dbo";
        options.MessageTable = "asyncresponse_transport_messages";
        options.AutoCreateSchema = true;
    })
    .WithInMemoryDurableFlows();
```

Competing workers claim rows with `UPDLOCK`, `ROWLOCK`, and `READPAST`. The target database must
already exist.

### MongoDB transport

```csharp
var connectionString = builder.Configuration.GetConnectionString("MongoDB")
    ?? throw new InvalidOperationException("ConnectionStrings:MongoDB is required.");

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithMongoDbTransport(options =>
    {
        options.ConnectionString = connectionString;
        options.DatabaseName = "orders";
        options.MessageCollection = "asyncresponse_transport_messages";
        options.AutoCreateIndexes = true;
        options.UseChangeStreamWake = true;
    })
    .WithInMemoryDurableFlows();
```

Messages are claimed atomically with `findOneAndUpdate`. Change-stream wake uses a replica set and
falls back to polling on standalone MongoDB.

## Durable-flow store examples

The flow-store guide keeps the state-store examples together with their atomicity, schema, client
lifetime, and expiry requirements:

| Store | NuGet package | Copy/paste example |
|---|---|---|
| In-memory | `AsyncResponse.Core` | [In-memory](durable-flow-state-stores.md#in-memory) |
| SQL Server | `AsyncResponse.DurableFlows.SqlServer` | [SQL Server](durable-flow-state-stores.md#sql-server) |
| PostgreSQL | `AsyncResponse.DurableFlows.PostgreSQL` | [PostgreSQL](durable-flow-state-stores.md#postgresql) |
| MySQL / MariaDB | `AsyncResponse.DurableFlows.MySql` | [MySQL or MariaDB](durable-flow-state-stores.md#mysql-or-mariadb) |
| SQLite | `AsyncResponse.DurableFlows.Sqlite` | [SQLite](durable-flow-state-stores.md#sqlite) |
| Oracle | `AsyncResponse.DurableFlows.Oracle` | [Oracle](durable-flow-state-stores.md#oracle) |
| MongoDB | `AsyncResponse.DurableFlows.MongoDB` | [MongoDB](durable-flow-state-stores.md#mongodb) |
| Azure Cosmos DB | `AsyncResponse.DurableFlows.Cosmos` | [Azure Cosmos DB](durable-flow-state-stores.md#azure-cosmos-db) |
| DynamoDB | `AsyncResponse.DurableFlows.DynamoDB` | [DynamoDB](durable-flow-state-stores.md#dynamodb) |
| Entity Framework Core | `AsyncResponse.DurableFlows.EFCore` | [EF Core](durable-flow-state-stores.md#entity-framework-core) |
| Application-owned | `AsyncResponse.Core` | [Custom atomic store](durable-flow-state-stores.md#application-owned-store) |

Flow-code examples are organized separately by behavior: [local and awaited steps](durable-flows.md),
[child flows](durable-flows.md#child-flows), [compensation](durable-flows.md#compensation),
[production patterns](durable-flows.md#cookbook-patterns-from-production-flows), and
[testing](durable-flows.md#testing-your-flows).

## Complete production composition

This example puts all three choices together: Redis owns waiter recovery, RabbitMQ moves worker and
response messages, and PostgreSQL stores atomic durable-flow ledgers.

```bash
dotnet add package AsyncResponse.Core
dotnet add package AsyncResponse.Channels.Redis
dotnet add package AsyncResponse.Transports.RabbitMQ
dotnet add package AsyncResponse.DurableFlows.PostgreSQL
```

```csharp
using StackExchange.Redis;

var redisConnectionString = builder.Configuration.GetConnectionString("Redis")
    ?? throw new InvalidOperationException("ConnectionStrings:Redis is required.");
var rabbitConnectionString = builder.Configuration.GetConnectionString("RabbitMQ")
    ?? throw new InvalidOperationException("ConnectionStrings:RabbitMQ is required.");
var postgresConnectionString = builder.Configuration.GetConnectionString("PostgreSQL")
    ?? throw new InvalidOperationException("ConnectionStrings:PostgreSQL is required.");

builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect(redisConnectionString));
builder.Services.AddScoped<TenantProvisioningFlow>();

builder.Services.AddAsyncResponse()
    .WithRedisChannel(options =>
    {
        options.KeyPrefix = "orders";
        options.RecoveryStateExpiry = TimeSpan.FromDays(14);
    })
    .WithRabbitMqTransport(options =>
    {
        options.ConnectionString = rabbitConnectionString;
        options.WorkerQueue = "orders-worker";
        options.ResponseQueue = "orders-response";
    })
    .WithPostgreSqlDurableFlows(options =>
    {
        options.StateExpiry = TimeSpan.FromDays(14);
        options.ExecutionLeaseDuration = TimeSpan.FromMinutes(1);
        options.ExecutionLeaseRenewInterval = TimeSpan.FromSeconds(20);
        options.ConnectionString = postgresConnectionString;
        options.SchemaName = "public";
        options.TableName = "asyncresponse_flow_state";
    });
```

Durable flows are checkpointed and replica-fenced, but their external side effects remain
at-least-once. Make step bodies and remote triggers idempotent.
