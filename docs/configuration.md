# Configuration

[← Back to README](../README.md)

`AddAsyncResponse()` registers the channel-agnostic engine but **no channel or transport** — chain
exactly one channel (`.WithInMemoryChannel()`, `.WithRedisChannel()`, `.WithNatsChannel()`,
`.WithPostgreSqlChannel(...)`, or `.WithSqlServerChannel(...)`) and exactly one transport
(`.WithInMemoryTransport()`, `.WithRedisTransport(...)`, `.WithAzureServiceBusTransport(...)`,
`.WithGooglePubSubTransport(...)`, `.WithRabbitMqTransport(...)`, `.WithNatsTransport(...)`,
`.WithPostgreSqlTransport(...)`, `.WithSqlServerTransport(...)`, or another full AsyncResponse
transport package). An app that starts without either one fails fast at host startup with setup guidance, so a
misconfiguration can never silently hang every waiter or drop worker dispatch. The recovery watchdog
is part of the engine and runs by default for whichever channel you choose.

This page is the consolidated options reference: engine options, durable-flow store package
options, channel options, transport options, and the per-transport delivery semantics behind them.

**On this page**

- [Engine options (`AsyncResponseOptions`)](#engine-options-asyncresponseoptions)
- [Durable-flow state store package options](#durable-flow-state-store-package-options)
- [Channel options](#channel-options)
- [Transport options](#transport-options) — including per-transport ACK/redelivery semantics
- [Redis-compatible servers](#redis-compatible-servers)

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
.WithInMemoryTransport();                                   // or .WithAzureServiceBusTransport(...) / .WithRabbitMqTransport(...)
```

## Engine options (`AsyncResponseOptions`)

Configured through the `AddAsyncResponse(options => …)` callback.

| Option | Default | Purpose |
|---|---|---|
| `Watchdog.Enabled` | `true` | Run the recovery watchdog in this host. Disable in all but one host when several share one store, so its scan and warnings aren't duplicated. |
| `Watchdog.Interval` | (see watchdog) | How often the watchdog scans persisted recovery state. |
| `Watchdog.StaleAfter` | (see watchdog) | Age at which an entry with no live waiter is reported stale. |
| `Watchdog.StartupDelay` | (see watchdog) | Delay before the first scan after host start. |
| `DurableFlows.StateExpiry` | 7 days | Idle TTL for persisted flow state; refreshed on every checkpoint, so it bounds the gap *between* checkpoints, not total run duration. |
| `DurableFlows.DefaultStepTimeout` | `null` (channel default) | Default timeout for `AwaitStepAsync` steps that don't pass one explicitly. |

See [durable-flows.md](durable-flows.md) for the flow API these options govern,
[recovery.md](recovery.md) for the watchdog in context, and [security.md](security.md) for
`.AuthorizeCallbacks(...)` and type-resolution registration, which are also chained off
`AddAsyncResponse()`.

## Durable-flow state store package options

Production durable flows should use an `AsyncResponse.DurableFlows.*` package or
`WithCustomDurableFlows<TStore>()`; the default recovery-backed store is for tests, development,
and migration.

| Package | Key options |
|---|---|
| `SqlServer` | `ConnectionString`, `SchemaName`, `TableName`, `AutoCreateSchema`, `PruneInterval` |
| `PostgreSQL` | `ConnectionString` or registered `NpgsqlDataSource`, `SchemaName`, `TableName`, `AutoCreateSchema`, `PruneInterval` |
| `MySql` | `ConnectionString`, `TableName`, `AutoCreateSchema`, `PruneInterval` |
| `Sqlite` | `ConnectionString`, `TableName`, `AutoCreateSchema`, `PruneInterval` |
| `Oracle` | `ConnectionString`, `TableName`, `AutoCreateSchema`, `PruneInterval` |
| `MongoDB` | `ConnectionString` or registered `IMongoDatabase`/`IMongoClient`, `DatabaseName`, `CollectionName`, `AutoCreateIndexes` |
| `Cosmos` | `ConnectionString` or registered `CosmosClient`, `DatabaseName`, `ContainerName`, `PartitionKeyPath`, `AutoCreateContainer`, `Throughput` |
| `DynamoDB` | registered/default `IAmazonDynamoDB`, `TableName`, `AutoCreateTable`, `EnableTimeToLive`, `TimeToLiveAttributeName` |

The SQL stores prune expired rows opportunistically on save, throttled by `PruneInterval`
(default 5 minutes; zero or negative prunes on every save); MongoDB, Cosmos, and DynamoDB use
native TTL instead. All packages register their store as a singleton and reuse a host-registered
client when one exists. See [durable-flow-state-stores.md](durable-flow-state-stores.md) for
package examples, lifetimes, cleanup mechanics, and schema ownership guidance.

## Channel options

Channel options are common where noted and channel-specific otherwise. They are set through the
channel registration callback (`.WithRedisChannel(options => …)`, `.WithNatsChannel(options => …)`,
`.WithPostgreSqlChannel(options => …)`, `.WithSqlServerChannel(options => …)`).

| Option | Channels | Default | Purpose |
|---|---|---|---|
| `KeyPrefix` | Redis | — | Isolate apps/environments sharing one Redis. **Persisted — treat as a deployment contract.** |
| `SubjectPrefix` | NATS | `asyncresponse` | Response subjects: `{prefix}.response.{cid}`. |
| `RecoveryBucket` | NATS | `asyncresponse-recovery` | JetStream KV bucket for recovery state. |
| `SchemaName` | PostgreSQL, SQL Server | `public` / `dbo` | Schema that contains the channel tables. |
| `ConnectionString` | SQL Server | — | SQL Server connection string; must point at an existing database (the package creates schema/tables, never the database). |
| `RecoveryStateTable` | PostgreSQL, SQL Server | `asyncresponse_recovery_state` | Durable lost-subscriber recovery registrations, one row per waiter. |
| `MessageTable` | PostgreSQL, SQL Server | `asyncresponse_channel_messages` | Stored response envelopes loaded after `LISTEN/NOTIFY` wakeups (PostgreSQL) or by the adaptive polling sweep (SQL Server). |
| `SubscriberTable` | PostgreSQL, SQL Server | `asyncresponse_channel_subscribers` | Live waiter heartbeat rows used for subscriber counts and delivery confirmation. |
| `NotificationChannel` | PostgreSQL | `asyncresponse_channel_notify` | PostgreSQL `LISTEN/NOTIFY` channel; must be a simple identifier. |
| `AutoCreateSchema` | PostgreSQL, SQL Server | `true` | Create schema/tables/indexes on first use; set `false` when migrations own DDL. |
| `MessageRetention` | PostgreSQL, SQL Server | 1 hour | How long response envelope rows remain available for missed notification / cross-process sweep recovery. |
| `DeliveryConfirmationTimeout` | PostgreSQL, SQL Server | 5 seconds | How long a publisher waits for live waiter confirmation before routing to lost-subscriber recovery. |
| `DeliveryConfirmationPollInterval` | PostgreSQL, SQL Server | 50 ms | Poll cadence for cross-process delivery confirmation. |
| `ListenerPollInterval` | PostgreSQL | 250 ms | Missed-notification safety scan interval. |
| `ActivePollInterval` / `IdlePollInterval` | SQL Server | 250 ms / 2 s | Adaptive polling wake (SQL Server has no `LISTEN/NOTIFY`): sweep cadence while waiters are subscribed, and the backed-off cadence while idle. Same-process deliveries never wait for the sweep. |
| `PendingMessageBatchSize` | PostgreSQL, SQL Server | 64 | Rows loaded per subscribed correlation id per listener/sweep pass. |
| `SubscriberHeartbeatInterval` / `SubscriberHeartbeatTimeout` | PostgreSQL, SQL Server | 10s / 30s | Heartbeat cadence and liveness window for active waiters. |
| `RecoveryStateExpiry` | Redis, NATS, PostgreSQL, SQL Server | 7 days | How long durable recovery state survives. Also the default wait timeout backstop. Don't set below your longest flow duration. |
| `DefaultTimeout` | all | `RecoveryStateExpiry` | Default per-waiter timeout when a flow doesn't call `WithTimeout`. |
| `IncludeRemoteStackTrace` | Redis, NATS, PostgreSQL, SQL Server | `true` | Whether the remote exception's stack trace travels on the wire (`Exception.Data["RemoteStackTrace"]`). See [security.md](security.md). |
| `MaxRemoteStackTraceLength` | Redis, NATS, PostgreSQL, SQL Server | `16384` | Length cap (chars) applied to the remote stack trace on both publish and receive. |

## Transport options

Transport options are set through the transport registration callback. Each transport package owns
its own option type; the common shapes are summarized here. See the transport sections in
[the README's Quick start](../README.md#quick-start) for full examples.

| Option | Transports | Purpose |
|---|---|---|
| `KeyPrefix` / `SubjectPrefix` / `SchemaName` / `TopicPrefix` | Redis / NATS / PostgreSQL / SQL Server / Kafka | Namespace for worker and response streams/subjects/tables/topics. |
| `ConnectionString` | Azure Service Bus | Service Bus namespace connection string. Omit when you register your own singleton `ServiceBusClient`. |
| `ServiceUrl` / `Region` / `AccessKey` / `SecretKey` | SQS | Endpoint and credentials. All optional: omit everything to use the AWS SDK default chain, set `ServiceUrl` for LocalStack or a proxy, or register your own singleton `Amazon.SQS.IAmazonSQS` (e.g. via `AWSSDK.Extensions.NETCore.Setup`) and the package reuses it. |
| `ConnectionString` | SQL Server | SQL Server connection string; must point at an existing database (the package creates schema/table/indexes, never the database). |
| `BootstrapServers` | Kafka | Comma-separated broker list. The package speaks the Kafka protocol via `Confluent.Kafka`, so Redpanda, Amazon MSK, WarpStream, Aiven, and Confluent Cloud all work. |
| `WorkerTopic` / `ResponseTopic` + `WorkerConsumerGroup` / `ResponseConsumerGroup` | Kafka | Topics for worker jobs and response ingress (default `{TopicPrefix}.transport.worker` / `.response`) and the consumer group per role. |
| `CreateTopics` / `TopicNumPartitions` / `TopicReplicationFactor` | Kafka | Provision missing topics on subscriber startup. Partitions are the unit of consumer parallelism and ordering; `-1` uses broker defaults. |
| `OffsetCommitInterval` | Kafka | Auto-commit cadence for offsets stored after each resolved message; a crash inside the window redelivers at-least-once. |
| `DeadLetterTopic` / `DeadLetterTopicSuffix` | Kafka | Explicit dead-letter topic, or the suffix appended per source topic (default `.deadletter` → `{topic}.deadletter`). |
| `ConfigureProducer` / `ConfigureConsumer` / `ConfigureAdminClient` | Kafka | Last-chance hooks over the Confluent client configs (security, compression, fetch tuning, `max.poll.interval.ms`, …). |
| `WorkerQueue` / `ResponseQueue` | Azure Service Bus | Service Bus queues used for worker jobs and response ingress; they must be distinct. |
| `WorkerQueue` / `ResponseQueue` | SQS | Queues for worker jobs and response ingress (distinct); each accepts a queue name (resolved once via `GetQueueUrl`) or a full queue URL. A name/URL ending in `.fifo` opts into FIFO publishing: the correlation id becomes the `MessageGroupId` (one flow's jobs stay ordered) and every message carries a unique `MessageDeduplicationId`. |
| `CreateQueues` / `DeadLetterQueueSuffix` / `MaxReceiveCount` | SQS | Provision the queues on startup, each with a native dead-letter queue (`{queue}-dlq`) wired through a redrive policy: SQS counts receives (`ApproximateReceiveCount`) and moves a message to the DLQ after `MaxReceiveCount`. Off by default — point at existing queues in production. |
| `WorkerSubscriber.VisibilityTimeout` / `RedeliveryDelay` | SQS | Per-receive visibility timeout (`null` uses the queue's setting) and the optional shortened invisibility applied via `ChangeMessageVisibility` when a handler fails; `null` lets the visibility timeout expire naturally. |
| `MessageTable` | PostgreSQL, SQL Server | Single queue table containing worker, response-ingress, and dead-letter rows. |
| `WorkerQueue` / `ResponseQueue` / `DeadLetterQueue` | PostgreSQL, SQL Server | Logical queue names stored in the queue table. They must be distinct. |
| `NotificationChannel` | PostgreSQL | `LISTEN/NOTIFY` channel that wakes PostgreSQL subscribers after publishes or retries. SQL Server has no equivalent: same-process publishes wake subscribers through an in-process signal, and cross-process rows are picked up within `EmptyPollDelay`. |
| `LockTimeout` | PostgreSQL, SQL Server | How long a claimed row stays locked before another subscriber may retry it. |
| `MaxMessagesPerReceive` / `ReceiveWaitTime` | Azure Service Bus, SQS | Receive-loop batch size and long-poll timeout for queue subscribers. SQS caps them at 10 messages and 20 seconds (the defaults). |
| `WorkerSubscriber.UseAckAfterEnqueue(...)` / `UseAckAfterReceive(...)` | all broker transports | Opt-in early-ACK dispatch for long-running workers: bounded in-process queue, configurable worker count, capacity, and drain timeout. |
| `WorkerSubscriber.MaxDeliveryAttempts` | all broker transports except Google Pub/Sub and SQS | Redeliveries before dead-lettering. Google Pub/Sub and SQS perform redelivery natively — bound attempts with the subscription's `DeadLetterPolicy` (Pub/Sub) or the queue's redrive policy `maxReceiveCount` (SQS, provisioned by `CreateQueues` or your infra). On RabbitMQ, values above 2 require a TTL-retry dead-letter cycle (plain `basic.nack` requeues are not counted by the broker) and log a startup warning otherwise. On Kafka, attempts are in-process retries with backoff (`HandlerRetryBaseDelay`/`HandlerRetryMaxDelay`) counted per process delivery — offsets cannot NACK a single message. |
| `SubscriberRetryBaseDelay` / `SubscriberRetryMaxDelay` | Google Pub/Sub, SQS | Bounded backoff for restarting a failed hosted subscriber (streaming-pull/long-poll fault, transient auth/startup errors). |
| `WorkerSubscriber.OnBackgroundFailure` | all broker transports | Hook for operator-visible metrics, alerting, or a durable dead-letter path when a background handler fails after early ACK. |
| `HostShutdownTimeout` | all broker transports | Must accommodate `ShutdownTimeout + BackgroundDrainTimeout`; mirror any custom `HostOptions.ShutdownTimeout`. |
| `DeclareTopology` | RabbitMQ | Declare durable exchanges/queues/bindings (`true`) or leave topology to your infra team (`false`). |
| `CorrelationIdAttribute` / `CorrelationIdHeader` / `CorrelationIdProperty` | Pub/Sub / SQS / RabbitMQ / Kafka / NATS / PostgreSQL / SQL Server / Azure Service Bus | Broker metadata key used to resolve the correlation id before falling back to JSON body paths. On Kafka the correlation id also becomes the message key, keeping one flow's jobs ordered within a partition; on FIFO SQS queues it becomes the `MessageGroupId` with the same per-flow ordering effect. |
| `CorrelationIdJsonPaths` | broker transports | JSON paths inspected when metadata does not carry the correlation id. PostgreSQL and SQL Server also unwrap nested JSON strings at those paths. |
| `DeadLetterEnabled` / `DeadLetterRetention` | Redis / NATS / PostgreSQL / SQL Server / Kafka | Whether poison messages are preserved and, for PostgreSQL and SQL Server, how long dead-letter rows are retained. |

A worker handler failure propagates out of the ingress to the transport dispatcher, which owns the
retry decision: in `AckAfterHandlerCompletes` the delivery is NACKed/abandoned and redelivered up
to `MaxDeliveryAttempts`, then dead-lettered; after an early ACK the failure is reported through
`OnBackgroundFailure` (and written to the transport's own dead-letter queue where one exists). A
failing worker never completes the waiter by itself — publish a failure response from the worker's
error handling when the flow should fail fast instead of waiting out its timeout.

Azure Service Bus uses peek-lock settlement. In `AckAfterHandlerCompletes`, a successful handler
completes the message, failures abandon it until `MaxDeliveryAttempts`, then dead-letter it through
Service Bus. In `AckAfterReceive`, the message is completed as soon as it enters the bounded
background queue; later handler failures cannot be broker-dead-lettered because the lock is gone, so
use `OnBackgroundFailure` for metrics, alerts, or a custom durable failure path. Mind the peek-lock
budget: a receive batch is processed sequentially, so the last message in a batch waits up to
`MaxMessagesPerReceive × handler latency` before settlement — keep that product well under the
queue's lock duration (or lower `MaxMessagesPerReceive`) to avoid `MessageLockLostException`
redeliveries of already-processed messages.

AWS SQS uses visibility-timeout settlement with long-poll `ReceiveMessage` (up to 10 messages and
20 seconds per call). In `AckAfterHandlerCompletes`, a successful handler deletes the message;
failures leave it invisible until the visibility timeout expires (or shorten the wait with
`RedeliveryDelay` via `ChangeMessageVisibility`), SQS redelivers it with an incremented
`ApproximateReceiveCount`, and the queue's redrive policy dead-letters it after `maxReceiveCount`
receives — redelivery accounting and the DLQ are fully native, so there is no app-level
`MaxDeliveryAttempts`. In `AckAfterEnqueue`, the message is deleted as soon as it enters the bounded
background queue; later handler failures cannot be redelivered because the message is gone, so use
`OnBackgroundFailure` for metrics, alerts, or a custom durable failure path. Mind the visibility
budget the same way as the Service Bus lock budget: a receive batch is processed sequentially, so
keep `MaxMessagesPerReceive × handler latency` under the queue's visibility timeout (or set
`WorkerSubscriber.VisibilityTimeout` higher / `MaxMessagesPerReceive` lower) to avoid duplicate
executions of already-processed messages. FIFO queues are opt-in by naming the queue `*.fifo`.

Kafka is built on classic consumer groups with manual offset management (`enable.auto.commit=true` +
`enable.auto.offset.store=false`; an offset is stored only once its message is fully resolved). Two
consequences to plan for: ordering is per-partition, so consumer parallelism equals the partition
count — size `TopicNumPartitions` accordingly — and a slow or retrying message delays its partition
(head-of-line blocking). Keep the worst-case in-process retry budget
(`MaxDeliveryAttempts × HandlerRetryMaxDelay`) well under the consumer's `max.poll.interval.ms`
(default 5 minutes, adjustable via `ConfigureConsumer`) or the broker evicts the consumer mid-retry.
In `AckAfterEnqueue`, the offset is stored at enqueue time and partition fetching pauses while the
bounded in-process queue is full; later handler failures are retried in-process, then reported via
`OnBackgroundFailure` and produced to the dead-letter topic with failure-detail headers. The message
that exhausts its attempts is always dead-lettered *and committed* so the partition keeps moving.

Redis Streams uses consumer groups. Each subscriber reads new entries with `XREADGROUP`; an entry
stays in the group's pending-entries list until the handler acknowledges it with `XACK`. A separate
reclaim loop takes over entries idle longer than `PendingMessageMinIdleTime` (`XPENDING` + `XCLAIM`),
so a crashed consumer's in-flight work is retried by a peer rather than stranded. `MaxDeliveryAttempts`
is counted from the stream delivery count; on exhaustion the entry is written to the dead-letter stream
and ACKed off the source. `AckAfterEnqueue` ACKs at enqueue time and reports later handler failures
through `OnBackgroundFailure`. Streams are trimmed with plain `XADD … MAXLEN ~ N` (no Redis 8 trim-mode
token), which keeps the transport portable across Redis-compatible servers (see below).

RabbitMQ uses publisher confirms and mandatory routing on publish, and per-message `basic.ack` /
`basic.nack` on consume, with a dead-letter exchange for poison messages. Because the broker does not
count plain `basic.nack`-requeues, a `WorkerSubscriber.MaxDeliveryAttempts` above 2 requires a
TTL-retry dead-letter cycle (declared when `DeclareTopology` is on); values above 2 without that cycle
log a startup warning. `AckAfterEnqueue` ACKs after the bounded enqueue and routes later failures to
`OnBackgroundFailure`.

Google Pub/Sub uses streaming pull with the client library extending the ack deadline while a handler
runs. Redelivery and dead-lettering are **native**, like SQS: bound them with the subscription's
`DeadLetterPolicy` and `maxDeliveryAttempts` rather than an app-level `MaxDeliveryAttempts`. A failed
handler NACKs the message for Pub/Sub to redeliver; `AckAfterEnqueue` ACKs after enqueue and reports
later failures through `OnBackgroundFailure`.

NATS JetStream uses explicit acknowledgement. A successful handler `ACK`s; a failure `NAK`s with a
delay so JetStream redelivers after a backoff, and a message that reaches `MaxDeliveryAttempts` is
written to the dead-letter stream. Consumers are durable, so a restarted subscriber resumes from its
last acknowledged position. `AckAfterReceive` ACKs as soon as the message enters the bounded queue.

The PostgreSQL and SQL Server transports are table-backed queues: a publish is an idempotent
`INSERT`, and each subscriber claims a batch of rows atomically — PostgreSQL with
`FOR UPDATE SKIP LOCKED`, SQL Server with `UPDLOCK, ROWLOCK, READPAST` (the same skip-locked effect) —
so competing subscribers never claim the same row. PostgreSQL wakes subscribers with `LISTEN/NOTIFY`;
SQL Server has no equivalent, so it wakes same-process subscribers through an in-process signal and
picks up cross-process rows within its adaptive poll interval. A claimed row stays locked for
`LockTimeout` before another subscriber may retry it, and a row that reaches `MaxDeliveryAttempts` is
moved to the dead-letter queue (rows in the same table). See [postgresql.md](postgresql.md) for the
PostgreSQL table layout, delivery-confirmation details, and connection-string tuning, and
[sqlserver.md](sqlserver.md) for the SQL Server pair (adaptive polling wake, `UPDLOCK/READPAST`
claims, application-lock DDL, and operational notes).

### Redis-compatible servers

The Redis channel (`AsyncResponse.Channels.Redis`) and Redis Streams transport
(`AsyncResponse.Transports.Redis`) talk RESP through `StackExchange.Redis` and use only widely
implemented commands, so they run unchanged on Redis-compatible servers. The command surface is:

| Component | Commands used | Portability |
|---|---|---|
| Channel | `SUBSCRIBE`/`PUBLISH` + `PUBSUB NUMSUB` (the "is anyone listening?" lost-subscriber probe), `GET`/`SET EX`/`DEL`, `SCAN`, and `WATCH`/`MULTI`/`EXEC` for the recovery-state CAS | pub/sub, strings, `SCAN`, transactions — universally supported |
| Transport | Streams: `XADD` (with `MAXLEN ~` approximate trim, no Redis 8 trim-mode token), `XGROUP CREATE`, `XREADGROUP`, `XPENDING`, `XCLAIM`, `XACK` | requires Redis Streams + consumer groups (Redis 5+) |

| Server | Channel | Transport | Notes |
|---|---|---|---|
| **Redis** 5+/8 | ✅ | ✅ | reference implementation |
| **Valkey** 7.2 / 8 | ✅ | ✅ | drop-in; the full Redis-backed integration suite passes against it end-to-end in CI |
| **Dragonfly** | ✅ | ✅ | RESP-compatible (channel + Streams transport commands validated against a live server); connect directly |
| **Garnet** 1.0 | ✅ | ❌ | implements pub/sub, strings, and `SCAN`, so it works as a **channel**, but has no stream commands, so it cannot back the Streams **transport** |
| **ElastiCache / MemoryDB / Azure Managed Redis** | ✅ | ✅ | managed Redis/Valkey — same command surface |

No configuration change is needed — point `IConnectionMultiplexer` at the server. A scheduled CI
matrix reruns the whole Redis-backed integration suite against **Valkey** to hold this claim; Valkey
is a true drop-in for the Aspire test harness (it shares the redis container launch contract).
**Dragonfly** is RESP-compatible and validated by running the real channel + transport against a live
server, but its container entrypoint differs from the redis image, so it is not exercised through that
Aspire harness — connect to it the same way you would any Redis. **Garnet** is validated as a channel
only; pairing it with the Redis transport fails fast because the stream commands are absent.
