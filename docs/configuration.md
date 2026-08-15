# Configuration

[← Back to README](../README.md)

`AddAsyncResponse()` registers the channel-agnostic engine but **selects no channel, transport, or
durable-flow store** — chain
exactly one channel (`.WithInMemoryChannel()`, `.WithRedisChannel()`, `.WithNatsChannel()`,
`.WithPostgreSqlChannel(...)`, `.WithSqlServerChannel(...)`, or `.WithMongoDbChannel(...)`) and
exactly one transport (`.WithInMemoryTransport()`, `.WithRedisTransport(...)`,
`.WithAzureServiceBusTransport(...)`, `.WithGooglePubSubTransport(...)`,
`.WithRabbitMqTransport(...)`, `.WithSqsTransport(...)`, `.WithKafkaTransport(...)`,
`.WithNatsTransport(...)`, `.WithPostgreSqlTransport(...)`, `.WithSqlServerTransport(...)`, or
`.WithMongoDbTransport(...)`), and exactly one flow store (`.WithInMemoryDurableFlows()`, a
`.With*DurableFlows(...)` provider, or `.WithDurableFlows<TStore>()`). An app that starts without any
one of the three fails fast with setup guidance. The recovery watchdog is part of the engine and
runs by default for whichever channel you choose.

This page is the consolidated options reference: engine options, durable-flow store package
options, channel options, transport options, and the per-transport delivery semantics behind them.
For setup code rather than option lookup, use the
[channel, transport, and flow-store examples](provider-examples.md).

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
.WithInMemoryTransport()                                    // or .WithAzureServiceBusTransport(...) / .WithRabbitMqTransport(...)
.WithInMemoryDurableFlows(options =>
{
    options.StateExpiry = TimeSpan.FromDays(14);             // idle TTL, refreshed at checkpoints
    options.ExecutionLeaseDuration = TimeSpan.FromMinutes(1);
    options.ExecutionLeaseRenewInterval = TimeSpan.FromSeconds(20);
});
```

## Engine options (`AsyncResponseOptions`)

Configured through the `AddAsyncResponse(options => …)` callback.

| Option | Default | Purpose |
|---|---|---|
| `Watchdog.Enabled` | `true` | Run the recovery watchdog in this host. Disable in all but one host when several share one store, so its scan and warnings aren't duplicated. |
| `Watchdog.Interval` | 6 hours | How often the watchdog scans persisted recovery state. |
| `Watchdog.StaleAfter` | 24 hours | Age at which an entry with no live waiter is reported stale. |
| `Watchdog.MaxScanEntries` | 100 000 | Upper bound on recovery entries one scan buffers before probing (unique correlation ids + individual correlation-less entries — a memory bound, not a flow count). Larger stores are reported for the buffered subset only: the report carries `Truncated`, the recovery health check degrades, the `asyncresponse.recovery.scan_truncated` gauge reads 1, and a warning is logged. |
| `Watchdog.ProbeConcurrency` | 8 | Upper bound on liveness probes one scan runs concurrently — each probe is its own round trip to the channel, and a scan issues one per buffered entry, so probing strictly sequentially would serialize up to `MaxScanEntries` round trips. Must be at least 1. |
| `Watchdog.StartupDelay` | 5 minutes | Delay before the first scan after host start. |

The watchdog values in the [example above](#configuration) are exactly these defaults — shown so
you can see which knobs exist, not because they need changing.

See [recovery.md](recovery.md) for the watchdog in context and [security.md](security.md) for
`.AuthorizeCallbacks(...)` and type-resolution registration, which are also chained off
`AddAsyncResponse()`.

## Durable-flow state store package options

`AddAsyncResponse()` does not choose a flow store. Complete registration with exactly one
`AsyncResponse.DurableFlows.*` provider, `.WithInMemoryDurableFlows()` for a process-local setup, or
`.WithDurableFlows<TStore>()` for an application-owned atomic store. Every variant accepts the
common flow-engine options in its own callback; provider variants add store-specific properties to
that same options object.

### Common durable-flow options

| Option | Default | Purpose |
|---|---|---|
| `StateExpiry` | 14 days | Idle TTL for persisted flow state; refreshed on every checkpoint, so it bounds the gap *between* checkpoints, not total run duration. Deliberately double the 7-day default step-timeout chain so a silent step faults before its ledger expires. Also bounds the longest single `DelayAsync`/`DelayUntilAsync` sleep: the 3650-day persistence ceiling **minus** this value (default → 3636 days), so a sleeping ledger's TTL always outlives its own wake-up by the full idle margin. |
| `MaxFlowIdLength` (const) | 400 | Portable flow-id length in characters — the `flow_id` column length in the SQL Server, MySQL, Oracle, and EF Core stores. Every final id (root, composed child `:{stepName}`, scheduled `sched:{name}:{timestamp}`) is validated at creation, so an id cannot work on one store and fail on another. |
| `MaxFlowIdBytes` (const) | 1023 | Portable flow-id size in UTF-8 bytes — the Cosmos DB id limit, which 400 multi-byte characters exceed. Ids must also avoid `/`, `\`, `?`, `#` (Cosmos rejects them) and control characters, and are compared ordinally (a binary collation is pinned on the relational columns). |
| `DefaultStepTimeout` | `null` (channel default) | Default timeout for `AwaitStepAsync` steps that don't pass one explicitly. |
| `ExecutionLeaseDuration` | 1 minute | How long one store lease owns a flow execution before another replica may take over after owner loss. |
| `ExecutionLeaseRenewInterval` | 20 seconds | Renewal cadence; must be positive and shorter than `ExecutionLeaseDuration`. |
| `ProgressPersistenceInterval` | 1 second | Minimum interval between writes caused only by progress reports. Faster updates are coalesced into the next checkpoint/outcome; zero writes every report. |
| `TimerInProcessThreshold` | 10 seconds | Timer remainders (`flow.DelayAsync`) at or under this wait in process under the execution lease; longer remainders suspend the run behind a delayed wake-up job when the transport supports native delayed delivery. Zero always prefers suspension; on transports without delayed delivery every timer waits in process regardless. See [timers-and-scheduling.md](timers-and-scheduling.md). |
| `MaxStateBytes` | DynamoDB 350 000 · Cosmos 1 900 000 · MongoDB 15 000 000 · `null` (unlimited) elsewhere | Serialized-ledger size budget checked on every create/checkpoint. An oversized write fails with a diagnosable error (flow id, size, limit) instead of the raw provider error, before the run burns redeliveries — defaults sit under each provider's hard item/document cap. Keep large payloads in your own storage and pass references (see the ledger-size note in [durable-flows.md](durable-flows.md#child-flows)). |

Configure these on the selected store, for example:

```csharp
.WithPostgreSqlDurableFlows(options =>
{
    options.StateExpiry = TimeSpan.FromDays(14);             // common engine option
    options.ExecutionLeaseDuration = TimeSpan.FromMinutes(1); // common engine option
    options.ConnectionString = connectionString;              // PostgreSQL store option
    options.SchemaName = "public";
})
// Cron-scheduled flows are registered on the same builder: five-field cron (validated here),
// optional time zone, and a deterministic input factory (it must produce the same value on
// every replica for the same occurrence).
.WithScheduledFlow<NightlyReportFlow, ReportInput>(
    "nightly-report", "0 6 * * *",
    occurrence => new ReportInput(occurrence),
    schedule => schedule.TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Europe/Berlin"));
```

### Provider-specific options

| Package | Provider-specific options (in addition to the common options above) |
|---|---|
| `SqlServer` | `ConnectionString`, `SchemaName`, `TableName`, `AutoCreateSchema`, `PruneInterval` |
| `PostgreSQL` | `ConnectionString` or registered `NpgsqlDataSource`, `SchemaName`, `TableName`, `AutoCreateSchema`, `PruneInterval` |
| `MySql` | `ConnectionString`, `TableName`, `AutoCreateSchema`, `PruneInterval` |
| `Sqlite` | `ConnectionString`, `TableName`, `AutoCreateSchema`, `PruneInterval` |
| `Oracle` | `ConnectionString`, `TableName`, `AutoCreateSchema`, `PruneInterval` |
| `MongoDB` | `ConnectionString` or registered `IMongoDatabase`/`IMongoClient`, `DatabaseName`, `CollectionName`, `AutoCreateIndexes` |
| `Cosmos` | `ConnectionString` or registered `CosmosClient`, `DatabaseName`, `ContainerName`, `PartitionKeyPath`, `AutoCreateContainer`, `Throughput` |
| `DynamoDB` | registered/default `IAmazonDynamoDB`, `TableName`, `AutoCreateTable`, `EnableTimeToLive`, `TimeToLiveAttributeName` |
| `EFCore` | application `DbContext` mapping via `ConfigureAsyncResponseDurableFlows(...)`; schema changes are owned by your EF migrations |

The SQL stores prune expired rows opportunistically on flow creation, throttled by `PruneInterval`
(default 5 minutes; zero or negative prunes on every save); MongoDB, Cosmos, and DynamoDB use
native TTL instead. All packages register their store as a singleton and reuse a host-registered
client when one exists. See [durable-flow-state-stores.md](durable-flow-state-stores.md) for
package examples, lifetimes, cleanup mechanics, and schema ownership guidance.

## Channel options

Channel options are common where noted and channel-specific otherwise. They are set through the
channel registration callback (`.WithRedisChannel(options => …)`, `.WithNatsChannel(options => …)`,
`.WithPostgreSqlChannel(options => …)`, `.WithSqlServerChannel(options => …)`,
`.WithMongoDbChannel(options => …)`).

Every channel has a complete registration in [provider-examples.md](provider-examples.md#channel-examples).

| Option | Channels | Default | Purpose |
|---|---|---|---|
| `KeyPrefix` | Redis | `asyncresponse` | Isolate apps/environments sharing one Redis. **Persisted — treat as a deployment contract.** |
| `SubjectPrefix` | NATS | `asyncresponse` | Response subjects: `{prefix}.response.{cid}`. |
| `RecoveryBucket` | NATS | `asyncresponse-recovery` | JetStream KV bucket for recovery state. |
| `SchemaName` | PostgreSQL, SQL Server | `public` / `dbo` | Schema that contains the channel tables. |
| `ConnectionString` | SQL Server, MongoDB | — | SQL Server: connection string; must point at an existing database (the package creates schema/tables, never the database). MongoDB: optional — the package prefers a host-registered `IMongoDatabase` (or `IMongoClient` + `DatabaseName`); against a single-node replica set include `directConnection=true`. |
| `DatabaseName` | MongoDB | — | Database used when no `IMongoDatabase` is registered. |
| `RecoveryStateTable` / `RecoveryStateCollection` | PostgreSQL, SQL Server, MongoDB | `asyncresponse_recovery_state` | Durable lost-subscriber recovery registrations, one row/document per waiter. MongoDB expires them natively via a TTL index. |
| `MessageTable` / `MessageCollection` | PostgreSQL, SQL Server, MongoDB | `asyncresponse_channel_messages` | Stored response envelopes loaded after `LISTEN/NOTIFY` wakeups (PostgreSQL), by the adaptive polling sweep (SQL Server), or after change-stream wakeups (MongoDB). |
| `SubscriberTable` / `SubscriberCollection` | PostgreSQL, SQL Server, MongoDB | `asyncresponse_channel_subscribers` | Live waiter heartbeat rows/documents used for subscriber counts and delivery confirmation. |
| `NotificationChannel` | PostgreSQL | `asyncresponse_channel_notify` | PostgreSQL `LISTEN/NOTIFY` channel; must be a simple identifier. |
| `AutoCreateSchema` / `AutoCreateIndexes` | PostgreSQL, SQL Server, MongoDB | `true` | Create schema/tables/indexes (or TTL + lookup indexes on MongoDB) on first use; set `false` when migrations/provisioning own DDL. With `AutoCreateIndexes = false`, MongoDB runs a one-time read-only check instead and **warns** (never throws) if the TTL or correlation-id lookup index is missing — indexes affect retention/performance, not correctness. |
| `UseChangeStreams` | MongoDB | `true` | Wake waiters with a change stream on the message collection (requires a replica set; single-node is sufficient). When disabled — or when the server is standalone — waiters fall back to `ListenerPollInterval` polling. |
| `MessageRetention` | PostgreSQL, SQL Server, MongoDB | 1 hour | How long response envelope rows/documents remain available for missed notification / cross-process sweep recovery. MongoDB reaps them natively via a TTL index. |
| `DeliveryConfirmationTimeout` | PostgreSQL, SQL Server, MongoDB | 5 seconds | How long a publisher waits for live waiter confirmation before routing to lost-subscriber recovery. |
| `DeliveryConfirmationPollInterval` | PostgreSQL, SQL Server, MongoDB | 50 ms | Poll cadence for cross-process delivery confirmation. |
| `ListenerPollInterval` | PostgreSQL, MongoDB | 250 ms | Missed-notification safety scan interval (and the wake cadence when MongoDB change streams are unavailable). |
| `ActivePollInterval` / `IdlePollInterval` | SQL Server | 250 ms / 2 s | Adaptive polling wake (SQL Server has no `LISTEN/NOTIFY`): sweep cadence while waiters are subscribed, and the backed-off cadence while idle. Same-process deliveries never wait for the sweep. |
| `PendingMessageBatchSize` | PostgreSQL, SQL Server, MongoDB | 64 | Keyset-page size for one subscribed correlation id. A listener/sweep continues through all pages; this tunes query/memory shape, not a delivery cap. |
| `SubscriberHeartbeatInterval` / `SubscriberHeartbeatTimeout` | PostgreSQL, SQL Server, MongoDB | 10s / 30s | Heartbeat cadence and liveness window. One channel-level loop batches the process's current active registrations per interval; abandoned rows/documents are not renewed. |
| `RecoveryStateExpiry` | Redis, NATS, PostgreSQL, SQL Server, MongoDB | 7 days | How long durable recovery state survives. Also the default wait timeout backstop. Don't set below your longest flow duration. While `DefaultTimeout` is unset it doubles as the waiter-timeout fallback and is then capped at ~49.7 days (the .NET timer ceiling); with `DefaultTimeout` configured it is a pure persistence TTL and may exceed the ceiling (e.g. 90-day retention), hard-capped at 10 years so "now + expiry" stamps can never overflow date arithmetic. `DefaultTimeout` and `DisposalDrainTimeout` always carry the timer cap, and every waiter's resolved timeout — explicit values included — must be positive and under it, rejected before the waiter registers anything. |
| `DefaultTimeout` | all | `RecoveryStateExpiry` | Default per-waiter timeout when a flow doesn't call `WithTimeout`. |
| `DisposalDrainTimeout` | all | 30 seconds | How long waiter disposal drains a delivery already in flight (an `Until` predicate mid-run) before abandoning it. A drained delivery settles the waiter as delivered; a lapsed budget faults it with `AsyncResponseIndeterminateDeliveryException` — never a cancellation, which would invite re-attaching to a possibly-consumed correlation id. |
| `IncludeRemoteStackTrace` | Redis, NATS, PostgreSQL, SQL Server, MongoDB | `true` | Whether the remote exception's stack trace travels on the wire (`Exception.Data["RemoteStackTrace"]`). See [security.md](security.md). |
| `MaxRemoteStackTraceLength` | Redis, NATS, PostgreSQL, SQL Server, MongoDB | `16384` | Length cap (chars) applied to the remote stack trace on both publish and receive. |

**Clock note for the database channels** (PostgreSQL, SQL Server, MongoDB): stored envelopes and
each waiter's delivery watermark are both stamped on the *database* clock — the publish path
returns the server-stamped `created_at` so even the same-process fast path compares like clocks —
which keeps app-host clock skew out of the delivery decision entirely. The 1 s watermark tolerance
covers the database clock's own granularity across statements, not app↔database skew. App clocks
only stamp non-delivery metadata (e.g. recovery-registration age for the watchdog's staleness
report), where ordinary NTP sync is ample.

## Transport options

Transport options are set through the transport registration callback. Each transport package owns
its own option type; the common shapes are summarized here. See
[Install and run](../README.md#install-and-run) for the local minimum and
[transport examples](provider-examples.md#transport-examples) for every provider.

The in-memory transport is configured directly on registration:

```csharp
.WithInMemoryTransport(options =>
{
    options.QueueCapacity = 1_024; // default; PublishAsync waits when full
    options.WorkerCount = 1;       // default; increase for independent parallel jobs
})
```

| Option | Transports | Purpose |
|---|---|---|
| `KeyPrefix` / `SubjectPrefix` / `SchemaName` / `TopicPrefix` | Redis / NATS / PostgreSQL / SQL Server / Kafka | Namespace for worker and response streams/subjects/tables/topics. NATS additionally caps every resolved subject and JetStream stream/consumer name at 255 characters, validated at startup before any subscriber connects. |
| `ConnectionString` | Azure Service Bus | Service Bus namespace connection string. Omit when you register your own singleton `ServiceBusClient`. |
| `ServiceUrl` / `Region` / `AccessKey` / `SecretKey` | SQS | Endpoint and credentials. All optional: omit everything to use the AWS SDK default chain, set `ServiceUrl` for LocalStack or a proxy, or register your own singleton `Amazon.SQS.IAmazonSQS` (e.g. via `AWSSDK.Extensions.NETCore.Setup`) and the package reuses it. |
| `ConnectionString` | SQL Server | SQL Server connection string; must point at an existing database (the package creates schema/table/indexes, never the database). |
| `BootstrapServers` | Kafka | Comma-separated broker list. The package speaks the Kafka protocol via `Confluent.Kafka`, so Redpanda, Amazon MSK, WarpStream, Aiven, and Confluent Cloud all work. |
| `WorkerTopic` / `ResponseTopic` + `WorkerConsumerGroup` / `ResponseConsumerGroup` | Kafka | Topics for worker jobs and response ingress (default `{TopicPrefix}.transport.worker` / `.response`) and the consumer group per role. |
| `CreateTopics` / `TopicNumPartitions` / `TopicReplicationFactor` | Kafka | Provision missing topics on subscriber startup. Partitions are the unit of consumer parallelism and ordering; `-1` uses broker defaults. |
| `OffsetCommitInterval` | Kafka | Auto-commit cadence for offsets stored after each resolved message; a crash inside the window redelivers at-least-once. |
| `MaxPollInterval` | Kafka | Maximum gap between consumer polls before the broker evicts the consumer from its group and rebalances its partitions (the librdkafka `max.poll.interval.ms`); default 5 minutes. The in-process handler-retry delays run on the poll thread, so startup validation requires the worst-case retry delay budget plus `PollTimeout` to fit within half of it (unlimited retries, `MaxDeliveryAttempts = 0`, have no finite budget and are the operator's call). |
| `DeadLetterTopic` / `DeadLetterTopicSuffix` | Kafka | Explicit dead-letter topic, or the suffix appended per source topic (default `.deadletter` → `{topic}.deadletter`). |
| `ConfigureProducer` / `ConfigureConsumer` / `ConfigureAdminClient` | Kafka | Last-chance hooks over the Confluent client configs (security, compression, fetch tuning, …). |
| `WorkerQueue` / `ResponseQueue` | Azure Service Bus | Service Bus queues used for worker jobs and response ingress; they must be distinct. |
| `WorkerQueue` / `ResponseQueue` | SQS | Queues for worker jobs and response ingress (distinct); each accepts a queue name (resolved once via `GetQueueUrl`) or a full queue URL. A name/URL ending in `.fifo` opts into FIFO publishing: the correlation id becomes the `MessageGroupId` (one flow's jobs stay ordered) and every message carries a unique `MessageDeduplicationId`. |
| `CreateQueues` / `DeadLetterQueueSuffix` / `MaxReceiveCount` | SQS | Provision the queues on startup, each with a native dead-letter queue (`{queue}-dlq`) wired through a redrive policy: SQS counts receives (`ApproximateReceiveCount`) and moves a message to the DLQ after `MaxReceiveCount`. Off by default — point at existing queues in production. |
| `WorkerSubscriber.VisibilityTimeout` / `RedeliveryDelay` | SQS | Per-receive visibility timeout (`null` uses the queue's setting) and the optional shortened invisibility applied via `ChangeMessageVisibility` when a handler fails; `null` lets the visibility timeout expire naturally. |
| `MessageTable` / `MessageCollection` | PostgreSQL, SQL Server, MongoDB | Single queue table/collection containing worker, response-ingress, and dead-letter rows/documents. |
| `ConnectionString` / `DatabaseName` | MongoDB | Optional — the package prefers a host-registered `IMongoDatabase` (or `IMongoClient` + `DatabaseName`). Against a single-node replica set include `directConnection=true`. |
| `WorkerQueue` / `ResponseQueue` / `DeadLetterQueue` | PostgreSQL, SQL Server, MongoDB | Logical queue names stored in the queue table/collection. They must be distinct. |
| `NotificationChannel` | PostgreSQL | `LISTEN/NOTIFY` channel that wakes PostgreSQL subscribers after publishes or retries. SQL Server has no equivalent: same-process publishes wake subscribers through an in-process signal, and cross-process rows are picked up within `EmptyPollDelay`. |
| `UseChangeStreamWake` | MongoDB | Wake idle subscribers with a change stream on the queue collection (requires a replica set). When disabled — or when the server is standalone — subscribers fall back to `EmptyPollDelay` polling. |
| `LockTimeout` | PostgreSQL, SQL Server, MongoDB | How long a claimed row/document stays locked (leased) before another subscriber may retry it. While a handler runs, the subscriber renews the claim automatically at half this cadence (fenced by `lock_id`), so one slow handler is not redelivered mid-execution. |
| `WorkerSubscriber.LockRenewalInterval` | Azure Service Bus | Peek-lock renewal cadence for received-but-unsettled messages while a batch is processed (default 10 s; `null` disables). Keeps slow handlers from losing their lock mid-batch — see the lock-budget note below. |
| `WorkerSubscriber.VisibilityRenewalInterval` | SQS | Opt-in visibility heartbeat for received-but-unprocessed batch messages (default `null` = off; requires `VisibilityTimeout` set and a shorter interval). Off by default because extending visibility overrides queue-tuned redrive timing, and on FIFO queues an extended message keeps its whole message group blocked if the consumer wedges. |
| `MaxMessagesPerReceive` / `ReceiveWaitTime` | Azure Service Bus, SQS | Receive-loop batch size and long-poll timeout for queue subscribers. SQS caps them at 10 messages and 20 seconds (the defaults). |
| `WorkerSubscriber.UseAckAfterEnqueue(...)` | all broker transports | Opt-in early-ACK dispatch for long-running workers: bounded in-process queue, configurable worker count, capacity, and drain timeout. Every broker transport exposes the same method name; the message is ACKed once it is accepted into the bounded in-process queue, before the handler runs. Durable-flow wake-ups ride this queue and lose broker redelivery under early ACK, so startup throws unless `DurableFlowOptions.AllowEarlyAckWorkerSubscriber = true` explicitly accepts the risk (see [transport-semantics.md](transport-semantics.md)). |
| `WorkerSubscriber.MaxDeliveryAttempts` | all broker transports except Google Pub/Sub and SQS | Redeliveries before dead-lettering. Google Pub/Sub and SQS perform redelivery natively — bound attempts with the subscription's `DeadLetterPolicy` (Pub/Sub) or the queue's redrive policy `maxReceiveCount` (SQS, provisioned by `CreateQueues` or your infra). On RabbitMQ, values above 2 require a TTL-retry dead-letter cycle (plain `basic.nack` requeues are not counted by the broker) and log a startup warning otherwise. On Kafka, attempts are in-process retries with backoff (`HandlerRetryBaseDelay`/`HandlerRetryMaxDelay`) counted per process delivery — offsets cannot NACK a single message. |
| `SubscriberRetryBaseDelay` / `SubscriberRetryMaxDelay` | Google Pub/Sub, RabbitMQ, SQS | Bounded backoff for restarting a failed hosted subscriber (streaming-pull/long-poll/consumer fault, transient auth/startup errors). On RabbitMQ, `NetworkRecoveryInterval` no longer paces subscriber restarts — these options govern that backoff instead. |
| `WorkerSubscriber.OnBackgroundFailure` | all broker transports | Hook for operator-visible metrics, alerting, or a durable dead-letter path when a background handler fails after early ACK. |
| `HostShutdownTimeout` | all broker transports | Must accommodate the transport's shutdown spend: `BackgroundDrainTimeout` (default 20 s), plus `ShutdownTimeout` (default 5 s) on the transports that bound a close/listen join with it (Azure Service Bus, RabbitMQ, Google Pub/Sub, PostgreSQL, MongoDB, SQS). Stock defaults fit the .NET host's 30 s default; mirror any custom `HostOptions.ShutdownTimeout` here so startup validation checks the real budget. SQS's `ShutdownTimeout` bounds its visibility-renewal task join after each batch (including the final one while the subscriber stops) rather than a one-time close. |
| `DeclareTopology` | RabbitMQ | Declare durable exchanges/queues/bindings (`true`) or leave topology to your infra team (`false`). |
| `CorrelationIdAttribute` / `CorrelationIdHeader` / `CorrelationIdProperty` | Pub/Sub / SQS / RabbitMQ / Kafka / NATS / PostgreSQL / SQL Server / MongoDB / Azure Service Bus | Broker metadata key used to resolve the correlation id before falling back to JSON body paths. On Kafka the correlation id also becomes the message key, keeping one flow's jobs ordered within a partition; on FIFO SQS queues it becomes the `MessageGroupId` with the same per-flow ordering effect. |
| `CorrelationIdJsonPaths` | broker transports | JSON paths inspected when metadata does not carry the correlation id. PostgreSQL, SQL Server, and MongoDB also unwrap nested JSON strings at those paths. |
| `DeadLetterEnabled` / `DeadLetterRetention` | Redis / NATS / PostgreSQL / SQL Server / MongoDB / Kafka | Whether poison messages are preserved. `DeadLetterRetention` exists only on PostgreSQL, SQL Server, and MongoDB (row/document retention); Redis and NATS bound their dead-letter streams via `DeadLetterStreamMaxLength` / `DeadLetterStreamMaxMessages` instead, and Kafka's `.deadletter` topic uses broker retention. See [transport-semantics.md](transport-semantics.md) for the full per-transport dead-letter matrix. |

A worker handler failure propagates out of the ingress to the transport dispatcher, which owns the
retry decision: in `AckAfterHandlerCompletes` the delivery is NACKed/abandoned and redelivered up
to `MaxDeliveryAttempts`, then dead-lettered; after an early ACK the failure is reported through
`OnBackgroundFailure` (and written to the transport's own dead-letter queue where one exists). A
failing worker never completes the waiter by itself — publish a failure response from the worker's
error handling when the flow should fail fast instead of waiting out its timeout.

Azure Service Bus uses peek-lock settlement. In `AckAfterHandlerCompletes`, a successful handler
completes the message, failures abandon it until `MaxDeliveryAttempts`, then dead-letter it through
Service Bus. In `AckAfterEnqueue`, the message is completed as soon as it enters the bounded
background queue; later handler failures cannot be broker-dead-lettered because the lock is gone, so
use `OnBackgroundFailure` for metrics, alerts, or a custom durable failure path. Mind the peek-lock
budget: a receive batch is processed sequentially, so the last message in a batch waits up to
`MaxMessagesPerReceive × handler latency` before settlement. By default the subscriber renews the
peek-lock of every unsettled batch message every `WorkerSubscriber.LockRenewalInterval` (10 s), so
slow handlers no longer hit `MessageLockLostException` redeliveries of already-processed messages;
if you disable renewal (`LockRenewalInterval = null`), keep that product well under the queue's
lock duration (or lower `MaxMessagesPerReceive`).

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
executions of already-processed messages — or opt into the
`WorkerSubscriber.VisibilityRenewalInterval` heartbeat, which re-extends unprocessed batch
messages' invisibility while the batch drains (see the option table for why it is off by default). FIFO queues are opt-in by naming the queue `*.fifo`.

Kafka is built on classic consumer groups with manual offset management (`enable.auto.commit=true` +
`enable.auto.offset.store=false`; an offset is stored only once its message is fully resolved). Two
consequences to plan for: ordering is per-partition, so consumer parallelism equals the partition
count — size `TopicNumPartitions` accordingly — and a slow or retrying message delays its partition
(head-of-line blocking). Keep the worst-case in-process retry budget
(`MaxDeliveryAttempts × HandlerRetryMaxDelay`) well under `MaxPollInterval` (default 5 minutes) or
the broker evicts the consumer mid-retry — startup validation enforces half that margin
automatically (see the option table above).
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
handler NACKs the message for Pub/Sub to redeliver. In `AckAfterEnqueue` the client's flow control
is bounded to the background queue capacity (`MaxOutstandingElementCount`), and a full queue parks
the delivery callback until capacity frees instead of NACKing — so backpressure cannot burn a
subscription `DeadLetterPolicy`'s delivery attempts; `AckAfterEnqueue` ACKs after enqueue and reports
later failures through `OnBackgroundFailure`.

NATS JetStream uses explicit acknowledgement. A successful handler `ACK`s; a failure `NAK`s with a
delay so JetStream redelivers after a backoff, and a message that reaches `MaxDeliveryAttempts` is
written to the dead-letter stream. Consumers are durable, so a restarted subscriber resumes from its
last acknowledged position. `AckAfterEnqueue` ACKs as soon as the message enters the bounded queue;
when that queue is full the consume loop pauses until capacity frees (a NAK is sent only on
shutdown/cancellation, so backpressure does not churn redeliveries).

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

MongoDB is the document analogue of that design: a publish is an idempotent insert (a retried
publish with the same id collides on `_id` and is treated as success), and competing subscribers
claim documents atomically with `findOneAndUpdate` — the claim increments the attempt count and
stamps a `lock_id` fence plus a `locked_until` lease evaluated against the server clock (`$$NOW`),
so publisher/consumer clock skew never fences messages in or out. Acks and NAKs are fenced by the
`lock_id`, and a document that reaches `MaxDeliveryAttempts` is moved to the dead-letter queue
(documents in the same collection) under an id derived deterministically from the source message,
so a crash between the dead-letter insert and the original delete cannot duplicate the DLQ entry.
Subscribers are woken by a change stream on the queue collection when the server is a replica set
(single-node is sufficient — the same requirement the MongoDB channel has for its response wake);
on a standalone server both degrade gracefully to interval polling. The channel stores response
envelopes, recovery registrations, and waiter heartbeats in TTL-indexed collections, so MongoDB
itself reaps expired documents — there is no application-side pruning.

Every MongoDB store (channel, transport, durable flows) also claims its effective collections —
derived ones such as the channel's `{MessageCollection}_counters` included — in a small reserved
`asyncresponse_ownership` collection at first use: one tiny document per collection, so two
components (in the same process or different hosts) misconfigured onto the same collection fail
startup with an error naming both claimants instead of silently corrupting each other's data.
Deployments that disable auto-creation own their provisioning and skip the ledger. Effective
namespaces (`database.collection`, UTF-8 bytes) are validated against MongoDB's sharded limit
(235 bytes) at store construction.

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
