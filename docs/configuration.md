# Configuration

[← Back to README](../README.md)

`AddAsyncResponse()` registers the channel-agnostic engine but **no channel or transport** — chain
exactly one channel (`.WithInMemoryChannel()`, `.WithRedisChannel()`, `.WithNatsChannel()`, or
`.WithPostgreSqlChannel(...)`) and exactly one transport (`.WithInMemoryTransport()`,
`.WithRedisTransport(...)`, `.WithAzureServiceBusTransport(...)`, `.WithGooglePubSubTransport(...)`, `.WithRabbitMqTransport(...)`,
`.WithNatsTransport(...)`, `.WithPostgreSqlTransport(...)`, or another full AsyncResponse transport
package). An app that starts without either one fails fast at host startup with setup guidance, so a
misconfiguration can never silently hang every waiter or drop worker dispatch. The recovery watchdog
is part of the engine and runs by default for whichever channel you choose.

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

See [recovery.md](recovery.md) for the watchdog in context, and [security.md](security.md) for
`.AuthorizeCallbacks(...)` and type-resolution registration, which are also chained off
`AddAsyncResponse()`.

## Channel options

Channel options are common where noted and channel-specific otherwise. They are set through the
channel registration callback (`.WithRedisChannel(options => …)`, `.WithNatsChannel(options => …)`,
`.WithPostgreSqlChannel(options => …)`).

| Option | Channels | Default | Purpose |
|---|---|---|---|
| `KeyPrefix` | Redis | — | Isolate apps/environments sharing one Redis. **Persisted — treat as a deployment contract.** |
| `SubjectPrefix` | NATS | `asyncresponse` | Response subjects: `{prefix}.response.{cid}`. |
| `RecoveryBucket` | NATS | `asyncresponse-recovery` | JetStream KV bucket for recovery state. |
| `SchemaName` | PostgreSQL | `public` | Schema that contains PostgreSQL channel tables. |
| `RecoveryStateTable` | PostgreSQL | `asyncresponse_recovery_state` | Durable lost-subscriber recovery registrations, one row per waiter. |
| `MessageTable` | PostgreSQL | `asyncresponse_channel_messages` | Stored response envelopes loaded after `LISTEN/NOTIFY` wakeups. |
| `SubscriberTable` | PostgreSQL | `asyncresponse_channel_subscribers` | Live waiter heartbeat rows used for subscriber counts and delivery confirmation. |
| `NotificationChannel` | PostgreSQL | `asyncresponse_channel_notify` | PostgreSQL `LISTEN/NOTIFY` channel; must be a simple identifier. |
| `AutoCreateSchema` | PostgreSQL | `true` | Create schema/tables/indexes on first use; set `false` when migrations own DDL. |
| `MessageRetention` | PostgreSQL | 1 hour | How long response envelope rows remain available for missed notification recovery. |
| `DeliveryConfirmationTimeout` | PostgreSQL | 5 seconds | How long a publisher waits for live waiter confirmation before routing to lost-subscriber recovery. |
| `DeliveryConfirmationPollInterval` | PostgreSQL | 50 ms | Poll cadence for cross-process delivery confirmation. |
| `ListenerPollInterval` | PostgreSQL | 250 ms | Missed-notification safety scan interval. |
| `PendingMessageBatchSize` | PostgreSQL | 64 | Rows loaded per subscribed correlation id per listener pass. |
| `SubscriberHeartbeatInterval` / `SubscriberHeartbeatTimeout` | PostgreSQL | 10s / 30s | Heartbeat cadence and liveness window for active waiters. |
| `RecoveryStateExpiry` | Redis, NATS, PostgreSQL | 7 days | How long durable recovery state survives. Also the default wait timeout backstop. Don't set below your longest flow duration. |
| `DefaultTimeout` | all | `RecoveryStateExpiry` | Default per-waiter timeout when a flow doesn't call `WithTimeout`. |
| `IncludeRemoteStackTrace` | Redis, NATS, PostgreSQL | `true` | Whether the remote exception's stack trace travels on the wire (`Exception.Data["RemoteStackTrace"]`). See [security.md](security.md). |
| `MaxRemoteStackTraceLength` | Redis, NATS, PostgreSQL | `16384` | Length cap (chars) applied to the remote stack trace on both publish and receive. |

## Transport options

Transport options are set through the transport registration callback. Each transport package owns
its own option type; the common shapes are summarized here. See the transport sections in
[the README's Quick start](../README.md#quick-start) for full examples.

| Option | Transports | Purpose |
|---|---|---|
| `KeyPrefix` / `SubjectPrefix` / `SchemaName` | Redis / NATS / PostgreSQL | Namespace for worker and response streams/subjects/tables. |
| `ConnectionString` | Azure Service Bus | Service Bus namespace connection string. Omit when you register your own singleton `ServiceBusClient`. |
| `WorkerQueue` / `ResponseQueue` | Azure Service Bus | Service Bus queues used for worker jobs and response ingress; they must be distinct. |
| `MessageTable` | PostgreSQL | Single queue table containing worker, response-ingress, and dead-letter rows. |
| `WorkerQueue` / `ResponseQueue` / `DeadLetterQueue` | PostgreSQL | Logical queue names stored in the PostgreSQL queue table. They must be distinct. |
| `NotificationChannel` | PostgreSQL | `LISTEN/NOTIFY` channel that wakes PostgreSQL subscribers after publishes or retries. |
| `LockTimeout` | PostgreSQL | How long a claimed row stays locked before another subscriber may retry it. |
| `MaxMessagesPerReceive` / `ReceiveWaitTime` | Azure Service Bus | Receive-loop batch size and long-poll timeout for queue subscribers. |
| `WorkerSubscriber.UseAckAfterEnqueue(...)` / `UseAckAfterReceive(...)` | all broker transports | Opt-in early-ACK dispatch for long-running workers: bounded in-process queue, configurable worker count, capacity, and drain timeout. |
| `WorkerSubscriber.MaxDeliveryAttempts` | all broker transports | Redeliveries before dead-lettering. |
| `WorkerSubscriber.OnBackgroundFailure` | all broker transports | Hook for operator-visible metrics, alerting, or a durable dead-letter path when a background handler fails after early ACK. |
| `HostShutdownTimeout` | all broker transports | Must accommodate `ShutdownTimeout + BackgroundDrainTimeout`; mirror any custom `HostOptions.ShutdownTimeout`. |
| `DeclareTopology` | RabbitMQ | Declare durable exchanges/queues/bindings (`true`) or leave topology to your infra team (`false`). |
| `CorrelationIdAttribute` / `CorrelationIdHeader` / `CorrelationIdProperty` | Pub/Sub / RabbitMQ / NATS / PostgreSQL / Azure Service Bus | Broker metadata key used to resolve the correlation id before falling back to JSON body paths. |
| `CorrelationIdJsonPaths` | broker transports | JSON paths inspected when metadata does not carry the correlation id. PostgreSQL also unwraps nested JSON strings at those paths. |
| `DeadLetterEnabled` / `DeadLetterRetention` | Redis / NATS / PostgreSQL | Whether poison messages are preserved and, for PostgreSQL, how long dead-letter rows are retained. |

Azure Service Bus uses peek-lock settlement. In `AckAfterHandlerCompletes`, a successful handler
completes the message, failures abandon it until `MaxDeliveryAttempts`, then dead-letter it through
Service Bus. In `AckAfterReceive`, the message is completed as soon as it enters the bounded
background queue; later handler failures cannot be broker-dead-lettered because the lock is gone, so
use `OnBackgroundFailure` for metrics, alerts, or a custom durable failure path.

See [postgresql.md](postgresql.md) for PostgreSQL table layout, delivery-confirmation details, and
connection-string tuning.
