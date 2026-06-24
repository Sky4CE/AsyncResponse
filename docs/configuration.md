# Configuration

[← Back to README](../README.md)

`AddAsyncResponse()` registers the channel-agnostic engine but **no channel or transport** — chain
exactly one channel (`.WithInMemoryChannel()`, `.WithRedisChannel()`, or `.WithNatsChannel()`) and
exactly one transport (`.WithInMemoryTransport()`, `.WithRedisTransport(...)`,
`.WithGooglePubSubTransport(...)`, `.WithRabbitMqTransport(...)`, `.WithNatsTransport(...)`, or
another full AsyncResponse transport package). An app that starts without either one fails fast at
host startup with setup guidance, so a misconfiguration can never silently hang every waiter or drop
worker dispatch. The recovery watchdog is part of the engine and runs by default for whichever
channel you choose.

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
channel registration callback (`.WithRedisChannel(options => …)`, `.WithNatsChannel(options => …)`).

| Option | Channels | Default | Purpose |
|---|---|---|---|
| `KeyPrefix` | Redis | — | Isolate apps/environments sharing one Redis. **Persisted — treat as a deployment contract.** |
| `SubjectPrefix` | NATS | `asyncresponse` | Response subjects: `{prefix}.response.{cid}`. |
| `RecoveryBucket` | NATS | `asyncresponse-recovery` | JetStream KV bucket for recovery state. |
| `RecoveryStateExpiry` | Redis, NATS | 7 days | How long durable recovery state survives. Also the default wait timeout backstop. Don't set below your longest flow duration. |
| `DefaultTimeout` | all | `RecoveryStateExpiry` | Default per-waiter timeout when a flow doesn't call `WithTimeout`. |
| `IncludeRemoteStackTrace` | Redis, NATS | `true` | Whether the remote exception's stack trace travels on the wire (`Exception.Data["RemoteStackTrace"]`). See [security.md](security.md). |
| `MaxRemoteStackTraceLength` | Redis, NATS | `16384` | Length cap (chars) applied to the remote stack trace on both publish and receive. |

## Transport options

Transport options are set through the transport registration callback. Each transport package owns
its own option type; the common shapes are summarized here. See the transport sections in
[the README's Quick start](../README.md#quick-start) for full examples.

| Option | Transports | Purpose |
|---|---|---|
| `KeyPrefix` / `SubjectPrefix` | Redis / NATS | Namespace for worker and response streams/subjects. |
| `WorkerSubscriber.UseAckAfterEnqueue(...)` / `UseAckAfterReceive(...)` | all broker transports | Opt-in early-ACK dispatch for long-running workers: bounded in-process queue, configurable worker count, capacity, and drain timeout. |
| `WorkerSubscriber.MaxDeliveryAttempts` | all broker transports | Redeliveries before dead-lettering. |
| `WorkerSubscriber.OnBackgroundFailure` | all broker transports | Hook for operator-visible metrics, alerting, or a durable dead-letter path when a background handler fails after early ACK. |
| `HostShutdownTimeout` | all broker transports | Must accommodate `ShutdownTimeout + BackgroundDrainTimeout`; mirror any custom `HostOptions.ShutdownTimeout`. |
| `DeclareTopology` | RabbitMQ | Declare durable exchanges/queues/bindings (`true`) or leave topology to your infra team (`false`). |
| `CorrelationIdAttribute` / `CorrelationIdHeader` | Pub/Sub / RabbitMQ / NATS | Broker metadata key used to resolve the correlation id before falling back to JSON body paths. |
