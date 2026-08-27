# Observability

[← Back to README](../README.md)

AsyncResponse emits both **traces** (`System.Diagnostics.Activity`) and **metrics**
(`System.Diagnostics.Metrics`) from a single source/meter named `"AsyncResponse"`. The library takes
no OpenTelemetry dependency; your host connects the source and meter to OpenTelemetry, Datadog, or
any other listener.

## Tracing

AsyncResponse emits spans from one source, `AsyncResponseDiagnostics.ActivitySourceName`
(`"AsyncResponse"`):

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
| `asyncresponse.ingress.raw_response` | raw response ingress — broker JSON published into the channel before payload typing |
| `asyncresponse.enqueue_worker`, `asyncresponse.worker.publish`, `asyncresponse.worker.execute` | worker enqueue, transport publish, and execution |
| `asyncresponse.redis.receive` | Redis Streams subscriber message handling |
| `asyncresponse.azure_service_bus.receive` | Azure Service Bus subscriber message handling |
| `asyncresponse.pubsub.receive` | Google Pub/Sub subscriber message handling |
| `asyncresponse.rabbitmq.receive` | RabbitMQ subscriber message handling |
| `asyncresponse.kafka.receive` | Kafka consumer message handling |
| `asyncresponse.sqs.receive` | AWS SQS subscriber message handling |
| `asyncresponse.nats.receive` | NATS JetStream subscriber message handling |
| `asyncresponse.postgresql.receive` | PostgreSQL transport subscriber message handling |
| `asyncresponse.sqlserver.receive` | SQL Server transport subscriber message handling |
| `asyncresponse.mongodb.receive` | MongoDB transport subscriber message handling |
| `asyncresponse.lost_subscriber.dispatch` | recovery callback routing when no waiter is alive |
| `asyncresponse.watchdog.scan` | recovery watchdog scans |
| `asyncresponse.flow.execute` | one durable-flow run execution, tagged `asyncresponse.flow_id` and `asyncresponse.flow_type` (see [durable-flows.md](durable-flows.md)) |

Every transport emits an `asyncresponse.worker.publish` producer span on publish and a consumer
receive span on consume (for both ACK modes). Each receive span carries the standard messaging
attributes (`messaging.system`, `messaging.destination.name`, and `messaging.message.id` where the
broker exposes one) plus the transport, role, ACK mode, and the AsyncResponse correlation id.
Transports that count delivery attempts also tag them on the receive span: PostgreSQL and
SQL Server use the standard `messaging.message.delivery_attempt`, Redis uses
`asyncresponse.redis.delivery_attempt`, and Kafka uses `asyncresponse.kafka.delivery_attempt`.

Common tags include `asyncresponse.correlation_id`, `asyncresponse.channel`,
`asyncresponse.transport`, `asyncresponse.payload_type`, `asyncresponse.subscribers`,
`asyncresponse.lost_subscriber_route`, and worker/reply-target details.

## Metrics

AsyncResponse publishes counters and observable gauges through a `System.Diagnostics.Metrics.Meter`
named `"AsyncResponse"` (constant `AsyncResponseDiagnostics.MeterName`). Subscribe with
OpenTelemetry's `AddMeter`:

```csharp
using AsyncResponse;
using OpenTelemetry.Metrics;

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter(AsyncResponseDiagnostics.MeterName)   // "AsyncResponse"
        .AddAspNetCoreInstrumentation());
```

### Instruments

| Instrument | Type | Tags | What it tells you |
|---|---|---|---|
| `asyncresponse.lost_subscriber.dispatches` | counter | `kind` = `response`\|`exception`, `route` = `resume`\|`failure`\|`keep_waiting`\|`mixed`\|`unclassified`, `invoked` = bool | The core "how often does recovery fire" SLO — every late response that found nobody listening, classified by how it was routed and whether a callback was actually invoked. `mixed` means shared-correlation registrations legitimately took different routes in one dispatch; each registration's own dispatch span carries its true route. |
| `asyncresponse.waiter.timeouts` | counter | `channel` | Waiters that hit their timeout before a terminal response. |
| `asyncresponse.worker.jobs` | counter | `outcome` = `executed`\|`failed`\|`rejected`\|`dropped` | Worker job dispatch outcomes. `failed` counts individual attempts; `rejected` is an envelope the ingress refused without dispatching — an unusable correlation id, a failed authorization, or a body no build can parse (acknowledged rather than redelivered forever); `dropped` is the in-memory transport's terminal outcome after `MaxDeliveryAttempts` (broker transports dead-letter instead). Alert on `rejected`: every one is a producer-side contract violation. |
| `asyncresponse.ingress.unroutable_responses` | counter | — | Inbound responses acknowledged without routing because they carry no correlation id (deliberate poison guard — redelivery could never route them). Alert on any non-zero rate: each one is a producer-side contract violation. |
| `asyncresponse.ingress.oversized_messages` | counter | `route` = `response`\|`worker` | Inbound messages acknowledged without processing because they exceed `AsyncResponseOptions.MaxInboundMessageChars`. Alert on any non-zero rate: the message is gone, and either a producer is sending more than the deployment allows or the cap is set too low. |
| `asyncresponse.recovery.outstanding` | observable gauge | — | Persisted recovery-state entries (from the watchdog scan). |
| `asyncresponse.recovery.active_waiters` | observable gauge | — | Entries that still have a live waiter. |
| `asyncresponse.recovery.stale` | observable gauge | — | Entries that are old and have no live waiter — probably stuck flows. |
| `asyncresponse.recovery.unprobeable` | observable gauge | — | Entries whose waiter liveness could not be probed (a probe outage, or no `IActiveSubscriberProbe` registered) — their staleness is unknown and they are never flagged stale. A non-zero value also degrades the recovery health check. |
| `asyncresponse.recovery.scan_truncated` | observable gauge | — | `1` when the last watchdog scan stopped at the `MaxScanEntries` buffer cap: `outstanding`/`stale` then describe the buffered subset only, and the recovery health check reports **Degraded**. Alert on it — a capped scan cannot attest staleness. |
| `asyncresponse.type_resolution.unresolved` | counter | `kind` = `service`\|`payload` | Callback/payload type names that could not be resolved (see [security.md](security.md)). |

The lost-subscriber counter is the one to alert on: a nonzero `route=failure` or
`route=unclassified` rate means flows are dying mid-wait and being failed on recovery (a
`route=keep_waiting` rate is benign by itself — non-terminal checkpoints arriving while nobody
listens — but pair it with the watchdog: a registration that keeps waiting and never resumes is a
stuck flow), and a rising
`asyncresponse.recovery.stale` gauge is your earliest signal of stuck flows.

> **Not emitted:** broker/store-native queue depth and size (Redis key count, JetStream stream
> backlog, Service Bus queue length, Pub/Sub subscription depth, PostgreSQL table row counts) are *not* surfaced by
> AsyncResponse — read those from your broker or database metrics. AsyncResponse only measures what
> happens inside the library.
