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
| `asyncresponse.enqueue_worker`, `asyncresponse.worker.publish`, `asyncresponse.worker.execute` | worker enqueue, transport publish, and execution |
| `asyncresponse.redis.receive` | Redis Streams subscriber message handling |
| `asyncresponse.pubsub.receive` | Google Pub/Sub subscriber message handling |
| `asyncresponse.rabbitmq.receive` | RabbitMQ subscriber message handling |
| `asyncresponse.lost_subscriber.dispatch` | recovery callback routing when no waiter is alive |
| `asyncresponse.watchdog.scan` | recovery watchdog scans |

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
| `asyncresponse.lost_subscriber.dispatches` | counter | `kind` = `response`\|`exception`, `route` = `resume`\|`failure`\|`unclassified`, `invoked` = bool | The core "how often does recovery fire" SLO — every late response that found nobody listening, classified by how it was routed and whether a callback was actually invoked. |
| `asyncresponse.waiter.timeouts` | counter | `channel` | Waiters that hit their timeout before a terminal response. |
| `asyncresponse.worker.jobs` | counter | `outcome` = `executed`\|`failed`\|`rejected` | Worker job dispatch outcomes. |
| `asyncresponse.recovery.outstanding` | observable gauge | — | Persisted recovery-state entries (from the watchdog scan). |
| `asyncresponse.recovery.active_waiters` | observable gauge | — | Entries that still have a live waiter. |
| `asyncresponse.recovery.stale` | observable gauge | — | Entries that are old and have no live waiter — probably stuck flows. |
| `asyncresponse.type_resolution.unresolved` | counter | `kind` = `service`\|`payload` | Callback/payload type names that could not be resolved (see [security.md](security.md)). |

The lost-subscriber counter is the one to alert on: a nonzero `route=failure` or
`route=unclassified` rate means flows are dying mid-wait and being failed on recovery, and a rising
`asyncresponse.recovery.stale` gauge is your earliest signal of stuck flows.

> **Not emitted:** broker/store-native queue depth and size (Redis key count, JetStream stream
> backlog, Pub/Sub subscription depth, PostgreSQL table row counts) are *not* surfaced by
> AsyncResponse — read those from your broker or database metrics. AsyncResponse only measures what
> happens inside the library.
