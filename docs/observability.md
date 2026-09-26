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
`asyncresponse.lost_subscriber_route`, and worker/reply-target details. Values that come off a
stream or a store before anything has validated them — the correlation id, the reply target's name
and transport, the worker service and method — are bounded and escaped (control characters,
line/paragraph separators, bidi controls and backslashes become `\uXXXX`; overlong values are cut
with an ellipsis), so an ordinary value is tagged unchanged but a hostile one cannot flood the
trace backend. On `asyncresponse.lost_subscriber.dispatch`, `asyncresponse.payload_type` is the
payload's real type — the materialized type, else the registration's persisted type name — and is
left unset for a raw payload with no registration.

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
| `asyncresponse.channel.overloaded_waits` | counter | `channel` = `redis`\|`nats` | Waits faulted as indeterminate because responses for their correlation id arrived faster than the wait could process them and the bounded per-wait buffer was full — 1,024 on Redis; on NATS the NATS.Net subscription buffer (the connection's `SubPendingChannelCapacity`, 16,384 by default) dropped a message. Redis and NATS channels only (the database channels keep a backlog server-side). Alert on any sustained rate: a consumer is saturated — speed up the completion predicate, publish fewer progress messages, or move the wait to a database channel. |
| `asyncresponse.channel.sweep.duration` | histogram (s) | `asyncresponse.channel` | Duration of one full dispatch sweep of a database response channel (PostgreSQL, SQL Server, MongoDB) across every locally subscribed correlation id. Only sweeps that visited a waiter and completed without a failed correlation id are recorded (a sweep that failed, or that the outage breaker broke off, measured the outage, not the sweep). A sweep longer than half of `DeliveryConfirmationTimeout` also logs a warning (at most once a minute): cross-process responses then risk being claimed for recovery before the sweep reaches their waiter — reduce local waiters per process, raise `DeliveryConfirmationTimeout`, or check database latency. |
| `asyncresponse.worker.jobs` | counter | `outcome` = `executed`\|`failed`\|`rejected`\|`redelayed`\|`dropped` | Worker job dispatch outcomes. `failed` counts individual attempts; `rejected` is an envelope refused without dispatching — an unusable correlation id or a body no build can parse, both acknowledged rather than redelivered forever, or an envelope stamped with a schema version this build cannot read or a target the callback authorizer refuses, both of which throw instead and take the transport's ordinary failure path (redelivery — a newer build, or a replica configured to allow the target, can run it — then dead-letter); `redelayed` is a job delivered before its due time and re-published for the remainder (one per hop of a chunked or early delayed delivery — not an execution); `dropped` is the in-memory transport's terminal outcome after `MaxDeliveryAttempts` (broker transports dead-letter instead). Alert on `rejected`: every one is a producer-side contract violation, a producer ahead of this build, or a target outside the allowlist. |
| `asyncresponse.worker.inmemory_overflow_depth` | observable gauge | — | Follow-up jobs the in-memory worker transport currently holds past `QueueCapacity` (summed over the process's transports), bounded by `InJobOverflowCapacity`. A depth that stays near the bound means a handler fans out faster than the workers drain. |
| `asyncresponse.worker.inmemory_overflow_rejections` | counter | — | Follow-up publishes the in-memory transport refused at `InJobOverflowCapacity`; the publishing job failed and is redelivered by the in-process retry ladder. Alert on any sustained rate: raise the capacities or add workers. |
| `asyncresponse.worker.inmemory_delayed_jobs` | observable gauge | — | Delayed jobs the in-memory worker transport currently holds — waiting on their due time, or fired and waiting for queue room (summed over the process's transports), bounded by `DelayedJobCapacity`. A count that stays near the bound means more flows are sleeping at once than the capacity was sized for. |
| `asyncresponse.worker.inmemory_delayed_rejections` | counter | — | Delayed publishes made from inside a running job (a flow parking on a timer) that the in-memory transport refused at `DelayedJobCapacity`; the publishing job failed and is redelivered by the in-process retry ladder. Alert on any sustained rate: raise `DelayedJobCapacity`. |
| `asyncresponse.ingress.unroutable_responses` | counter | — | Inbound responses acknowledged without routing because they carry no correlation id (deliberate poison guard — redelivery could never route them). Alert on any non-zero rate: each one is a producer-side contract violation. |
| `asyncresponse.ingress.oversized_messages` | counter | `route` = `response`\|`worker` | Inbound messages acknowledged without processing because they exceed `AsyncResponseOptions.MaxInboundMessageChars`. Alert on any non-zero rate: the message is gone, and either a producer is sending more than the deployment allows or the cap is set too low. |
| `asyncresponse.recovery.outstanding` | observable gauge | — | Persisted recovery-state entries (from the watchdog scan). |
| `asyncresponse.recovery.active_waiters` | observable gauge | — | Entries that still have a live waiter. |
| `asyncresponse.recovery.stale` | observable gauge | — | Entries that are old and have no live waiter — probably stuck flows. |
| `asyncresponse.recovery.unprobeable` | observable gauge | — | Entries whose waiter liveness could not be probed (a probe outage, or no `IActiveSubscriberProbe` registered) — their staleness is unknown and they are never flagged stale. A non-zero value also degrades the recovery health check. |
| `asyncresponse.recovery.scan_truncated` | observable gauge | — | `1` when the last watchdog scan stopped at the `MaxScanEntries` buffer cap: `outstanding`/`stale` then describe the buffered subset only, and the recovery health check reports **Degraded**. Alert on it — a capped scan cannot attest staleness. |
| `asyncresponse.type_resolution.unresolved` | counter | `kind` = `service`\|`payload`\|`resolver` | Callback/payload type names that could not be resolved (see [security.md](security.md)). `resolver` counts a registered `AsyncResponseTypeResolution` resolver that threw — once per throw, even when a later resolver then resolved the name — so a broken resolver is visible apart from a name nothing knows. |
| `asyncresponse.flow_state.pruned_rows` | counter | `provider` | Expired durable-flow ledger rows deleted by the relational stores' opportunistic prune (PostgreSQL, SQL Server, MySQL, SQLite, Oracle, EF Core). |
| `asyncresponse.flow_state.prune_failures` | counter | `provider` | Opportunistic prunes that failed; the flow creation they rode on still succeeded and the next `PruneInterval` retries. Alert on a sustained rate: expired rows are accumulating. |
| `asyncresponse.flow_state.prune_budget_exhausted` | counter | `provider` | Prunes that stopped at `PruneBudget` with a full last batch — expired rows remain and the backlog is outgrowing the prune. Raise `PruneBudget` or shorten `PruneInterval`. |

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
