# Observability

[← Back to README](../README.md)

AsyncResponse emits **traces** (`System.Diagnostics.Activity`) and **metrics**
(`System.Diagnostics.Metrics`) from one `ActivitySource` and one `Meter`, both named
`"AsyncResponse"`. The library takes no OpenTelemetry dependency; your host connects them to
OpenTelemetry, Datadog, or any other listener.

## Tracing

Subscribe to `AsyncResponseDiagnostics.ActivitySourceName` (`"AsyncResponse"`):

```csharp
using AsyncResponse;
using OpenTelemetry.Trace;

builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddSource(AsyncResponseDiagnostics.ActivitySourceName)
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation());
```

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
| `asyncresponse.lost_subscriber.dispatch` | recovery callback routing when no waiter is alive, tagged `asyncresponse.lost_subscriber.kind` (`response`\|`exception`) and `asyncresponse.lost_subscriber_route` |
| `asyncresponse.watchdog.scan` | one recovery watchdog scan, tagged with its counts (`asyncresponse.watchdog.total_entries`, `stale_entries`, `unprobeable_entries`, `unreadable_entries`, …) |
| `asyncresponse.flow.execute` | one durable-flow run execution, tagged `asyncresponse.flow_id` and `asyncresponse.flow_type` (see [durable-flows.md](durable-flows.md)) |

Every transport, the in-memory one included, emits an `asyncresponse.worker.publish` producer
span on publish; every broker and database transport also emits its consumer receive span on
consume (both ACK modes). A receive span carries the standard messaging attributes
(`messaging.system`, `messaging.destination.name`, and `messaging.message.id` where the broker
exposes one), the transport (`asyncresponse.transport`), role and ACK mode
(`asyncresponse.<transport>.role` / `.ack_mode`), and the correlation id. Transports that count
delivery attempts tag them too: PostgreSQL, SQL Server and MongoDB use the standard
`messaging.message.delivery_attempt`; Redis and Kafka use `asyncresponse.redis.delivery_attempt` and
`asyncresponse.kafka.delivery_attempt`.

Common tags include `asyncresponse.correlation_id`, `asyncresponse.channel`,
`asyncresponse.transport`, `asyncresponse.payload_type`, `asyncresponse.subscribers`,
`asyncresponse.worker.service` / `.method`, `asyncresponse.reply_target.name` / `.transport`, and
`error.type` on failures. Values read off a stream or store before validation (the correlation id,
reply target, worker service and method) are bounded and escaped — control characters, line and
paragraph separators, bidi controls and backslashes become `\uXXXX`, overlong values are cut with an
ellipsis — so an ordinary value is tagged unchanged but a hostile one cannot flood the trace backend.
On `asyncresponse.lost_subscriber.dispatch`, `asyncresponse.payload_type` is the materialized type,
else the registration's persisted type name, and is unset for a raw payload with no registration.

## Metrics

Subscribe to `AsyncResponseDiagnostics.MeterName` (`"AsyncResponse"`):

```csharp
using AsyncResponse;
using OpenTelemetry.Metrics;

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter(AsyncResponseDiagnostics.MeterName)   // "AsyncResponse"
        .AddAspNetCoreInstrumentation());
```

### Instruments

| Instrument | Type (unit) | Tags | What it tells you |
|---|---|---|---|
| `asyncresponse.lost_subscriber.dispatches` | counter (`{dispatch}`) | `kind` = `response`\|`exception`, `route` = `resume`\|`failure`\|`keep_waiting`\|`mixed`\|`unclassified`, `invoked` = bool | The core "how often does recovery fire" SLO: every response or exception published with nobody listening, by route and whether a callback actually ran. `mixed` means shared-correlation registrations took different routes in one dispatch; each registration's own span carries its true route. |
| `asyncresponse.waiter.timeouts` | counter (`{timeout}`) | `channel` = `inmemory`\|`redis`\|`nats`\|`postgresql`\|`sqlserver`\|`mongodb` | Waiters that hit their timeout before a terminal response. |
| `asyncresponse.channel.overloaded_waits` | counter (`{wait}`) | `channel` = `redis`\|`nats` | Waits faulted as indeterminate because responses for their correlation id arrived faster than the wait could process them and the bounded per-wait buffer was full — 1,024 on Redis; on NATS the subscription buffer (the connection's `SubPendingChannelCapacity`, 16,384 by default) dropped a message. The database channels keep their backlog server-side. Alert on any sustained rate: speed up the completion predicate, publish fewer progress messages, or move the wait to a database channel. |
| `asyncresponse.channel.sweep.duration` | histogram (`s`) | `asyncresponse.channel` = `postgresql`\|`sqlserver`\|`mongodb` | One full dispatch sweep of a database response channel across every locally subscribed correlation id. Recorded only for sweeps that visited a waiter and had no failed correlation id (a failed sweep measures the outage, not the sweep). A sweep longer than half of `DeliveryConfirmationTimeout` also logs a warning (at most once a minute): cross-process responses then risk being claimed for recovery before the sweep reaches their waiter — reduce waiters per process, raise `DeliveryConfirmationTimeout`, or check database latency. |
| `asyncresponse.worker.jobs` | counter (`{job}`) | `outcome` = `executed`\|`failed`\|`rejected`\|`redelayed`\|`dropped` | Worker job outcomes. `failed` counts individual attempts. `rejected` is an envelope refused without dispatch: an unusable correlation id or an unparseable body (both acknowledged), or a newer schema version or a target the callback authorizer refuses (both thrown, so the transport redelivers — a newer or differently configured replica can run it — then dead-letters). `redelayed` is a delayed job delivered early and re-published for the remainder (one per hop, not an execution). `dropped` is the in-memory transport's terminal outcome after `MaxDeliveryAttempts` (broker transports dead-letter instead). Alert on `rejected`: each is a producer-side contract violation, a producer ahead of this build, or a target outside the allowlist. |
| `asyncresponse.worker.inmemory_overflow_depth` | observable gauge (`{job}`) | — | Follow-up jobs the in-memory worker transport holds past `QueueCapacity` (summed over the process's transports), bounded by `InJobOverflowCapacity`. Staying near the bound means a handler fans out faster than the workers drain. |
| `asyncresponse.worker.inmemory_overflow_rejections` | counter (`{job}`) | — | Follow-up publishes the in-memory transport refused at `InJobOverflowCapacity`; the publishing job failed and is retried by the in-process retry ladder. Alert on any sustained rate: raise the capacities or add workers. |
| `asyncresponse.worker.inmemory_delayed_jobs` | observable gauge (`{job}`) | — | Delayed jobs the in-memory worker transport holds — waiting on their due time, or fired and waiting for queue room (summed over the process's transports), bounded by `DelayedJobCapacity`. Staying near the bound means more flows sleep at once than the capacity was sized for. |
| `asyncresponse.worker.inmemory_delayed_rejections` | counter (`{job}`) | — | Delayed publishes made from inside a running job (a flow parking on a timer) that the in-memory transport refused at `DelayedJobCapacity`; the publishing job failed and is retried by the in-process retry ladder. Alert on any sustained rate: raise `DelayedJobCapacity`. |
| `asyncresponse.flow.own_job_redeliveries` | counter (`{delivery}`) | `resolution` = `redelayed`\|`waiting` | Durable-flow wake-ups that found the execution lease held by a live execution of their **own** job: a broker in-flight ceiling lapsed under a running handler. Never acknowledged as duplicates — `redelayed`: re-published as the same job past the holder's lease; `waiting`: handed back to the transport (`DurableFlowLeaseContendedException`). Shorten the park (`DurableFlowOptions.MaxInProcessParkDuration`) or raise the broker ceiling (see [durable-flows.md](durable-flows.md#what-happens-when-things-die)). |
| `asyncresponse.ingress.unroutable_responses` | counter (`{message}`) | — | Inbound responses acknowledged without routing because their correlation id is missing or outside the portable contract (see [security.md](security.md#explicit-correlation-id)) — redelivery could never route them. Alert on any non-zero rate: each is a producer-side contract violation. |
| `asyncresponse.ingress.oversized_messages` | counter (`{message}`) | `route` = `response`\|`worker` | Inbound messages acknowledged without processing because they exceed `AsyncResponseOptions.MaxInboundMessageChars`. Alert on any non-zero rate: the message is gone, and either a producer sends more than the deployment allows or the cap is too low. |
| `asyncresponse.recovery.outstanding` | observable gauge (`{entry}`) | — | Persisted recovery registrations at the last watchdog scan. |
| `asyncresponse.recovery.active_waiters` | observable gauge (`{entry}`) | — | Registrations that still have a live waiter. |
| `asyncresponse.recovery.stale` | observable gauge (`{entry}`) | — | Registrations that are old and have no live waiter — probably stuck flows. |
| `asyncresponse.recovery.unprobeable` | observable gauge (`{entry}`) | — | Registrations whose waiter liveness could not be probed (a probe outage, or no `IActiveSubscriberProbe` registered); their staleness is unknown and never flagged. Non-zero degrades the recovery health check. |
| `asyncresponse.recovery.unreadable` | observable gauge (`{entry}`) | — | Stored registrations the last scan found but this build cannot read (malformed, incomplete identity, or a newer schema version). A response for their correlation id is refused and redelivered, readable siblings included. Non-zero degrades the health check; expect it briefly during a rolling upgrade that raised the schema version — otherwise it needs an operator (the store logs a warning for each). |
| `asyncresponse.recovery.scan_truncated` | observable gauge (`{scan}`) | — | `1` when the last scan stopped at `Watchdog.MaxScanEntries`: `outstanding`/`stale` describe the buffered subset only and the health check reports **Degraded**. Alert on it — a capped scan cannot attest staleness. |
| `asyncresponse.type_resolution.unresolved` | counter (`{failure}`) | `kind` = `service`\|`payload`\|`resolver` | Persisted callback/payload type names that could not be resolved (see [security.md](security.md#type-resolution-for-plugins--assemblyloadcontext)). `resolver` counts a registered resolver that threw — once per throw, even when a later resolver then answered — so a broken resolver is visible apart from a name nothing knows. |
| `asyncresponse.flow_state.pruned_rows` | counter (`{row}`) | `provider` | Expired durable-flow ledger rows deleted by the relational stores' opportunistic prune (PostgreSQL, SQL Server, MySQL, SQLite, Oracle, EF Core). |
| `asyncresponse.flow_state.prune_failures` | counter (`{failure}`) | `provider` | Opportunistic prunes that failed; the flow creation they rode on still succeeded and the next `PruneInterval` retries. Alert on a sustained rate: expired rows are accumulating. |
| `asyncresponse.flow_state.prune_budget_exhausted` | counter (`{prune}`) | `provider` | Prunes that stopped at `PruneBudget` with a full last batch — the expired backlog is outgrowing the prune. Raise `PruneBudget` or shorten `PruneInterval`. |
| `asyncresponse.flow_state.checkpoint.size` | histogram (`By`) | `provider` | UTF-8 size of every durable-flow ledger a store serialized for a write (a create or a checkpoint, whether or not it then won its revision check), recorded by every bundled store (`provider` = `InMemory` for the in-memory one) once per serialization — a retried write records again. Not recorded: `ValidateCreate`'s preflight, or a ledger refused by `MaxStateBytes`. Every write serializes the **whole** ledger, so the **sum** is the cumulative serialization cost (it grows with the square of a run's retained steps) and the **count** the number of writes. Use the distribution to set `LedgerSizeWarningBytes`, `MaxRetainedSteps` and `MaxStateBytes`; see [supported ledger budgets](durable-flows.md#supported-ledger-budgets). Measured only while something listens. |

Alert on the lost-subscriber counter: a non-zero `route=failure` or `route=unclassified` rate means
flows are dying mid-wait and being failed on recovery. `route=keep_waiting` is benign by itself
(checkpoints arriving while nobody listens), but a registration that keeps waiting and never resumes
is a stuck flow — pair it with the watchdog. A rising `asyncresponse.recovery.stale` gauge is your
earliest signal of stuck flows.

### A telemetry failure never decides an outcome

Metrics and spans are recorded on the library's decision paths (after a worker job ran, after a lost
response was routed), and a listener's callback runs on the recording thread. A `MeterListener`
measurement callback, or an `ActivityListener` sampling, started or **stopped** callback, that throws
costs **that measurement or span** and nothing else: every measurement goes through a guarded
recorder, and every span starts and ends through guarded helpers. A span's stopped callback runs
when the span ends — after the work it describes is done — so an unguarded end turned a worker job
that ran, a response that was published, or a recovery callback that was invoked into a failed
delivery the transport redelivered, and the work ran again. Every operation span (publish, receive,
ingress, worker execution, recovery dispatch, flow execution, watchdog scan) and the waiter's
`asyncresponse.wait` span now end without letting that callback's exception escape, and the ambient
span is restored to the one the operation started under.

The same holds for log lines on those paths — the worker executor, the ingress, the recovery
dispatcher, and `IDurableFlows.StartAsync`, which returns the id of
the run it published even when the logging provider throws (Microsoft.Extensions.Logging rethrows a
provider's failure).

One thing remains the host's to keep healthy: observable gauges, which are read on the listener's
own thread.

> **Not emitted:** broker/store-native queue depth and size (Redis key count, JetStream stream
> backlog, Service Bus queue length, Pub/Sub subscription depth, database table row counts) — read
> those from your broker or database metrics. AsyncResponse measures only what happens inside the
> library.
