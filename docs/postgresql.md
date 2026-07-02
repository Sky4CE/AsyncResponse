# PostgreSQL Channel And Transport

[← Back to README](../README.md)

`AsyncResponse.Channels.PostgreSQL` and `AsyncResponse.Transports.PostgreSQL` let one PostgreSQL
database act as both the durable response/recovery channel and the worker/response-ingress transport.
They are separate NuGet packages because apps often want only one side: for example, PostgreSQL for
recovery but an external broker for worker dispatch, or Redis/NATS for responses but PostgreSQL for a
simple durable worker queue.

## Channel Architecture

The channel keeps large payloads out of `NOTIFY`. Publishing writes the serialized response envelope
to `asyncresponse_channel_messages`, then sends a notification whose payload is only the correlation
id. Local listener loops load pending rows from the table and deliver them to live waiters. Very long
correlation ids produce an empty notification payload, which asks listeners to scan all local
subscriptions; this stays under PostgreSQL's 8 KB notification payload limit.

Active waiters write heartbeat rows to `asyncresponse_channel_subscribers`. A publisher first checks
for live subscribers; if none exist, it routes directly to lost-subscriber recovery. If subscribers do
exist, the publisher inserts a message row and waits for delivery confirmation:

1. Same-process delivery completes an in-memory confirmation immediately.
2. Cross-process delivery sets `acked_at`, which the publisher polls as a fallback.
3. If no waiter confirms before `DeliveryConfirmationTimeout`, the publisher atomically sets
   `recovery_claimed = true` while `acked_at IS NULL` and dispatches the persisted recovery callback.

That last claim is the race guard: a slow live waiter and the recovery callback cannot both own the
same response.

## Recovery State

`asyncresponse_recovery_state` stores one row per waiter registration, keyed by correlation id and
registration id. Shared-correlation waits therefore survive redeploys correctly: if several waiters
registered callbacks for the same correlation id, a late response dispatches all stored registrations.

The Core watchdog scans the same table through `IRecoveryStateScanner` and checks live waiters through
`IActiveSubscriberProbe`, so `AddAsyncResponseRecoveryCheck()` works with PostgreSQL exactly like Redis
or NATS.

## Transport Architecture

The transport uses one queue table, `asyncresponse_transport_messages`, with a logical `queue` column:

| Logical queue | Default | Purpose |
|---|---|---|
| `WorkerQueue` | `worker` | Serialized `WorkerJobEnvelope` rows consumed by `PostgreSqlWorkerSubscriber`. |
| `ResponseQueue` | `response` | Raw response JSON rows consumed by `PostgreSqlResponseIngressSubscriber`. |
| `DeadLetterQueue` | `deadletter` | Poison rows and failures that happen after early ACK. |

Subscribers claim work with `FOR UPDATE SKIP LOCKED`, increment `attempts`, and set a row-local
`lock_id`/`locked_until`. `AckAfterHandlerCompletes` deletes the row after the handler succeeds and
releases it for redelivery on failure. `AckAfterReceive` deletes the row after it enters a bounded
background queue; if the handler later fails, the original row is already acknowledged, so the
dispatcher writes a dead-letter row and invokes `OnBackgroundFailure`.

Response ingress reads the correlation id from `CorrelationIdHeader` first, then from configured JSON
paths such as `CorrelationId`, `CustomParameters.CorrelationId`, and nested JSON strings.

## Schema Creation

Both packages can create their tables on startup (`AutoCreateSchema = true`). Channel and transport
take the same transaction-scoped advisory lock for the configured schema before DDL runs. This matters
because `CREATE ... IF NOT EXISTS` can still race through PostgreSQL system catalogs when multiple
processes start together.

Set `AutoCreateSchema = false` when migrations own the schema. Keep channel and transport table names
distinct even when they share the same schema.

## Configuration Checklist

```csharp
builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(
    builder.Configuration.GetConnectionString("PostgreSQL")! +
    ";No Reset On Close=true;Max Auto Prepare=20"));

builder.Services.AddAsyncResponse()
    .WithPostgreSqlChannel(options =>
    {
        options.SchemaName = "public";
        options.RecoveryStateTable = "asyncresponse_recovery_state";
        options.MessageTable = "asyncresponse_channel_messages";
        options.SubscriberTable = "asyncresponse_channel_subscribers";
        options.NotificationChannel = "asyncresponse_channel_notify";
        options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5);
    })
    .WithPostgreSqlTransport(options =>
    {
        options.MessageTable = "asyncresponse_transport_messages";
        options.WorkerQueue = "worker";
        options.ResponseQueue = "response";
        options.DeadLetterQueue = "deadletter";
        options.WorkerSubscriber.UseAckAfterReceive(4, 256);
    });
```

Recommended Npgsql connection-string settings:

| Setting | Why |
|---|---|
| `No Reset On Close=true` | Avoids `DISCARD ALL` on every pooled check-in; AsyncResponse uses dedicated long-lived `LISTEN` connections, so pooled query connections do not need reset for listener state. |
| `Max Auto Prepare=20` | Keeps the recurring table queries prepared across reuse, reducing parse/plan CPU under load. |
| `Maximum Pool Size` | Size deliberately for all app instances sharing the same server; early-ACK load tests can otherwise exhaust PostgreSQL's `max_connections`. |

## Operational Notes

- Use simple PostgreSQL identifiers for schema/table/notification names: letters, digits, and
  underscores, not starting with a digit.
- Keep `SubscriberHeartbeatInterval` lower than `SubscriberHeartbeatTimeout`; publishers use these
  rows to decide whether to wait for live delivery. Heartbeat upserts retry on transient database
  errors (logged at Warning) — a subscriber row only expires if upserts keep failing for longer
  than `SubscriberHeartbeatTimeout`, so leave headroom for more than one interval inside the
  timeout.
- Keep `DeliveryConfirmationTimeout` long enough for the slowest expected live delivery, but short
  enough that a truly lost subscriber routes to recovery promptly.
- Set `DeadLetterRetention` if operators do not inspect dead-letter rows indefinitely.
- Monitor PostgreSQL table size, dead-letter count, connection usage, and lock waits from your
  database tooling. AsyncResponse reports library metrics, not database-native queue depth.
