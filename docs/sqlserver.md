# SQL Server channel and transport

[← Back to README](../README.md)

`AsyncResponse.Channels.SqlServer` and `AsyncResponse.Transports.SqlServer` let one Microsoft SQL
Server database act as both the durable response/recovery channel and the worker/response-ingress
transport. They are separate NuGet packages because apps often want only one side: for example,
SQL Server for recovery but an external broker for worker dispatch, or Redis/NATS for responses but
SQL Server for a simple durable worker queue. The design mirrors the PostgreSQL pair
([postgresql.md](postgresql.md)); the differences below come from what SQL Server does and does not
provide.

## Channel architecture

SQL Server has no `LISTEN/NOTIFY`, so the channel wakes active waiters with an **adaptive polling
sweep** instead of a server push (Service Broker/`SqlDependency` are deliberately not used — they
are frequently disabled by DBAs and `SqlDependency` is effectively legacy; a Service Broker wake
mode can be added later behind the same options if demand appears):

- Publishing writes the serialized response envelope to `asyncresponse_channel_messages`.
- **Same-process delivery never waits for the sweep**: the publisher dispatches directly to local
  waiters and confirms through an in-memory completion — zero polling on the common path.
- A single dispatch loop sweeps the message table for the subscribed correlation ids every
  `ActivePollInterval` (default 250 ms) **while any waiter is subscribed**, and backs off to
  `IdlePollInterval` (default 2 s) while the channel is idle. Cross-process deliveries therefore
  land within one active poll interval; an idle app costs one cheap query every idle interval.
- A new waiter re-arms the tight interval immediately and triggers a targeted scan of its own
  correlation id, so a response stored before the waiter subscribed is picked up at once.
- The sweep advances a stable `created_at, id` keyset cursor until every retained row for that
  correlation id is considered. `PendingMessageBatchSize` controls page shape; it no longer limits
  one sweep to the oldest batch, so sustained progress cannot starve a later terminal response.

Active waiters write rows to `asyncresponse_channel_subscribers`; one channel-level loop snapshots
the registrations that are still active locally and extends only those rows in bounded SQL batches
per heartbeat interval. A publisher first checks for live subscribers; if none exist, it routes
directly to lost-subscriber recovery. If subscribers do exist, the publisher inserts a message row
and waits for delivery confirmation:

1. Same-process delivery completes an in-memory confirmation immediately.
2. Cross-process delivery sets `acked_at`, which the publisher polls as a fallback.
3. If no waiter confirms before `DeliveryConfirmationTimeout`, the publisher atomically sets
   `recovery_claimed = 1` while `acked_at IS NULL` and dispatches the persisted recovery callback.

That last claim is the race guard: a slow live waiter and the recovery callback cannot both own the
same response. Row expiries (`expires_at`) are always computed on the **database clock**
(`SYSUTCDATETIME()`), as is the waiter's delivery watermark, so app-side clock skew cannot drop or
resurrect messages.

## Recovery state

`asyncresponse_recovery_state` stores one row per waiter registration, keyed by correlation id and
registration id. Shared-correlation waits therefore survive redeploys correctly: if several waiters
registered callbacks for the same correlation id, a late response dispatches all stored registrations.

The Core watchdog scans the same table through `IRecoveryStateScanner` and checks live waiters through
`IActiveSubscriberProbe`, so `AddAsyncResponseRecoveryCheck()` works with SQL Server exactly like
Redis, NATS, or PostgreSQL.

## Transport architecture

The transport uses one queue table, `asyncresponse_transport_messages`, with a logical `queue` column:

| Logical queue | Default | Purpose |
|---|---|---|
| `WorkerQueue` | `worker` | Serialized `WorkerJobEnvelope` rows consumed by `SqlServerWorkerSubscriber`. |
| `ResponseQueue` | `response` | Raw response JSON rows consumed by `SqlServerResponseIngressSubscriber`. |
| `DeadLetterQueue` | `deadletter` | Poison rows and failures that happen after early ACK. |

Subscribers claim work with `UPDLOCK, ROWLOCK, READPAST` — SQL Server's equivalent of PostgreSQL's
`FOR UPDATE SKIP LOCKED` — increment `attempts`, and set a row-local `lock_id`/`locked_until`.
`AckAfterHandlerCompletes` deletes the row after the handler succeeds and releases it for redelivery
on failure. `AckAfterEnqueue` deletes the row after it enters a bounded background queue; if the
handler later fails, the original row is already acknowledged, so the dispatcher writes a dead-letter
row (in the same transaction as the original delete when dead-lettering a poison row) and invokes
`OnBackgroundFailure`. Publishes are idempotent: the caller-supplied id is inserted with an
insert-if-absent (`WHERE NOT EXISTS` under `UPDLOCK, HOLDLOCK`, duplicate-key races treated as
success), so a retried publish never enqueues the same job twice.

There is no cross-process publish notification: a publish in the same process wakes its subscribers
immediately through an in-process signal, and other processes pick the row up within
`WorkerSubscriber.EmptyPollDelay` / `ResponseSubscriber.EmptyPollDelay` (default 250 ms).

Response ingress reads the correlation id from `CorrelationIdHeader` first, then from configured JSON
paths such as `CorrelationId`, `CustomParameters.CorrelationId`, and nested JSON strings. Both the
publish and receive paths emit OpenTelemetry spans with standard messaging attributes
(`messaging.system = sqlserver`, destination, delivery attempt).

## Schema creation

Both packages can create their schema, tables, and indexes on startup (`AutoCreateSchema = true`).
Channel and transport take the same transaction-scoped application lock
(`sp_getapplock`, resource `asyncresponse:ddl:{SchemaName}`) before DDL runs, so concurrent app
instances — and the channel and transport inside one app — never race each other through the
`IF NOT EXISTS` guards.

The packages do **not** create the database itself: point `ConnectionString` at an existing database
(the sample app ships a small provisioner that creates it for containers/dev). Set
`AutoCreateSchema = false` when migrations own the schema. Keep channel and transport table names
distinct even when they share the same schema. Correlation ids are stored as `nvarchar(400)` key
columns — keep ids at or under 400 characters (generated ids are far shorter).

## Configuration checklist

```csharp
builder.Services.AddAsyncResponse()
    .WithSqlServerChannel(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("SqlServer");
        options.SchemaName = "dbo";
        options.RecoveryStateTable = "asyncresponse_recovery_state";
        options.MessageTable = "asyncresponse_channel_messages";
        options.SubscriberTable = "asyncresponse_channel_subscribers";
        options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5);
        options.ActivePollInterval = TimeSpan.FromMilliseconds(250);
        options.IdlePollInterval = TimeSpan.FromSeconds(2);
    })
    .WithSqlServerTransport(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("SqlServer");
        options.MessageTable = "asyncresponse_transport_messages";
        options.WorkerQueue = "worker";
        options.ResponseQueue = "response";
        options.DeadLetterQueue = "deadletter";
        options.WorkerSubscriber.UseAckAfterEnqueue(4, 256);
    })
    .WithSqlServerDurableFlows(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("SqlServer");
        options.TableName = "asyncresponse_flow_state";
        options.StateExpiry = TimeSpan.FromDays(14);
    });
```

Connection-string notes:

| Setting | Why |
|---|---|
| `Max Pool Size` | Size deliberately for all app instances sharing the same server; early-ACK load can otherwise exhaust SQL Server's worker/connection budget. |
| `TrustServerCertificate=True` | Needed against dev/CI containers with self-signed certificates; use a real certificate in production instead. |
| `Database=...` | Must name an existing database — the packages create schema/tables, never the database. |

## Operational notes

- Use simple SQL Server identifiers for schema/table names: letters, digits, and underscores, not
  starting with a digit.
- `ActivePollInterval` bounds cross-process response latency; `IdlePollInterval` bounds idle database
  load. Same-process deliveries (the common case when the waiter and the publisher share the app)
  never wait for either.
- Keep `SubscriberHeartbeatInterval` lower than `SubscriberHeartbeatTimeout`; publishers use these
  rows to decide whether to wait for live delivery. Registration writes one row, then each interval
  updates the process's current active-registration snapshot in bounded batches. Rows no longer in
  that snapshot are allowed to expire even if cleanup deletion failed. A failed batch is logged and
  the next interval retries, so leave enough timeout headroom for multiple attempts.
- `PendingMessageBatchSize` is a page-size tuning knob, not a cap per sweep. Smaller pages lower
  peak materialization; larger pages reduce round trips under progress-heavy correlations.
- Keep `DeliveryConfirmationTimeout` long enough for the slowest expected live delivery (including
  one cross-process `ActivePollInterval`), but short enough that a truly lost subscriber routes to
  recovery promptly.
- Set `DeadLetterRetention` if operators do not inspect dead-letter rows indefinitely.
- Transient faults (deadlock 1205, lock timeout 1222, Azure SQL throttling codes, broken
  connections) are retried on the publish paths with bounded backoff.
- Monitor table size, dead-letter count, connection usage, and lock waits from your database
  tooling. AsyncResponse reports library metrics, not database-native queue depth.
