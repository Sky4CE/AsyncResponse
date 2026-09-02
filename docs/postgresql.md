# PostgreSQL channel and transport

[← Back to README](../README.md)

`AsyncResponse.Channels.PostgreSQL` and `AsyncResponse.Transports.PostgreSQL` let one PostgreSQL
database act as both the durable response/recovery channel and the worker/response-ingress transport.
They are separate NuGet packages because apps often want only one side: for example, PostgreSQL for
recovery but an external broker for worker dispatch, or Redis/NATS for responses but PostgreSQL for a
simple durable worker queue.

## Channel architecture

The channel keeps large payloads out of `NOTIFY`. Publishing writes the serialized response envelope
to `asyncresponse_channel_messages`, then sends a notification whose payload is only the correlation
id. Local listener loops load pending rows from the table and deliver them to live waiters. Very long
correlation ids produce an empty notification payload, which asks listeners to scan all local
subscriptions; this stays under PostgreSQL's 8 KB notification payload limit.

`NOTIFY` is only a wake hint. Signals are deliberately coalesced in a bounded in-process channel,
and the periodic safety scan remains authoritative. For each subscribed correlation id the reader
uses a stable `created_at, id` keyset cursor until the retained result set is exhausted; a terminal
response therefore cannot sit forever behind the oldest `PendingMessageBatchSize` progress rows.

Active waiters write rows to `asyncresponse_channel_subscribers`; one channel-level loop snapshots
the registrations that are still active locally and extends only those rows with one statement per
heartbeat interval. A publisher first checks for live subscribers; if none exist, it routes directly
to lost-subscriber recovery. If subscribers do exist, the publisher inserts a message row and waits
for delivery confirmation:

1. Same-process delivery completes an in-memory confirmation immediately.
2. Cross-process delivery sets `acked_at` (plus `acked_seq`, drawn from a per-schema sequence),
   which the publisher polls as a fallback.
3. If no waiter confirms before `DeliveryConfirmationTimeout`, the publisher atomically sets
   `recovery_claimed = true` while `acked_at IS NULL` and dispatches the persisted recovery callback.

That last claim is the race guard: a slow live waiter and the recovery callback cannot both own the
same response. `acked_seq` and each subscription's registration draw from the same monotonic
sequence, arbitrating "acked before this waiter registered" (history, not redelivered) versus
"acked to a fan-out group including this waiter" (delivered) even when both events land on the
same server-clock tick — with one conservative residual: a claim whose sequence draw stalled
across ticks resolves as history, never as a replayed response. The same-process fast path honors
that arbitration too: an idempotent duplicate publish (a retry carrying the same message id)
dispatches with the stored row's settlement columns rather than a fresh unacked view, so it
cannot replay an already-consumed response to a waiter that registered after the ack.

## Recovery state

`asyncresponse_recovery_state` stores one row per waiter registration, keyed by correlation id and
registration id. Shared-correlation waits therefore survive redeploys correctly: if several waiters
registered callbacks for the same correlation id, a late response dispatches all stored registrations.

The Core watchdog scans the same table through `IRecoveryStateScanner` and checks live waiters through
`IActiveSubscriberProbe`, so `AddAsyncResponseRecoveryCheck()` works with PostgreSQL exactly like Redis
or NATS.

## Transport architecture

The transport uses one queue table, `asyncresponse_transport_messages`, with a logical `queue` column:

| Logical queue | Default | Purpose |
|---|---|---|
| `WorkerQueue` | `worker` | Serialized `WorkerJobEnvelope` rows consumed by `PostgreSqlWorkerSubscriber`. |
| `ResponseQueue` | `response` | Raw response JSON rows consumed by `PostgreSqlResponseIngressSubscriber`. |
| `DeadLetterQueue` | `deadletter` | Poison rows and failures that happen after early ACK. |

Subscribers claim work with `FOR UPDATE SKIP LOCKED`, increment `attempts`, and set a row-local
`lock_id`/`locked_until`. `AckAfterHandlerCompletes` deletes the row after the handler succeeds and
releases it for redelivery on failure. `AckAfterEnqueue` deletes the row after it enters a bounded
background queue; if the handler later fails, the original row is already acknowledged, so the
dispatcher writes a dead-letter row and invokes `OnBackgroundFailure`.

Response ingress reads the correlation id from `CorrelationIdHeader` first, then from configured JSON
paths such as `CorrelationId`, `CustomParameters.CorrelationId`, and nested JSON strings.

## Schema creation

Both packages can create their tables on startup (`AutoCreateSchema = true`). Channel and transport
take the same transaction-scoped advisory lock for the configured schema before DDL runs. This matters
because `CREATE ... IF NOT EXISTS` can still race through PostgreSQL system catalogs when multiple
processes start together.

Set `AutoCreateSchema = false` when migrations own the schema. Keep channel and transport table names
distinct even when they share the same schema.

With `AutoCreateSchema = false`, the transport now verifies its operator-provisioned queue table
against the catalog too, at first use — matching the channel's existing behavior: an absent table
is assumed not yet migrated and re-checked on the next operation, while a present table with the
wrong shape throws with the fix instead of failing silently at the first publish or claim.

The channel does the same: with `AutoCreateSchema = false` it runs its full relation verification
(tables, columns, indexes, and the deterministic-collation requirement on identity columns such as
`correlation_id`) at first use, not just the migration probe below — an operator-provisioned
column with a non-deterministic collation is rejected with the fix instead of silently
cross-routing responses at runtime.

### Upgrading a manually managed schema

1.0.0 added a monotonic ack sequence to the channel message table. With `AutoCreateSchema = false`
the channel validates these objects once at startup and fails with an actionable error until the
migration below is applied (names shown for the default `public.asyncresponse_channel_messages`;
the sequence is always `{message_table}_ack_seq` in the same schema):

```sql
ALTER TABLE public.asyncresponse_channel_messages ADD COLUMN IF NOT EXISTS acked_seq bigint NULL;
CREATE SEQUENCE IF NOT EXISTS public.asyncresponse_channel_messages_ack_seq AS bigint;
```

The column is nullable and the migration is safe to run while old-version hosts are still up:
rows they ack carry no sequence and fall back to the previous watermark rule.

## Configuration checklist

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
        options.WorkerSubscriber.UseAckAfterEnqueue(4, 256);
    })
    .WithPostgreSqlDurableFlows(options =>
    {
        options.SchemaName = "public";
        options.TableName = "asyncresponse_flow_state";
        options.StateExpiry = TimeSpan.FromDays(14);
    });
```

Recommended Npgsql connection-string settings:

| Setting | Why |
|---|---|
| `No Reset On Close=true` | Avoids `DISCARD ALL` on every pooled check-in; AsyncResponse uses dedicated long-lived `LISTEN` connections, so pooled query connections do not need reset for listener state. |
| `Max Auto Prepare=20` | Keeps the recurring table queries prepared across reuse, reducing parse/plan CPU under load. |
| `Maximum Pool Size` | Size deliberately for all app instances sharing the same server; early-ACK load tests can otherwise exhaust PostgreSQL's `max_connections`. |

## Operational notes

- Use simple PostgreSQL identifiers for schema/table/notification names: letters, digits, and
  underscores, not starting with a digit, at most 63 characters (PostgreSQL silently truncates
  longer names, so validation rejects them). Derived names — `{MessageTable}_ack_seq` and the
  `*_idx` indexes — reserve their suffix space by truncating the table stem, and validation
  rejects a configuration whose effective name plan collides (for example a table occupying a
  derived name, or two near-cap tables whose truncated stems derive the same index name). When
  the channel, transport, and durable-flow stores share one schema, each additionally verifies
  its relations against the catalog after schema creation (kind and, for indexes, owning table)
  — a name occupied by another component's object fails startup with a rename error instead of
  `CREATE ... IF NOT EXISTS` silently skipping the DDL.
- **Identity columns need a deterministic collation.** `correlation_id`/`registration_id`
  (channel), `queue` (transport), and `flow_id` (durable-flow store) are compared ordinally, so a
  non-deterministic ICU collation folds distinct ids onto one key — lookups cross-match and the
  second id is rejected on insert. Startup verification reads each column's collation from the
  catalog and fails actionably if it is not deterministic (`"C"` always qualifies). This check
  needs **PostgreSQL 12+** (`pg_collation.collisdeterministic`), on top of the covering-index
  support (`pg_index.indnkeyatts`) catalog verification has required since PostgreSQL 11.
- Keep `SubscriberHeartbeatInterval` lower than `SubscriberHeartbeatTimeout`; publishers use these
  rows to decide whether to wait for live delivery. Registration writes one row, then each interval
  performs one update for the process's current active-registration snapshot. Rows no longer in that
  snapshot are allowed to expire even if cleanup deletion failed. A failed batch is logged and the
  next interval retries, so leave enough timeout headroom for multiple attempts.
- `PendingMessageBatchSize` is a page-size tuning knob, not a cap per sweep. Smaller pages lower
  peak materialization; larger pages reduce round trips when one correlation id carries heavy
  progress traffic.
- Keep `DeliveryConfirmationTimeout` long enough for the slowest expected live delivery, but short
  enough that a truly lost subscriber routes to recovery promptly.
- Set `DeadLetterRetention` if operators do not inspect dead-letter rows indefinitely.
- Monitor PostgreSQL table size, dead-letter count, connection usage, and lock waits from your
  database tooling. AsyncResponse reports library metrics, not database-native queue depth.
