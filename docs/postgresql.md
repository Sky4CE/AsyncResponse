# PostgreSQL channel and transport

[← Back to README](../README.md)

`AsyncResponse.Channels.PostgreSQL` and `AsyncResponse.Transports.PostgreSQL` let one PostgreSQL
database act as both the durable response/recovery channel and the worker/response-ingress transport.
They are separate NuGet packages because apps often want only one side: for example, PostgreSQL for
recovery but an external broker for worker dispatch, or Redis/NATS for responses but PostgreSQL for a
simple durable worker queue. The [SQL Server pair](sqlserver.md) mirrors this design.

## Channel architecture

The channel keeps payloads out of `NOTIFY`. Publishing writes the serialized response envelope to
`asyncresponse_channel_messages`, then sends a notification whose payload is only the correlation
id (at most 400 characters, `AsyncResponseChannelOptions.MaxCorrelationIdLength`, so always under
PostgreSQL's 8,000-byte payload limit). Local listener loops load pending rows from the table and
deliver them to live waiters.

`NOTIFY` is only a wake hint. Signals are coalesced in a bounded in-process channel, and the
periodic safety sweep remains authoritative. For each subscribed correlation id the reader keeps a
stable `created_at, id` keyset cursor across dispatch passes, with bounded pages per pass and
periodic reconciliation for late commits, so terminal responses beyond the first progress batch
are reached without monopolizing other correlations.

Active waiters write rows to `asyncresponse_channel_subscribers`; one channel-level loop snapshots
the registrations still active locally and extends only those rows, with one statement per
heartbeat interval. A publisher first checks for live subscribers; if none exist, it routes directly
to lost-subscriber recovery. Otherwise it inserts a message row and waits for delivery confirmation:

1. Same-process delivery completes an in-memory confirmation immediately.
2. Cross-process delivery sets `acked_at` (plus `acked_seq`, drawn from the message table's own
   sequence, `{message_table}_ack_seq`), which the publisher polls as a fallback.
3. If no waiter confirms before `DeliveryConfirmationTimeout`, the publisher atomically sets
   `recovery_claimed = true` while `acked_at IS NULL` and dispatches the persisted recovery callback.

That last claim is the race guard: a slow live waiter and the recovery callback cannot both own the
same response. Row expiries and delivery watermarks use the database clock (`now()`), so app-side
clock skew cannot drop or resurrect messages. `acked_seq` and each subscription's registration
draw from the same monotonic sequence, which separates "acked before this waiter registered"
(history, not redelivered) from "acked to a fan-out group including this waiter" (delivered) even
within one server-clock tick. The one conservative residual: a claim whose sequence draw stalled
across ticks resolves as history, never as a replayed response. The same-process fast path honors
this too: an idempotent duplicate publish (a retry carrying the same message id) dispatches with the
stored row's settlement columns, so it cannot replay an already-consumed response to a waiter that
registered after the ack.

## Recovery state

`asyncresponse_recovery_state` stores one row per waiter registration, keyed by correlation id and
registration id. Shared-correlation waits therefore survive redeploys: if several waiters registered
callbacks for the same correlation id, a late response dispatches all stored registrations.

The Core watchdog scans the same table through `IRecoveryStateScanner` and checks live waiters through
`IActiveSubscriberProbe`, so `AddAsyncResponseRecoveryCheck()` works with PostgreSQL exactly like
Redis, NATS, SQL Server, or MongoDB.

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
dispatcher writes a dead-letter row and invokes `OnBackgroundFailure`. Publishes are idempotent: the
caller-supplied id is inserted with `ON CONFLICT (id) DO NOTHING`, so a retried publish never
enqueues the same job twice.

A publish of an immediately claimable row sends `NOTIFY` on `NotificationChannel` (default
`asyncresponse_transport_notify`) with the queue name as payload, waking that queue's subscribers
in every process; `WorkerSubscriber.EmptyPollDelay` / `ResponseSubscriber.EmptyPollDelay` (default
250 ms) is the polling fallback. A NAK and a delayed publish send no wake: the row is not claimable
until its delay has passed, and the next poll after that picks it up.

The claim orders by `(available_at, created_at)` — availability order, which equals publish order
for every row that was neither delayed nor released for retry — and the dequeue index
`{message_table}_ready_idx` is keyed on exactly `(queue, available_at, created_at)`, so a claim is
one ordered index descent that stops at the first unleased row.

Response ingress reads the correlation id from `CorrelationIdHeader` first, then from configured JSON
paths such as `CorrelationId`, `CustomParameters.CorrelationId`, and nested JSON strings.

## Schema creation

Both packages can create their schema, tables, and indexes on startup (`AutoCreateSchema = true`,
the default). Channel, transport, and the PostgreSQL durable-flow store take the same
transaction-scoped advisory lock for the configured schema before DDL runs, because
`CREATE ... IF NOT EXISTS` can still race through the system catalogs when several processes start
together. The startup DDL of all three runs under the same bounds:

- Every DDL transaction starts with `SET LOCAL lock_timeout = '5s'`, before it takes the advisory
  lock, so neither the advisory lock nor a table lock is waited for longer than 5 s.
- Only objects the catalog shows missing are altered or built. `ALTER TABLE … ADD COLUMN IF NOT
  EXISTS` and `CREATE INDEX IF NOT EXISTS` take their table lock *before* they find the object
  present, so they are not run unconditionally; on an up-to-date schema the startup DDL takes no
  table lock at all.
- Work whose run time grows with the table — a `jsonb` → `text` conversion, an index build on an
  existing table, the channel's column additions — runs in its own transaction after the
  schema-wide advisory lock is released, under an advisory lock scoped to the table
  (`asyncresponse:ddl:{schema}.{table}`) and an hour-long command timeout. Other stores sharing the
  schema never wait for it.
- A failed attempt — a lock wait that timed out, or any server error from that long-running work
  except a name collision (a `statement_timeout`, a view or rule depending on a converted column, a
  full disk) — is retried after a jittered 30–60 s window; until then the store's operations on
  that host fail at once, naming the cause. A non-server failure (a dropped connection) and a
  transient fault the server ends immediately — deadlock (40P01), serialization failure (40001),
  server shutdown (57P01) — latch nothing: the next operation runs the DDL again.
- In the transport, one attempt runs at a time, shared by every operation waiting for it; a caller's
  cancellation ends only that caller's wait and does not roll back a conversion or index build.

Find a lock holder that makes the DDL time out in `pg_locks` joined to `pg_stat_activity`.

Set `AutoCreateSchema = false` when migrations own the schema. Keep channel and transport table names
distinct even when they share the same schema. With `AutoCreateSchema = false`, both packages verify
the operator-provisioned relations against the catalog at first use — tables, columns, indexes, and
the deterministic-collation requirement on identity columns (see
[Operational notes](#operational-notes)). An absent table is assumed not yet migrated and re-checked
on the next operation; a present table with the wrong shape throws with the fix instead of failing
at the first publish or claim.

### Upgrading a manually managed schema

Schemas created by earlier builds need the changes below. With `AutoCreateSchema = true` the store
applies them itself on first start, under the bounds above; with `AutoCreateSchema = false` it fails
(or, for the dequeue index, warns) with the statement to run.

**Channel ack sequence.** The message table needs `acked_seq` and its sequence (names shown for the
default `public.asyncresponse_channel_messages`; the sequence is always `{message_table}_ack_seq` in
the same schema):

```sql
ALTER TABLE public.asyncresponse_channel_messages ADD COLUMN IF NOT EXISTS acked_seq bigint NULL;
CREATE SEQUENCE IF NOT EXISTS public.asyncresponse_channel_messages_ack_seq AS bigint;
```

The column is nullable and the migration is safe to run while older hosts are still up: rows they
ack carry no sequence and fall back to the timestamp watermark rule.

**Transport dequeue index.** The claim needs `{message_table}_ready_idx` over
`(queue, available_at, created_at)`. The previous `{message_table}_claim_idx` over
`(queue, available_at, locked_until, created_at)` cannot serve the claim's order, so draining a
burst of K rows cost O(K²). On an operator-managed schema the new index is verified when present and
only warned about when absent (it is claim performance, not correctness). Build it without a write
lock, and drop the old one once no host runs the previous build:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS asyncresponse_transport_messages_ready_idx
    ON public.asyncresponse_transport_messages (queue, available_at, created_at);
DROP INDEX CONCURRENTLY IF EXISTS public.asyncresponse_transport_messages_claim_idx;
```

On an auto-created schema the store builds the missing index itself with a plain `CREATE INDEX`,
which holds a SHARE lock that blocks every write to the queue table (publishes, claims, acks, NAKs,
lease renewals — older hosts' included) while it runs. It never drops the old index: `DROP INDEX`
needs ACCESS EXCLUSIVE on a live queue, so that is the operator's call. Its 5 s lock wait outlasts
an ordinary autovacuum but not a lock that does not yield (an anti-wraparound autovacuum, a manual
`VACUUM`/`ANALYZE`, a long-running or idle-in-transaction writer); on such a table each attempt
parks every statement on the queue for those 5 s, then fails without changing anything and retries
after the 30–60 s window. On a large table (retained dead letters, parked durable-flow timers) run
the `CONCURRENTLY` build above **before** rolling out, after the `text` conversion below. An index
that exists but is not yet valid (a `CONCURRENTLY` build still running, or one that failed) is a
warning, not a failure; a host starting meanwhile neither waits for it nor builds its own. If a
concurrent build failed, drop the invalid index and run it again.

**Transport `text` columns.** `payload_json`/`headers_json` are `text`, like the channel's and the
durable-flow store's document columns: `jsonb` rejects the `\u0000` escape System.Text.Json emits
for U+0000 (so a job carrying a NUL was unpublishable) and re-sorts object keys (moving a `$type`
discriminator). On an auto-created schema the first start converts an existing table once, in one
`ALTER TABLE … ALTER COLUMN … TYPE` that **rewrites the table under an ACCESS EXCLUSIVE lock**,
blocking every statement on the queue while it runs. On a table with a large dead-letter backlog,
prune first or run the statement below by hand in a maintenance window. A role-level
`statement_timeout` still applies; a conversion it cuts short rolls back and is retried after the
30–60 s window. With `AutoCreateSchema = false` the store refuses a table still on `jsonb` with this
migration:

```sql
ALTER TABLE public.asyncresponse_transport_messages
    ALTER COLUMN payload_json TYPE text USING payload_json::text,
    ALTER COLUMN headers_json DROP DEFAULT,
    ALTER COLUMN headers_json TYPE text USING headers_json::text,
    ALTER COLUMN headers_json SET DEFAULT '{}';
```

For a large table, the order is: this `ALTER` in a maintenance window, then the `CONCURRENTLY`
index build, then the rollout — once the index exists the store takes no lock on the table at all.

**The conversion is one-way.** A previous-build host verifies these columns as `jsonb` at startup,
so once any host has converted the table a previous-build host that **restarts** fails (and with it
every publish) until it runs the new build; already-running old hosts keep working through the
assignment cast. Plan the rollout so no previous-build host restarts after the first new host
starts. Rolling back needs the reverse `ALTER … TYPE jsonb USING payload_json::jsonb` (and the
`headers_json` default), which fails on any row holding a `\u0000` escape: delete or repair those
rows first.

Objects you attached to the `jsonb` columns yourself — a GIN or expression index, a view, a rule, a
generated column — make the automatic conversion fail (PostgreSQL will not change the type of a
column they depend on), and every retry queues an ACCESS EXCLUSIVE request on the queue table. Drop
them before the upgrade and recreate them afterwards; an expression index over
`payload_json::jsonb` would make every publish carrying a `\u0000` fail again.

A NUL in a failure's exception message is replaced with U+FFFD in `dead_letter_reason`, since a
PostgreSQL `text` value cannot hold one.

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
        options.SchemaName = "public";
        options.MessageTable = "asyncresponse_transport_messages";
        options.NotificationChannel = "asyncresponse_transport_notify";
        options.WorkerQueue = "worker";
        options.ResponseQueue = "response";
        options.DeadLetterQueue = "deadletter";
        // Early ACK (WorkerSubscriber.UseAckAfterEnqueue) with durable flows also needs
        // AllowEarlyAckWorkerSubscriber = true — see transport-semantics.md.
    })
    .WithPostgreSqlDurableFlows(options =>
    {
        options.SchemaName = "public";
        options.TableName = "asyncresponse_flow_state";
        options.StateExpiry = TimeSpan.FromDays(14);
    });
```

The values shown are the defaults. Every option and its default is listed in
[configuration.md](configuration.md#channel-options).

Recommended Npgsql connection-string settings:

| Setting | Why |
|---|---|
| `No Reset On Close=true` | Avoids `DISCARD ALL` on every pooled check-in. AsyncResponse's `LISTEN` connections come from the same pool and run `UNLISTEN *` before they go back to it (on channel disposal, on every listen reconnect, and on every transport subscriber restart), so pooled query connections need no reset for listener state. Teardown waits at most 1 s for the `UNLISTEN` (the rest finishes in the background). A failed `UNLISTEN` on a still-open connection is retried once with a 30 s timeout; if that fails too, the data source's pool is cleared so the listening connection is never reused — the application's own pool when it shares the data source. Each clear is logged (a warning at most once a minute). |
| `Max Auto Prepare=20` | Keeps the recurring table queries prepared across reuse, reducing parse/plan CPU under load. |
| `Maximum Pool Size` | Size deliberately for all app instances sharing the same server; early-ACK load can otherwise exhaust PostgreSQL's `max_connections`. |

## Operational notes

- **Identifiers.** Use simple PostgreSQL identifiers for schema/table/notification names: letters,
  digits, and underscores, not starting with a digit, at most 63 characters (PostgreSQL silently
  truncates longer names, so validation rejects them). Derived names — `{MessageTable}_ack_seq` and
  the `*_idx` indexes — reserve their suffix space by truncating the table stem, and validation
  rejects a configuration whose name plan collides (a table occupying a derived name, or two
  near-cap tables deriving the same index name). When the channel, transport, and durable-flow
  store share one schema, each also verifies its relations against the catalog (kind and, for
  indexes, owning table), so a name occupied by another component's object fails startup with a
  rename error instead of `CREATE ... IF NOT EXISTS` silently skipping the DDL.
- **Identity columns need a deterministic collation.** `correlation_id`/`registration_id`
  (channel), `queue` (transport), and `flow_id` (durable-flow store) are compared ordinally; a
  non-deterministic ICU collation folds distinct ids onto one key, so lookups cross-match. Catalog
  verification fails actionably on such a column (`"C"` always qualifies) and needs
  **PostgreSQL 12+** (`pg_collation.collisdeterministic`).
- **The channel's `LISTEN` needs a session-pooled or direct connection.** Behind a transaction- or
  statement-mode pooler (PgBouncer `pool_mode = transaction`, most serverless proxies) the `LISTEN`
  runs on a server connection that goes straight back to the pool, and no notification ever reaches
  the channel. It then logs `PostgreSQL LISTEN loop failed` every reconnect cycle and runs on the
  sweep alone (the wake-down cadence below), so cross-process responses arrive up to 1.25 s late and
  more of them race the publisher's confirmation budget. Register the data source the channel uses
  against the server directly or through a session-mode pool; the rest of the application can keep
  a transaction-mode pooler. A pool that hands the delivery probe back to the same server
  connection can pass it by luck, so do not rely on the probe to detect this.
- **LISTEN liveness.** A `LISTEN` counts only once a delivery probe comes back: the listen
  connection sends itself a `NOTIFY` and must receive it within 5 s. The same probe runs after 10 s
  without a notification, so a half-open socket or a connection that stopped receiving fails into
  the reconnect path (backoff from 100 ms to 5 s; a `LISTEN` that stayed up at least 5 s resets it).
  Each proven `LISTEN` triggers one immediate full sweep for the notifications published while none
  was up.
- **Sweep cadence.** `FullSweepInterval` (default 5 s) throttles the safety sweep only while a
  proven `LISTEN` is up. Before the first one and from any listen failure until the next, the sweep
  is the only cross-process wake and runs every `min(FullSweepInterval, DeliveryConfirmationTimeout
  / 4)` — 1.25 s on defaults. A full sweep dispatches up to 8 correlation ids at a time, and one
  id's failure does not stop the others. When the first 8 ids of a pass all fail with a transient
  fault (the database is down), the rest of the pass is skipped and the logged exception carries the
  first 3 failures; a requested sweep cut short this way, and an id whose own pass failed
  transiently, is retried after that same floor rather than the full throttle. A sweep that visited
  a waiter without failures is recorded on the `asyncresponse.channel.sweep.duration` histogram;
  one longer than half of `DeliveryConfirmationTimeout` logs a warning (at most once a minute),
  because past that point responses only the sweep delivers can be claimed for lost-subscriber
  recovery under live waiters.
- **Heartbeats.** Keep `SubscriberHeartbeatInterval` (default 10 s) lower than
  `SubscriberHeartbeatTimeout` (default 30 s); publishers use the subscriber rows to decide whether
  to wait for live delivery. Rows no longer in the process's active snapshot are allowed to expire
  even if cleanup deletion failed. A failed round is retried on a short backoff (at most 1 s, never
  later than the interval); the first failure of a run logs a warning, later ones at most one per
  interval. A publish from the waiter's own process treats its live local subscription as live even
  while the row has lapsed.
- **Forward cursor.** Normal scans keep a forward `created_at, id` cursor per local subscription
  group. Caught-up polls revisit only the last database-clock tick, so a same-timestamp message with
  a lower random id is still picked up. New subscriptions reset the cursor to apply each waiter's
  own watermark; acknowledged messages remain eligible for legitimate cross-process fan-out.
- **Late-commit lookback.** `created_at` is stamped when a response's INSERT runs, not when it
  commits, so a response can become visible *behind* a cursor that already read a later row. For a
  while after the cursor moves (twice the window), a pass revisits a lookback window behind it —
  half of `DeliveryConfirmationTimeout`, at most 2 s — and the late commit's own wake delivers it
  within its publisher's confirmation budget. The whole window is revisited at most once per
  `ListenerPollInterval` (or once per lookback, if longer) per correlation id; passes in between
  read the last tick only and schedule a rescan for when that throttle ends. A commit slower than the
  window is left to history reconciliation and can route to recovery. Rows read again are not
  re-queued.
- **History reconciliation.** `HistoryReconciliationInterval` (default 5 s after the last completed
  reconciliation) starts a retained-history pass, one page per dispatch pass, so large histories add
  poll intervals to the discovery of late commits; it is linear in retained rows. Size waiter
  timeouts and `MessageRetention` accordingly. The interval, the lookback window, and seen-message
  aging run on the real monotonic clock, never on an injected `TimeProvider`.
- **Paging and backpressure.** `PendingMessageBatchSize` (default 64) sets the page size; a
  correlation gets at most 16 forward pages plus one reconciliation page per pass before yielding.
  Delivery is serialized per correlation id on a bounded (1,024-item) executor that the sweep admits
  to **without waiting**: when one id's executor is full (a waiter wedged in a slow `Until` predicate
  under a progress flood), the rest of that id's rows stay unclaimed in order and only that id is
  rescanned after one poll interval, while every other id keeps delivering. The same-process fast
  path also never parks the publisher: at capacity it leaves the stored row to the sweep. Failed
  delivery claims request an immediate rewind; acknowledged payload bodies are hydrated only when a
  waiter needs them.
- **Confirmation budget.** Keep `DeliveryConfirmationTimeout` long enough for the slowest expected
  live delivery, but short enough that a truly lost subscriber routes to recovery promptly. A
  transient fault in the confirmation poll reads as "not yet delivered", and the recovery claim at
  its deadline is retried on the publish retry policy (the response row is already stored).
- **Dead-letter retention.** Set the transport's `DeadLetterRetention` if operators do not inspect
  dead-letter rows indefinitely. The prune runs after a publish at most once a minute per process,
  in batches of 1,000 drained for up to 2 s, and never fails the publish it follows. The channel's
  expired-row prunes use the same bounded loop, throttled by `PruneInterval`.
- **Monitoring.** Watch table size, dead-letter count, connection usage, and lock waits from your
  database tooling. AsyncResponse reports library metrics, not database-native queue depth.
