# SQL Server channel and transport

[← Back to README](../README.md)

`AsyncResponse.Channels.SqlServer` and `AsyncResponse.Transports.SqlServer` let one Microsoft SQL
Server database act as both the durable response/recovery channel and the worker/response-ingress
transport. They are separate NuGet packages because apps often want only one side: for example,
SQL Server for recovery but an external broker for worker dispatch, or Redis/NATS for responses but
SQL Server for a simple durable worker queue. The design mirrors the [PostgreSQL pair](postgresql.md);
this page covers what differs because of what SQL Server does and does not provide.

## Channel architecture

SQL Server has no `LISTEN/NOTIFY`, so the channel wakes active waiters with an **adaptive polling
sweep** instead of a server push. Service Broker and `SqlDependency` are deliberately not used: DBAs
frequently disable them, and `SqlDependency` is effectively legacy.

- Publishing writes the serialized response envelope to `asyncresponse_channel_messages`.
- **Same-process delivery never waits for the sweep**: the publisher dispatches directly to local
  waiters and confirms through an in-memory completion.
- A single dispatch loop sweeps the message table for the subscribed correlation ids every
  `ActivePollInterval` (default 250 ms) **while any waiter is subscribed**, and backs off to
  `IdlePollInterval` (default 2 s) while the channel is idle. Cross-process deliveries normally land
  on the next active poll; backlog, backpressure, and late-commit reconciliation can add intervals.
- A new waiter re-arms the tight interval and triggers a targeted scan of its own correlation id, so
  a response stored before the waiter subscribed is picked up at once.
- The poll deadline is **absolute**: local publishes and new waiters trigger targeted scans but do
  not postpone the next sweep, so a process that keeps publishing to its own waiters still sweeps
  every `ActivePollInterval` and responses written by other processes land on schedule.
- The sweep keeps a stable `created_at, id` cursor per correlation across passes, with bounded pages
  per pass and periodic reconciliation for late commits (see [Operational notes](#operational-notes)).

Active waiters write rows to `asyncresponse_channel_subscribers`; one channel-level loop snapshots
the registrations still active locally and extends only those rows, in bounded SQL batches per
heartbeat interval. A publisher first checks for live subscribers; if none exist, it routes directly
to lost-subscriber recovery. Otherwise it inserts a message row and waits for delivery confirmation:

1. Same-process delivery completes an in-memory confirmation immediately.
2. Cross-process delivery sets `acked_at` (plus `acked_seq`, drawn from the message table's own
   `SEQUENCE`, `{message_table}_ack_seq`), which the publisher polls as a fallback.
3. If no waiter confirms before `DeliveryConfirmationTimeout`, the publisher atomically sets
   `recovery_claimed = 1` while `acked_at IS NULL` and dispatches the persisted recovery callback.

That last claim is the race guard: a slow live waiter and the recovery callback cannot both own the
same response. Row expiries and delivery watermarks use the database clock (`SYSUTCDATETIME()`), so
app-side clock skew cannot drop or resurrect messages. `acked_seq` and each subscription's
registration draw from the same monotonic sequence, which separates "acked before this waiter
registered" (history, not redelivered) from "acked to a fan-out group including this waiter"
(delivered) even within one server-clock tick. The one conservative residual: a claim whose sequence
draw stalled across ticks resolves as history, never as a replayed response. The same-process fast
path honors this too: an idempotent duplicate publish (a retry carrying the same message id)
dispatches with the stored row's settlement columns, so it cannot replay an already-consumed
response to a waiter that registered after the ack.

## Recovery state

`asyncresponse_recovery_state` stores one row per waiter registration, keyed by correlation id and
registration id. Shared-correlation waits therefore survive redeploys: if several waiters registered
callbacks for the same correlation id, a late response dispatches all stored registrations.

A registration is written once, when the waiter is created, and is not refreshed while the waiter
lives. It expires `RecoveryStateExpiry` later — or, for a waiter whose timeout (explicit, or
`DefaultTimeout`) is longer than that, when the wait itself ends, so the tail of a long wait keeps
its recovery.

The Core watchdog scans the same table through `IRecoveryStateScanner` and checks live waiters through
`IActiveSubscriberProbe`, so `AddAsyncResponseRecoveryCheck()` works with SQL Server exactly like
Redis, NATS, PostgreSQL, or MongoDB.

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
on failure. Both settlements are fenced on `lock_id`, so they are idempotent, and a transient fault
on one (a deadlock victim, a timeout, a broken pooled connection) is retried (up to 4 attempts, none
started past a third of `LockTimeout`) — a single lost round trip no longer leaves a completed job
leased until a subscriber runs it again. A release while the subscriber is stopping is not retried;
the lease lapses to the same effect. `AckAfterEnqueue` deletes the row after it enters a bounded background queue; if the
handler later fails, the original row is already acknowledged, so the dispatcher writes a dead-letter
row and invokes `OnBackgroundFailure`. Dead-lettering a poison row moves it in one transaction.
Publishes are idempotent: the caller-supplied id is inserted with an insert-if-absent
(`WHERE NOT EXISTS` under `UPDLOCK, HOLDLOCK`, duplicate-key races treated as success), so a retried
publish never enqueues the same job twice.

There is no cross-process publish notification: a publish wakes same-process subscribers through an
in-process signal, and other processes pick the row up within `WorkerSubscriber.EmptyPollDelay` /
`ResponseSubscriber.EmptyPollDelay` (default 250 ms). A NAK and a delayed publish raise no wake: the
row is not claimable until its delay has passed, and the next poll after that picks it up.

The claim orders by `(available_at, created_at)` — availability order, which equals publish order
for every row that was neither delayed nor released for retry — and the dequeue index
`{message_table}_ready_idx` is keyed on exactly `(queue, available_at, created_at)`, so a claim is
one ordered index seek that stops at the first unleased row.

The three logical queues share one table and are told apart by `queue` alone, so the claim matches
it exactly: `queue = @queue AND queue + N'.' = @queue + N'.' COLLATE Latin1_General_100_BIN2`. SQL
Server pads the shorter operand of an equality comparison with spaces under *every* collation,
binary ones included, so without the sentinel `worker ` would answer a query for `worker`; the
explicit collation stops a column on a case-insensitive default from answering with `WORKER`. The
plain comparison is kept as the driver so the index is still seeked.

Lease renewal reads its row count from `SELECT @@ROWCOUNT`, so a server-wide `SET NOCOUNT ON`
(`sp_configure 'user options', 512`) cannot make a successful renewal look like a lost lease.

Response ingress reads the correlation id from `CorrelationIdHeader` first, then from configured JSON
paths such as `CorrelationId`, `CustomParameters.CorrelationId`, and nested JSON strings.

## Schema creation

Both packages can create their schema, tables, and indexes on startup (`AutoCreateSchema = true`,
the default). Channel, transport, and the SQL Server durable-flow store take the same
transaction-scoped application lock (`sp_getapplock`, resource `asyncresponse:ddl:{SchemaName}`)
before DDL runs, so concurrent app instances never race each other through the `IF NOT EXISTS`
guards. The transport holds it only for the schema and the table: its index builds, whose run time
grows with the table, run afterwards in their own transaction under a table-scoped lock
(`asyncresponse:ddl:{SchemaName}.{MessageTable}`), so the other stores' startup DDL never waits for
them. In the transport, one attempt runs at a time, shared by every operation waiting for it; a
caller's cancellation ends only that caller's wait and does not roll back an index build (the
channel runs its DDL on the calling operation's own token).

The packages do **not** create the database: point `ConnectionString` at an existing database (the
sample app ships a small provisioner that creates it for containers/dev). Set
`AutoCreateSchema = false` when migrations own the schema. Keep channel and transport table names
distinct even when they share the same schema.

When your migration owns the transport table, match this shape:

```sql
CREATE TABLE dbo.asyncresponse_transport_messages (
    id uniqueidentifier NOT NULL PRIMARY KEY NONCLUSTERED,
    queue nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
    payload_json nvarchar(max) NOT NULL,
    headers_json nvarchar(max) NOT NULL DEFAULT N'{}',
    created_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
    available_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
    locked_until datetime2 NULL,
    lock_id uniqueidentifier NULL,
    attempts int NOT NULL DEFAULT 0,
    dead_letter_reason nvarchar(max) NULL
);
CREATE INDEX asyncresponse_transport_messages_ready_idx
    ON dbo.asyncresponse_transport_messages (queue, available_at, created_at);
CREATE INDEX asyncresponse_transport_messages_created_idx
    ON dbo.asyncresponse_transport_messages (created_at);
```

Both packages verify their relations against the catalog at first use — with
`AutoCreateSchema = false` as with `true` (after their own DDL). An absent table is assumed not yet
migrated and re-checked on the next operation; a present table with the wrong shape throws with the
fix instead of failing at the first publish or claim. What verification checks:

- **Column types, including scale.** A bare `datetime2` is `datetime2(7)`; a reduced scale
  (`datetime2(3)`) is rejected, because SQL Server rounds on store and a lower-scale column could
  round a timestamp below an already-observed watermark.
- **Primary keys by key columns only**, so adding your own clustered index (for example on
  `created_at`) beside the nonclustered PK is accepted.
- **Binary collation on identity columns** — the channel's `correlation_id`, and the durable-flow
  store's `flow_id` — see [Operational notes](#operational-notes).
- **The transport's `queue` column only on a table the store created**, where it must be exactly
  `nvarchar(200) COLLATE Latin1_General_100_BIN2`. On a table your migration owns the claim
  predicate supplies the binary collation and defeats padding itself, so a `varchar` or
  case-insensitive `queue` is accepted as-is (nvarchar is still recommended: queue names are
  Unicode).
- **Transport indexes only on a table the store created** (the name-only `IF NOT EXISTS` guard would
  otherwise accept a same-name index with the wrong definition). On a table your migration owns the
  indexes are yours: they carry claim performance, not correctness, and the store **warns** at
  first use when no enabled, unfiltered index is keyed `(queue, available_at, created_at)` — under
  any name — since every claim would then sort the ready set under `UPDLOCK`.
- **The channel's ack sequence** as the monotonic clock it is: `bigint`, `INCREMENT BY 1`,
  `NO CYCLE`, and the `bigint` maximum as `MAXVALUE` (a restricted maximum would pass startup and
  exhaust mid-production with error 11728).

### Upgrading a manually managed schema

Schemas created by earlier builds need the changes below. With `AutoCreateSchema = true` the store
applies them itself on first start; with `AutoCreateSchema = false` it fails (or, for the dequeue
index, warns) with the statement to run.

**Channel ack sequence.** The message table needs `acked_seq` and its sequence (names shown for the
default `dbo.asyncresponse_channel_messages`; the sequence is always `{message_table}_ack_seq` in the
same schema):

```sql
IF COL_LENGTH(N'dbo.asyncresponse_channel_messages', N'acked_seq') IS NULL
    ALTER TABLE dbo.asyncresponse_channel_messages ADD acked_seq bigint NULL;
IF NOT EXISTS (SELECT 1 FROM sys.sequences
               WHERE name = N'asyncresponse_channel_messages_ack_seq' AND schema_id = SCHEMA_ID(N'dbo'))
    CREATE SEQUENCE dbo.asyncresponse_channel_messages_ack_seq AS bigint START WITH 1;
```

The column is nullable and the migration is safe to run while older hosts are still up: rows they
ack carry no sequence and fall back to the timestamp watermark rule.

**Transport dequeue index.** The claim needs `{message_table}_ready_idx` over
`(queue, available_at, created_at)`. The previous `{message_table}_claim_idx` over
`(queue, available_at, locked_until, created_at)` cannot serve the claim's order: the plan either
sorted the whole ready set — U-locking every row it scanned, so competing `READPAST` claimers
skipped them all and slept — or walked the `created_at` index through other queues' rows, so
draining a burst of K rows cost O(K²). The previous build's `_claim_idx` does not satisfy the
missing-index warning. Create the new index, and drop the old one once no host runs the previous
build:

```sql
CREATE INDEX asyncresponse_transport_messages_ready_idx
    ON dbo.asyncresponse_transport_messages (queue, available_at, created_at)
    WITH (ONLINE = ON); -- where the edition supports it; omit the WITH clause otherwise
DROP INDEX IF EXISTS asyncresponse_transport_messages_claim_idx ON dbo.asyncresponse_transport_messages;
```

On an auto-created schema the store builds a missing `_ready_idx` (or `_created_idx`) itself and
never drops the old index. That build is a plain, offline `CREATE INDEX` under the table's own
application lock: it blocks every write to the queue table (publishes, claims, acks, NAKs, lease
renewals — older hosts' included) while it runs, under an hour-long command timeout instead of
SqlClient's 30 s. Its lock wait is bounded by a 5 s `LOCK_TIMEOUT`; on a busy table (a long-running
transaction, an index rebuild, a bulk load, another host's build) each attempt queues every write
behind it for those 5 s, then fails without changing anything and retries after a jittered 30–60 s
window, during which the host's transport operations fail at once, naming the lock wait. A build
that fails any other way — a full log (9002) or filegroup (1105), an exceeded command timeout — is
retried after the same window. A transient connection fault on the table-lock step (severity 20 or
above, a reset connection, an Azure failover error) latches nothing, and the next operation retries
at once. Find the holder in `sys.dm_tran_locks` joined to `sys.dm_exec_sessions`. On a large table
(retained dead letters, parked durable-flow timers) create the index **before** rolling out, with
`ONLINE = ON` where your edition supports it (Enterprise, Developer, Azure SQL); the store then
builds nothing.

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
        options.SchemaName = "dbo";
        options.MessageTable = "asyncresponse_transport_messages";
        options.WorkerQueue = "worker";
        options.ResponseQueue = "response";
        options.DeadLetterQueue = "deadletter";
        // Early ACK (WorkerSubscriber.UseAckAfterEnqueue) with durable flows also needs
        // AllowEarlyAckWorkerSubscriber = true — see transport-semantics.md.
    })
    .WithSqlServerDurableFlows(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("SqlServer");
        options.SchemaName = "dbo";
        options.TableName = "asyncresponse_flow_state";
        options.StateExpiry = TimeSpan.FromDays(14);
    });
```

Apart from `ConnectionString` (required), the values shown are the defaults. Every option and its
default is listed in [configuration.md](configuration.md#channel-options).

Connection-string notes:

| Setting | Why |
|---|---|
| `Max Pool Size` | Size deliberately for all app instances sharing the same server; early-ACK load can otherwise exhaust SQL Server's worker/connection budget. |
| `TrustServerCertificate=True` | Needed against dev/CI containers with self-signed certificates; use a real certificate in production instead. |
| `Database=...` | Must name an existing database — the packages create schema/tables, never the database. |

## Operational notes

- **Identifiers.** Use simple SQL Server identifiers for schema/table names: letters, digits, and
  underscores, not starting with a digit, at most 128 characters (`sysname`). Derived names —
  `{MessageTable}_ack_seq` and the `*_idx` indexes — reserve their suffix space by truncating the
  table stem, and validation rejects a channel configuration whose tables collide with each other
  or with the derived sequence name. Catalog verification also catches a name occupied by another
  component's object when stores share a schema.
- **Identity columns need a binary collation.** `correlation_id` (channel) and `flow_id`
  (durable-flow store) are compared ordinally; any non-binary collation folds something the library
  treats as distinct (case under `_CI_`, accents under `_AI_`). Auto-created tables get
  `COLLATE Latin1_General_100_BIN2`; verification rejects an operator-provisioned column whose
  collation is not `_BIN`/`_BIN2`. Because `=` pads trailing spaces under every collation, the
  library rejects correlation ids that begin or end with a space.
- **Correlation id length.** `correlation_id` is `nvarchar(400)`, matching
  `AsyncResponseChannelOptions.MaxCorrelationIdLength` (400); every channel rejects longer ids.
- **Poll intervals.** `ActivePollInterval` bounds cross-process response latency; `IdlePollInterval`
  bounds idle database load. Same-process deliveries never wait for either.
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
  half of `DeliveryConfirmationTimeout`, at most 2 s — and the next full sweep (every poll under the
  default `FullSweepInterval = null`) delivers it within its publisher's confirmation budget. The
  whole window is revisited at most once per poll interval (or once per lookback, if longer) per
  correlation id; passes in between read the last tick only and schedule a rescan. A commit slower
  than the window is left to history reconciliation and can route to recovery. Rows read again are
  not re-queued.
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
  rescanned after one poll interval, while every other id keeps delivering. Failed delivery claims
  request an immediate rewind. Acknowledged payload bodies are hydrated only when a waiter needs
  them, in statements of at most 1,000 ids (SQL Server's 2,100-parameter cap).
- **Sweep failures.** A full sweep dispatches up to 8 correlation ids at a time, and one id's
  failure does not stop the others. When the first 8 ids of a pass all fail with a transient fault
  (the database is down), the rest of the pass is skipped and the logged exception carries the
  first 3 failures. With `FullSweepInterval` set, a sweep cut short this way, and an id whose own
  pass failed transiently, is retried after `min(FullSweepInterval, DeliveryConfirmationTimeout /
  4)` instead of the full throttle. Pool exhaustion (SqlClient's "Timeout expired … obtaining a
  connection from the pool", an `InvalidOperationException`) is raised as a transient
  `TimeoutException`, so the retry policies and this breaker count it. A sweep that visited a waiter
  without failures is recorded on the `asyncresponse.channel.sweep.duration` histogram; one longer
  than half of `DeliveryConfirmationTimeout` logs a warning (at most once a minute), because past
  that point responses only the sweep delivers can be claimed for lost-subscriber recovery under
  live waiters.
- **Confirmation budget.** Keep `DeliveryConfirmationTimeout` long enough for the slowest expected
  live delivery (including one cross-process `ActivePollInterval`), but short enough that a truly
  lost subscriber routes to recovery promptly. The poll sweep is how every cross-process response is
  delivered, so an `ActivePollInterval` or `FullSweepInterval` above half of
  `DeliveryConfirmationTimeout` logs a warning when the channel is constructed: a response can then
  reach its publisher's deadline before a sweep visits it, and is claimed for recovery while its
  waiter is live.
- **Ambient transactions.** The channel's connections are opened with `Enlist=false`, whatever the
  configured connection string says, so its statements never join an ambient `System.Transactions`
  transaction (`TransactionScope`). Every channel statement is its own autocommit by design — a
  delivery or recovery claim, a subscriber row and a published response must be visible to other
  processes immediately, and the publish protocol (insert, then wait for another process to claim
  the row) cannot complete inside a transaction that commits later. So a response published inside
  a scope is stored and delivered at once and is **not** undone if the scope rolls back; publish it
  after the commit when it must depend on the outcome. The channel's background loops never run in
  the context of the request that created the first waiter (its `Activity`, log scope or
  transaction). The transport is unaffected: its publish follows the configured `Enlist` setting as
  before.
- **Transient faults.** Deadlock 1205, lock timeout 1222, Azure SQL throttling codes, and broken
  connections are retried with bounded backoff on the response insert, the recovery claim at the end
  of the confirmation wait, and the transport enqueue; the confirmation poll reads them as "not yet
  delivered" until its deadline. Subscriber registration and counting are not retried: a failure
  there fails the waiter registration (before its trigger runs) or the publish.
- **Dead-letter retention.** Set the transport's `DeadLetterRetention` if operators do not inspect
  dead-letter rows indefinitely. The prune runs after a publish at most once a minute per process,
  in `DELETE TOP (1000)` batches drained for up to 2 s, and never fails the publish it follows. The
  channel's expired-row prunes use the same bounded loop, throttled by `PruneInterval`, and also
  remove subscriber rows orphaned by a crashed process. Each batch turns `NOCOUNT` off for itself,
  so a server-wide `SET NOCOUNT ON` cannot end a drain after one batch.
- **Monitoring.** Watch table size, dead-letter count, connection usage, and lock waits from your
  database tooling. AsyncResponse reports library metrics, not database-native queue depth.
