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
  normally land on the next active poll; backlog, backpressure, and late-commit reconciliation
  can add intervals. An idle app avoids full retained-history reads on every tick.
- A new waiter re-arms the tight interval immediately and triggers a targeted scan of its own
  correlation id, so a response stored before the waiter subscribed is picked up at once.
- The poll deadline is **absolute**: local publishes and new waiters wake the loop for a targeted
  scan of their own correlation id, but they do not postpone the next sweep. A process that keeps
  publishing to its own waiters still sweeps every `ActivePollInterval`, so a response written by
  another process lands on schedule. (Earlier versions restarted the poll timer on every wake-up
  and swept only when that timer won the race, so sustained local traffic held cross-process
  responses back until the publisher's delivery confirmation lapsed and routed them to
  lost-subscriber recovery.)
- The sweep keeps a stable `created_at, id` cursor across passes. Each correlation yields after
  16 pages and continues on a later pass; periodic history reconciliation catches late commits
  behind the cursor, including responses already acknowledged by another process.
- Delivery is serialized per correlation id on a bounded (1024-item) executor, and the sweep
  admits work to it **without waiting**: a correlation id whose executor is full (a waiter wedged
  in a slow `Until` predicate under a progress flood) has the rest of its rows left unclaimed, in
  order, and is rescanned alone after one poll interval, while every other correlation id keeps
  delivering. Until round 35 the sweep awaited that capacity, so one saturated correlation
  stalled every waiter in the process.

Active waiters write rows to `asyncresponse_channel_subscribers`; one channel-level loop snapshots
the registrations that are still active locally and extends only those rows in bounded SQL batches
per heartbeat interval. A publisher first checks for live subscribers; if none exist, it routes
directly to lost-subscriber recovery. If subscribers do exist, the publisher inserts a message row
and waits for delivery confirmation:

1. Same-process delivery completes an in-memory confirmation immediately.
2. Cross-process delivery sets `acked_at` (plus `acked_seq`, drawn from the message table's own
   `SEQUENCE`, `{message_table}_ack_seq`),
   which the publisher polls as a fallback.
3. If no waiter confirms before `DeliveryConfirmationTimeout`, the publisher atomically sets
   `recovery_claimed = 1` while `acked_at IS NULL` and dispatches the persisted recovery callback.

That last claim is the race guard: a slow live waiter and the recovery callback cannot both own the
same response. Row expiries (`expires_at`) are always computed on the **database clock**
(`SYSUTCDATETIME()`), as is the waiter's delivery watermark, so app-side clock skew cannot drop or
resurrect messages. `acked_seq` and each subscription's registration draw from the same monotonic
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
`WorkerSubscriber.EmptyPollDelay` / `ResponseSubscriber.EmptyPollDelay` (default 250 ms). A NAK
and a delayed publish raise no wake: the row is not claimable until its delay has passed, and the
next poll after that picks it up.

The claim orders by `(available_at, created_at)` — availability order, which equals publish order
for every row that was neither delayed nor released for retry — and the dequeue index
`{message_table}_ready_idx` is keyed on exactly `(queue, available_at, created_at)`, so a claim is
one ordered index seek that stops at the first unleased row. The lease renewal reads its row count
from `SELECT @@ROWCOUNT`, so a server-wide `SET NOCOUNT ON` (`sp_configure 'user options', 512`)
cannot make a successful renewal look like a lost lease.

Response ingress reads the correlation id from `CorrelationIdHeader` first, then from configured JSON
paths such as `CorrelationId`, `CustomParameters.CorrelationId`, and nested JSON strings. Both the
publish and receive paths emit OpenTelemetry spans with standard messaging attributes
(`messaging.system = sqlserver`, destination, delivery attempt).

## Schema creation

Both packages can create their schema, tables, and indexes on startup (`AutoCreateSchema = true`).
Channel and transport take the same transaction-scoped application lock
(`sp_getapplock`, resource `asyncresponse:ddl:{SchemaName}`) before DDL runs, so concurrent app
instances — and the channel and transport inside one app — never race each other through the
`IF NOT EXISTS` guards. The transport holds it only for the schema and the table: its index builds,
whose run time grows with the table, run afterwards in a transaction of their own under the
table's lock (`asyncresponse:ddl:{SchemaName}.{MessageTable}`), so the channel's and the
durable-flow store's startup DDL on the same schema never waits for them. In the transport, one
startup-DDL attempt runs at a time, shared by every operation waiting for it, and a caller's
cancellation ends only that caller's wait — it does not roll back an index build in progress (the
channel runs its DDL on the calling operation's own token).

The packages do **not** create the database itself: point `ConnectionString` at an existing database
(the sample app ships a small provisioner that creates it for containers/dev). Set
`AutoCreateSchema = false` when migrations own the schema. Keep channel and transport table names
distinct even when they share the same schema.

When your migration owns the transport table, match this shape — the `queue` column in particular:

```sql
CREATE TABLE dbo.asyncresponse_transport_messages (
    id uniqueidentifier NOT NULL PRIMARY KEY NONCLUSTERED,
    -- nvarchar, not varchar or nchar: queue names are Unicode, and a blank-padded nchar column
    -- cannot be matched exactly at all. The binary collation is belt-and-braces — the claim
    -- predicate carries its own COLLATE — but it makes the intent visible in the schema.
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

The store verifies this shape against the catalog the first time it's used — under
`AutoCreateSchema = false` just as under `AutoCreateSchema = true` (after its own DDL): an absent
table is assumed not yet migrated and re-checked on the next operation, while a present table with
the wrong shape throws with the fix instead of failing silently at the first claim. Verification
now also checks scale on every fractional-seconds column: a bare `datetime2` above is already
`datetime2(7)`, so the shape needs no change, but a manually reduced scale (`datetime2(3)`,
`datetime2(0)`) is rejected — SQL Server rounds on store, so a lower-scale column is a different
clock, not a coarser view of the same one, and could round a timestamp below an already-observed
watermark. The primary key is compared on its **key** columns only: the `PRIMARY KEY NONCLUSTERED`
above, with the clustered index on `created_at`, is the accepted shape — SQL Server lists the
clustering key on the nonclustered PK at ordinal 0, and it is not part of the key. The one column verification deliberately leaves alone on a table it did not create is
`queue`: the claim predicate below supplies the binary collation itself and defeats padding on its
own, so a schema an older build or a hand-written migration left as `varchar` or on the server's
case-insensitive default still claims exactly and is accepted as-is. On a table the store created,
the declared `nvarchar(200) COLLATE Latin1_General_100_BIN2` is held to exactly — there, a
difference means the column was altered afterwards. The derived indexes are verified the same way
on tables this build created (the name-only `IF NOT EXISTS` guard would otherwise accept a
same-name index with the wrong definition and silently cost the claim its seek); on a table your
migration owns, the indexes above are yours to keep in shape — they carry claim performance, not
correctness — but the store **warns** once at first use when no index on the table leads on
`queue`, since every claim would then scan the whole table under `UPDLOCK` with nothing else to
show for it.

The three logical queues share this one table and are told apart by the `queue` column alone, so
the claim query matches it exactly — `queue = @queue AND queue + N'.' = @queue + N'.' COLLATE
Latin1_General_100_BIN2`. The sentinel is there because SQL Server pads the shorter operand of an
equality comparison with spaces under *every* collation, binary ones included, so `worker ` would
otherwise answer a query for `worker`; the explicit collation is there because a column left on a
case-insensitive server default would otherwise answer with `WORKER`. The plain comparison is kept
as the driver so the claim index is still seeked.

The channel does the same: with `AutoCreateSchema = false` it runs its full relation verification
(tables, columns, indexes, and the binary-collation requirement on identity columns such as
`correlation_id`) at first use, not just the migration probe below — an operator-provisioned
column under a case-insensitive collation is rejected with the fix instead of silently
cross-routing responses at runtime (SQL Server `=` also pads trailing spaces under every collation,
binary ones included, which is why correlation ids with surrounding spaces are refused up front).

### Upgrading a manually managed schema

1.0.0 added a monotonic ack sequence to the channel message table. With `AutoCreateSchema = false`
the channel validates these objects once at startup and fails with an actionable error until the
migration below is applied (names shown for the default `dbo.asyncresponse_channel_messages`; the
sequence is always `{message_table}_ack_seq` in the same schema):

```sql
IF COL_LENGTH(N'dbo.asyncresponse_channel_messages', N'acked_seq') IS NULL
    ALTER TABLE dbo.asyncresponse_channel_messages ADD acked_seq bigint NULL;
IF NOT EXISTS (SELECT 1 FROM sys.sequences
               WHERE name = N'asyncresponse_channel_messages_ack_seq' AND schema_id = SCHEMA_ID(N'dbo'))
    CREATE SEQUENCE dbo.asyncresponse_channel_messages_ack_seq AS bigint START WITH 1;
```

The column is nullable and the migration is safe to run while old-version hosts are still up:
rows they ack carry no sequence and fall back to the previous watermark rule.

The transport's dequeue index changed shape (PostgreSQL parity). It is now
`{message_table}_ready_idx` over `(queue, available_at, created_at)`, and the claim orders by that
tail. The previous `{message_table}_claim_idx` over `(queue, available_at, locked_until,
created_at)` behind `ORDER BY created_at` could not serve its own ordering past the
`available_at` range: the plan either sorted the whole ready set — U-locking every row it scanned,
so competing `READPAST` claimers skipped them all and slept — or walked `created_idx` through the
older rows of the other logical queues (retained dead letters, delayed jobs), so draining a burst
of K rows cost O(K²). On an auto-created schema this build creates the new index itself when it
is absent, and leaves the old one alone. That build — and the `created_at` index's, when that one is
missing — is a plain, offline `CREATE INDEX`, run under the table's own application lock after the
schema-wide one is released (see [Schema creation](#schema-creation)): it blocks every write to the
queue table — publishes, claims, acks, NAKs, and lease renewals, old-build hosts' included — for as
long as it runs. It runs under an hour-long command timeout instead of SqlClient's 30 s, so it
finishes once instead of timing out and being retried by every later operation (the store retries
its startup DDL until it succeeds); waiting for its lock is bounded by a 5 s `LOCK_TIMEOUT`. On a
busy table — a long-running transaction, an index rebuild, a bulk load holding a conflicting lock,
another host's build of the same index — it fails without changing anything, and the host waits
30–60 s (jittered) before its next attempt: every attempt queues every write to the queue table, on
every host, behind its lock request for those 5 s, so until the retry the host's transport
operations fail at once, naming the lock wait. A build that fails any other way — a full log (9002)
or filegroup (1105), a build that outran its command timeout — is retried after the same window
rather than by the next operation. A transient connection fault on the table-lock step (severity
20 or above, a reset connection, an Azure failover error) latches nothing: that step took no lock,
and the next operation retries at once. Find the holder in `sys.dm_tran_locks`
joined to `sys.dm_exec_sessions`. On a large table (retained dead letters, parked durable-flow
timers) create the index **before** rolling out — with `ONLINE = ON` where your edition supports
it (Enterprise, Developer, Azure SQL), which keeps the table writable while it builds — and the
store then finds it and builds nothing. With `AutoCreateSchema = false`, create it when convenient and drop the old one once no
host runs the previous build; until then the store logs a warning at startup unless some enabled,
unfiltered index is keyed `(queue, available_at, created_at)` — under any name, but the previous
build's `_claim_idx` does not count, since it cannot serve the claim's order:

```sql
CREATE INDEX asyncresponse_transport_messages_ready_idx
    ON dbo.asyncresponse_transport_messages (queue, available_at, created_at)
    WITH (ONLINE = ON); -- where the edition supports it; omit the WITH clause otherwise
DROP INDEX IF EXISTS asyncresponse_transport_messages_claim_idx ON dbo.asyncresponse_transport_messages;
```

The channel's sequence is
verified as the monotonic clock it is: `bigint`, `INCREMENT BY 1`, `NO CYCLE`, and the `bigint`
maximum as its `MAXVALUE` (a restricted maximum would pass startup and then exhaust
mid-production with error 11728). Correlation ids are stored as `nvarchar(400)` key
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
  starting with a digit, at most 128 characters (sysname). Derived names —
  `{MessageTable}_ack_seq` and the `*_idx` indexes — reserve their suffix space by truncating the
  table stem, and validation rejects a configuration whose tables collide with the derived
  sequence name.
- `ActivePollInterval` bounds cross-process response latency; `IdlePollInterval` bounds idle database
  load. Same-process deliveries (the common case when the waiter and the publisher share the app)
  never wait for either.
- Keep `SubscriberHeartbeatInterval` lower than `SubscriberHeartbeatTimeout`; publishers use these
  rows to decide whether to wait for live delivery. Registration writes one row, then each interval
  updates the process's current active-registration snapshot in bounded batches. Rows no longer in
  that snapshot are allowed to expire even if cleanup deletion failed. A failed round is retried
  on a short backoff (at most one second, never later than the interval) rather than a full
  interval, so the rows come back promptly once the database answers again after an outage. The
  first failure of a run logs a warning, later ones at most one per heartbeat interval (the rest
  at Debug). A publish from the waiter's own process treats its live local subscription as live
  even while the row has lapsed, so it is delivered instead of routed to lost-subscriber recovery.
- Normal scans retain a forward `created_at, id` cursor per local subscription group. Caught-up
  polls revisit only the last database-clock tick, so a new message with the same timestamp and
  a lower random id is picked up promptly; older consumed headers are read again only by the
  late-commit lookback window below (at most once per poll interval, and at most the lookback
  apart, per correlation id) and by
  history reconciliation. New subscriptions reset the cursor to apply each waiter's own
  watermark; acknowledged messages remain eligible for legitimate cross-process fan-out.
- `created_at` is stamped when a response's INSERT runs, not when it commits, so a response can
  become visible *behind* a cursor that already read a later row. For a short while after the
  cursor moves (twice the window), a pass therefore revisits a lookback window behind it —
  half of `DeliveryConfirmationTimeout`, at most 2 seconds — and the next full sweep (every
  `ActivePollInterval` poll under the default `FullSweepInterval = null`) delivers it before its
  publisher's confirmation budget runs out. The whole window is revisited at most once per poll
  interval per correlation id — or once per lookback, when the poll interval is longer: the
  passes in between (every local publish signals one) read the last tick only and schedule a
  rescan of the id for when that throttle ends, so a steady stream on one id no longer re-reads
  the window on every pass. A commit slower than that
  window from its stamp is left to history reconciliation (below) and can route to recovery.
  Rows a revisit reads again are not re-queued: processed ones are screened by the waiters' seen
  sets, and ones still waiting in the executor by the scan's own queued set.
- A late transaction can commit behind a creation-time cursor. `HistoryReconciliationInterval`
  (default 5 seconds after the last completed reconciliation) starts a retained-history pass;
  one page is reconciled per dispatch pass, with continuation after a poll interval. Large
  histories therefore add polling intervals to discovery of late commits. Size waiter timeouts
  and message retention accordingly. History reconciliation is still linear in retained rows;
  it is separate from normal forward delivery, not a claim that all history I/O disappears.
  The interval, the lookback window and seen-message aging run on the real monotonic clock (they
  pace database state that moves in real time), never on an injected or stepped wall clock.
- `PendingMessageBatchSize` controls page size. A correlation receives at most 16 forward pages
  plus one reconciliation page per pass before yielding to other correlations. Full executor
  queues leave the refused page's cursor unchanged for retry (a refused lookback revisit puts the
  cursor back where the pass found it, and keeps the lookback window open while refusals last). Failed delivery claims request
  an immediate rewind. Acknowledged payload bodies are hydrated only when a waiter needs them,
  in statements of at most 1,000 ids (SQL Server's 2,100-parameter cap), whatever the page size.
- A full sweep (every subscribed correlation id) dispatches up to 8 correlation ids at a time,
  and one id's failure no longer stops the others. When the first 8 ids of a pass to query the
  store all fail with a transient fault (the database is down, not one row poisoned) the rest of
  the pass is skipped, and the logged exception carries the first 3 failures and counts the
  others; an id whose waiters are all mid-cleanup queries nothing and does not count. A
  requested sweep the breaker cuts short is retried after `min(FullSweepInterval,
  DeliveryConfirmationTimeout / 4)`, never sooner, and when `FullSweepInterval` is set longer
  than that, an id whose own pass failed transiently without tripping it is rescanned on its own
  after that same floor instead of waiting for the next throttled sweep. (Under the default
  `FullSweepInterval = null` every poll is a full sweep, which covers both.) The pool running out
  of connections — SqlClient's "Timeout expired … obtaining a connection from the pool", an
  `InvalidOperationException` — is raised as a transient `TimeoutException`, so the retry
  policies and this breaker count it. Each sweep that visited a waiter without any failure has
  its duration recorded on the `asyncresponse.channel.sweep.duration` histogram (tag
  `asyncresponse.channel`), and such a sweep longer than half of `DeliveryConfirmationTimeout`
  logs a warning (at most once a minute): past that point, responses only the sweep delivers can
  be claimed for lost-subscriber recovery under live waiters. A sweep with failures is neither
  recorded nor warned about — it measured connect timeouts, not the sweep.
- Keep `DeliveryConfirmationTimeout` long enough for the slowest expected live delivery (including
  one cross-process `ActivePollInterval`), but short enough that a truly lost subscriber routes to
  recovery promptly. A transient fault in the confirmation poll reads as "not yet delivered"
  rather than failing the publish, and the recovery claim at its deadline is retried.
- Set `DeadLetterRetention` if operators do not inspect dead-letter rows indefinitely. The prune
  runs after a publish at most once a minute per process, in `DELETE TOP (1000)` batches drained
  for up to 2 s while each batch comes back full (each batch turns `NOCOUNT` off for itself, so a
  server-wide `SET NOCOUNT ON` cannot end the drain after one batch); it never fails the publish it
  follows. The
  channel's expired-row prunes use the same bounded, budgeted, failure-swallowing loop (their
  batches turn `NOCOUNT` off for themselves too), so a
  deployment publishing faster than one batch per `PruneInterval` no longer outgrows them, and
  subscriber rows orphaned by a crashed process are pruned table-wide.
- Transient faults (deadlock 1205, lock timeout 1222, Azure SQL throttling codes, broken
  connections) are retried with bounded backoff on the response insert, the recovery claim at the
  end of the confirmation wait, and the transport enqueue; the confirmation poll tolerates them
  until its deadline. Subscriber registration and counting are not retried: a failure there
  fails the waiter registration (before its trigger runs) or the publish.
- Monitor table size, dead-letter count, connection usage, and lock waits from your database
  tooling. AsyncResponse reports library metrics, not database-native queue depth.
