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
keeps a stable `created_at, id` keyset cursor across dispatch passes, with bounded pages per
pass and periodic reconciliation for late commits. Continued paging reaches terminal responses
beyond the first progress batch without monopolizing other correlations.

Active waiters write rows to `asyncresponse_channel_subscribers`; one channel-level loop snapshots
the registrations that are still active locally and extends only those rows with one statement per
heartbeat interval. A publisher first checks for live subscribers; if none exist, it routes directly
to lost-subscriber recovery. If subscribers do exist, the publisher inserts a message row and waits
for delivery confirmation:

1. Same-process delivery completes an in-memory confirmation immediately.
2. Cross-process delivery sets `acked_at` (plus `acked_seq`, drawn from the message table's own
   sequence, `{message_table}_ack_seq`),
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
take the same transaction-scoped advisory lock for the configured schema before DDL runs (the
PostgreSQL durable-flow store takes it too). This matters because `CREATE ... IF NOT EXISTS` can still
race through PostgreSQL system catalogs when multiple processes start together.

The startup DDL of all three stores runs under the same bounds:

- Every DDL transaction starts with `SET LOCAL lock_timeout = '5s'`, before it takes the advisory
  lock, so neither the advisory lock (held by another host's DDL) nor a table lock is waited for
  longer than 5 s.
- Only objects the catalog shows missing are altered or built. `ALTER TABLE … ADD COLUMN IF NOT
  EXISTS` and `CREATE INDEX IF NOT EXISTS` take their ACCESS EXCLUSIVE / SHARE lock on the table
  *before* they find the object present, so running them unconditionally queued every start behind
  any conflicting lock holder, with every statement on the table queued behind that request. On a
  schema that is already up to date the startup DDL now takes no table lock at all.
- Work whose run time grows with the table — the one-time `jsonb` → `text` conversions, an index
  build on an existing table, the channel's column additions — runs in a transaction of its own,
  after the schema-wide advisory lock has been released, under an advisory lock scoped to the table
  (`asyncresponse:ddl:{schema}.{table}`), with an hour-long command timeout. Other stores sharing the
  schema never wait for it.
- A failed attempt — a lock wait that timed out, or any server error from that long-running work
  except a name collision (a `statement_timeout` it outran, a view or rule depending on a converted
  column, a full disk) — is retried after a jittered 30–60 s window. Until then the store's
  operations on that host fail at once, naming the cause, instead of each one re-running the DDL. A
  failure that is not a server error (a dropped connection) latches nothing, and neither does a
  transient fault that ends the statement as soon as the server detects it — a deadlock (40P01), a
  serialization failure (40001), a server shutdown (57P01): the next operation runs the DDL again.
- In the transport, one attempt runs at a time, shared by every operation waiting for it, and a
  caller's cancellation (a request's token) ends only that caller's wait: it does not roll back a
  conversion or index build in progress.

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

With `AutoCreateSchema = true` the channel's startup DDL follows the bounds above: on a schema
created by an earlier build it adds `recovery_claimed` / `acked_seq`, converts `envelope_json` /
`state_json` from `jsonb`, and builds its indexes only when the catalog shows them missing, so a
routine restart takes no lock on the channel tables.

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

The transport's dequeue index also changed shape. It is now
`{message_table}_ready_idx` over `(queue, available_at, created_at)` — the claim orders by exactly
that tail, so it is one ordered index descent that stops at the first unleased row. The previous
`{message_table}_claim_idx` over `(queue, available_at, locked_until, created_at)` could not serve
its own ordering behind the `available_at` range predicate, so every claim either walked the
`created_at` index through the older rows of the other logical queues (retained dead letters,
delayed jobs) or sorted the whole ready set — draining a burst of K rows cost O(K²).

With `AutoCreateSchema = false` the new index is **verified when present and only warned about when
absent**: it is claim performance, not correctness, so a schema still carrying the old index keeps
starting. Create it when convenient — `CONCURRENTLY` needs no write lock — and drop the old one
once no host runs the previous build:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS asyncresponse_transport_messages_ready_idx
    ON public.asyncresponse_transport_messages (queue, available_at, created_at);
DROP INDEX CONCURRENTLY IF EXISTS public.asyncresponse_transport_messages_claim_idx;
```

On an auto-created schema this build creates the new index itself when it is absent, and leaves
the old one alone: `DROP INDEX` takes an ACCESS EXCLUSIVE lock on a live queue, so dropping it is
the operator's call. That build is a plain, non-concurrent `CREATE INDEX` in a transaction of its own,
under the table's advisory lock rather than the schema's (so the channel's and durable-flow store's
startup DDL on the same schema never waits for it): it holds a SHARE lock that blocks every write to
the queue table — publishes, claims, acks, NAKs, and lease renewals, old-build hosts' included — for
as long as it runs. It runs under an hour-long command timeout instead of the data source's, so it
finishes once instead of rolling back at 30 s and being retried by every later operation (the store
retries its startup DDL until it succeeds), and a publish whose own token is cancelled meanwhile does
not cancel it. Waiting for its lock is bounded by a 5 s `lock_timeout`, which
outwaits an ordinary autovacuum (it cancels itself for a conflicting lock request) but not a lock
that does not yield — an anti-wraparound autovacuum, a manual `VACUUM` or `ANALYZE`, a long-running
or idle-in-transaction session that wrote to the table (and, for the conversion below, `pg_dump` or
any open transaction that read it). On such a busy table the startup DDL fails without
changing anything, and the host waits 30–60 s (jittered) before its next attempt: every attempt
parks every statement on the queue table, on every host, behind its lock request for those 5 s,
so until the retry the host's transport operations fail at once, naming the lock wait. Find the
holder in `pg_locks` joined to `pg_stat_activity`. On a large table (retained dead letters, parked
durable-flow timers) run the `CREATE INDEX CONCURRENTLY` above **before** rolling out — but only once
the columns are already `text`: the one-time `jsonb` → `text` conversion below runs first, needs
`ACCESS EXCLUSIVE` (so it waits behind a running `CONCURRENTLY` build) and rewrites every index
anyway. For a large table from a previous build, run the documented migration `ALTER` in a
maintenance window, then the `CONCURRENTLY` build, then roll out — from the `ALTER` on, no
previous-build host may restart (see "The conversion is one-way" below), so roll out straight after
the build; the store creates only an index that does not exist yet, so it then takes no lock on the
table at all.

A dequeue index that exists but is not yet valid and ready — a `CONCURRENTLY` build still
running, or one that failed and left an invalid index behind — is a warning, not a startup
failure, on either kind of schema (only an absent index is built), and once the columns are
`text` a host starting during the build neither waits for it nor builds its own. If a concurrent build failed, drop the invalid index and
run the statement again.

The transport's `payload_json`/`headers_json` columns are now `text`, like the channel's and the
durable-flow store's: `jsonb` rejects the `\u0000` escape System.Text.Json emits
for U+0000, so a job or response carrying a NUL in a string argument or context value could not
be published on PostgreSQL at all, and `jsonb` re-sorted object keys (moving a `$type`
discriminator behind shorter keys). On an auto-created schema the first start of this build
converts an existing table in place, once — one `ALTER TABLE … ALTER COLUMN … TYPE` for both
columns that **rewrites the table under an ACCESS EXCLUSIVE lock**, blocking every statement on
the queue (old-build hosts' included) while it runs, so on a table holding a large retained
dead-letter backlog run it in a maintenance window, or prune first. The rewrite runs under the
same hour-long command timeout rather than the data source's (one that outran that rolled back and
was retried, lock and all, by every later operation); a `statement_timeout` configured for the role
still applies — a conversion it cuts short is rolled back and retried after the same 30–60 s window,
not by the next operation — and a table whose rewrite could outrun either is converted by hand,
ahead of the rollout, with the `ALTER TABLE` below. Waiting for the lock is bounded by the same 5 s
`lock_timeout`, so on a busy table the conversion fails fast, changes nothing, and is retried
after the same 30–60 s window.

**The conversion is one-way.** The previous build verifies `payload_json`/`headers_json` as
`jsonb`, exactly, at startup. Once any host of this build has converted the table, a
previous-build host that **restarts** fails that check — and with it every publish — until it runs
this build too; previous-build hosts that are already running keep working (their `jsonb` values
are stored into the `text` columns through the assignment cast). Rolling the deployment back needs
the reverse `ALTER TABLE … ALTER COLUMN payload_json TYPE jsonb USING payload_json::jsonb, …`
(and the `headers_json` default), which fails as soon as any row holds a document `jsonb` rejects —
a `\u0000` escape, exactly what this build makes publishable: delete or repair those rows first.
Plan the rollout so no previous-build host restarts after the first host of this build starts.

Objects you attached to the `jsonb` columns yourself — a GIN or expression index, a view, a rule,
a generated column — make the automatic conversion fail, because PostgreSQL will not change the type
of a column they depend on: every transport operation of a host that attempts it fails, and each of
its retries (every 30–60 s) queues an ACCESS EXCLUSIVE request on the queue table, until they are
dropped. Drop them before the upgrade and recreate them
afterwards (an expression index over `payload_json::jsonb` would make every publish carrying a
`\u0000` fail again). With `AutoCreateSchema = false` the store refuses a table still on `jsonb`
with the migration to run:

```sql
ALTER TABLE public.asyncresponse_transport_messages
    ALTER COLUMN payload_json TYPE text USING payload_json::text,
    ALTER COLUMN headers_json DROP DEFAULT,
    ALTER COLUMN headers_json TYPE text USING headers_json::text,
    ALTER COLUMN headers_json SET DEFAULT '{}';
```

A NUL in a failure's exception message is replaced with U+FFFD in `dead_letter_reason`, since a
PostgreSQL `text` value cannot hold one at all.

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
| `No Reset On Close=true` | Avoids `DISCARD ALL` on every pooled check-in. AsyncResponse's `LISTEN` connections come from the same pool and run `UNLISTEN *` before they go back to it (on channel disposal, on every channel listen reconnect — behind a transaction-mode pooler that can be every 5–10 s — and on every transport subscriber restart), so pooled query connections do not need a reset for listener state. The teardown waits at most 1 second for it (the rest finishes in the background, so a half-open socket cannot stall disposal). A failed `UNLISTEN` on a connection still open after an established `LISTEN` is retried once with a 30-second timeout (an overloaded server that timed out the first usually answers the second); only if that fails too is the connection kept out of the pool by clearing the data source's pool — the application's own pool when it shares the data source, so every connection reconnects. The channel and the transport log each clear they make (a warning at most once a minute, Debug in between). |
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
  snapshot are allowed to expire even if cleanup deletion failed. A failed round is retried on a
  short backoff (at most one second, never later than the interval) rather than a full interval,
  so the rows come back promptly once the database answers again after an outage. The first
  failure of a run logs a warning, later ones at most one per heartbeat interval (the rest at
  Debug).
  A publish from the waiter's own process treats its live local subscription as live even while
  the row has lapsed, so it is delivered instead of routed to lost-subscriber recovery.
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
  half of `DeliveryConfirmationTimeout`, at most 2 seconds — and the late commit's own wake
  delivers it before its publisher's confirmation budget runs out. The whole window is revisited
  at most once per `ListenerPollInterval` per correlation id — or once per lookback, when the poll
  interval is longer: the passes in between (every notification and local publish signals one)
  read the last tick only and schedule a rescan of the id for when that throttle ends, so a late
  commit is still found while the window is open, whatever the poll interval, and a steady stream
  on one id no longer re-reads the window on every pass. A
  commit slower than that window from its stamp is left to history reconciliation (below) and
  can route to recovery. Rows a revisit reads again are not re-queued: processed ones are
  screened by the waiters' seen sets, and ones still waiting in the executor by the scan's own
  queued set.
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
  cursor back where the pass found it, and keeps the lookback window open while refusals last). Failed delivery claims request an immediate rewind.
  Acknowledged payload bodies are hydrated only when a waiter needs them.
- Delivery is serialized per correlation id on a bounded (1024-item) executor. The sweep admits
  work to it **without waiting**: when one correlation id's executor is full — a waiter wedged in
  a slow `Until` predicate under a progress flood — the rest of that id's messages stay unclaimed
  in the table, in order, and only that id is rescanned after one poll interval; every other
  correlation id keeps delivering. (Until round 35 the sweep awaited the capacity, so one
  saturated correlation stalled every waiter in the process.) The same-process fast path admits
  without waiting too: at capacity, or while the executor is being retired, the stored message is
  left to the sweep, which admits it in order — the publisher is never parked on the executor.
- The `FullSweepInterval` throttle applies only while a `LISTEN` is established and proven to
  deliver. Before the first one, and from any listen-connection failure until the next proven
  `LISTEN`, the sweep is the only cross-process wake and runs every `min(FullSweepInterval,
  DeliveryConfirmationTimeout / 4)` — 1.25 seconds on defaults, leaving most of the publisher's
  confirmation budget for the sweep itself, without holding up to 8 pooled connections on every
  tick while the database is already struggling. Each (re)established `LISTEN` triggers one
  immediate full sweep for the notifications published while none was up. A failure after a
  `LISTEN` that stayed up at least 5 seconds (the reconnect backoff's cap) starts the backoff
  over; one that is accepted and then dropped straight away keeps backing off toward that cap, so
  it cannot reconnect (and request a full sweep) every 100 ms.
  A successful `LISTEN` only counts once a delivery probe comes back: the listen connection sends
  itself a `NOTIFY` with a fresh payload and must receive it within 5 seconds (PostgreSQL delivers
  a listening session its own notifications). The same probe runs after 10 seconds without a
  notification, so a half-open socket (an idle connection silently dropped by a NAT or load
  balancer) — or a connection that stopped receiving — fails into the reconnect path instead of
  blocking forever. (The liveness check used to be a `SELECT 1`, which proves the socket, not
  delivery.)
- **The channel's `LISTEN` needs a session-pooled or direct connection.** Behind a transaction-
  or statement-mode pooler (PgBouncer `pool_mode = transaction`, and most serverless proxies) the
  `LISTEN` runs on a server connection that goes straight back to the pool, and no notification
  ever reaches the channel. The delivery probe then never comes back: the channel logs `PostgreSQL
  LISTEN loop failed` every reconnect cycle and runs on the sweep alone (the wake-down cadence
  above), so cross-process responses arrive up to 1.25 seconds late and more of them race the
  publisher's confirmation budget. Register the data source the channel uses against the server
  directly or through a session-mode pool; queries elsewhere in the application can keep using a
  transaction-mode pooler. A pool that happens to hand the probe back to the same server
  connection can pass it by luck, so do not rely on the probe to detect the misconfiguration.
- A full sweep (every subscribed correlation id) dispatches up to 8 correlation ids at a time,
  and one id's failure no longer stops the others. When the first 8 ids of a pass to query the
  store all fail with a transient fault (the database is down, not one row poisoned) the rest of
  the pass is skipped, and the logged exception carries the first 3 failures and counts the
  others; an id whose waiters are all mid-cleanup queries nothing and does not count. A
  requested sweep the breaker cuts short (the one an established `LISTEN` asks for, or one a
  signal overflow or a cut-short targeted pass asked for) is retried after `min(FullSweepInterval,
  DeliveryConfirmationTimeout / 4)` — never sooner, and again at that cadence while it keeps
  tripping — rather than left to the regular throttle. While the throttle is longer than that
  floor, an id whose own pass (a notification's, a publish's) failed transiently without
  tripping the breaker is rescanned on its own after the same floor, instead of waiting up to
  `FullSweepInterval` — the whole confirmation budget on defaults — for the next sweep. Each
  sweep that visited a waiter without any failure has its duration recorded on the
  `asyncresponse.channel.sweep.duration` histogram (tag `asyncresponse.channel`), and such a
  sweep longer than half of `DeliveryConfirmationTimeout` logs a warning (at most once a minute):
  past that point, responses only the sweep delivers can be claimed for lost-subscriber recovery
  under live waiters. A sweep with failures is neither recorded nor warned about — it measured
  connect timeouts, not the sweep.
- Keep `DeliveryConfirmationTimeout` long enough for the slowest expected live delivery, but short
  enough that a truly lost subscriber routes to recovery promptly. A transient fault in the
  confirmation poll reads as "not yet delivered" rather than failing the publish, and the recovery
  claim at its deadline is retried on the publish retry policy — the response row is already
  stored, and a failed publish would be re-published under a new message id.
- Set `DeadLetterRetention` if operators do not inspect dead-letter rows indefinitely.
- Monitor PostgreSQL table size, dead-letter count, connection usage, and lock waits from your
  database tooling. AsyncResponse reports library metrics, not database-native queue depth.
