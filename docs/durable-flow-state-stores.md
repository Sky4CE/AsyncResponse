# Durable-flow state stores

[← Back to the documentation index](README.md)

Durable flows keep a small JSON ledger for each run. That ledger is the source of truth for
completed steps, pending waits, run status, optimistic revision, and execution ownership. Choosing
the store is therefore a correctness decision, not just a persistence detail.

**On this page**

- [Choose a store](#choose-a-store)
- [The safety model](#the-safety-model-is-mandatory)
- [Registration and client lifetimes](#registration-and-client-lifetimes)
- [Provider examples](#provider-examples)
- [Schema ownership and fail-fast behavior](#schema-ownership-and-fail-fast-behavior)
- [Expiry and cleanup](#expiry-and-cleanup)
- [Custom-store checklist](#custom-store-checklist)

`AddAsyncResponse()` does **not** select storage implicitly. Every application chooses exactly one
store; startup fails fast when the choice is missing. The store callback also owns the common flow
settings, so engine and provider configuration stay together:

```csharp
var connectionString = builder.Configuration.GetConnectionString("SqlServer")
    ?? throw new InvalidOperationException("ConnectionStrings:SqlServer is required.");

builder.Services.AddAsyncResponse()
    .WithSqlServerChannel(options =>
        options.ConnectionString = connectionString)
    .WithSqlServerTransport(options =>
        options.ConnectionString = connectionString)
    .WithSqlServerDurableFlows(options =>
    {
        options.StateExpiry = TimeSpan.FromDays(14);
        options.ExecutionLeaseDuration = TimeSpan.FromMinutes(1);
        options.ExecutionLeaseRenewInterval = TimeSpan.FromSeconds(20);
        options.ConnectionString = connectionString;
        options.SchemaName = "dbo";
        options.TableName = "asyncresponse_flow_state";
    });
```

For tests, applications not yet starting flows, and deliberately one-process flows, select the
process-local store:

```csharp
builder.Services
    .AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithInMemoryDurableFlows();
```

The flow API is covered in [durable-flows.md](durable-flows.md). Package option defaults are in
[configuration.md](configuration.md#durable-flow-state-store-package-options). Channel and transport
registrations are in [provider-examples.md](provider-examples.md).

## Choose a store

| Provider | NuGet package | Registration | Clock authority | Best fit |
|---|---|---|---|---|
| In-memory | `AsyncResponse.Core` | `WithInMemoryDurableFlows()` | App (one process) | Tests, development, one process; state is lost on restart |
| SQL Server | `AsyncResponse.DurableFlows.SqlServer` | `WithSqlServerDurableFlows(...)` | Database | Existing SQL Server applications |
| PostgreSQL | `AsyncResponse.DurableFlows.PostgreSQL` | `WithPostgreSqlDurableFlows(...)` | Database | Existing PostgreSQL applications |
| MySQL / MariaDB | `AsyncResponse.DurableFlows.MySql` | `WithMySqlDurableFlows(...)` | Database | Existing MySQL or MariaDB applications |
| SQLite | `AsyncResponse.DurableFlows.Sqlite` | `WithSqliteDurableFlows(...)` | App (single node) | One-node services that need restart durability |
| Oracle | `AsyncResponse.DurableFlows.Oracle` | `WithOracleDurableFlows(...)` | Database | Existing Oracle applications |
| MongoDB | `AsyncResponse.DurableFlows.MongoDB` | `WithMongoDbDurableFlows(...)` | Database | Document-store applications; native TTL cleanup |
| Azure Cosmos DB | `AsyncResponse.DurableFlows.Cosmos` | `WithCosmosDurableFlows(...)` | App — sync worker clocks | Cosmos-native applications; per-item TTL |
| DynamoDB | `AsyncResponse.DurableFlows.DynamoDB` | `WithDynamoDbDurableFlows(...)` | App — sync worker clocks | AWS-native applications; conditional writes and native TTL |
| Entity Framework Core | `AsyncResponse.DurableFlows.EFCore` | `WithEFCoreDurableFlows<TDbContext>()` | App — sync worker clocks | Put the ledger in an existing relational `DbContext` and migration pipeline |
| Application-owned | `AsyncResponse.Core` | `WithDurableFlows<TStore>()` | Your choice | A storage system not covered above |

Prefer the database your application already operates. A separate workflow database is not
required, and the flow store is independent of the response channel and worker transport.

**Clock authority** is who evaluates lease and expiry comparisons. *Database* stores run that math
on the database server's clock, so worker clock skew cannot fence two nodes onto the same lease.
(For MongoDB this includes flow creation, which reads the server clock via the `hello` command —
the store's effective server floor is therefore mongod 4.2.10 / 4.4.2 or newer, matching the
`$$NOW` requirement of 4.2+.)
*App* stores (Cosmos, DynamoDB, EFCore) compare against `DateTime.UtcNow` on the worker because
their storage APIs offer no usable server-clock expression — a deliberate, documented trade-off in
each store's source. Multi-node deployments on an app-clock store must keep worker clocks
NTP-synchronized well inside `ExecutionLeaseDuration`; if you cannot guarantee that, prefer a
database-clock store. SQLite and in-memory are single-node by nature, so the app clock is exact
there.

## The safety model is mandatory

There is one `IFlowStateStore` contract. Every implementation must provide the atomic operations
below; the three members with a default body are optional to override but part of the contract:

```csharp
public interface IFlowStateStore
{
    // Optional (default: no-op). Deterministic, I/O-free checks run before a start is published.
    void ValidateCreate(string flowId, FlowState state, TimeSpan ttl) { }

    Task<bool> TryCreateAsync(
        string flowId, FlowState state, TimeSpan ttl,
        CancellationToken cancellationToken = default);

    Task<FlowState?> LoadAsync(
        string flowId,
        CancellationToken cancellationToken = default);

    // Optional (default: LoadAsync). See invariant 7.
    Task<FlowState?> LoadCurrentAsync(
        string flowId,
        CancellationToken cancellationToken = default);

    Task<bool> TryUpdateAsync(
        string flowId, FlowState state, long expectedRevision, TimeSpan ttl,
        string? leaseId = null,
        CancellationToken cancellationToken = default);

    Task<bool> TryAcquireLeaseAsync(
        string flowId, string leaseId, TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    Task<bool> TryRenewLeaseAsync(
        string flowId, string leaseId, TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    Task ReleaseLeaseAsync(
        string flowId, string leaseId,
        CancellationToken cancellationToken = default);

    // Optional (default: null, "this store cannot report leases"). See invariant 6.
    Task<FlowLeaseObservation?> ObserveLeaseAsync(
        string flowId,
        CancellationToken cancellationToken = default);

    Task<bool> TryDeleteAsync(
        string flowId,
        CancellationToken cancellationToken = default);
}
```

The required invariants are:

1. `TryCreateAsync` is insert-if-absent. An expired record may be replaced, but two callers can
   never both create the same live `flowId`. New ledgers start at revision `0`.
2. `TryUpdateAsync` is compare-and-swap. It succeeds only when the stored revision equals
   `expectedRevision`, and the new state revision is exactly `expectedRevision + 1`.
3. When an update supplies `leaseId`, the same unexpired lease must still own the ledger. A stale
   executor therefore cannot checkpoint after another replica takes over.
4. Acquire succeeds only for an unowned or expired lease. Renew succeeds only for the current,
   unexpired owner. Release never clears another owner's lease.
5. Loads treat expired records as absent (`null`). A record that is present but cannot be trusted
   — malformed JSON, an unrecognized schema version, a revision inside the JSON that disagrees
   with the stored one, a flow id inside the JSON that is not the key — is **not** absent: it
   throws `FlowStateUnreadableException`, because callers acknowledge a wake-up on `null` and an
   acknowledged wake-up strands the run that is still in the table. For the same reason `null`
   must be authoritative: a store whose plain reads can miss a present record confirms the
   absence before reporting it, and throws when it cannot.
6. `ObserveLeaseAsync` reports the lease **as persisted** — owner and absolute UTC expiry — without
   judging whether it has lapsed: an expired lease nobody has re-acquired is still reported, a
   ledger nobody holds (or an absent one) is `FlowLeaseObservation.Unheld`, and `null` means only
   "this store cannot report leases". Every built-in store implements it. See
   [Lease contention and deployments that change the lease duration](#lease-contention-and-deployments-that-change-the-lease-duration).
7. `LoadCurrentAsync` reflects every write the store acknowledged before the call, including
   another process's. The engine uses it wherever a load's answer lets it acknowledge a delivery
   **without writing** — where no revision or lease fence would correct a stale read: a recovered
   response that matched no pending step or found the run finished, a failure signal that did not
   fail the run, a wake-up or resume of a run that does not read `Running`, the read-back after a
   start's create found an existing ledger, a re-attaching step checking whether recovery already
   completed it, settling whether a cancelled checkpoint committed, **every** read a parent makes
   of the child flow it awaits (it memoizes that outcome under fences that cover only itself), and
   a parked child's ancestor walk before it stops. Finished statuses need it too: after a finished
   run's ledger is deleted and its id reused, an older copy shows the *previous* run. The default
   (`LoadAsync`) is right for a backend whose reads cannot return an older copy of a present
   record; a store that cannot deliver the guarantee on some backend configuration should refuse
   that configuration at provisioning, as the Cosmos DB store does.

Decorators around a store must forward `LoadCurrentAsync` and `ObserveLeaseAsync` — the interface
defaults silently downgrade the inner store. There is no weaker compatibility path and no
process-local fallback for an incomplete custom store, so single-node tests and multi-replica
production share one correctness model. The in-memory store satisfies the same contract inside one
process; it cannot make state survive or coordinate a different process.

Fencing prevents two healthy workers from checkpointing one run concurrently. It does not make an
external side effect and the following checkpoint one transaction; steps and triggers must still be
idempotent.

### Initial-state preflight

`IDurableFlows.StartAsync` calls `ValidateCreate` before publishing the start job. Every bundled
store checks the same deterministic state and size constraints as its create path without
contacting the database (Cosmos DB measures the complete escaped document), so an oversized initial
ledger fails the start with `FlowStateTooLargeException` and nothing enqueued. The preflight is not
a reservation: transient faults belong in the real write, the published job still creates the
ledger if the starter crashes or its own write fails transiently, and `TryCreateAsync` must enforce
the same validation for callers that bypass the starter. A deterministic size or argument error
from the starter's post-publish write is propagated even when a custom store did not preflight it.

### Lease contention and deployments that change the lease duration

A wake-up that finds the execution lease held cannot tell, from the failed acquire alone, whether
the holder is executing or died inside its unexpired lease window. Acknowledging it in the second
case drops the run's only wake-up, so the engine acknowledges a contended wake-up as a duplicate
**only on evidence from the store**, never because its own lease window elapsed:

- it records the first `ObserveLeaseAsync` result and keeps polling, every
  `ExecutionLeaseRenewInterval` or 2 seconds, whichever is shorter;
- a later observation with a **different owner**, or the **same owner and a later expiry**, can
  only have been written by a worker that acquired or renewed the lease meanwhile — a live holder.
  When that holder is driven by a *different* job, its own unacknowledged job covers the run, and
  the wake-up is acknowledged, typically within one renewal interval. When the lease records
  **this delivery's own job** (a broker in-flight ceiling lapsed under the running handler), the
  delivery is the last copy of the wake-up: it is re-published delayed past the holder's lease on
  a transport with delayed delivery, and otherwise keeps waiting and ends in
  `DurableFlowLeaseContendedException` — see
  [what happens when things die](durable-flows.md#what-happens-when-things-die). A lease or job
  that carries no job identity cannot be told apart and is acknowledged as a duplicate;
- an observation that **never changes** is a dead holder's lease. The wake-up waits for the
  *persisted* expiry, then acquires the lease and executes from the last checkpoint;
- if neither happens within one local lease window past the persisted expiry (a store clock far
  from this host's, or a store that reports a lease it will not hand over), the wake-up fails with
  `DurableFlowLeaseContendedException` and the worker transport redelivers it;
- the persisted expiry can extend the wait at most `DurableFlowOptions.MaxLeaseContentionWait`
  (default 1 hour) past the moment the wake-up started waiting, so a store clock far ahead of this
  host (or a shifted expiry column) cannot park the delivery and its worker slot indefinitely.
  Past the budget the wake-up fails the same way and is redelivered. This host's own lease window
  (`ExecutionLeaseDuration + ExecutionLeaseRenewInterval`) is always waited, whatever the budget.

This is what makes **changing `ExecutionLeaseDuration` between deployments safe**: the wait is
bounded by the lease the previous deployment actually wrote, not by the new configuration, so a
successor configured with a 30-second lease that meets a crashed owner's 10-minute lease waits out
the 10 minutes. A deployment that issues leases longer than `MaxLeaseContentionWait` should raise
that budget in step; otherwise a wake-up behind such a lease is handed back at the budget and
spends delivery attempts until the lease lapses.

Two operational consequences:

- the handler of a contended wake-up can stay parked for as long as the longest lease still
  persisted, up to `MaxLeaseContentionWait`. Transports redeliver a job whose visibility or lock
  lapses meanwhile; the extra delivery waits the same way and the first one to acquire the lease
  wins;
- an application-owned store that leaves `ObserveLeaseAsync` at its default gives the engine no
  evidence. It still takes over a dead holder's lease inside its own lease window, but a wake-up
  that stays contended through that window is **never acknowledged**: it throws
  `DurableFlowLeaseContendedException`, so duplicates of a long-running execution burn transport
  delivery attempts and can dead-letter. Implement the method — it is one read of two columns.

## Registration and client lifetimes

Built-in provider packages register their store as a singleton. Provisioning metadata is cached
once per process, and expensive control-plane calls are not repeated for every flow execution.

Provider packages reuse an application-registered client when present, including
`NpgsqlDataSource`, `IMongoDatabase`/`IMongoClient`, `CosmosClient`, and `IAmazonDynamoDB`. Otherwise
they create and own a client from the configured options. They do not expose that internally
created client as an unrelated bare DI service.

`WithDurableFlows<TStore>()` uses a scoped default for an application-owned store, so it can depend
on a scoped unit of work (such as an EF Core `DbContext`). To use another lifetime or a factory,
register `TStore` **before** the call: the extension forwards `IFlowStateStore` to your
registration and mirrors its lifetime as seen at that point in the chain. A registration added or
re-lifetimed afterwards leaves the forward stale (worst case a scoped forward to a singleton store,
which the first execution scope disposes); startup compares the two and fails fast with the fix.

Register exactly one durable-flow store. Startup validation rejects both missing and multiple store
selections.

## Provider examples

Every provider has a complete registration example below. The examples use an in-memory channel and
transport so the state-store choice is easy to see; in production, replace those two calls with the
channel and transport that fit your deployment. The flow store is an independent third choice.

### In-memory

No additional package is required. This store implements the full atomic contract, but only inside
one process and only for that process's lifetime.

```csharp
builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithInMemoryDurableFlows();
```

### SQL Server

```csharp
var connectionString = builder.Configuration.GetConnectionString("SqlServer")
    ?? throw new InvalidOperationException("ConnectionStrings:SqlServer is required.");

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithSqlServerDurableFlows(options =>
    {
        options.ConnectionString = connectionString;
        options.SchemaName = "dbo";
        options.TableName = "asyncresponse_flow_state";
        options.AutoCreateSchema = true; // set false after deploying your own migration
    });
```

The connection string must name an existing database. Automatic provisioning creates the schema,
table, and expiry index, not the database itself.

Every statement whose row count the store decides on (create, checkpoint, lease acquire and
renewal, delete, prune) sets `SET NOCOUNT OFF` for itself, so a server whose sessions start with
NOCOUNT on (`sp_configure 'user options', 512`) works unchanged.

### PostgreSQL

The store can build its own data source from `options.ConnectionString`. Reusing one application-wide
`NpgsqlDataSource` also lets the PostgreSQL channel, transport, and flow store share the same pool:

```csharp
using Npgsql;

var connectionString = builder.Configuration.GetConnectionString("PostgreSQL")
    ?? throw new InvalidOperationException("ConnectionStrings:PostgreSQL is required.");

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithPostgreSqlDurableFlows(options =>
    {
        options.SchemaName = "public";
        options.TableName = "asyncresponse_flow_state";
        options.AutoCreateSchema = true; // set false after deploying your own migration
    });
```

Without the shared data source, set `options.ConnectionString = connectionString` in
`WithPostgreSqlDurableFlows(...)` instead.

### MySQL or MariaDB

```csharp
var connectionString = builder.Configuration.GetConnectionString("MySql")
    ?? throw new InvalidOperationException("ConnectionStrings:MySql is required.");

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithMySqlDurableFlows(options =>
    {
        options.ConnectionString = connectionString;
        options.TableName = "asyncresponse_flow_state";
        options.AutoCreateSchema = true;
    });
```

With `AutoCreateSchema = false` the table is yours to provision. Two properties of it are
load-bearing, and the store verifies both at startup rather than letting them fail silently later:

```sql
CREATE TABLE asyncresponse_flow_state (
    -- Binary collation: the default folds case, which makes two flow ids the library treats as
    -- distinct collide on the key.
    flow_id varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
    -- utf8mb4 here too: the ledger JSON embeds the same arbitrary text as flow ids, and a
    -- narrower inherited charset truncates or rejects non-Latin state. The store verifies this.
    state_json longtext CHARACTER SET utf8mb4 NOT NULL,
    expires_at_utc datetime(6) NOT NULL,
    updated_at_utc datetime(6) NOT NULL,
    revision bigint NOT NULL DEFAULT 0,
    lease_id varchar(64) NULL,
    lease_expires_at_utc datetime(6) NULL,
    INDEX asyncresponse_flow_state_expires_idx (expires_at_utc)
);
```

The **primary key on the whole of `flow_id`** is the one to keep if you change anything: starting a
flow is an insert-if-absent, and the store learns that a ledger already exists from MySQL's
duplicate-key error. Without that key nothing reports the duplicate, so two concurrent starts of the
same flow id both succeed and the flow runs twice. Any single-column unique index does the job; a
composite one does not, and neither does a **prefix** key (`UNIQUE (flow_id(100))`) — a common way
to fit an index under MySQL's key-length limit, but it constrains only the first *n* characters, so
two distinct ids sharing that prefix collide and the second flow never starts. Startup verification
refuses all three, along with columns too narrow or too coarse to hold what the store writes. A
duplicate-key error (`1062`) is confirmed as "this flow id exists" on the connection the create
already holds, so concurrent identical starts never need a second pooled connection.

The connection string must not set `UseAffectedRows=true`: it turns MySqlConnector's row counts
from rows *matched* into rows *changed*, and an UPDATE that rewrites identical values (a lease
renewal landing in the same microsecond) would read as a lost lease. The store refuses it from the
string alone, **when the host starts** — not on the first operation, by which time `StartAsync` has
already published and returned an id for a run that can never execute.

**Ledger size.** A MySQL write is limited not by `longtext` (4 GB) but by the server's
`max_allowed_packet` — 4 MB on MySQL 5.7, 16 MB on MariaDB, 64 MB on MySQL 8.0 — which the whole
statement must fit, with the ledger's quotes and backslashes escaped (doubled). The store reads
`@@max_allowed_packet` at host start (or, if the server does not answer then, when it first verifies
its table) and from then on refuses a ledger whose escaped size exceeds it, less 64 KiB, with
`FlowStateTooLargeException` — in addition to `MaxStateBytes` when you set one — instead of the
server's opaque "packet bigger than 'max_allowed_packet'" error, which the executor would retry
into the dead-letter queue. Raise the server variable, or keep large payloads out of flow state, if
a ledger outgrows it.

### SQLite

> The store sets `PRAGMA journal_mode=WAL` when it auto-creates the schema: concurrent flow
> executors on one node are exactly the workload WAL exists for (readers never block behind a
> writer). The mode persists in the database file. If you provision the database yourself
> (`AutoCreateSchema = false`) — or host the EF Core store on SQLite — run the pragma once when
> creating the file.

SQLite is a useful one-node durable option: the ledger survives restarts without a separate server,
but the file remains local to one host.

```csharp
builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithSqliteDurableFlows(options =>
    {
        options.ConnectionString = "Data Source=asyncresponse-flow-state.db";
        options.TableName = "asyncresponse_flow_state";
    });
```

With `AutoCreateSchema = false` the table is yours to provision. Two properties of it are
load-bearing, and the store verifies both — by SQLite **affinity**, not exact spelling, so any
declaration that behaves like this passes — rather than letting them fail silently later:

```sql
CREATE TABLE asyncresponse_flow_state (
    flow_id TEXT NOT NULL PRIMARY KEY,
    state_json TEXT NOT NULL,
    expires_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    lease_id TEXT NULL,
    lease_expires_at_utc TEXT NULL
);
CREATE INDEX asyncresponse_flow_state_expires_idx ON asyncresponse_flow_state (expires_at_utc);
```

`flow_id` must also keep SQLite's default **BINARY** collation. `COLLATE NOCASE` (on the column or
the primary key) folds `Order-A1` and `order-a1` onto one row, so two distinct runs share a ledger
and each overwrites the other's checkpoints. `PRAGMA table_info` does not report a collation, so the
store parses the stored `CREATE TABLE` text and refuses a folding one at startup.

The **primary key on `flow_id` alone** is the one to keep if you change anything: starting a flow
targets `ON CONFLICT(flow_id)`, which needs a uniqueness constraint on exactly that column.
**`TEXT` affinity on `expires_at_utc` and `lease_expires_at_utc`** matters just as much: expiry and
lease fencing compare the stored ISO-8601 strings lexicographically, and a numeric affinity
silently coerces digit-only values and breaks that ordering.

Verification runs the first time the store opens a connection. An absent table is assumed not yet
migrated and re-checked on the next operation; a present table with the wrong shape — a missing
column, a mismatched affinity or nullability, no single-column primary key, or an extra `NOT NULL`
column with no default — throws with the fix.

### Oracle

```csharp
var connectionString = builder.Configuration.GetConnectionString("Oracle")
    ?? throw new InvalidOperationException("ConnectionStrings:Oracle is required.");

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithOracleDurableFlows(options =>
    {
        options.ConnectionString = connectionString;
        options.TableName = "ASYNCRESPONSE_FLOW_STATE";
        options.AutoCreateSchema = true;
    });
```

Oracle 12.1 and earlier have a 30-character identifier limit; if the generated expiry-index name
would exceed it, shorten `TableName`. Startup also rejects a `TableName` that collides with its own
derived index name (a 128-character name already ending `_EXPIRES_IDX`), since tables and indexes
share one namespace and the collision would silently leave the expiry index uncreated.

Give the store its own connection string, one no other component runs `ALTER SESSION` on. The
startup check that refuses linguistic comparison (`NLS_COMP=LINGUISTIC` with a folding `NLS_SORT`)
reads one pooled session, and ODP.NET returns pooled sessions with their altered NLS state intact,
so a component sharing the pool could later lend the store a session whose `flow_id =` predicates
fold case. A distinct connection string gets its own pool.

### MongoDB

```csharp
var connectionString = builder.Configuration.GetConnectionString("MongoDB")
    ?? throw new InvalidOperationException("ConnectionStrings:MongoDB is required.");

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithMongoDbDurableFlows(options =>
    {
        options.ConnectionString = connectionString;
        options.DatabaseName = "orders";
        options.CollectionName = "asyncresponse_flow_state";
        options.AutoCreateIndexes = true;
    });
```

If the application already registers `IMongoDatabase`, the store reuses it and only
`CollectionName` is needed; with a registered `IMongoClient`, configure `DatabaseName`. By default
the store also claims its collection in the reserved `asyncresponse_ownership` collection, so
another AsyncResponse component misconfigured onto the same collection fails startup
(`UseOwnershipLedger`, see [configuration.md](configuration.md#durable-flow-state-store-package-options)).

**Read and write concerns are pinned, whatever the registered connection says:**

- Ledger reads go to the **primary**. A `readPreference=secondaryPreferred` connection would
  otherwise read a lagging secondary, where a stale revision replays a checkpointed step and a
  not-yet-replicated ledger reads as absent — the one answer that acknowledges a wake-up.
- Ledger writes (creates, checkpoints, lease acquire/renew/release, deletes) use `w: "majority"`.
  Under an inherited `w=1` a failover can roll back the lease a worker executes under or the step
  result it just recorded. The majority write is **bounded**: an inherited `wtimeoutMS` (and
  `journal`) is kept, otherwise the store applies a 10 s `wtimeout`, so a primary-secondary-arbiter
  set with its secondary down fails writes instead of blocking them. A lapsed `wtimeout` fails the
  write as retriable even though the primary applied it; the revision and lease fences make the
  retry safe. Restore the secondary (or remove the arbiter) rather than lowering the write concern.
- Index creation (`AutoCreateIndexes = true`) uses `w: 1`: under `w: "majority"` even a
  `createIndexes` with nothing to build waits for majority acknowledgement, so a host started while
  the set could not reach a majority failed every operation — reads included — until it recovered.
  Index DDL is idempotent; a build a failover rolled back reruns at the next start.
- Ordinary loads keep the registered read concern — primary reads see every write the store
  acknowledged, and the fences reject whatever a stale read would decide. `LoadCurrentAsync`
  (invariant 7 above) reads with **`linearizable`** read concern instead, because a primary deposed
  by a partition can still serve reads until it notices, and only a linearizable read refuses
  there. A plain `LoadAsync` that finds **no** live ledger repeats the read the same way before
  answering "absent"; a load that finds its ledger costs nothing extra.
- Linearizable reads are bounded by `maxTimeMS` (10 s): on a degraded set they fail rather than
  block, and the delivery is retried (`IDurableFlows.ResumeAsync` surfaces the failure to its
  caller; a re-attach check falls through to the normal wait). A standalone server, or a
  Mongo-compatible service that refuses the `linearizable` level itself (Amazon DocumentDB), gets
  the plain read from the first refusal on; a timeout, step-down, or recovering node is never
  mistaken for such a refusal.

The MongoDB channel and transport apply their own bounded write concerns — see
[transport semantics](transport-semantics.md).

The collection must keep the default **simple** collation. A collection created with a default
collation builds its `_id` index (the flow id) under it, so case- or accent-variant ids would
collide. The store checks the `_id` index at first use — on either `AutoCreateIndexes` setting;
with `true`, only when its credentials may list indexes — and refuses a folding collation. The
`_id` index cannot be rebuilt: recreate the collection without a collation and copy the documents.

The ledger's instants are always written as BSON dates, whatever `DateTime` serializer the host
registered globally: expiry and lease filters compare them with `$$NOW`, and the TTL monitor reaps
only dates. A document whose `expires_at_utc` is missing or not a date is refused as unreadable
(`FlowStateUnreadableException`), never read as absent or replaced by a create.

**Hosts that registered a non-date `DateTime` serializer globally** (for example
`BsonSerializer.RegisterSerializer(new DateTimeSerializer(BsonType.String))`, or a `Document` or
`Int64` representation) under older releases may hold ledgers whose `expires_at_utc` is not a
date. The TTL monitor never reaps them and no create replaces them, so every reuse of such an id —
a caller-chosen id or a scheduler occurrence — hits an unreadable ledger: `StartAsync` returns the
id with a warning, the start job fails with `FlowStateUnreadableException` until it is
dead-lettered, and `GetStateAsync` throws. Once no run needs them, delete them (add an `_id`
condition to clear one flow; the exception's reason carries the same command):

```javascript
db.getCollection("asyncresponse_flow_state").deleteMany({ expires_at_utc: { $not: { $type: "date" } } })
```

With `AutoCreateIndexes = false` the store verifies at startup that the provisioned collection
carries a TTL index on `expires_at_utc`
(`createIndex({ expires_at_utc: 1 }, { expireAfterSeconds: 0 })`) and fails with an actionable
error when it is missing — that index is the store's only cleanup mechanism.

### Azure Cosmos DB

```csharp
var connectionString = builder.Configuration.GetConnectionString("Cosmos")
    ?? throw new InvalidOperationException("ConnectionStrings:Cosmos is required.");

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithCosmosDurableFlows(options =>
    {
        options.ConnectionString = connectionString;
        options.DatabaseName = "orders";
        options.ContainerName = "asyncresponse_flow_state";
        options.PartitionKeyPath = "/flowId";
        options.AutoCreateContainer = true;
    });
```

An application-registered `CosmosClient` is reused automatically; omit `ConnectionString` in that
case. Existing containers must already use the configured partition key and have TTL enabled.

#### Account requirements

The store's first operation reads the account, before it creates or validates anything on it,
and **refuses to run** — every operation throws `InvalidOperationException` naming the cause,
until the configuration is fixed — unless both hold:

| Requirement | Why | How to meet it |
|---|---|---|
| Reads run at **Session** or **Strong** consistency: the `CosmosClient`'s own `ConsistencyLevel` override when it sets one, else the account default | Below Session a read carries no session token, so the read behind `LoadCurrentAsync` can still be served by a lagging replica. The engine acknowledges deliveries on that read without writing: a recovered response, a failure signal or a resume is then dropped against an older copy of the ledger. | Run the account at Session (the Cosmos DB default) or Strong. On a Bounded Staleness account set `CosmosClientOptions.ConsistencyLevel = ConsistencyLevel.Session`; a client can weaken the account's level, never strengthen it. |
| The account has **one write region** | An ETag-fenced write that succeeds in one region does not exclude the same write succeeding in another. Two workers can each acquire the same execution lease and run the same flow; the account resolves the conflict last-writer-wins after both have run. | Use an account with a single write region (read regions are fine). |

An account that cannot be read establishes neither requirement: the operation fails with the
account read's own exception and the next operation asks again. The same check runs **when the
host starts**, and a refusal there fails the start — which is where a deploy sees it, because
`IDurableFlows.StartAsync` tolerates store faults after its publish (in a process that never runs
the hosted services, a start on a refused account returns the id with a warning and the job fails
in the workers until it is dead-lettered). An account that cannot be read within ten seconds at
startup is logged at Warning and left to the first operation, so an unreachable Cosmos DB does not
block host startup.

`CosmosDurableFlowOptions.AllowUnsafeAccountConfiguration` (default `false`) turns the refusal into
a warning. It exists for the **Cosmos DB emulator**, whose account default is Eventual and cannot be
raised by the client, and for tests; with it set the store gives none of the guarantees above — do
not set it for a production account.

```csharp
.WithCosmosDurableFlows(options =>
{
    options.DatabaseName = "orders";

    // Local development against the emulator only.
    options.AllowUnsafeAccountConfiguration = builder.Environment.IsDevelopment();
});
```

#### Document size

`MaxStateBytes` (1 900 000 bytes by default) bounds the **document** Cosmos receives, not just the
ledger: the ledger JSON travels inside it as the `stateJson` string, so every quote and backslash
is escaped a second time (a 1.2 MB ledger of escaped characters is a 2.4 MB document, over the 2 MB
item cap). The ledger JSON alone is checked first as a cheap pre-check; the complete document is
then measured through the host's custom Cosmos serializer when one is configured, or — for the SDK
default — by writing the known document fields with Newtonsoft's `JsonTextWriter` (same escaping,
date formatting, and null handling, no reflection, so the path stays trim/AOT-safe). An oversized
document fails the write with `FlowStateTooLargeException` naming the document size.

#### Reads that must be current

Every ledger operation treats only a `404` with sub-status `0` as a possibly absent flow. Cosmos
also answers `404` while the ledger still exists — `1002` (`ReadSessionNotAvailable`: the replicas
in reach are behind the client's session token) and `1003`/`1004` (container or database
recreated) — and those surface as errors, so the wake-up is retried instead of acknowledged and an
update or lease call does not misread them as a lost lease.

A sub-status `0` is still only one replica's answer: Session consistency is read-your-writes for
the writing client only, so **another process** can read a replica that has not applied the write
yet (a plain `404`, or an older document). Cosmos cannot strengthen a single read, so the reads
that can acknowledge a delivery first go through the **write path**: a conditional
`PatchItemAsync` whose `If-Match` can never hold. The write region's primary answers `404` for "no
such ledger" and `412` for "exists", and the SDK records the `412`'s session token, so the read
that follows cannot be served by a replica behind it.

- `LoadAsync` probes only when its read says the ledger is absent or logically expired. If the
  write path keeps reporting the ledger present while reads keep answering `404`, the ledger is
  present but hidden by its server `ttl`, so the load reports "no state" (below Session
  consistency — reachable only with `AllowUnsafeAccountConfiguration` — nothing a read returns can
  prove absence, so it throws `FlowStateUnreadableException` instead).
- `ObserveLeaseAsync` probes before every observation — a stale baseline would make an old renewal
  look like proof of a live holder. That costs one bodiless request per contention poll.
- `LoadCurrentAsync` probes before every read, covering every case in
  [invariant 7](#the-safety-model-is-mandatory).

This holds at **Session** (made current by the recorded token) and **Strong** consistency. Weaker
levels send no session token on reads, and multiple write regions have no single authoritative
write path — which is why provisioning refuses both (see
[account requirements](#account-requirements)).

#### Other behavior

- Document instants are normalized to UTC as they are read, so a registered serializer with local
  time-zone handling (Newtonsoft `DateTimeZoneHandling.Local`) cannot shift expiry and lease
  decisions by the host's offset. A document without `expiresAtUtc` is refused as unreadable —
  never read as absent, replaced, checkpointed, or leased.
- Lease maintenance (acquire, the renewal heartbeat, release) never moves the ledger body: it reads
  a projection of the lease fields and `_etag` with a partition-scoped query (`EnableScanInQuery`
  set, so an `IndexingMode.None` container works too) and applies a conditional `PatchItemAsync` of
  `leaseId`, `leaseExpiresAtUtc`, and `ttl`, fenced by `IfMatchEtag`. Wire and CPU cost are
  O(lease fields); the request-unit charge still follows the service's accounting for the whole
  document, so measure RU on your own ledger sizes. A projection without `_etag` (a serializer that
  hides system properties) throws rather than reporting the lease free. Checkpoints
  (`TryUpdateAsync`) replace the document.
- A write between a lease call's read and its patch (usually the holder's own checkpoint) fails the
  patch with `412`, and the store retries after a short jittered pause. A renewal whose attempts
  all lose that race while the lease still reads held and live throws instead of answering
  `false`, so the engine retries it rather than abandoning a healthy execution; an acquire that
  keeps losing still answers `false`.

### DynamoDB

The package reuses a registered `IAmazonDynamoDB`; otherwise it uses the normal AWS SDK credential
and region chain.

```csharp
builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithDynamoDbDurableFlows(options =>
    {
        options.TableName = "AsyncResponseFlowState";
        options.AutoCreateTable = true;       // use infrastructure-as-code in production
        options.EnableTimeToLive = true;
        options.TimeToLiveAttributeName = "expires_at";
    });
```

Run every worker against **one Region's replica** of the table. With a global table in the default
multi-Region eventual consistency mode, conditional writes and `ConsistentRead` are evaluated
against the local Region's replica and concurrent writes to one item resolve last-writer-wins, so
workers in two Regions can both win the same create, lease acquire, or revision-checked
checkpoint — the run executes twice and one checkpoint silently replaces the other. This is the
same hole as a Cosmos DB account with multiple write regions.

### Entity Framework Core

The ledger becomes part of the application's own relational model and migration pipeline. The
store works with any EF Core relational provider.

```csharp
using AsyncResponse.DurableFlows.EFCore;
using Microsoft.EntityFrameworkCore;

public sealed class AppDbContext(DbContextOptions<AppDbContext> options)
    : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.ConfigureAsyncResponseDurableFlows(
            // Flow ids are compared ORDINALLY, so the key column must be case-sensitive. This
            // package runs no DDL and cannot know your provider — and the SQL Server and MySQL
            // defaults are case-INSENSITIVE, which folds "flow-a" and "FLOW-A" onto one key.
            // The bundled SQL Server/MySQL stores pin this in their own DDL; the PostgreSQL
            // store verifies the column's collation is deterministic.
            flowIdCollation: AsyncResponseFlowIdCollations.PostgreSql);
    }
}

var connectionString = builder.Configuration.GetConnectionString("PostgreSQL")
    ?? throw new InvalidOperationException("ConnectionStrings:PostgreSQL is required.");

builder.Services.AddDbContextFactory<AppDbContext>(options =>
    options.UseNpgsql(connectionString));

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithEFCoreDurableFlows<AppDbContext>();
```

The EF Core store prefers `IDbContextFactory<TContext>` when registered; otherwise it creates a
scope for `TContext`, and parallel flow executions never share a context. Reads are no-tracking,
and conditional updates and deletes execute in the database and decide from the affected-row count,
so a provider that reports none (SQL Server sessions with NOCOUNT on by default) fails with an
actionable `InvalidOperationException` instead of reading every write as lost. Every store query
calls `IgnoreQueryFilters()`: the ledger is keyed by flow id alone, and an application-wide filter
(a tenant filter on every entity type) would otherwise hide rows and make the worker's create
collide with a row it cannot see. The `revision` column keeps its `DEFAULT 0` in migrations, but
every insert names it, so a table provisioned without the default works too.

After adding `ConfigureAsyncResponseDurableFlows()`, generate and deploy a normal EF migration.
The package never creates or alters the schema itself.

Lazy-loading proxies (`UseLazyLoadingProxies()`) are fine. Change-tracking proxies
(`UseChangeTrackingProxies()`) require every mapped property to be `virtual`, which
`DurableFlowStateRecord`'s are not: give the ledger its own `DbContext` there.

**Set `flowIdCollation`.** The schema is yours, so the `flow_id` collation is too, and on SQL
Server and MySQL the database default is case-insensitive: two ids differing only in case become
one primary key, so the second `StartAsync` fails as a duplicate and a load returns the other run's
state. Pass the constant for your provider (`AsyncResponseFlowIdCollations.SqlServer` / `.MySql` /
`.PostgreSql` / `.Sqlite`). On SQL Server and MySQL the store **fails the host start** if the
mapping declares no collation or one that is not binary (`_BIN2` on SQL Server, `_bin` on MySQL) — a
merely case-sensitive one still folds accents (`_CS_AI`) or full-width forms (any collation without
`_WS`). A context that does not map the ledger at all fails the start the same way. Both checks
read only the model, so no database is contacted; a context that cannot be created while the host
starts (a factory that needs a request scope) is logged and left to the store's first operation.

### Application-owned store

Use the custom registration only when none of the provider packages fits. The store must implement
the complete contract [above](#the-safety-model-is-mandatory) — see the
[custom-store checklist](#custom-store-checklist) — and registration adds no weaker fallback.

```csharp
builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithDurableFlows<MyFlowStateStore>(options =>
    {
        options.StateExpiry = TimeSpan.FromDays(14);
        options.ExecutionLeaseDuration = TimeSpan.FromMinutes(1);
        options.ExecutionLeaseRenewInterval = TimeSpan.FromSeconds(20);
    });
```

`MyFlowStateStore` is registered as scoped by default. For another lifetime or a factory,
pre-register it before the chain (see [client lifetimes](#registration-and-client-lifetimes)):

```csharp
builder.Services.AddSingleton<MyFlowStateStore>();

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithDurableFlows<MyFlowStateStore>();
```

## Schema ownership and fail-fast behavior

Provider `AutoCreate...` options are convenient for local development. In production, prefer
migrations or infrastructure-as-code and disable automatic DDL where available.

The current relational shape is:

```text
flow_id              string primary key (case-sensitive/binary collation)
state_json           large text (PostgreSQL: text, NOT jsonb)
expires_at_utc       UTC timestamp
updated_at_utc       UTC timestamp
revision             64-bit integer, not null
lease_id             nullable string(64)
lease_expires_at_utc nullable UTC timestamp
index on expires_at_utc
```

Document stores persist the same fields. DynamoDB uses `flow_id` as the partition key, Unix seconds
for the TTL attribute, and Unix milliseconds for lease expiry.

On PostgreSQL `state_json` is `text`, not `jsonb`: `jsonb` rejects the `\u0000` escape
`System.Text.Json` emits for U+0000 (SQLSTATE 22P05), so such a ledger could never be written.
Nothing queries inside the ledger, so `text` costs nothing. With `AutoCreateSchema = true` an
existing `jsonb` column is converted in place on first use — an `ALTER TABLE` that rewrites the
table under an ACCESS EXCLUSIVE lock and blocks every flow operation on every host while it runs,
so on a large table run it by hand before the rollout. With your own migration, deploy
`ALTER TABLE ... ALTER COLUMN state_json TYPE text USING state_json::text`; the startup verifier
rejects a `jsonb` column with that instruction.

The PostgreSQL store's startup DDL shares the channel's and transport's bounds (see
[PostgreSQL › Schema creation](postgresql.md#schema-creation)): the expiry index is built only when
the catalog shows it absent; lock waits, the schema's advisory lock included, are bounded by a 5 s
`lock_timeout`; the conversion and the index build run in their own transaction under the table's
advisory lock with an hour-long command timeout; and a failed attempt is retried after a jittered
30–60 s window, during which that host's flow operations fail at once, naming the cause.

The library does not silently upgrade an incomplete concurrency schema:

- SQL packages create the complete table when missing. If a table exists without revision or lease
  columns, operations fail; deploy the correct migration before the application.
- With automatic DDL disabled, the SQL packages verify an existing table's shape on first use.
  String widths are minimums (SQL Server, MySQL, Oracle): a `flow_id nvarchar(450)` or
  `lease_id nvarchar(100)` passes, a narrower column does not; the binary `flow_id` collation and
  full-precision timestamps are required, a `revision` default is not. The expiry index is
  performance-only and never fails startup: PostgreSQL logs a warning when no index is named
  `{table}_expires_idx` (harmless if your migration indexed `expires_at_utc` under another name) or
  when it exists but is not valid and ready (a running or failed `CREATE INDEX CONCURRENTLY` — drop
  and recreate a failed one); the other packages do not check theirs.
- MongoDB creates the TTL index when missing and, with `AutoCreateIndexes = false`, verifies an
  equivalent one exists. It never drops or rewrites an application-owned index; an equivalent TTL
  index under another name (such as `expires_at_utc_1`) is accepted.
- Cosmos auto-create uses the configured partition key and enables per-item TTL on a new container.
  An existing container must already use that partition key and have TTL enabled, or first use fails.
- DynamoDB validates that TTL is enabled (or being enabled) on the configured attribute; a table
  with TTL on another attribute fails clearly. With `AutoCreateTable = false` the check runs
  regardless of `EnableTimeToLive`, which only governs whether auto-creation enables TTL — the store
  has no application-side pruning, so a table without TTL would grow without bound.
- EF Core never runs DDL.

Persisted state has two revision copies: the indexed/provider field and the value inside
`state_json`. A record where they disagree, or whose `state_json` names a different flow id than
its key, is refused as unreadable (`FlowStateUnreadableException`) — never reported as absent, since
"absent" acknowledges its wake-up. MongoDB, Cosmos DB, and DynamoDB records without a physical
revision are rejected the same way, so a malformed or partially migrated record never enters
execution with a fabricated revision.

## Expiry and cleanup

`StateExpiry` is an idle TTL (default 14 days): every successful checkpoint refreshes it, so it
limits the gap between checkpoints, not total flow duration — see
[durable-flows.md](durable-flows.md#storage-where-flow-state-lives) for how to size it. Loads always
filter expired state; physical cleanup is separate:

| Store | Cleanup |
|---|---|
| PostgreSQL, SQL Server, MySQL, SQLite, Oracle, EF Core | Opportunistic expired-row prune on flow creation, throttled by `PruneInterval` (default 5 minutes), draining 1000-row batches while they come back full for up to `PruneBudget` (default 2 seconds; zero = one batch). A failed prune (a deadlock victim, a lock timeout) is skipped until the next interval and never fails the `StartAsync` it rides on. Deleted rows, failures, and a lapsed budget with rows remaining are counted on the `AsyncResponse` meter and logged at Warning. The EF Core prune re-checks expiry on every row it deletes, so a ledger a concurrent create just replaced in place is never removed |
| MongoDB | TTL index on `expires_at_utc`; reads still filter because Mongo's TTL monitor is periodic |
| Cosmos DB | Container TTL plus a per-item `ttl` value |
| DynamoDB | Native TTL on `TimeToLiveAttributeName`; expiry is rounded up to avoid shortening the requested lifetime |
| In-memory | Expired entries are removed on access or replacement, and flow creation sweeps all expired entries at most once per minute of the engine clock |

Keep `StateExpiry` longer than the longest legitimate period without a checkpoint. Deleting a
ledger or allowing it to expire while a flow is suspended makes its outcome unknowable.

## Custom-store checklist

Before using a custom store in production, test all of these against the real backend:

- many concurrent creates for one id produce exactly one winner;
- stale revision updates return `false` and never overwrite newer JSON;
- an update carrying the wrong or expired lease returns `false`;
- a lease cannot be renewed or released by another owner;
- takeover works after lease expiry;
- `ObserveLeaseAsync` returns the persisted owner and expiry raw: `Unheld` before any acquire,
  after a release, and for a missing flow (never `null`); a strictly later expiry after every
  renewal; and the old owner, unchanged, for a lease that has expired but not been re-acquired;
- `LoadAsync` answers `null` only for a ledger that is authoritatively absent (or expired), because
  the engine acknowledges a wake-up — and a recovered response — on it. A backend whose plain reads
  can miss a present record (replica or session reads, a deposed primary) confirms the absence
  first and throws when it cannot, as the Cosmos DB (write-path probe) and MongoDB (`linearizable`
  re-read) stores do;
- `LoadCurrentAsync` meets [invariant 7](#the-safety-model-is-mandatory) — override the default when
  your backend's reads can return an older copy of a present record;
- decorators forward `LoadCurrentAsync` and `ObserveLeaseAsync`;
- TTL refresh and expired-record replacement are atomic;
- **unreadable is not missing:** malformed JSON, an unknown schema version, a revision inside the
  JSON that disagrees with the stored one, and a stored `flowId` that is not the key all throw
  `FlowStateUnreadableException`, never return `null` — otherwise a rolling deployment (an older
  replica reading a newer row) or a corrupt row acknowledges a live flow's only wake-up;
- the store stamps the TTL it is given: the engine already raises it to reach
  `FlowState.RetainUntilUtc` for non-terminal runs;
- `flow_id` compares **ordinally**: a case- or accent-insensitive collation folds distinct runs onto
  one row (the built-in stores verify the deployed collation at startup);
- `ValidateCreate` covers your deterministic limits (see [initial-state preflight](#initial-state-preflight));
- cancellation reaches network/database operations;
- transient failures do not fall back to unconditional writes.

Execution leases use absolute UTC expiry. Keep hosts time-synchronized and set
`ExecutionLeaseDuration` comfortably above clock skew, network jitter, and the renewal interval.

The built-in stores run the same atomic create/revision/lease contract suite against their real
providers in integration tests. Reusing one of them is the shortest path to a replica-safe store.
