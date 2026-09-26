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

There is one `IFlowStateStore` contract. Every implementation must provide all of these atomic
operations:

```csharp
public interface IFlowStateStore
{
    Task<bool> TryCreateAsync(
        string flowId, FlowState state, TimeSpan ttl,
        CancellationToken cancellationToken = default);

    Task<FlowState?> LoadAsync(
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

There is no weaker compatibility path and no process-local fallback for an incomplete custom
store. That keeps single-node tests and multi-replica production on the same correctness model.
The in-memory implementation satisfies the same atomic contract inside one process; it cannot make
state survive or coordinate a different process.

Durable-flow fencing prevents two healthy workers from checkpointing one run concurrently. It does
not make an external side effect and the following checkpoint one transaction. Steps and triggers
must still be idempotent.

### Lease contention and deployments that change the lease duration

A wake-up that finds the execution lease held cannot tell, from the failed acquire alone, whether
the holder is executing or died inside its unexpired lease window. Acknowledging it in the second
case drops the run's only wake-up, so the engine acknowledges a contended wake-up as a duplicate
**only on evidence from the store**, never because its own lease window elapsed:

- it records the first `ObserveLeaseAsync` result and keeps polling (every
  `ExecutionLeaseRenewInterval`, at most every 2 seconds);
- a later observation with a **different owner**, or the **same owner and a later expiry**, can
  only have been written by a worker that acquired or renewed the lease in the meantime — a live
  holder. When that holder is driven by a *different* job, its own unacknowledged job covers the
  run, and the wake-up is acknowledged, typically within one renewal interval of the holder rather
  than after a full lease window. When the lease records **this delivery's own job** (a broker
  in-flight ceiling lapsed under the running handler), the proof is no licence to acknowledge: the
  delivery is the last copy of the wake-up, so it is re-published delayed past the holder's lease
  on a transport with delayed delivery and otherwise keeps waiting, ending in
  `DurableFlowLeaseContendedException` — see
  [what happens when things die](durable-flows.md#what-happens-when-things-die). A lease or a job
  written before job identities were recorded cannot be told apart and is acknowledged as before;
- an observation that **never changes** is a dead holder's lease. The wake-up waits for the
  *persisted* expiry, then acquires the lease and executes from the last checkpoint;
- if neither happens within one local lease window past the persisted expiry (a store clock far
  from this host's, or a store that reports a lease it will not hand over), the wake-up fails with
  `DurableFlowLeaseContendedException` and the worker transport redelivers it;
- the persisted expiry moves the deadline at most `DurableFlowOptions.MaxLeaseContentionWait`
  (default 1 hour) past the moment the wake-up started waiting. The expiry is data the waiting host
  does not control: without a ceiling, a store clock hours ahead of this host — or an expiry column
  read back shifted — parked the delivery for as long as the bad value said, polling the store
  every two seconds and holding its worker slot, and never reached the exception that names clock
  skew as the cause. Past the budget the wake-up fails the same way and is redelivered. This
  host's own lease window is always waited, whatever the budget is set to.

This is what makes **changing `ExecutionLeaseDuration` between deployments safe**. The wait is
bounded by the lease the previous deployment actually wrote, not by the new deployment's
configuration: a successor configured with a 30-second lease that meets a crashed owner's
10-minute lease waits out the 10 minutes. A deployment that issues leases longer than
`MaxLeaseContentionWait` should raise that budget in step — otherwise a wake-up behind such a
lease is handed back to the transport at the budget and spends delivery attempts until the lease
lapses. (Earlier versions waited only
`ExecutionLeaseDuration + ExecutionLeaseRenewInterval` of the *waiting* host and then acknowledged
the wake-up as a duplicate, so shortening the lease could strand every run whose redelivery
arrived before the old lease expired — `Running`, `Attempts` unchanged, nothing left to wake it.)

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
on a scoped unit of work. You may pre-register `TStore` with another lifetime; the extension keeps
that registration and forwards `IFlowStateStore` to it.

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

Every statement whose row count the store decides on — create, checkpoint, lease acquire and
renewal, delete, prune — turns `SET NOCOUNT OFF` on for itself, so a server whose sessions start
with NOCOUNT on (`sp_configure 'user options', 512`) works unchanged. Without it every count read
as -1: creates reported "exists", checkpoints a lost revision race, and acquires a contended lease.

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

A duplicate-key failure on create (`1062`) is confirmed as "this flow id exists" on the connection
the create already holds, so an idempotent re-start never needs a second pooled connection — a
pool of one serves it, and concurrent identical starts cannot starve the pool waiting on each
other.

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
refuses all three, along with columns too narrow or too coarse to hold what the store writes.

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
targets `ON CONFLICT(flow_id)`, which needs a uniqueness constraint on exactly that column — a
composite key constrains a different tuple, so the upsert fails at the first flow instead of at
startup. **`TEXT` affinity on `expires_at_utc` and `lease_expires_at_utc`** matters just as much:
expiry and lease fencing compare the stored ISO-8601 strings lexicographically, and a numeric
affinity silently coerces digit-only values and breaks that ordering. (Every declared column's
affinity and nullability is verified, not just these two — they are the ones a subtly wrong type
breaks silently instead of loudly.)

Verification runs the first time the store opens a connection: an absent table is assumed not yet
migrated and is re-checked on the next operation rather than failing startup, while a present table
with the wrong shape — a missing column, a mismatched affinity or nullability, no single-column
primary key, or an extra `NOT NULL` column with no default — throws with the fix instead of failing
silently at the first flow.

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

Oracle 12.1 and earlier have a 30-character identifier limit. If the generated expiry-index name
would exceed it, shorten `TableName`. Startup also rejects a `TableName` that collides with its own
derived index name — a 128-character name already ending `_EXPIRES_IDX`, which the truncated index
name reproduces exactly — since Oracle shares one namespace for
tables and indexes and the index create would otherwise fail with an error indistinguishable from a
benign already-exists race, silently leaving the expiry index never created.

Give the store its own connection string, one no other component runs `ALTER SESSION` on. The
startup check that refuses linguistic comparison (`NLS_COMP=LINGUISTIC` with a folding `NLS_SORT`)
reads one pooled session, which stands for every session the instance, client configuration and
logon triggers set up — but ODP.NET returns pooled sessions with their altered NLS state intact,
so a component sharing the connection string (and therefore the pool) that alters the sessions it
opens can later lend the store a session whose `flow_id =` predicates fold case. Any distinct
connection string gets its own pool.

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
`CollectionName` is needed. With a registered `IMongoClient`, configure `DatabaseName`. Ledger
reads are pinned to the primary regardless of the registered connection's read preference: a
`readPreference=secondaryPreferred` string would otherwise route loads to a lagging secondary,
where a stale revision replays an already-checkpointed step and a not-yet-replicated ledger reads
as absent — the one answer that acknowledges a wake-up.

Ledger writes — creates, checkpoints, lease acquire/renew/release, deletes — use
`w: "majority"` regardless of the registered connection's write concern. Under an inherited `w=1`
the primary acknowledges a lease or checkpoint before any secondary has it, and a failover rolls
it back: the lease a worker is executing under, or the step result it just recorded, disappears
and the step's side effect runs again. The majority write is **bounded**: an inherited
`wtimeoutMS` (and `journal`) is kept, and without one the store applies a 10 s `wtimeout`. An
unbounded majority blocked every ledger write indefinitely on a primary-secondary-arbiter replica
set whose secondary was down (the majority of data-bearing nodes can then never acknowledge, which
is why MongoDB 5.0+ defaults such sets to `w: 1`). A lapsed `wtimeout` fails the write as a
retriable error even though the primary applied it; the revision and lease fences already make
the retry safe. Restore the secondary — or remove the arbiter — rather than lowering the write
concern. The read concern is left as registered for ordinary loads — primary reads see every
write the store had acknowledged, and the revision and lease fences reject whatever a stale read
would otherwise decide. `LoadCurrentAsync`, which the engine uses where it acts on a load with no
fence behind it (a recovered response matching no pending step, a failure or resume for a run
that is not running, the read-back after a start's create lost, a re-attaching step checking
whether recovery already completed it, and settling whether a checkpoint cancelled mid-write
committed), reads with `linearizable` read concern instead: a primary that a network partition
has deposed without its noticing still serves reads for up to an election timeout, and only a
linearizable read refuses there (a majority snapshot on that node is just as stale). It is bounded
by `maxTimeMS` (10 s, the default write bound), so while the set is degraded it fails rather than
blocking — the delivery is then retried, except that `IDurableFlows.ResumeAsync` surfaces the
failure to its caller and the re-attach check falls through to the normal wait; a standalone
server rejects the read concern and the store falls back to the plain read there — as it does,
from the first refusal on, on a Mongo-compatible service that refuses the `linearizable` level
itself (Amazon DocumentDB does); a timeout, step-down or recovering node is never taken for such a
refusal and fails the read instead. An ordinary
`LoadAsync` that finds **no** live ledger repeats the read the same way before it answers
"absent", because absence has no fence behind it either: the engine acknowledges a wake-up on it,
and a recovered response whose ledger read as absent was consumed with the run still `Running`
and its step never checkpointed (a deposed primary misses a ledger the new primary created or
extended). A load that finds its ledger costs nothing extra, and one that cannot confirm absence —
a degraded set, a deposed primary — fails, so the delivery is retried instead of acknowledged. The MongoDB
channel pins the same bounded majority (a response or claim whose `wtimeout` lapsed was applied on
the primary, so the channel reads it back by id with `local` read concern — a majority read,
inherited from the database, could miss the very write it checks — and acts on what is stored),
and so does the transport on its publishes and dead-letter inserts (one
whose `wtimeout` lapsed counts as written; its lease writes and deletes use `w: 1` — see
[transport semantics](transport-semantics.md)).

The collection must keep the default **simple** collation. A collection created with a default
collation builds its `_id` index — the flow id — under it, so case- or accent-variant flow ids
would collide; the store checks the `_id` index at first use (on either `AutoCreateIndexes`
setting; with `AutoCreateIndexes = true`, only when its credentials may list indexes) and refuses
a folding collation with an actionable error. The `_id` index cannot be
rebuilt: recreate the collection without a collation and copy the documents over.

The ledger's instants are always written as BSON dates, whatever `DateTime` serializer the host
registered globally — every expiry and lease filter compares them with `$$NOW`, and the TTL
monitor reaps only dates. The members carry their own serializer, so a host-registered custom
`IBsonSerializer<DateTime>` neither changes them nor fails the ledger's class map. A document whose
`expires_at_utc` is missing or not a date is refused as unreadable (`FlowStateUnreadableException`),
never read as absent or replaced by a create.

**Upgrading a host that registered a non-date `DateTime` serializer globally** (for example
`BsonSerializer.RegisterSerializer(new DateTimeSerializer(BsonType.String))`, or a `Document` or
`Int64` representation): earlier releases wrote such ledgers' `expires_at_utc` in that
representation. Those documents used to read as absent and be replaced by the next create; now no
create replaces them (and, as before, the TTL monitor never reaps them), so every reuse of such an
id — a caller-chosen id or a scheduler occurrence — runs into an unreadable ledger:
`StartAsync` still returns the id (with a warning), but the start job fails with
`FlowStateUnreadableException` on every delivery until it is dead-lettered, and `GetStateAsync`
throws, until the document is removed. Once no run needs them, delete them (the exception's reason carries the same
command; add an `_id` condition to clear one flow):

```javascript
db.getCollection("asyncresponse_flow_state").deleteMany({ expires_at_utc: { $not: { $type: "date" } } })
```

With `AutoCreateIndexes = false` the store verifies at startup that the provisioned collection
carries a TTL index on `expires_at_utc` (`createIndex({ expires_at_utc: 1 }, { expireAfterSeconds: 0 })`)
and fails with an actionable error when it is missing — that index is the store's only cleanup
mechanism, so without the check an unprovisioned reaper meant unbounded ledger growth with no
symptom but disk.

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

`MaxStateBytes` (1.9 MB by default) bounds the **document** Cosmos receives, measured through the
registered client's serializer: the ledger JSON travels inside it as the `stateJson` string, so
every quote and backslash in the ledger is escaped a second time and a 1.2 MB ledger of escaped
characters is a 2.4 MB document — over the 2 MB item cap, and refused by Cosmos on every retry.
An oversized document fails the write with `FlowStateTooLargeException` naming the document size
instead; the ledger JSON alone is checked first, as the cheap pre-check it can only understate.

Every ledger operation — loads, updates, lease acquire/renew/release, and deletes — treats only a
`404` with sub-status `0` as a possibly absent flow. Cosmos also answers `404` for conditions
where the ledger still exists — `1002` (`ReadSessionNotAvailable`: the replicas in reach are behind
the session token the client already holds) and `1003`/`1004` (container or database recreated) —
and those surface as errors so the wake-up is retried or dead-lettered instead of being
acknowledged against a live run, and so an update or lease call does not misread one as a lost
lease.

A sub-status `0` is still only one replica's answer. Session consistency is read-your-writes for
the client that wrote; a **different process** never received the writer's session token, so its
read can be served by a replica that has not applied the write yet — a plain `404`, or an older
version of the document, never a `1002`. Cosmos cannot strengthen a read per request (unlike
DynamoDB's `ConsistentRead` or MongoDB's primary reads), so the three reads whose answer lets a
delivery be acknowledged go through the **write path** first: a conditional `PatchItemAsync` whose
`If-Match` can never hold. Writes are served by the write region's primary, so its `404` is an
authoritative "no such ledger" and its `412` means the ledger exists; the SDK also records the
`412`'s session token, so the read that follows cannot be served by a replica behind it.

- `LoadAsync` does this only when its read says the ledger is absent or logically expired — a load
  that finds its document costs nothing extra. If the write path keeps reporting the ledger present
  while reads keep answering `404`, the answer depends on the client's effective consistency (its
  `ConsistencyLevel` override, else the account default): under Session or Strong each `412` made
  the re-read current, so the ledger is physically present but hidden by its server ttl and not yet
  purged — the load reports "no state"; under Bounded Staleness, Consistent Prefix or Eventual
  nothing a read returns can prove absence, so it throws `FlowStateUnreadableException` instead.
- `ObserveLeaseAsync` does it before every observation, because a stale baseline makes a renewal
  written *before* the delivery started waiting look like proof of a live holder. It costs one
  extra bodiless request per poll (every two seconds, or each `ExecutionLeaseRenewInterval` when
  that is shorter) while a delivery waits behind a held lease.
- `LoadCurrentAsync` does it before every read. The engine calls it where a load's answer would
  let it acknowledge a delivery **without writing** — a recovered response that matches no pending
  step, a correlation-scoped failure for an id no step is pending on, a failure signal for a run
  that reads `Suspended` (an operator may just have set it back to `Running`), and a resume of a
  run that does not read `Running` — and only after a plain `LoadAsync` has already reached that
  conclusion. A decision that ends in a revision- or lease-fenced write is corrected by the fence
  when its read was stale; these are not, and an older copy of a present ledger (the holder
  checkpointed the breadcrumb, this process's replica has not applied it) dropped the recovered
  payload, the failure, or the operator's resume for good. A start job whose create reported an
  existing ledger also reads it back this way when its plain load finds no ledger, or one bound to
  different work, so the starter's fresh create is not missed and the start is not dropped. Two
  more reads go through it: a re-attaching awaited step checking whether a recovery already
  completed it (a stale "no" would wait out the step's whole deadline), and an execution whose
  caller cancelled a checkpoint mid-write, which settles once, before its next step, whether that
  write committed.

What that guarantees depends on the client's effective consistency level: **Strong**, and **Bounded
Staleness** read from the write region, were already current; **Session** (the account default)
is made current by the recorded token; **Bounded Staleness** read from another region,
**Consistent Prefix**, and **Eventual** send no session token on reads, so only the absence answer
is authoritative there and an observation can still lag — run the store at Session or Strong.
When the store has a logger with warnings enabled (it gets the host's through
`WithCosmosDurableFlows`), provisioning resolves the effective level once and logs a warning when it
is neither Session nor Strong (Bounded Staleness included); it does not refuse to run, since the
emulator defaults to Eventual.
An account with **multiple write regions** has no single authoritative write path (two regions can
both win the same ETag-fenced lease write), so none of these guarantees hold on one.

Document instants are normalized to UTC as they are read: a registered serializer with local time
zone handling (Newtonsoft `DateTimeZoneHandling.Local`) hands them back as `DateTimeKind.Local`
with local ticks, and comparing those with the UTC clock shifted every expiry and lease decision by
the host's zone offset. A document without its `expiresAtUtc` is refused as unreadable — never read
as absent, replaced by a create, checkpointed, or leased.

The lease projection query filters on `id` with `EnableScanInQuery` set, so a container provisioned
with `IndexingMode.None` (a pure key-value container) serves leases too; the scan covers one
document in one partition.

Lease maintenance — acquire, the renewal heartbeat (every `ExecutionLeaseRenewInterval`, 20 seconds
by default), and release — never moves the ledger body. Each reads a projection of the lease
fields and the document's `_etag` with a partition-scoped point query, then applies a conditional
**partial update** (`PatchItemAsync` on `leaseId`, `leaseExpiresAtUtc`, and `ttl`, fenced by
`IfMatchEtag`, with no content in the response). Earlier versions point-read the whole document
and replaced it, so an idle execution moved and re-serialized its entire ledger twice per
heartbeat, proportional to ledger size. Wire and CPU cost are now O(lease fields); the
request-unit charge still follows the service's accounting for the loaded document, so measure RU
on your own ledger sizes before sizing throughput. A projection that comes back without `_etag`
(a serializer that hides system properties) throws rather than reporting the lease free.
Checkpoints (`TryUpdateAsync`) still replace the document — they carry the new ledger.

Because a lease write reads first and patches second, any write to the document between the two
moves the `_etag` and fails the patch with `412` — most often the holder's own checkpoints, which
a checkpoint-dense flow commits back to back. The store retries after a short jittered pause. A
renewal whose attempts all lose that race while every read showed the lease still held and live
throws instead of answering `false`: the engine retries a failed renewal on its short backoff until
the lease deadline, whereas `false` would abandon a healthy execution as having lost its lease. An
acquire that keeps losing the race still answers `false`.

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
scope for `TContext`. Parallel flow executions never share a context. Reads are no-tracking and
conditional updates/deletes execute in the database. Those decide from the affected-row count, so
a provider that reports none (SQL Server sessions with NOCOUNT on by default) fails with an
actionable `InvalidOperationException` instead of reading every write as lost. The opportunistic
prune deletes in expiry order and re-checks expiry on every row it deletes, so a ledger a
concurrent create has just replaced in place is never removed with the expired batch. Every store
query ignores the context's global query filters (`IgnoreQueryFilters()`): the ledger is keyed by
flow id alone, so an application-wide filter — a tenant filter added to every entity type — would
otherwise hide rows written under another tenant, and the worker's create would collide with a row
it cannot see. The `revision` column keeps its `DEFAULT 0` in migrations, but every insert names
it, so a table provisioned without the default works too.

After adding `ConfigureAsyncResponseDurableFlows()`, generate and deploy a normal EF migration.
The package never creates or alters the schema itself.

A context that uses lazy-loading proxies (`UseLazyLoadingProxies()`) can map the ledger entity.
Change-tracking proxies (`UseChangeTrackingProxies()`) require every mapped property to be
`virtual`, which `DurableFlowStateRecord`'s are not: give the ledger its own `DbContext` there.

**Set `flowIdCollation`.** The schema is yours, so the collation of the `flow_id` key column is
too — and on SQL Server and MySQL the database default is case-insensitive, which makes two flow
ids differing only in case a single primary key: the second `StartAsync` fails as a duplicate and
a load returns the other run's state. Pass the constant for your provider
(`AsyncResponseFlowIdCollations.SqlServer` / `.MySql` / `.PostgreSql` / `.Sqlite`); the bundled
SQL Server and MySQL stores pin the equivalent in their own DDL, and the PostgreSQL store verifies
its column's collation is deterministic. On SQL Server and MySQL the EF Core store **fails at
startup** if the mapping does not declare one, and equally if it declares one that is not ordinal —
`_BIN2` on SQL Server, `_bin` on MySQL. Only a binary collation qualifies: a merely case-sensitive
one still folds accents (`_CS_AI`) or full-width forms (any collation without `_WS`).

### Application-owned store

Use the custom registration only when none of the provider packages fits. The store must implement
the complete atomic contract shown above; registration does not add a weaker fallback.

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

`MyFlowStateStore` is registered as scoped by default, so it may depend on a scoped unit of work.
Pre-register it before the chain when a different lifetime or factory is required:

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
`System.Text.Json` emits for U+0000 (SQLSTATE 22P05), so a ledger every other store accepts would
fail every write — the flow could not start, or its checkpoints failed until the job dead-lettered.
Nothing queries inside the ledger, so `text` costs nothing. With `AutoCreateSchema = true` an
existing `jsonb` column is converted in place on first use — one `ALTER TABLE` that rewrites the
table under an ACCESS EXCLUSIVE lock, blocking every flow operation on every host while it runs, so
on a large table run it by hand before the rollout; with your own migration, deploy
`ALTER TABLE ... ALTER COLUMN state_json TYPE text USING state_json::text` — the startup verifier
rejects a `jsonb` column with that instruction rather than failing later on one unlucky payload.

The PostgreSQL store's startup DDL runs under the bounds described in
[PostgreSQL › Schema creation](postgresql.md#schema-creation), shared with the channel and the
transport: the expiry index is built only when the catalog shows it absent (`CREATE INDEX IF NOT
EXISTS` takes its SHARE lock on the table before it finds the name taken, which queued every start
behind any open checkpoint writer or an operator's `CREATE INDEX CONCURRENTLY`); lock waits,
the schema's advisory lock included, are bounded by a 5 s `lock_timeout`; the conversion and the
index build run in a transaction of their own, under the table's advisory lock and an hour-long
command timeout; and a failed attempt — a lock held elsewhere, a `statement_timeout` the rewrite
outran — is retried after a jittered 30–60 s window, during which that host's flow operations fail
at once, naming the cause.

The library does not silently upgrade an incomplete concurrency schema:

- SQL packages create the complete table when missing. If a table exists without revision or lease
  columns, operations fail; deploy the correct migration before the application.
- With automatic DDL disabled, the SQL packages verify an existing table's shape on first use.
  String widths are minimums (SQL Server, MySQL, Oracle): a `flow_id nvarchar(450)` or
  `lease_id nvarchar(100)` passes, a narrower column does not, and the binary `flow_id` collation
  and full-precision timestamps stay required. No `revision` default is required — every insert
  names the column. The expiry index is performance-only and never fails startup: PostgreSQL
  verifies `{table}_expires_idx` when it is present and logs a warning when no index carries that
  name (harmless when your migration created an index on `expires_at_utc` under another name) or
  when it exists but is not yet valid and ready (a `CREATE INDEX CONCURRENTLY` still running, or one
  that failed — drop and recreate it then), and the other packages do not check theirs.
- MongoDB creates the required TTL index when missing (and, with `AutoCreateIndexes = false`,
  verifies an equivalent one exists — the TTL index is its only cleanup mechanism). It does not
  drop or rewrite a conflicting application-owned index; an equivalent TTL index under another
  name (such as `expires_at_utc_1`, what `createIndex({ expires_at_utc: 1 }, { expireAfterSeconds: 0 })`
  creates) is accepted as it stands.
- Cosmos auto-create uses the configured partition key and enables per-item TTL on a new container.
  An existing container must already use that partition key and have TTL enabled, or first use fails.
- DynamoDB validates that TTL is enabled or being enabled on the configured attribute. A table with
  TTL on another attribute fails clearly instead of leaking expired ledgers. With
  `AutoCreateTable = false` the check runs regardless of `EnableTimeToLive` — that flag governs
  whether auto-creation enables TTL, not whether an operator-provisioned table is verified to
  have it (this store has no application-side pruning, so a table without TTL grows without
  bound).
- EF Core never runs DDL. Generate and deploy an EF migration after adding
  `ConfigureAsyncResponseDurableFlows()`.

Persisted state has two revision copies: the indexed/provider field and the value inside
`state_json`. Loads require them to match; a record where they disagree, like one whose
`state_json` names a different flow id than its key, is refused as unreadable
(`FlowStateUnreadableException` naming both revisions) — never reported as absent, since the
record is physically there and "absent" acknowledges its wake-up. MongoDB, Cosmos DB, and DynamoDB
records without a physical revision are rejected the same way. This prevents a malformed or
partially migrated record from entering execution with a fabricated revision.

## Expiry and cleanup

`StateExpiry`, configured on the selected `With*DurableFlows(...)` variant, is an idle TTL. Every
successful checkpoint refreshes it, so it limits the maximum gap between checkpoints rather than
total flow duration. Loads always filter expired
state; physical cleanup is separate:

| Store | Cleanup |
|---|---|
| PostgreSQL, SQL Server, MySQL, SQLite, Oracle | Opportunistic expired-row prune on flow creation, throttled by `PruneInterval` (default 5 minutes), draining 1000-row batches while they come back full for up to `PruneBudget` (default 2 seconds; zero = one batch); a prune that fails — a deadlock victim, a lock timeout — is skipped until the next interval, never failing the `StartAsync` it rides on (loads filter on expiry, so the cost until then is disk). Deleted rows, failures, and a lapsed budget with rows remaining are counted on the `AsyncResponse` meter and logged at Warning through the store's `ILogger` |
| EF Core | Provider-side expired-row cleanup through the mapped table, pruned opportunistically like the SQL stores (same batches, budget, metrics, and logging) |
| MongoDB | TTL index on `expires_at_utc`; reads still filter because Mongo's TTL monitor is periodic |
| Cosmos DB | Container TTL plus a per-item `ttl` value |
| DynamoDB | Native TTL on `TimeToLiveAttributeName`; expiry is rounded up to avoid shortening the requested lifetime |
| In-memory | Expired entries are removed on access or replacement, and every flow creation sweeps all expired entries at most once per minute of the engine clock — so a long-lived process with unique flow ids does not retain expired ledgers |

Keep `StateExpiry` longer than the longest legitimate period without a checkpoint. Deleting a
ledger or allowing it to expire while a flow is suspended makes its outcome unknowable.

## Custom-store checklist

Use `.WithDurableFlows<MyFlowStateStore>()` only when the built-in packages do not fit. Before using
a custom store in production, test all of these against the real backend:

- many concurrent creates for one id produce exactly one winner;
- stale revision updates return `false` and never overwrite newer JSON;
- an update carrying the wrong or expired lease returns `false`;
- a lease cannot be renewed or released by another owner;
- takeover works after lease expiry;
- `ObserveLeaseAsync` returns the persisted owner and expiry raw: `Unheld` before any acquire,
  after a release, and for a missing flow (never `null`); a strictly later expiry after every
  renewal; and the old owner, unchanged, for a lease that has expired but not been re-acquired.
  A decorator around a store must forward it — the interface default silently downgrades the
  inner store to "cannot report leases";
- `LoadAsync` answers `null` only for a ledger that is authoritatively absent (or expired): the
  engine acknowledges a wake-up — and a recovered response — on it. A backend whose plain reads
  can miss a present record (replica or session reads, a deposed primary) confirms the absence
  before reporting it and throws when it cannot, as the Cosmos DB (write-path probe) and MongoDB
  (`linearizable` re-read) stores do;
- `LoadCurrentAsync` reflects every write the backend acknowledged before the call, including
  another process's. The default (`LoadAsync`) is right for a backend whose reads cannot return an
  older copy of a present record; override it when they can (replica or session reads). A
  decorator must forward it for the same reason as `ObserveLeaseAsync`;
- TTL refresh and expired-record replacement are atomic;
- **unreadable is not missing:** malformed JSON, an unknown schema version, a revision inside
  the JSON that disagrees with the stored one, and a stored `flowId` that is not the key all throw
  `FlowStateUnreadableException` — never `null`. Returning `null` there says "this flow was
  deleted", and the caller acknowledges the wake-up that was a live flow's only one — the failure
  mode a rolling deployment hits when an older replica reads a row a newer one wrote, and the one
  a corrupt or mis-restored row hits on any deployment;
- a ledger write honors the run's retention floor: the engine raises the TTL it passes to
  `TryUpdateAsync` to reach `FlowState.RetainUntilUtc` for non-terminal runs, so a store needs
  no special handling — but it must stamp the TTL it is given, not one of its own;
- `flow_id` compares **ordinally**: a case- or accent-insensitive column collation folds distinct
  runs onto one row. The built-in stores verify the deployed collation at startup and refuse a
  folding one rather than corrupting state silently;
- cancellation reaches network/database operations;
- transient failures do not fall back to unconditional writes.

Execution leases use absolute UTC expiry. Keep hosts time-synchronized and set
`ExecutionLeaseDuration` comfortably above clock skew, network jitter, and the renewal interval.

The built-in stores run the same atomic create/revision/lease contract suite against their real
providers in integration tests. Reusing one of them is the shortest path to a replica-safe store.

### Cosmos sizing and Native AOT

The complete-document size guard uses the host's custom Cosmos serializer when configured.
For the SDK default it writes the known scalar document fields with `JsonTextWriter`, including
Newtonsoft escaping, date formatting, indentation and null handling, without reflective object
serialization. Tests compare the size against the default serializer for Unicode, escaped
payloads and lease fields. The package's strict trimming/AOT analyzer build must remain clean.

## Initial-state preflight

`IDurableFlows.StartAsync` calls `IFlowStateStore.ValidateCreate` before publishing the start job.
All bundled stores check the same deterministic state/size constraints as their creation path,
without contacting the database. Cosmos measures the complete escaped document. An oversized
initial ledger raises the shared `FlowStateTooLargeException` before enqueueing work.

Custom stores remain source-compatible because the interface supplies a no-op default. Override
it for deterministic limits, leave transient connection checks in the actual write, and continue
to enforce validation in `TryCreateAsync` for callers that bypass the starter. The preflight is
not a reservation or a transaction; the published job still creates the ledger if the starter
crashes or its subsequent write encounters a transient outage.
