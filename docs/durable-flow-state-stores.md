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
5. Loads treat expired, malformed, identity-mismatched, revision-mismatched, and unrecognized-schema
   records as absent.

There is no weaker compatibility path and no process-local fallback for an incomplete custom
store. That keeps single-node tests and multi-replica production on the same correctness model.
The in-memory implementation satisfies the same atomic contract inside one process; it cannot make
state survive or coordinate a different process.

Durable-flow fencing prevents two healthy workers from checkpointing one run concurrently. It does
not make an external side effect and the following checkpoint one transaction. Steps and triggers
must still be idempotent.

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
derived index name — e.g. one already ending `_EXPIRES_IDX` — since Oracle shares one namespace for
tables and indexes and the index create would otherwise fail with an error indistinguishable from a
benign already-exists race, silently leaving the expiry index never created.

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
`CollectionName` is needed. With a registered `IMongoClient`, configure `DatabaseName`.

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
            // The bundled PostgreSQL/SQL Server/MySQL stores pin this in their own DDL.
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
conditional updates/deletes execute in the database.

After adding `ConfigureAsyncResponseDurableFlows()`, generate and deploy a normal EF migration.
The package never creates or alters the schema itself.

**Set `flowIdCollation`.** The schema is yours, so the collation of the `flow_id` key column is
too — and on SQL Server and MySQL the database default is case-insensitive, which makes two flow
ids differing only in case a single primary key: the second `StartAsync` fails as a duplicate and
a load returns the other run's state. Pass the constant for your provider
(`AsyncResponseFlowIdCollations.SqlServer` / `.MySql` / `.PostgreSql` / `.Sqlite`); the bundled
relational stores pin the equivalent in their own DDL. On those two providers the store **fails at
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
flow_id              string primary key
state_json           large text / JSON
expires_at_utc       UTC timestamp
updated_at_utc       UTC timestamp
revision             64-bit integer, not null
lease_id             nullable string(64)
lease_expires_at_utc nullable UTC timestamp
index on expires_at_utc
```

Document stores persist the same fields. DynamoDB uses `flow_id` as the partition key, Unix seconds
for the TTL attribute, and Unix milliseconds for lease expiry.

The library does not silently upgrade an incomplete concurrency schema:

- SQL packages create the complete table when missing. If a table exists without revision or lease
  columns, operations fail; deploy the correct migration before the application.
- MongoDB creates the required TTL index when missing. It does not drop or rewrite a conflicting
  application-owned index.
- Cosmos auto-create uses the configured partition key and enables per-item TTL on a new container.
  An existing container must already use that partition key and have TTL enabled, or first use fails.
- DynamoDB validates that TTL is enabled or being enabled on the configured attribute. A table with
  TTL on another attribute fails clearly instead of leaking expired ledgers.
- EF Core never runs DDL. Generate and deploy an EF migration after adding
  `ConfigureAsyncResponseDurableFlows()`.

Persisted state has two revision copies: the indexed/provider field and the value inside
`state_json`. Loads require them to match. MongoDB, Cosmos DB, and DynamoDB records without a
physical revision are rejected. This prevents a malformed or partially migrated record from
entering execution with a fabricated revision.

## Expiry and cleanup

`StateExpiry`, configured on the selected `With*DurableFlows(...)` variant, is an idle TTL. Every
successful checkpoint refreshes it, so it limits the maximum gap between checkpoints rather than
total flow duration. Loads always filter expired
state; physical cleanup is separate:

| Store | Cleanup |
|---|---|
| PostgreSQL, SQL Server, MySQL, SQLite, Oracle | Opportunistic expired-row prune on flow creation, throttled by `PruneInterval` (default 5 minutes) |
| EF Core | Provider-side expired-row cleanup through the mapped table |
| MongoDB | TTL index on `expires_at_utc`; reads still filter because Mongo's TTL monitor is periodic |
| Cosmos DB | Container TTL plus a per-item `ttl` value |
| DynamoDB | Native TTL on `TimeToLiveAttributeName`; expiry is rounded up to avoid shortening the requested lifetime |
| In-memory | Expired entries are removed on access or replacement |

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
- TTL refresh and expired-record replacement are atomic;
- malformed JSON, wrong `flowId`, missing/mismatched revision, and missing or unsupported schema versions load
  as `null`;
- cancellation reaches network/database operations;
- transient failures do not fall back to unconditional writes.

Execution leases use absolute UTC expiry. Keep hosts time-synchronized and set
`ExecutionLeaseDuration` comfortably above clock skew, network jitter, and the renewal interval.

The built-in stores run the same atomic create/revision/lease contract suite against their real
providers in integration tests. Reusing one of them is the shortest path to a replica-safe store.
