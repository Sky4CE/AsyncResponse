# Durable flow state stores

This page covers where durable-flow ledgers (`FlowState`) live in production: the eight built-in
store packages, how they register and provision themselves, how expired state is cleaned up per
backend, and how to implement a custom `IFlowStateStore` when none of them fits. The flow API
itself is documented in [durable-flows.md](durable-flows.md); the per-package option lists are
summarized in [configuration.md](configuration.md#durable-flow-state-store-package-options).

**On this page**

- [Supported packages](#supported-packages)
- [Registration and lifetimes](#registration-and-lifetimes)
- [Package examples](#package-examples)
- [Schema ownership](#schema-ownership)
- [Expired-state cleanup](#expired-state-cleanup)
- [Provider notes](#provider-notes)
- [Custom stores](#custom-stores)

Production durable flows should keep `FlowState` in storage owned by your application. The
recommended path is one of the built-in durable-flow store packages:

```csharp
builder.Services
    .AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithSqlServerDurableFlows(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("SqlServer");
        options.SchemaName = "dbo";
        options.TableName = "asyncresponse_flow_state";
    });
```

The default `RecoveryBackedFlowStateStore` stays available for tests, development, and migration,
but it stores flow ledgers in the configured channel recovery store. Those stores are often
TTL/cache-shaped, so the default logs a warning the first time it persists flow state.

## Supported packages

| Package | Fluent registration | Backing store |
|---|---|---|
| `AsyncResponse.DurableFlows.SqlServer` | `WithSqlServerDurableFlows(...)` | SQL Server table |
| `AsyncResponse.DurableFlows.PostgreSQL` | `WithPostgreSqlDurableFlows(...)` | PostgreSQL table |
| `AsyncResponse.DurableFlows.MySql` | `WithMySqlDurableFlows(...)` | MySQL or MariaDB table |
| `AsyncResponse.DurableFlows.Sqlite` | `WithSqliteDurableFlows(...)` | SQLite table |
| `AsyncResponse.DurableFlows.Oracle` | `WithOracleDurableFlows(...)` | Oracle table |
| `AsyncResponse.DurableFlows.MongoDB` | `WithMongoDbDurableFlows(...)` | MongoDB collection |
| `AsyncResponse.DurableFlows.Cosmos` | `WithCosmosDurableFlows(...)` | Azure Cosmos DB container |
| `AsyncResponse.DurableFlows.DynamoDB` | `WithDynamoDbDurableFlows(...)` | DynamoDB table |
| `AsyncResponse.DurableFlows.EFCore` | `WithEFCoreDurableFlows<TDbContext>(...)` | A table in your own `DbContext` (any EF Core relational provider) |

All nine stores run their contract tests against real servers in the default CI integration
suite — the fixture provisions live SQL Server, PostgreSQL, MySQL, MongoDB, and DynamoDB
(LocalStack) containers plus `gvenzl/oracle-free` and the Azure Cosmos DB emulator. Set
`ASYNCRESPONSE_ITEST_SKIP_ORACLE_COSMOS=true` to skip the two heavyweight Oracle/Cosmos containers
(the store tests then skip cleanly); the EF Core store rides the SQL Server container through the
`Microsoft.EntityFrameworkCore.SqlServer` provider. SQLite additionally has in-repo contract,
end-to-end, and concurrency-storm unit tests, and the EF Core store repeats that unit suite over
the EF SQLite provider in both context-resolution modes.

## Registration and lifetimes

Every package registers its store as a **singleton**: schema, index, container, or table
provisioning is cached per store instance and runs once per process, and control-plane calls
(Cosmos metadata operations, DynamoDB `DescribeTable`/`CreateTable`) are not re-issued per flow
execution. The `IFlowStateStore` interface is forwarded to that concrete singleton, so resolving
either yields the same instance.

Client ownership follows one rule — **no package registers a bare client service**
(`NpgsqlDataSource`, `IMongoClient`/`IMongoDatabase`, `CosmosClient`, `IAmazonDynamoDB`), so
unrelated resolutions of those types are never answered, or broken, by a store package:

- If the host has already registered the client, the store reuses it.
- Otherwise the store creates one from its options (or, for DynamoDB, the default AWS
  credential/region chain) and owns it — the client is disposed with the container.

`WithCustomDurableFlows<TStore>()` is the one deliberately **scoped** registration: your own store
can depend on scoped services (an EF Core `DbContext`, a per-request unit of work) and is resolved
from a fresh scope per flow execution. It `TryAdd`s the concrete `TStore` as scoped and forwards
`IFlowStateStore` to it — which is exactly how the packages layer on top of it: they pre-register
the concrete store as a singleton (so the scoped `TryAdd` is a no-op), then call
`WithCustomDurableFlows<TStore>()` for the interface forwarding.

## Package examples

SQL Server:

```csharp
using AsyncResponse.DurableFlows.SqlServer;

builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithSqlServerDurableFlows(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("SqlServer");
        options.SchemaName = "dbo";
        options.TableName = "asyncresponse_flow_state";
        options.AutoCreateSchema = true; // set false when migrations own DDL
    });
```

PostgreSQL, reusing an app-wide `NpgsqlDataSource`:

```csharp
using AsyncResponse.DurableFlows.PostgreSQL;
using Npgsql;

builder.Services.AddSingleton(_ =>
    NpgsqlDataSource.Create(builder.Configuration.GetConnectionString("PostgreSQL")!));

builder.Services.AddAsyncResponse()
    .WithPostgreSqlChannel()
    .WithRabbitMqTransport(...)
    .WithPostgreSqlDurableFlows(options =>
    {
        options.SchemaName = "public";
        options.TableName = "asyncresponse_flow_state";
    });
```

MySQL or MariaDB:

```csharp
using AsyncResponse.DurableFlows.MySql;

builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithMySqlDurableFlows(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("MySql");
        options.TableName = "asyncresponse_flow_state";
    });
```

SQLite:

```csharp
using AsyncResponse.DurableFlows.Sqlite;

builder.Services.AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithSqliteDurableFlows(options =>
    {
        options.ConnectionString = "Data Source=asyncresponse-flow-state.db";
    });
```

Oracle:

```csharp
using AsyncResponse.DurableFlows.Oracle;

builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithOracleDurableFlows(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("Oracle");
        options.TableName = "ASYNCRESPONSE_FLOW_STATE";
    });
```

MongoDB:

```csharp
using AsyncResponse.DurableFlows.MongoDB;

builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithMongoDbDurableFlows(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("MongoDB");
        options.DatabaseName = "app";
        options.CollectionName = "asyncresponse_flow_state";
    });
```

Cosmos DB:

```csharp
using AsyncResponse.DurableFlows.Cosmos;

builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithCosmosDurableFlows(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("Cosmos");
        options.DatabaseName = "app";
        options.ContainerName = "asyncresponse_flow_state";
        options.PartitionKeyPath = "/flowId";
    });
```

DynamoDB:

```csharp
using AsyncResponse.DurableFlows.DynamoDB;

builder.Services.AddAsyncResponse()
    .WithRedisChannel()
    .WithSqsTransport(...)
    .WithDynamoDbDurableFlows(options =>
    {
        options.TableName = "AsyncResponseFlowState";
        options.AutoCreateTable = true;
        options.EnableTimeToLive = true;
    });
```

Entity Framework Core — the ledger table lives inside your own `DbContext`, so it works with any
EF Core relational provider and rides your existing migration pipeline:

```csharp
using AsyncResponse.DurableFlows.EFCore;

public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Maps DurableFlowStateRecord; add a normal migration afterwards.
        modelBuilder.ConfigureAsyncResponseDurableFlows();
    }
}

builder.Services.AddDbContext<AppDbContext>(options => options.UseNpgsql(connectionString));
// or AddDbContextFactory<AppDbContext>(...) — the store prefers the factory when one is registered.

builder.Services.AddAsyncResponse()
    .WithPostgreSqlChannel(...)
    .WithPostgreSqlTransport(...)
    .WithEFCoreDurableFlows<AppDbContext>();
```

Every operation leases a fresh context (from `IDbContextFactory<TContext>` when registered,
otherwise the scoped `TContext` from a new scope), so parallel flow executions never share a
`DbContext`. Reads are no-tracking; deletes and pruning use `ExecuteDeleteAsync`, updates use
`ExecuteUpdateAsync`. Column names match the other relational packages, so the table is
interchangeable with the ones the SQL Server/PostgreSQL/MySQL/SQLite packages create.

## Schema ownership

Every package can create its table, collection index, container, or DynamoDB table on first use.
That is convenient for development and tests. For production environments that use migrations or
infrastructure-as-code, set the package's `AutoCreate...` option to `false` and provision the
same shape yourself.

The one exception is `AsyncResponse.DurableFlows.EFCore`: it never runs DDL. The table is part of
your `DbContext` model (via `ConfigureAsyncResponseDurableFlows()`), so it is created by whatever
already creates the rest of your schema — EF migrations, `EnsureCreated`, or your own scripts.

Concurrent first-use provisioning is safe across processes:

- **PostgreSQL / SQL Server** serialize DDL with the same transaction-scoped advisory lock /
  `sp_getapplock` (resource `asyncresponse:ddl:{SchemaName}`) as the channel and transport
  packages, so flow-store DDL also serializes with any channel/transport DDL running against the
  same schema. `CREATE ... IF NOT EXISTS` alone is not atomic against a concurrent create — the
  lock is what prevents the catalog collision.
- **DynamoDB** tolerates a concurrent `CreateTable` (`ResourceInUseException` means another
  process won the race), waits for the table to become `ACTIVE`, and checks the TTL status before
  enabling it rather than blind-enabling and swallowing the error.
- **Oracle** ignores `ORA-00955` (object already exists) on `CREATE TABLE`/`CREATE INDEX`.
- **MongoDB / Cosmos** use natively idempotent create-if-not-exists index/container calls.

Relational table shape:

```sql
flow_id         string primary key
state_json      large text / json
expires_at_utc  UTC timestamp
updated_at_utc  UTC timestamp
index on expires_at_utc
```

Document store shape:

```json
{
  "id": "flow-id",
  "flowId": "flow-id",
  "stateJson": "{... FlowState ...}",
  "expiresAtUtc": "2026-07-09T12:00:00Z",
  "updatedAtUtc": "2026-07-09T11:00:00Z"
}
```

DynamoDB item shape:

```text
flow_id    S  partition key
state_json S
expires_at N  Unix seconds, optional DynamoDB TTL attribute
updated_at N  Unix seconds
```

## Expired-state cleanup

Loads always treat expired state as absent — the expiry filter on read is the correctness
mechanism. Cleanup of the expired rows/documents themselves differs per family:

| Store family | Cleanup mechanism |
|---|---|
| SQL stores (PostgreSQL, SQL Server, MySQL, SQLite, Oracle) | **Opportunistic prune on save**: `SaveAsync` deletes expired rows, throttled by the `PruneInterval` option (default 5 minutes; zero or negative prunes on every save). Pruning only bounds table growth — it never affects correctness. |
| MongoDB | **Native TTL index** (`expireAfterSeconds = 0` on `expires_at_utc`): MongoDB reaps expired ledgers itself. A pre-existing plain (non-TTL) index with the same name, e.g. from an earlier package version, is replaced in place. Loads still filter on expiry because the TTL monitor runs only periodically (~60 s). |
| Cosmos DB | **Container TTL + per-item `ttl`**: auto-create enables `DefaultTimeToLive = -1` (per-item TTL without a container-wide default) and each save writes a per-item `ttl`; a pre-existing container without TTL enabled is upgraded in place. |
| DynamoDB | **Native TTL** on the expiry attribute (`expires_at`, Unix seconds). The expiry epoch is rounded **up** to whole seconds so the effective TTL is never shorter than requested. |

## Provider notes

- **Oracle** — the default index name (`{TableName}_EXPIRES_IDX` on the default
  `ASYNCRESPONSE_FLOW_STATE` table) exceeds the 30-character identifier limit of Oracle ≤ 12.1,
  so the default table name requires **Oracle 12.2+**. On older servers, shorten `TableName`.
- **Oracle** — `SaveAsync` retries once on `ORA-00001`: Oracle's `MERGE` has no `HOLDLOCK`
  equivalent, so two concurrent saves for the same *new* flow id can both take the
  `WHEN NOT MATCHED` branch; the retry takes the `MATCHED` branch — ordinary last-writer-wins,
  matching the other stores' atomic upserts.
- **MySQL / MariaDB** — the upsert uses `VALUES()` in `ON DUPLICATE KEY UPDATE` deliberately:
  MySQL 8.0.20+ deprecates it in favor of row aliases, but `VALUES()` is the only syntax MariaDB
  supports.
- **SQL Server** — the upsert is `MERGE ... WITH (HOLDLOCK)`, making concurrent saves for one
  flow id atomic.

## Custom stores

Use `WithCustomDurableFlows<TStore>()` when a built-in package does not fit your persistence
model. The library calls only three methods:

```csharp
public interface IFlowStateStore
{
    Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken ct = default);
    Task<FlowState?> LoadAsync(string flowId, CancellationToken ct = default);
    Task<bool> TryDeleteAsync(string flowId, CancellationToken ct = default);
}
```

Custom relational adapter (PostgreSQL/SQLite-style upsert shown; use your provider's equivalent
upsert syntax):

```csharp
using AsyncResponse;
using System.Data.Common;
using System.Text.Json;

public sealed class MyFlowStateStore(
    Func<CancellationToken, ValueTask<DbConnection>> openConnection) : IFlowStateStore
{
    public async Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken ct = default)
    {
        var now = DateTime.UtcNow;
        await using var connection = await openConnection(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO asyncresponse_flow_state(flow_id, state_json, expires_at_utc, updated_at_utc)
            VALUES(@flow_id, @state_json, @expires_at_utc, @updated_at_utc)
            ON CONFLICT(flow_id) DO UPDATE SET
                state_json = excluded.state_json,
                expires_at_utc = excluded.expires_at_utc,
                updated_at_utc = excluded.updated_at_utc
            """;
        Add(command, "flow_id", flowId);
        Add(command, "state_json", JsonSerializer.Serialize(state));
        Add(command, "expires_at_utc", now.Add(ttl));
        Add(command, "updated_at_utc", now);
        await command.ExecuteNonQueryAsync(ct);
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken ct = default)
    {
        await using var connection = await openConnection(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT state_json
            FROM asyncresponse_flow_state
            WHERE flow_id = @flow_id AND expires_at_utc > @now_utc
            """;
        Add(command, "flow_id", flowId);
        Add(command, "now_utc", DateTime.UtcNow);

        var json = await command.ExecuteScalarAsync(ct) as string;
        if (json is null)
            return null;

        var state = JsonSerializer.Deserialize<FlowState>(json);
        return state is not null && FlowStateSchema.IsReadable(state.SchemaVersion) ? state : null;
    }

    public async Task<bool> TryDeleteAsync(string flowId, CancellationToken ct = default)
    {
        await using var connection = await openConnection(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM asyncresponse_flow_state WHERE flow_id = @flow_id";
        Add(command, "flow_id", flowId);
        return await command.ExecuteNonQueryAsync(ct) > 0;
    }

    private static void Add(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = "@" + name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
```

Register the custom store:

```csharp
builder.Services
    .AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithCustomDurableFlows<MyFlowStateStore>();
```

Document and key-value stores follow the same shape: upsert one JSON document/item keyed by
`flowId`, filter reads by expiry, reject future `FlowStateSchema` versions, and delete by key.
