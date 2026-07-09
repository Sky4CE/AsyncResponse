# Durable flow state stores

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

All packages register `IFlowStateStore` as scoped through `WithCustomDurableFlows<TStore>()`
internally, so scoped database dependencies work normally. Package tests cover registration for
every store, real SQLite contract/end-to-end/concurrency runs, live SQL Server, PostgreSQL, MySQL,
MongoDB, and DynamoDB contract tests through the integration fixture, and opt-in Oracle/Cosmos DB
contract tests via `ASYNCRESPONSE_ITEST_ORACLE_CONNECTION_STRING` and
`ASYNCRESPONSE_ITEST_COSMOS_CONNECTION_STRING`.

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

## Schema ownership

Every package can create its table, collection index, container, or DynamoDB table on first use.
That is convenient for development and tests. For production environments that use migrations or
infrastructure-as-code, set the package's `AutoCreate...` option to `false` and provision the
same shape yourself.

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
