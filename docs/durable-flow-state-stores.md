# Durable flow state stores

Production durable flows should keep `FlowState` in storage owned by your application:

```csharp
builder.Services
    .AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithDurableFlows<RelationalFlowStateStore>();
```

The default `RecoveryBackedFlowStateStore` stays available for tests, development, and migration,
but it stores flow ledgers in the channel recovery store. Those stores are usually TTL/cache
shaped, so the default logs a warning the first time it persists flow state.

The examples below use three store shapes:

| Store | Use for |
|---|---|
| `RelationalFlowStateStore` | SQL Server, PostgreSQL, MySQL, MariaDB, SQLite, Oracle |
| `DocumentFlowStateStore` | MongoDB, Azure Cosmos DB |
| `KeyValueFlowStateStore` | DynamoDB or another durable key-value table |

All three `IFlowStateStore` implementations are covered by
`DurableFlowStateStoreExampleTests`: save/load/expiry/delete contract tests plus an end-to-end
durable-flow run through `WithDurableFlows<TStore>()`.

## Relational databases

This one store works for SQL Server, PostgreSQL, MySQL/MariaDB, SQLite, and Oracle because it uses
plain SQL plus provider parameters.

```csharp
using AsyncResponse;
using System.Data.Common;
using System.Text.Json;

public sealed class RelationalFlowStateStore(
    Func<CancellationToken, ValueTask<DbConnection>> openConnection) : IFlowStateStore
{
    public async Task SaveAsync(
        string flowId,
        FlowState state,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentNullException.ThrowIfNull(state);

        var now = DateTime.UtcNow;
        var json = JsonSerializer.Serialize(state);
        var expires = now.Add(ttl);

        await using var connection = await openConnection(cancellationToken);
        var updated = await ExecuteAsync(
            connection,
            """
            UPDATE async_response_flow_state
               SET state_json = @state_json,
                   expires_at_utc = @expires_at_utc,
                   updated_at_utc = @updated_at_utc
             WHERE flow_id = @flow_id
            """,
            cancellationToken,
            ("flow_id", flowId),
            ("state_json", json),
            ("expires_at_utc", expires),
            ("updated_at_utc", now));

        if (updated != 0)
            return;

        await ExecuteAsync(
            connection,
            """
            INSERT INTO async_response_flow_state
                (flow_id, state_json, expires_at_utc, updated_at_utc)
            VALUES
                (@flow_id, @state_json, @expires_at_utc, @updated_at_utc)
            """,
            cancellationToken,
            ("flow_id", flowId),
            ("state_json", json),
            ("expires_at_utc", expires),
            ("updated_at_utc", now));
    }

    public async Task<FlowState?> LoadAsync(
        string flowId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        await using var connection = await openConnection(cancellationToken);
        await using var command = CreateCommand(
            connection,
            """
            SELECT state_json
              FROM async_response_flow_state
             WHERE flow_id = @flow_id
               AND expires_at_utc > @now_utc
            """,
            ("flow_id", flowId),
            ("now_utc", DateTime.UtcNow));

        var json = await command.ExecuteScalarAsync(cancellationToken) as string;
        if (json is null)
            return null;

        var state = JsonSerializer.Deserialize<FlowState>(json);
        return state is not null && FlowStateSchema.IsReadable(state.SchemaVersion) ? state : null;
    }

    public async Task<bool> TryDeleteAsync(
        string flowId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        await using var connection = await openConnection(cancellationToken);
        return await ExecuteAsync(
            connection,
            "DELETE FROM async_response_flow_state WHERE flow_id = @flow_id",
            cancellationToken,
            ("flow_id", flowId)) > 0;
    }

    private static async Task<int> ExecuteAsync(
        DbConnection connection,
        string sql,
        CancellationToken cancellationToken,
        params (string Name, object Value)[] parameters)
    {
        await using var command = CreateCommand(connection, sql, parameters);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static DbCommand CreateCommand(
        DbConnection connection,
        string sql,
        params (string Name, object Value)[] parameters)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = "@" + name;
            parameter.Value = value;
            command.Parameters.Add(parameter);
        }

        return command;
    }
}
```

Register it by supplying the provider connection:

```csharp
using System.Data.Common;

builder.Services.AddScoped<Func<CancellationToken, ValueTask<DbConnection>>>(_ =>
    async cancellationToken =>
    {
        var connection = new /* provider connection */("<connection string>");
        await connection.OpenAsync(cancellationToken);
        return connection;
    });

builder.Services
    .AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithDurableFlows<RelationalFlowStateStore>();
```

Provider constructors:

```csharp
// SQL Server
var connection = new Microsoft.Data.SqlClient.SqlConnection(sqlServerConnectionString);

// PostgreSQL
var connection = new Npgsql.NpgsqlConnection(postgresConnectionString);

// MySQL / MariaDB (MySqlConnector)
var connection = new MySqlConnector.MySqlConnection(mySqlConnectionString);

// SQLite
var connection = new Microsoft.Data.Sqlite.SqliteConnection(sqliteConnectionString);

// Oracle
var connection = new Oracle.ManagedDataAccess.Client.OracleConnection(oracleConnectionString);
```

Schemas:

```sql
-- SQL Server
CREATE TABLE dbo.async_response_flow_state (
    flow_id nvarchar(200) NOT NULL PRIMARY KEY,
    state_json nvarchar(max) NOT NULL,
    expires_at_utc datetime2 NOT NULL,
    updated_at_utc datetime2 NOT NULL
);

CREATE INDEX ix_async_response_flow_state_expires_at_utc
    ON dbo.async_response_flow_state (expires_at_utc);
```

```sql
-- PostgreSQL
CREATE TABLE async_response_flow_state (
    flow_id text PRIMARY KEY,
    state_json jsonb NOT NULL,
    expires_at_utc timestamptz NOT NULL,
    updated_at_utc timestamptz NOT NULL
);

CREATE INDEX ix_async_response_flow_state_expires_at_utc
    ON async_response_flow_state (expires_at_utc);
```

```sql
-- MySQL / MariaDB
CREATE TABLE async_response_flow_state (
    flow_id varchar(200) NOT NULL PRIMARY KEY,
    state_json json NOT NULL,
    expires_at_utc datetime(6) NOT NULL,
    updated_at_utc datetime(6) NOT NULL,
    INDEX ix_async_response_flow_state_expires_at_utc (expires_at_utc)
);
```

```sql
-- SQLite
CREATE TABLE async_response_flow_state (
    flow_id TEXT NOT NULL PRIMARY KEY,
    state_json TEXT NOT NULL,
    expires_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE INDEX ix_async_response_flow_state_expires_at_utc
    ON async_response_flow_state (expires_at_utc);
```

```sql
-- Oracle
CREATE TABLE async_response_flow_state (
    flow_id varchar2(200) NOT NULL PRIMARY KEY,
    state_json clob NOT NULL,
    expires_at_utc timestamp NOT NULL,
    updated_at_utc timestamp NOT NULL
);

CREATE INDEX ix_async_response_flow_state_exp
    ON async_response_flow_state (expires_at_utc);
```

## Document databases

Use this store for MongoDB and Cosmos DB. The store is provider-neutral; the adapter below it is
the database-specific part.

```csharp
using AsyncResponse;
using System.Text.Json;

public sealed record FlowStateDocument(
    string FlowId,
    string StateJson,
    DateTime ExpiresAtUtc,
    DateTime UpdatedAtUtc)
{
    // Cosmos DB's default item id. MongoDB will simply store it as another field.
    public string id => FlowId;
}

public interface IFlowStateDocuments
{
    Task UpsertAsync(FlowStateDocument document, CancellationToken cancellationToken);
    Task<FlowStateDocument?> FindAsync(string flowId, DateTime nowUtc, CancellationToken cancellationToken);
    Task<bool> DeleteAsync(string flowId, CancellationToken cancellationToken);
}

public sealed class DocumentFlowStateStore(IFlowStateDocuments documents) : IFlowStateStore
{
    public Task SaveAsync(
        string flowId,
        FlowState state,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        var now = DateTime.UtcNow;
        return documents.UpsertAsync(
            new FlowStateDocument(flowId, JsonSerializer.Serialize(state), now.Add(ttl), now),
            cancellationToken);
    }

    public async Task<FlowState?> LoadAsync(
        string flowId,
        CancellationToken cancellationToken = default)
    {
        var document = await documents.FindAsync(flowId, DateTime.UtcNow, cancellationToken);
        if (document is null)
            return null;

        var state = JsonSerializer.Deserialize<FlowState>(document.StateJson);
        return state is not null && FlowStateSchema.IsReadable(state.SchemaVersion) ? state : null;
    }

    public Task<bool> TryDeleteAsync(
        string flowId,
        CancellationToken cancellationToken = default)
        => documents.DeleteAsync(flowId, cancellationToken);
}
```

MongoDB adapter:

```csharp
using MongoDB.Driver;

public sealed class MongoFlowStateDocuments(
    IMongoCollection<FlowStateDocument> collection) : IFlowStateDocuments
{
    public async Task UpsertAsync(FlowStateDocument document, CancellationToken cancellationToken)
        => await collection.ReplaceOneAsync(
            x => x.FlowId == document.FlowId,
            document,
            new ReplaceOptions { IsUpsert = true },
            cancellationToken);

    public Task<FlowStateDocument?> FindAsync(
        string flowId,
        DateTime nowUtc,
        CancellationToken cancellationToken)
        => collection
            .Find(x => x.FlowId == flowId && x.ExpiresAtUtc > nowUtc)
            .FirstOrDefaultAsync(cancellationToken);

    public async Task<bool> DeleteAsync(string flowId, CancellationToken cancellationToken)
    {
        var result = await collection.DeleteOneAsync(x => x.FlowId == flowId, cancellationToken);
        return result.DeletedCount > 0;
    }
}
```

Mongo indexes:

```javascript
db.async_response_flow_state.createIndex({ FlowId: 1 }, { unique: true });
db.async_response_flow_state.createIndex({ ExpiresAtUtc: 1 });
```

Cosmos DB adapter:

```csharp
using Microsoft.Azure.Cosmos;
using System.Net;

public sealed class CosmosFlowStateDocuments(Container container) : IFlowStateDocuments
{
    public async Task UpsertAsync(FlowStateDocument document, CancellationToken cancellationToken)
        => await container.UpsertItemAsync(document, new PartitionKey(document.FlowId), cancellationToken: cancellationToken);

    public async Task<FlowStateDocument?> FindAsync(
        string flowId,
        DateTime nowUtc,
        CancellationToken cancellationToken)
    {
        try
        {
            var response = await container.ReadItemAsync<FlowStateDocument>(
                flowId,
                new PartitionKey(flowId),
                cancellationToken: cancellationToken);

            return response.Resource.ExpiresAtUtc > nowUtc ? response.Resource : null;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<bool> DeleteAsync(string flowId, CancellationToken cancellationToken)
    {
        try
        {
            await container.DeleteItemAsync<FlowStateDocument>(
                flowId,
                new PartitionKey(flowId),
                cancellationToken: cancellationToken);
            return true;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }
}
```

For Cosmos DB, configure the container with partition key `/FlowId`.

Register the document store:

```csharp
builder.Services.AddScoped<IFlowStateDocuments, MongoFlowStateDocuments>();
// or:
builder.Services.AddScoped<IFlowStateDocuments, CosmosFlowStateDocuments>();

builder.Services
    .AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithDurableFlows<DocumentFlowStateStore>();
```

## Key-value databases

Use this for DynamoDB or another durable key-value table.

```csharp
using AsyncResponse;
using System.Text.Json;

public interface IFlowStateKeyValueTable
{
    Task PutAsync(string key, string json, DateTime expiresAtUtc, CancellationToken cancellationToken);
    Task<(string Json, DateTime ExpiresAtUtc)?> GetAsync(string key, DateTime nowUtc, CancellationToken cancellationToken);
    Task<bool> DeleteAsync(string key, CancellationToken cancellationToken);
}

public sealed class KeyValueFlowStateStore(IFlowStateKeyValueTable table) : IFlowStateStore
{
    public Task SaveAsync(
        string flowId,
        FlowState state,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
        => table.PutAsync(flowId, JsonSerializer.Serialize(state), DateTime.UtcNow.Add(ttl), cancellationToken);

    public async Task<FlowState?> LoadAsync(
        string flowId,
        CancellationToken cancellationToken = default)
    {
        var item = await table.GetAsync(flowId, DateTime.UtcNow, cancellationToken);
        if (item is null)
            return null;

        var state = JsonSerializer.Deserialize<FlowState>(item.Value.Json);
        return state is not null && FlowStateSchema.IsReadable(state.SchemaVersion) ? state : null;
    }

    public Task<bool> TryDeleteAsync(
        string flowId,
        CancellationToken cancellationToken = default)
        => table.DeleteAsync(flowId, cancellationToken);
}
```

DynamoDB adapter:

```csharp
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

public sealed class DynamoFlowStateTable(
    IAmazonDynamoDB dynamo,
    string tableName) : IFlowStateKeyValueTable
{
    public async Task PutAsync(
        string key,
        string json,
        DateTime expiresAtUtc,
        CancellationToken cancellationToken)
        => await dynamo.PutItemAsync(new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["flow_id"] = new() { S = key },
                ["state_json"] = new() { S = json },
                ["expires_at_unix"] = new() { N = new DateTimeOffset(expiresAtUtc).ToUnixTimeSeconds().ToString() },
                ["updated_at_unix"] = new() { N = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString() }
            }
        }, cancellationToken);

    public async Task<(string Json, DateTime ExpiresAtUtc)?> GetAsync(
        string key,
        DateTime nowUtc,
        CancellationToken cancellationToken)
    {
        var response = await dynamo.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["flow_id"] = new() { S = key }
            },
            ConsistentRead = true
        }, cancellationToken);

        if (response.Item.Count == 0)
            return null;

        var expires = DateTimeOffset
            .FromUnixTimeSeconds(long.Parse(response.Item["expires_at_unix"].N))
            .UtcDateTime;

        return expires > nowUtc ? (response.Item["state_json"].S, expires) : null;
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken)
    {
        var response = await dynamo.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["flow_id"] = new() { S = key }
            },
            ReturnValues = ReturnValue.ALL_OLD
        }, cancellationToken);

        return response.Attributes.Count > 0;
    }
}
```

DynamoDB table:

```text
Table: async_response_flow_state
Partition key: flow_id (S)
Optional TTL attribute: expires_at_unix (N)
```

Register the key-value store:

```csharp
builder.Services.AddScoped<IFlowStateKeyValueTable>(sp =>
    new DynamoFlowStateTable(
        sp.GetRequiredService<IAmazonDynamoDB>(),
        "async_response_flow_state"));

builder.Services
    .AddAsyncResponse()
    .WithRedisChannel()
    .WithRabbitMqTransport(...)
    .WithDurableFlows<KeyValueFlowStateStore>();
```
