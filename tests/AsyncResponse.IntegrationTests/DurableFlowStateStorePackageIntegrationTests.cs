using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using AsyncResponse.DurableFlows.Cosmos;
using AsyncResponse.DurableFlows.DynamoDB;
using AsyncResponse.DurableFlows.EFCore;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.DurableFlows.MySql;
using AsyncResponse.DurableFlows.Oracle;
using AsyncResponse.DurableFlows.PostgreSQL;
using AsyncResponse.DurableFlows.SqlServer;
using AsyncResponse.DurableFlows.Sqlite;
using Microsoft.Azure.Cosmos;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System.Text.Json;
using Xunit;
using static AsyncResponse.IntegrationTests.FlowStoreContract;

namespace AsyncResponse.IntegrationTests;

[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class DurableFlowStateStorePackageIntegrationTests(DataBatchFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task SqlitePackageStore_RoundTrips_Expires_Deletes()
    {
        var databasePath = Path.Combine(Path.GetTempPath(), $"ar-durable-flow-itest-{Guid.NewGuid():N}.db");
        try
        {
            var store = new SqliteFlowStateStore(
                Options.Create(new SqliteDurableFlowOptions
                {
                    ConnectionString = $"Data Source={databasePath}",
                    TableName = "flow_state"
                }));

            await AssertStoreContractAsync(store);
        }
        finally
        {
            File.Delete(databasePath);
        }
    }

    [Fact]
    public async Task PostgreSqlPackageStore_RoundTrips_Expires_Deletes()
    {
        var schema = NewIdentifier("df_pg", 32);
        await using var dataSource = NpgsqlDataSource.Create(Fixture.PostgreSqlConnectionString);
        try
        {
            var store = new PostgreSqlFlowStateStore(
                dataSource,
                Options.Create(new PostgreSqlDurableFlowOptions
                {
                    SchemaName = schema,
                    TableName = "flow_state"
                }));

            await AssertStoreContractAsync(store, seedRawStateAsync: async (flowId, stateJson) =>
            {
                await using var connection = await dataSource.OpenConnectionAsync();
                await using var seed = connection.CreateCommand();
                seed.CommandText =
                    $"""INSERT INTO "{schema}"."flow_state" (flow_id, state_json, expires_at_utc, updated_at_utc, revision) VALUES (@id, @json::jsonb, now() + interval '5 minutes', now(), 0);""";
                seed.Parameters.AddWithValue("id", flowId);
                seed.Parameters.AddWithValue("json", stateJson);
                await seed.ExecuteNonQueryAsync();
            });

        }
        finally
        {
            await using var cleanup = await dataSource.OpenConnectionAsync();
            await using var command = cleanup.CreateCommand();
            command.CommandText = $"""DROP SCHEMA IF EXISTS "{schema}" CASCADE;""";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task SqlServerPackageStore_RoundTrips_Expires_Deletes()
    {
        var schema = NewIdentifier("df_sql", 32);
        try
        {
            var store = new SqlServerFlowStateStore(
                Options.Create(new SqlServerDurableFlowOptions
                {
                    ConnectionString = Fixture.SqlServerConnectionString,
                    SchemaName = schema,
                    TableName = "flow_state"
                }));

            await AssertStoreContractAsync(store, seedRawStateAsync: async (flowId, stateJson) =>
            {
                await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
                await connection.OpenAsync();
                await using var seed = connection.CreateCommand();
                seed.CommandText =
                    $"INSERT INTO [{schema}].[flow_state] (flow_id, state_json, expires_at_utc, updated_at_utc, revision) " +
                    "VALUES (@id, @json, DATEADD(MINUTE, 5, SYSUTCDATETIME()), SYSUTCDATETIME(), 0);";
                seed.Parameters.AddWithValue("@id", flowId);
                seed.Parameters.AddWithValue("@json", stateJson);
                await seed.ExecuteNonQueryAsync();
            });
        }
        finally
        {
            await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"""
                IF OBJECT_ID(N'{schema}.flow_state', N'U') IS NOT NULL
                    DROP TABLE [{schema}].[flow_state];
                IF SCHEMA_ID(N'{schema}') IS NOT NULL
                    EXEC(N'DROP SCHEMA [{schema}]');
                """;
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task EFCorePackageStore_RoundTrips_Expires_Deletes_AndSurvivesStorm()
    {
        var schema = NewIdentifier("df_ef", 32);
        var services = new ServiceCollection();
        services.AddSingleton(new EFCoreFlowSchema(schema));
        services.AddDbContextFactory<EFCoreFlowDbContext>(options => options.UseSqlServer(Fixture.SqlServerConnectionString));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        try
        {
            // The store never runs DDL: create the table exactly the way an application migration
            // would — from the ConfigureAsyncResponseDurableFlows model mapping.
            var factory = provider.GetRequiredService<IDbContextFactory<EFCoreFlowDbContext>>();
            await using (var context = await factory.CreateDbContextAsync())
                await context.GetService<IRelationalDatabaseCreator>().CreateTablesAsync();

            var store = new EFCoreFlowStateStore<EFCoreFlowDbContext>(
                provider.GetRequiredService<IServiceScopeFactory>(),
                Options.Create(new EFCoreDurableFlowOptions()));

            await AssertStoreContractAsync(store);

            // Concurrency storm: parallel save/load/delete against the real database must never
            // share a DbContext (the store leases one per operation).
            await Parallel.ForEachAsync(
                Enumerable.Range(0, 64),
                new ParallelOptions { MaxDegreeOfParallelism = 8 },
                async (i, _) =>
                {
                    var flowId = $"flow-storm-{i}";
                    Assert.True(await store.TryCreateAsync(flowId, CreateState(flowId), TimeSpan.FromMinutes(5)));
                    Assert.NotNull(await store.LoadAsync(flowId));
                    Assert.True(await store.TryDeleteAsync(flowId));
                    Assert.Null(await store.LoadAsync(flowId));
                });
        }
        finally
        {
            await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"""
                IF OBJECT_ID(N'{schema}.asyncresponse_flow_state', N'U') IS NOT NULL
                    DROP TABLE [{schema}].[asyncresponse_flow_state];
                IF SCHEMA_ID(N'{schema}') IS NOT NULL
                    EXEC(N'DROP SCHEMA [{schema}]');
                """;
            await command.ExecuteNonQueryAsync();
        }
    }

    private sealed record EFCoreFlowSchema(string Name);

    private sealed class EFCoreFlowDbContext(DbContextOptions<EFCoreFlowDbContext> options, EFCoreFlowSchema schema)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.ConfigureAsyncResponseDurableFlows(schema: schema.Name);
    }

    [Fact]
    public async Task MySqlPackageStore_RoundTrips_Expires_Deletes()
    {
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql", 64);
        try
        {
            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table
                }));

            await AssertStoreContractAsync(store);
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task MySqlPackageStore_RejectsIncompleteExistingSchema()
    {
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql_legacy", 64);
        try
        {
            await using (var connection = new MySqlConnection(Fixture.MySqlConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    CREATE TABLE `{table}` (
                        flow_id varchar(400) NOT NULL PRIMARY KEY,
                        state_json longtext NOT NULL,
                        expires_at_utc datetime(6) NOT NULL,
                        updated_at_utc datetime(6) NOT NULL,
                        INDEX `{table}_expires_idx` (expires_at_utc)
                    );
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table
                }));

            await Assert.ThrowsAsync<MySqlException>(
                () => store.TryCreateAsync("incomplete", CreateState("incomplete"), TimeSpan.FromMinutes(5)));
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task MongoDbPackageStore_RoundTrips_Expires_Deletes()
    {
        await WaitForMongoDbAsync();
        var databaseName = NewIdentifier("df_mongo", 63);
        var client = new MongoClient(Fixture.MongoDbConnectionString);
        try
        {
            var store = new MongoDbFlowStateStore(
                client.GetDatabase(databaseName),
                Options.Create(new MongoDbDurableFlowOptions
                {
                    CollectionName = "flow_state"
                }));

            await AssertStoreContractAsync(store, seedRawStateAsync: async (flowId, stateJson) =>
                await client.GetDatabase(databaseName).GetCollection<BsonDocument>("flow_state").InsertOneAsync(new BsonDocument
                {
                    ["_id"] = flowId,
                    ["state_json"] = stateJson,
                    ["expires_at_utc"] = DateTime.UtcNow.AddMinutes(5),
                    ["updated_at_utc"] = DateTime.UtcNow,
                    ["revision"] = 0L
                }));

            var legacyFlowId = "legacy-mongo-flow";
            await client.GetDatabase(databaseName).GetCollection<BsonDocument>("flow_state").InsertOneAsync(new BsonDocument
            {
                ["_id"] = legacyFlowId,
                ["state_json"] = JsonSerializer.Serialize(CreateState(legacyFlowId)),
                ["expires_at_utc"] = DateTime.UtcNow.AddMinutes(5),
                ["updated_at_utc"] = DateTime.UtcNow
            });
            Assert.Null(await store.LoadAsync(legacyFlowId));
            Assert.False(await store.TryAcquireLeaseAsync(legacyFlowId, "owner", TimeSpan.FromMinutes(1)));
        }
        finally
        {
            await client.DropDatabaseAsync(databaseName);
        }
    }

    [Fact]
    public async Task DynamoDbPackageStore_RoundTrips_Expires_Deletes()
    {
        using var client = CreateDynamoDbClient();
        var table = "AsyncResponseFlowState" + Guid.NewGuid().ToString("N");
        try
        {
            var store = new DynamoDbFlowStateStore(
                client,
                Options.Create(new DynamoDbDurableFlowOptions
                {
                    TableName = table
                }));

            // DynamoDB TTL has whole-second granularity and the store now rounds the expiry epoch
            // UP (never shorter than requested), so the read-filter can consider a 1s-TTL item live
            // for up to ~2s after the save — wait past that worst case.
            await AssertStoreContractAsync(store, TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(2500));

            var legacyFlowId = "legacy-dynamo-flow";
            var now = DateTimeOffset.UtcNow;
            await client.PutItemAsync(new PutItemRequest
            {
                TableName = table,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["flow_id"] = new() { S = legacyFlowId },
                    ["state_json"] = new() { S = JsonSerializer.Serialize(CreateState(legacyFlowId)) },
                    ["expires_at"] = new() { N = now.AddMinutes(5).ToUnixTimeSeconds().ToString(System.Globalization.CultureInfo.InvariantCulture) },
                    ["updated_at"] = new() { N = now.ToUnixTimeSeconds().ToString(System.Globalization.CultureInfo.InvariantCulture) }
                }
            });
            Assert.Null(await store.LoadAsync(legacyFlowId));
            Assert.False(await store.TryAcquireLeaseAsync(legacyFlowId, "owner", TimeSpan.FromMinutes(1)));
        }
        finally
        {
            try
            {
                await client.DeleteTableAsync(table);
            }
            catch (ResourceNotFoundException)
            {
            }
        }
    }



    private async Task WaitForMySqlAsync()
        => await EventuallyAsync(async () =>
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
        });

    private async Task WaitForMongoDbAsync()
        => await EventuallyAsync(async () =>
        {
            var client = new MongoClient(Fixture.MongoDbConnectionString);
            using var cursor = await client.ListDatabaseNamesAsync();
            _ = await cursor.AnyAsync();
        });



    private AmazonDynamoDBClient CreateDynamoDbClient()
        => new(
            new BasicAWSCredentials("test", "test"),
            new AmazonDynamoDBConfig
            {
                ServiceURL = Fixture.LocalStackServiceUrl,
                AuthenticationRegion = "us-east-1"
            });




}
