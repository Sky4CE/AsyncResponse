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
using MongoDB.Driver;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

[Collection(IntegrationCollection.Name)]
public sealed class DurableFlowStateStorePackageIntegrationTests(IntegrationFixture fixture) : IntegrationTestBase(fixture)
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

            await AssertStoreContractAsync(store);
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

            await AssertStoreContractAsync(store);
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
                    await store.SaveAsync(flowId, CreateState(flowId), TimeSpan.FromMinutes(5));
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

            await AssertStoreContractAsync(store);
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

    [Fact]
    public async Task OraclePackageStore_RoundTrips_Expires_Deletes_WhenConnectionStringIsConfigured()
    {
        var connectionString = Environment.GetEnvironmentVariable("ASYNCRESPONSE_ITEST_ORACLE_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString))
            Assert.Skip("Set ASYNCRESPONSE_ITEST_ORACLE_CONNECTION_STRING to run the Oracle durable-flow store contract test.");

        await WaitForOracleAsync(connectionString);
        var table = NewIdentifier("DF_ORACLE", 18).ToUpperInvariant();
        try
        {
            var store = new OracleFlowStateStore(
                Options.Create(new OracleDurableFlowOptions
                {
                    ConnectionString = connectionString,
                    TableName = table
                }));

            await AssertStoreContractAsync(store);
        }
        finally
        {
            await using var connection = new OracleConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE {table} PURGE";
            try
            {
                await command.ExecuteNonQueryAsync();
            }
            catch (OracleException ex) when (ex.Number == 942)
            {
            }
        }
    }

    [Fact]
    public async Task CosmosPackageStore_RoundTrips_Expires_Deletes_WhenConnectionStringIsConfigured()
    {
        var connectionString = Environment.GetEnvironmentVariable("ASYNCRESPONSE_ITEST_COSMOS_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString))
            Assert.Skip("Set ASYNCRESPONSE_ITEST_COSMOS_CONNECTION_STRING to run the Cosmos DB durable-flow store contract test.");

        var databaseName = NewIdentifier("df_cosmos", 63);
        using var client = new CosmosClient(connectionString, GetCosmosClientOptions(connectionString));
        await WaitForCosmosAsync(client);
        try
        {
            var store = new CosmosFlowStateStore(
                client,
                Options.Create(new CosmosDurableFlowOptions
                {
                    DatabaseName = databaseName,
                    ContainerName = "flow_state"
                }));

            await AssertStoreContractAsync(store);
        }
        finally
        {
            try
            {
                await client.GetDatabase(databaseName).DeleteAsync();
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
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

    private async Task WaitForOracleAsync(string connectionString)
        => await EventuallyAsync(async () =>
        {
            await using var connection = new OracleConnection(connectionString);
            await connection.OpenAsync();
        });

    private async Task WaitForCosmosAsync(CosmosClient client)
        => await EventuallyAsync(async () => _ = await client.ReadAccountAsync());

    private static CosmosClientOptions GetCosmosClientOptions(string connectionString)
    {
        var options = new CosmosClientOptions();
        if (!IsLocalCosmosEndpoint(connectionString))
            return options;

        options.ConnectionMode = ConnectionMode.Gateway;
        options.LimitToEndpoint = true;
        options.HttpClientFactory = () => new HttpClient(new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
        });
        return options;
    }

    private static bool IsLocalCosmosEndpoint(string connectionString)
    {
        foreach (var segment in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var pair = segment.Split('=', 2, StringSplitOptions.TrimEntries);
            if (pair.Length == 2 &&
                pair[0].Equals("AccountEndpoint", StringComparison.OrdinalIgnoreCase) &&
                Uri.TryCreate(pair[1], UriKind.Absolute, out var endpoint))
            {
                return endpoint.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase) ||
                       endpoint.Host is "127.0.0.1" or "::1";
            }
        }

        return false;
    }

    private AmazonDynamoDBClient CreateDynamoDbClient()
        => new(
            new BasicAWSCredentials("test", "test"),
            new AmazonDynamoDBConfig
            {
                ServiceURL = Fixture.LocalStackServiceUrl,
                AuthenticationRegion = "us-east-1"
            });

    private static async Task EventuallyAsync(Func<Task> action)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await action();
                return;
            }
            catch (Exception ex)
            {
                last = ex;
                await Task.Delay(500);
            }
        }

        throw new TimeoutException("The backing store did not become ready within 30 seconds.", last);
    }

    private static async Task AssertStoreContractAsync(
        IFlowStateStore store,
        TimeSpan? expiryTtl = null,
        TimeSpan? expiryDelay = null)
    {
        var state = CreateState("flow-itest");

        await store.SaveAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));
        var loaded = await store.LoadAsync(state.FlowId!);
        Assert.NotNull(loaded);
        Assert.Equal(FlowRunStatus.Running, loaded!.Status);
        Assert.True(loaded.Steps!["step-a"].Completed);

        state.Status = FlowRunStatus.Succeeded;
        state.LastMessage = "done";
        await store.SaveAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(state.FlowId!))!.Status);

        await store.SaveAsync("expired-flow", CreateState("expired-flow"), expiryTtl ?? TimeSpan.FromMilliseconds(1));
        await Task.Delay(expiryDelay ?? TimeSpan.FromMilliseconds(30));
        Assert.Null(await store.LoadAsync("expired-flow"));

        Assert.True(await store.TryDeleteAsync(state.FlowId!));
        Assert.Null(await store.LoadAsync(state.FlowId!));
        Assert.False(await store.TryDeleteAsync(state.FlowId!));
    }

    private static FlowState CreateState(string flowId)
        => new()
        {
            FlowId = flowId,
            FlowTypeName = typeof(DurableFlowStateStorePackageIntegrationTests).FullName,
            InputTypeName = typeof(int).FullName,
            InputJson = JsonSerializer.Serialize(7),
            Status = FlowRunStatus.Running,
            LastMessage = "started",
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow,
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
            {
                ["step-a"] = new() { Completed = true, ResultJson = "123", CompletedAtUtc = DateTime.UtcNow }
            }
        };

    private static string NewIdentifier(string prefix, int maxLength)
    {
        var identifier = $"{prefix}_{Guid.NewGuid():N}";
        return identifier.Length <= maxLength ? identifier : identifier[..maxLength];
    }
}
