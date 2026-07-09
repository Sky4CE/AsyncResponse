using Amazon.DynamoDBv2;
using AsyncResponse.DurableFlows.Cosmos;
using AsyncResponse.DurableFlows.DynamoDB;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.DurableFlows.MySql;
using AsyncResponse.DurableFlows.Oracle;
using AsyncResponse.DurableFlows.PostgreSQL;
using AsyncResponse.DurableFlows.Sqlite;
using AsyncResponse.DurableFlows.SqlServer;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Npgsql;
using System.Security.Cryptography;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class DurableFlowStateStoreExampleTests
{
    [Fact]
    public async Task SqlitePackageStore_RoundTrips_Expires_Deletes()
    {
        await using var database = new TempSqliteDatabase();
        var store = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString
        }));

        await AssertStoreContractAsync(store);
    }

    [Fact]
    public async Task SqlitePackageStore_RunsDurableFlowEndToEnd()
    {
        await using var database = new TempSqliteDatabase();
        await RunFlowWithStoreAsync(builder => builder.WithSqliteDurableFlows(options =>
        {
            options.ConnectionString = database.ConnectionString;
        }));
    }

    [Fact]
    public async Task SqlitePackageStore_ConcurrentSaveLoadDeleteStorm()
    {
        await using var database = new TempSqliteDatabase();
        var store = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString + ";Default Timeout=10"
        }));

        await Parallel.ForEachAsync(
            Enumerable.Range(0, 200),
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

    [Theory]
    [MemberData(nameof(PackageRegistrations))]
    public void DurableFlowPackages_RegisterScopedFlowStateStore(
        string packageName,
        Type storeType,
        Action<IServiceCollection, AsyncResponseRegistrationBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        var builder = services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport();
        configure(services, builder);

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using var scope = provider.CreateScope();

        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();
        Assert.IsType(storeType, store);
        Assert.NotNull(packageName);
    }

    public static TheoryData<string, Type, Action<IServiceCollection, AsyncResponseRegistrationBuilder>> PackageRegistrations()
        => new()
        {
            {
                "AsyncResponse.DurableFlows.SqlServer",
                typeof(SqlServerFlowStateStore),
                (_, builder) => builder.WithSqlServerDurableFlows(options =>
                {
                    options.ConnectionString = "Server=localhost;Database=asyncresponse_tests;User ID=sa;Password=unused;TrustServerCertificate=True";
                    options.AutoCreateSchema = false;
                })
            },
            {
                "AsyncResponse.DurableFlows.PostgreSQL",
                typeof(PostgreSqlFlowStateStore),
                (services, builder) =>
                {
                    services.AddSingleton(_ => NpgsqlDataSource.Create("Host=localhost;Username=postgres;Password=postgres;Database=asyncresponse_tests;Pooling=false"));
                    builder.WithPostgreSqlDurableFlows(options => options.AutoCreateSchema = false);
                }
            },
            {
                "AsyncResponse.DurableFlows.MySql",
                typeof(MySqlFlowStateStore),
                (_, builder) => builder.WithMySqlDurableFlows(options =>
                {
                    options.ConnectionString = "Server=localhost;Database=asyncresponse_tests;User ID=root;Password=unused;";
                    options.AutoCreateSchema = false;
                })
            },
            {
                "AsyncResponse.DurableFlows.Sqlite",
                typeof(SqliteFlowStateStore),
                (_, builder) => builder.WithSqliteDurableFlows(options =>
                {
                    options.ConnectionString = "Data Source=:memory:";
                    options.AutoCreateSchema = false;
                })
            },
            {
                "AsyncResponse.DurableFlows.Oracle",
                typeof(OracleFlowStateStore),
                (_, builder) => builder.WithOracleDurableFlows(options =>
                {
                    options.ConnectionString = "User Id=asyncresponse;Password=unused;Data Source=localhost/XEPDB1";
                    options.AutoCreateSchema = false;
                })
            },
            {
                "AsyncResponse.DurableFlows.MongoDB",
                typeof(MongoDbFlowStateStore),
                (_, builder) => builder.WithMongoDbDurableFlows(options =>
                {
                    options.ConnectionString = "mongodb://localhost:27017";
                    options.DatabaseName = "asyncresponse_tests";
                    options.AutoCreateIndexes = false;
                })
            },
            {
                "AsyncResponse.DurableFlows.Cosmos",
                typeof(CosmosFlowStateStore),
                (services, builder) =>
                {
                    services.AddSingleton(_ => new CosmosClient("https://localhost:8081/", Convert.ToBase64String(RandomNumberGenerator.GetBytes(64))));
                    builder.WithCosmosDurableFlows(options =>
                    {
                        options.DatabaseName = "asyncresponse_tests";
                        options.AutoCreateContainer = false;
                    });
                }
            },
            {
                "AsyncResponse.DurableFlows.DynamoDB",
                typeof(DynamoDbFlowStateStore),
                (services, builder) =>
                {
                    services.AddSingleton(Mock.Of<IAmazonDynamoDB>());
                    builder.WithDynamoDbDurableFlows(options => options.AutoCreateTable = false);
                }
            }
        };

    private static async Task AssertStoreContractAsync(IFlowStateStore store)
    {
        var state = CreateState("flow-example");

        await store.SaveAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));

        var loaded = await store.LoadAsync(state.FlowId!);
        Assert.NotNull(loaded);
        Assert.Equal(FlowRunStatus.Running, loaded!.Status);
        Assert.True(loaded.Steps!["step-a"].Completed);
        Assert.Equal("7", loaded.Values!["tenant"]);

        state.Status = FlowRunStatus.Succeeded;
        state.LastMessage = "done";
        await store.SaveAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(state.FlowId!))!.Status);

        await store.SaveAsync("expired-flow", CreateState("expired-flow"), TimeSpan.FromMilliseconds(1));
        await Task.Delay(30);
        Assert.Null(await store.LoadAsync("expired-flow"));

        Assert.True(await store.TryDeleteAsync(state.FlowId!));
        Assert.Null(await store.LoadAsync(state.FlowId!));
        Assert.False(await store.TryDeleteAsync(state.FlowId!));
    }

    private static async Task RunFlowWithStoreAsync(Action<AsyncResponseRegistrationBuilder> configureStore)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<FlowProbe>();
        services.AddScoped<TestOnboardingFlow>();

        var builder = services.AddAsyncResponse()
            .WithInMemoryChannel(options =>
            {
                options.DefaultTimeout = TimeSpan.FromSeconds(10);
                options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
            })
            .WithInMemoryTransport();
        configureStore(builder);

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<FlowProbe>();

        var flowId = await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new TestFlowInput(7));
        var run = executor.ExecuteAsync(flowId);
        var correlationId = await probe.TriggerFired.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Running, Message = "halfway" }, correlationId);
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, correlationId);
        await run.WaitAsync(TimeSpan.FromSeconds(5));

        var state = await flows.GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Succeeded, state!.Status);
        Assert.True(state.Steps!["compute-stamp"].Completed);
        Assert.True(state.Steps["remote-op"].Completed);
        Assert.True(state.Steps["notify"].Completed);
    }

    private static FlowState CreateState(string flowId)
        => new()
        {
            FlowId = flowId,
            FlowTypeName = typeof(TestOnboardingFlow).FullName,
            InputTypeName = typeof(TestFlowInput).FullName,
            InputJson = JsonSerializer.Serialize(new TestFlowInput(7)),
            Status = FlowRunStatus.Running,
            LastMessage = "started",
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow,
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
            {
                ["step-a"] = new() { Completed = true, ResultJson = "123", CompletedAtUtc = DateTime.UtcNow }
            },
            Values = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["tenant"] = "7"
            }
        };

    private sealed class TempSqliteDatabase : IAsyncDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"ar-flow-state-{Guid.NewGuid():N}.db");

        public string ConnectionString => $"Data Source={_path}";

        public ValueTask DisposeAsync()
        {
            File.Delete(_path);
            return ValueTask.CompletedTask;
        }
    }
}
