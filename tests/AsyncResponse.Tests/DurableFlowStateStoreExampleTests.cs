using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using AsyncResponse.DurableFlows.Cosmos;
using AsyncResponse.DurableFlows.DynamoDB;
using AsyncResponse.DurableFlows.EFCore;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.DurableFlows.MySql;
using AsyncResponse.DurableFlows.Oracle;
using AsyncResponse.DurableFlows.PostgreSQL;
using AsyncResponse.DurableFlows.Sqlite;
using AsyncResponse.DurableFlows.SqlServer;
using Microsoft.Azure.Cosmos;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
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
    private static TableDescription ValidDynamoTable()
        => new()
        {
            TableStatus = TableStatus.ACTIVE,
            KeySchema = [new KeySchemaElement("flow_id", KeyType.HASH)],
            AttributeDefinitions = [new AttributeDefinition("flow_id", ScalarAttributeType.S)]
        };

    [Fact]
    public async Task DynamoDbPackageStore_RejectsTtlConfiguredOnWrongAttribute()
    {
        var client = new Mock<IAmazonDynamoDB>();
        client
            .Setup(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTableResponse
            {
                Table = ValidDynamoTable()
            });
        client
            .Setup(database => database.DescribeTimeToLiveAsync(
                It.Is<DescribeTimeToLiveRequest>(request => request.TableName == "flows"),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTimeToLiveResponse
            {
                TimeToLiveDescription = new TimeToLiveDescription
                {
                    AttributeName = "wrong_expiry",
                    TimeToLiveStatus = TimeToLiveStatus.ENABLED
                }
            });
        var store = new DynamoDbFlowStateStore(client.Object, Options.Create(new DynamoDbDurableFlowOptions
        {
            TableName = "flows",
            TimeToLiveAttributeName = "expires_at"
        }));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(1)));

        Assert.Contains("wrong_expiry", ex.Message, StringComparison.Ordinal);
        Assert.Contains("expires_at", ex.Message, StringComparison.Ordinal);
        client.Verify(database => database.PutItemAsync(
            It.IsAny<PutItemRequest>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task DynamoDbPackageStore_RejectsWrongManualTableKey()
    {
        var client = new Mock<IAmazonDynamoDB>();
        client
            .Setup(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTableResponse
            {
                Table = new TableDescription
                {
                    TableStatus = TableStatus.ACTIVE,
                    KeySchema = [new KeySchemaElement("wrong_key", KeyType.HASH)],
                    AttributeDefinitions = [new AttributeDefinition("wrong_key", ScalarAttributeType.S)]
                }
            });
        var store = new DynamoDbFlowStateStore(client.Object, Options.Create(new DynamoDbDurableFlowOptions
        {
            TableName = "flows",
            AutoCreateTable = false
        }));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.LoadAsync("flow"));

        Assert.Contains("flow_id", exception.Message, StringComparison.Ordinal);
        client.Verify(database => database.GetItemAsync(
            It.IsAny<GetItemRequest>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

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
    public async Task SqlitePackageStore_AllowsOnlyOneExecutorAcrossServiceProviders()
    {
        await using var database = new TempSqliteDatabase();
        var probe = new LeaseExecutionProbe();
        await using var providerA = BuildLeaseProvider(database.ConnectionString, probe);
        await using var providerB = BuildLeaseProvider(database.ConnectionString, probe);

        var flowId = await providerA.GetRequiredService<IDurableFlows>()
            .StartAsync<LeaseGuardedFlow, TestFlowInput>(new TestFlowInput(1), "leased-flow");
        var runA = providerA.GetRequiredService<IDurableFlowExecutor>().ExecuteAsync(flowId);
        var runB = providerB.GetRequiredService<IDurableFlowExecutor>().ExecuteAsync(flowId);

        await probe.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(100);
        Assert.Equal(1, probe.Runs);

        probe.Release.TrySetResult();
        await Task.WhenAll(runA, runB).WaitAsync(TimeSpan.FromSeconds(5));

        var state = await providerA.GetRequiredService<IDurableFlows>().GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Succeeded, state!.Status);
        Assert.Equal(1, state.Attempts);
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
                Assert.True(await store.TryCreateAsync(flowId, CreateState(flowId), TimeSpan.FromMinutes(5)));
                Assert.NotNull(await store.LoadAsync(flowId));
                Assert.True(await store.TryDeleteAsync(flowId));
                Assert.Null(await store.LoadAsync(flowId));
            });
    }

    [Fact]
    public async Task SqlitePackageStore_AtomicCreateRevisionAndLeaseContract()
    {
        await using var database = new TempSqliteDatabase();
        var store = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString + ";Default Timeout=10"
        }));

        var creates = await Task.WhenAll(Enumerable.Range(0, 32).Select(_ =>
            store.TryCreateAsync("atomic-flow", CreateState("atomic-flow"), TimeSpan.FromMinutes(5))));
        Assert.Equal(1, creates.Count(static created => created));

        var state = await store.LoadAsync("atomic-flow");
        Assert.NotNull(state);
        Assert.Equal(0, state!.Revision);

        Assert.True(await store.TryAcquireLeaseAsync("atomic-flow", "owner-a", TimeSpan.FromMinutes(1)));
        Assert.False(await store.TryAcquireLeaseAsync("atomic-flow", "owner-b", TimeSpan.FromMinutes(1)));

        state.LastMessage = "updated";
        state.Revision = 1;
        Assert.False(await store.TryUpdateAsync("atomic-flow", state, 0, TimeSpan.FromMinutes(5), "owner-b"));
        Assert.True(await store.TryUpdateAsync("atomic-flow", state, 0, TimeSpan.FromMinutes(5), "owner-a"));
        Assert.False(await store.TryUpdateAsync("atomic-flow", state, 0, TimeSpan.FromMinutes(5), "owner-a"));
        Assert.Equal(1, (await store.LoadAsync("atomic-flow"))!.Revision);

        Assert.False(await store.TryRenewLeaseAsync("atomic-flow", "owner-b", TimeSpan.FromMinutes(1)));
        Assert.True(await store.TryRenewLeaseAsync("atomic-flow", "owner-a", TimeSpan.FromMinutes(1)));
        await store.ReleaseLeaseAsync("atomic-flow", "owner-a");
        Assert.True(await store.TryAcquireLeaseAsync("atomic-flow", "owner-b", TimeSpan.FromMinutes(1)));

        await store.TryCreateAsync("expired-recreate", CreateState("expired-recreate"), TimeSpan.FromMilliseconds(1));
        await Task.Delay(30);
        var replacement = CreateState("expired-recreate");
        Assert.True(await store.TryCreateAsync("expired-recreate", replacement, TimeSpan.FromMinutes(5)));
        Assert.Equal("expired-recreate", (await store.LoadAsync("expired-recreate"))!.FlowId);
    }

    [Fact]
    public async Task SqlitePackageStore_RejectsIncompleteExistingSchema()
    {
        await using var database = new TempSqliteDatabase();
        await using (var connection = new SqliteConnection(database.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                """
                CREATE TABLE asyncresponse_flow_state (
                    flow_id TEXT NOT NULL PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    expires_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                );
                """;
            await command.ExecuteNonQueryAsync();
        }

        var store = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString
        }));
        await Assert.ThrowsAsync<SqliteException>(
            () => store.TryCreateAsync("incomplete", CreateState("incomplete"), TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task SqlitePackageStore_PhysicallyPrunesExpiredRows()
    {
        await using var database = new TempSqliteDatabase();
        var store = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString,
            PruneInterval = TimeSpan.Zero // prune on every create
        }));

        Assert.True(await store.TryCreateAsync("expired-flow", CreateState("expired-flow"), TimeSpan.FromMilliseconds(1)));
        await Task.Delay(30);
        Assert.True(await store.TryCreateAsync("live-flow", CreateState("live-flow"), TimeSpan.FromMinutes(5)));

        // Regression guard: expired rows must be physically deleted by the opportunistic prune,
        // not merely filtered out on load — otherwise the table grows forever.
        await using var connection = new SqliteConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM \"asyncresponse_flow_state\" WHERE flow_id = 'expired-flow';";
        Assert.Equal(0L, (long)(await command.ExecuteScalarAsync())!);

        Assert.NotNull(await store.LoadAsync("live-flow"));
    }

    [Theory]
    [MemberData(nameof(PackageRegistrations))]
    public async Task DurableFlowPackages_RegisterSingletonFlowStateStore(
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

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using var scope = provider.CreateScope();
        using var otherScope = provider.CreateScope();

        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();
        var commonOptions = provider.GetRequiredService<DurableFlowOptions>();
        Assert.IsType(storeType, store);
        Assert.IsAssignableFrom<IFlowStateStore>(store);
        Assert.NotNull(packageName);
        Assert.Equal(TimeSpan.FromDays(13), commonOptions.StateExpiry);
        Assert.NotEqual(typeof(DurableFlowOptions), commonOptions.GetType());

        // Regression guard: package stores must be process-wide singletons. The executor resolves
        // the store from a fresh scope per flow execution, so a scoped store would re-run schema
        // provisioning (DDL / Cosmos metadata / DynamoDB control-plane calls) on every single run.
        Assert.Same(store, otherScope.ServiceProvider.GetRequiredService<IFlowStateStore>());
        Assert.Same(store, scope.ServiceProvider.GetRequiredService(storeType));
    }

    public static TheoryData<string, Type, Action<IServiceCollection, AsyncResponseRegistrationBuilder>> PackageRegistrations()
        => new()
        {
            {
                "AsyncResponse.DurableFlows.SqlServer",
                typeof(SqlServerFlowStateStore),
                (_, builder) => builder.WithSqlServerDurableFlows(options =>
                {
                    ConfigureCommonOptions(options);
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
                    builder.WithPostgreSqlDurableFlows(options =>
                    {
                        ConfigureCommonOptions(options);
                        options.AutoCreateSchema = false;
                    });
                }
            },
            {
                "AsyncResponse.DurableFlows.MySql",
                typeof(MySqlFlowStateStore),
                (_, builder) => builder.WithMySqlDurableFlows(options =>
                {
                    ConfigureCommonOptions(options);
                    options.ConnectionString = "Server=localhost;Database=asyncresponse_tests;User ID=root;Password=unused;";
                    options.AutoCreateSchema = false;
                })
            },
            {
                "AsyncResponse.DurableFlows.Sqlite",
                typeof(SqliteFlowStateStore),
                (_, builder) => builder.WithSqliteDurableFlows(options =>
                {
                    ConfigureCommonOptions(options);
                    options.ConnectionString = "Data Source=:memory:";
                    options.AutoCreateSchema = false;
                })
            },
            {
                "AsyncResponse.DurableFlows.Oracle",
                typeof(OracleFlowStateStore),
                (_, builder) => builder.WithOracleDurableFlows(options =>
                {
                    ConfigureCommonOptions(options);
                    options.ConnectionString = "User Id=asyncresponse;Password=unused;Data Source=localhost/XEPDB1";
                    options.AutoCreateSchema = false;
                })
            },
            {
                "AsyncResponse.DurableFlows.MongoDB",
                typeof(MongoDbFlowStateStore),
                (_, builder) => builder.WithMongoDbDurableFlows(options =>
                {
                    ConfigureCommonOptions(options);
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
                        ConfigureCommonOptions(options);
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
                    builder.WithDynamoDbDurableFlows(options =>
                    {
                        ConfigureCommonOptions(options);
                        options.AutoCreateTable = false;
                    });
                }
            },
            {
                "AsyncResponse.DurableFlows.EFCore",
                typeof(EFCoreFlowStateStore<TestFlowDbContext>),
                (services, builder) =>
                {
                    services.AddDbContext<TestFlowDbContext>(options => options.UseSqlite("Data Source=:memory:"));
                    builder.WithEFCoreDurableFlows<TestFlowDbContext>(ConfigureCommonOptions);
                }
            }
        };

    private static void ConfigureCommonOptions(DurableFlowOptions options)
    {
        options.StateExpiry = TimeSpan.FromDays(13);
        options.ExecutionLeaseDuration = TimeSpan.FromMinutes(2);
        options.ExecutionLeaseRenewInterval = TimeSpan.FromSeconds(30);
    }

    private static async Task AssertStoreContractAsync(IFlowStateStore store)
    {
        var state = CreateState("flow-example");

        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));

        var loaded = await store.LoadAsync(state.FlowId!);
        Assert.NotNull(loaded);
        Assert.Equal(FlowRunStatus.Running, loaded!.Status);
        Assert.True(loaded.Steps!["step-a"].Completed);
        Assert.Equal("7", loaded.Values!["tenant"]);

        state.Status = FlowRunStatus.Succeeded;
        state.LastMessage = "done";
        state.Revision = 1;
        Assert.True(await store.TryUpdateAsync(state.FlowId!, state, expectedRevision: 0, TimeSpan.FromMinutes(5)));
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(state.FlowId!))!.Status);

        Assert.True(await store.TryCreateAsync("expired-flow", CreateState("expired-flow"), TimeSpan.FromMilliseconds(1)));
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

    private static ServiceProvider BuildLeaseProvider(string connectionString, LeaseExecutionProbe probe)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(probe);
        services.AddScoped<LeaseGuardedFlow>();
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithSqliteDurableFlows(options => options.ConnectionString = connectionString + ";Default Timeout=10");
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
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

        // Pooling=False: every closed connection releases its file handle immediately, so
        // cleanup can delete the temp database on Windows and no process-wide pool state
        // couples parallel tests (SqliteConnection.ClearAllPools() here previously flushed
        // OTHER tests' idle connections mid-run and manifested as 'database is locked').
        public string ConnectionString => $"Data Source={_path};Pooling=False";

        public ValueTask DisposeAsync()
        {
            // Pooling is disabled in the connection string, so the last closed context already
            // released the file handle; deletion stays best-effort temp hygiene regardless.
            foreach (var file in new[] { _path, _path + "-wal", _path + "-shm" })
            {
                try
                {
                    File.Delete(file);
                }
                catch (IOException)
                {
                }
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class LeaseExecutionProbe
    {
        private int _runs;
        public int Runs => Volatile.Read(ref _runs);
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Enter()
        {
            Interlocked.Increment(ref _runs);
            Started.TrySetResult();
        }
    }

    private sealed class LeaseGuardedFlow(LeaseExecutionProbe probe) : IDurableFlow<TestFlowInput>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, TestFlowInput input)
        {
            probe.Enter();
            await probe.Release.Task.ConfigureAwait(false);
        }
    }

}
