using AsyncResponse.DurableFlows.EFCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>The application-owned context the EFCore store rides in for these tests.</summary>
internal sealed class TestFlowDbContext(DbContextOptions<TestFlowDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.ConfigureAsyncResponseDurableFlows();
}

/// <summary>A context that forgot to call <c>ConfigureAsyncResponseDurableFlows()</c>.</summary>
internal sealed class UnmappedFlowDbContext(DbContextOptions<UnmappedFlowDbContext> options) : DbContext(options);

public sealed class EFCoreDurableFlowStateStoreTests
{
    [Fact]
    public async Task EFCoreStore_RoundTrips_Expires_Deletes_WithScopedDbContext()
    {
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        await using var provider = BuildScopedContextProvider(database.ConnectionString);

        await AssertStoreContractAsync(CreateStore(provider));
    }

    [Fact]
    public async Task EFCoreStore_RoundTrips_Expires_Deletes_WithDbContextFactory()
    {
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        await using var provider = BuildFactoryProvider(database.ConnectionString);

        await AssertStoreContractAsync(CreateStore(provider));
    }

    [Fact]
    public async Task EFCoreStore_RunsDurableFlowEndToEnd()
    {
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();

        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<FlowProbe>();
        services.AddScoped<TestOnboardingFlow>();
        services.AddDbContext<TestFlowDbContext>(options => options.UseSqlite(database.ConnectionString));

        services.AddAsyncResponse()
            .WithInMemoryChannel(options =>
            {
                options.DefaultTimeout = TimeSpan.FromSeconds(10);
                options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
            })
            .WithInMemoryTransport()
            .WithEFCoreDurableFlows<TestFlowDbContext>();

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

    [Fact]
    public async Task EFCoreStore_ConcurrentSaveLoadDeleteStorm_WithScopedDbContext()
    {
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        await using var provider = BuildScopedContextProvider(database.ConnectionString + ";Default Timeout=10");

        await RunStormAsync(CreateStore(provider));
    }

    [Fact]
    public async Task EFCoreStore_ConcurrentSaveLoadDeleteStorm_WithDbContextFactory()
    {
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        await using var provider = BuildFactoryProvider(database.ConnectionString + ";Default Timeout=10");

        await RunStormAsync(CreateStore(provider));
    }

    [Fact]
    public async Task EFCoreStore_ConcurrentCreatesOfSameFlowIds_LoseTheInsertRaceGracefully()
    {
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        await using var provider = BuildFactoryProvider(database.ConnectionString + ";Default Timeout=10");
        var store = CreateStore(provider);

        // Eight flow ids, each hammered by eight concurrent creates: losers return false instead of
        // surfacing the provider's unique-key exception.
        await Parallel.ForEachAsync(
            Enumerable.Range(0, 64),
            new ParallelOptions { MaxDegreeOfParallelism = 8 },
            async (i, _) =>
            {
                var flowId = $"flow-race-{i % 8}";
                await store.TryCreateAsync(flowId, CreateState(flowId), TimeSpan.FromMinutes(5));
            });

        for (var i = 0; i < 8; i++)
            Assert.NotNull(await store.LoadAsync($"flow-race-{i}"));
    }

    [Fact]
    public async Task EFCoreStore_DoesNotMisreportNonDuplicateInsertFailureAsExistingFlow()
    {
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        await database.ExecuteSqlAsync(
            """
            CREATE TRIGGER reject_flow_insert
            BEFORE INSERT ON asyncresponse_flow_state
            WHEN NEW.flow_id = 'rejected-flow'
            BEGIN
                SELECT RAISE(ABORT, 'insert rejected by test trigger');
            END;
            """);
        await using var provider = BuildFactoryProvider(database.ConnectionString);
        var store = CreateStore(provider);

        await Assert.ThrowsAsync<DbUpdateException>(
            () => store.TryCreateAsync(
                "rejected-flow",
                CreateState("rejected-flow"),
                TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task EFCoreStore_RevisionAndLeaseContract()
    {
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        await using var provider = BuildFactoryProvider(database.ConnectionString);
        IFlowStateStore store = CreateStore(provider);

        var state = CreateState("concurrent-flow");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        Assert.False(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        Assert.True(await store.TryAcquireLeaseAsync(state.FlowId!, "owner-a", TimeSpan.FromMinutes(1)));
        Assert.False(await store.TryAcquireLeaseAsync(state.FlowId!, "owner-b", TimeSpan.FromMinutes(1)));

        state.Revision = 1;
        state.LastMessage = "updated";
        Assert.True(await store.TryUpdateAsync(state.FlowId!, state, 0, TimeSpan.FromMinutes(5), "owner-a"));
        Assert.False(await store.TryUpdateAsync(state.FlowId!, state, 0, TimeSpan.FromMinutes(5), "owner-a"));
        Assert.Equal(1, (await store.LoadAsync(state.FlowId!))!.Revision);

        await store.ReleaseLeaseAsync(state.FlowId!, "owner-a");
        Assert.True(await store.TryAcquireLeaseAsync(state.FlowId!, "owner-b", TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.TryAcquireLeaseAsync(state.FlowId!, "owner-b", TimeSpan.Zero));
    }

    [Fact]
    public async Task EFCoreStore_PhysicallyPrunesExpiredRows()
    {
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        await using var provider = BuildFactoryProvider(database.ConnectionString);
        var store = CreateStore(provider, pruneInterval: TimeSpan.Zero); // prune on every create

        Assert.True(await store.TryCreateAsync("expired-flow", CreateState("expired-flow"), TimeSpan.FromMilliseconds(1)));
        await Task.Delay(30);
        Assert.True(await store.TryCreateAsync("live-flow", CreateState("live-flow"), TimeSpan.FromMinutes(5)));

        // Regression guard: expired rows must be physically deleted by the opportunistic prune,
        // not merely filtered out on load — otherwise the table grows forever.
        Assert.Equal(0, await database.CountRowsAsync("expired-flow"));
        Assert.NotNull(await store.LoadAsync("live-flow"));
    }

    [Fact]
    public async Task EFCoreStore_ThrottledPrune_TreatsExpiredAsAbsentWithoutDeleting()
    {
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        await using var provider = BuildFactoryProvider(database.ConnectionString);
        var store = CreateStore(provider, pruneInterval: TimeSpan.FromHours(1));

        // The first create always prunes (the throttle starts elapsed); this one arms the throttle.
        Assert.True(await store.TryCreateAsync("expired-flow", CreateState("expired-flow"), TimeSpan.FromMilliseconds(1)));
        await Task.Delay(30);
        Assert.True(await store.TryCreateAsync("live-flow", CreateState("live-flow"), TimeSpan.FromMinutes(5)));

        // Within the prune interval the expired row survives physically, but loads must already
        // treat it as absent — expiry correctness never depends on pruning.
        Assert.Equal(1, await database.CountRowsAsync("expired-flow"));
        Assert.Null(await store.LoadAsync("expired-flow"));
    }

    [Fact]
    public async Task EFCoreStore_Throws_WhenModelDoesNotMapTheStateTable()
    {
        await using var database = new TempSqliteDatabase();
        var services = new ServiceCollection();
        services.AddDbContext<UnmappedFlowDbContext>(options => options.UseSqlite(database.ConnectionString));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        var store = new EFCoreFlowStateStore<UnmappedFlowDbContext>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new EFCoreDurableFlowOptions()));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => store.LoadAsync("flow-any"));
        Assert.Contains(nameof(EFCoreDurableFlowModelBuilderExtensions.ConfigureAsyncResponseDurableFlows), exception.Message);
    }

    [Fact]
    public async Task EFCoreStore_DisposesFactoryContext_WhenModelDoesNotMapTheStateTable()
    {
        await using var database = new TempSqliteDatabase();
        var services = new ServiceCollection();
        services.AddDbContextFactory<UnmappedFlowDbContext>(options => options.UseSqlite(database.ConnectionString));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var store = new EFCoreFlowStateStore<UnmappedFlowDbContext>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new EFCoreDurableFlowOptions()));

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.LoadAsync("flow-any"));
    }

    private static ServiceProvider BuildScopedContextProvider(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddDbContext<TestFlowDbContext>(options => options.UseSqlite(connectionString));
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static ServiceProvider BuildFactoryProvider(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddDbContextFactory<TestFlowDbContext>(options => options.UseSqlite(connectionString));
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static EFCoreFlowStateStore<TestFlowDbContext> CreateStore(
        IServiceProvider provider,
        TimeSpan? pruneInterval = null)
        => new(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(pruneInterval is { } interval
                ? new EFCoreDurableFlowOptions { PruneInterval = interval }
                : new EFCoreDurableFlowOptions()));

    private static async Task RunStormAsync(IFlowStateStore store)
    {
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
        Assert.True(await store.TryUpdateAsync(state.FlowId!, state, 0, TimeSpan.FromMinutes(5)));
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(state.FlowId!))!.Status);

        Assert.True(await store.TryCreateAsync("expired-flow", CreateState("expired-flow"), TimeSpan.FromMilliseconds(1)));
        await Task.Delay(30);
        Assert.Null(await store.LoadAsync("expired-flow"));

        Assert.True(await store.TryDeleteAsync(state.FlowId!));
        Assert.Null(await store.LoadAsync(state.FlowId!));
        Assert.False(await store.TryDeleteAsync(state.FlowId!));
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
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"ar-efcore-flow-state-{Guid.NewGuid():N}.db");

        // Pooling=False: every closed connection releases its file handle immediately, so
        // cleanup can delete the temp database on Windows and no process-wide pool state
        // couples parallel tests (SqliteConnection.ClearAllPools() here previously flushed
        // OTHER tests' idle connections mid-run and manifested as 'database is locked').
        public string ConnectionString => $"Data Source={_path};Pooling=False";

        /// <summary>Creates the state table — the store itself never runs DDL.</summary>
        public async Task EnsureSchemaAsync()
        {
            await using var context = CreateContext();
            await context.Database.EnsureCreatedAsync();

            // WAL keeps readers from blocking behind writers; without it the concurrent-storm
            // test hits SQLITE_BUSY on slow CI disks. The EFCore store is provider-agnostic, so
            // journal mode is the schema owner's job — which in these tests is this helper
            // (persistent per database file, one-time cost).
            await context.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");
        }

        public async Task<int> CountRowsAsync(string flowId)
        {
            await using var context = CreateContext();
            return await context.Set<DurableFlowStateRecord>().CountAsync(r => r.FlowId == flowId);
        }

        public async Task ExecuteSqlAsync(string sql)
        {
            await using var context = CreateContext();
            await context.Database.ExecuteSqlRawAsync(sql);
        }

        private TestFlowDbContext CreateContext()
            => new(new DbContextOptionsBuilder<TestFlowDbContext>().UseSqlite(ConnectionString).Options);

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
}
