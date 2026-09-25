using AsyncResponse.DurableFlows.EFCore;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
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

/// <summary>Mapped without a flow-id collation — the default that is unsafe on SQL Server/MySQL.</summary>
internal sealed class UncollatedFlowDbContext(DbContextOptions<UncollatedFlowDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.ConfigureAsyncResponseDurableFlows();
}

/// <summary>Mapped WITH the SQL Server collation, the way the docs prescribe.</summary>
internal sealed class CollatedFlowDbContext(DbContextOptions<CollatedFlowDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.ConfigureAsyncResponseDurableFlows(
            flowIdCollation: AsyncResponseFlowIdCollations.SqlServer);
}

/// <summary>
/// Mapped with a real, case-SENSITIVE SQL Server collation that still folds accents and full-width
/// forms — the plausible wrong answer, and the one a "declared something" check would accept.
/// </summary>
internal sealed class CaseSensitiveFlowDbContext(DbContextOptions<CaseSensitiveFlowDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.ConfigureAsyncResponseDurableFlows(flowIdCollation: "Latin1_General_100_CS_AS");
}

public sealed class EFCoreDurableFlowStateStoreTests
{
    [Fact]
    public async Task EFCoreStore_OnACaseFoldingProvider_RefusesAMappingWithoutAFlowIdCollation()
    {
        // The schema is the application's, so this package cannot pin the collation itself — but
        // it must not run silently against a mapping that leaves flow_id to a provider whose
        // default folds case. On SQL Server that makes 'flow-a' and 'FLOW-A' one primary key: the
        // second StartAsync fails as a duplicate and a load returns the other run's state.
        // No server is contacted here — building the model is enough to decide.
        var services = new ServiceCollection();
        services.AddDbContext<UncollatedFlowDbContext>(options => options.UseSqlServer("Server=unused;Database=unused;"));
        await using var provider = services.BuildServiceProvider();

        var store = new EFCoreFlowStateStore<UncollatedFlowDbContext>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new EFCoreDurableFlowOptions()));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.LoadAsync("any-flow"));
        Assert.Contains("without a collation", exception.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(AsyncResponseFlowIdCollations), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EFCoreStore_OnACaseFoldingProvider_AcceptsAMappingThatDeclaresTheCollation()
    {
        // The counterpart, and the load-bearing half: the declaration has to reach the store
        // through the RUNTIME model. EF Core strips relational configuration the runtime never
        // reads (asking a runtime property for its collation throws), so the mapping records the
        // choice as a model annotation — if that annotation did not survive, this fact fails and
        // the guard above would be rejecting correctly-configured applications.
        var services = new ServiceCollection();
        services.AddDbContext<CollatedFlowDbContext>(options => options.UseSqlServer("Server=unused;Database=unused;"));
        await using var provider = services.BuildServiceProvider();

        var store = new EFCoreFlowStateStore<CollatedFlowDbContext>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new EFCoreDurableFlowOptions()));

        // Past the mapping check, the connection attempt is what fails — never the collation guard.
        var exception = await Record.ExceptionAsync(() => store.LoadAsync("any-flow"));
        Assert.NotNull(exception);
        Assert.DoesNotContain("without a collation", exception.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task EFCoreStore_OnACaseFoldingProvider_RefusesADeclaredCollationThatIsNotOrdinal()
    {
        // "I declared a collation" and "I declared an ordinal one" are different claims, and only
        // the second is what the primary key needs. Latin1_General_100_CS_AS is a perfectly valid
        // SQL Server collation — and case-sensitive, which is the intuitive answer — yet probed on
        // SQL Server 2022 it still reports 'ab' = 'ａｂ'; every _CS_AI collation folds accents the
        // same way. Only _BIN2 compares by code point, so only _BIN2 may pass.
        var services = new ServiceCollection();
        services.AddDbContext<CaseSensitiveFlowDbContext>(options => options.UseSqlServer("Server=unused;Database=unused;"));
        await using var provider = services.BuildServiceProvider();

        var store = new EFCoreFlowStateStore<CaseSensitiveFlowDbContext>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new EFCoreDurableFlowOptions()));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.LoadAsync("any-flow"));
        Assert.Contains("Latin1_General_100_CS_AS", exception.Message, StringComparison.Ordinal);
        Assert.Contains("does not compare byte-wise", exception.Message, StringComparison.Ordinal);
        Assert.Contains(AsyncResponseFlowIdCollations.SqlServer, exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    // SQL Server: only a binary collation is ordinal. _CS_AS is the plausible wrong answer (it is
    // case-sensitive and still folds full-width forms); _CS_AI additionally folds accents.
    [InlineData("Microsoft.EntityFrameworkCore.SqlServer", "Latin1_General_100_BIN2", true)]
    [InlineData("Microsoft.EntityFrameworkCore.SqlServer", "Latin1_General_BIN", true)]
    [InlineData("Microsoft.EntityFrameworkCore.SqlServer", "Latin1_General_100_CS_AS", false)]
    [InlineData("Microsoft.EntityFrameworkCore.SqlServer", "SQL_Latin1_General_CP1_CI_AS", false)]
    // MySQL, through both the official provider id and Pomelo's.
    [InlineData("Pomelo.EntityFrameworkCore.MySql", "utf8mb4_bin", true)]
    [InlineData("MySql.EntityFrameworkCore", "utf8mb4_bin", true)]
    [InlineData("Pomelo.EntityFrameworkCore.MySql", "utf8mb4_0900_as_cs", false)]
    [InlineData("Pomelo.EntityFrameworkCore.MySql", "utf8mb4_0900_ai_ci", false)]
    public void FlowIdCollationRules_NameWhatIsOrdinalPerProvider(string providerName, string collation, bool ordinal)
    {
        // The MySQL branch cannot be reached through a DbContext here — the test project references
        // no MySQL provider — so the rule table is exercised directly. Without this, half the guard
        // ships on the strength of the SQL Server branch alone.
        var rules = FlowIdCollationRules.CaseFoldingProvider(providerName);

        Assert.NotNull(rules);
        Assert.Equal(ordinal, rules.IsOrdinal(collation));
        Assert.True(rules.IsOrdinal(rules.Recommended), "the constant this provider recommends must satisfy its own rule");
    }

    [Theory]
    // Byte-wise by default: nothing to declare, so nothing to refuse.
    [InlineData("Npgsql.EntityFrameworkCore.PostgreSQL")]
    [InlineData("Microsoft.EntityFrameworkCore.Sqlite")]
    // Unknown third-party provider: the benefit of the doubt, not a startup failure it has no
    // documented way to satisfy.
    [InlineData("Contoso.EntityFrameworkCore.Something")]
    [InlineData(null)]
    public void FlowIdCollationRules_LeaveNonCaseFoldingProvidersAlone(string? providerName)
        => Assert.Null(FlowIdCollationRules.CaseFoldingProvider(providerName));

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
        await using var provider = BuildScopedContextProvider(database.ConnectionString + ";Default Timeout=60");

        await RunStormAsync(CreateStore(provider));
    }

    [Fact]
    public async Task EFCoreStore_ConcurrentSaveLoadDeleteStorm_WithDbContextFactory()
    {
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        await using var provider = BuildFactoryProvider(database.ConnectionString + ";Default Timeout=60");

        await RunStormAsync(CreateStore(provider));
    }

    [Fact]
    public async Task EFCoreStore_ConcurrentCreatesOfSameFlowIds_LoseTheInsertRaceGracefully()
    {
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        await using var provider = BuildFactoryProvider(database.ConnectionString + ";Default Timeout=60");
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

    [Fact]
    public async Task TryCreate_ReplacesAnExpiredLedgerInPlace_WithoutASeparateInsert()
    {
        // Regression: the expired-ledger replace was ExecuteDelete followed by a separate
        // SaveChanges insert — two transactions, where every sibling store replaces atomically. A
        // failure between them destroyed the expired row with no replacement. The replace is now a
        // single in-place update, so an insert that cannot succeed is never needed for it.
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        var interceptor = new ArmedThrowingSaveChangesInterceptor();
        var services = new ServiceCollection();
        services.AddDbContextFactory<TestFlowDbContext>(options => options
            .UseSqlite(database.ConnectionString)
            .AddInterceptors(interceptor));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var store = CreateStore(provider);

        Assert.True(await store.TryCreateAsync("expired-flow", CreateState("expired-flow"), TimeSpan.FromMilliseconds(1)));
        await Task.Delay(20);
        interceptor.Armed = true; // from here on any SaveChanges fails: the replace must not need one

        var replacement = CreateState("expired-flow");
        replacement.LastMessage = "replaced";
        Assert.True(await store.TryCreateAsync("expired-flow", replacement, TimeSpan.FromMinutes(5)));

        var loaded = await store.LoadAsync("expired-flow");
        Assert.NotNull(loaded);
        Assert.Equal("replaced", loaded!.LastMessage);
    }

    [Fact]
    public void LedgerEntity_CanBeProxied_SoALazyLoadingProxiesContextCanMapIt()
    {
        // Regression: DurableFlowStateRecord was sealed, and EF Core's proxies convention rejects
        // every sealed entity type at model finalisation ("UseLazyLoadingProxies requires all
        // entity types to be public, unsealed…") — an application context that uses proxies
        // could not host the ledger, the store's only supported setup. EF Core Proxies builds its
        // proxies with Castle DynamicProxy, which is what this asks directly.
        var proxy = new Castle.DynamicProxy.ProxyGenerator().CreateClassProxy<DurableFlowStateRecord>();

        Assert.IsAssignableFrom<DurableFlowStateRecord>(proxy);
        Assert.NotEqual(typeof(DurableFlowStateRecord), proxy.GetType());
    }

    [Fact]
    public async Task Prune_OnSqlite_ReChecksExpiryOnTheRowItDeletes_InExpiryOrder()
    {
        // Regression: `Where(expired).OrderBy(FlowId).Take(n).ExecuteDelete()` is not a valid
        // single-table delete for the relational providers (an ordering or a limit sends them to
        // the `DELETE … WHERE <pk> IN/EXISTS (SELECT … WHERE expired ORDER BY … LIMIT n)` fallback),
        // so the expiry predicate lived only inside the subquery. A TryCreate that replaced the
        // expired row in place between the subquery's read and the delete reaching the row lost its
        // fresh ledger to the prune: under PostgreSQL READ COMMITTED (and SQL Server RCSI) the outer
        // DELETE re-checks only its own predicate, and it had none. The expiry predicate must sit on
        // the DELETE's own WHERE — and the batch is taken in expiry order, which the expires index
        // serves, not flow-id order, which made every batch sort or walk the whole backlog.
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        var capture = new CapturingCommandInterceptor();
        var services = new ServiceCollection();
        services.AddDbContextFactory<TestFlowDbContext>(options => options
            .UseSqlite(database.ConnectionString)
            .AddInterceptors(capture));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var store = CreateStore(provider, pruneInterval: TimeSpan.Zero);

        Assert.True(await store.TryCreateAsync("expired-flow", CreateState("expired-flow"), TimeSpan.FromMinutes(5)));
        await database.ExpireAsync("expired-flow");
        Assert.True(await store.TryCreateAsync("live-flow", CreateState("live-flow"), TimeSpan.FromMinutes(5)));

        AssertPruneDeleteReChecksExpiry(capture.Commands.First(c => c.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase)));
        // And the reshaped statement still does its job on a real database.
        Assert.Equal(0, await database.CountRowsAsync("expired-flow"));
        Assert.NotNull(await store.LoadAsync("live-flow"));
    }

    [Fact]
    public async Task Prune_OnSqlServer_ReChecksExpiryOnTheRowItDeletes_InExpiryOrder()
    {
        // The same pin against the SQL Server provider's own rendering — the provider on which the
        // lost run was reported (RCSI, the Azure SQL default). No server is contacted: the connection
        // open and every command are intercepted, so only the SQL EF Core renders is observed.
        var capture = new CapturingCommandInterceptor { SuppressWith = sql => sql.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase) ? 0 : 1 };
        var services = new ServiceCollection();
        services.AddDbContextFactory<CollatedFlowDbContext>(options => options
            .UseSqlServer("Server=unused;Database=unused;")
            .AddInterceptors(capture, new SuppressOpenInterceptor()));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var store = new EFCoreFlowStateStore<CollatedFlowDbContext>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new EFCoreDurableFlowOptions { PruneInterval = TimeSpan.Zero }));

        Assert.True(await store.TryCreateAsync("flow-a", CreateState("flow-a"), TimeSpan.FromMinutes(5)));

        AssertPruneDeleteReChecksExpiry(Assert.Single(capture.Commands, c => c.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase)));
    }

    private static void AssertPruneDeleteReChecksExpiry(string sql)
    {
        // Everything before the first SELECT is the DELETE's own predicate on the row it deletes.
        var subquery = sql.IndexOf("SELECT", StringComparison.OrdinalIgnoreCase);
        Assert.True(subquery > 0, sql);
        Assert.Contains("expires_at_utc", sql[..subquery], StringComparison.Ordinal);
        Assert.Matches(@"ORDER BY\s+\S*expires_at_utc", sql);
        Assert.DoesNotMatch(@"ORDER BY\s+\S*flow_id", sql);
    }

    [Fact]
    public async Task RowCountWrites_RefuseAnUnavailableRowCount_InsteadOfReportingALostWrite()
    {
        // Regression: a SQL Server with NOCOUNT on by default (`sp_configure 'user options', 512`)
        // makes every ExecuteUpdate/ExecuteDelete report -1. The store compared `> 0`, so every
        // acquire, renew, checkpoint and delete read as lost although the row was written — flows
        // churned through lease contention into the dead-letter queue with no hint at the cause. A
        // negative count is not "0 rows": it is "the database did not say", and the store says so.
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        var capture = new CapturingCommandInterceptor();
        var services = new ServiceCollection();
        services.AddDbContextFactory<TestFlowDbContext>(options => options
            .UseSqlite(database.ConnectionString)
            .AddInterceptors(capture));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        IFlowStateStore store = CreateStore(provider);

        var state = CreateState("nocount-flow");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        capture.ReportRowCount = -1;

        var acquire = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.TryAcquireLeaseAsync(state.FlowId!, "owner-a", TimeSpan.FromMinutes(1)));
        Assert.Contains("NOCOUNT", acquire.Message, StringComparison.Ordinal);
        state.Revision = 1;
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.TryUpdateAsync(state.FlowId!, state, 0, TimeSpan.FromMinutes(5)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.TryDeleteAsync(state.FlowId!));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.TryCreateAsync("other-flow", CreateState("other-flow"), TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task TryCreate_WhenTheInsertCollidesWithARowThatHasSinceExpired_ReplacesItInsteadOfReportingExists()
    {
        // Regression (store half of the lost-start window): the in-place replace judged expiry with
        // a `now` taken before the prune (up to PruneBudget earlier), and the duplicate-key fallback
        // asked "does ANY row carry this id" — so a row that expired between the replace and the
        // insert made TryCreate answer "exists". The executor then loaded the ledger, found it
        // expired, and acknowledged the start job: the new run was never created. Here the first
        // replace is made to miss exactly as a stale `now` makes it miss; the fallback must see the
        // row is expired and replace it.
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        var capture = new CapturingCommandInterceptor();
        var services = new ServiceCollection();
        services.AddDbContextFactory<TestFlowDbContext>(options => options
            .UseSqlite(database.ConnectionString)
            .AddInterceptors(capture));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var store = CreateStore(provider, pruneInterval: TimeSpan.FromHours(1));

        // Arms the prune throttle (the first create always prunes) and leaves an expired row behind.
        Assert.True(await store.TryCreateAsync("reused-flow", CreateState("reused-flow"), TimeSpan.FromMinutes(5)));
        await database.ExpireAsync("reused-flow");
        capture.MissNextReplace = true;

        var replacement = CreateState("reused-flow");
        replacement.LastMessage = "second run";
        Assert.True(await store.TryCreateAsync("reused-flow", replacement, TimeSpan.FromMinutes(5)));
        Assert.Equal("second run", (await store.LoadAsync("reused-flow"))!.LastMessage);
    }

    [Fact]
    public async Task TryCreate_WhenAPeerPrunesTheCollidingRowBeforeTheExistenceCheck_InsertsInsteadOfThrowing()
    {
        // Regression: the insert collided with an expired row, a peer's prune deleted that row, and
        // the existence check then found nothing — so the store rethrew the raw duplicate-key
        // exception for a create that would now succeed, costing the job a delivery attempt and an
        // error log. Neither a live owner nor a failure: try again.
        await using var database = new TempSqliteDatabase();
        await database.EnsureSchemaAsync();
        var capture = new CapturingCommandInterceptor();
        var services = new ServiceCollection();
        services.AddDbContextFactory<TestFlowDbContext>(options => options
            .UseSqlite(database.ConnectionString)
            .AddInterceptors(capture));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var store = CreateStore(provider, pruneInterval: TimeSpan.FromHours(1));

        Assert.True(await store.TryCreateAsync("pruned-flow", CreateState("pruned-flow"), TimeSpan.FromMinutes(5)));
        await database.ExpireAsync("pruned-flow");
        capture.MissNextReplace = true;
        capture.BeforeNextExistenceCheck = () => database.ExecuteSqlAsync("DELETE FROM asyncresponse_flow_state WHERE flow_id = 'pruned-flow';");

        var replacement = CreateState("pruned-flow");
        replacement.LastMessage = "second run";
        Assert.True(await store.TryCreateAsync("pruned-flow", replacement, TimeSpan.FromMinutes(5)));
        Assert.Equal("second run", (await store.LoadAsync("pruned-flow"))!.LastMessage);
    }

    /// <summary>
    /// Records every command EF Core sends and, when armed, bends the answers the way a racing
    /// database would: suppress commands (with a scripted row count), report an unavailable row
    /// count, make the in-place replace miss once, or run a peer's write just before the insert's
    /// duplicate-key fallback asks whether the row exists.
    /// </summary>
    private sealed class CapturingCommandInterceptor : DbCommandInterceptor
    {
        public System.Collections.Concurrent.ConcurrentQueue<string> Commands { get; } = new();

        public Func<string, int>? SuppressWith { get; init; }

        public int? ReportRowCount { get; set; }

        public bool MissNextReplace { get; set; }

        public Func<Task>? BeforeNextExistenceCheck { get; set; }

        public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(
            System.Data.Common.DbCommand command,
            CommandEventData eventData,
            InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
        {
            Commands.Enqueue(command.CommandText);
            if (SuppressWith is { } suppress)
                return ValueTask.FromResult(InterceptionResult<int>.SuppressWithResult(suppress(command.CommandText)));
            if (MissNextReplace && command.CommandText.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase)
                && command.CommandText.Contains("state_json", StringComparison.Ordinal))
            {
                MissNextReplace = false;
                return ValueTask.FromResult(InterceptionResult<int>.SuppressWithResult(0));
            }

            return base.NonQueryExecutingAsync(command, eventData, result, cancellationToken);
        }

        public override ValueTask<int> NonQueryExecutedAsync(
            System.Data.Common.DbCommand command,
            CommandExecutedEventData eventData,
            int result,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(ReportRowCount ?? result);

        public override async ValueTask<InterceptionResult<System.Data.Common.DbDataReader>> ReaderExecutingAsync(
            System.Data.Common.DbCommand command,
            CommandEventData eventData,
            InterceptionResult<System.Data.Common.DbDataReader> result,
            CancellationToken cancellationToken = default)
        {
            Commands.Enqueue(command.CommandText);
            if (BeforeNextExistenceCheck is { } peer && command.CommandText.Contains("EXISTS", StringComparison.OrdinalIgnoreCase))
            {
                BeforeNextExistenceCheck = null;
                await peer();
            }

            return result;
        }
    }

    /// <summary>Lets a provider render and "execute" commands without a server to connect to.</summary>
    private sealed class SuppressOpenInterceptor : DbConnectionInterceptor
    {
        public override ValueTask<InterceptionResult> ConnectionOpeningAsync(
            System.Data.Common.DbConnection connection,
            ConnectionEventData eventData,
            InterceptionResult result,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(InterceptionResult.Suppress());
    }

    private sealed class ArmedThrowingSaveChangesInterceptor : SaveChangesInterceptor
    {
        public bool Armed { get; set; }

        public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
            DbContextEventData eventData,
            InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
            => Armed
                ? throw new InvalidOperationException("the insert was lost")
                : base.SavingChangesAsync(eventData, result, cancellationToken);
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

        // Default (pooled) connections on purpose: with WAL, pooling keeps the shared-memory
        // index warm and write locks microsecond-scale. Pooling=False looked like the clean
        // fix for Windows file-lock cleanup, but it makes every operation pay a full file
        // open plus a WAL checkpoint on close — concurrent storms then exhaust their busy
        // timeout on slow CI disks. Cleanup instead clears THIS database's pool (targeted,
        // unlike the process-global ClearAllPools) before deleting the files.
        public string ConnectionString => $"Data Source={_path}";

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

        /// <summary>
        /// Backdates a ledger's expiry far into the past: an expired row without waiting out a
        /// short TTL on the real clock, which a clock step or a slow runner makes timing-dependent.
        /// </summary>
        public async Task ExpireAsync(string flowId)
        {
            await using var context = CreateContext();
            var longExpired = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            Assert.Equal(1, await context.Set<DurableFlowStateRecord>()
                .Where(r => r.FlowId == flowId)
                .ExecuteUpdateAsync(setters => setters.SetProperty(r => r.ExpiresAtUtc, longExpired)));
        }

        private TestFlowDbContext CreateContext()
            => new(new DbContextOptionsBuilder<TestFlowDbContext>().UseSqlite(ConnectionString).Options);

        public ValueTask DisposeAsync()
        {
            // Release this database's pooled handles (targeted — other tests' pools are not
            // touched), then best-effort delete the file and its WAL sidecars.
            SqliteConnection.ClearPool(new SqliteConnection(ConnectionString));
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
