using System.Reflection;
using System.Text.Json;
using AsyncResponse.DurableFlows.EFCore;
using AsyncResponse.DurableFlows.MySql;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 65, stores/CI group: store refusals that need no I/O fail the HOST START (not the first
/// operation, after the starter has already published and returned an id), and the MySQL store
/// holds a ledger under the server's max_allowed_packet instead of failing with an opaque packet
/// error. The MySQL internals are reached by reflection so each fact fails — rather than fails to
/// compile — against the code before the fix.
/// </summary>
public sealed class Round65StoresciTests
{
    // An endpoint nothing listens on: any connection attempt fails fast, so a fact that passes
    // proves the refusal came from configuration alone.
    private const string UnreachableMySql = "Server=127.0.0.1;Port=9;Database=x;Uid=u;Pwd=p;Connection Timeout=1";

    // ---------------------------------------------------------------- L-18: MySQL UseAffectedRows

    [Fact]
    public async Task MySql_UseAffectedRows_FailsTheHostStart()
    {
        // The refusal used to live in EnsureCreatedAsync — the first operation. StartAsync
        // publishes first and swallows the starter's create failure, so every start was accepted,
        // returned its id, and dead-lettered in the workers.
        await using var provider = BuildHost(builder => builder.WithMySqlDurableFlows(options =>
        {
            options.ConnectionString = UnreachableMySql + ";UseAffectedRows=true";
            options.AutoCreateSchema = false;
        }));
        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains("UseAffectedRows", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MySql_OptionsValidate_RefusesUseAffectedRows_AndNamesAMalformedKeywordWithoutEchoingTheString()
    {
        var affected = new MySqlDurableFlowOptions { ConnectionString = UnreachableMySql + ";UseAffectedRows=true" };
        Assert.Contains("UseAffectedRows", Assert.Throws<InvalidOperationException>(affected.Validate).Message, StringComparison.Ordinal);

        // Parsing moved into Validate, so a connection string MySqlConnector cannot parse now fails
        // here too — as an options error that does not repeat the secret-bearing string.
        var malformed = new MySqlDurableFlowOptions { ConnectionString = "Server=h;Pwd=s3cret;NotAnOption=1" };
        var parse = Assert.Throws<InvalidOperationException>(malformed.Validate);
        Assert.Contains(nameof(MySqlDurableFlowOptions.ConnectionString), parse.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("s3cret", parse.Message, StringComparison.Ordinal);

        // The default stays accepted.
        new MySqlDurableFlowOptions { ConnectionString = UnreachableMySql }.Validate();
        new MySqlDurableFlowOptions { ConnectionString = UnreachableMySql + ";UseAffectedRows=false" }.Validate();
    }

    // ------------------------------------------------------------ L-18: EF Core mapping at start

    [Fact]
    public async Task EFCore_AContextThatDoesNotMapTheLedger_FailsTheHostStart()
    {
        await using var provider = BuildHost(
            builder => builder.WithEFCoreDurableFlows<UnmappedFlowDbContext>(),
            services => services.AddDbContextFactory<UnmappedFlowDbContext>(options => options.UseSqlite("Data Source=unused-r65.db")));
        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains($"does not map {nameof(DurableFlowStateRecord)}", ex.Message, StringComparison.Ordinal);
        Assert.False(File.Exists("unused-r65.db"), "the start check must not open the database");
    }

    [Fact]
    public async Task EFCore_ACaseFoldingProviderWithoutAnOrdinalCollation_FailsTheHostStart()
    {
        // SQL Server context mapped without the collation: no server is contacted, the model and
        // the provider name decide.
        await using var provider = BuildHost(
            builder => builder.WithEFCoreDurableFlows<UncollatedFlowDbContext>(),
            services => services.AddDbContext<UncollatedFlowDbContext>(options => options.UseSqlServer("Server=unused;Database=unused;")));
        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains("without a collation", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EFCore_ACorrectlyMappedContext_StartsWithoutContactingTheDatabase()
    {
        await using var provider = BuildHost(
            builder => builder.WithEFCoreDurableFlows<CollatedFlowDbContext>(),
            services => services.AddDbContext<CollatedFlowDbContext>(options => options.UseSqlServer("Server=unused;Database=unused;Connect Timeout=1")));
        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        // 'unused' does not resolve: had the probe opened a connection, the start would throw.
        await validator.StartAsync(CancellationToken.None);
    }

    [Fact]
    public async Task EFCore_AContextThatCannotBeCreatedAtStart_IsLeftToTheFirstOperation()
    {
        // Only the mapping REFUSAL fails the start. Creating the context runs the application's
        // own factory, which may need what only a running host provides.
        var logger = new CollectingLogger();
        await using var provider = BuildHost(
            builder => builder.WithEFCoreDurableFlows<UnmappedFlowDbContext>(),
            services =>
            {
                services.AddSingleton(logger.For<EFCoreFlowStateStore<UnmappedFlowDbContext>>());
                services.AddSingleton<IDbContextFactory<UnmappedFlowDbContext>, ThrowingFactory>();
            });
        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        await validator.StartAsync(CancellationToken.None);

        Assert.Single(logger.Messages, message => message.Contains("could not create a 'UnmappedFlowDbContext' while the host started", StringComparison.Ordinal));
        var store = provider.GetRequiredService<EFCoreFlowStateStore<UnmappedFlowDbContext>>();
        var first = await Assert.ThrowsAsync<InvalidOperationException>(() => store.LoadAsync("flow"));
        Assert.Equal(ThrowingFactory.Message, first.Message);

        // The caller's own cancellation is not a fault to swallow.
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Assert.IsAssignableFrom<IFlowStateStoreStartupProbe>((object)store).VerifyConfigurationAsync(cancelled.Token));
    }

    // ------------------------------------------------------- L-19: MySQL max_allowed_packet cap

    [Theory]
    [InlineData(4L * 1024 * 1024, 4L * 1024 * 1024 - 64 * 1024)]   // MySQL 5.7 default
    [InlineData(16L * 1024 * 1024, 16L * 1024 * 1024 - 64 * 1024)] // MariaDB default
    [InlineData(64L * 1024 * 1024, 64L * 1024 * 1024 - 64 * 1024)] // MySQL 8.0 default
    [InlineData(64L * 1024, 0L)]                                    // no room left: no check
    [InlineData(1024L, 0L)]
    public void MySql_PacketLedgerBudget_IsThePacketLessTheStatementHeadroom(long maxAllowedPacket, long expected)
        => Assert.Equal(expected, InvokeMySqlStatic<long>("PacketLedgerBudget", maxAllowedPacket));

    [Fact]
    public void MySql_EscapedLedgerBytes_CountsEveryDoubledQuoteAndBackslash()
    {
        Assert.Equal(3L, InvokeMySqlStatic<long>("EscapedLedgerBytes", "abc"));
        Assert.Equal(7L, InvokeMySqlStatic<long>("EscapedLedgerBytes", "a'b\\c"));
        Assert.Equal(4L, InvokeMySqlStatic<long>("EscapedLedgerBytes", "é'"));
    }

    [Fact]
    public async Task MySql_OnceThePacketBudgetIsKnown_AnOversizedLedgerIsRefusedByName_BeforeAnyWrite()
    {
        // A server whose max_allowed_packet is 1 MiB, already verified: ~960 KiB of escaped ledger.
        // A ledger past it used to reach the server and fail there (ER_NET_PACKET_TOO_LARGE or a
        // dropped connection) on every redelivery; it is now refused with the shared exception
        // that names the ledger and its size — before a connection is opened (the endpoint is
        // unreachable, so reaching it would surface a MySqlException instead).
        var store = new MySqlFlowStateStore(Options.Create(new MySqlDurableFlowOptions
        {
            ConnectionString = UnreachableMySql,
            AutoCreateSchema = false
        }));
        SimulateVerifiedServer(store, maxAllowedPacket: 1024 * 1024);
        const long budget = 1024L * 1024 - 64 * 1024;

        var update = await Assert.ThrowsAsync<FlowStateTooLargeException>(
            () => store.TryUpdateAsync("flow-big", LargeState("flow-big", bytes: 1_200_000), expectedRevision: 0, TimeSpan.FromMinutes(5)));
        Assert.Equal(budget, update.MaxStateBytes);
        Assert.True(update.SerializedSizeBytes > budget);

        var initial = LargeState("flow-big", bytes: 1_200_000, revision: 0);
        await Assert.ThrowsAsync<FlowStateTooLargeException>(
            () => store.TryCreateAsync("flow-big", initial, TimeSpan.FromMinutes(5)));
        // The starter's pre-publish check sees the same budget, so the start is refused before it
        // publishes anything.
        Assert.Throws<FlowStateTooLargeException>(() => store.ValidateCreate("flow-big", initial, TimeSpan.FromMinutes(5)));

        // The budget is judged on the ESCAPED size: 200 000 backslashes nest into ~800 KB of UTF-8
        // (each is escaped once in the input JSON and again in the ledger) that doubles to ~1.6 MB
        // on the wire — refused although its plain size fits.
        var escaping = LargeState("flow-escaping", bytes: 0, revision: 0, padding: '\\', count: 200_000);
        Assert.Throws<FlowStateTooLargeException>(() => store.ValidateCreate("flow-escaping", escaping, TimeSpan.FromMinutes(5)));

        // A ledger under the budget passes the guard and goes on to the (unreachable) server —
        // including one past HALF the packet that a worst-case bound refused (critic follow-up:
        // an in-flight run checkpointing fine under the same server must keep doing so).
        store.ValidateCreate("flow-small", LargeState("flow-small", bytes: 1_000, revision: 0), TimeSpan.FromMinutes(5));
        store.ValidateCreate("flow-mid", LargeState("flow-mid", bytes: 600_000, revision: 0), TimeSpan.FromMinutes(5));
        var write = await Record.ExceptionAsync(() => store.TryUpdateAsync("flow-mid", LargeState("flow-mid", bytes: 600_000), expectedRevision: 0, TimeSpan.FromMinutes(5)));
        Assert.IsNotType<FlowStateTooLargeException>(write);
    }

    [Fact]
    public async Task MySql_AnExplicitBudgetBelowThePacketCap_StillWins()
    {
        var store = new MySqlFlowStateStore(Options.Create(new MySqlDurableFlowOptions
        {
            ConnectionString = UnreachableMySql,
            AutoCreateSchema = false,
            MaxStateBytes = 2_000
        }));
        SimulateVerifiedServer(store, maxAllowedPacket: 64L * 1024 * 1024);

        var ex = await Assert.ThrowsAsync<FlowStateTooLargeException>(
            () => store.TryUpdateAsync("flow-b", LargeState("flow-b", bytes: 5_000), expectedRevision: 0, TimeSpan.FromMinutes(5)));
        Assert.Equal(2_000, ex.MaxStateBytes);
    }

    // ------------------------------------------------------------------------------- helpers

    private static ServiceProvider BuildHost(Action<AsyncResponseRegistrationBuilder> durableFlows, Action<IServiceCollection>? extra = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        extra?.Invoke(services);
        var builder = services.AddAsyncResponse().WithInMemoryChannel().WithInMemoryTransport();
        durableFlows(builder);
        return services.BuildServiceProvider();
    }

    private static T InvokeMySqlStatic<T>(string name, params object?[] args)
    {
        var method = typeof(MySqlFlowStateStore).GetMethod(name, BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);
        Assert.True(method is not null, $"MySqlFlowStateStore.{name} does not exist");
        return (T)method.Invoke(null, args)!;
    }

    /// <summary>What the store's first schema verification leaves behind on a server with this packet limit.</summary>
    private static void SimulateVerifiedServer(MySqlFlowStateStore store, long maxAllowedPacket)
    {
        var budget = typeof(MySqlFlowStateStore).GetField("_packetLedgerBudget", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.True(budget is not null, "MySqlFlowStateStore learns no packet-derived budget");
        budget.SetValue(store, InvokeMySqlStatic<long>("PacketLedgerBudget", maxAllowedPacket));
        typeof(MySqlFlowStateStore).GetField("_created", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(store, true);
    }

    private static FlowState LargeState(string flowId, int bytes, long revision = 1, char padding = 'x', int count = -1)
        => new()
        {
            FlowId = flowId,
            FlowTypeName = "Round65.Flow",
            InputTypeName = "Round65.Input",
            InputJson = JsonSerializer.Serialize(new { Padding = new string(padding, count >= 0 ? count : bytes) }),
            Status = FlowRunStatus.Running,
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow,
            Revision = revision
        };

    private sealed class ThrowingFactory : IDbContextFactory<UnmappedFlowDbContext>
    {
        public const string Message = "this context needs a request scope";

        public UnmappedFlowDbContext CreateDbContext() => throw new InvalidOperationException(Message);
    }
}
