using System.Diagnostics.Metrics;
using System.Reflection;
using AsyncResponse.DurableFlows.EFCore;
using AsyncResponse.DurableFlows.MySql;
using AsyncResponse.DurableFlows.Oracle;
using AsyncResponse.DurableFlows.PostgreSQL;
using AsyncResponse.DurableFlows.Sqlite;
using AsyncResponse.DurableFlows.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round-34 pins that use API introduced by the same fix pass (they cannot compile against the
/// pre-fix build; their red-on-old proof is the compile break). The behavior pins that DO compile
/// on the old build are in <see cref="Round34RegressionTests"/>.
/// </summary>
public sealed class Round34NewApiTests
{
    private const string SharedTypeName = "AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared";

    // ---------------------------------------------------------------------------------------------
    // F8 — the dedicated exception the ingress passes through.

    [Fact]
    public void RecoveryCallbackFailedException_CarriesTheCorrelationIdAttemptsAndCause()
    {
        var cause = new TimeoutException("dependency down");

        var ex = new RecoveryCallbackFailedException("corr-1", 4, cause);

        Assert.Equal("corr-1", ex.CorrelationId);
        Assert.Equal(4, ex.Attempts);
        Assert.Same(cause, ex.InnerException);
        Assert.Contains("corr-1", ex.Message, StringComparison.Ordinal);
        Assert.Contains("not acknowledged", ex.Message, StringComparison.Ordinal);
        Assert.Equal(4, LostSubscriberCallbackDispatcher.FailureCallbackAttempts);
    }

    // ---------------------------------------------------------------------------------------------
    // F9 — the scheduler's re-drive knobs validate at registration.

    [Fact]
    public void WithScheduledFlow_RejectsANonPositiveRedriveInterval_AndANegativeStartupWindow()
    {
        Assert.Throws<InvalidOperationException>(() => Register(o => o.RedriveInterval = TimeSpan.Zero));
        Assert.Throws<InvalidOperationException>(() => Register(o => o.RedriveInterval = TimeSpan.FromDays(60)));
        Assert.Throws<ArgumentException>(() => Register(o => o.StartupRedriveWindow = TimeSpan.FromSeconds(-1)));

        // Zero disables the startup probe and is legal.
        Register(o => o.StartupRedriveWindow = TimeSpan.Zero);

        var defaults = new ScheduledFlowOptions();
        Assert.Equal(TimeSpan.FromSeconds(30), defaults.RedriveInterval);
        Assert.Equal(TimeSpan.FromHours(1), defaults.StartupRedriveWindow);

        static void Register(Action<ScheduledFlowOptions> configure)
            => new ServiceCollection()
                .AddAsyncResponse()
                .WithInMemoryChannel()
                .WithInMemoryTransport()
                .WithInMemoryDurableFlows()
                .WithScheduledFlow<NightlyReportFlow, ReportInput>("nightly", "0 6 * * *", occurrence => new ReportInput(occurrence), configure);
    }

    // ---------------------------------------------------------------------------------------------
    // F5 — the prune budget: option validation, batch draining, metrics, and failure logging.

    [Theory]
    [MemberData(nameof(RelationalOptionTypes))]
    public void PruneBudget_DefaultsToTwoSeconds_AndRejectsANegativeValue(Type optionsType)
    {
        var options = Activator.CreateInstance(optionsType)!;
        var budget = optionsType.GetProperty("PruneBudget")!;
        Assert.Equal(TimeSpan.FromSeconds(2), budget.GetValue(options));

        // The relational stores validate through Options.Validate(); EF Core validates in its
        // store constructor (its options type has no Validate method), pinned separately below.
        if (optionsType.GetMethod("Validate", BindingFlags.Public | BindingFlags.Instance) is not { } validate)
            return;

        MakeValid(options);
        validate.Invoke(options, null);
        budget.SetValue(options, TimeSpan.Zero);
        validate.Invoke(options, null);
        budget.SetValue(options, TimeSpan.FromSeconds(-1));
        var thrown = Assert.Throws<TargetInvocationException>(() => validate.Invoke(options, null));
        Assert.IsType<InvalidOperationException>(thrown.InnerException);
        Assert.Contains("PruneBudget", thrown.InnerException!.Message, StringComparison.Ordinal);

        static void MakeValid(object options)
        {
            // The server stores validate a connection string alongside the budget; identifiers
            // default valid.
            options.GetType().GetProperty("ConnectionString")?.SetValue(options, "Server=localhost;Database=asyncresponse");
        }
    }

    [Fact]
    public void EFCorePruneBudget_NegativeValue_IsRejectedByTheStoreConstructor()
    {
        var thrown = Assert.Throws<InvalidOperationException>(() => new EFCoreFlowStateStore<Microsoft.EntityFrameworkCore.DbContext>(
            new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            Microsoft.Extensions.Options.Options.Create(new EFCoreDurableFlowOptions { PruneBudget = TimeSpan.FromSeconds(-1) })));
        Assert.Contains("PruneBudget", thrown.Message, StringComparison.Ordinal);
    }

    public static TheoryData<Type> RelationalOptionTypes =>
    [
        typeof(EFCoreDurableFlowOptions),
        typeof(MySqlDurableFlowOptions),
        typeof(OracleDurableFlowOptions),
        typeof(PostgreSqlDurableFlowOptions),
        typeof(SqliteDurableFlowOptions),
        typeof(SqlServerDurableFlowOptions)
    ];

    /// <summary>
    /// The shared prune helper, reflected per provider assembly (each compiles its own copy):
    /// full batches keep it draining, a short batch stops it, a zero budget keeps the historical
    /// single batch, and the deleted rows land on the meter.
    /// </summary>
    [Theory]
    [MemberData(nameof(RelationalOptionTypes))]
    public async Task PruneQuietly_DrainsFullBatchesUntilAShortOne_AndCountsTheRows(Type providerOptionsType)
    {
        var pruneQuietly = PruneQuietlyMethod(providerOptionsType);
        var batchSize = PruneBatchSize(providerOptionsType);
        Assert.Equal(1000, batchSize);

        var batches = new Queue<int>([batchSize, batchSize, 7, batchSize]);
        var calls = 0;
        var measurements = await CollectAsync(() => Quietly(
            pruneQuietly,
            () => { calls++; return Task.FromResult(batches.Dequeue()); },
            TimeSpan.FromMinutes(1),
            $"probe-{providerOptionsType.Name}",
            logger: null));

        // Two full batches, then the short one ends the drain; the fourth is never requested.
        Assert.Equal(3, calls);
        var pruned = Assert.Single(measurements, m => m.Instrument == "asyncresponse.flow_state.pruned_rows" && Equals(m.Tags["provider"], $"probe-{providerOptionsType.Name}"));
        Assert.Equal(2 * batchSize + 7, pruned.Value);

        // A zero budget runs exactly one batch even when it comes back full — and says so.
        var logger = new CollectingLogger();
        calls = 0;
        measurements = await CollectAsync(() => Quietly(
            pruneQuietly,
            () => { calls++; return Task.FromResult(batchSize); },
            TimeSpan.Zero,
            $"zero-{providerOptionsType.Name}",
            logger));
        Assert.Equal(1, calls);
        Assert.Contains(measurements, m => m.Instrument == "asyncresponse.flow_state.prune_budget_exhausted" && Equals(m.Tags["provider"], $"zero-{providerOptionsType.Name}"));
        Assert.Contains(logger.Messages, m => m.Contains("PruneBudget", StringComparison.Ordinal) && m.Contains("expired rows remaining", StringComparison.Ordinal));
    }

    /// <summary>A failed prune is contained (the create it rides on succeeds), but no longer silent.</summary>
    [Theory]
    [MemberData(nameof(RelationalOptionTypes))]
    public async Task PruneQuietly_ContainsAFailure_ButLogsAndCountsIt_AndPropagatesCancellation(Type providerOptionsType)
    {
        var pruneQuietly = PruneQuietlyMethod(providerOptionsType);
        var logger = new CollectingLogger();
        var provider = $"fail-{providerOptionsType.Name}";

        var measurements = await CollectAsync(() => Quietly(
            pruneQuietly,
            () => throw new InvalidOperationException("deadlock victim"),
            TimeSpan.FromSeconds(1),
            provider,
            logger));

        Assert.Contains(measurements, m => m.Instrument == "asyncresponse.flow_state.prune_failures" && Equals(m.Tags["provider"], provider));
        var entry = Assert.Single(logger.Entries, e => e.Message.Contains("prune failed", StringComparison.Ordinal));
        Assert.IsType<InvalidOperationException>(entry.Exception);
        Assert.Contains(provider, entry.Message, StringComparison.Ordinal);

        await Assert.ThrowsAsync<OperationCanceledException>(() => Quietly(pruneQuietly, () => throw new OperationCanceledException(), TimeSpan.Zero, provider, logger));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Quietly(pruneQuietly, () => Task.FromCanceled<int>(new CancellationToken(canceled: true)), TimeSpan.Zero, provider, logger));
    }

    private static MethodInfo PruneQuietlyMethod(Type providerOptionsType)
    {
        var shared = providerOptionsType.Assembly.GetType(SharedTypeName, throwOnError: true)!;
        var method = shared.GetMethod("PruneQuietlyAsync", BindingFlags.Public | BindingFlags.Static);
        Assert.NotNull(method);
        Assert.Equal(
            [typeof(Func<Task<int>>), typeof(TimeSpan), typeof(string), typeof(ILogger)],
            method!.GetParameters().Select(p => p.ParameterType).ToArray());
        return method;
    }

    private static int PruneBatchSize(Type providerOptionsType)
    {
        var shared = providerOptionsType.Assembly.GetType(SharedTypeName, throwOnError: true)!;
        var field = shared.GetField("PruneBatchSize", BindingFlags.Public | BindingFlags.Static);
        Assert.NotNull(field);
        return (int)field!.GetRawConstantValue()!;
    }

    private static Task Quietly(MethodInfo pruneQuietly, Func<Task<int>> batch, TimeSpan budget, string provider, ILogger? logger)
        => (Task)pruneQuietly.Invoke(null, [batch, budget, provider, logger])!;

    private sealed record Measurement(string Instrument, long Value, Dictionary<string, object?> Tags);

    private static async Task<List<Measurement>> CollectAsync(Func<Task> action)
    {
        var measurements = new List<Measurement>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == AsyncResponseDiagnostics.MeterName)
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var tagDict = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var tag in tags)
                tagDict[tag.Key] = tag.Value;
            lock (measurements)
                measurements.Add(new Measurement(instrument.Name, value, tagDict));
        });
        listener.Start();

        await action();

        lock (measurements)
            return [.. measurements];
    }
}
