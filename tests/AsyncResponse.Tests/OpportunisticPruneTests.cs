using System.Diagnostics;
using System.Reflection;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The shared opportunistic-prune shape (<c>src/Shared/OpportunisticPrune.cs</c>, source-linked into
/// the PostgreSQL/SQL Server channel packages and the three database transports — every fact on the
/// helper runs against all five compilations by reflection), plus the channel throttles that use it.
/// </summary>
public sealed class OpportunisticPruneTests
{
    public static TheoryData<Type> AnchorTypes =>
    [
        typeof(PostgreSqlAsyncResponseChannelOptions),
        typeof(SqlServerAsyncResponseChannelOptions),
        typeof(PostgreSqlAsyncResponseTransportOptions),
        typeof(SqlServerAsyncResponseTransportOptions),
        typeof(MongoDbAsyncResponseTransportOptions)
    ];

    /// <summary>
    /// Regression: one batch per window was a hard ceiling (the SQL Server channel's 1,000 expired
    /// messages per 30 s PruneInterval, ~33 rows/s per process; the SQL Server transport's 1,000 dead
    /// letters a minute), which any deployment producing faster outgrew forever. Full batches now
    /// keep draining until one comes back short.
    /// </summary>
    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public async Task Drain_KeepsDeletingFullBatches_UntilOneComesBackShort(Type anchor)
    {
        var results = new Queue<int>([1000, 1000, 1000, 7, 1000]);
        var calls = 0;

        // An explicit, generous budget: the loop's stop condition is what this pins, and the real
        // 2 s default could lapse between batches on a stalled runner and cut the drain short.
        await DrainAsync(anchor, _ => { calls++; return Task.FromResult(results.Dequeue()); }, new CollectingLogger(), CancellationToken.None, GenerousBudget);

        Assert.Equal(4, calls);
    }

    /// <summary>
    /// The first batch always runs; after it the budget bounds the drain, and a lapsed budget with
    /// rows remaining is a warning (the backlog is outgrowing the prune), never an error.
    /// </summary>
    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public async Task Drain_StopsAtItsBudget_AfterTheFirstBatch_AndWarns(Type anchor)
    {
        var calls = 0;
        var logger = new CollectingLogger();

        await DrainAsync(anchor, _ => { calls++; return Task.FromResult(1000); }, logger, CancellationToken.None, TimeSpan.Zero);

        Assert.Equal(1, calls);
        Assert.Contains(logger.Messages, message => message.Contains("stopped at its", StringComparison.Ordinal));
    }

    /// <summary>
    /// Regression: prunes were awaited bare, so a prune that lost a lock wait, a connection, or a
    /// deadlock (1205) failed the publish, waiter registration, or probe it rode on — on a transport
    /// publish AFTER the row committed, so the caller's retry ran the job twice. Every failure,
    /// cancellation included, is now logged and swallowed.
    /// </summary>
    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public async Task Drain_SwallowsEveryFailure_IncludingCancellation(Type anchor)
    {
        var logger = new CollectingLogger();

        // A store failure mid-drain: logged, swallowed, and the drain stops. (A generous budget, so
        // the second batch runs however long the first took to return on a stalled runner.)
        var calls = 0;
        await DrainAsync(
            anchor,
            _ => ++calls == 1 ? Task.FromResult(1000) : throw new InvalidOperationException("deadlock victim"),
            logger,
            CancellationToken.None,
            GenerousBudget);
        Assert.Equal(2, calls);
        Assert.Contains(logger.Entries, entry => entry.Exception is InvalidOperationException && entry.Message.Contains("failed after deleting 1000", StringComparison.Ordinal));

        // The caller's token firing mid-DELETE: swallowed too (the caller's next statement sees it).
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await DrainAsync(anchor, token => Task.FromCanceled<int>(token), logger, cancelled.Token);

        // A cancellation the caller did not ask for (a provider timeout surfacing as OCE): swallowed.
        await DrainAsync(anchor, _ => throw new OperationCanceledException("provider timeout"), logger, CancellationToken.None);
    }

    /// <summary>
    /// Regression (fixpoint r2): the helper's log calls ran bare, so a logging provider that throws
    /// (MEL rethrows a provider's failure) escaped this never-throw helper — from the failure
    /// catch, and from the budget-lapse warning via that same catch's own log — failing a transport
    /// publish AFTER its row committed, so the caller's retry ran the job twice.
    /// </summary>
    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public async Task Drain_NeverThrows_WhenItsOwnLogLinesThrow(Type anchor)
    {
        var logger = new CollectingLogger { ThrowOnMessageContaining = "test prune" };

        // A failed batch: its warning throws.
        await DrainAsync(anchor, _ => throw new InvalidOperationException("deadlock victim"), logger, CancellationToken.None);

        // A lapsed budget with rows remaining: its warning throws (and, bare, was caught by the
        // failure arm and logged a second time — which threw out of the helper).
        await DrainAsync(anchor, _ => Task.FromResult(1000), logger, CancellationToken.None, TimeSpan.Zero);

        // A cancellation the caller asked for: its Debug line throws.
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await DrainAsync(anchor, token => Task.FromCanceled<int>(token), logger, cancelled.Token);

        Assert.Contains(logger.Messages, message => message.Contains("failed after deleting 0", StringComparison.Ordinal));
        Assert.Single(logger.Messages, message => message.Contains("stopped at its", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Messages, message => message.Contains("failed after deleting 1000", StringComparison.Ordinal));
        Assert.Contains(logger.Messages, message => message.Contains("was cancelled", StringComparison.Ordinal));
    }

    /// <summary>
    /// The throttle's first call always runs (0 = never pruned) and a second inside the interval does
    /// not; a stamp one interval old in the MONOTONIC past lets the next window through.
    /// </summary>
    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void ShouldRun_ThrottlesPerInterval_OnTheMonotonicClock(Type anchor)
    {
        var interval = TimeSpan.FromMinutes(10);
        var stamp = 0L;

        Assert.True(ShouldRun(anchor, ref stamp, interval));
        Assert.NotEqual(0L, stamp);
        Assert.False(ShouldRun(anchor, ref stamp, interval));

        stamp = Stopwatch.GetTimestamp() - (long)((interval.TotalSeconds + 1) * Stopwatch.Frequency);
        Assert.True(ShouldRun(anchor, ref stamp, interval));

        Assert.True(ShouldRun(anchor, ref stamp, TimeSpan.Zero));
    }

    /// <summary>
    /// Regression: the channel prune throttles stamped <c>DateTime.UtcNow.Ticks</c>, so a backward
    /// system-clock step (VM restore, NTP step) kept <c>now - last</c> below the interval for the size
    /// of the step and suspended the housekeeping on that process for as long. The stamp is now a
    /// Stopwatch timestamp, which no wall-clock step moves.
    /// </summary>
    [Fact]
    public void ChannelThrottles_StampTheMonotonicClock_NotTheWallClock()
    {
        using var dataSource = NpgsqlDataSource.Create("Host=localhost;Database=unused;Username=unused;Password=unused");
        var postgres = new PostgreSqlChannelSql(dataSource, Options.Create(new PostgreSqlAsyncResponseChannelOptions { PruneInterval = TimeSpan.FromMinutes(10) }));
        var sqlServer = new SqlServerChannelSql(Options.Create(new SqlServerAsyncResponseChannelOptions
        {
            ConnectionString = "Server=localhost;Database=unused;User Id=sa;Password=unused;TrustServerCertificate=True",
            PruneInterval = TimeSpan.FromMinutes(10)
        }));

        foreach (var store in new object[] { postgres, sqlServer })
        {
            var stamp = 0L;
            var before = Stopwatch.GetTimestamp();
            object?[] args = [stamp];
            Assert.True((bool)store.GetType().GetMethod("ShouldPrune", BindingFlags.NonPublic | BindingFlags.Instance)!.Invoke(store, args)!);
            var after = Stopwatch.GetTimestamp();

            Assert.InRange((long)args[0]!, before, after);
        }
    }

    private static readonly TimeSpan GenerousBudget = TimeSpan.FromMinutes(5);

    private static Task DrainAsync(
        Type anchor,
        Func<CancellationToken, Task<int>> batch,
        ILogger logger,
        CancellationToken cancellationToken,
        TimeSpan? budget = null)
    {
        var method = Helper(anchor).GetMethod("DrainQuietlyAsync", BindingFlags.Public | BindingFlags.Static)!;
        try
        {
            return (Task)method.Invoke(null, [batch, logger, "test prune", cancellationToken, budget])!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }
    }

    private static bool ShouldRun(Type anchor, ref long stamp, TimeSpan interval)
    {
        object?[] args = [stamp, interval];
        var result = (bool)Helper(anchor).GetMethod("ShouldRun", BindingFlags.Public | BindingFlags.Static)!.Invoke(null, args)!;
        stamp = (long)args[0]!;
        return result;
    }

    private static Type Helper(Type anchor)
        => anchor.Assembly.GetType("AsyncResponse.Internal.OpportunisticPrune", throwOnError: true)!;
}
