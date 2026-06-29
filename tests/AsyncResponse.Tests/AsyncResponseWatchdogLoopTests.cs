using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The watchdog background loop drives the scan: it stays idle when disabled or when the channel
/// has no scanner, publishes a snapshot flagging stale (no-subscriber, aged) entries, treats an
/// entry with a live waiter as healthy, and reports a scan failure via the snapshot's error rather
/// than crashing the host.
/// </summary>
public class AsyncResponseWatchdogLoopTests
{
    private static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task Disabled_DoesNotScanOrPublish()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: false), new FakeScanner(StaleEntry()), new FakeProbe(0));

        await watchdog.StartAsync(CancellationToken.None);
        try
        {
            await Task.Delay(200); // give a (disabled) loop ample time to misbehave
            Assert.Null(state.Latest);
        }
        finally
        {
            await watchdog.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task NoScanner_StaysIdle()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: true), scanner: null, new FakeProbe(0));

        await watchdog.StartAsync(CancellationToken.None);
        try
        {
            await Task.Delay(200);
            Assert.Null(state.Latest);
        }
        finally
        {
            await watchdog.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task AgedEntryWithNoSubscriber_IsReportedStale()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: true), new FakeScanner(StaleEntry("stuck-cid")), new FakeProbe(0));

        var snapshot = await RunUntilPublishedAsync(watchdog, state);

        Assert.Null(snapshot.Error);
        Assert.NotNull(snapshot.Report);
        var stale = Assert.Single(snapshot.Report!.StaleEntries);
        Assert.Equal("stuck-cid", stale.CorrelationId);
    }

    [Fact]
    public async Task AgedEntryWithLiveWaiter_IsNotStale()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: true), new FakeScanner(StaleEntry()), new FakeProbe(activeSubscribers: 1));

        var snapshot = await RunUntilPublishedAsync(watchdog, state);

        Assert.NotNull(snapshot.Report);
        Assert.Empty(snapshot.Report!.StaleEntries);
        Assert.Equal(1, snapshot.Report.EntriesWithActiveWaiter);
    }

    [Fact]
    public async Task EntryWithoutRegisteredAt_IsReportedAsUnknownAge()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: true), new FakeScanner(new RecoveryState
        {
            CorrelationId = "unknown-age",
            PayloadTypeFullName = typeof(OperationResult).FullName
        }), new FakeProbe(0));

        var snapshot = await RunUntilPublishedAsync(watchdog, state);

        Assert.NotNull(snapshot.Report);
        Assert.Equal(1, snapshot.Report!.UnknownAgeEntries);
        Assert.Empty(snapshot.Report.StaleEntries);
    }

    [Fact]
    public async Task NullScannerEntries_AreSkipped()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(
            state,
            Options(enabled: true),
            new FakeScanner(null, StaleEntry("live-cid")),
            new FakeProbe(activeSubscribers: 1));

        var snapshot = await RunUntilPublishedAsync(watchdog, state);

        Assert.NotNull(snapshot.Report);
        Assert.Equal(1, snapshot.Report!.TotalEntries);
        Assert.Equal(1, snapshot.Report.EntriesWithActiveWaiter);
    }

    [Fact]
    public async Task DuplicateCorrelationIds_AreProbedAndCountedOnce()
    {
        var state = new AsyncResponseWatchdogState();
        var probe = new FakeProbe(activeSubscribers: 0);
        var watchdog = Build(
            state,
            Options(enabled: true),
            new FakeScanner(StaleEntry("same-cid"), StaleEntry("same-cid")),
            probe);

        var snapshot = await RunUntilPublishedAsync(watchdog, state);

        Assert.NotNull(snapshot.Report);
        Assert.Equal(1, snapshot.Report!.TotalEntries);
        Assert.Single(snapshot.Report.StaleEntries);
        Assert.Equal(1, probe.Calls);
    }


    [Fact]
    public async Task MissingSubscriberProbe_TreatsLivenessAsUnknown()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: true), new FakeScanner(StaleEntry()), probe: null);

        var snapshot = await RunUntilPublishedAsync(watchdog, state);

        Assert.NotNull(snapshot.Report);
        Assert.Equal(1, snapshot.Report!.TotalEntries);
        Assert.Empty(snapshot.Report.StaleEntries);
        Assert.Equal(0, snapshot.Report.EntriesWithActiveWaiter);
    }

    [Fact]
    public async Task SubscriberProbeFailure_TreatsLivenessAsUnknown()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: true), new FakeScanner(StaleEntry()), new ThrowingProbe());

        var snapshot = await RunUntilPublishedAsync(watchdog, state);

        Assert.NotNull(snapshot.Report);
        Assert.Equal(1, snapshot.Report!.TotalEntries);
        Assert.Empty(snapshot.Report.StaleEntries);
        Assert.Equal(0, snapshot.Report.EntriesWithActiveWaiter);
    }

    [Fact]
    public async Task ScanFailure_IsPublishedAsError()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: true), new ThrowingScanner(), new FakeProbe(0));

        var snapshot = await RunUntilPublishedAsync(watchdog, state);

        Assert.Null(snapshot.Report);
        Assert.NotNull(snapshot.Error);
        Assert.Contains("scan boom", snapshot.Error!, StringComparison.Ordinal);
    }

    private static AsyncResponseWatchdog Build(
        AsyncResponseWatchdogState state,
        IOptions<AsyncResponseOptions> options,
        IRecoveryStateScanner? scanner,
        IActiveSubscriberProbe? probe)
        => new(
            scanner is null ? [] : [scanner],
            probe is null ? [] : [probe],
            state,
            options,
            NullLogger<AsyncResponseWatchdog>.Instance);

    private static IOptions<AsyncResponseOptions> Options(bool enabled) => Microsoft.Extensions.Options.Options.Create(
        new AsyncResponseOptions
        {
            Watchdog = new AsyncResponseWatchdogOptions
            {
                Enabled = enabled,
                StartupDelay = TimeSpan.Zero,
                Interval = TimeSpan.FromMilliseconds(50),
                StaleAfter = TimeSpan.FromMinutes(1)
            }
        });

    private static RecoveryState StaleEntry(string correlationId = "cid") => new()
    {
        CorrelationId = correlationId,
        PayloadTypeFullName = typeof(OperationResult).FullName,
        RegisteredAtUtc = DateTime.UtcNow.AddHours(-1) // older than the 1-minute stale threshold
    };

    private static async Task<AsyncResponseWatchdogSnapshot> RunUntilPublishedAsync(
        AsyncResponseWatchdog watchdog, AsyncResponseWatchdogState state)
    {
        await watchdog.StartAsync(CancellationToken.None);
        try
        {
            var deadline = DateTime.UtcNow + PublishTimeout;
            while (DateTime.UtcNow < deadline)
            {
                if (state.Latest is { } snapshot)
                    return snapshot;
                await Task.Delay(20);
            }

            throw new TimeoutException("Watchdog did not publish a snapshot in time.");
        }
        finally
        {
            await watchdog.StopAsync(CancellationToken.None);
        }
    }

    private sealed class FakeScanner(params RecoveryState?[] states) : IRecoveryStateScanner
    {
        public async IAsyncEnumerable<RecoveryState> ScanAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var state in states)
                yield return state!;
            await Task.CompletedTask;
        }
    }

    private sealed class ThrowingScanner : IRecoveryStateScanner
    {
        public IAsyncEnumerable<RecoveryState> ScanAsync(CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("scan boom");
    }

    private sealed class FakeProbe(long activeSubscribers) : IActiveSubscriberProbe
    {
        public int Calls { get; private set; }

        public ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
        {
            Calls++;
            return ValueTask.FromResult(activeSubscribers);
        }
    }

    private sealed class ThrowingProbe : IActiveSubscriberProbe
    {
        public ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("probe boom");
    }
}
