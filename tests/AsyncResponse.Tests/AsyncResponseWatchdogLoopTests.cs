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
    // Generous: this is a wait-until budget (loops exit the moment the snapshot publishes,
    // so healthy runs stay fast), and 5s starved out on oversubscribed 2-core CI runners.
    private static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(30);

    [Fact]
    public void Construction_OverCeilingInterval_FailsFastAtStartup()
    {
        // Regression (review fix): Interval arms Task.Delay in the scan loop but was never
        // validated — an over-ceiling value passed registration and threw mid-run, faulting the
        // background service (and, with the .NET default StopHost behavior, the whole host).
        var options = Microsoft.Extensions.Options.Options.Create(new AsyncResponseOptions
        {
            Watchdog = new AsyncResponseWatchdogOptions { Interval = TimeSpan.FromDays(60) }
        });

        var ex = Assert.Throws<InvalidOperationException>(() =>
            Build(new AsyncResponseWatchdogState(), options, new FakeScanner(), new FakeProbe(0)));
        Assert.Contains(nameof(AsyncResponseWatchdogOptions.Interval), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Construction_NegativeStartupDelay_FailsFastAtStartup()
    {
        // Same regression, StartupDelay flavor: it arms the first Task.Delay of the loop, and a
        // negative value used to fault the service instead of failing at construction.
        var options = Microsoft.Extensions.Options.Options.Create(new AsyncResponseOptions
        {
            Watchdog = new AsyncResponseWatchdogOptions { StartupDelay = TimeSpan.FromSeconds(-1) }
        });

        var ex = Assert.Throws<InvalidOperationException>(() =>
            Build(new AsyncResponseWatchdogState(), options, new FakeScanner(), new FakeProbe(0)));
        Assert.Contains(nameof(AsyncResponseWatchdogOptions.StartupDelay), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Construction_NonPositiveProbeConcurrency_FailsFastAtStartup()
    {
        // ProbeConcurrency feeds Parallel.ForEachAsync mid-scan, which rejects values below 1
        // with the same delayed, host-stopping failure mode as an out-of-range delay.
        var options = Microsoft.Extensions.Options.Options.Create(new AsyncResponseOptions
        {
            Watchdog = new AsyncResponseWatchdogOptions { ProbeConcurrency = 0 }
        });

        var ex = Assert.Throws<InvalidOperationException>(() =>
            Build(new AsyncResponseWatchdogState(), options, new FakeScanner(), new FakeProbe(0)));
        Assert.Contains(nameof(AsyncResponseWatchdogOptions.ProbeConcurrency), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Disabled_DoesNotScanOrPublish_AndMarksItsStateIdle()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: false), new FakeScanner(StaleEntry()), new FakeProbe(0));

        await watchdog.StartAsync(CancellationToken.None);
        try
        {
            // The idle marker doubles as "ExecuteAsync has run" (hosting may defer it past StartAsync).
            await WaitUntilAsync(() => state.Scanning is false);
            await Task.Delay(200); // give a (disabled) loop ample time to misbehave
            Assert.Null(state.Latest);
            Assert.Contains(nameof(AsyncResponseWatchdogOptions.Enabled), state.IdleReason!, StringComparison.Ordinal);
        }
        finally
        {
            await watchdog.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task NoScanner_StaysIdle_AndMarksItsStateIdle()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: true), scanner: null, new FakeProbe(0));

        await watchdog.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntilAsync(() => state.Scanning is false);
            await Task.Delay(200);
            Assert.Null(state.Latest);
            Assert.Contains(nameof(IRecoveryStateScanner), state.IdleReason!, StringComparison.Ordinal);
        }
        finally
        {
            await watchdog.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task ScanningWatchdog_MarksItsStateScanning()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: true), new FakeScanner(StaleEntry()), new FakeProbe(0));

        await RunUntilPublishedAsync(watchdog, state);

        Assert.True(state.Scanning);
        Assert.Null(state.IdleReason);
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
    public async Task YoungEntryWithNoSubscriber_IsNotStale()
    {
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: true), new FakeScanner(new RecoveryState
        {
            CorrelationId = "young-cid",
            PayloadTypeFullName = typeof(OperationResult).FullName,
            RegisteredAtUtc = DateTime.UtcNow
        }), new FakeProbe(0));

        var snapshot = await RunUntilPublishedAsync(watchdog, state);

        Assert.NotNull(snapshot.Report);
        Assert.Equal(1, snapshot.Report!.TotalEntries);
        Assert.Empty(snapshot.Report.StaleEntries);
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
    public async Task CanceledProbe_AbortsTheScan_WithoutPublishingASnapshot()
    {
        // Regression (review fix): a probe canceled by shutdown used to be swallowed into the
        // generic -1 (unknown) fallback, so the scan ground through every remaining entry
        // (throw/catch per id, delaying shutdown) and then published a snapshot attesting a
        // completed pass whose liveness was never probed. The cancellation must abort the pass.
        var state = new AsyncResponseWatchdogState();
        var entries = Enumerable.Range(0, 50).Select(i => StaleEntry($"cid-{i:D2}")).ToArray();
        var probe = new CancellationObservingProbe();
        // Strictly sequential probing keeps "the third call parks" deterministic; the parallel
        // fan-out's cancellation propagation has its own coverage.
        var watchdog = Build(state, Options(enabled: true, probeConcurrency: 1), new FakeScanner(entries), probe);

        await watchdog.StartAsync(CancellationToken.None);
        try
        {
            await probe.ThirdCallReached.Task.WaitAsync(PublishTimeout);
        }
        finally
        {
            // Cancels the scan token while the third probe call is parked on it; that call then
            // surfaces OperationCanceledException exactly as a canceled store client would.
            await watchdog.StopAsync(CancellationToken.None);
        }

        Assert.True(probe.Calls <= 5, $"expected the canceled probe to abort the scan, but {probe.Calls} of the 50 entries were probed");
        Assert.Null(state.Latest);
    }

    private sealed class CancellationObservingProbe : IActiveSubscriberProbe
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public TaskCompletionSource ThirdCallReached { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _calls) < 3)
                return 0;

            ThirdCallReached.TrySetResult();
            // Parks until StopAsync cancels the scan token; the resulting TaskCanceledException
            // is an OperationCanceledException carrying that token.
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return 0;
        }
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
        Assert.Equal(1, snapshot.Report.UnprobeableEntries);
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
        Assert.Equal(1, snapshot.Report.UnprobeableEntries);
    }

    [Fact]
    public async Task ProbeConcurrencyOne_ProbesStrictlySequentially()
    {
        var state = new AsyncResponseWatchdogState();
        var probe = new OverlapObservingProbe();
        var entries = Enumerable.Range(0, 4).Select(i => StaleEntry($"seq-{i}")).ToArray();
        var watchdog = Build(state, Options(enabled: true, probeConcurrency: 1), new FakeScanner(entries), probe);

        var snapshot = await RunUntilPublishedAsync(watchdog, state);

        Assert.Equal(4, snapshot.Report!.TotalEntries);
        Assert.Equal(1, probe.MaxInFlight);
    }

    /// <summary>Tracks how many probe calls overlap; each call lingers long enough to be seen.</summary>
    private sealed class OverlapObservingProbe : IActiveSubscriberProbe
    {
        private int _inFlight;
        private int _maxInFlight;

        public int MaxInFlight => Volatile.Read(ref _maxInFlight);

        public async ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
        {
            var inFlight = Interlocked.Increment(ref _inFlight);
            int seen;
            while (inFlight > (seen = Volatile.Read(ref _maxInFlight))
                   && Interlocked.CompareExchange(ref _maxInFlight, inFlight, seen) != seen)
            {
            }

            try
            {
                await Task.Delay(50, cancellationToken);
                return 1;
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }
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

    [Fact]
    public async Task ScanCancellation_OnStop_IsHandledGracefully()
    {
        var state = new AsyncResponseWatchdogState();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var scanner = new CancellationScanner(entered);
        var watchdog = Build(state, Options(enabled: true), scanner, new FakeProbe(0));

        await watchdog.StartAsync(CancellationToken.None);
        try
        {
            await entered.Task.WaitAsync(PublishTimeout);
        }
        finally
        {
            await watchdog.StopAsync(CancellationToken.None);
        }

        Assert.Null(state.Latest);
    }

    private sealed class CancellationScanner(TaskCompletionSource entered) : IRecoveryStateScanner
    {
        public async IAsyncEnumerable<RecoveryState> ScanAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            yield break;
        }
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

    private static IOptions<AsyncResponseOptions> Options(bool enabled, int probeConcurrency = 8) => Microsoft.Extensions.Options.Options.Create(
        new AsyncResponseOptions
        {
            Watchdog = new AsyncResponseWatchdogOptions
            {
                Enabled = enabled,
                StartupDelay = TimeSpan.Zero,
                // These tests inspect the first publication; keep the next scan outside that window.
                Interval = PublishTimeout + PublishTimeout,
                StaleAfter = TimeSpan.FromMinutes(1),
                ProbeConcurrency = probeConcurrency
            }
        });

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + PublishTimeout;
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException("Condition not reached in time.");
            await Task.Delay(20);
        }
    }

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
