using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// What the recovery health check attests end-to-end through the watchdog: a probe outage or a
/// scan loop that never publishes must not read as a clean pass, a deliberately idle host says so
/// explicitly, the reported scan interval survives sub-minute values, and liveness probes fan out
/// instead of serializing one round trip per entry.
/// </summary>
public class WatchdogScanAttestationTests
{
    private static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task ProbeOutage_DegradesTheHealthCheck_InsteadOfAttestingACleanPass()
    {
        // Unknown liveness is never flagged stale (no false alarms), so with every probe failing
        // the stale counter is zero; the check must say "could not assess", not "no stale state".
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(), new FakeScanner(StaleEntry("outage-cid")), new NegativeProbe());

        var snapshot = await RunUntilPublishedAsync(watchdog, state);
        Assert.NotNull(snapshot.Report);

        var result = await new AsyncResponseRecoveryHealthCheck(state).CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Contains("probe", result.Description!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DisabledHost_HealthCheckReportsThisHostDoesNotScan()
    {
        // The documented multi-host pattern disables the watchdog on all but one host. That
        // host's check must stay Healthy (alert-quiet) while attesting explicitly that it does
        // not scan, instead of a "no scan yet" that never becomes true.
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: false), new FakeScanner(StaleEntry()), new FakeProbe(0));

        await watchdog.StartAsync(CancellationToken.None);
        try
        {
            var result = await CheckUntilAsync(state, r => r.Data.ContainsKey("scanning"), TimeSpan.FromSeconds(5));

            Assert.Equal(HealthStatus.Healthy, result.Status);
            Assert.Contains("does not scan", result.Description!, StringComparison.Ordinal);
            Assert.Equal(false, result.Data["scanning"]);
            Assert.IsType<string>(result.Data["reason"]);
        }
        finally
        {
            await watchdog.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task ArmedScanLoopThatNeverPublishes_DegradesPastItsFirstScanBudget()
    {
        // A watchdog whose very first scan hangs (or whose loop died before publishing) leaves
        // the snapshot null forever; "no scan yet" must stop masking it once the startup delay
        // plus two intervals have passed without a publication.
        var clock = new TestTimeProvider();
        var state = new AsyncResponseWatchdogState();
        var scannerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var watchdog = Build(
            state,
            Options(interval: TimeSpan.FromMinutes(30), startupDelay: TimeSpan.Zero),
            new HangingScanner(scannerEntered),
            new FakeProbe(0),
            clock);

        await watchdog.StartAsync(CancellationToken.None);
        try
        {
            await scannerEntered.Task.WaitAsync(PublishTimeout);
            var healthCheck = new AsyncResponseRecoveryHealthCheck(state, clock);

            var withinBudget = await healthCheck.CheckHealthAsync(new HealthCheckContext());
            Assert.Equal(HealthStatus.Healthy, withinBudget.Status);

            clock.Advance(TimeSpan.FromMinutes(61)); // past startup delay (0) + 2 x 30min

            var overdue = await healthCheck.CheckHealthAsync(new HealthCheckContext());
            Assert.Equal(HealthStatus.Degraded, overdue.Status);
            Assert.Contains("first scan", overdue.Description!, StringComparison.OrdinalIgnoreCase);
            Assert.Null(state.Latest);
        }
        finally
        {
            await watchdog.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task SubMinuteScanInterval_IsReportedLosslesslyInHealthData()
    {
        // Alert math derived from a whole-minutes payload divides by zero (or trips constantly)
        // for any sub-minute interval; the payload must carry the interval without truncation.
        var state = new AsyncResponseWatchdogState();
        state.Publish(new AsyncResponseWatchdogSnapshot(
            DateTime.UtcNow,
            TimeSpan.FromSeconds(90),
            new AsyncResponseWatchdogReport(1, 1, [], 0),
            Error: null));

        var result = await new AsyncResponseRecoveryHealthCheck(state).CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
        Assert.Equal(90d, Assert.IsType<double>(result.Data["scanIntervalSeconds"]));
        Assert.Equal(TimeSpan.FromSeconds(90).ToString(), result.Data["scanInterval"]);
        Assert.False(result.Data.ContainsKey("scanIntervalMinutes"));
    }

    [Fact]
    public async Task LivenessProbes_FanOutConcurrently()
    {
        // Each probe is its own channel round trip; strictly sequential awaits serialize up to
        // MaxScanEntries of them per scan. Each probe call parks until a second call is in
        // flight (with a fallback so a sequential scan finishes and fails the assertion).
        var state = new AsyncResponseWatchdogState();
        var probe = new ConcurrencyTrackingProbe();
        var entries = Enumerable.Range(0, 4).Select(i => StaleEntry($"fan-{i}")).ToArray();
        var watchdog = Build(state, Options(), new FakeScanner(entries), probe);

        var snapshot = await RunUntilPublishedAsync(watchdog, state);

        Assert.Equal(4, snapshot.Report!.TotalEntries);
        Assert.True(probe.MaxInFlight >= 2, $"expected concurrent probes, but at most {probe.MaxInFlight} was in flight");
    }

    [Fact]
    public async Task ConcurrentProbes_ClassifyExactlyLikeSequentialProbes()
    {
        // The fan-out must preserve the per-id semantics: probe failure means unknown liveness
        // (never stale), a live count keeps the entry healthy, zero plus age means stale.
        var state = new AsyncResponseWatchdogState();
        var probe = new MappedProbe(new Dictionary<string, long?>(StringComparer.Ordinal)
        {
            ["stale-a"] = 0,
            ["live-b"] = 3,
            ["down-c"] = null // probe throws for this id
        });
        var watchdog = Build(
            state,
            Options(),
            new FakeScanner(StaleEntry("stale-a"), StaleEntry("live-b"), StaleEntry("down-c")),
            probe);

        var snapshot = await RunUntilPublishedAsync(watchdog, state);

        Assert.NotNull(snapshot.Report);
        Assert.Equal(3, snapshot.Report!.TotalEntries);
        Assert.Equal(1, snapshot.Report.EntriesWithActiveWaiter);
        Assert.Equal("stale-a", Assert.Single(snapshot.Report.StaleEntries).CorrelationId);
    }

    private static AsyncResponseWatchdog Build(
        AsyncResponseWatchdogState state,
        IOptions<AsyncResponseOptions> options,
        IRecoveryStateScanner? scanner,
        IActiveSubscriberProbe? probe,
        TimeProvider? timeProvider = null)
        => new(
            scanner is null ? [] : [scanner],
            probe is null ? [] : [probe],
            state,
            options,
            NullLogger<AsyncResponseWatchdog>.Instance,
            timeProvider);

    private static IOptions<AsyncResponseOptions> Options(
        bool enabled = true, TimeSpan? interval = null, TimeSpan? startupDelay = null)
        => Microsoft.Extensions.Options.Options.Create(new AsyncResponseOptions
        {
            Watchdog = new AsyncResponseWatchdogOptions
            {
                Enabled = enabled,
                StartupDelay = startupDelay ?? TimeSpan.Zero,
                // These tests inspect the first publication; keep the next scan outside that window.
                Interval = interval ?? PublishTimeout + PublishTimeout,
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

    /// <summary>Re-evaluates the health check until it matches (hosting may defer ExecuteAsync past StartAsync).</summary>
    private static async Task<HealthCheckResult> CheckUntilAsync(
        AsyncResponseWatchdogState state, Func<HealthCheckResult, bool> ready, TimeSpan budget)
    {
        var healthCheck = new AsyncResponseRecoveryHealthCheck(state);
        var deadline = DateTime.UtcNow + budget;
        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        while (!ready(result) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20);
            result = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        }

        return result;
    }

    private sealed class FakeScanner(params RecoveryState[] states) : IRecoveryStateScanner
    {
        public async IAsyncEnumerable<RecoveryState> ScanAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var state in states)
                yield return state;
            await Task.CompletedTask;
        }
    }

    private sealed class HangingScanner(TaskCompletionSource entered) : IRecoveryStateScanner
    {
        public async IAsyncEnumerable<RecoveryState> ScanAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            yield break;
        }
    }

    private sealed class FakeProbe(long activeSubscribers) : IActiveSubscriberProbe
    {
        public ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(activeSubscribers);
    }

    /// <summary>A probe that cannot determine liveness for any id, as during a broker outage.</summary>
    private sealed class NegativeProbe : IActiveSubscriberProbe
    {
        public ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(-1L);
    }

    /// <summary>Canned per-id results; a null mapping makes the probe throw for that id.</summary>
    private sealed class MappedProbe(Dictionary<string, long?> results) : IActiveSubscriberProbe
    {
        public ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
            => results[correlationId] is { } count
                ? ValueTask.FromResult(count)
                : throw new InvalidOperationException($"probe down for {correlationId}");
    }

    /// <summary>Parks each call until two are in flight, recording the maximum observed overlap.</summary>
    private sealed class ConcurrencyTrackingProbe : IActiveSubscriberProbe
    {
        private readonly TaskCompletionSource _pairInFlight = new(TaskCreationOptions.RunContinuationsAsynchronously);
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

            if (inFlight >= 2)
                _pairInFlight.TrySetResult();

            try
            {
                // The fallback lets a strictly sequential scan complete (and fail the assertion)
                // instead of deadlocking on a pair that can never form.
                await Task.WhenAny(_pairInFlight.Task, Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None));
                return 0;
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }
    }
}
