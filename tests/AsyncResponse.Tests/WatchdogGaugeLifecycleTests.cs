using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Diagnostics.Metrics;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Serialized against the rest of the suite: the recovery gauges read one process-wide state
/// holder, and test classes running their own watchdogs in parallel would swap it mid-assertion.
/// </summary>
[CollectionDefinition(nameof(WatchdogGaugeLifecycleTests), DisableParallelization = true)]
public sealed class WatchdogGaugeLifecycleCollection;

/// <summary>
/// Which watchdog the process-wide recovery gauges report across host lifecycles: only a watchdog
/// that actually scans may take over the holder, a stopping host releases it so the gauges cannot
/// pin a disposed host's final snapshot forever, and that release never clears a successor's state.
/// </summary>
[Collection(nameof(WatchdogGaugeLifecycleTests))]
public sealed class WatchdogGaugeLifecycleTests
{
    private static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task DisabledWatchdog_DoesNotHijackTheGaugeHolder()
    {
        // Multi-host guidance says "set Watchdog.Enabled = false in all but one host". The
        // disabled host's state stays empty forever, so letting it take over the holder would
        // permanently zero the gauges for the host that scans.
        var scanningState = new AsyncResponseWatchdogState();
        var scanning = Build(scanningState, Options(enabled: true), new FakeScanner(Entries(3)), new FakeProbe(1));
        await StartAndAwaitFirstSnapshotAsync(scanning, scanningState);
        try
        {
            Assert.Equal(3, ReadGauge("asyncresponse.recovery.outstanding"));

            var idle = Build(new AsyncResponseWatchdogState(), Options(enabled: false), new FakeScanner(Entries(1)), new FakeProbe(1));
            await idle.StartAsync(CancellationToken.None);
            try
            {
                await Task.Delay(200); // ample time for the (disabled) watchdog to misbehave
                Assert.Equal(3, ReadGauge("asyncresponse.recovery.outstanding"));
            }
            finally
            {
                await idle.StopAsync(CancellationToken.None);
                idle.Dispose();
            }

            Assert.Equal(3, ReadGauge("asyncresponse.recovery.outstanding"));
        }
        finally
        {
            await scanning.StopAsync(CancellationToken.None);
            scanning.Dispose();
        }
    }

    [Fact]
    public async Task StoppedWatchdog_ReleasesTheGaugeHolder_SoGaugesReadZero()
    {
        // After the last host stops there is no live scan to attest; the still-registered gauges
        // must read zero rather than exporting the disposed host's final snapshot (and keeping
        // its stale-entry list GC-rooted) for process lifetime.
        var state = new AsyncResponseWatchdogState();
        var watchdog = Build(state, Options(enabled: true), new FakeScanner(Entries(2)), new FakeProbe(1));
        await StartAndAwaitFirstSnapshotAsync(watchdog, state);

        Assert.Equal(2, ReadGauge("asyncresponse.recovery.outstanding"));

        await watchdog.StopAsync(CancellationToken.None);
        watchdog.Dispose();

        Assert.Equal(0, ReadGauge("asyncresponse.recovery.outstanding"));
    }

    [Fact]
    public async Task PredecessorStop_DoesNotClearTheSuccessorsState()
    {
        var firstState = new AsyncResponseWatchdogState();
        var first = Build(firstState, Options(enabled: true), new FakeScanner(Entries(1)), new FakeProbe(1));
        await StartAndAwaitFirstSnapshotAsync(first, firstState);

        var secondState = new AsyncResponseWatchdogState();
        var second = Build(secondState, Options(enabled: true), new FakeScanner(Entries(5)), new FakeProbe(1));
        await StartAndAwaitFirstSnapshotAsync(second, secondState);
        try
        {
            Assert.Equal(5, ReadGauge("asyncresponse.recovery.outstanding"));

            // The release is identity-conditional: the predecessor no longer owns the holder.
            await first.StopAsync(CancellationToken.None);
            first.Dispose();

            Assert.Equal(5, ReadGauge("asyncresponse.recovery.outstanding"));
        }
        finally
        {
            await second.StopAsync(CancellationToken.None);
            second.Dispose();
        }

        Assert.Equal(0, ReadGauge("asyncresponse.recovery.outstanding"));
    }

    /// <summary>Reads the current value of one AsyncResponse observable gauge.</summary>
    private static long ReadGauge(string instrumentName)
    {
        long? value = null;
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == AsyncResponseDiagnostics.MeterName && instrument.Name == instrumentName)
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) => value = measurement);
        listener.Start();
        listener.RecordObservableInstruments();

        Assert.NotNull(value); // the gauge is registered once a watchdog begins scanning
        return value!.Value;
    }

    private static AsyncResponseWatchdog Build(
        AsyncResponseWatchdogState state,
        IOptions<AsyncResponseOptions> options,
        IRecoveryStateScanner scanner,
        IActiveSubscriberProbe probe)
        => new([scanner], [probe], state, options, NullLogger<AsyncResponseWatchdog>.Instance);

    private static IOptions<AsyncResponseOptions> Options(bool enabled) => Microsoft.Extensions.Options.Options.Create(
        new AsyncResponseOptions
        {
            Watchdog = new AsyncResponseWatchdogOptions
            {
                Enabled = enabled,
                StartupDelay = TimeSpan.Zero,
                // These tests inspect the first publication; keep the next scan outside that window.
                Interval = PublishTimeout + PublishTimeout,
                StaleAfter = TimeSpan.FromMinutes(1)
            }
        });

    private static RecoveryState[] Entries(int count)
        => [.. Enumerable.Range(0, count).Select(i => new RecoveryState
        {
            CorrelationId = $"gauge-cid-{count}-{i}",
            PayloadTypeFullName = typeof(OperationResult).FullName,
            RegisteredAtUtc = DateTime.UtcNow
        })];

    private static async Task StartAndAwaitFirstSnapshotAsync(AsyncResponseWatchdog watchdog, AsyncResponseWatchdogState state)
    {
        await watchdog.StartAsync(CancellationToken.None);
        var deadline = DateTime.UtcNow + PublishTimeout;
        while (state.Latest is null)
        {
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException("Watchdog did not publish a snapshot in time.");
            await Task.Delay(20);
        }
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

    private sealed class FakeProbe(long activeSubscribers) : IActiveSubscriberProbe
    {
        public ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(activeSubscribers);
    }
}
