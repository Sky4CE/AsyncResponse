using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Core paths the rest of the suite never reaches: recovery entries with no correlation id, the
/// serial-executor registry's tombstone expiry, and a recovery bucket refilled after everything in
/// it expired.
/// </summary>
public sealed class CoreUncoveredPathTests
{
    private static readonly DateTime Now = new(2026, 6, 11, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan StaleAfter = TimeSpan.FromHours(24);

    /// <summary>
    /// Entries with no correlation id cannot be deduped against each other — each is its own
    /// registration — so they bypass the grouping and are classified individually.
    /// </summary>
    [Fact]
    public void Evaluate_ClassifiesCorrelationlessEntriesIndividually()
    {
        var report = AsyncResponseWatchdogReport.Evaluate(
            [
                new RecoveryStateObservation("", Now - TimeSpan.FromHours(30), 0, null),
                new RecoveryStateObservation("", Now - TimeSpan.FromHours(30), 0, null),
                new RecoveryStateObservation(null!, Now - TimeSpan.FromMinutes(5), 0, null),
                new RecoveryStateObservation(null!, null, 0, null),
                new RecoveryStateObservation("grouped", Now - TimeSpan.FromHours(30), 1, null)
            ],
            Now,
            StaleAfter);

        // Both correlationless stale entries survive as separate rows — grouping would have
        // collapsed them into one.
        Assert.Equal(5, report.TotalEntries);
        Assert.Equal(2, report.StaleEntries.Count);
        Assert.Equal(1, report.EntriesWithActiveWaiter);
        Assert.Equal(1, report.UnknownAgeEntries);
    }

    /// <summary>Dedupe keeps the oldest known registration; a known age always beats an unknown one.</summary>
    [Fact]
    public void IsOlder_PrefersTheOldestKnownRegistration()
    {
        var older = Observation(Now - TimeSpan.FromHours(2));
        var newer = Observation(Now - TimeSpan.FromHours(1));
        var unknown = Observation(null);

        Assert.True(AsyncResponseWatchdogReport.IsOlder(older, newer));
        Assert.False(AsyncResponseWatchdogReport.IsOlder(newer, older));
        Assert.True(AsyncResponseWatchdogReport.IsOlder(older, unknown));
        Assert.False(AsyncResponseWatchdogReport.IsOlder(unknown, older));
        Assert.False(AsyncResponseWatchdogReport.IsOlder(unknown, unknown));

        static RecoveryStateObservation Observation(DateTime? registeredAtUtc)
            => new("corr", registeredAtUtc, 0, null);
    }

    /// <summary>
    /// The scan carries correlationless entries through its own ungrouped list, and tags the scan
    /// activity with the resulting buckets.
    /// </summary>
    [Fact]
    public async Task Scan_ReportsCorrelationlessEntriesAndTagsTheActivity()
    {
        using var activities = new AsyncResponseActivityCollector();
        var state = new AsyncResponseWatchdogState();
        var watchdog = new AsyncResponseWatchdog(
            [new FakeScanner(
                new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "", RegisteredAtUtc = DateTime.UtcNow.AddDays(-3) },
                null,
                new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "grouped", RegisteredAtUtc = DateTime.UtcNow.AddDays(-3) })],
            [new FakeProbe(0)],
            state,
            Options.Create(new AsyncResponseOptions
            {
                Watchdog = new AsyncResponseWatchdogOptions
                {
                    Enabled = true,
                    StartupDelay = TimeSpan.Zero,
                    // The first scan runs immediately after StartupDelay; Interval only paces the
                    // SECOND. Keep it far beyond the test's lifetime so exactly one scan can ever
                    // run — with a 20 ms interval, a loaded runner could squeeze a second scan in
                    // before StopAsync, and the exactly-one activity assertion below flaked
                    // (observed on CI's macos-latest).
                    Interval = TimeSpan.FromMinutes(5),
                    StaleAfter = StaleAfter
                }
            }),
            NullLogger<AsyncResponseWatchdog>.Instance);

        await watchdog.StartAsync(CancellationToken.None);
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (state.Latest is null && DateTime.UtcNow < deadline)
                await Task.Delay(15);

            var snapshot = Assert.IsType<AsyncResponseWatchdogSnapshot>(state.Latest);
            Assert.Null(snapshot.Error);
            var report = Assert.IsType<AsyncResponseWatchdogReport>(snapshot.Report);
            // The null entry is skipped; the correlationless one is kept alongside the grouped one.
            Assert.Equal(2, report.TotalEntries);
            // Only the grouped entry is stale: liveness cannot be probed without a correlation id,
            // so the correlationless one reports unknown liveness and is deliberately not flagged.
            Assert.Equal("grouped", Assert.Single(report.StaleEntries).CorrelationId);
        }
        finally
        {
            await watchdog.StopAsync(CancellationToken.None);
        }

        activities.Single("asyncresponse.watchdog.scan", "asyncresponse.watchdog.total_entries", 2);
    }

    /// <summary>Retiring a subscription the registry never saw is a no-op, not a throw.</summary>
    [Fact]
    public void OnSubscriptionRetired_IgnoresAnUnknownChannel()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        registry.OnSubscriptionRetired("never-registered");

        // Registration counting still works afterwards.
        registry.OnSubscriptionRegistered("channel");
        registry.OnSubscriptionRetired("channel");
        registry.OnSubscriptionRetired("channel");
    }

    /// <summary>
    /// A tombstone only blocks executor re-creation for its lifetime. Once expired it is dropped on
    /// sight and the enqueue proceeds — otherwise a reused correlation id would be silently starved.
    /// </summary>
    [Fact]
    public async Task Enqueue_DropsAnExpiredTombstoneAndRunsTheWork()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        // The lifetime is a fixed 30s constant, so the only way to reach the expiry branch in a
        // test is to plant an already-expired tombstone.
        Tombstones(registry)["stale-channel"] = DateTime.UtcNow.AddMinutes(-1);

        var ran = false;
        await registry.EnqueueAsync("stale-channel", () =>
        {
            ran = true;
            return Task.CompletedTask;
        });
        await registry.RemoveAsync("stale-channel");

        // The work ran, so the expired tombstone was dropped rather than blocking the enqueue.
        // Retiring the executor afterwards leaves a fresh (unexpired) tombstone in its place.
        Assert.True(ran);
        Assert.True(Tombstones(registry)["stale-channel"] > DateTime.UtcNow);
    }

    /// <summary>Retirement sweeps expired tombstones and leaves live ones alone.</summary>
    [Fact]
    public async Task Retirement_PrunesOnlyTheExpiredTombstones()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        await registry.EnqueueAsync("worked", () => Task.CompletedTask);

        var tombstones = Tombstones(registry);
        tombstones["expired"] = DateTime.UtcNow.AddMinutes(-1);
        tombstones["live"] = DateTime.UtcNow.AddMinutes(5);

        // Retiring an executor is what triggers the sweep.
        await registry.RemoveAsync("worked");

        Assert.DoesNotContain("expired", tombstones.Keys);
        Assert.Contains("live", tombstones.Keys);
    }

    /// <summary>
    /// A bucket whose entries have all expired is pruned to empty and then refilled by the save in
    /// flight, rather than the save being lost against a stale compare operand.
    /// </summary>
    [Fact]
    public async Task RecoveryStore_RefillsABucketWhoseEntriesAllExpired()
    {
        var store = new InMemoryRecoveryStateStore();
        var first = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr" };
        await store.SaveAsync("corr", first, TimeSpan.FromMilliseconds(1));

        // Let the only entry lapse, so the next save prunes the bucket to empty and upserts into it.
        await Task.Delay(30);

        var second = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr" };
        await store.SaveAsync("corr", second, TimeSpan.FromMinutes(5));

        var states = await store.GetAllAsync("corr");
        Assert.Equal(second.RegistrationId, Assert.Single(states).RegistrationId);
    }

    private static Dictionary<string, DateTime> Tombstones(SerialExecutorRegistry registry)
        => (Dictionary<string, DateTime>)typeof(SerialExecutorRegistry)
            .GetField("_tombstones", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(registry)!;

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

    private sealed class FakeProbe(long activeSubscribers) : IActiveSubscriberProbe
    {
        public ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(activeSubscribers);
    }
}
