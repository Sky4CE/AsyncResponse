using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The readiness surface of the watchdog: stale recovery state must show up as Degraded (an
/// operator signal; map Degraded to HTTP 200 on readiness endpoints) and never as Unhealthy —
/// a stuck business flow must not pull a healthy process out of rotation.
/// </summary>
public class RecoveryHealthCheckTests
{
    private static readonly TimeSpan Interval = TimeSpan.FromHours(6);

    [Fact]
    public async Task NoScanYet_ReportsHealthy()
    {
        var result = await CheckAsync(state => { });

        Assert.Equal(HealthStatus.Healthy, result.Status);
        Assert.Contains("not completed a scan", result.Description, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CleanScan_ReportsHealthyWithStats()
    {
        var result = await CheckAsync(state => state.Publish(Snapshot(CleanReport(totalEntries: 3, withWaiter: 2))));

        Assert.Equal(HealthStatus.Healthy, result.Status);
        Assert.True(result.Data.ContainsKey("stats"));
        Assert.True(result.Data.ContainsKey("lastScanUtc"));
    }

    [Fact]
    public async Task StaleEntries_ReportDegradedWithDetails()
    {
        var stale = new RecoveryStateObservation("corr-stuck-1", DateTime.UtcNow - TimeSpan.FromDays(2), 0, typeof(OperationResult).FullName);
        var report = new AsyncResponseWatchdogReport(5, 1, [stale], 0);

        var result = await CheckAsync(state => state.Publish(Snapshot(report)));

        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Contains("1 async-response flow(s) look stuck", result.Description, StringComparison.Ordinal);
        Assert.True(result.Data.ContainsKey("staleEntries"));
    }

    [Fact]
    public async Task ScanFailure_ReportsDegraded()
    {
        var result = await CheckAsync(state => state.Publish(new AsyncResponseWatchdogSnapshot(
            DateTime.UtcNow, Interval, Report: null, Error: "redis unavailable")));

        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Contains("redis unavailable", result.Description, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WatchdogStoppedPublishing_ReportsDegraded()
    {
        // Snapshot older than twice the scan interval: the watchdog loop is likely dead.
        var result = await CheckAsync(state => state.Publish(new AsyncResponseWatchdogSnapshot(
            DateTime.UtcNow - TimeSpan.FromHours(13), Interval, CleanReport(0, 0), Error: null)));

        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Contains("stopped reporting", result.Description, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ManyStaleEntries_AreCappedInPayload()
    {
        var staleEntries = Enumerable.Range(1, 12)
            .Select(i => new RecoveryStateObservation($"corr-{i}", DateTime.UtcNow - TimeSpan.FromDays(2), 0, null))
            .ToList();
        var report = new AsyncResponseWatchdogReport(12, 0, staleEntries, 0);

        var result = await CheckAsync(state => state.Publish(Snapshot(report)));

        Assert.Equal(HealthStatus.Degraded, result.Status);
        var listed = Assert.IsAssignableFrom<System.Collections.IEnumerable>(result.Data["staleEntries"]);
        Assert.Equal(10, listed.Cast<object>().Count());
        Assert.Equal(2, result.Data["staleEntriesTruncated"]);
    }

    [Fact]
    public async Task AddAsyncResponseRecoveryCheck_RegistersNamedHealthCheckAndState()
    {
        var services = new ServiceCollection();

        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddHealthChecks().AddAsyncResponseRecoveryCheck("async-response-custom", ["ready"]);
        await using var provider = services.BuildServiceProvider();

        var state = provider.GetRequiredService<AsyncResponseWatchdogState>();
        Assert.Same(state, provider.GetRequiredService<AsyncResponseWatchdogState>());

        var health = await provider
            .GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(_ => true);

        Assert.Equal(HealthStatus.Healthy, health.Status);
        var entry = Assert.Single(health.Entries);
        Assert.Equal("async-response-custom", entry.Key);
        Assert.Equal(HealthStatus.Healthy, entry.Value.Status);
    }

    [Fact]
    public async Task TruncatedScanWithZeroStale_ReportsDegraded()
    {
        // Zero stale entries in a truncated scan is not a verdict — arbitrarily many stale
        // entries can sit past the buffer cap, so the check must not attest Healthy.
        var result = await CheckAsync(state => state.Publish(Snapshot(
            CleanReport(totalEntries: 2, withWaiter: 2) with { Truncated = true })));

        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Contains("truncated", result.Description!, StringComparison.OrdinalIgnoreCase);
    }

    private static async Task<HealthCheckResult> CheckAsync(Action<AsyncResponseWatchdogState> arrange)
    {
        var state = new AsyncResponseWatchdogState();
        arrange(state);
        var healthCheck = new AsyncResponseRecoveryHealthCheck(state);

        return await healthCheck.CheckHealthAsync(new HealthCheckContext());
    }

    private static AsyncResponseWatchdogSnapshot Snapshot(AsyncResponseWatchdogReport report)
        => new(DateTime.UtcNow, Interval, report, Error: null);

    private static AsyncResponseWatchdogReport CleanReport(int totalEntries, int withWaiter)
        => new(totalEntries, withWaiter, [], UnknownAgeEntries: 0);
}
