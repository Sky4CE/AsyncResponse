using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace AsyncResponse;

/// <summary>
/// Surfaces the async-response watchdog findings on the health endpoints (e.g. <c>/readyz</c>).
/// <para>
/// Reads the snapshot cached by <see cref="AsyncResponseWatchdogState"/> — probes never touch the
/// recovery store. The check reports at most <see cref="HealthStatus.Degraded"/>: stale recovery
/// state means business flows are likely stuck and need operator attention, but the process itself
/// is fully able to serve traffic, so this check should never flip readiness to 503 and pull
/// instances out of rotation (map <see cref="HealthStatus.Degraded"/> to HTTP 200 in your
/// health-endpoint options, which is the ASP.NET Core default).
/// </para>
/// <list type="bullet">
/// <item><description><b>Healthy</b> — last scan found no stale entries (or no scan has run yet:
/// the watchdog starts with a delay, and readiness must not block on it).</description></item>
/// <item><description><b>Degraded</b> — stale entries exist, the last scan failed, or the
/// watchdog stopped publishing (snapshot older than twice the scan interval).</description></item>
/// </list>
/// </summary>
public sealed class AsyncResponseRecoveryHealthCheck(AsyncResponseWatchdogState _state) : IHealthCheck
{
    /// <summary>Caps the number of stale entries listed in the health payload.</summary>
    private const int MaxReportedStaleEntries = 10;

    /// <summary>Runs the CheckHealthAsync operation.</summary>
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        => Task.FromResult(Evaluate(_state.Latest, DateTime.UtcNow));

    internal static HealthCheckResult Evaluate(AsyncResponseWatchdogSnapshot? snapshot, DateTime utcNow)
    {
        if (snapshot is null)
        {
            return HealthCheckResult.Healthy(
                "Async-response watchdog has not completed a scan yet.",
                new Dictionary<string, object> { ["scanned"] = false });
        }

        var snapshotAge = utcNow - snapshot.ScanCompletedUtc;
        if (snapshotAge > snapshot.ScanInterval * 2)
        {
            return HealthCheckResult.Degraded(
                $"Async-response watchdog stopped reporting: last scan {snapshot.ScanCompletedUtc:u} is older than twice the scan interval ({snapshot.ScanInterval}).",
                data: BuildData(snapshot));
        }

        if (snapshot.Error is not null)
        {
            return HealthCheckResult.Degraded(
                $"Async-response watchdog scan failed: {snapshot.Error}",
                data: BuildData(snapshot));
        }

        var report = snapshot.Report!;
        if (report.StaleEntries.Count > 0)
        {
            return HealthCheckResult.Degraded(
                $"{report.StaleEntries.Count} async-response flow(s) look stuck: persisted recovery state with no live waiter and no response. Investigate and resume or fail them.",
                data: BuildData(snapshot));
        }

        return HealthCheckResult.Healthy(
            "No stale async-response recovery state.",
            BuildData(snapshot));
    }

    private static Dictionary<string, object> BuildData(AsyncResponseWatchdogSnapshot snapshot)
    {
        var data = new Dictionary<string, object>
        {
            ["lastScanUtc"] = snapshot.ScanCompletedUtc,
            ["scanIntervalMinutes"] = (int)snapshot.ScanInterval.TotalMinutes
        };

        if (snapshot.Report is not { } report)
            return data;

        data["stats"] = new
        {
            outstandingRegistrations = report.TotalEntries,
            withLiveWaiter = report.EntriesWithActiveWaiter,
            stale = report.StaleEntries.Count,
            unknownAge = report.UnknownAgeEntries
        };

        if (report.StaleEntries.Count > 0)
        {
            data["staleEntries"] = report.StaleEntries
                .Take(MaxReportedStaleEntries)
                .Select(e => new
                {
                    correlationId = e.CorrelationId,
                    payloadType = e.PayloadTypeFullName,
                    registeredAtUtc = e.RegisteredAtUtc
                })
                .ToList();

            if (report.StaleEntries.Count > MaxReportedStaleEntries)
                data["staleEntriesTruncated"] = report.StaleEntries.Count - MaxReportedStaleEntries;
        }

        return data;
    }
}
