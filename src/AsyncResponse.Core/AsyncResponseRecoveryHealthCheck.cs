using Microsoft.Extensions.Diagnostics.HealthChecks;
using System.Text.Json.Serialization;

namespace AsyncResponse;

/// <summary>
/// Aggregate counts the recovery health check reports under the <c>stats</c> key of its
/// <see cref="HealthCheckResult.Data"/>. A named type (with pinned JSON property names) rather
/// than an anonymous one so trimmed/Native AOT apps can serialize health reports: register it —
/// plus <see cref="AsyncResponseStaleRecoveryEntry"/> — in the app's
/// <see cref="JsonSerializerContext"/> if the app writes health data as JSON.
/// </summary>
public sealed record AsyncResponseRecoveryStats(
    [property: JsonPropertyName("outstandingRegistrations")] int OutstandingRegistrations,
    [property: JsonPropertyName("withLiveWaiter")] int WithLiveWaiter,
    [property: JsonPropertyName("stale")] int Stale,
    [property: JsonPropertyName("unknownAge")] int UnknownAge,
    [property: JsonPropertyName("unprobeable")] int Unprobeable = 0);

/// <summary>
/// One stale recovery registration listed under the <c>staleEntries</c> key of the recovery
/// health check's <see cref="HealthCheckResult.Data"/> (JSON names pinned; see
/// <see cref="AsyncResponseRecoveryStats"/> for the AOT registration note).
/// </summary>
public sealed record AsyncResponseStaleRecoveryEntry(
    [property: JsonPropertyName("correlationId")] string? CorrelationId,
    [property: JsonPropertyName("payloadType")] string? PayloadType,
    [property: JsonPropertyName("registeredAtUtc")] DateTime? RegisteredAtUtc);

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
/// <item><description><b>Healthy</b> — last scan found no stale entries; no scan has run yet but
/// the watchdog is inside its first-scan budget (it starts with a delay, and readiness must not
/// block on it); or this host's watchdog is deliberately idle (disabled, or the channel has no
/// scanner — the multi-host pattern), reported as explicit data rather than an alert.</description></item>
/// <item><description><b>Degraded</b> — stale entries exist, the last scan failed, the last scan
/// was truncated at the buffer cap (its verdict covers a subset only), waiter liveness could not
/// be probed for some entries (their staleness is unknown), or the watchdog stopped publishing
/// (snapshot older than twice the scan interval, or no first snapshot after the startup delay
/// plus twice the interval).</description></item>
/// </list>
/// </summary>
public sealed class AsyncResponseRecoveryHealthCheck(AsyncResponseWatchdogState _state, TimeProvider? _timeProvider = null) : IHealthCheck
{
    /// <summary>Caps the number of stale entries listed in the health payload.</summary>
    private const int MaxReportedStaleEntries = 10;

    /// <summary>Runs the CheckHealthAsync operation.</summary>
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        => Task.FromResult(Evaluate(_state.Latest, _state.Activation, (_timeProvider ?? TimeProvider.System).GetUtcNow().UtcDateTime));

    internal static HealthCheckResult Evaluate(
        AsyncResponseWatchdogSnapshot? snapshot,
        AsyncResponseWatchdogState.WatchdogActivation? activation,
        DateTime utcNow)
    {
        if (snapshot is null)
            return EvaluateBeforeFirstScan(activation, utcNow);

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

        if (report.Truncated)
        {
            // Zero stale entries in a truncated scan is not a verdict — arbitrarily many stale
            // entries can sit past the buffer cap. "The scan was incomplete" is a health fact.
            return HealthCheckResult.Degraded(
                "Async-response watchdog scan was truncated at the MaxScanEntries buffer cap; staleness was assessed for the buffered subset only. Raise AsyncResponseOptions.Watchdog.MaxScanEntries to restore full coverage.",
                data: BuildData(snapshot));
        }

        if (report.UnprobeableEntries > 0)
        {
            // Unknown liveness is never flagged stale (no false alarms), so without this a probe
            // outage would zero every counter and read as a clean pass the scan never computed.
            return HealthCheckResult.Degraded(
                $"Async-response watchdog could not probe waiter liveness for {report.UnprobeableEntries} of {report.TotalEntries} recovery registration(s) (probe outage, or no IActiveSubscriberProbe registered); their staleness is unknown.",
                data: BuildData(snapshot));
        }

        return HealthCheckResult.Healthy(
            "No stale async-response recovery state.",
            BuildData(snapshot));
    }

    /// <summary>
    /// Attestation before any snapshot exists. Three honest answers instead of a blanket Healthy:
    /// a deliberately idle watchdog (disabled, or no scanner — the documented multi-host pattern)
    /// stays alert-quiet but says so in data; an armed scan loop is Healthy only inside its
    /// first-scan budget (startup delay plus two intervals) — past it the loop is as dead as one
    /// that stopped publishing, and "no scan yet" must not mask that forever; with no activation
    /// marker (watchdog not started, or none registered) there is no deadline to hold it to.
    /// </summary>
    private static HealthCheckResult EvaluateBeforeFirstScan(
        AsyncResponseWatchdogState.WatchdogActivation? activation,
        DateTime utcNow)
    {
        if (activation is { Scanning: false })
        {
            return HealthCheckResult.Healthy(
                $"Async-response watchdog on this host does not scan ({activation.IdleReason}); recovery staleness is attested by the scanning host.",
                new Dictionary<string, object>
                {
                    ["scanning"] = false,
                    ["reason"] = activation.IdleReason ?? "unknown"
                });
        }

        if (activation is { Scanning: true, StartedUtc: { } startedUtc })
        {
            var firstScanDueByUtc = startedUtc + activation.StartupDelay + 2 * activation.Interval;
            var data = new Dictionary<string, object>
            {
                ["scanned"] = false,
                ["scanning"] = true,
                ["firstScanDueByUtc"] = firstScanDueByUtc
            };

            if (utcNow > firstScanDueByUtc)
            {
                return HealthCheckResult.Degraded(
                    $"Async-response watchdog never completed its first scan: it started {startedUtc:u} and published nothing within the startup delay ({activation.StartupDelay}) plus twice the scan interval ({activation.Interval}).",
                    data: data);
            }

            return HealthCheckResult.Healthy("Async-response watchdog has not completed a scan yet.", data);
        }

        return HealthCheckResult.Healthy(
            "Async-response watchdog has not completed a scan yet.",
            new Dictionary<string, object> { ["scanned"] = false });
    }

    private static Dictionary<string, object> BuildData(AsyncResponseWatchdogSnapshot snapshot)
    {
        var data = new Dictionary<string, object>
        {
            ["lastScanUtc"] = snapshot.ScanCompletedUtc,
            // Human-readable text plus a lossless numeric — alert math derived from a
            // whole-minutes value reads 0 for any sub-minute interval.
            ["scanInterval"] = snapshot.ScanInterval.ToString(),
            ["scanIntervalSeconds"] = snapshot.ScanInterval.TotalSeconds
        };

        if (snapshot.Report is not { } report)
            return data;

        data["truncated"] = report.Truncated;

        data["stats"] = new AsyncResponseRecoveryStats(
            report.TotalEntries,
            report.EntriesWithActiveWaiter,
            report.StaleEntries.Count,
            report.UnknownAgeEntries,
            report.UnprobeableEntries);

        if (report.StaleEntries.Count > 0)
        {
            data["staleEntries"] = report.StaleEntries
                .Take(MaxReportedStaleEntries)
                .Select(e => new AsyncResponseStaleRecoveryEntry(
                    e.CorrelationId,
                    e.PayloadTypeFullName,
                    e.RegisteredAtUtc))
                .ToList();

            if (report.StaleEntries.Count > MaxReportedStaleEntries)
                data["staleEntriesTruncated"] = report.StaleEntries.Count - MaxReportedStaleEntries;
        }

        return data;
    }
}
