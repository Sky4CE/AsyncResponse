using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace AsyncResponse;

/// <summary>Options for the async-response recovery watchdog.</summary>
public sealed class AsyncResponseWatchdogOptions
{
    /// <summary>
    /// Whether the watchdog runs. Default: <c>true</c>. Set to <c>false</c> to disable it — for
    /// example in all but one host when several hosts share one durable recovery store, so the
    /// scan and its warnings are not duplicated.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>How often the watchdog scans the persisted recovery state. Default: 6 hours.</summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromHours(6);

    /// <summary>
    /// Age past which a recovery entry with no live subscriber is reported as stale.
    /// Default: 24 hours.
    /// </summary>
    public TimeSpan StaleAfter { get; set; } = TimeSpan.FromHours(24);

    /// <summary>Delay before the first scan, so startup is never blocked. Default: 5 minutes.</summary>
    public TimeSpan StartupDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Upper bound on the recovery entries one scan buffers (the scan dedupes in memory before
    /// probing liveness). When the store holds more, the scan stops enumerating at the cap,
    /// reports the buffered subset (<see cref="AsyncResponseWatchdogReport.Truncated"/> is set,
    /// the health check degrades), and logs a warning — bounding scan memory on very large
    /// stores at the cost of an incomplete staleness report. The count is a MEMORY bound, not a
    /// flow count: grouped entries occupy one slot per unique correlation id, correlation-less
    /// entries one slot per row. Default: 100 000.
    /// </summary>
    public int MaxScanEntries { get; set; } = 100_000;
}

/// <summary>
/// Snapshot of one persisted recovery entry as observed by the watchdog.
/// </summary>
/// <param name="CorrelationId">The correlation id the entry belongs to.</param>
/// <param name="RegisteredAtUtc">When the waiter registered, or <c>null</c> if unknown.</param>
/// <param name="ActiveSubscribers">
/// Live subscribers awaiting this correlation id's channel: <c>0</c> = no live waiter, a positive
/// value = at least one, a negative value = liveness could not be probed (no
/// <see cref="IActiveSubscriberProbe"/>).
/// </param>
/// <param name="PayloadTypeFullName">The payload type the waiter subscribed for.</param>
public sealed record RecoveryStateObservation(
    string? CorrelationId,
    DateTime? RegisteredAtUtc,
    long ActiveSubscribers,
    string? PayloadTypeFullName);

/// <summary>
/// Outcome of one watchdog scan attempt, as published for consumers
/// (e.g. <see cref="AsyncResponseRecoveryHealthCheck"/>).
/// </summary>
/// <param name="ScanCompletedUtc">When the scan attempt finished.</param>
/// <param name="ScanInterval">The configured scan interval, so consumers can judge snapshot freshness.</param>
/// <param name="Report">The evaluation result; <c>null</c> when the scan failed.</param>
/// <param name="Error">The scan failure message; <c>null</c> when the scan succeeded.</param>
public sealed record AsyncResponseWatchdogSnapshot(
    DateTime ScanCompletedUtc,
    TimeSpan ScanInterval,
    AsyncResponseWatchdogReport? Report,
    string? Error);

/// <summary>
/// Holds the latest watchdog scan result. The watchdog is the single writer; readers (e.g. the
/// readiness health check) get a cheap, consistent snapshot without touching the recovery store.
/// </summary>
public sealed class AsyncResponseWatchdogState
{
    private volatile AsyncResponseWatchdogSnapshot? _latest;

    /// <summary>The most recent scan outcome, or <c>null</c> when no scan has completed yet.</summary>
    public AsyncResponseWatchdogSnapshot? Latest => _latest;

    /// <summary>Publishes the latest watchdog snapshot for health checks and metrics.</summary>
    public void Publish(AsyncResponseWatchdogSnapshot snapshot) => _latest = snapshot;
}

/// <summary>Result of evaluating a snapshot of the persisted recovery state.</summary>
/// <param name="Truncated">
/// Whether the scan stopped at <see cref="AsyncResponseWatchdogOptions.MaxScanEntries"/> before
/// exhausting the store — the counts and stale list then describe the buffered subset only, and
/// the health check degrades rather than attesting a staleness verdict it cannot back.
/// </param>
public sealed record AsyncResponseWatchdogReport(
    int TotalEntries,
    int EntriesWithActiveWaiter,
    IReadOnlyList<RecoveryStateObservation> StaleEntries,
    int UnknownAgeEntries,
    bool Truncated = false)
{
    /// <summary>
    /// Pure evaluation: an entry is <em>stale</em> when nobody is subscribed to its channel
    /// (the waiter died) and it has been registered for longer than <paramref name="staleAfter"/>
    /// without any response triggering the lost-subscriber recovery. Entries without a
    /// registration timestamp are reported separately as unknown-age. Entries whose liveness could
    /// not be probed (negative <see cref="RecoveryStateObservation.ActiveSubscribers"/>) are never
    /// flagged stale, to avoid false positives.
    /// </summary>
    public static AsyncResponseWatchdogReport Evaluate(
        IReadOnlyCollection<RecoveryStateObservation> entries,
        DateTime utcNow,
        TimeSpan staleAfter)
    {
        // Dedupe keeps the OLDEST registration per correlation id. Sibling registrations share a
        // correlation id by design (fan-out waiters, a flow re-attaching after a crash), and the
        // scanner contract deliberately promises no ordering — preferring the oldest makes the
        // verdict order-independent, so a young sibling can never mask an older stale one.
        // Entries without a correlation id cannot be grouped and are classified individually.
        // Structures stay lazily allocated: an empty snapshot allocates nothing.
        Dictionary<string, RecoveryStateObservation>? byCorrelationId = null;
        List<RecoveryStateObservation>? ungrouped = null;

        foreach (var entry in entries)
        {
            if (string.IsNullOrEmpty(entry.CorrelationId))
            {
                (ungrouped ??= []).Add(entry);
                continue;
            }

            byCorrelationId ??= new Dictionary<string, RecoveryStateObservation>(entries.Count, StringComparer.Ordinal);
            if (!byCorrelationId.TryGetValue(entry.CorrelationId, out var kept) || IsOlder(entry, kept))
                byCorrelationId[entry.CorrelationId] = entry;
        }

        var totalEntries = 0;
        var entriesWithActiveWaiter = 0;
        var unknownAgeEntries = 0;
        List<RecoveryStateObservation>? staleEntries = null;

        if (byCorrelationId is not null)
        {
            foreach (var entry in byCorrelationId.Values)
                Classify(entry, utcNow, staleAfter, ref totalEntries, ref entriesWithActiveWaiter, ref unknownAgeEntries, ref staleEntries);
        }

        if (ungrouped is not null)
        {
            foreach (var entry in ungrouped)
                Classify(entry, utcNow, staleAfter, ref totalEntries, ref entriesWithActiveWaiter, ref unknownAgeEntries, ref staleEntries);
        }

        return new AsyncResponseWatchdogReport(
            totalEntries,
            entriesWithActiveWaiter,
            staleEntries ?? [],
            unknownAgeEntries);
    }

    /// <summary>Prefers the entry with the oldest known registration; a known age beats an unknown one.</summary>
    internal static bool IsOlder(RecoveryStateObservation candidate, RecoveryStateObservation kept)
        => candidate.RegisteredAtUtc is { } candidateRegistered
           && (kept.RegisteredAtUtc is not { } keptRegistered || candidateRegistered < keptRegistered);

    private static void Classify(
        RecoveryStateObservation entry,
        DateTime utcNow,
        TimeSpan staleAfter,
        ref int totalEntries,
        ref int entriesWithActiveWaiter,
        ref int unknownAgeEntries,
        ref List<RecoveryStateObservation>? staleEntries)
    {
        totalEntries++;
        var activeSubscribers = entry.ActiveSubscribers;
        if (activeSubscribers > 0)
        {
            entriesWithActiveWaiter++;
            return;
        }

        // Negative liveness means it could not be probed; never flag those as stale.
        if (activeSubscribers != 0)
            return;

        if (entry.RegisteredAtUtc is not { } registeredAtUtc)
        {
            unknownAgeEntries++;
            return;
        }

        if (utcNow - registeredAtUtc >= staleAfter)
            (staleEntries ??= []).Add(entry);
    }
}

/// <summary>
/// Periodic, report-only scanner of the persisted async-response recovery state. It is part of the
/// engine and runs by default for whatever channel is registered: it enumerates recovery entries
/// through <see cref="IRecoveryStateScanner"/> and checks waiter liveness through
/// <see cref="IActiveSubscriberProbe"/>, so it is independent of any specific store or broker.
/// <para>
/// Every recovery entry represents an outstanding wait registration. A healthy entry either has a
/// live subscriber (the waiter is awaiting in some process) or is young — armed recovery state for
/// a response that has not arrived yet. An entry that is <em>old</em> and has <em>no subscriber</em>
/// means the waiter died and nothing (response, resume, retry) has touched the flow since: the
/// precursor of an operation stuck "in progress". The watchdog logs a warning per such entry and
/// publishes a summary snapshot for the health check. It deliberately performs no remediation —
/// recovery belongs to the lost-subscriber dispatcher and the flows' own retry paths.
/// </para>
/// </summary>
internal sealed class AsyncResponseWatchdog : BackgroundService
{
    private readonly IRecoveryStateScanner? _scanner;
    private readonly IActiveSubscriberProbe? _subscriberProbe;
    private readonly AsyncResponseWatchdogState _state;
    private readonly AsyncResponseWatchdogOptions _options;
    private readonly ILogger<AsyncResponseWatchdog> _logger;

    /// <summary>Creates the background recovery watchdog.</summary>
    public AsyncResponseWatchdog(
        IEnumerable<IRecoveryStateScanner> scanners,
        IEnumerable<IActiveSubscriberProbe> subscriberProbes,
        AsyncResponseWatchdogState state,
        IOptions<AsyncResponseOptions> options,
        ILogger<AsyncResponseWatchdog> logger)
    {
        _scanner = scanners.FirstOrDefault();
        _subscriberProbe = subscriberProbes.FirstOrDefault();
        _state = state;
        _options = options.Value.Watchdog;
        _logger = logger;

        AsyncResponseDiagnostics.EnsureWatchdogGauges(state);
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Recovery watchdog disabled via options; not scanning.");
            return;
        }

        if (_scanner is null)
        {
            _logger.LogInformation("Recovery watchdog idle: no IRecoveryStateScanner registered (the configured channel does not support scanning).");
            return;
        }

        _logger.LogInformation("Recovery watchdog started. Interval: {Interval}, stale threshold: {StaleAfter}.", _options.Interval, _options.StaleAfter);

        try
        {
            await Task.Delay(_options.StartupDelay, stoppingToken).ConfigureAwait(false);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var report = await ScanOnceAsync(stoppingToken).ConfigureAwait(false);
                    _state.Publish(new AsyncResponseWatchdogSnapshot(DateTime.UtcNow, _options.Interval, report, Error: null));
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Recovery watchdog scan failed; next attempt in {Interval}.", _options.Interval);
                    _state.Publish(new AsyncResponseWatchdogSnapshot(DateTime.UtcNow, _options.Interval, Report: null, Error: ex.Message));
                }

                await Task.Delay(_options.Interval, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Host shutdown.
        }
    }

    private async Task<AsyncResponseWatchdogReport> ScanOnceAsync(CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.watchdog.scan");

        try
        {
            // Phase 1 — stream the scan and dedupe, keeping the OLDEST registration per
            // correlation id (see Evaluate for why oldest). Only the fields the classifier needs
            // are buffered, not whole recovery states. Buffering before probing also lets the
            // scanner's enumeration (a long-lived reader connection on the relational stores)
            // finish before the per-id probe connections open.
            Dictionary<string, (DateTime? RegisteredAtUtc, string? PayloadTypeFullName)>? byCorrelationId = null;
            List<(string? CorrelationId, DateTime? RegisteredAtUtc, string? PayloadTypeFullName)>? ungrouped = null;
            var truncated = false;
            int BufferedCount() => (byCorrelationId?.Count ?? 0) + (ungrouped?.Count ?? 0);

            await foreach (var entry in _scanner!.ScanAsync(cancellationToken).ConfigureAwait(false))
            {
                if (entry is null)
                    continue;

                if (string.IsNullOrEmpty(entry.CorrelationId))
                {
                    // The cap gates growth only — replacing an already-buffered correlation id
                    // with an older sibling costs nothing, so oldest-wins keeps working at the cap.
                    if (BufferedCount() >= _options.MaxScanEntries)
                    {
                        truncated = true;
                        break;
                    }

                    (ungrouped ??= []).Add((entry.CorrelationId, entry.RegisteredAtUtc, entry.PayloadTypeFullName));
                    continue;
                }

                byCorrelationId ??= new Dictionary<string, (DateTime?, string?)>(StringComparer.Ordinal);
                if (byCorrelationId.TryGetValue(entry.CorrelationId, out var kept))
                {
                    if (entry.RegisteredAtUtc is { } candidate && (kept.RegisteredAtUtc is not { } existing || candidate < existing))
                        byCorrelationId[entry.CorrelationId] = (entry.RegisteredAtUtc, entry.PayloadTypeFullName);
                }
                else
                {
                    if (BufferedCount() >= _options.MaxScanEntries)
                    {
                        truncated = true;
                        break;
                    }

                    byCorrelationId[entry.CorrelationId] = (entry.RegisteredAtUtc, entry.PayloadTypeFullName);
                }
            }

            // Phase 2 — one liveness probe per unique correlation id, then the same pure
            // classifier the report type exposes publicly, so this scan and Evaluate (the tested
            // and benchmarked surface) can never drift apart again.
            var observations = new List<RecoveryStateObservation>((byCorrelationId?.Count ?? 0) + (ungrouped?.Count ?? 0));

            if (byCorrelationId is not null)
            {
                foreach (var (correlationId, entry) in byCorrelationId)
                {
                    observations.Add(new RecoveryStateObservation(
                        correlationId,
                        entry.RegisteredAtUtc,
                        await CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false),
                        entry.PayloadTypeFullName));
                }
            }

            if (ungrouped is not null)
            {
                foreach (var entry in ungrouped)
                {
                    observations.Add(new RecoveryStateObservation(
                        entry.CorrelationId,
                        entry.RegisteredAtUtc,
                        await CountActiveSubscribersAsync(entry.CorrelationId, cancellationToken).ConfigureAwait(false),
                        entry.PayloadTypeFullName));
                }
            }

            var report = AsyncResponseWatchdogReport.Evaluate(observations, DateTime.UtcNow, _options.StaleAfter);

            if (truncated)
            {
                // Carried on the report itself, not just telemetry: the health check and gauges
                // read the report, and a silently truncated scan would otherwise attest a
                // staleness verdict it never actually computed.
                report = report with { Truncated = true };
                activity?.SetTag("asyncresponse.watchdog.truncated", true);
                _logger.LogWarning(
                    "Recovery watchdog scan stopped at the {MaxScanEntries}-entry buffer cap; staleness is reported for that subset only. Raise AsyncResponseOptions.Watchdog.MaxScanEntries to cover more (scan memory scales with the cap).",
                    _options.MaxScanEntries);
            }

            activity?.SetTag("asyncresponse.watchdog.total_entries", report.TotalEntries);
            activity?.SetTag("asyncresponse.watchdog.active_waiters", report.EntriesWithActiveWaiter);
            activity?.SetTag("asyncresponse.watchdog.stale_entries", report.StaleEntries.Count);
            activity?.SetTag("asyncresponse.watchdog.unknown_age_entries", report.UnknownAgeEntries);

            _logger.LogInformation("Recovery watchdog scan complete. Outstanding registrations: {Total}, with live waiter: {Active}, stale (no waiter, older than {StaleAfter}): {Stale}, unknown age: {UnknownAge}.", report.TotalEntries, report.EntriesWithActiveWaiter, _options.StaleAfter, report.StaleEntries.Count, report.UnknownAgeEntries);

            foreach (var stale in report.StaleEntries)
            {
                _logger.LogWarning("Stale async-response recovery state — correlationId {CorrelationId}, payload type {PayloadType}, registered {RegisteredAtUtc}, no live subscriber. The owning flow is likely stuck; investigate and resume or fail it.", stale.CorrelationId, stale.PayloadTypeFullName, stale.RegisteredAtUtc);
            }

            return report;
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>
    /// Liveness for one entry. Returns <c>-1</c> (unknown) when there is no probe or no correlation
    /// id; the report treats unknown liveness as "not stale" so it never raises a false alarm.
    /// </summary>
    private async ValueTask<long> CountActiveSubscribersAsync(string? correlationId, CancellationToken cancellationToken)
    {
        if (_subscriberProbe is null || string.IsNullOrWhiteSpace(correlationId))
            return -1;

        try
        {
            return await _subscriberProbe.CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Recovery watchdog failed to probe subscribers for correlationId {CorrelationId}.", correlationId);
            return -1;
        }
    }
}
