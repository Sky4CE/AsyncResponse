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
    /// Upper bound on the random extra delay added to the startup delay and to every interval
    /// wait. Default: 10% of <see cref="Interval"/>.
    /// <para>
    /// Replicas deployed together start together, so a fixed interval keeps them scanning in
    /// lockstep forever: every host walks the same recovery store and fires its own liveness probe
    /// per correlation id at the same instant, turning a routine scan into a synchronized burst
    /// against the channel. An independent offset per replica spreads that out and costs nothing —
    /// the scan is a periodic report, so when it runs within the interval does not matter.
    /// </para>
    /// <para>
    /// Set to <see cref="TimeSpan.Zero"/> for an exactly-periodic scan (single-host deployments,
    /// or tests that assert on scan timing).
    /// </para>
    /// </summary>
    public TimeSpan? IntervalJitter { get; set; }

    /// <summary>The resolved jitter bound: <see cref="IntervalJitter"/> or 10% of the interval.</summary>
    internal TimeSpan ResolvedJitter => IntervalJitter ?? TimeSpan.FromTicks(Interval.Ticks / 10);

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

    /// <summary>
    /// Upper bound on liveness probes one scan runs concurrently. Each probe is its own round
    /// trip to the channel (a store query or broker request) and a scan issues one per buffered
    /// entry, so probing strictly sequentially would serialize up to <see cref="MaxScanEntries"/>
    /// round trips per scan. Must be at least 1 (strictly sequential). Default: 8.
    /// </summary>
    public int ProbeConcurrency { get; set; } = 8;

    /// <summary>Validates the scan-loop knobs so a bad value fails at startup, not mid-scan.</summary>
    internal void Validate()
    {
        // Interval and StartupDelay arm Task.Delay in the scan loop; an out-of-range value there
        // would throw outside the per-scan try, fault the background service mid-run, and (with
        // the default BackgroundServiceExceptionBehavior.StopHost) take the host down instead of
        // failing fast where the misconfiguration is visible.
        AsyncResponseChannelOptions.EnsureTimerBacked(Interval, nameof(AsyncResponseWatchdogOptions), nameof(Interval));
        AsyncResponseChannelOptions.EnsureTimerBackedAllowZero(StartupDelay, nameof(AsyncResponseWatchdogOptions), nameof(StartupDelay));

        // Jitter is added to those same waits, so it shares their ceiling. Validated on the
        // RESOLVED value: the 10% default of a near-ceiling interval is itself timer-armed.
        AsyncResponseChannelOptions.EnsureTimerBackedAllowZero(ResolvedJitter, nameof(AsyncResponseWatchdogOptions), nameof(IntervalJitter));

        // The probe fan-out degree feeds Parallel.ForEachAsync mid-scan, which rejects values
        // below 1 with the same delayed, host-stopping failure mode.
        if (ProbeConcurrency < 1)
            throw new InvalidOperationException(
                $"{nameof(AsyncResponseWatchdogOptions)}.{nameof(ProbeConcurrency)} must be at least 1.");

        // The buffer cap gates growth with ">= MaxScanEntries", so a non-positive value truncates
        // on the FIRST entry: every scan then classifies an empty set and publishes a report that
        // is Truncated with zeroed counters. The health check degrades permanently and no stale
        // registration is ever reported — the stuck-flow alarm is silently off while the logs
        // still show a scan completing each interval. Nothing later in the scan can catch this,
        // so it has to fail here. ("Unlimited" is not a supported value: the cap is a memory
        // bound, and int.MaxValue is the way to ask for effectively no limit.)
        if (MaxScanEntries < 1)
            throw new InvalidOperationException(
                $"{nameof(AsyncResponseWatchdogOptions)}.{nameof(MaxScanEntries)} must be at least 1; it bounds scan memory, " +
                $"so use {int.MaxValue} for effectively no limit rather than zero.");

        // Staleness is judged as "utcNow - registeredAtUtc >= StaleAfter", so a non-positive
        // threshold makes EVERY live registration stale and logs a Warning per entry on every
        // scan — an alarm that fires constantly is the same as no alarm at all.
        if (StaleAfter <= TimeSpan.Zero)
            throw new InvalidOperationException(
                $"{nameof(AsyncResponseWatchdogOptions)}.{nameof(StaleAfter)} must be positive; a non-positive threshold reports " +
                "every live registration as stale.");
    }
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
    private volatile WatchdogActivation? _activation;

    /// <summary>The most recent scan outcome, or <c>null</c> when no scan has completed yet.</summary>
    public AsyncResponseWatchdogSnapshot? Latest => _latest;

    /// <summary>
    /// Whether this host's watchdog scans: <c>true</c> once its scan loop is armed, <c>false</c>
    /// when it declined to run (disabled via options, or the channel registers no scanner), and
    /// <c>null</c> while unknown (the watchdog has not started, or none is registered).
    /// </summary>
    public bool? Scanning => _activation?.Scanning;

    /// <summary>Why this host's watchdog does not scan, when <see cref="Scanning"/> is <c>false</c>.</summary>
    public string? IdleReason => _activation is { Scanning: false } activation ? activation.IdleReason : null;

    internal WatchdogActivation? Activation => _activation;

    /// <summary>Publishes the latest watchdog snapshot for health checks and metrics.</summary>
    public void Publish(AsyncResponseWatchdogSnapshot snapshot) => _latest = snapshot;

    /// <summary>
    /// Marks this host's watchdog as armed, so the health check can hold it to a first-scan
    /// deadline instead of attesting "no scan yet" for a loop that died before ever publishing.
    /// </summary>
    internal void MarkScanning(DateTime startedUtc, TimeSpan startupDelay, TimeSpan interval)
        => _activation = new WatchdogActivation(Scanning: true, IdleReason: null, startedUtc, startupDelay, interval);

    /// <summary>
    /// Marks this host's watchdog as deliberately idle, so the health check can attest "this host
    /// does not scan" (the documented multi-host pattern) instead of "no scan yet".
    /// </summary>
    internal void MarkIdle(string reason)
        => _activation = new WatchdogActivation(Scanning: false, reason, StartedUtc: null, default, default);

    /// <summary>How the watchdog resolved its startup guards. Single writer: the watchdog.</summary>
    internal sealed record WatchdogActivation(
        bool Scanning,
        string? IdleReason,
        DateTime? StartedUtc,
        TimeSpan StartupDelay,
        TimeSpan Interval);
}

/// <summary>Result of evaluating a snapshot of the persisted recovery state.</summary>
/// <param name="TotalEntries">Recovery registrations observed, deduplicated per correlation id.</param>
/// <param name="EntriesWithActiveWaiter">Observed entries with at least one live subscriber.</param>
/// <param name="StaleEntries">Entries with no live waiter registered longer ago than the staleness threshold.</param>
/// <param name="UnknownAgeEntries">Entries with no live waiter and no registration timestamp — reported separately, never flagged stale.</param>
/// <param name="UnprobeableEntries">
/// Entries whose waiter liveness could not be probed (negative
/// <see cref="RecoveryStateObservation.ActiveSubscribers"/>: the probe failed, or no
/// <see cref="IActiveSubscriberProbe"/> is registered). Their staleness is unknown and they are
/// never flagged stale, so the health check degrades rather than letting a probe outage read as
/// a clean pass with zeroed counters.
/// </param>
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
    int UnprobeableEntries = 0,
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
        var unprobeableEntries = 0;
        List<RecoveryStateObservation>? staleEntries = null;

        if (byCorrelationId is not null)
        {
            foreach (var entry in byCorrelationId.Values)
                Classify(entry, utcNow, staleAfter, ref totalEntries, ref entriesWithActiveWaiter, ref unknownAgeEntries, ref unprobeableEntries, ref staleEntries);
        }

        if (ungrouped is not null)
        {
            foreach (var entry in ungrouped)
                Classify(entry, utcNow, staleAfter, ref totalEntries, ref entriesWithActiveWaiter, ref unknownAgeEntries, ref unprobeableEntries, ref staleEntries);
        }

        return new AsyncResponseWatchdogReport(
            totalEntries,
            entriesWithActiveWaiter,
            staleEntries ?? [],
            unknownAgeEntries,
            unprobeableEntries);
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
        ref int unprobeableEntries,
        ref List<RecoveryStateObservation>? staleEntries)
    {
        totalEntries++;
        var activeSubscribers = entry.ActiveSubscribers;
        if (activeSubscribers > 0)
        {
            entriesWithActiveWaiter++;
            return;
        }

        // Negative liveness means it could not be probed; never flag those as stale, but count
        // them — dropped from every bucket, a probe outage would read as a clean pass.
        if (activeSubscribers != 0)
        {
            unprobeableEntries++;
            return;
        }

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
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<AsyncResponseWatchdog> _logger;

    /// <summary>Creates the background recovery watchdog.</summary>
    public AsyncResponseWatchdog(
        IEnumerable<IRecoveryStateScanner> scanners,
        IEnumerable<IActiveSubscriberProbe> subscriberProbes,
        AsyncResponseWatchdogState state,
        IOptions<AsyncResponseOptions> options,
        ILogger<AsyncResponseWatchdog> logger,
        TimeProvider? timeProvider = null)
    {
        _scanner = scanners.FirstOrDefault();
        _subscriberProbe = subscriberProbes.FirstOrDefault();
        _state = state;
        _options = options.Value.Watchdog;
        _options.Validate();
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger;
    }

    /// <summary>
    /// A base wait plus a random offset up to <see cref="AsyncResponseWatchdogOptions.ResolvedJitter"/>,
    /// so replicas that started together do not stay in step. Drawn per wait rather than once per
    /// process: a single per-process offset keeps the SPACING identical, so two replicas that
    /// happen to draw similar offsets collide on every scan thereafter instead of just the first.
    /// </summary>
    private TimeSpan NextWait(TimeSpan baseDelay)
    {
        var jitter = _options.ResolvedJitter;
        return jitter <= TimeSpan.Zero
            ? baseDelay
            : baseDelay + TimeSpan.FromTicks(Random.Shared.NextInt64(jitter.Ticks + 1));
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _state.MarkIdle($"disabled via {nameof(AsyncResponseOptions)}.{nameof(AsyncResponseOptions.Watchdog)}.{nameof(AsyncResponseWatchdogOptions.Enabled)}");
            _logger.LogInformation("Recovery watchdog disabled via options; not scanning.");
            return;
        }

        if (_scanner is null)
        {
            _state.MarkIdle($"no {nameof(IRecoveryStateScanner)} registered (the configured channel does not support scanning)");
            _logger.LogInformation("Recovery watchdog idle: no IRecoveryStateScanner registered (the configured channel does not support scanning).");
            return;
        }

        // Only a watchdog that actually scans may take over the process-wide gauge holder: a
        // disabled or scanner-less host (the documented multi-host pattern) would otherwise
        // permanently zero the gauges for the host that does scan.
        _state.MarkScanning(_timeProvider.GetUtcNow().UtcDateTime, _options.StartupDelay, _options.Interval);
        AsyncResponseDiagnostics.EnsureWatchdogGauges(_state);

        _logger.LogInformation("Recovery watchdog started. Interval: {Interval}, stale threshold: {StaleAfter}.", _options.Interval, _options.StaleAfter);

        try
        {
            await Task.Delay(NextWait(_options.StartupDelay), _timeProvider, stoppingToken).ConfigureAwait(false);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var report = await ScanOnceAsync(stoppingToken).ConfigureAwait(false);
                    _state.Publish(new AsyncResponseWatchdogSnapshot(_timeProvider.GetUtcNow().UtcDateTime, _options.Interval, report, Error: null));
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Recovery watchdog scan failed; next attempt in {Interval}.", _options.Interval);
                    _state.Publish(new AsyncResponseWatchdogSnapshot(_timeProvider.GetUtcNow().UtcDateTime, _options.Interval, Report: null, Error: ex.Message));
                }

                await Task.Delay(NextWait(_options.Interval), _timeProvider, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Host shutdown.
        }
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            AsyncResponseDiagnostics.ReleaseWatchdogGauges(_state);
        }
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        // Covers hosts torn down without a graceful StopAsync. The release is identity-conditional
        // and idempotent, so the double call on the graceful path is harmless.
        AsyncResponseDiagnostics.ReleaseWatchdogGauges(_state);
        base.Dispose();
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

            // Phase 2 — one liveness probe per unique correlation id, fanned out with bounded
            // concurrency (each probe is its own channel round trip; strictly sequential awaits
            // would serialize up to MaxScanEntries of them per scan), then the same pure
            // classifier the report type exposes publicly, so this scan and Evaluate (the tested
            // and benchmarked surface) can never drift apart again.
            var pending = new List<(string? CorrelationId, DateTime? RegisteredAtUtc, string? PayloadTypeFullName)>(BufferedCount());

            if (byCorrelationId is not null)
            {
                foreach (var (correlationId, entry) in byCorrelationId)
                    pending.Add((correlationId, entry.RegisteredAtUtc, entry.PayloadTypeFullName));
            }

            if (ungrouped is not null)
                pending.AddRange(ungrouped);

            // Per-slot writes keep the result independent of probe completion order. A probe
            // canceled by shutdown still aborts the whole fan-out: CountActiveSubscribersAsync
            // rethrows, and ForEachAsync cancels its siblings and surfaces the cancellation.
            var observations = new RecoveryStateObservation[pending.Count];
            await Parallel.ForEachAsync(
                Enumerable.Range(0, pending.Count),
                new ParallelOptions { MaxDegreeOfParallelism = _options.ProbeConcurrency, CancellationToken = cancellationToken },
                async (index, ct) =>
                {
                    var (correlationId, registeredAtUtc, payloadTypeFullName) = pending[index];
                    observations[index] = new RecoveryStateObservation(
                        correlationId,
                        registeredAtUtc,
                        await CountActiveSubscribersAsync(correlationId, ct).ConfigureAwait(false),
                        payloadTypeFullName);
                }).ConfigureAwait(false);

            var report = AsyncResponseWatchdogReport.Evaluate(observations, _timeProvider.GetUtcNow().UtcDateTime, _options.StaleAfter);

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
            activity?.SetTag("asyncresponse.watchdog.unprobeable_entries", report.UnprobeableEntries);

            _logger.LogInformation("Recovery watchdog scan complete. Outstanding registrations: {Total}, with live waiter: {Active}, stale (no waiter, older than {StaleAfter}): {Stale}, unknown age: {UnknownAge}, liveness unprobeable: {Unprobeable}.", report.TotalEntries, report.EntriesWithActiveWaiter, _options.StaleAfter, report.StaleEntries.Count, report.UnknownAgeEntries, report.UnprobeableEntries);

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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown must abort the scan here, not degrade to -1: swallowed, a canceled probe
            // would let the loop grind through every remaining entry (throw/catch per id, delaying
            // shutdown) and then publish a snapshot attesting a completed scan whose liveness was
            // never probed. ExecuteAsync catches this as its stop signal.
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Recovery watchdog failed to probe subscribers for correlationId {CorrelationId}.", correlationId);
            return -1;
        }
    }
}
