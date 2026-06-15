using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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

    public void Publish(AsyncResponseWatchdogSnapshot snapshot) => _latest = snapshot;
}

/// <summary>Result of evaluating a snapshot of the persisted recovery state.</summary>
public sealed record AsyncResponseWatchdogReport(
    int TotalEntries,
    int EntriesWithActiveWaiter,
    IReadOnlyList<RecoveryStateObservation> StaleEntries,
    int UnknownAgeEntries)
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
        var entriesWithActiveWaiter = entries.Count(e => e.ActiveSubscribers > 0);
        var unknownAgeEntries = entries.Count(e => e.ActiveSubscribers == 0 && e.RegisteredAtUtc is null);
        var staleEntries = entries
            .Where(e => e.ActiveSubscribers == 0
                && e.RegisteredAtUtc is not null
                && utcNow - e.RegisteredAtUtc.Value >= staleAfter)
            .ToList();

        return new AsyncResponseWatchdogReport(entries.Count, entriesWithActiveWaiter, staleEntries, unknownAgeEntries);
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
    private const string SERVICE_NAME = nameof(AsyncResponseWatchdog);

    private readonly IRecoveryStateScanner? _scanner;
    private readonly IActiveSubscriberProbe? _subscriberProbe;
    private readonly AsyncResponseWatchdogState _state;
    private readonly AsyncResponseWatchdogOptions _options;
    private readonly ILogger<AsyncResponseWatchdog> _logger;

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
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("{ServiceName}: disabled via options; not scanning.", SERVICE_NAME);
            return;
        }

        if (_scanner is null)
        {
            _logger.LogInformation(
                "{ServiceName}: no IRecoveryStateScanner registered; the configured channel does not support scanning, so the watchdog is idle.",
                SERVICE_NAME);
            return;
        }

        _logger.LogInformation("{ServiceName}: started. Interval: {Interval}, stale threshold: {StaleAfter}.",
            SERVICE_NAME, _options.Interval, _options.StaleAfter);

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
                    _logger.LogError(ex, "{ServiceName}: scan failed; next attempt in {Interval}.", SERVICE_NAME, _options.Interval);
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
        var snapshot = new List<RecoveryStateObservation>();

        await foreach (var entry in _scanner!.ScanAsync(cancellationToken).ConfigureAwait(false))
        {
            if (entry is null)
                continue;

            var activeSubscribers = await CountActiveSubscribersAsync(entry.CorrelationId, cancellationToken).ConfigureAwait(false);

            snapshot.Add(new RecoveryStateObservation(
                entry.CorrelationId,
                entry.RegisteredAtUtc,
                activeSubscribers,
                entry.PayloadTypeFullName));
        }

        var report = AsyncResponseWatchdogReport.Evaluate(snapshot, DateTime.UtcNow, _options.StaleAfter);

        _logger.LogInformation(
            "{ServiceName}: scan complete. Outstanding registrations: {Total}, with live waiter: {Active}, stale (no waiter, older than {StaleAfter}): {Stale}, unknown age: {UnknownAge}.",
            SERVICE_NAME, report.TotalEntries, report.EntriesWithActiveWaiter, _options.StaleAfter, report.StaleEntries.Count, report.UnknownAgeEntries);

        foreach (var stale in report.StaleEntries)
        {
            _logger.LogWarning(
                "{ServiceName}: stale async-response recovery state detected — correlationId {CorrelationId}, payload type {PayloadType}, registered {RegisteredAtUtc:u}, no live subscriber. The owning flow is likely stuck; investigate and resume or fail it.",
                SERVICE_NAME, stale.CorrelationId, stale.PayloadTypeFullName, stale.RegisteredAtUtc);
        }

        return report;
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
            _logger.LogDebug(ex, "{ServiceName}: failed to probe subscribers for correlationId {CorrelationId}.", SERVICE_NAME, correlationId);
            return -1;
        }
    }
}
