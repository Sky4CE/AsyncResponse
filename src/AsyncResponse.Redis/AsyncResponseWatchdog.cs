using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Text.Json;

namespace AsyncResponse.Redis;

/// <summary>Options for the async-response watchdog.</summary>
public sealed class AsyncResponseWatchdogOptions
{
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
public sealed record RecoveryStateObservation(
    string RecoveryKey,
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
/// readiness health check) get a cheap, consistent snapshot without touching Redis.
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
    /// registration timestamp are reported separately as unknown-age.
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
/// Periodic, report-only scanner of the persisted async-response recovery state.
/// <para>
/// Every recovery entry in Redis represents an outstanding wait registration. A healthy entry
/// either has a live subscriber (the waiter is awaiting in some process) or is young — armed
/// recovery state for a response that has not arrived yet. An entry that is <em>old</em> and has
/// <em>no subscriber</em> means the waiter died and nothing (response, resume, retry) has touched
/// the flow since: the precursor of an operation stuck "in progress". The watchdog logs a warning
/// per such entry and publishes a summary snapshot for the health check. It deliberately performs
/// no remediation — recovery belongs to the lost-subscriber dispatcher and the flows' own retry
/// paths.
/// </para>
/// </summary>
internal sealed class AsyncResponseWatchdog : BackgroundService
{
    private const string SERVICE_NAME = nameof(AsyncResponseWatchdog);

    private readonly IConnectionMultiplexer _multiplexer;
    private readonly AsyncResponseWatchdogState _state;
    private readonly RedisKeySchema _keys;
    private readonly AsyncResponseWatchdogOptions _options;
    private readonly ILogger<AsyncResponseWatchdog> _logger;

    public AsyncResponseWatchdog(
        IConnectionMultiplexer multiplexer,
        AsyncResponseWatchdogState state,
        IOptions<RedisAsyncResponseOptions> transportOptions,
        IOptions<AsyncResponseWatchdogOptions> watchdogOptions,
        ILogger<AsyncResponseWatchdog> logger)
    {
        _multiplexer = multiplexer;
        _state = state;
        _keys = new RedisKeySchema(transportOptions.Value.KeyPrefix);
        _options = watchdogOptions.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("{ServiceName}: started. Interval: {Interval}, stale threshold: {StaleAfter}.",
            SERVICE_NAME, _options.Interval, _options.StaleAfter);

        try
        {
            await Task.Delay(_options.StartupDelay, stoppingToken).ConfigureAwait(false);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var report = await ScanOnceAsync().ConfigureAwait(false);
                    _state.Publish(new AsyncResponseWatchdogSnapshot(DateTime.UtcNow, _options.Interval, report, Error: null));
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

    private async Task<AsyncResponseWatchdogReport> ScanOnceAsync()
    {
        var connectedServers = _multiplexer.GetEndPoints()
            .Select(endPoint => _multiplexer.GetServer(endPoint))
            .Where(server => server.IsConnected)
            .ToList();

        var snapshot = new List<RecoveryStateObservation>();
        var seenKeys = new HashSet<string>(StringComparer.Ordinal);
        var database = _multiplexer.GetDatabase();

        foreach (var server in connectedServers)
        {
            foreach (var key in server.Keys(pattern: _keys.RecoveryKeyPattern, pageSize: 250))
            {
                var recoveryKey = key.ToString();
                if (!seenKeys.Add(recoveryKey))
                    continue;

                var value = await database.StringGetAsync(recoveryKey).ConfigureAwait(false);
                if (value.IsNullOrEmpty)
                    continue;

                RecoveryState? entry;
                try
                {
                    entry = JsonSerializer.Deserialize<RecoveryState>(value.ToString());
                }
                catch (JsonException ex)
                {
                    _logger.LogWarning(ex, "{ServiceName}: unreadable recovery state at {RecoveryKey}; skipping.", SERVICE_NAME, recoveryKey);
                    continue;
                }

                if (entry is null)
                    continue;

                var correlationId = entry.CorrelationId ?? _keys.CorrelationIdFromRecoveryKey(recoveryKey);
                var channel = _keys.Channel(correlationId);

                snapshot.Add(new RecoveryStateObservation(
                    recoveryKey,
                    correlationId,
                    entry.RegisteredAtUtc,
                    GetSubscriberCount(connectedServers, channel),
                    entry.PayloadTypeFullName));
            }
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
    /// Subscriptions live on whichever node the client subscribed through, so the count is the
    /// maximum across all connected endpoints.
    /// </summary>
    private long GetSubscriberCount(IReadOnlyList<IServer> servers, RedisChannel channel)
    {
        long subscribers = 0;
        foreach (var server in servers)
        {
            try
            {
                subscribers = Math.Max(subscribers, server.SubscriptionSubscriberCount(channel));
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "{ServiceName}: failed to read subscriber count for channel {Channel}.", SERVICE_NAME, channel.ToString());
            }
        }

        return subscribers;
    }
}
