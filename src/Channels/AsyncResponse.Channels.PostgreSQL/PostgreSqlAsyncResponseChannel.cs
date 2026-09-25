// Binds the shared database-channel machinery (src/Channels/Shared/DbChannelShared.cs, compiled
// into this project) to this provider's concrete seam types. See the note atop the shared file.
global using DbChannelStore = AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelSql;
global using DbChannelMessage = AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelMessage;
global using DbChannelOptions = AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseChannelOptions;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace AsyncResponse.Channels.PostgreSQL;

/// <summary>
/// PostgreSQL-backed response channel using <c>LISTEN/NOTIFY</c> for active waiter wakeups and
/// PostgreSQL tables for durable recovery state.
/// </summary>
internal sealed class PostgreSqlAsyncResponseChannel : DbAsyncResponseChannelBase
{
    /// <summary>Creates a PostgreSQL-backed async-response channel.</summary>
    public PostgreSqlAsyncResponseChannel(
        IServiceScopeFactory scopeFactory,
        PostgreSqlChannelSql sql,
        IRecoveryStateStore recoveryStateStore,
        IOptions<PostgreSqlAsyncResponseChannelOptions> options,
        AsyncResponseContextPropagation propagation,
        ILogger<PostgreSqlAsyncResponseChannel> logger,
        TimeProvider? timeProvider = null)
        : base(
            scopeFactory,
            sql,
            recoveryStateStore,
            options.Value,
            propagation,
            logger,
            channelTypeName: nameof(PostgreSqlAsyncResponseChannel),
            providerName: "PostgreSQL",
            activityTag: "postgresql",
            subscriberRecordNoun: "row",
            localDispatchRetryHint: "listener retry will pick it up",
            timeProvider)
    {
    }

    /// <inheritdoc />
    protected override string ChannelName(string correlationId) => $"{_options.NotificationChannel}:{correlationId}";

    /// <inheritdoc />
    protected override TimeSpan CurrentPollInterval() => _options.ListenerPollInterval;

    // True only while a LISTEN is established: set once LISTEN succeeds on the listen connection,
    // cleared as soon as that connection fails, until the next successful LISTEN. Read on every
    // poll tick, so volatile.
    private volatile bool _listening;

    /// <summary>
    /// The throttled sweep applies only while NOTIFY carries normal delivery. Without an
    /// established LISTEN — before the first one, and across every reconnect after a dropped,
    /// half-open or refused listen connection — the sweep is the ONLY cross-process wake, and the
    /// 5s default throttle equals <c>DeliveryConfirmationTimeout</c>: publishers in other
    /// processes gave up and claimed their responses for lost-subscriber recovery a beat before
    /// the healthy waiter's throttled sweep found them. Until LISTEN is back the sweep runs at the
    /// wake-down interval (a quarter of the confirmation budget), not on every tick.
    /// </summary>
    protected override TimeSpan? CurrentFullSweepInterval() => _listening ? _options.FullSweepInterval : WakeDownFullSweepInterval();

    /// <inheritdoc />
    protected override Task? StartWakeListener(CancellationToken cancellationToken)
        => Task.Run(() => ListenLoopAsync(cancellationToken));

    /// <summary>
    /// A LISTEN (re)established: NOTIFY carries delivery again, and the NOTIFYs published while
    /// none was up are gone, so sweep every waiter once now instead of leaving the responses they
    /// announced to the next throttled sweep.
    /// </summary>
    private void OnWakeListenerEstablished()
    {
        _listening = true;
        SignalDispatcher();
    }

    /// <inheritdoc />
    protected override IAsyncResponseWaiter<T> CreateWaiter<T>(Task<T> responseTask, Func<ValueTask> cleanupAsync)
        => new PostgreSqlAsyncResponseWaiter<T>(responseTask, cleanupAsync);

    private async Task ListenLoopAsync(CancellationToken cancellationToken)
    {
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            // Stopwatch timestamp of this attempt's established LISTEN, if it got that far.
            long? listeningSince = null;
            try
            {
                await _store.ExecuteListenAsync(
                    payload =>
                    {
                        SignalDispatcher(string.IsNullOrEmpty(payload) ? null : payload);
                        return Task.CompletedTask;
                    },
                    cancellationToken,
                    () =>
                    {
                        listeningSince = Stopwatch.GetTimestamp();
                        OnWakeListenerEstablished();
                    }).ConfigureAwait(false);
                failures = 0;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _listening = false;
                // A LISTEN that stayed up a healthy run ends the failure run (see
                // _wakeListenerHealthyRun). The listen method returns only on shutdown (every
                // failure throws), so the reset after it never runs.
                if (listeningSince is { } since && Stopwatch.GetElapsedTime(since) >= _wakeListenerHealthyRun)
                    failures = 0;
                failures++;
                var delay = AsyncResponseRetry.Backoff(failures, TimeSpan.FromMilliseconds(100), WakeListenerMaxRetryDelay);
                _logger.LogWarning(ex, "PostgreSQL LISTEN loop failed; retrying in {Delay}.", delay);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
