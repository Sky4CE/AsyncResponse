// Binds the shared database-channel machinery (src/Channels/Shared/DbChannelShared.cs, compiled
// into this project) to this provider's concrete seam types. See the note atop the shared file.
global using DbChannelStore = AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelSql;
global using DbChannelMessage = AsyncResponse.Channels.PostgreSQL.PostgreSqlChannelMessage;
global using DbChannelOptions = AsyncResponse.Channels.PostgreSQL.PostgreSqlAsyncResponseChannelOptions;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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

    /// <inheritdoc />
    protected override Task? StartWakeListener(CancellationToken cancellationToken)
        => Task.Run(() => ListenLoopAsync(cancellationToken));

    /// <inheritdoc />
    protected override IAsyncResponseWaiter<T> CreateWaiter<T>(Task<T> responseTask, Func<ValueTask> cleanupAsync)
        => new PostgreSqlAsyncResponseWaiter<T>(responseTask, cleanupAsync);

    private async Task ListenLoopAsync(CancellationToken cancellationToken)
    {
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _store.ExecuteListenAsync(payload =>
                {
                    SignalDispatcher(string.IsNullOrEmpty(payload) ? null : payload);
                    return Task.CompletedTask;
                }, cancellationToken).ConfigureAwait(false);
                failures = 0;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                failures++;
                var delay = AsyncResponseRetry.Backoff(failures, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(5));
                _logger.LogWarning(ex, "PostgreSQL LISTEN loop failed; retrying in {Delay}.", delay);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
