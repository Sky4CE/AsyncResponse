// Binds the shared database-channel machinery (src/Channels/Shared/DbChannelShared.cs, compiled
// into this project) to this provider's concrete seam types. See the note atop the shared file.
global using DbChannelStore = AsyncResponse.Channels.SqlServer.SqlServerChannelSql;
global using DbChannelMessage = AsyncResponse.Channels.SqlServer.SqlServerChannelMessage;
global using DbChannelOptions = AsyncResponse.Channels.SqlServer.SqlServerAsyncResponseChannelOptions;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AsyncResponse.Channels.SqlServer;

/// <summary>
/// Microsoft SQL Server-backed response channel using an adaptive polling sweep for active waiter
/// wakeups (SQL Server has no <c>LISTEN/NOTIFY</c>) and SQL Server tables for durable recovery state.
/// Same-process deliveries bypass the sweep entirely; cross-process deliveries are picked up within
/// <see cref="SqlServerAsyncResponseChannelOptions.ActivePollInterval"/>, and the sweep backs off to
/// <see cref="SqlServerAsyncResponseChannelOptions.IdlePollInterval"/> while no waiters are subscribed.
/// </summary>
internal sealed class SqlServerAsyncResponseChannel : DbAsyncResponseChannelBase
{
    /// <summary>Creates a SQL Server-backed async-response channel.</summary>
    public SqlServerAsyncResponseChannel(
        IServiceScopeFactory scopeFactory,
        SqlServerChannelSql sql,
        IRecoveryStateStore recoveryStateStore,
        IOptions<SqlServerAsyncResponseChannelOptions> options,
        AsyncResponseContextPropagation propagation,
        ILogger<SqlServerAsyncResponseChannel> logger,
        TimeProvider? timeProvider = null)
        : base(
            scopeFactory,
            sql,
            recoveryStateStore,
            options.Value,
            propagation,
            logger,
            channelTypeName: nameof(SqlServerAsyncResponseChannel),
            providerName: "SQL Server",
            activityTag: "sqlserver",
            subscriberRecordNoun: "row",
            localDispatchRetryHint: "the sweep retry will pick it up",
            timeProvider)
    {
    }

    /// <inheritdoc />
    protected override string ChannelName(string correlationId) => $"{_options.SchemaName}.{_options.MessageTable}:{correlationId}";

    /// <summary>
    /// The adaptive sweep cadence: tight while any waiter is subscribed (cross-process deliveries
    /// must land promptly), backed off while the channel is idle so an inactive application does not
    /// keep polling the database at delivery latency.
    /// </summary>
    protected override TimeSpan CurrentPollInterval()
        => _subscriptions.IsEmpty ? _options.IdlePollInterval : _options.ActivePollInterval;

    // No StartWakeListener override: SQL Server has no push notification, so the adaptive dispatch
    // sweep above IS the cross-process wake mechanism and the base keeps no listener task.

    /// <inheritdoc />
    protected override IAsyncResponseWaiter<T> CreateWaiter<T>(Task<T> responseTask, Func<ValueTask> cleanupAsync)
        => new SqlServerAsyncResponseWaiter<T>(responseTask, cleanupAsync);
}
