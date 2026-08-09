namespace AsyncResponse.Channels.SqlServer;

/// <summary>
/// Options for the Microsoft SQL Server-backed async-response channel.
/// <para>
/// SQL Server has no <c>LISTEN/NOTIFY</c>, so active waiters are woken by an adaptive polling sweep:
/// while any waiter is subscribed the dispatch loop scans the message table every
/// <see cref="ActivePollInterval"/>, and with no waiters it backs off to <see cref="IdlePollInterval"/>.
/// Same-process publishes bypass the sweep and deliver immediately. Response envelopes are stored in
/// a table; durable <see cref="RecoveryState"/> entries live in a separate table so late responses
/// can resume or fail flows after the original waiter process dies.
/// </para>
/// </summary>
public sealed class SqlServerAsyncResponseChannelOptions : DurableAsyncResponseChannelOptions
{
    /// <summary>The channel name reported to the startup validator.</summary>
    public const string ChannelName = "SqlServer";

    /// <summary>
    /// SQL Server connection string used for every channel operation. Required. The database it
    /// targets must already exist; the channel creates only its schema, tables, and indexes.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>Database schema that contains the channel tables. Default: <c>dbo</c>.</summary>
    public string SchemaName { get; set; } = "dbo";

    /// <summary>
    /// Table storing durable recovery registrations. Each waiter registration is one row keyed by
    /// correlation id and registration id.
    /// </summary>
    public string RecoveryStateTable { get; set; } = "asyncresponse_recovery_state";

    /// <summary>
    /// Table storing response envelopes until they expire. The adaptive polling sweep loads pending
    /// envelopes from this table and delivers them to local waiters.
    /// </summary>
    public string MessageTable { get; set; } = "asyncresponse_channel_messages";

    /// <summary>
    /// Table storing short-lived live-subscriber heartbeats for watchdog liveness and the publish
    /// fast path.
    /// </summary>
    public string SubscriberTable { get; set; } = "asyncresponse_channel_subscribers";

    /// <summary>
    /// Creates the schema, tables, and indexes on first use. Disable when migrations provision them
    /// out of band.
    /// </summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// How long response-envelope rows are retained for active waiter delivery and cross-process
    /// sweep recovery. Expired rows are pruned opportunistically during channel operations.
    /// </summary>
    public TimeSpan MessageRetention { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// How long a publisher waits for a live waiter to acknowledge loading a response envelope
    /// before treating the response as lost-subscriber delivery. Default: 5 seconds.
    /// </summary>
    public TimeSpan DeliveryConfirmationTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Poll interval used while a publisher waits for delivery acknowledgement. Default: 50 ms.
    /// </summary>
    public TimeSpan DeliveryConfirmationPollInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Sweep interval used by the dispatch loop while at least one waiter is subscribed. This bounds
    /// the wake latency of a response published by another process, so keep it tight. Same-process
    /// deliveries do not wait for the sweep. Default: 250 ms.
    /// </summary>
    public TimeSpan ActivePollInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Sweep interval used by the dispatch loop while no waiters are subscribed, so an idle
    /// application does not hammer the database. Must be at least <see cref="ActivePollInterval"/>;
    /// a new waiter re-arms the tight interval immediately. Default: 2 seconds.
    /// </summary>
    public TimeSpan IdlePollInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Number of pending response messages loaded per subscribed correlation id per sweep pass.
    /// Default: 64.
    /// </summary>
    public int PendingMessageBatchSize { get; set; } = 64;

    /// <summary>
    /// How often a live waiter refreshes its subscriber heartbeat row. Default: 10 seconds.
    /// </summary>
    public TimeSpan SubscriberHeartbeatInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long a subscriber heartbeat remains live without refresh. Keep this above
    /// <see cref="SubscriberHeartbeatInterval"/>. Default: 30 seconds.
    /// </summary>
    public TimeSpan SubscriberHeartbeatTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Minimum interval between opportunistic prunes of expired channel rows. Pruning is housekeeping
    /// only (read queries filter on expiry), so throttling it keeps publishes off a full-table delete
    /// on every call. Set to <see cref="TimeSpan.Zero"/> to prune on every operation. Default: 30 seconds.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Maximum attempts for a response-row insert. Set to 1 to disable publish retries. Default: 3.</summary>
    public int PublishMaxAttempts { get; set; } = 3;

    /// <summary>Initial delay before retrying a failed response-row insert. Default: 50 ms.</summary>
    public TimeSpan PublishRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Maximum delay between response-row insert retries. Default: 1 second.</summary>
    public TimeSpan PublishRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Validates the option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        // Shared channel knobs (RecoveryStateExpiry, DefaultTimeout, DisposalDrainTimeout) go
        // through the ONE base guard set — a bespoke duplicate here silently missed every knob
        // added to the base later (DisposalDrainTimeout was validated nowhere on this provider).
        ValidateShared(nameof(SqlServerAsyncResponseChannelOptions));

        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseChannelOptions)}.{nameof(ConnectionString)} must be configured.");

        SqlServerChannelSql.ValidateIdentifier(SchemaName, nameof(SchemaName));
        SqlServerChannelSql.ValidateIdentifier(RecoveryStateTable, nameof(RecoveryStateTable));
        SqlServerChannelSql.ValidateIdentifier(MessageTable, nameof(MessageTable));
        SqlServerChannelSql.ValidateIdentifier(SubscriberTable, nameof(SubscriberTable));

        EnsurePersistedTtl(MessageRetention, nameof(SqlServerAsyncResponseChannelOptions), nameof(MessageRetention));
        EnsurePersistedTtl(DeliveryConfirmationTimeout, nameof(SqlServerAsyncResponseChannelOptions), nameof(DeliveryConfirmationTimeout));
        EnsureTimerBacked(DeliveryConfirmationPollInterval, nameof(SqlServerAsyncResponseChannelOptions), nameof(DeliveryConfirmationPollInterval));
        EnsureTimerBacked(ActivePollInterval, nameof(SqlServerAsyncResponseChannelOptions), nameof(ActivePollInterval));
        EnsureTimerBacked(IdlePollInterval, nameof(SqlServerAsyncResponseChannelOptions), nameof(IdlePollInterval));
        EnsureTimerBacked(SubscriberHeartbeatInterval, nameof(SqlServerAsyncResponseChannelOptions), nameof(SubscriberHeartbeatInterval));
        EnsurePersistedTtl(SubscriberHeartbeatTimeout, nameof(SqlServerAsyncResponseChannelOptions), nameof(SubscriberHeartbeatTimeout));

        if (ActivePollInterval > IdlePollInterval)
            throw new InvalidOperationException(
                $"{nameof(SqlServerAsyncResponseChannelOptions)}.{nameof(ActivePollInterval)} cannot exceed " +
                $"{nameof(SqlServerAsyncResponseChannelOptions)}.{nameof(IdlePollInterval)}; the idle interval is the backed-off sweep.");

        if (MaxRemoteStackTraceLength < 0)
            throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseChannelOptions)}.{nameof(MaxRemoteStackTraceLength)} must not be negative.");

        if (PendingMessageBatchSize <= 0)
            throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseChannelOptions)}.{nameof(PendingMessageBatchSize)} must be positive.");

        if (SubscriberHeartbeatInterval >= SubscriberHeartbeatTimeout)
            throw new InvalidOperationException(
                $"{nameof(SqlServerAsyncResponseChannelOptions)}.{nameof(SubscriberHeartbeatInterval)} must be less than " +
                $"{nameof(SqlServerAsyncResponseChannelOptions)}.{nameof(SubscriberHeartbeatTimeout)}.");

        if (PruneInterval < TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseChannelOptions)}.{nameof(PruneInterval)} must not be negative.");

        if (PublishMaxAttempts <= 0)
            throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseChannelOptions)}.{nameof(PublishMaxAttempts)} must be positive.");

        EnsureTimerBacked(PublishRetryBaseDelay, nameof(SqlServerAsyncResponseChannelOptions), nameof(PublishRetryBaseDelay));
        EnsureTimerBacked(PublishRetryMaxDelay, nameof(SqlServerAsyncResponseChannelOptions), nameof(PublishRetryMaxDelay));
        if (PublishRetryBaseDelay > PublishRetryMaxDelay)
            throw new InvalidOperationException(
                $"{nameof(SqlServerAsyncResponseChannelOptions)}.{nameof(PublishRetryBaseDelay)} cannot exceed {nameof(PublishRetryMaxDelay)}.");
    }

}
