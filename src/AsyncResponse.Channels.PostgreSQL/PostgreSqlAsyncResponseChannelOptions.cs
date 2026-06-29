namespace AsyncResponse.Channels.PostgreSQL;

/// <summary>
/// Options for the PostgreSQL-backed async-response channel.
/// <para>
/// Active waiters are woken with PostgreSQL <c>LISTEN/NOTIFY</c>. Response envelopes are stored in a
/// table and notifications carry only the message id, avoiding PostgreSQL's small NOTIFY payload
/// limit. Durable <see cref="RecoveryState"/> entries live in a separate table so late responses can
/// resume or fail flows after the original waiter process dies.
/// </para>
/// </summary>
public sealed class PostgreSqlAsyncResponseChannelOptions : DurableAsyncResponseChannelOptions
{
    /// <summary>The channel name reported to the startup validator.</summary>
    public const string ChannelName = "PostgreSQL";

    /// <summary>Database schema that contains the channel tables. Default: <c>public</c>.</summary>
    public string SchemaName { get; set; } = "public";

    /// <summary>
    /// Table storing durable recovery registrations. Each waiter registration is one row keyed by
    /// correlation id and registration id.
    /// </summary>
    public string RecoveryStateTable { get; set; } = "asyncresponse_recovery_state";

    /// <summary>
    /// Table storing response envelopes until they expire. Notifications carry message ids; waiters
    /// load the envelope from this table.
    /// </summary>
    public string MessageTable { get; set; } = "asyncresponse_channel_messages";

    /// <summary>
    /// Table storing short-lived live-subscriber heartbeats for watchdog liveness and the publish
    /// fast path.
    /// </summary>
    public string SubscriberTable { get; set; } = "asyncresponse_channel_subscribers";

    /// <summary>
    /// PostgreSQL notification channel used to wake local listener loops. Must be a simple
    /// PostgreSQL identifier. Default: <c>asyncresponse_channel_notify</c>.
    /// </summary>
    public string NotificationChannel { get; set; } = "asyncresponse_channel_notify";

    /// <summary>
    /// Creates the schema, tables, and indexes on first use. Disable when migrations provision them
    /// out of band.
    /// </summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// How long response-envelope rows are retained for active waiter delivery and missed-notify
    /// recovery. Expired rows are pruned opportunistically during channel operations.
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
    /// Fallback poll interval used by the listener loop to catch messages if a notification is
    /// missed during reconnect. Default: 250 ms.
    /// </summary>
    public TimeSpan ListenerPollInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Number of pending response messages loaded per subscribed correlation id per listener pass.
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

    /// <summary>Validates the option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        PostgreSqlChannelSql.ValidateIdentifier(SchemaName, nameof(SchemaName));
        PostgreSqlChannelSql.ValidateIdentifier(RecoveryStateTable, nameof(RecoveryStateTable));
        PostgreSqlChannelSql.ValidateIdentifier(MessageTable, nameof(MessageTable));
        PostgreSqlChannelSql.ValidateIdentifier(SubscriberTable, nameof(SubscriberTable));
        PostgreSqlChannelSql.ValidateIdentifier(NotificationChannel, nameof(NotificationChannel));

        Positive(RecoveryStateExpiry, nameof(RecoveryStateExpiry));
        Positive(MessageRetention, nameof(MessageRetention));
        Positive(DeliveryConfirmationTimeout, nameof(DeliveryConfirmationTimeout));
        Positive(DeliveryConfirmationPollInterval, nameof(DeliveryConfirmationPollInterval));
        Positive(ListenerPollInterval, nameof(ListenerPollInterval));
        Positive(SubscriberHeartbeatInterval, nameof(SubscriberHeartbeatInterval));
        Positive(SubscriberHeartbeatTimeout, nameof(SubscriberHeartbeatTimeout));

        if (DefaultTimeout is { } defaultTimeout && defaultTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(PostgreSqlAsyncResponseChannelOptions)}.{nameof(DefaultTimeout)} must be positive when set.");

        if (MaxRemoteStackTraceLength < 0)
            throw new InvalidOperationException($"{nameof(PostgreSqlAsyncResponseChannelOptions)}.{nameof(MaxRemoteStackTraceLength)} must not be negative.");

        if (PendingMessageBatchSize <= 0)
            throw new InvalidOperationException($"{nameof(PostgreSqlAsyncResponseChannelOptions)}.{nameof(PendingMessageBatchSize)} must be positive.");

        if (SubscriberHeartbeatInterval >= SubscriberHeartbeatTimeout)
            throw new InvalidOperationException(
                $"{nameof(PostgreSqlAsyncResponseChannelOptions)}.{nameof(SubscriberHeartbeatInterval)} must be less than " +
                $"{nameof(PostgreSqlAsyncResponseChannelOptions)}.{nameof(SubscriberHeartbeatTimeout)}.");
    }

    private static void Positive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(PostgreSqlAsyncResponseChannelOptions)}.{name} must be positive.");
    }
}
