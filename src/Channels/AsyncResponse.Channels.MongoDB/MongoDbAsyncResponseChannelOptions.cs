namespace AsyncResponse.Channels.MongoDB;

/// <summary>
/// Options for the MongoDB-backed async-response channel.
/// <para>
/// Active waiters are woken with a MongoDB change stream watching inserts to the response-message
/// collection; the change event carries the correlation id, so only the signaled waiter's messages
/// are scanned. Response envelopes are stored as documents, and durable <see cref="RecoveryState"/>
/// entries live in a TTL-indexed collection so late responses can resume or fail flows after the
/// original waiter process dies. Change streams require the server to run as a replica set (a
/// single-node replica set is sufficient); without one the channel degrades to interval polling.
/// </para>
/// </summary>
public sealed class MongoDbAsyncResponseChannelOptions : DurableAsyncResponseChannelOptions
{
    /// <summary>The channel name reported to the startup validator.</summary>
    public const string ChannelName = "MongoDB";

    /// <summary>
    /// Optional MongoDB connection string used when no <c>IMongoDatabase</c> or <c>IMongoClient</c>
    /// is registered with the host.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>Optional database name used when no <c>IMongoDatabase</c> is registered.</summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// Collection storing durable recovery registrations. Each waiter registration is one document
    /// keyed by correlation id and registration id, expired natively by a TTL index.
    /// </summary>
    public string RecoveryStateCollection { get; set; } = "asyncresponse_recovery_state";

    /// <summary>
    /// Collection storing response envelopes until they expire. The change stream watches inserts to
    /// this collection; waiters load the envelope documents from it.
    /// </summary>
    public string MessageCollection { get; set; } = "asyncresponse_channel_messages";

    /// <summary>
    /// Collection storing short-lived live-subscriber heartbeats for watchdog liveness and the
    /// publish fast path, expired natively by a TTL index.
    /// </summary>
    public string SubscriberCollection { get; set; } = "asyncresponse_channel_subscribers";

    /// <summary>
    /// Creates the TTL and lookup indexes on first use. Disable when provisioning owns index DDL.
    /// </summary>
    public bool AutoCreateIndexes { get; set; } = true;

    /// <summary>
    /// Watches the message collection with a change stream so active waiters are woken with
    /// broker-grade latency. Requires a replica set. When disabled — or when the server reports
    /// change streams as unsupported — waiters fall back to <see cref="ListenerPollInterval"/>
    /// polling. Default: <c>true</c>.
    /// </summary>
    public bool UseChangeStreams { get; set; } = true;

    /// <summary>
    /// How long response-envelope documents are retained for active waiter delivery and
    /// missed-notification recovery. Expired documents are reaped by the TTL index.
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
    /// Fallback poll interval used by the dispatch loop to catch messages if a change-stream event
    /// is missed during reconnect (or change streams are unavailable). Default: 250 ms.
    /// </summary>
    public TimeSpan ListenerPollInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Number of pending response messages loaded per subscribed correlation id per dispatch pass.
    /// Default: 64.
    /// </summary>
    public int PendingMessageBatchSize { get; set; } = 64;

    /// <summary>
    /// How often a live waiter refreshes its subscriber heartbeat document. Default: 10 seconds.
    /// </summary>
    public TimeSpan SubscriberHeartbeatInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long a subscriber heartbeat remains live without refresh. Keep this above
    /// <see cref="SubscriberHeartbeatInterval"/>. Default: 30 seconds.
    /// </summary>
    public TimeSpan SubscriberHeartbeatTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Maximum attempts for a response-document insert. Set to 1 to disable publish retries. Default: 3.</summary>
    public int PublishMaxAttempts { get; set; } = 3;

    /// <summary>Initial delay before retrying a failed response-document insert. Default: 50 ms.</summary>
    public TimeSpan PublishRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Maximum delay between response-document insert retries. Default: 1 second.</summary>
    public TimeSpan PublishRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Validates the option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        // Shared channel knobs (RecoveryStateExpiry, DefaultTimeout, DisposalDrainTimeout) go
        // through the ONE base guard set — a bespoke duplicate here silently missed every knob
        // added to the base later (DisposalDrainTimeout was validated nowhere on this provider).
        ValidateShared(nameof(MongoDbAsyncResponseChannelOptions));

        MongoDbChannelStore.ValidateCollectionName(RecoveryStateCollection, nameof(RecoveryStateCollection));
        MongoDbChannelStore.ValidateCollectionName(MessageCollection, nameof(MessageCollection));
        MongoDbChannelStore.ValidateCollectionName(SubscriberCollection, nameof(SubscriberCollection));

        if (StringComparer.Ordinal.Equals(RecoveryStateCollection, MessageCollection)
            || StringComparer.Ordinal.Equals(RecoveryStateCollection, SubscriberCollection)
            || StringComparer.Ordinal.Equals(MessageCollection, SubscriberCollection))
        {
            throw new InvalidOperationException(
                $"{nameof(MongoDbAsyncResponseChannelOptions)}.{nameof(RecoveryStateCollection)}, " +
                $"{nameof(MessageCollection)}, and {nameof(SubscriberCollection)} must be distinct collections.");
        }

        Positive(MessageRetention, nameof(MessageRetention));
        Positive(DeliveryConfirmationTimeout, nameof(DeliveryConfirmationTimeout));
        Positive(DeliveryConfirmationPollInterval, nameof(DeliveryConfirmationPollInterval));
        Positive(ListenerPollInterval, nameof(ListenerPollInterval));
        Positive(SubscriberHeartbeatInterval, nameof(SubscriberHeartbeatInterval));
        Positive(SubscriberHeartbeatTimeout, nameof(SubscriberHeartbeatTimeout));

        if (MaxRemoteStackTraceLength < 0)
            throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseChannelOptions)}.{nameof(MaxRemoteStackTraceLength)} must not be negative.");

        if (PendingMessageBatchSize <= 0)
            throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseChannelOptions)}.{nameof(PendingMessageBatchSize)} must be positive.");

        if (SubscriberHeartbeatInterval >= SubscriberHeartbeatTimeout)
            throw new InvalidOperationException(
                $"{nameof(MongoDbAsyncResponseChannelOptions)}.{nameof(SubscriberHeartbeatInterval)} must be less than " +
                $"{nameof(MongoDbAsyncResponseChannelOptions)}.{nameof(SubscriberHeartbeatTimeout)}.");

        if (PublishMaxAttempts <= 0)
            throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseChannelOptions)}.{nameof(PublishMaxAttempts)} must be positive.");

        Positive(PublishRetryBaseDelay, nameof(PublishRetryBaseDelay));
        Positive(PublishRetryMaxDelay, nameof(PublishRetryMaxDelay));
        if (PublishRetryBaseDelay > PublishRetryMaxDelay)
            throw new InvalidOperationException(
                $"{nameof(MongoDbAsyncResponseChannelOptions)}.{nameof(PublishRetryBaseDelay)} cannot exceed {nameof(PublishRetryMaxDelay)}.");
    }

    private static void Positive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseChannelOptions)}.{name} must be positive.");
    }
}
