namespace AsyncResponse.Transports.Redis;

/// <summary>
/// Controls when a Redis stream message is acknowledged relative to AsyncResponse handling.
/// </summary>
public enum RedisAckMode
{
    /// <summary>
    /// ACK only after the AsyncResponse handler completes successfully. If the handler throws, the
    /// message stays in the Redis pending-entry list and is reclaimed after
    /// <see cref="RedisSubscriberOptions.PendingMessageMinIdleTime"/>.
    /// </summary>
    AckAfterHandlerCompletes = 0,

    /// <summary>
    /// ACK immediately after the stream entry is accepted into a bounded in-process background
    /// queue. Handler failures are logged, reported through
    /// <see cref="RedisSubscriberOptions.OnBackgroundFailure"/>, and dead-lettered when enabled
    /// because Redis has already been ACKed.
    /// </summary>
    AckAfterEnqueue = 1
}

/// <summary>
/// Describes a handler failure that happened after a Redis stream entry was already ACKed by
/// <see cref="RedisAckMode.AckAfterEnqueue"/>.
/// </summary>
public sealed class RedisBackgroundFailureContext
{
    internal RedisBackgroundFailureContext(
        string stream,
        string consumerGroup,
        string subscriberRole,
        string messageId,
        string? correlationId,
        Exception exception)
    {
        Stream = stream;
        ConsumerGroup = consumerGroup;
        SubscriberRole = subscriberRole;
        MessageId = messageId;
        CorrelationId = correlationId;
        Exception = exception;
    }

    /// <summary>The Redis stream the entry came from.</summary>
    public string Stream { get; }

    /// <summary>The Redis consumer group that received the entry.</summary>
    public string ConsumerGroup { get; }

    /// <summary>The logical subscriber role, such as <c>Worker</c> or <c>ResponseIngress</c>.</summary>
    public string SubscriberRole { get; }

    /// <summary>The Redis stream entry id.</summary>
    public string MessageId { get; }

    /// <summary>The AsyncResponse correlation id, when one was available.</summary>
    public string? CorrelationId { get; }

    /// <summary>The exception thrown by the background handler.</summary>
    public Exception Exception { get; }
}

/// <summary>Per-stream Redis subscriber behavior.</summary>
public sealed class RedisSubscriberOptions
{
    /// <summary>
    /// Controls when a Redis stream entry is ACKed. Defaults to
    /// <see cref="RedisAckMode.AckAfterHandlerCompletes"/>.
    /// </summary>
    public RedisAckMode AckMode { get; set; } = RedisAckMode.AckAfterHandlerCompletes;

    /// <summary>Maximum number of new stream entries read per polling command. Default: <c>16</c>.</summary>
    public int BatchSize { get; set; } = 16;

    /// <summary>Delay between empty XREADGROUP polls. Default: <c>50ms</c>.</summary>
    public TimeSpan EmptyPollDelay { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// How long a failed pending entry must be idle before another loop claims it for retry.
    /// Default: <c>30s</c>.
    /// </summary>
    public TimeSpan PendingMessageMinIdleTime { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How often the subscriber scans the pending-entry list for retryable entries.
    /// Default: <c>5s</c>.
    /// </summary>
    public TimeSpan PendingClaimInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Maximum number of pending entries inspected/claimed per scan. Default: <c>16</c>.</summary>
    public int PendingClaimBatchSize { get; set; } = 16;

    /// <summary>
    /// Maximum number of delivery attempts before a failing message is ACKed and written to the
    /// dead-letter stream. <c>0</c> means unlimited retries. Default: <c>5</c>.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 5;

    /// <summary>
    /// Number of background workers used by <see cref="RedisAckMode.AckAfterEnqueue"/>.
    /// Must be explicitly set to a positive value for early ACK mode.
    /// </summary>
    public int BackgroundWorkerCount { get; set; }

    /// <summary>
    /// Maximum number of entries waiting in the background queue for
    /// <see cref="RedisAckMode.AckAfterEnqueue"/>. When full, the entry remains pending for retry.
    /// </summary>
    public int BackgroundQueueCapacity { get; set; }

    /// <summary>Maximum time to wait for queued/running background handlers while the hosted subscriber stops.</summary>
    public TimeSpan BackgroundDrainTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Optional callback invoked when a background handler fails after the Redis entry was already
    /// ACKed. Use it to increment operator-visible metrics or alert on already-ACKed work.
    /// </summary>
    public Func<RedisBackgroundFailureContext, ValueTask>? OnBackgroundFailure { get; set; }

    /// <summary>Explicitly opts this subscriber into ACK-after-enqueue behavior.</summary>
    public RedisSubscriberOptions UseAckAfterEnqueue(
        int backgroundWorkerCount,
        int backgroundQueueCapacity,
        TimeSpan? backgroundDrainTimeout = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundWorkerCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundQueueCapacity);

        if (backgroundDrainTimeout is { } timeout && timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(backgroundDrainTimeout), timeout, "Drain timeout must be positive.");

        AckMode = RedisAckMode.AckAfterEnqueue;
        BackgroundWorkerCount = backgroundWorkerCount;
        BackgroundQueueCapacity = backgroundQueueCapacity;

        if (backgroundDrainTimeout is not null)
            BackgroundDrainTimeout = backgroundDrainTimeout.Value;

        return this;
    }
}
