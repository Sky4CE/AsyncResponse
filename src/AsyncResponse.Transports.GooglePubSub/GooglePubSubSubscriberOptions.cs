namespace AsyncResponse.Transports.GooglePubSub;

/// <summary>
/// Controls when a Google Pub/Sub message is acknowledged relative to AsyncResponse handling.
/// </summary>
public enum GooglePubSubAckMode
{
    /// <summary>
    /// ACK only after the AsyncResponse handler completes successfully; NACK if the handler throws.
    /// This is the default and preserves Pub/Sub retry semantics for handler failures.
    /// </summary>
    AckAfterHandlerCompletes = 0,

    /// <summary>
    /// ACK immediately after the message is accepted into a bounded in-process background queue.
    /// Handler failures are logged because Pub/Sub has already been ACKed.
    /// </summary>
    AckAfterEnqueue = 1
}

/// <summary>
/// Per-subscription Google Pub/Sub subscriber behavior.
/// </summary>
public sealed class GooglePubSubSubscriberOptions
{
    /// <summary>
    /// Controls when the Pub/Sub callback returns ACK. Defaults to
    /// <see cref="GooglePubSubAckMode.AckAfterHandlerCompletes"/>.
    /// </summary>
    public GooglePubSubAckMode AckMode { get; set; } = GooglePubSubAckMode.AckAfterHandlerCompletes;

    /// <summary>
    /// Number of background workers used by <see cref="GooglePubSubAckMode.AckAfterEnqueue"/>.
    /// Must be explicitly set to a positive value for early ACK mode.
    /// </summary>
    public int BackgroundWorkerCount { get; set; }

    /// <summary>
    /// Maximum number of messages waiting in the background queue for
    /// <see cref="GooglePubSubAckMode.AckAfterEnqueue"/>. Must be explicitly set to a positive value.
    /// When full, the Pub/Sub callback returns NACK so the message can be redelivered.
    /// </summary>
    public int BackgroundQueueCapacity { get; set; }

    /// <summary>
    /// Maximum time to wait for queued/running background handlers while the hosted subscriber stops.
    /// </summary>
    public TimeSpan BackgroundDrainTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Explicitly opts this subscriber into ACK-after-enqueue behavior.
    /// </summary>
    public GooglePubSubSubscriberOptions UseAckAfterEnqueue(
        int backgroundWorkerCount,
        int backgroundQueueCapacity,
        TimeSpan? backgroundDrainTimeout = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundWorkerCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundQueueCapacity);

        if (backgroundDrainTimeout is { } timeout && timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(backgroundDrainTimeout), timeout, "Drain timeout must be positive.");

        AckMode = GooglePubSubAckMode.AckAfterEnqueue;
        BackgroundWorkerCount = backgroundWorkerCount;
        BackgroundQueueCapacity = backgroundQueueCapacity;

        if (backgroundDrainTimeout is not null)
            BackgroundDrainTimeout = backgroundDrainTimeout.Value;

        return this;
    }
}
