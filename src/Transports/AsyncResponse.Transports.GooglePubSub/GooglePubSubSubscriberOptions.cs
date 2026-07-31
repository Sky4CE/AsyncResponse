using Google.Cloud.PubSub.V1;

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
    /// Handler failures are logged and reported through
    /// <see cref="GooglePubSubSubscriberOptions.OnBackgroundFailure"/> because Pub/Sub has already
    /// been ACKed.
    /// </summary>
    AckAfterEnqueue = 1
}

/// <summary>
/// Describes a handler failure that happened after a Google Pub/Sub message was already ACKed by
/// <see cref="GooglePubSubAckMode.AckAfterEnqueue"/>.
/// </summary>
public sealed class GooglePubSubBackgroundFailureContext
{
    internal GooglePubSubBackgroundFailureContext(
        string subscriptionId,
        string subscriberRole,
        PubsubMessage message,
        Exception exception)
    {
        SubscriptionId = subscriptionId;
        SubscriberRole = subscriberRole;
        Message = message;
        Exception = exception;
    }

    /// <summary>The subscription whose background worker was handling the message.</summary>
    public string SubscriptionId { get; }

    /// <summary>The logical subscriber role, such as <c>Worker</c> or <c>ResponseIngress</c>.</summary>
    public string SubscriberRole { get; }

    /// <summary>The Pub/Sub message that failed after being ACKed.</summary>
    public PubsubMessage Message { get; }

    /// <summary>The Pub/Sub message id, when provided by Google Pub/Sub.</summary>
    public string MessageId => Message.MessageId;

    /// <summary>The exception thrown by the background handler.</summary>
    public Exception Exception { get; }
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
    /// Values greater than one allow concurrent handling and therefore do not preserve message
    /// ordering.
    /// </summary>
    public int BackgroundWorkerCount { get; set; }

    /// <summary>
    /// Maximum number of messages waiting in the background queue for
    /// <see cref="GooglePubSubAckMode.AckAfterEnqueue"/>. Must be explicitly set to a positive value.
    /// When full, the Pub/Sub callback parks awaiting queue space and ACKs once the message is
    /// accepted — it does not NACK on a full queue; NACK is returned only when the write fails
    /// (shutdown/disposal), so the message is redelivered rather than lost.
    /// </summary>
    public int BackgroundQueueCapacity { get; set; }

    /// <summary>
    /// Maximum time to wait for queued/running background handlers while the hosted subscriber stops.
    /// </summary>
    public TimeSpan BackgroundDrainTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Optional callback invoked when a background handler fails after the message was already ACKed.
    /// Use it to publish to a dead-letter path, increment operator-visible metrics, or alert on
    /// already-ACKed work that Pub/Sub cannot redeliver.
    /// </summary>
    public Func<GooglePubSubBackgroundFailureContext, ValueTask>? OnBackgroundFailure { get; set; }

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
