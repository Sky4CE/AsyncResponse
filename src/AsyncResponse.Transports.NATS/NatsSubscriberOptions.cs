namespace AsyncResponse.Transports.NATS;

/// <summary>
/// Controls when a NATS JetStream message is acknowledged relative to AsyncResponse handling.
/// </summary>
public enum NatsAckMode
{
    /// <summary>
    /// ACK only after the AsyncResponse handler completes successfully. If the handler throws, the
    /// message is NAKed and redelivered after <see cref="NatsSubscriberOptions.RedeliveryDelay"/>
    /// until <see cref="NatsSubscriberOptions.MaxDeliveryAttempts"/> is reached.
    /// </summary>
    AckAfterHandlerCompletes = 0,

    /// <summary>
    /// ACK immediately after the message is accepted into a bounded in-process background queue.
    /// Handler failures are logged, reported through
    /// <see cref="NatsSubscriberOptions.OnBackgroundFailure"/>, and dead-lettered when enabled because
    /// JetStream has already been ACKed.
    /// </summary>
    AckAfterReceive = 1
}

/// <summary>
/// Describes a handler failure that happened after a NATS JetStream message was already ACKed by
/// <see cref="NatsAckMode.AckAfterReceive"/>.
/// </summary>
public sealed class NatsBackgroundFailureContext
{
    internal NatsBackgroundFailureContext(
        string subject,
        string consumer,
        string subscriberRole,
        long numDelivered,
        string? correlationId,
        Exception exception)
    {
        Subject = subject;
        Consumer = consumer;
        SubscriberRole = subscriberRole;
        NumDelivered = numDelivered;
        CorrelationId = correlationId;
        Exception = exception;
    }

    /// <summary>The NATS subject the message came from.</summary>
    public string Subject { get; }

    /// <summary>The durable JetStream consumer that received the message.</summary>
    public string Consumer { get; }

    /// <summary>The logical subscriber role, such as <c>Worker</c> or <c>ResponseIngress</c>.</summary>
    public string SubscriberRole { get; }

    /// <summary>The JetStream delivery count for the message.</summary>
    public long NumDelivered { get; }

    /// <summary>The AsyncResponse correlation id, when one was available.</summary>
    public string? CorrelationId { get; }

    /// <summary>The exception thrown by the background handler.</summary>
    public Exception Exception { get; }
}

/// <summary>Per-subject NATS JetStream subscriber behavior.</summary>
public sealed class NatsSubscriberOptions
{
    /// <summary>Controls when a message is ACKed. Defaults to <see cref="NatsAckMode.AckAfterHandlerCompletes"/>.</summary>
    public NatsAckMode AckMode { get; set; } = NatsAckMode.AckAfterHandlerCompletes;

    /// <summary>Maximum number of in-flight messages pulled from the consumer at once. Default: <c>16</c>.</summary>
    public int BatchSize { get; set; } = 16;

    /// <summary>
    /// Maximum number of delivery attempts before a failing message is ACKed and written to the
    /// dead-letter subject. <c>0</c> means unlimited retries. Default: <c>5</c>.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 5;

    /// <summary>Delay requested when NAKing a failed message, so redelivery is not immediate. Default: <c>5s</c>.</summary>
    public TimeSpan RedeliveryDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Number of background workers used by <see cref="NatsAckMode.AckAfterReceive"/>. Must be positive for early ACK.</summary>
    public int BackgroundWorkerCount { get; set; }

    /// <summary>
    /// Maximum number of messages waiting in the background queue for
    /// <see cref="NatsAckMode.AckAfterReceive"/>. When full, the consume loop pauses pulling new
    /// messages until capacity frees.
    /// </summary>
    public int BackgroundQueueCapacity { get; set; }

    /// <summary>Maximum time to wait for queued/running background handlers while the hosted subscriber stops.</summary>
    public TimeSpan BackgroundDrainTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Optional callback invoked when a background handler fails after the JetStream message was
    /// already ACKed. Use it to increment operator-visible metrics or alert on already-ACKed work.
    /// </summary>
    public Func<NatsBackgroundFailureContext, ValueTask>? OnBackgroundFailure { get; set; }

    /// <summary>Explicitly opts this subscriber into ACK-after-receive (early ACK) behavior.</summary>
    public NatsSubscriberOptions UseAckAfterReceive(
        int backgroundWorkerCount,
        int backgroundQueueCapacity,
        TimeSpan? backgroundDrainTimeout = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundWorkerCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundQueueCapacity);

        if (backgroundDrainTimeout is { } timeout && timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(backgroundDrainTimeout), timeout, "Drain timeout must be positive.");

        AckMode = NatsAckMode.AckAfterReceive;
        BackgroundWorkerCount = backgroundWorkerCount;
        BackgroundQueueCapacity = backgroundQueueCapacity;

        if (backgroundDrainTimeout is not null)
            BackgroundDrainTimeout = backgroundDrainTimeout.Value;

        return this;
    }
}
