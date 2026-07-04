namespace AsyncResponse.Transports.SQS;

/// <summary>Controls when an SQS message is deleted relative to AsyncResponse handling.</summary>
public enum SqsAckMode
{
    /// <summary>
    /// Delete the message only after the AsyncResponse handler completes successfully. Handler
    /// failures leave the message invisible until its visibility timeout expires (or shorten the
    /// wait via <see cref="SqsSubscriberOptions.RedeliveryDelay"/>); SQS then redelivers it, and the
    /// queue's redrive policy dead-letters it after <c>maxReceiveCount</c> receives.
    /// </summary>
    AckAfterHandlerCompletes = 0,

    /// <summary>
    /// Delete the message immediately after it is accepted into a bounded in-process background
    /// queue. Handler failures are logged and reported through
    /// <see cref="SqsSubscriberOptions.OnBackgroundFailure"/> because SQS can no longer redeliver a
    /// deleted message.
    /// </summary>
    AckAfterEnqueue = 1
}

/// <summary>Describes a handler failure that happened after an SQS message was already deleted.</summary>
public sealed class SqsBackgroundFailureContext
{
    internal SqsBackgroundFailureContext(
        string queue,
        string subscriberRole,
        string messageId,
        int receiveCount,
        string? correlationId,
        Exception exception)
    {
        Queue = queue;
        SubscriberRole = subscriberRole;
        MessageId = messageId;
        ReceiveCount = receiveCount;
        CorrelationId = correlationId;
        Exception = exception;
    }

    /// <summary>The SQS queue the message came from.</summary>
    public string Queue { get; }

    /// <summary>The logical subscriber role, such as <c>Worker</c> or <c>ResponseIngress</c>.</summary>
    public string SubscriberRole { get; }

    /// <summary>The SQS message id of the already-deleted message.</summary>
    public string MessageId { get; }

    /// <summary>The SQS <c>ApproximateReceiveCount</c> observed when the message was received.</summary>
    public int ReceiveCount { get; }

    /// <summary>The AsyncResponse correlation id, when one was available.</summary>
    public string? CorrelationId { get; }

    /// <summary>The exception thrown by the background handler.</summary>
    public Exception Exception { get; }
}

/// <summary>Per-queue SQS subscriber behavior.</summary>
public sealed class SqsSubscriberOptions
{
    /// <summary>Controls when a message is deleted. Defaults to <see cref="SqsAckMode.AckAfterHandlerCompletes"/>.</summary>
    public SqsAckMode AckMode { get; set; } = SqsAckMode.AckAfterHandlerCompletes;

    /// <summary>
    /// Visibility timeout applied per receive. <c>null</c> (the default) uses the queue's configured
    /// visibility timeout. Must exceed the slowest expected handler in
    /// <see cref="SqsAckMode.AckAfterHandlerCompletes"/> so an in-flight message is not redelivered
    /// while still being handled.
    /// </summary>
    public TimeSpan? VisibilityTimeout { get; set; }

    /// <summary>
    /// When set, a failed handler shortens the message's remaining invisibility to this delay via
    /// <c>ChangeMessageVisibility</c>, scheduling a faster redelivery than waiting out the full
    /// visibility timeout. <c>null</c> (the default) lets the visibility timeout expire naturally.
    /// Redelivery accounting stays native either way: every receive increments
    /// <c>ApproximateReceiveCount</c>, and the queue's redrive policy dead-letters the message after
    /// <c>maxReceiveCount</c>.
    /// </summary>
    public TimeSpan? RedeliveryDelay { get; set; }

    /// <summary>Number of background workers used by <see cref="SqsAckMode.AckAfterEnqueue"/>.</summary>
    public int BackgroundWorkerCount { get; set; }

    /// <summary>Maximum number of deleted messages waiting in the background queue.</summary>
    public int BackgroundQueueCapacity { get; set; }

    /// <summary>Maximum time to wait for queued/running background handlers while stopping.</summary>
    public TimeSpan BackgroundDrainTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Optional callback invoked when a background handler fails after the message was already
    /// deleted. Use it to publish to a dead-letter path, increment operator-visible metrics, or
    /// alert on already-ACKed work that SQS cannot redeliver.
    /// </summary>
    public Func<SqsBackgroundFailureContext, ValueTask>? OnBackgroundFailure { get; set; }

    /// <summary>Explicitly opts this subscriber into delete-after-enqueue behavior.</summary>
    public SqsSubscriberOptions UseAckAfterEnqueue(
        int backgroundWorkerCount,
        int backgroundQueueCapacity,
        TimeSpan? backgroundDrainTimeout = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundWorkerCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundQueueCapacity);
        if (backgroundDrainTimeout is { } timeout && timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(backgroundDrainTimeout), timeout, "Drain timeout must be positive.");

        AckMode = SqsAckMode.AckAfterEnqueue;
        BackgroundWorkerCount = backgroundWorkerCount;
        BackgroundQueueCapacity = backgroundQueueCapacity;
        if (backgroundDrainTimeout is not null)
            BackgroundDrainTimeout = backgroundDrainTimeout.Value;
        return this;
    }
}
