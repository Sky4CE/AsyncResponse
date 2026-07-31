namespace AsyncResponse.Transports.Kafka;

/// <summary>
/// Controls when a Kafka message's offset is committed relative to AsyncResponse handling.
/// </summary>
public enum KafkaAckMode
{
    /// <summary>
    /// Commit the offset only after the AsyncResponse handler completes. If the handler throws, the
    /// message is retried in-process with backoff (Kafka offsets cannot NACK a single message);
    /// after <see cref="KafkaSubscriberOptions.MaxDeliveryAttempts"/> the message is produced to
    /// the dead-letter topic and its offset is committed so the partition keeps moving. Messages
    /// are processed serially per assignment, preserving per-partition ordering.
    /// </summary>
    AckAfterHandlerCompletes = 0,

    /// <summary>
    /// Commit the offset immediately after the message is accepted into a bounded in-process
    /// background queue. Handler failures are retried in-process, logged, reported through
    /// <see cref="KafkaSubscriberOptions.OnBackgroundFailure"/>, and dead-lettered when enabled
    /// because the offset has already been committed. When the queue saturates, consumption is
    /// paused on all assigned partitions until capacity frees.
    /// </summary>
    AckAfterEnqueue = 1
}

/// <summary>
/// Describes a handler failure that happened after a Kafka message's offset was already committed
/// by <see cref="KafkaAckMode.AckAfterEnqueue"/>.
/// </summary>
public sealed class KafkaBackgroundFailureContext
{
    internal KafkaBackgroundFailureContext(
        string topic,
        string consumerGroup,
        string subscriberRole,
        int partition,
        long offset,
        string? correlationId,
        Exception exception)
    {
        Topic = topic;
        ConsumerGroup = consumerGroup;
        SubscriberRole = subscriberRole;
        Partition = partition;
        Offset = offset;
        CorrelationId = correlationId;
        Exception = exception;
    }

    /// <summary>The Kafka topic the message came from.</summary>
    public string Topic { get; }

    /// <summary>The Kafka consumer group that received the message.</summary>
    public string ConsumerGroup { get; }

    /// <summary>The logical subscriber role, such as <c>Worker</c> or <c>ResponseIngress</c>.</summary>
    public string SubscriberRole { get; }

    /// <summary>The Kafka partition the message was read from.</summary>
    public int Partition { get; }

    /// <summary>The Kafka offset of the message within its partition.</summary>
    public long Offset { get; }

    /// <summary>The AsyncResponse correlation id, when one was available.</summary>
    public string? CorrelationId { get; }

    /// <summary>The exception thrown by the background handler.</summary>
    public Exception Exception { get; }
}

/// <summary>Per-topic Kafka subscriber behavior.</summary>
public sealed class KafkaSubscriberOptions
{
    /// <summary>
    /// Controls when a Kafka message's offset is committed. Defaults to
    /// <see cref="KafkaAckMode.AckAfterHandlerCompletes"/>.
    /// </summary>
    public KafkaAckMode AckMode { get; set; } = KafkaAckMode.AckAfterHandlerCompletes;

    /// <summary>
    /// Maximum time one poll waits for a message before the subscriber loop re-checks cancellation
    /// and backpressure state. Default: <c>200ms</c>.
    /// </summary>
    public TimeSpan PollTimeout { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// Delay between capacity re-checks while consumption is paused because the
    /// <see cref="KafkaAckMode.AckAfterEnqueue"/> background queue is full. Default: <c>50ms</c>.
    /// </summary>
    public TimeSpan BackpressurePollDelay { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Maximum number of in-process delivery attempts before a failing message is produced to the
    /// dead-letter topic and its offset committed. Kafka offsets cannot NACK a single message, so
    /// retries run in-process with backoff and stall the message's partition while they run (per
    /// classic consumer-group semantics). <c>0</c> means unlimited retries. Attempts are counted
    /// per process delivery: a consumer restart before the offset commit resets the count.
    /// Default: <c>5</c>.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 5;

    /// <summary>Initial delay between in-process handler retry attempts. Default: <c>100ms</c>.</summary>
    public TimeSpan HandlerRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Maximum delay between in-process handler retry attempts. Keep the total retry budget well
    /// below the consumer's <c>max.poll.interval.ms</c> (default 5 minutes) or the broker will
    /// evict the consumer from its group mid-retry. Default: <c>5s</c>.
    /// </summary>
    public TimeSpan HandlerRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Number of background workers used by <see cref="KafkaAckMode.AckAfterEnqueue"/>.
    /// Must be explicitly set to a positive value for early ACK mode.
    /// </summary>
    public int BackgroundWorkerCount { get; set; }

    /// <summary>
    /// Maximum number of messages waiting in the background queue for
    /// <see cref="KafkaAckMode.AckAfterEnqueue"/>. When full, partition consumption is paused
    /// until capacity frees.
    /// </summary>
    public int BackgroundQueueCapacity { get; set; }

    /// <summary>Maximum time to wait for queued/running background handlers while the hosted subscriber stops.</summary>
    public TimeSpan BackgroundDrainTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Optional callback invoked when a background handler fails after the message's offset was
    /// already committed by <see cref="KafkaAckMode.AckAfterEnqueue"/>. Use it to increment
    /// operator-visible metrics or alert on already-committed work.
    /// </summary>
    public Func<KafkaBackgroundFailureContext, ValueTask>? OnBackgroundFailure { get; set; }

    /// <summary>Explicitly opts this subscriber into ACK-after-enqueue behavior.</summary>
    public KafkaSubscriberOptions UseAckAfterEnqueue(
        int backgroundWorkerCount,
        int backgroundQueueCapacity,
        TimeSpan? backgroundDrainTimeout = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundWorkerCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundQueueCapacity);

        if (backgroundDrainTimeout is { } timeout && timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(backgroundDrainTimeout), timeout, "Drain timeout must be positive.");

        AckMode = KafkaAckMode.AckAfterEnqueue;
        BackgroundWorkerCount = backgroundWorkerCount;
        BackgroundQueueCapacity = backgroundQueueCapacity;

        if (backgroundDrainTimeout is not null)
            BackgroundDrainTimeout = backgroundDrainTimeout.Value;

        return this;
    }
}
