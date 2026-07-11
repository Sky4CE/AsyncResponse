namespace AsyncResponse.Transports.RabbitMQ;

/// <summary>
/// Controls when a RabbitMQ delivery is acknowledged relative to AsyncResponse handling.
/// </summary>
public enum RabbitMqAckMode
{
    /// <summary>
    /// ACK only after the AsyncResponse handler completes successfully; NACK/requeue if the handler
    /// throws. This is the default and preserves broker redelivery for handler failures.
    /// </summary>
    AckAfterHandlerCompletes = 0,

    /// <summary>
    /// ACK immediately after the delivery is accepted into a bounded in-process background queue.
    /// Handler failures are logged and reported through <see cref="RabbitMqSubscriberOptions.OnBackgroundFailure"/>
    /// because RabbitMQ has already been ACKed.
    /// </summary>
    AckAfterEnqueue = 1
}

/// <summary>
/// Describes a handler failure that happened after a RabbitMQ delivery was already ACKed by
/// <see cref="RabbitMqAckMode.AckAfterEnqueue"/>.
/// </summary>
public sealed class RabbitMqBackgroundFailureContext
{
    internal RabbitMqBackgroundFailureContext(
        string queue,
        string subscriberRole,
        string exchange,
        string routingKey,
        ulong deliveryTag,
        Exception exception)
    {
        Queue = queue;
        SubscriberRole = subscriberRole;
        Exchange = exchange;
        RoutingKey = routingKey;
        DeliveryTag = deliveryTag;
        Exception = exception;
    }

    /// <summary>The queue whose background worker was handling the delivery.</summary>
    public string Queue { get; }

    /// <summary>The logical subscriber role, such as <c>Worker</c> or <c>ResponseIngress</c>.</summary>
    public string SubscriberRole { get; }

    /// <summary>The exchange the delivery came from.</summary>
    public string Exchange { get; }

    /// <summary>The routing key the delivery came with.</summary>
    public string RoutingKey { get; }

    /// <summary>The RabbitMQ delivery tag.</summary>
    public ulong DeliveryTag { get; }

    /// <summary>The exception thrown by the background handler.</summary>
    public Exception Exception { get; }
}

/// <summary>
/// Per-queue RabbitMQ subscriber behavior.
/// </summary>
public sealed class RabbitMqSubscriberOptions
{
    /// <summary>
    /// Controls when the RabbitMQ delivery is ACKed. Defaults to
    /// <see cref="RabbitMqAckMode.AckAfterHandlerCompletes"/>.
    /// </summary>
    public RabbitMqAckMode AckMode { get; set; } = RabbitMqAckMode.AckAfterHandlerCompletes;

    /// <summary>
    /// Prefetch count set through <c>basic.qos</c>. Default: <c>16</c>.
    /// </summary>
    public ushort PrefetchCount { get; set; } = 16;

    /// <summary>
    /// Maximum number of times a delivery may be attempted in <see cref="RabbitMqAckMode.AckAfterHandlerCompletes"/>
    /// mode before a failing handler rejects it without requeue (dead-lettering it via
    /// <see cref="RabbitMqAsyncResponseOptions.DeadLetterExchange"/> when configured, otherwise dropping it).
    /// Default: <c>0</c>, meaning unlimited — a failing handler requeues forever, which can hot-loop on a poison
    /// message. Set a positive cap (with a dead-letter exchange) to bound retries. The attempt count is read from
    /// the broker's <c>x-death</c> header and the <c>redelivered</c> flag; because <c>basic.nack</c> requeue does
    /// not increment <c>x-death</c>, the resolved attempt never exceeds 2 on its own, so values above 2 only take
    /// effect when the dead-letter path forms a TTL-retry cycle that re-delivers the message (each dead-letter
    /// hop increments <c>x-death</c>). A value above 2 without such a cycle behaves like 2 and logs a startup
    /// warning. Ignored for <see cref="RabbitMqAckMode.AckAfterEnqueue"/>, which acknowledges before handling
    /// and never redelivers.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; }

    /// <summary>
    /// Number of background workers used by <see cref="RabbitMqAckMode.AckAfterEnqueue"/>.
    /// Must be explicitly set to a positive value for early ACK mode.
    /// </summary>
    public int BackgroundWorkerCount { get; set; }

    /// <summary>
    /// Maximum number of deliveries waiting in the background queue for
    /// <see cref="RabbitMqAckMode.AckAfterEnqueue"/>. When full, the delivery is NACKed with requeue.
    /// </summary>
    public int BackgroundQueueCapacity { get; set; }

    /// <summary>
    /// Maximum time to wait for queued/running background handlers while the hosted subscriber stops.
    /// </summary>
    public TimeSpan BackgroundDrainTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Optional callback invoked when a background handler fails after the delivery was already ACKed.
    /// Use it to publish to a dead-letter path, increment operator-visible metrics, or alert on
    /// already-ACKed work that RabbitMQ cannot redeliver.
    /// </summary>
    public Func<RabbitMqBackgroundFailureContext, ValueTask>? OnBackgroundFailure { get; set; }

    /// <summary>
    /// Explicitly opts this subscriber into ACK-after-enqueue behavior.
    /// </summary>
    public RabbitMqSubscriberOptions UseAckAfterEnqueue(
        int backgroundWorkerCount,
        int backgroundQueueCapacity,
        TimeSpan? backgroundDrainTimeout = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundWorkerCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundQueueCapacity);

        if (backgroundDrainTimeout is { } timeout && timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(backgroundDrainTimeout), timeout, "Drain timeout must be positive.");

        AckMode = RabbitMqAckMode.AckAfterEnqueue;
        BackgroundWorkerCount = backgroundWorkerCount;
        BackgroundQueueCapacity = backgroundQueueCapacity;

        if (backgroundDrainTimeout is not null)
            BackgroundDrainTimeout = backgroundDrainTimeout.Value;

        return this;
    }
}
