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
    /// Maximum time to wait for queued/running background handlers while the hosted subscriber stops
    /// (<see cref="GooglePubSubAckMode.AckAfterEnqueue"/>; a quarter of it is reserved for surfacing
    /// whatever is still queued once the rest lapses). In
    /// <see cref="GooglePubSubAckMode.AckAfterHandlerCompletes"/> mode it bounds the wait for the
    /// handlers still running before the subscriber client is stopped, so their Acks land first —
    /// shortened to what <see cref="GooglePubSubAsyncResponseOptions.HostShutdownTimeout"/> still
    /// leaves after <see cref="GooglePubSubAsyncResponseOptions.ShutdownTimeout"/>, never validated
    /// (a stop that cannot wait only costs a redelivery). Default: <c>20s</c>.
    /// </summary>
    public TimeSpan BackgroundDrainTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Optional callback invoked when a background handler fails after the message was already ACKed.
    /// Use it to publish to a dead-letter path, increment operator-visible metrics, or alert on
    /// already-ACKed work that Pub/Sub cannot redeliver.
    /// </summary>
    public Func<GooglePubSubBackgroundFailureContext, ValueTask>? OnBackgroundFailure { get; set; }

    /// <summary>
    /// Longest the Pub/Sub client keeps extending one message's ack deadline while it is held in
    /// this process (the SDK's <c>SubscriberClient.Settings.MaxTotalAckExtension</c>). Default:
    /// <c>60 minutes</c>, the SDK default. Must be at least one minute and at most the .NET timer
    /// ceiling (~49.7 days); keep it under the subscription's message retention.
    /// <para>
    /// This is the transport's <em>in-flight ceiling</em>. The clock starts when the client receives
    /// the message, not when its handler starts. Once it lapses the client stops extending the
    /// deadline, the current lease (up to 60 seconds) runs out, and Pub/Sub redelivers the
    /// <em>same</em> message — to this or another subscriber, counting a delivery attempt against
    /// the subscription's <c>DeadLetterPolicy</c> — while the first handler is still running. The
    /// first handler is not cancelled, and its late ACK is best-effort: it cannot recall a copy
    /// Pub/Sub has already handed out, so the work runs twice.
    /// A handler that can legitimately run longer than this must raise it; the worker
    /// subscriber's value is what <see cref="GooglePubSubWorkerTransport"/> advertises through
    /// <see cref="IWorkerTransportInFlightLimit"/>, so durable-flow timers that wait in process are
    /// planned inside it.
    /// </para>
    /// <para>
    /// The floor exists because the first lease already lasts the client's 60-second ack deadline:
    /// a smaller value cannot make Pub/Sub redeliver sooner, it would only shrink the ceiling flows
    /// plan against.
    /// </para>
    /// </summary>
    public TimeSpan MaxTotalAckExtension { get; set; } = TimeSpan.FromMinutes(60);

    /// <summary>
    /// Number of streaming-pull connections (SDK <c>SubscriberServiceApiClient</c>s) the subscriber
    /// client opens. Must be between 1 and 256 (the SDK's range). Default: <c>1</c>.
    /// <para>
    /// The SDK's own default is the machine's CPU count, and it applies the flow-control limits to
    /// <em>each</em> connection's fetch independently while limiting concurrent handlers once for
    /// the whole client: with N connections the process leases up to N ×
    /// <see cref="MaxOutstandingMessages"/> messages but runs at most
    /// <see cref="MaxOutstandingMessages"/> handlers. The surplus sits leased and idle — withheld
    /// from other subscriber processes and spending its <see cref="MaxTotalAckExtension"/> budget
    /// before a handler ever starts — which is the wrong trade for job-style handlers that run for
    /// seconds to hours. One connection keeps the leased set equal to the running set; raise it only
    /// when a single stream's fetch throughput (not handler time) is the bottleneck.
    /// </para>
    /// </summary>
    public int ClientCount { get; set; } = 1;

    /// <summary>
    /// Flow-control ceiling on messages the client holds un-ACKed at once (SDK
    /// <c>FlowControlSettings.MaxOutstandingElementCount</c>). In
    /// <see cref="GooglePubSubAckMode.AckAfterHandlerCompletes"/> this is the maximum number of
    /// handlers running concurrently in this process — including durable flows parked in process on
    /// a timer. Must be positive. Default: <c>1000</c>, the SDK default.
    /// Ignored in <see cref="GooglePubSubAckMode.AckAfterEnqueue"/>, where the streaming pull is
    /// bounded to <see cref="BackgroundQueueCapacity"/> instead.
    /// </summary>
    public int MaxOutstandingMessages { get; set; } = 1000;

    /// <summary>
    /// Flow-control ceiling on the total size, in bytes, of the messages the client holds un-ACKed
    /// at once (SDK <c>FlowControlSettings.MaxOutstandingByteCount</c>). Must be positive; a single
    /// message larger than the ceiling is still delivered, on its own. Default: <c>100,000,000</c>
    /// (100 MB), the SDK default. Ignored in <see cref="GooglePubSubAckMode.AckAfterEnqueue"/>.
    /// </summary>
    public long MaxOutstandingBytes { get; set; } = 100_000_000;

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
