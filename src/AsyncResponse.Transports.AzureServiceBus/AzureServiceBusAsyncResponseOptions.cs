namespace AsyncResponse.Transports.AzureServiceBus;

/// <summary>Options for the Azure Service Bus AsyncResponse transport.</summary>
public sealed class AzureServiceBusAsyncResponseOptions
{
    /// <summary>The transport name reported to reply targets and startup validation.</summary>
    public const string TransportName = "AzureServiceBus";

    /// <summary>
    /// Service Bus namespace connection string. Required unless an <see cref="Azure.Messaging.ServiceBus.ServiceBusClient"/>
    /// singleton is already registered in the application's service container.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>Service Bus queue used by <see cref="AzureServiceBusWorkerTransport"/> to publish worker jobs.</summary>
    public string WorkerQueue { get; set; } = "asyncresponse-worker";

    /// <summary>Worker queue handling options.</summary>
    public AzureServiceBusSubscriberOptions WorkerSubscriber { get; } = new();

    /// <summary>Service Bus queue consumed by the hosted response-ingress subscriber.</summary>
    public string ResponseQueue { get; set; } = "asyncresponse-response";

    /// <summary>Response queue handling options.</summary>
    public AzureServiceBusSubscriberOptions ResponseSubscriber { get; } = new();

    /// <summary>The logical reply target name used by <c>WithReplyTarget()</c>. Default: <c>default</c>.</summary>
    public string DefaultReplyTargetName { get; set; } = "default";

    /// <summary>
    /// Named reply targets exposed to Core through <see cref="IAsyncResponseReplyTargetProvider"/>.
    /// When empty, <see cref="ResponseQueue"/> becomes the default reply target.
    /// </summary>
    public Dictionary<string, AzureServiceBusReplyTargetOptions> ReplyTargets { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Application property used to carry the AsyncResponse correlation id. The transport also sets the
    /// Service Bus system <c>CorrelationId</c> property for interoperability.
    /// </summary>
    public string CorrelationIdProperty { get; set; } = "correlationId";

    /// <summary>
    /// JSON paths inspected when a response message does not carry the correlation id in the Service
    /// Bus system property or <see cref="CorrelationIdProperty"/>. Paths are case-insensitive and
    /// support nested JSON strings.
    /// </summary>
    public string[] CorrelationIdJsonPaths { get; set; } =
    [
        "CorrelationId",
        "CustomParameters",
        "CustomParameters.CorrelationId",
        "PubSubParams.CustomParameters",
        "PubSubParams.CustomParameters.CorrelationId",
        "DagJsonParameters.CorrelationId"
    ];

    /// <summary>Maximum messages requested from Service Bus in one receive call.</summary>
    public int MaxMessagesPerReceive { get; set; } = 16;

    /// <summary>Maximum time a subscriber receive call waits for messages before polling again.</summary>
    public TimeSpan ReceiveWaitTime { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Maximum attempts for Service Bus send operations. Set to 1 to disable transport-level retries.</summary>
    public int PublishMaxAttempts { get; set; } = 3;

    /// <summary>Initial delay before retrying a failed send operation.</summary>
    public TimeSpan PublishRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Maximum delay between send retry attempts.</summary>
    public TimeSpan PublishRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Initial delay after a subscriber loop failure.</summary>
    public TimeSpan SubscriberRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>Maximum delay after repeated subscriber loop failures.</summary>
    public TimeSpan SubscriberRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How long hosted subscribers and cached senders are allowed to close gracefully.</summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// The hosting shutdown budget that must contain Service Bus receiver/sender shutdown plus
    /// <see cref="AzureServiceBusSubscriberOptions.BackgroundDrainTimeout"/> when a subscriber uses
    /// <see cref="AzureServiceBusAckMode.AckAfterReceive"/>.
    /// </summary>
    public TimeSpan? HostShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Adds or replaces a named Azure Service Bus reply target.</summary>
    public AzureServiceBusAsyncResponseOptions AddReplyTarget(string name, string queue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);

        ReplyTargets[name] = new AzureServiceBusReplyTargetOptions { Queue = queue };
        return this;
    }
}

/// <summary>Options for one named Azure Service Bus async-response reply target.</summary>
public sealed class AzureServiceBusReplyTargetOptions
{
    /// <summary>Queue remote systems should publish responses to.</summary>
    public string? Queue { get; set; }

    /// <summary>Additional values copied to the transport-neutral reply target.</summary>
    public Dictionary<string, string> Properties { get; } = new(StringComparer.Ordinal);
}

/// <summary>Controls when a Service Bus message is acknowledged relative to handling.</summary>
public enum AzureServiceBusAckMode
{
    /// <summary>
    /// Complete the message only after the AsyncResponse handler completes successfully. Handler
    /// failures abandon the message until <see cref="AzureServiceBusSubscriberOptions.MaxDeliveryAttempts"/>
    /// is reached, then move it to the Service Bus dead-letter subqueue.
    /// </summary>
    AckAfterHandlerCompletes = 0,

    /// <summary>
    /// Complete the message immediately after it is accepted into a bounded in-process background
    /// queue. Handler failures are logged and reported through <see cref="AzureServiceBusSubscriberOptions.OnBackgroundFailure"/>
    /// because the original Service Bus lock has already been completed.
    /// </summary>
    AckAfterReceive = 1
}

/// <summary>Describes a handler failure that happened after a Service Bus message was already completed.</summary>
public sealed class AzureServiceBusBackgroundFailureContext
{
    internal AzureServiceBusBackgroundFailureContext(
        string queue,
        string subscriberRole,
        long sequenceNumber,
        string messageId,
        string? correlationId,
        Exception exception)
    {
        Queue = queue;
        SubscriberRole = subscriberRole;
        SequenceNumber = sequenceNumber;
        MessageId = messageId;
        CorrelationId = correlationId;
        Exception = exception;
    }

    /// <summary>The Service Bus queue the message came from.</summary>
    public string Queue { get; }

    /// <summary>The logical subscriber role, such as <c>Worker</c> or <c>ResponseIngress</c>.</summary>
    public string SubscriberRole { get; }

    /// <summary>The Service Bus sequence number for the already-completed message.</summary>
    public long SequenceNumber { get; }

    /// <summary>The Service Bus message id.</summary>
    public string MessageId { get; }

    /// <summary>The AsyncResponse correlation id, when one was available.</summary>
    public string? CorrelationId { get; }

    /// <summary>The exception thrown by the background handler.</summary>
    public Exception Exception { get; }
}

/// <summary>Per-queue Azure Service Bus subscriber behavior.</summary>
public sealed class AzureServiceBusSubscriberOptions
{
    /// <summary>Controls when a message is completed. Defaults to <see cref="AzureServiceBusAckMode.AckAfterHandlerCompletes"/>.</summary>
    public AzureServiceBusAckMode AckMode { get; set; } = AzureServiceBusAckMode.AckAfterHandlerCompletes;

    /// <summary>
    /// Maximum delivery attempts before a failing message is dead-lettered. <c>0</c> means unlimited
    /// application-level retries and leaves any broker-level MaxDeliveryCount policy to Service Bus.
    /// Default: <c>5</c>.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 5;

    /// <summary>Number of messages the receiver prefetches locally. Default: <c>0</c>.</summary>
    public int PrefetchCount { get; set; }

    /// <summary>Number of background workers used by <see cref="AzureServiceBusAckMode.AckAfterReceive"/>.</summary>
    public int BackgroundWorkerCount { get; set; }

    /// <summary>Maximum number of completed messages waiting in the background queue.</summary>
    public int BackgroundQueueCapacity { get; set; }

    /// <summary>Maximum time to wait for queued/running background handlers while stopping.</summary>
    public TimeSpan BackgroundDrainTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Optional callback invoked when a background handler fails after the message was already completed.</summary>
    public Func<AzureServiceBusBackgroundFailureContext, ValueTask>? OnBackgroundFailure { get; set; }

    /// <summary>Explicitly opts this subscriber into complete-after-receive behavior.</summary>
    public AzureServiceBusSubscriberOptions UseAckAfterReceive(
        int backgroundWorkerCount,
        int backgroundQueueCapacity,
        TimeSpan? backgroundDrainTimeout = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundWorkerCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundQueueCapacity);
        if (backgroundDrainTimeout is { } timeout && timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(backgroundDrainTimeout), timeout, "Drain timeout must be positive.");

        AckMode = AzureServiceBusAckMode.AckAfterReceive;
        BackgroundWorkerCount = backgroundWorkerCount;
        BackgroundQueueCapacity = backgroundQueueCapacity;
        if (backgroundDrainTimeout is not null)
            BackgroundDrainTimeout = backgroundDrainTimeout.Value;
        return this;
    }
}
