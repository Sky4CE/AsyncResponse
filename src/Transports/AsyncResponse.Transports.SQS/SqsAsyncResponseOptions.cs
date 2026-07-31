namespace AsyncResponse.Transports.SQS;

/// <summary>Options for the AWS SQS AsyncResponse transport.</summary>
public sealed class SqsAsyncResponseOptions
{
    /// <summary>The transport name reported to reply targets and startup validation.</summary>
    public const string TransportName = "AmazonSQS";

    /// <summary>
    /// Custom SQS endpoint, such as a LocalStack edge URL (<c>http://localhost:4566</c>). Leave
    /// <c>null</c> to use the standard AWS endpoint resolution for <see cref="Region"/>. Ignored when
    /// an <c>Amazon.SQS.IAmazonSQS</c> singleton is already registered in the service container.
    /// </summary>
    public string? ServiceUrl { get; set; }

    /// <summary>
    /// AWS region system name (<c>us-east-1</c>, …). Leave <c>null</c> to use the AWS SDK default
    /// region resolution (environment, profile, instance metadata).
    /// </summary>
    public string? Region { get; set; }

    /// <summary>
    /// Explicit AWS access key. Leave <c>null</c> (with <see cref="SecretKey"/>) to use the AWS SDK
    /// default credential chain. Both must be set together; useful for emulators such as LocalStack.
    /// </summary>
    public string? AccessKey { get; set; }

    /// <summary>Explicit AWS secret key paired with <see cref="AccessKey"/>.</summary>
    public string? SecretKey { get; set; }

    /// <summary>
    /// SQS queue used by <see cref="SqsWorkerTransport"/> to publish worker jobs. Accepts a queue
    /// name (resolved once via <c>GetQueueUrl</c>) or a full queue URL. A name or URL ending in
    /// <c>.fifo</c> opts the worker path into FIFO publishing: the correlation id becomes the
    /// <c>MessageGroupId</c> so one flow's jobs stay ordered.
    /// </summary>
    public string WorkerQueue { get; set; } = "asyncresponse-worker";

    /// <summary>Worker queue handling options.</summary>
    public SqsSubscriberOptions WorkerSubscriber { get; } = new();

    /// <summary>
    /// SQS queue consumed by the hosted response-ingress subscriber. Accepts a queue name or URL and
    /// must be distinct from <see cref="WorkerQueue"/>.
    /// </summary>
    public string ResponseQueue { get; set; } = "asyncresponse-response";

    /// <summary>Response queue handling options.</summary>
    public SqsSubscriberOptions ResponseSubscriber { get; } = new();

    /// <summary>The logical reply target name used by <c>WithReplyTarget()</c>. Default: <c>default</c>.</summary>
    public string DefaultReplyTargetName { get; set; } = "default";

    /// <summary>
    /// Named reply targets exposed to Core through <see cref="IAsyncResponseReplyTargetProvider"/>.
    /// When empty, <see cref="ResponseQueue"/> becomes the default reply target.
    /// </summary>
    public Dictionary<string, SqsReplyTargetOptions> ReplyTargets { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// SQS message attribute that carries the AsyncResponse correlation id. Default:
    /// <c>correlationId</c>.
    /// </summary>
    public string CorrelationIdAttribute { get; set; } = "correlationId";

    /// <summary>
    /// JSON paths inspected when a response message does not carry the correlation id as a message
    /// attribute. Paths are case-insensitive and support nested JSON strings, such as
    /// <c>CustomParameters</c> containing serialized JSON.
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

    /// <summary>
    /// Maximum messages requested from SQS in one <c>ReceiveMessage</c> call. SQS allows 1–10;
    /// default: <c>10</c>.
    /// </summary>
    public int MaxMessagesPerReceive { get; set; } = 10;

    /// <summary>
    /// Long-poll wait time per <c>ReceiveMessage</c> call. SQS allows 0–20 seconds; the default of
    /// 20 seconds minimizes empty-receive billing and wake-up latency.
    /// </summary>
    public TimeSpan ReceiveWaitTime { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Provision <see cref="WorkerQueue"/> and <see cref="ResponseQueue"/> (plus one dead-letter
    /// queue each, wired through a native redrive policy with <see cref="MaxReceiveCount"/>) on
    /// startup. Only queues configured by name are provisioned; queue URLs are assumed to exist.
    /// Default: <c>false</c> — production queues are usually owned by infrastructure code.
    /// </summary>
    public bool CreateQueues { get; set; }

    /// <summary>
    /// Suffix appended to a provisioned queue's name to derive its dead-letter queue (before the
    /// <c>.fifo</c> suffix for FIFO queues). Default: <c>-dlq</c>.
    /// </summary>
    public string DeadLetterQueueSuffix { get; set; } = "-dlq";

    /// <summary>
    /// <c>maxReceiveCount</c> written into the redrive policy of provisioned queues: how many
    /// receives (tracked natively by SQS via <c>ApproximateReceiveCount</c>) a message survives
    /// before SQS moves it to the dead-letter queue. Default: <c>5</c>.
    /// </summary>
    public int MaxReceiveCount { get; set; } = 5;

    /// <summary>
    /// <c>MessageGroupId</c> used when publishing to a FIFO worker queue and the job carries no
    /// correlation id. Default: <c>asyncresponse</c>.
    /// </summary>
    public string FifoMessageGroupIdFallback { get; set; } = "asyncresponse";

    /// <summary>Maximum attempts for SQS send operations. Set to 1 to disable transport-level retries.</summary>
    public int PublishMaxAttempts { get; set; } = 3;

    /// <summary>Initial delay before retrying a failed send operation.</summary>
    public TimeSpan PublishRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Maximum delay between send retry attempts.</summary>
    public TimeSpan PublishRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Initial delay after a subscriber loop failure.</summary>
    public TimeSpan SubscriberRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>Maximum delay after repeated subscriber loop failures.</summary>
    public TimeSpan SubscriberRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The hosting shutdown budget that must contain
    /// <see cref="SqsSubscriberOptions.BackgroundDrainTimeout"/> when a subscriber uses
    /// <see cref="SqsAckMode.AckAfterEnqueue"/>. Defaults to the Generic Host default of 30 seconds.
    /// Set to <c>null</c> only when this budget is validated externally.
    /// </summary>
    public TimeSpan? HostShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Adds or replaces a named SQS reply target.</summary>
    public SqsAsyncResponseOptions AddReplyTarget(string name, string queue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);

        ReplyTargets[name] = new SqsReplyTargetOptions { Queue = queue };
        return this;
    }
}

/// <summary>Options for one named SQS async-response reply target.</summary>
public sealed class SqsReplyTargetOptions
{
    /// <summary>Queue (name or URL) remote systems should publish responses to.</summary>
    public string? Queue { get; set; }

    /// <summary>Additional values copied to the transport-neutral reply target.</summary>
    public Dictionary<string, string> Properties { get; } = new(StringComparer.Ordinal);
}
