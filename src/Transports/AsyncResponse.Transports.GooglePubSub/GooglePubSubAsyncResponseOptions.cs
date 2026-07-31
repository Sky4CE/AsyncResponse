namespace AsyncResponse.Transports.GooglePubSub;

/// <summary>
/// Options for the Google Pub/Sub AsyncResponse transport.
/// </summary>
public sealed class GooglePubSubAsyncResponseOptions
{
    public const string TransportName = "google-pubsub";

    /// <summary>Google Cloud project id containing the topics/subscriptions.</summary>
    public string? ProjectId { get; set; }

    /// <summary>Topic id used by <see cref="GooglePubSubWorkerTransport"/> to publish worker jobs.</summary>
    public string? WorkerTopicId { get; set; }

    /// <summary>Subscription id consumed by the worker subscriber hosted service.</summary>
    public string? WorkerSubscriptionId { get; set; }

    /// <summary>
    /// Worker subscription handling options. The default acknowledgement mode waits until the worker
    /// handler completes before ACKing; call <see cref="GooglePubSubSubscriberOptions.UseAckAfterEnqueue"/>
    /// explicitly to opt into early ACK after bounded background enqueue.
    /// </summary>
    public GooglePubSubSubscriberOptions WorkerSubscriber { get; } = new();

    /// <summary>
    /// Topic id that async-response messages are published to (by the remote system or worker) and
    /// that <see cref="ResponseSubscriptionId"/> is attached to. The transport consumes responses
    /// through the subscription; it does not itself publish to this topic.
    /// </summary>
    public string? ResponseTopicId { get; set; }

    /// <summary>Subscription id consumed by the response-ingress hosted service.</summary>
    public string? ResponseSubscriptionId { get; set; }

    /// <summary>
    /// Response subscription handling options. Keep the default unless response ingress is known to be
    /// durable enough to tolerate ACKing before processing completes.
    /// </summary>
    public GooglePubSubSubscriberOptions ResponseSubscriber { get; } = new();

    /// <summary>The logical reply target name used by <c>WithReplyTarget()</c>. Default: <c>default</c>.</summary>
    public string DefaultReplyTargetName { get; set; } = "default";

    /// <summary>
    /// Named reply targets exposed to Core through <see cref="IAsyncResponseReplyTargetProvider"/>.
    /// When empty, <see cref="ResponseTopicId"/> becomes the default reply target.
    /// </summary>
    public Dictionary<string, GooglePubSubReplyTargetOptions> ReplyTargets { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Pub/Sub message attribute that carries the AsyncResponse correlation id. Default:
    /// <c>correlationId</c>.
    /// </summary>
    public string CorrelationIdAttribute { get; set; } = "correlationId";

    /// <summary>
    /// JSON paths inspected when a response message does not carry the correlation id as an
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

    /// <summary>Initial delay before restarting a failed hosted subscriber.</summary>
    public TimeSpan SubscriberRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Maximum delay between restarts of a repeatedly failing hosted subscriber.</summary>
    public TimeSpan SubscriberRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Bounds the subscriber-client stop and publisher shutdown while hosted services stop. These
    /// complete in milliseconds when healthy; when they do not, the client is abandoned anyway, so
    /// keep this short — it counts against the host's shutdown budget. Default: <c>5s</c>.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The hosting shutdown budget that must contain Pub/Sub client shutdown plus
    /// <see cref="GooglePubSubSubscriberOptions.BackgroundDrainTimeout"/> when a subscriber uses
    /// <see cref="GooglePubSubAckMode.AckAfterEnqueue"/>. Defaults to the Generic Host default of
    /// 30 seconds. Set to <c>null</c> only when this budget is validated externally.
    /// </summary>
    public TimeSpan? HostShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Adds or replaces a named Google Pub/Sub reply target.</summary>
    public GooglePubSubAsyncResponseOptions AddReplyTarget(string name, string projectId, string topicId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(projectId);
        ArgumentException.ThrowIfNullOrWhiteSpace(topicId);

        ReplyTargets[name] = new GooglePubSubReplyTargetOptions
        {
            ProjectId = projectId,
            TopicId = topicId
        };

        return this;
    }
}

/// <summary>Options for one named Google Pub/Sub async-response reply target.</summary>
public sealed class GooglePubSubReplyTargetOptions
{
    /// <summary>Google Cloud project id containing the response topic. Defaults to the transport's <see cref="GooglePubSubAsyncResponseOptions.ProjectId"/>.</summary>
    public string? ProjectId { get; set; }

    /// <summary>Topic id remote systems should publish responses to.</summary>
    public string? TopicId { get; set; }

    /// <summary>Additional values copied to the transport-neutral reply target.</summary>
    public Dictionary<string, string> Properties { get; } = new(StringComparer.Ordinal);
}
