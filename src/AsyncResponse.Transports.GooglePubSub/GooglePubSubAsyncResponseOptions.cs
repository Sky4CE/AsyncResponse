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
    /// Topic id that async-response messages are published to (by the remote system or worker) and
    /// that <see cref="ResponseSubscriptionId"/> is attached to. The transport consumes responses
    /// through the subscription; it does not itself publish to this topic.
    /// </summary>
    public string? ResponseTopicId { get; set; }

    /// <summary>Subscription id consumed by the response-ingress hosted service.</summary>
    public string? ResponseSubscriptionId { get; set; }

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

    /// <summary>How long hosted subscribers/publishers are allowed to shut down gracefully.</summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(15);

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
