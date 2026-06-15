namespace AsyncResponse.GooglePubSub;

/// <summary>
/// Options for Google Pub/Sub AsyncResponse adapters.
/// </summary>
public sealed class GooglePubSubAsyncResponseOptions
{
    /// <summary>Google Cloud project id containing the topics/subscriptions.</summary>
    public string? ProjectId { get; set; }

    /// <summary>Topic id used by <see cref="GooglePubSubWorkerTransport"/> to publish worker jobs.</summary>
    public string? WorkerTopicId { get; set; }

    /// <summary>Subscription id consumed by the worker subscriber hosted service.</summary>
    public string? WorkerSubscriptionId { get; set; }

    /// <summary>
    /// Topic id that async-response messages are published to (by the remote system or worker) and
    /// that <see cref="ResponseSubscriptionId"/> is attached to. The adapter consumes responses
    /// through the subscription; it does not itself publish to this topic.
    /// </summary>
    public string? ResponseTopicId { get; set; }

    /// <summary>Subscription id consumed by the response-ingress hosted service.</summary>
    public string? ResponseSubscriptionId { get; set; }

    /// <summary>
    /// Pub/Sub message attribute that carries the AsyncResponse correlation id. Default:
    /// <c>correlationId</c>.
    /// </summary>
    public string CorrelationIdAttribute { get; set; } = "correlationId";

    /// <summary>How long hosted subscribers/publishers are allowed to shut down gracefully.</summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(15);
}
