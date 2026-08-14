using Google.Cloud.PubSub.V1;

namespace AsyncResponse.Transports.GooglePubSub;

internal interface IGooglePubSubPublisherClient
{
    Task<string> PublishAsync(PubsubMessage message, CancellationToken cancellationToken);
    Task ShutdownAsync(TimeSpan timeout);
}

internal sealed class GooglePubSubPublisherClientAdapter(PublisherClient inner) : IGooglePubSubPublisherClient
{
    /// <summary>Publishes the supplied message.</summary>
    public Task<string> PublishAsync(PubsubMessage message, CancellationToken cancellationToken)
    {
        // PublisherClient exposes no cancellation: once handed over, the message sits in the
        // client's local batch queue and WILL be flushed (DisposeAsync's ShutdownAsync completes
        // only "when all queued messages have been published"). Checking before the hand-off keeps
        // a publish cancelled at shutdown from being enqueued-then-delivered while its caller was
        // told nothing was sent; a token firing mid-flight still only abandons the wait — that
        // residual at-least-once window is inherent to the SDK.
        cancellationToken.ThrowIfCancellationRequested();
        return inner.PublishAsync(message).WaitAsync(cancellationToken);
    }

    /// <summary>Runs the ShutdownAsync operation.</summary>
    public Task ShutdownAsync(TimeSpan timeout)
        => inner.ShutdownAsync(timeout);
}

internal interface IGooglePubSubSubscriberClient
{
    Task StartAsync(Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>> handler);
    Task StopAsync(SubscriberClient.ShutdownOptions options, CancellationToken cancellationToken);
}

internal sealed class GooglePubSubSubscriberClientAdapter(SubscriberClient inner) : IGooglePubSubSubscriberClient
{
    /// <summary>Starts this service.</summary>
    public Task StartAsync(Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>> handler)
        => inner.StartAsync(handler);

    /// <summary>Stops this service.</summary>
    public Task StopAsync(SubscriberClient.ShutdownOptions options, CancellationToken cancellationToken)
        => inner.StopAsync(options, cancellationToken);
}
