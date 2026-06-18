using Google.Cloud.PubSub.V1;

namespace AsyncResponse.Transports.GooglePubSub;

internal interface IGooglePubSubPublisherClient
{
    Task<string> PublishAsync(PubsubMessage message);
    Task ShutdownAsync(TimeSpan timeout);
}

internal sealed class GooglePubSubPublisherClientAdapter(PublisherClient inner) : IGooglePubSubPublisherClient
{
    public Task<string> PublishAsync(PubsubMessage message)
        => inner.PublishAsync(message);

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
    public Task StartAsync(Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>> handler)
        => inner.StartAsync(handler);

    public Task StopAsync(SubscriberClient.ShutdownOptions options, CancellationToken cancellationToken)
        => inner.StopAsync(options, cancellationToken);
}
