using AsyncResponse;
using AsyncResponse.Transports.GooglePubSub;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// DI registration for the Google Pub/Sub AsyncResponse transport.
/// </summary>
public static class GooglePubSubAsyncResponseServiceCollectionExtensions
{
    /// <summary>
    /// Registers Google Pub/Sub as the worker transport and response ingress in one call:
    /// <list type="bullet">
    /// <item><description>worker jobs are published to <see cref="GooglePubSubAsyncResponseOptions.WorkerTopicId"/>;</description></item>
    /// <item><description>a hosted subscriber consumes <see cref="GooglePubSubAsyncResponseOptions.WorkerSubscriptionId"/> and executes the jobs;</description></item>
    /// <item><description>a hosted subscriber consumes <see cref="GooglePubSubAsyncResponseOptions.ResponseSubscriptionId"/> and feeds responses into <see cref="IAsyncResponseIngress"/>.</description></item>
    /// </list>
    /// Pub/Sub is a transport, not a recovery store: pair it with a channel
    /// (<c>.WithInMemoryChannel()</c> for simple apps, or <c>.WithRedisChannel()</c> when late
    /// responses must survive redeploys), which provides the waiter side and recovery state.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithGooglePubSubTransport(
        this AsyncResponseRegistrationBuilder builder,
        Action<GooglePubSubAsyncResponseOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var services = builder.Services;
        services.AddOptions();
        services.Configure(configure);

        services.TryAddSingleton<GooglePubSubWorkerTransport>();
        services.Replace(ServiceDescriptor.Singleton<IWorkerTransport>(provider =>
            provider.GetRequiredService<GooglePubSubWorkerTransport>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseReplyTargetProvider, GooglePubSubReplyTargetProvider>());
        services.AddSingleton(new AsyncResponseTransportMarker("GooglePubSub"));

        services.AddHostedService<GooglePubSubWorkerSubscriber>();
        services.AddHostedService<GooglePubSubResponseIngressSubscriber>();

        return builder;
    }
}
