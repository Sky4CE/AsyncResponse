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
        services.AddSingleton(provider =>
        {
            // Resolved ack modes declared to the Core startup validator, which vetoes early ACK on
            // the worker queue durable-flow wake-ups ride (see AsyncResponseStartupValidator).
            var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<GooglePubSubAsyncResponseOptions>>().Value;
            return new AsyncResponseTransportMarker("GooglePubSub")
            {
                WorkerSubscriberUsesEarlyAck = options.WorkerSubscriber.AckMode == GooglePubSubAckMode.AckAfterEnqueue,
                WorkerAckModePath = $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(options.WorkerSubscriber)}.{nameof(options.WorkerSubscriber.AckMode)}",
                ResponseSubscriberUsesEarlyAck = options.ResponseSubscriber.AckMode == GooglePubSubAckMode.AckAfterEnqueue,
                ResponseAckModePath = $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(options.ResponseSubscriber)}.{nameof(options.ResponseSubscriber.AckMode)}"
            };
        });

        services.AddHostedService<GooglePubSubWorkerSubscriber>();
        services.AddHostedService<GooglePubSubResponseIngressSubscriber>();

        return builder;
    }
}
