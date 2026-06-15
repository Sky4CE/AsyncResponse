using AsyncResponse;
using AsyncResponse.GooglePubSub;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// DI registrations for Google Pub/Sub AsyncResponse adapters.
/// </summary>
public static class GooglePubSubAsyncResponseServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="GooglePubSubWorkerTransport"/> as the <see cref="IWorkerTransport"/>.
    /// Worker jobs are published to the configured Pub/Sub topic.
    /// </summary>
    public static IServiceCollection AddGooglePubSubWorkerTransport(
        this IServiceCollection services,
        Action<GooglePubSubAsyncResponseOptions>? configure = null)
    {
        ConfigureGooglePubSub(services, configure);
        services.AddAsyncResponseBuilder();
        services.TryAddSingleton<GooglePubSubWorkerTransport>();
        services.Replace(ServiceDescriptor.Singleton<IWorkerTransport>(provider =>
            provider.GetRequiredService<GooglePubSubWorkerTransport>()));
        return services;
    }

    /// <summary>
    /// Registers a hosted subscriber that consumes Pub/Sub worker-job messages and feeds them
    /// into <see cref="IAsyncResponseIngress.HandleWorkerMessageAsync"/>.
    /// </summary>
    public static IServiceCollection AddGooglePubSubWorkerSubscriber(
        this IServiceCollection services,
        Action<GooglePubSubAsyncResponseOptions>? configure = null)
    {
        ConfigureGooglePubSub(services, configure);
        services.AddAsyncResponseBuilder();
        services.AddHostedService<GooglePubSubWorkerSubscriber>();
        return services;
    }

    /// <summary>
    /// Registers a hosted subscriber that consumes Pub/Sub response messages and feeds them into
    /// <see cref="IAsyncResponseIngress.HandleResponseMessageAsync"/>.
    /// </summary>
    public static IServiceCollection AddGooglePubSubResponseIngress(
        this IServiceCollection services,
        Action<GooglePubSubAsyncResponseOptions>? configure = null)
    {
        ConfigureGooglePubSub(services, configure);
        services.AddAsyncResponseBuilder();
        services.AddHostedService<GooglePubSubResponseIngressSubscriber>();
        return services;
    }

    /// <summary>
    /// Convenience registration for all Google Pub/Sub adapters: Core's process-local response
    /// channel/recovery store, worker publisher, worker subscriber, and response-ingress
    /// subscriber. Add <c>AddRedisAsyncResponse()</c> when you need a durable response channel
    /// and recovery store.
    /// </summary>
    public static IServiceCollection AddGooglePubSubAsyncResponse(
        this IServiceCollection services,
        Action<GooglePubSubAsyncResponseOptions>? configure = null)
    {
        ConfigureGooglePubSub(services, configure);
        services.AddAsyncResponse();
        services.AddGooglePubSubWorkerTransport();
        services.AddGooglePubSubWorkerSubscriber();
        services.AddGooglePubSubResponseIngress();
        return services;
    }

    private static void ConfigureGooglePubSub(
        IServiceCollection services,
        Action<GooglePubSubAsyncResponseOptions>? configure)
    {
        services.AddOptions();
        if (configure is not null)
        {
            services.Configure(configure);
        }
    }
}
