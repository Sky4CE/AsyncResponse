using AsyncResponse;
using AsyncResponse.Transports.AzureServiceBus;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>DI registration for the Azure Service Bus AsyncResponse transport.</summary>
public static class AzureServiceBusAsyncResponseServiceCollectionExtensions
{
    /// <summary>
    /// Registers Azure Service Bus as the worker transport and response ingress in one call:
    /// <list type="bullet">
    /// <item><description>worker jobs are published to <see cref="AzureServiceBusAsyncResponseOptions.WorkerQueue"/>;</description></item>
    /// <item><description>a hosted subscriber reads that worker queue and executes worker jobs;</description></item>
    /// <item><description>a hosted subscriber reads <see cref="AzureServiceBusAsyncResponseOptions.ResponseQueue"/> and feeds responses into <see cref="IAsyncResponseIngress"/>.</description></item>
    /// </list>
    /// Azure Service Bus is a transport, not a recovery store: pair it with a channel
    /// (<c>.WithInMemoryChannel()</c> for simple apps, or a durable channel such as
    /// <c>.WithRedisChannel()</c>, <c>.WithNatsChannel()</c>, or <c>.WithPostgreSqlChannel()</c> when
    /// late responses must survive redeploys).
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithAzureServiceBusTransport(
        this AsyncResponseRegistrationBuilder builder,
        Action<AzureServiceBusAsyncResponseOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var services = builder.Services;
        services.AddOptions();
        services.Configure(configure);

        services.TryAddSingleton(AzureServiceBusClientResolver.Create);
        services.TryAddSingleton(provider => new AzureServiceBusWorkerTransport(
            provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AzureServiceBusAsyncResponseOptions>>(),
            provider.GetRequiredService<IAzureServiceBusClient>()));
        services.Replace(ServiceDescriptor.Singleton<IWorkerTransport>(provider =>
            provider.GetRequiredService<AzureServiceBusWorkerTransport>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseReplyTargetProvider, AzureServiceBusReplyTargetProvider>());
        services.AddSingleton(new AsyncResponseTransportMarker(AzureServiceBusAsyncResponseOptions.TransportName));

        services.AddHostedService<AzureServiceBusWorkerSubscriber>();
        services.AddHostedService<AzureServiceBusResponseIngressSubscriber>();

        return builder;
    }
}
