using AsyncResponse;
using AsyncResponse.Transports.RabbitMQ;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// DI registration for the RabbitMQ AsyncResponse transport.
/// </summary>
public static class RabbitMqAsyncResponseServiceCollectionExtensions
{
    /// <summary>
    /// Registers RabbitMQ as the worker transport and response ingress in one call:
    /// <list type="bullet">
    /// <item><description>worker jobs are published to <see cref="RabbitMqAsyncResponseOptions.WorkerExchange"/> with <see cref="RabbitMqAsyncResponseOptions.WorkerRoutingKey"/>;</description></item>
    /// <item><description>a hosted consumer reads <see cref="RabbitMqAsyncResponseOptions.WorkerQueue"/> and executes worker jobs;</description></item>
    /// <item><description>a hosted consumer reads <see cref="RabbitMqAsyncResponseOptions.ResponseQueue"/> and feeds responses into <see cref="IAsyncResponseIngress"/>.</description></item>
    /// </list>
    /// RabbitMQ is a transport, not a recovery store: pair it with a channel
    /// (<c>.WithInMemoryChannel()</c> for simple apps, or <c>.WithRedisChannel()</c> when late
    /// responses must survive redeploys), which provides the waiter side and recovery state.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithRabbitMqTransport(
        this AsyncResponseRegistrationBuilder builder,
        Action<RabbitMqAsyncResponseOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var services = builder.Services;
        services.AddOptions();
        services.Configure(configure);

        services.TryAddSingleton<RabbitMqWorkerTransport>();
        services.Replace(ServiceDescriptor.Singleton<IWorkerTransport>(provider =>
            provider.GetRequiredService<RabbitMqWorkerTransport>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseReplyTargetProvider, RabbitMqReplyTargetProvider>());
        services.AddSingleton(provider =>
        {
            // Resolved ack modes declared to the Core startup validator, which vetoes early ACK on
            // the worker queue durable-flow wake-ups ride (see AsyncResponseStartupValidator).
            var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<RabbitMqAsyncResponseOptions>>().Value;
            return new AsyncResponseTransportMarker("RabbitMQ")
            {
                WorkerSubscriberUsesEarlyAck = options.WorkerSubscriber.AckMode == RabbitMqAckMode.AckAfterEnqueue,
                WorkerAckModePath = $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(options.WorkerSubscriber)}.{nameof(options.WorkerSubscriber.AckMode)}",
                ResponseSubscriberUsesEarlyAck = options.ResponseSubscriber.AckMode == RabbitMqAckMode.AckAfterEnqueue,
                ResponseAckModePath = $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(options.ResponseSubscriber)}.{nameof(options.ResponseSubscriber.AckMode)}"
            };
        });

        services.AddHostedService<RabbitMqWorkerSubscriber>();
        services.AddHostedService<RabbitMqResponseIngressSubscriber>();

        return builder;
    }
}
