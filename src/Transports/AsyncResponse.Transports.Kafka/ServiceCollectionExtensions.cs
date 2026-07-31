using AsyncResponse;
using AsyncResponse.Transports.Kafka;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// DI registration for the Apache Kafka AsyncResponse transport.
/// </summary>
public static class KafkaAsyncResponseTransportServiceCollectionExtensions
{
    /// <summary>
    /// Registers Apache Kafka as the worker transport and response ingress in one call:
    /// <list type="bullet">
    /// <item><description>worker jobs are produced to the configured worker topic;</description></item>
    /// <item><description>a hosted consumer-group subscriber reads worker messages and executes jobs;</description></item>
    /// <item><description>a hosted consumer-group subscriber reads response messages and feeds them into <see cref="IAsyncResponseIngress"/>.</description></item>
    /// </list>
    /// Kafka is a transport, not the waiter/recovery channel — its partitioned log has no targeted
    /// waiter wake and no per-key TTL store. Pair it with <c>.WithRedisChannel()</c>,
    /// <c>.WithNatsChannel()</c>, or <c>.WithPostgreSqlChannel()</c> when late responses must
    /// survive redeploys, or with <c>.WithInMemoryChannel()</c> for simple single-process waits.
    /// <para>
    /// The package speaks the Kafka protocol via <c>Confluent.Kafka</c>, so it also works against
    /// Redpanda, Amazon MSK, WarpStream, Aiven, and Confluent Cloud.
    /// </para>
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithKafkaTransport(
        this AsyncResponseRegistrationBuilder builder,
        Action<KafkaAsyncResponseTransportOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var services = builder.Services;
        services.AddOptions();
        services.Configure(configure);

        services.TryAddSingleton<IKafkaProducerClient>(provider => new KafkaProducerClientAdapter(
            provider.GetRequiredService<IOptions<KafkaAsyncResponseTransportOptions>>().Value));
        services.TryAddSingleton<IKafkaConsumerClientFactory>(provider => new KafkaConsumerClientFactory(
            provider.GetRequiredService<IOptions<KafkaAsyncResponseTransportOptions>>().Value));
        services.TryAddSingleton<IKafkaAdminClient>(provider => new KafkaAdminClientAdapter(
            provider.GetRequiredService<IOptions<KafkaAsyncResponseTransportOptions>>().Value));

        services.TryAddSingleton(provider => new KafkaWorkerTransport(
            provider.GetRequiredService<IOptions<KafkaAsyncResponseTransportOptions>>(),
            provider.GetRequiredService<IKafkaProducerClient>()));
        services.Replace(ServiceDescriptor.Singleton<IWorkerTransport>(provider =>
            provider.GetRequiredService<KafkaWorkerTransport>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseReplyTargetProvider, KafkaReplyTargetProvider>());
        services.AddSingleton(provider =>
        {
            // Resolved ack modes declared to the Core startup validator, which vetoes early ACK on
            // the worker queue durable-flow wake-ups ride (see AsyncResponseStartupValidator).
            var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<KafkaAsyncResponseTransportOptions>>().Value;
            return new AsyncResponseTransportMarker("Kafka")
            {
                WorkerSubscriberUsesEarlyAck = options.WorkerSubscriber.AckMode == KafkaAckMode.AckAfterEnqueue,
                WorkerAckModePath = $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(options.WorkerSubscriber)}.{nameof(options.WorkerSubscriber.AckMode)}",
                ResponseSubscriberUsesEarlyAck = options.ResponseSubscriber.AckMode == KafkaAckMode.AckAfterEnqueue,
                ResponseAckModePath = $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(options.ResponseSubscriber)}.{nameof(options.ResponseSubscriber.AckMode)}"
            };
        });

        services.AddHostedService<KafkaWorkerSubscriber>();
        services.AddHostedService<KafkaResponseIngressSubscriber>();

        return builder;
    }
}
