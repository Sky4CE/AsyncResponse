using AsyncResponse;
using AsyncResponse.Transports.Redis;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// DI registration for the Redis Streams AsyncResponse transport.
/// </summary>
public static class RedisAsyncResponseTransportServiceCollectionExtensions
{
    /// <summary>
    /// Registers Redis Streams as the worker transport and response ingress in one call:
    /// <list type="bullet">
    /// <item><description>worker jobs are XADDed to the configured worker stream;</description></item>
    /// <item><description>a hosted consumer group subscriber reads worker entries and executes jobs;</description></item>
    /// <item><description>a hosted consumer group subscriber reads response entries and feeds them into <see cref="IAsyncResponseIngress"/>.</description></item>
    /// </list>
    /// Redis Streams is a transport, not the waiter/recovery channel. Pair it with
    /// <c>.WithRedisChannel()</c> when late responses must survive redeploys, or with
    /// <c>.WithInMemoryChannel()</c> for simple single-process waits.
    /// <para>
    /// Requires a <c>StackExchange.Redis.IConnectionMultiplexer</c> singleton registered by the
    /// host. Reuse the same multiplexer as the Redis channel package when both are installed.
    /// </para>
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithRedisTransport(
        this AsyncResponseRegistrationBuilder builder,
        Action<RedisAsyncResponseTransportOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var services = builder.Services;
        services.AddOptions();
        services.Configure(configure);

        services.TryAddSingleton<RedisWorkerTransport>();
        services.Replace(ServiceDescriptor.Singleton<IWorkerTransport>(provider =>
            provider.GetRequiredService<RedisWorkerTransport>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseReplyTargetProvider, RedisReplyTargetProvider>());
        services.AddSingleton(provider =>
        {
            // Resolved ack modes declared to the Core startup validator, which vetoes early ACK on
            // the worker queue durable-flow wake-ups ride (see AsyncResponseStartupValidator).
            var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<RedisAsyncResponseTransportOptions>>().Value;
            return new AsyncResponseTransportMarker("Redis")
            {
                WorkerSubscriberUsesEarlyAck = options.WorkerSubscriber.AckMode == RedisAckMode.AckAfterEnqueue,
                WorkerAckModePath = $"{nameof(RedisAsyncResponseTransportOptions)}.{nameof(options.WorkerSubscriber)}.{nameof(options.WorkerSubscriber.AckMode)}",
                ResponseSubscriberUsesEarlyAck = options.ResponseSubscriber.AckMode == RedisAckMode.AckAfterEnqueue,
                ResponseAckModePath = $"{nameof(RedisAsyncResponseTransportOptions)}.{nameof(options.ResponseSubscriber)}.{nameof(options.ResponseSubscriber.AckMode)}"
            };
        });

        services.AddHostedService<RedisWorkerSubscriber>();
        services.AddHostedService<RedisResponseIngressSubscriber>();

        return builder;
    }
}
