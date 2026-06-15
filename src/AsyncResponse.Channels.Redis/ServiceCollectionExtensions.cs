using AsyncResponse;
using AsyncResponse.Channels.Redis;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// DI registration for the Redis-backed AsyncResponse channel package.
/// </summary>
public static class RedisAsyncResponseServiceCollectionExtensions
{
    /// <summary>
    /// Registers Redis pub/sub as the response channel — behind <see cref="IAsyncResponsePublisher"/>
    /// and <see cref="IAsyncResponseSubscriber"/> — together with the durable Redis recovery-state
    /// store, so a response that arrives after the waiter died (e.g. a redeploy) can still resume or
    /// fail the owning flow. The same instance also serves the engine's recovery-state scanner and
    /// active-subscriber probe, so the watchdog works against Redis automatically.
    /// <para>
    /// Requires a <c>StackExchange.Redis.IConnectionMultiplexer</c> singleton registered by the
    /// host — reuse the application's existing multiplexer; don't create a second connection.
    /// Publisher/subscriber resolve to one shared instance: per-interface instances would split the
    /// channel's internal per-channel state.
    /// </para>
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithRedisChannel(
        this AsyncResponseRegistrationBuilder builder,
        Action<RedisAsyncResponseOptions>? configure = null)
    {
        var services = builder.Services;
        services.AddOptions();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        // Durable recovery store; also the watchdog's recovery-state scanner.
        services.TryAddSingleton<RedisRecoveryStateStore>();
        services.Replace(ServiceDescriptor.Singleton<IRecoveryStateStore>(provider => provider.GetRequiredService<RedisRecoveryStateStore>()));
        services.Replace(ServiceDescriptor.Singleton<IRecoveryStateScanner>(provider => provider.GetRequiredService<RedisRecoveryStateStore>()));

        // Redis pub/sub response channel: publisher + subscriber + the watchdog's liveness probe,
        // all one shared instance.
        services.TryAddSingleton<RedisAsyncResponseChannel>();
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponsePublisher>(provider => provider.GetRequiredService<RedisAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseSubscriber>(provider => provider.GetRequiredService<RedisAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IActiveSubscriberProbe>(provider => provider.GetRequiredService<RedisAsyncResponseChannel>()));

        services.AddSingleton(new AsyncResponseChannelMarker("Redis"));
        return builder;
    }
}
