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
    /// Registers Redis pub/sub as the response channel — behind <see cref="IAsyncResponsePublisher"/>,
    /// <see cref="IAsyncResponseSubscriber"/>, and <see cref="IRecoverableAsyncResponseSubscriber"/> —
    /// together with the durable Redis recovery-state store, so a response that arrives after the
    /// waiter died (e.g. a redeploy) can still resume or fail the owning flow. It also registers
    /// <see cref="IRecoverableAsyncResponseBuilder"/> for fluent durable recovery flows. The same
    /// channel instance serves the engine's recovery-state scanner and active-subscriber probe, so the
    /// watchdog works against Redis automatically.
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
        services.Replace(ServiceDescriptor.Singleton<IRawAsyncResponsePublisher>(provider => provider.GetRequiredService<RedisAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseSubscriber>(provider => provider.GetRequiredService<RedisAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IRecoverableAsyncResponseSubscriber>(provider => provider.GetRequiredService<RedisAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IActiveSubscriberProbe>(provider => provider.GetRequiredService<RedisAsyncResponseChannel>()));

        // Durable recovery capability: expose the recoverable fluent builder only when the Redis
        // channel package is the selected response channel. Plain IAsyncResponseBuilder still works
        // and shares the same implementation, but its static type does not offer recovery callbacks.
        services.Replace(ServiceDescriptor.Singleton<IRecoverableAsyncResponseBuilder>(provider => new RecoverableAsyncResponseBuilder(
            provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>(),
            provider.GetService<IWorkerTransport>(),
            provider.GetService<IAsyncResponseReplyTargetProvider>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>(),
            provider.GetService<TimeProvider>())));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseBuilder>(provider => provider.GetRequiredService<IRecoverableAsyncResponseBuilder>()));

        // The resolved default waiter timeout is declared through the marker so the startup
        // validator can require the durable-flow ledger TTL to out-live a timeout-less awaited
        // step, and the flow engine can extend a parked ledger by it — without either referencing
        // channel option types.
        services.AddSingleton(provider =>
        {
            var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<RedisAsyncResponseOptions>>().Value;
            return new AsyncResponseChannelMarker("Redis")
            {
                EffectiveDefaultWaitTimeout = options.DefaultTimeout ?? options.RecoveryStateExpiry
            };
        });
        return builder;
    }
}
