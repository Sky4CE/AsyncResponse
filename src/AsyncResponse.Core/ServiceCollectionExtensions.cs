using AsyncResponse;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Core registrations for AsyncResponse. Everything is configured through the fluent builder
/// returned by <see cref="AddAsyncResponse"/>: chain a channel and, optionally, a worker transport.
/// </summary>
public static class AsyncResponseCoreServiceCollectionExtensions
{
    /// <summary>
    /// Registers the channel-agnostic AsyncResponse engine (fluent waiter builder, transport-neutral
    /// ingress, worker-job executor, and the recovery watchdog) and returns a builder to configure
    /// the rest. It deliberately registers <em>no</em> response channel: chain exactly one
    /// (<see cref="WithInMemoryChannel"/> or the Redis channel package's <c>WithRedisChannel</c>) — an app
    /// that starts without a channel fails fast at host startup. Optionally chain a worker transport
    /// (<see cref="WithInMemoryTransport"/> or the Google Pub/Sub transport package's
    /// <c>WithGooglePubSubTransport</c>).
    /// </summary>
    public static AsyncResponseRegistrationBuilder AddAsyncResponse(
        this IServiceCollection services,
        Action<AsyncResponseOptions>? configure = null)
    {
        services.AddOptions();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        // Channel-agnostic engine.
        services.TryAddSingleton<AsyncResponseContextPropagation>();
        services.TryAddSingleton<WorkerJobExecutor>();
        services.TryAddSingleton<IAsyncResponseIngress, AsyncResponseIngress>();
        services.TryAddSingleton<IAsyncResponseBuilder>(provider => new AsyncResponseBuilder(
            provider.GetRequiredService<IAsyncResponseSubscriber>(),
            provider.GetService<IWorkerTransport>(),
            provider.GetService<IAsyncResponseReplyTargetProvider>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>()));

        // The recovery watchdog is part of the engine and runs by default for whatever channel is
        // registered (scanning + liveness go through IRecoveryStateScanner / IActiveSubscriberProbe).
        services.TryAddSingleton<AsyncResponseWatchdogState>();
        services.AddHostedService<AsyncResponseWatchdog>();

        // Fail fast at host startup if no channel was chained on.
        services.AddHostedService<AsyncResponseStartupValidator>();

        return new AsyncResponseRegistrationBuilder(services);
    }

    /// <summary>
    /// Registers an application <see cref="IAsyncResponseContextPropagator"/> that carries ambient
    /// context (trace id, principal, tenant, …) across the serialization boundary into worker jobs
    /// and lost-subscriber recovery callbacks. In-process hops flow ambient state automatically via
    /// the captured <see cref="System.Threading.ExecutionContext"/>; propagators are only needed for
    /// context that must survive serialization (broker-backed workers, recovery after a redeploy).
    /// Register one per concern (e.g. a trace propagator and a principal propagator).
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithContextPropagator<TPropagator>(this AsyncResponseRegistrationBuilder builder)
        where TPropagator : class, IAsyncResponseContextPropagator
    {
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IAsyncResponseContextPropagator, TPropagator>());
        return builder;
    }

    /// <summary>
    /// Registers the process-local response channel and recovery store. Waiters, subscriptions,
    /// and recovery state all live in memory and disappear when the process exits — the simplest
    /// setup, with no durable recovery. Pair with <see cref="WithInMemoryTransport"/> for a fully
    /// in-memory setup including background worker jobs.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithInMemoryChannel(
        this AsyncResponseRegistrationBuilder builder,
        Action<InMemoryAsyncResponseOptions>? configure = null)
    {
        var services = builder.Services;
        services.AddOptions();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<InMemoryRecoveryStateStore>();
        services.TryAddSingleton<IRecoveryStateStore>(provider => provider.GetRequiredService<InMemoryRecoveryStateStore>());
        services.TryAddSingleton<IRecoveryStateScanner>(provider => provider.GetRequiredService<InMemoryRecoveryStateStore>());

        services.TryAddSingleton<InMemoryAsyncResponseChannel>();
        services.TryAddSingleton<IAsyncResponsePublisher>(provider => provider.GetRequiredService<InMemoryAsyncResponseChannel>());
        services.TryAddSingleton<IAsyncResponseSubscriber>(provider => provider.GetRequiredService<InMemoryAsyncResponseChannel>());
        services.TryAddSingleton<IActiveSubscriberProbe>(provider => provider.GetRequiredService<InMemoryAsyncResponseChannel>());

        services.AddSingleton(new AsyncResponseChannelMarker("InMemory"));
        return builder;
    }

    /// <summary>
    /// Registers the in-memory (in-process) worker transport and its background consumer. Jobs run
    /// in the current process and survive only as long as it does — suitable for development, tests,
    /// and single-node deployments. For distributed, durable execution use a broker-backed transport
    /// (e.g. the Google Pub/Sub transport package's <c>WithGooglePubSubTransport</c>) and feed consumed
    /// messages into <see cref="IAsyncResponseIngress.HandleWorkerMessageAsync"/>.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithInMemoryTransport(this AsyncResponseRegistrationBuilder builder)
    {
        var services = builder.Services;
        services.TryAddSingleton<WorkerJobExecutor>();
        services.TryAddSingleton<InMemoryWorkerTransport>();
        services.TryAddSingleton<IWorkerTransport>(provider => provider.GetRequiredService<InMemoryWorkerTransport>());
        services.AddHostedService<InMemoryWorkerHost>();
        return builder;
    }
}
