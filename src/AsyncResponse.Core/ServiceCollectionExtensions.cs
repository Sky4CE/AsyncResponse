using AsyncResponse;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Core registrations for AsyncResponse.
/// </summary>
public static class AsyncResponseCoreServiceCollectionExtensions
{
    /// <summary>
    /// Registers AsyncResponse with the default process-local response channel and recovery
    /// store. This is the simplest setup: no Redis, no durable recovery, but the same
    /// async/await request-response pattern, predicates, timeouts, and broker ingress.
    /// </summary>
    public static IServiceCollection AddAsyncResponse(
        this IServiceCollection services,
        Action<AsyncResponseOptions>? configure = null)
    {
        services.AddOptions();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<IRecoveryStateStore, InMemoryRecoveryStateStore>();
        services.TryAddSingleton<InMemoryAsyncResponseChannel>();
        services.TryAddSingleton<IAsyncResponsePublisher>(provider => provider.GetRequiredService<InMemoryAsyncResponseChannel>());
        services.TryAddSingleton<IAsyncResponseSubscriber>(provider => provider.GetRequiredService<InMemoryAsyncResponseChannel>());
        services.AddAsyncResponseBuilder();

        return services;
    }

    /// <summary>
    /// Registers the fluent <see cref="IAsyncResponseBuilder"/>, transport-neutral
    /// <see cref="IAsyncResponseIngress"/>, and <see cref="WorkerJobExecutor"/>.
    /// Requires an <see cref="IAsyncResponseSubscriber"/> and <see cref="IAsyncResponsePublisher"/>
    /// to be registered by <c>AddAsyncResponse()</c> or a backend package. An
    /// <see cref="IWorkerTransport"/> is optional: without one, <c>EnqueueWorkerAsync</c> throws
    /// with guidance.
    /// </summary>
    public static IServiceCollection AddAsyncResponseBuilder(this IServiceCollection services)
    {
        services.TryAddSingleton<WorkerJobExecutor>();
        services.TryAddSingleton<IAsyncResponseIngress, AsyncResponseIngress>();
        services.TryAddSingleton<IAsyncResponseBuilder>(provider => new AsyncResponseBuilder(
            provider.GetRequiredService<IAsyncResponseSubscriber>(),
            provider.GetService<IWorkerTransport>()));
        return services;
    }

    /// <summary>
    /// Registers the in-process <see cref="IWorkerTransport"/> and its background consumer.
    /// Jobs are executed within the current process and survive only as long as it does —
    /// suitable for development, tests, and single-node deployments. For distributed,
    /// durable execution, implement <see cref="IWorkerTransport"/> against your broker and feed
    /// consumed messages into <see cref="IAsyncResponseIngress.HandleWorkerMessageAsync"/>.
    /// </summary>
    public static IServiceCollection AddInProcessWorkerQueue(this IServiceCollection services)
    {
        services.TryAddSingleton<WorkerJobExecutor>();
        services.TryAddSingleton<InProcessWorkerTransport>();
        services.TryAddSingleton<IWorkerTransport>(provider => provider.GetRequiredService<InProcessWorkerTransport>());
        services.AddHostedService<InProcessWorkerHost>();
        return services;
    }

    /// <summary>
    /// Registers the in-process <see cref="IWorkerTransport"/>. Kept for compatibility; new code
    /// can use <see cref="AddInProcessWorkerQueue(IServiceCollection)"/> for clearer terminology.
    /// </summary>
    public static IServiceCollection AddInProcessWorkerTransport(this IServiceCollection services)
        => services.AddInProcessWorkerQueue();
}
