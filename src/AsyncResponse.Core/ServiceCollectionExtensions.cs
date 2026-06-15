using AsyncResponse;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Core registrations for AsyncResponse. Transport packages (e.g. <c>AsyncResponse.Redis</c>)
/// call these from their own <c>Add…</c> methods; hosts normally do not call them directly.
/// </summary>
public static class AsyncResponseCoreServiceCollectionExtensions
{
    /// <summary>
    /// Registers the fluent <see cref="IAsyncResponseBuilder"/> and the
    /// <see cref="WorkerJobExecutor"/>. Requires an <see cref="IAsyncResponseSubscriber"/>
    /// (provided by a transport package). An <see cref="IWorkerTransport"/> is optional:
    /// without one, <c>EnqueueWorkerAsync</c> throws with guidance.
    /// </summary>
    public static IServiceCollection AddAsyncResponseBuilder(this IServiceCollection services)
    {
        services.TryAddSingleton<WorkerJobExecutor>();
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
    public static IServiceCollection AddInProcessWorkerTransport(this IServiceCollection services)
    {
        services.TryAddSingleton<WorkerJobExecutor>();
        services.TryAddSingleton<InProcessWorkerTransport>();
        services.TryAddSingleton<IWorkerTransport>(provider => provider.GetRequiredService<InProcessWorkerTransport>());
        services.AddHostedService<InProcessWorkerHost>();
        return services;
    }
}
