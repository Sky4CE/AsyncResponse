namespace AsyncResponse;

/// <summary>
/// Starts and manages durable flows (<see cref="IDurableFlow{TInput}"/>). Registered by
/// <c>AddAsyncResponse()</c>; the default flow-state store uses the configured channel's recovery
/// store for tests/dev/migration. Production durable flows should register an app-owned
/// <see cref="IFlowStateStore"/>.
/// </summary>
public interface IDurableFlows
{
    /// <summary>
    /// Creates a flow run and enqueues its execution on the worker transport. Returns the flow id.
    /// <para>
    /// Pass <paramref name="flowId"/> to make the start idempotent: starting an id that already
    /// exists re-enqueues the existing run (which skips completed steps) instead of creating a
    /// duplicate — safe for retried API calls and operator "kick" actions.
    /// </para>
    /// </summary>
    /// <typeparam name="TFlow">The flow class; must be registered in DI and resolvable by its persisted type name.</typeparam>
    /// <typeparam name="TInput">The flow input, persisted as JSON with the flow state.</typeparam>
    Task<string> StartAsync<TFlow, TInput>(
        TInput input,
        string? flowId = null,
        CancellationToken cancellationToken = default)
        where TFlow : class, IDurableFlow<TInput>;

    /// <summary>
    /// Re-enqueues execution of an existing run — completed steps are skipped and the in-flight
    /// awaited step re-attaches. No-op for runs that already succeeded or failed.
    /// </summary>
    Task ResumeAsync(string flowId, CancellationToken cancellationToken = default);

    /// <summary>Loads a snapshot of the flow run's state, or <c>null</c> when unknown or expired.</summary>
    Task<FlowState?> GetStateAsync(string flowId, CancellationToken cancellationToken = default);
}
