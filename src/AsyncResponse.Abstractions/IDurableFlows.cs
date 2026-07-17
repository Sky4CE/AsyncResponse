using System.Diagnostics.CodeAnalysis;

namespace AsyncResponse;

/// <summary>
/// Starts and manages durable flows (<see cref="IDurableFlow{TInput}"/>). Registered only when the
/// application explicitly selects an atomic flow-state store, for example with
/// <c>WithInMemoryDurableFlows()</c> or a provider-backed durable-flow package.
/// </summary>
public interface IDurableFlows
{
    /// <summary>
    /// Creates a flow run and enqueues its execution on the worker transport. Returns the flow id.
    /// <para>
    /// Pass a non-empty <paramref name="flowId"/> to make the start idempotent: starting an id that
    /// already exists with the same flow type and semantically identical input re-enqueues the
    /// existing run (which skips completed steps). Reusing the id for different work is rejected.
    /// </para>
    /// </summary>
    /// <typeparam name="TFlow">The flow class; must be registered in DI and resolvable by its persisted type name.</typeparam>
    /// <typeparam name="TInput">The flow input, persisted as JSON with the flow state.</typeparam>
    /// <exception cref="ArgumentException"><paramref name="flowId"/> is empty or whitespace.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="flowId"/> already belongs to different work.</exception>
    Task<string> StartAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.Interfaces)] TFlow, TInput>(
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
