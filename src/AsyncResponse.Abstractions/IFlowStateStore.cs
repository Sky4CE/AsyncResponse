namespace AsyncResponse;

/// <summary>
/// Persists durable-flow run state (<see cref="FlowState"/>).
/// <para>
/// The default implementation in <c>AsyncResponse.Core</c> stores flow state through the
/// configured channel's <see cref="IRecoveryStateStore"/> for tests, development, and migration.
/// Production durable flows should register an app-owned implementation with
/// <c>AddAsyncResponse().WithDurableFlows&lt;TStore&gt;()</c>; the library calls only these three
/// members.
/// </para>
/// </summary>
public interface IFlowStateStore
{
    /// <summary>Saves (creates or replaces) the state of one flow run.</summary>
    Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default);

    /// <summary>Loads the state of one flow run, or <c>null</c> when unknown, expired, or unreadable.</summary>
    Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default);

    /// <summary>Deletes the state of one flow run; <c>true</c> when an entry was removed.</summary>
    Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default);
}
