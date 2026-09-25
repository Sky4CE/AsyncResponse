namespace AsyncResponse;

/// <summary>
/// A view of <paramref name="inner"/> whose loads are <see cref="IFlowStateStore.LoadCurrentAsync"/>,
/// so the executor can re-run a <see cref="FlowStateConcurrency.MutateAsync"/> authoritatively
/// before it acknowledges a delivery on a decision that writes nothing (see
/// <see cref="IFlowStateStore.LoadCurrentAsync"/>). Every other member forwards unchanged —
/// including the default interface members, which would otherwise answer with their defaults
/// instead of the inner store's overrides.
/// </summary>
internal sealed class CurrentReadFlowStateStore(IFlowStateStore inner) : IFlowStateStore
{
    public void ValidateCreate(string flowId, FlowState state, TimeSpan ttl)
        => inner.ValidateCreate(flowId, state, ttl);

    public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
        => inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

    public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
        => inner.LoadCurrentAsync(flowId, cancellationToken);

    public Task<FlowState?> LoadCurrentAsync(string flowId, CancellationToken cancellationToken = default)
        => inner.LoadCurrentAsync(flowId, cancellationToken);

    public Task<bool> TryUpdateAsync(
        string flowId,
        FlowState state,
        long expectedRevision,
        TimeSpan ttl,
        string? leaseId = null,
        CancellationToken cancellationToken = default)
        => inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

    public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

    public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

    public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
        => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

    public Task<FlowLeaseObservation?> ObserveLeaseAsync(string flowId, CancellationToken cancellationToken = default)
        => inner.ObserveLeaseAsync(flowId, cancellationToken);

    public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
        => inner.TryDeleteAsync(flowId, cancellationToken);
}
