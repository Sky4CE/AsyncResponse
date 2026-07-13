namespace AsyncResponse;

/// <summary>
/// Persists durable-flow ledgers with the atomic operations required for safe multi-replica
/// execution. Implementations must provide insert-if-absent creation, revision-checked updates,
/// and renewable owner-fenced execution leases.
/// </summary>
public interface IFlowStateStore
{
    /// <summary>Atomically creates a new flow ledger; returns false when the id already exists.</summary>
    Task<bool> TryCreateAsync(
        string flowId,
        FlowState state,
        TimeSpan ttl,
        CancellationToken cancellationToken = default);

    /// <summary>Loads the state of one flow run, or <c>null</c> when unknown, expired, or unreadable.</summary>
    Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically replaces a ledger when its stored revision matches <paramref name="expectedRevision"/>.
    /// When <paramref name="leaseId"/> is supplied, that same unexpired execution lease must own
    /// the ledger. The supplied state's revision is the new revision to persist.
    /// </summary>
    Task<bool> TryUpdateAsync(
        string flowId,
        FlowState state,
        long expectedRevision,
        TimeSpan ttl,
        string? leaseId = null,
        CancellationToken cancellationToken = default);

    /// <summary>Atomically acquires an expired or unowned execution lease.</summary>
    Task<bool> TryAcquireLeaseAsync(
        string flowId,
        string leaseId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    /// <summary>Renews an unexpired lease owned by <paramref name="leaseId"/>.</summary>
    Task<bool> TryRenewLeaseAsync(
        string flowId,
        string leaseId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    /// <summary>Releases the lease when it is still owned by <paramref name="leaseId"/>.</summary>
    Task ReleaseLeaseAsync(
        string flowId,
        string leaseId,
        CancellationToken cancellationToken = default);

    /// <summary>Deletes the state of one flow run; <c>true</c> when an entry was removed.</summary>
    Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default);
}
