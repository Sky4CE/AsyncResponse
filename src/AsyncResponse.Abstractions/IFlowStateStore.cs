namespace AsyncResponse;

/// <summary>
/// Persists durable-flow ledgers with the atomic operations required for safe multi-replica
/// execution. Implementations must provide insert-if-absent creation, revision-checked updates,
/// and renewable owner-fenced execution leases.
/// </summary>
public interface IFlowStateStore
{
    /// <summary>
    /// Checks deterministic creation constraints, including the serialized state-size budget,
    /// without writing state or contacting external infrastructure. The starter calls this before
    /// publishing its start job. Implementations must still validate actual writes; this is not a
    /// reservation. The default is a no-op for compatibility with application-owned stores.
    /// </summary>
    /// <exception cref="FlowStateTooLargeException">The initial state exceeds the store's budget.</exception>
    void ValidateCreate(string flowId, FlowState state, TimeSpan ttl) { }

    /// <summary>Atomically creates a new flow ledger; returns false when the id already exists.</summary>
    Task<bool> TryCreateAsync(
        string flowId,
        FlowState state,
        TimeSpan ttl,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Loads the state of one flow run, or <c>null</c> when the run is genuinely gone — unknown,
    /// pruned, or expired. A row that exists but cannot be interpreted — malformed JSON, an
    /// unknown schema version, a revision inside the JSON that disagrees with the stored one, a
    /// flow id inside the JSON that is not the key — is <em>not</em> absence and must throw
    /// <see cref="FlowStateUnreadableException"/>: callers acknowledge a wake-up on <c>null</c>,
    /// so reporting a live-but-unreadable ledger that way strands the run.
    /// </summary>
    /// <exception cref="FlowStateUnreadableException">The ledger exists but is uninterpretable or inconsistent.</exception>
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

    /// <summary>
    /// Reports the execution lease currently persisted for <paramref name="flowId"/> — the raw
    /// owner and expiry, without judging whether it has lapsed. Returns
    /// <see cref="FlowLeaseObservation.Unheld"/> when no lease is held (or the ledger is absent),
    /// and <c>null</c> when this store cannot report leases at all, which is the default for
    /// compatibility with application-owned stores.
    /// <para>
    /// A wake-up that finds the lease held uses this to tell a live holder from a dead one: a
    /// lease whose owner or expiry changes while the wake-up waits was acquired or renewed by a
    /// live worker, so the wake-up is a duplicate; a lease that never changes belongs to a dead
    /// holder and is waited out to its <em>persisted</em> expiry, whatever lease duration issued
    /// it. A store that returns <c>null</c> gives the engine no such evidence, so a wake-up that
    /// cannot acquire the lease within its own lease window is never acknowledged: it fails with
    /// <see cref="DurableFlowLeaseContendedException"/> and the worker transport redelivers it.
    /// </para>
    /// </summary>
    Task<FlowLeaseObservation?> ObserveLeaseAsync(string flowId, CancellationToken cancellationToken = default)
        => Task.FromResult<FlowLeaseObservation?>(null);

    /// <summary>Deletes the state of one flow run; <c>true</c> when an entry was removed.</summary>
    Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default);
}
