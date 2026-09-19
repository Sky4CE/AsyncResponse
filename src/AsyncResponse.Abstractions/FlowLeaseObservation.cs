namespace AsyncResponse;

/// <summary>
/// The execution lease a store has persisted for one flow ledger, exactly as stored: who holds it
/// and until when. Returned by <see cref="IFlowStateStore.ObserveLeaseAsync"/>.
/// <para>
/// The engine never judges <see cref="ExpiresAtUtc"/> against its own clock to decide ownership —
/// that stays the store's job in <see cref="IFlowStateStore.TryAcquireLeaseAsync"/>. It compares
/// two observations of the same ledger with each other: a different <see cref="LeaseId"/>, or a
/// later <see cref="ExpiresAtUtc"/> under the same id, can only have been written by a worker that
/// acquired or renewed the lease in between, which is the proof of a live holder that a waiter's
/// own lease configuration cannot give.
/// </para>
/// </summary>
public sealed class FlowLeaseObservation
{
    /// <summary>The observation of a ledger nobody holds (never leased, released, or absent).</summary>
    public static FlowLeaseObservation Unheld { get; } = new(null, null);

    /// <param name="leaseId">The persisted lease owner, or <c>null</c> when no lease is held.</param>
    /// <param name="expiresAtUtc">The persisted lease expiry, or <c>null</c> when no lease is held.</param>
    public FlowLeaseObservation(string? leaseId, DateTime? expiresAtUtc)
    {
        LeaseId = leaseId;
        ExpiresAtUtc = expiresAtUtc;
    }

    /// <summary>The persisted lease owner; <c>null</c> when no lease is held.</summary>
    public string? LeaseId { get; }

    /// <summary>
    /// The persisted lease expiry (UTC), possibly already in the past — an expired lease stays on
    /// the row until someone acquires or releases it. <c>null</c> when no lease is held.
    /// </summary>
    public DateTime? ExpiresAtUtc { get; }
}
