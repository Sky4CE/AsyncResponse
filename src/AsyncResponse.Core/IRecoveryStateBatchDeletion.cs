namespace AsyncResponse;

/// <summary>
/// A recovery-state store that removes several registrations of one correlation id in a single
/// operation. The lost-subscriber dispatcher consumes every registration whose callback succeeded,
/// and in a store that keeps a correlation id's registrations together in one value (Redis, NATS)
/// each single <see cref="IRecoveryStateStore.TryDeleteAsync"/> read, deserialized, filtered and
/// rewrote everything that remained: a fan-out over N registrations processed
/// N + (N−1) + … + 1 entries — quadratic CPU, allocation and network — while the delivery stayed
/// open, and every concurrent registration change cost the whole remainder again as a CAS retry.
/// <para>
/// A store that implements this has the registrations consumed by one fan-out removed together,
/// in one conditional rewrite, after every registration has been dispatched and before the
/// dispatch settles — so a redelivery for a sibling that failed still reaches that sibling alone.
/// A store that does not keeps the per-registration <see cref="IRecoveryStateStore.TryDeleteAsync"/>,
/// called as each callback succeeds; an application-owned store needs nothing new.
/// </para>
/// <para>
/// The contract is <see cref="IRecoveryStateStore.TryDeleteAsync"/>'s, for a set: remove exactly the
/// named registrations — never one added concurrently, never one this build cannot read — keep
/// every survivor's own expiry, and return how many were removed. Best-effort in the same way: a
/// registration left behind stays until its TTL or the next delivery.
/// </para>
/// </summary>
internal interface IRecoveryStateBatchDeletion
{
    /// <summary>
    /// Removes every registration in <paramref name="registrationIds"/> stored under
    /// <paramref name="correlationId"/>, in one conditional write; returns how many were removed
    /// (<c>0</c> when none was present, or when every optimistic attempt lost to a concurrent writer).
    /// </summary>
    Task<int> TryDeleteManyAsync(
        string correlationId,
        IReadOnlyCollection<Guid> registrationIds,
        CancellationToken cancellationToken = default);
}
