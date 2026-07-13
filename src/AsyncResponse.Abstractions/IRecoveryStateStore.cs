namespace AsyncResponse;

/// <summary>
/// Persists per-correlation recovery state for lost-subscriber routing.
/// <para>
/// Response channels use this store to remember the callbacks registered by a waiter. If a
/// response later arrives while no live waiter is subscribed, the publisher can load this state
/// and invoke the appropriate resume/failure callback. Implementations may be durable
/// (Redis/PostgreSQL) or process-local (the default in-memory store in <c>AsyncResponse.Core</c>).
/// </para>
/// </summary>
public interface IRecoveryStateStore
{
    /// <summary>Saves a recovery registration for <paramref name="correlationId"/>.</summary>
    Task SaveAsync(
        string correlationId,
        RecoveryState state,
        TimeSpan ttl,
        CancellationToken cancellationToken = default);

    /// <summary>Loads every live recovery registration for <paramref name="correlationId"/>.</summary>
    Task<IReadOnlyList<RecoveryState>> GetAllAsync(
        string correlationId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically deletes one recovery registration without affecting other waiters that share the
    /// same <paramref name="correlationId"/>. <paramref name="registrationId"/> must be non-empty.
    /// </summary>
    Task<bool> TryDeleteAsync(
        string correlationId,
        Guid registrationId,
        CancellationToken cancellationToken = default);
}
