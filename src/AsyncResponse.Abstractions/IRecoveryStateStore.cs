namespace AsyncResponse;

/// <summary>
/// Persists per-correlation recovery state for lost-subscriber routing.
/// <para>
/// Response channels use this store to remember the callbacks registered by a waiter. If a
/// response later arrives while no live waiter is subscribed, the publisher can load this state
/// and invoke the appropriate resume/failure callback. Implementations may be durable
/// (Redis/Postgres) or process-local (the default in-memory store in <c>AsyncResponse.Core</c>).
/// </para>
/// </summary>
public interface IRecoveryStateStore
{
    /// <summary>Saves or replaces the recovery state for <paramref name="correlationId"/>.</summary>
    Task SaveAsync(
        string correlationId,
        RecoveryState state,
        TimeSpan ttl,
        CancellationToken cancellationToken = default);

    /// <summary>Loads recovery state for <paramref name="correlationId"/>, or <c>null</c> when absent or expired.</summary>
    Task<RecoveryState?> GetAsync(
        string correlationId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes recovery state for <paramref name="correlationId"/>.
    /// Returns <c>true</c> when a state entry was removed, or <c>false</c> when it was already gone.
    /// </summary>
    Task<bool> TryDeleteAsync(
        string correlationId,
        CancellationToken cancellationToken = default);
}
