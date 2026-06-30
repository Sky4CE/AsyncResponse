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

    /// <summary>Loads recovery state for <paramref name="correlationId"/>, or <c>null</c> when absent or expired.</summary>
    Task<RecoveryState?> GetAsync(
        string correlationId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Loads every recovery registration for <paramref name="correlationId"/>. The default keeps
    /// custom stores source-compatible by exposing their existing single-entry behavior as a
    /// one-element list.
    /// </summary>
    async Task<IReadOnlyList<RecoveryState>> GetAllAsync(
        string correlationId,
        CancellationToken cancellationToken = default)
    {
        var state = await GetAsync(correlationId, cancellationToken).ConfigureAwait(false);
        return state is null ? [] : [state];
    }

    /// <summary>
    /// Deletes recovery state for <paramref name="correlationId"/>.
    /// Returns <c>true</c> when a state entry was removed, or <c>false</c> when it was already gone.
    /// </summary>
    Task<bool> TryDeleteAsync(
        string correlationId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes one recovery registration for <paramref name="correlationId"/>. The default preserves
    /// the pre-list behavior for custom stores by deleting the whole correlation-id entry.
    /// </summary>
    Task<bool> TryDeleteAsync(
        string correlationId,
        Guid registrationId,
        CancellationToken cancellationToken = default)
        => TryDeleteAsync(correlationId, cancellationToken);
}
