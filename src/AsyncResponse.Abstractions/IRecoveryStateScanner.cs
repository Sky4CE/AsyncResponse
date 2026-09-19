namespace AsyncResponse;

/// <summary>
/// Optional capability of an <see cref="IRecoveryStateStore"/>: enumerate every persisted
/// <see cref="RecoveryState"/> entry. The async-response watchdog uses it to find outstanding
/// wait registrations regardless of the backing channel — a durable store (e.g. Redis) scans its
/// keyspace, the process-local store walks its in-memory map.
/// <para>
/// Built-in stores implement this on the same instance that implements
/// <see cref="IRecoveryStateStore"/>; both are registered together by the channel's <c>With…</c>
/// registration. A custom store that does not implement it simply disables the watchdog.
/// </para>
/// </summary>
public interface IRecoveryStateScanner
{
    /// <summary>
    /// Streams a point-in-time view of the persisted recovery entries. Implementations skip
    /// entries that have already expired and may yield a best-effort snapshot rather than a
    /// transactionally consistent one.
    /// <para>
    /// "Best effort" covers consistency, not coverage: an implementation that cannot inspect part
    /// of its storage — a disconnected server, an unreachable shard — must <em>throw</em>, never
    /// complete with the reachable subset. The watchdog reports a failed scan as
    /// <c>Degraded</c>, and a completed one as a verdict over everything that exists: an outage
    /// that enumerates as "no registrations" would clear the stuck-flow alarm exactly when the
    /// evidence became unavailable.
    /// </para>
    /// </summary>
    IAsyncEnumerable<RecoveryState> ScanAsync(CancellationToken cancellationToken = default);
}
