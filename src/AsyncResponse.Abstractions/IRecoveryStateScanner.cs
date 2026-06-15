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
    /// </summary>
    IAsyncEnumerable<RecoveryState> ScanAsync(CancellationToken cancellationToken = default);
}
