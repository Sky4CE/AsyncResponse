using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace AsyncResponse;

/// <summary>
/// Process-local recovery state store. Useful for the default no-infrastructure setup, tests,
/// and single-process apps. It is intentionally not durable: entries disappear when the process
/// exits.
/// </summary>
internal sealed class InMemoryRecoveryStateStore : IRecoveryStateStore, IRecoveryStateScanner
{
    private sealed record Entry(RecoveryState State, DateTime ExpiresAtUtc);

    private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.Ordinal);

    public Task SaveAsync(
        string correlationId,
        RecoveryState state,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        ArgumentNullException.ThrowIfNull(state);
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl), "TTL must be greater than zero.");

        cancellationToken.ThrowIfCancellationRequested();
        _entries[correlationId] = new Entry(state, DateTime.UtcNow.Add(ttl));
        return Task.CompletedTask;
    }

    public Task<RecoveryState?> GetAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();

        if (!_entries.TryGetValue(correlationId, out var entry))
            return Task.FromResult<RecoveryState?>(null);

        if (entry.ExpiresAtUtc <= DateTime.UtcNow)
        {
            _entries.TryRemove(correlationId, out _);
            return Task.FromResult<RecoveryState?>(null);
        }

        return Task.FromResult<RecoveryState?>(entry.State);
    }

    public Task<bool> TryDeleteAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_entries.TryRemove(correlationId, out _));
    }

    public async IAsyncEnumerable<RecoveryState> ScanAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask.ConfigureAwait(false); // process-local store: no async I/O to await

        var nowUtc = DateTime.UtcNow;

        // Enumerate the dictionary directly rather than materializing ConcurrentDictionary.Values.
        // The Values property builds a fresh List/ReadOnlyCollection holding a copy of every value,
        // acquiring all of the dictionary's bucket locks to do so — an O(N) heap allocation and a
        // latency spike that grow with the store on every scan. The dictionary's own enumerator is
        // allocation-light and lock-free: it walks the buckets in place and hands back
        // KeyValuePair<,> structs without snapshotting, so a scan over N entries adds no per-scan
        // heap traffic beyond the states it actually yields.
        foreach (var (_, entry) in _entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (entry.ExpiresAtUtc > nowUtc)
                yield return entry.State;
        }
    }
}
