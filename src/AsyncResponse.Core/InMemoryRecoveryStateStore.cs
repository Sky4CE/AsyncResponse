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

    private readonly object _gate = new();
    private readonly Dictionary<string, List<Entry>> _entries = new(StringComparer.Ordinal);

    /// <inheritdoc />
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
        if (state.RegistrationId == Guid.Empty)
            state.RegistrationId = Guid.NewGuid();

        lock (_gate)
        {
            var nowUtc = DateTime.UtcNow;
            var expiresAtUtc = nowUtc.Add(ttl);
            if (!_entries.TryGetValue(correlationId, out var entries))
            {
                _entries[correlationId] = [new Entry(state, expiresAtUtc)];
                return Task.CompletedTask;
            }

            entries.RemoveAll(entry => entry.ExpiresAtUtc <= nowUtc || entry.State.RegistrationId == state.RegistrationId);
            entries.Add(new Entry(state, expiresAtUtc));
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<RecoveryState?> GetAsync(string correlationId, CancellationToken cancellationToken = default)
        => (await GetAllAsync(correlationId, cancellationToken).ConfigureAwait(false)).FirstOrDefault();

    /// <inheritdoc />
    public Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!_entries.TryGetValue(correlationId, out var entries))
                return Task.FromResult<IReadOnlyList<RecoveryState>>([]);

            var nowUtc = DateTime.UtcNow;
            entries.RemoveAll(entry => entry.ExpiresAtUtc <= nowUtc);
            if (entries.Count == 0)
            {
                _entries.Remove(correlationId);
                return Task.FromResult<IReadOnlyList<RecoveryState>>([]);
            }

            var states = entries
                .Where(entry => RecoveryStateSchema.IsReadable(entry.State.SchemaVersion))
                .Select(entry => entry.State)
                .ToArray();
            return Task.FromResult<IReadOnlyList<RecoveryState>>(states);
        }
    }

    /// <inheritdoc />
    public Task<bool> TryDeleteAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
            return Task.FromResult(_entries.Remove(correlationId));
    }

    /// <inheritdoc />
    public Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (!_entries.TryGetValue(correlationId, out var entries))
                return Task.FromResult(false);

            var removed = entries.RemoveAll(entry => entry.State.RegistrationId == registrationId) > 0;
            if (entries.Count == 0)
                _entries.Remove(correlationId);

            return Task.FromResult(removed);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RecoveryState> ScanAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask.ConfigureAwait(false); // process-local store: no async I/O to await

        RecoveryState[] states;
        lock (_gate)
        {
            var nowUtc = DateTime.UtcNow;
            List<string>? emptyKeys = null;
            var liveStates = new List<RecoveryState>();

            foreach (var (correlationId, entries) in _entries)
            {
                entries.RemoveAll(entry => entry.ExpiresAtUtc <= nowUtc);
                if (entries.Count == 0)
                {
                    (emptyKeys ??= []).Add(correlationId);
                    continue;
                }

                foreach (var entry in entries)
                {
                    if (RecoveryStateSchema.IsReadable(entry.State.SchemaVersion))
                        liveStates.Add(entry.State);
                }
            }

            if (emptyKeys is not null)
            {
                foreach (var key in emptyKeys)
                    _entries.Remove(key);
            }

            states = liveStates.ToArray();
        }

        foreach (var state in states)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return state;
        }
    }
}
