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

    private readonly ConcurrentDictionary<string, EntryBucket> _entries = new(StringComparer.Ordinal);

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
        if (!string.Equals(state.CorrelationId, correlationId, StringComparison.Ordinal))
            throw new ArgumentException("The recovery-state correlation id must match the store key.", nameof(state));
        if (state.SchemaVersion != RecoveryStateSchema.Current)
            throw new ArgumentException("The recovery state must use the current schema version.", nameof(state));

        cancellationToken.ThrowIfCancellationRequested();
        if (state.RegistrationId == Guid.Empty)
            state.RegistrationId = Guid.NewGuid();

        var nowUtc = DateTime.UtcNow;
        var entry = new Entry(state, nowUtc.Add(ttl));
        while (true)
        {
            if (!_entries.TryGetValue(correlationId, out var bucket))
            {
                if (_entries.TryAdd(correlationId, EntryBucket.Single(entry)))
                    return Task.CompletedTask;

                continue;
            }

            var next = bucket.PruneExpired(nowUtc).Upsert(entry);
            if (_entries.TryUpdate(correlationId, next, bucket))
                return Task.CompletedTask;
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();

        while (_entries.TryGetValue(correlationId, out var bucket))
        {
            var pruned = bucket.PruneExpired(DateTime.UtcNow);
            if (pruned.IsEmpty)
            {
                TryRemove(correlationId, bucket);
                return Task.FromResult<IReadOnlyList<RecoveryState>>([]);
            }

            if (!pruned.Equals(bucket) && !_entries.TryUpdate(correlationId, pruned, bucket))
                continue;

            return Task.FromResult(pruned.ReadableStates());
        }

        return Task.FromResult<IReadOnlyList<RecoveryState>>([]);
    }

    /// <inheritdoc />
    public Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        if (registrationId == Guid.Empty)
            throw new ArgumentException("Registration id cannot be empty.", nameof(registrationId));
        cancellationToken.ThrowIfCancellationRequested();

        while (_entries.TryGetValue(correlationId, out var bucket))
        {
            var pruned = bucket.PruneExpired(DateTime.UtcNow);
            if (pruned.IsEmpty)
            {
                TryRemove(correlationId, bucket);
                return Task.FromResult(false);
            }

            var next = pruned.Remove(registrationId, out var removed);
            if (!removed)
            {
                if (!pruned.Equals(bucket) && !_entries.TryUpdate(correlationId, pruned, bucket))
                    continue;

                return Task.FromResult(false);
            }

            if (next.IsEmpty)
            {
                if (TryRemove(correlationId, bucket))
                    return Task.FromResult(true);
            }
            else if (_entries.TryUpdate(correlationId, next, bucket))
            {
                return Task.FromResult(true);
            }
        }

        return Task.FromResult(false);
    }

    private bool TryRemove(string correlationId, EntryBucket bucket)
        => ((ICollection<KeyValuePair<string, EntryBucket>>)_entries)
            .Remove(new KeyValuePair<string, EntryBucket>(correlationId, bucket));

    // Deliberately not a flat ConcurrentDictionary<(correlationId, registrationId), Entry>: the
    // hot-path lookup is GetAllAsync(correlationId) — every lost-subscriber dispatch — which needs
    // all of one correlation id's registrations in O(1)+small-array, and TTL pruning is per-bucket.
    // A flat tuple key would make both O(total entries). The reference-identity Equals below is
    // what lets a prune+mutate publish atomically via TryUpdate's compare operand.
    private readonly struct EntryBucket : IEquatable<EntryBucket>
    {
        private readonly Entry? _single;
        private readonly Entry[]? _many;

        private EntryBucket(Entry? single, Entry[]? many)
        {
            _single = single;
            _many = many;
        }

        public bool IsEmpty => _single is null && _many is null;
        public Entry? SingleEntry => _single;
        public Entry[]? ManyEntries => _many;

        public static EntryBucket Single(Entry entry) => new(entry, null);

        public EntryBucket Upsert(Entry entry)
        {
            if (_single is null)
            {
                if (_many is null)
                    return Single(entry);

                for (var i = 0; i < _many.Length; i++)
                {
                    if (_many[i].State.RegistrationId != entry.State.RegistrationId)
                        continue;

                    var replaced = (Entry[])_many.Clone();
                    replaced[i] = entry;
                    return new EntryBucket(null, replaced);
                }

                var appended = new Entry[_many.Length + 1];
                Array.Copy(_many, appended, _many.Length);
                appended[^1] = entry;
                return new EntryBucket(null, appended);
            }

            if (_single.State.RegistrationId == entry.State.RegistrationId)
                return Single(entry);

            return new EntryBucket(null, [_single, entry]);
        }

        public EntryBucket PruneExpired(DateTime nowUtc)
        {
            if (_single is not null)
                return _single.ExpiresAtUtc <= nowUtc ? default : this;

            if (_many is null)
                return this;

            var liveCount = 0;
            Entry? lastLive = null;
            foreach (var entry in _many)
            {
                if (entry.ExpiresAtUtc <= nowUtc)
                    continue;

                liveCount++;
                lastLive = entry;
            }

            if (liveCount == _many.Length)
                return this;
            if (liveCount == 0)
                return default;
            if (liveCount == 1)
                return Single(lastLive!);

            var live = new Entry[liveCount];
            var index = 0;
            foreach (var entry in _many)
            {
                if (entry.ExpiresAtUtc > nowUtc)
                    live[index++] = entry;
            }

            return new EntryBucket(null, live);
        }

        public EntryBucket Remove(Guid registrationId, out bool removed)
        {
            if (_single is not null)
            {
                removed = _single.State.RegistrationId == registrationId;
                return removed ? default : this;
            }

            if (_many is null)
            {
                removed = false;
                return this;
            }

            var removeIndex = -1;
            for (var i = 0; i < _many.Length; i++)
            {
                if (_many[i].State.RegistrationId == registrationId)
                {
                    removeIndex = i;
                    break;
                }
            }

            if (removeIndex < 0)
            {
                removed = false;
                return this;
            }

            removed = true;
            if (_many.Length == 2)
                return Single(_many[removeIndex == 0 ? 1 : 0]);

            var remaining = new Entry[_many.Length - 1];
            if (removeIndex > 0)
                Array.Copy(_many, 0, remaining, 0, removeIndex);
            if (removeIndex < _many.Length - 1)
                Array.Copy(_many, removeIndex + 1, remaining, removeIndex, _many.Length - removeIndex - 1);

            return new EntryBucket(null, remaining);
        }

        public IReadOnlyList<RecoveryState> ReadableStates()
        {
            if (_single is not null)
                return RecoveryStateSchema.IsReadable(_single.State.SchemaVersion) ? [_single.State] : [];

            if (_many is null)
                return [];

            var readableCount = 0;
            foreach (var entry in _many)
            {
                if (RecoveryStateSchema.IsReadable(entry.State.SchemaVersion))
                    readableCount++;
            }

            if (readableCount == 0)
                return [];

            var states = new RecoveryState[readableCount];
            var index = 0;
            foreach (var entry in _many)
            {
                if (RecoveryStateSchema.IsReadable(entry.State.SchemaVersion))
                    states[index++] = entry.State;
            }

            return states;
        }

        public bool Equals(EntryBucket other)
            => ReferenceEquals(_single, other._single) && ReferenceEquals(_many, other._many);

        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
        public override bool Equals(object? obj)
            => obj is EntryBucket other && Equals(other);

        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
        public override int GetHashCode()
            => HashCode.Combine(
                _single is null ? 0 : RuntimeHelpers.GetHashCode(_single),
                _many is null ? 0 : RuntimeHelpers.GetHashCode(_many));
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RecoveryState> ScanAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask.ConfigureAwait(false); // process-local store: no async I/O to await

        var nowUtc = DateTime.UtcNow;
        foreach (var (correlationId, bucket) in _entries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var pruned = bucket.PruneExpired(nowUtc);
            if (pruned.IsEmpty)
            {
                TryRemove(correlationId, bucket);
                continue;
            }

            if (!pruned.Equals(bucket))
                _entries.TryUpdate(correlationId, pruned, bucket);

            if (pruned.SingleEntry is { } single)
            {
                if (RecoveryStateSchema.IsReadable(single.State.SchemaVersion))
                    yield return single.State;
                continue;
            }

            if (pruned.ManyEntries is null)
                continue;

            foreach (var entry in pruned.ManyEntries)
            {
                if (RecoveryStateSchema.IsReadable(entry.State.SchemaVersion))
                    yield return entry.State;
            }
        }
    }
}
