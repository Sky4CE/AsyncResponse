using System.Collections.Concurrent;

namespace AsyncResponse;

/// <summary>Atomic process-local flow-state store for development, tests, and single-process apps.</summary>
internal sealed class InMemoryFlowStateStore : IFlowStateStore
{
    // Saturating expiry stamp: the ttl parameter arrives from callers as well as options, and the
    // external stores deliberately saturate the same arithmetic — a raw Add threw
    // ArgumentOutOfRangeException on large ttls where every other store clamped.
    private static DateTime Expiry(DateTime now, TimeSpan ttl)
        => ttl >= DateTime.MaxValue - now ? DateTime.MaxValue : now.Add(ttl);

    private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private readonly TimeProvider _timeProvider;

    /// <summary>Creates the store; expiry and lease stamps come from the engine's clock.</summary>
    public InMemoryFlowStateStore(TimeProvider? timeProvider = null)
        => _timeProvider = timeProvider ?? TimeProvider.System;

    public Task<bool> TryCreateAsync(
        string flowId,
        FlowState state,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        ValidateWrite(flowId, state, ttl);
        cancellationToken.ThrowIfCancellationRequested();
        if (state.Revision != 0)
            throw new ArgumentException("A new flow ledger must start at revision zero.", nameof(state));

        var now = _timeProvider.GetUtcNow().UtcDateTime;
        var created = CreateEntry(state, Expiry(now, ttl));
        while (true)
        {
            if (_entries.TryAdd(flowId, created))
                return Task.FromResult(true);

            if (!_entries.TryGetValue(flowId, out var existing))
                continue;

            if (existing.ExpiresAtUtc > now)
                return Task.FromResult(false);

            if (_entries.TryUpdate(flowId, created, existing))
                return Task.FromResult(true);
        }
    }

    public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        cancellationToken.ThrowIfCancellationRequested();

        while (_entries.TryGetValue(flowId, out var entry))
        {
            if (entry.ExpiresAtUtc <= _timeProvider.GetUtcNow().UtcDateTime)
            {
                _entries.TryRemove(KeyValuePair.Create(flowId, entry));
                continue;
            }

            // Unreadable JSON or an unknown schema version throws out of here rather than
            // masquerading as a deleted flow; revision/identity mismatch still reads as absent.
            // Same contract as the durable stores — see DurableFlowStoreShared.ReadState.
            var state = FlowStateJson.Deserialize(entry.StateJson, flowId);
            return Task.FromResult(
                state.Revision == entry.Revision
                && string.Equals(state.FlowId, flowId, StringComparison.Ordinal)
                    ? state
                    : null);
        }

        return Task.FromResult<FlowState?>(null);
    }

    public Task<bool> TryUpdateAsync(
        string flowId,
        FlowState state,
        long expectedRevision,
        TimeSpan ttl,
        string? leaseId = null,
        CancellationToken cancellationToken = default)
    {
        ValidateWrite(flowId, state, ttl);
        cancellationToken.ThrowIfCancellationRequested();
        if (expectedRevision < 0)
            throw new ArgumentOutOfRangeException(nameof(expectedRevision), "Expected revision cannot be negative.");
        if (state.Revision != checked(expectedRevision + 1))
            throw new ArgumentException("The new flow-state revision must increment the expected revision by one.", nameof(state));

        while (_entries.TryGetValue(flowId, out var current))
        {
            var now = _timeProvider.GetUtcNow().UtcDateTime;
            if (current.ExpiresAtUtc <= now || current.Revision != expectedRevision)
                return Task.FromResult(false);
            if (leaseId is not null
                && (!string.Equals(current.LeaseId, leaseId, StringComparison.Ordinal)
                    || current.LeaseExpiresAtUtc <= now))
                return Task.FromResult(false);

            var updated = CreateEntry(
                state,
                Expiry(now, ttl),
                current.LeaseId,
                current.LeaseExpiresAtUtc);
            if (_entries.TryUpdate(flowId, updated, current))
                return Task.FromResult(true);
        }

        return Task.FromResult(false);
    }

    public Task<bool> TryAcquireLeaseAsync(
        string flowId,
        string leaseId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
        => TryChangeLeaseAsync(flowId, leaseId, leaseDuration, acquire: true, cancellationToken);

    public Task<bool> TryRenewLeaseAsync(
        string flowId,
        string leaseId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
        => TryChangeLeaseAsync(flowId, leaseId, leaseDuration, acquire: false, cancellationToken);

    public Task ReleaseLeaseAsync(
        string flowId,
        string leaseId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseId);
        cancellationToken.ThrowIfCancellationRequested();

        while (_entries.TryGetValue(flowId, out var current))
        {
            if (!string.Equals(current.LeaseId, leaseId, StringComparison.Ordinal))
                break;

            if (_entries.TryUpdate(flowId, current with { LeaseId = null, LeaseExpiresAtUtc = null }, current))
                break;
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Breaks every held execution lease — the test harness's crash semantics for a simulated
    /// restart. A crashed process goes silent and its leases expire; a simulated restart shares
    /// the virtual clock with the "crashed" incarnation, whose parked executions would otherwise
    /// keep renewing against this shared store forever and the new incarnation could never take
    /// their flows over. Breaking the lease makes the zombie's next renewal fail (its lease loop
    /// marks itself lost and stops) and lets the new incarnation acquire immediately.
    /// </summary>
    internal void ExpireAllLeases()
    {
        foreach (var flowId in _entries.Keys)
        {
            // CAS loop: a zombie renewal can swap the entry between the read and the update, and
            // TryUpdate compares against the snapshot — a silently lost break would recreate the
            // exact "executing on another live worker" hang this method exists to eliminate.
            // Retry until no lease is observed; once cleared, the zombie's next renewal fails
            // (its lease id no longer matches) and its loop stops, so this converges.
            while (_entries.TryGetValue(flowId, out var entry)
                   && entry.LeaseId is not null
                   && !_entries.TryUpdate(flowId, entry with { LeaseId = null, LeaseExpiresAtUtc = null }, entry))
            {
            }
        }
    }

    public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_entries.TryRemove(flowId, out _));
    }

    private Task<bool> TryChangeLeaseAsync(
        string flowId,
        string leaseId,
        TimeSpan leaseDuration,
        bool acquire,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseId);
        if (leaseDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(leaseDuration), "Lease duration must be greater than zero.");
        cancellationToken.ThrowIfCancellationRequested();

        while (_entries.TryGetValue(flowId, out var current))
        {
            var now = _timeProvider.GetUtcNow().UtcDateTime;
            if (current.ExpiresAtUtc <= now)
                return Task.FromResult(false);

            var ownsLease = string.Equals(current.LeaseId, leaseId, StringComparison.Ordinal);
            if (acquire ? current.LeaseId is not null && current.LeaseExpiresAtUtc > now && !ownsLease : !ownsLease || current.LeaseExpiresAtUtc <= now)
                return Task.FromResult(false);

            var updated = current with
            {
                LeaseId = leaseId,
                LeaseExpiresAtUtc = now.Add(leaseDuration)
            };
            if (_entries.TryUpdate(flowId, updated, current))
                return Task.FromResult(true);
        }

        return Task.FromResult(false);
    }

    private static Entry CreateEntry(
        FlowState state,
        DateTime expiresAtUtc,
        string? leaseId = null,
        DateTime? leaseExpiresAtUtc = null)
        => new(FlowStateJson.Serialize(state), state.Revision, expiresAtUtc, leaseId, leaseExpiresAtUtc);

    private static void ValidateWrite(string flowId, FlowState state, TimeSpan ttl)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentNullException.ThrowIfNull(state);
        if (!string.Equals(state.FlowId, flowId, StringComparison.Ordinal))
            throw new ArgumentException("The flow state id must match the store key.", nameof(state));
        if (state.SchemaVersion != FlowStateSchema.Current)
            throw new ArgumentException("The flow state must use the current schema version.", nameof(state));
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl), "TTL must be greater than zero.");
    }

    private sealed record Entry(
        string StateJson,
        long Revision,
        DateTime ExpiresAtUtc,
        string? LeaseId = null,
        DateTime? LeaseExpiresAtUtc = null);
}
