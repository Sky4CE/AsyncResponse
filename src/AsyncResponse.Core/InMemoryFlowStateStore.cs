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

        var now = DateTime.UtcNow;
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
            if (entry.ExpiresAtUtc <= DateTime.UtcNow)
            {
                _entries.TryRemove(KeyValuePair.Create(flowId, entry));
                continue;
            }

            var state = FlowStateJson.Deserialize(entry.StateJson);
            return Task.FromResult(
                state is not null
                && FlowStateSchema.IsReadable(state.SchemaVersion)
                && state.Revision == entry.Revision
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
            var now = DateTime.UtcNow;
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
            var now = DateTime.UtcNow;
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
