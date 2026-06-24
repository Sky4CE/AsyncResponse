using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace AsyncResponse.Channels.NATS;

/// <summary>
/// NATS JetStream Key-Value implementation of <see cref="IRecoveryStateStore"/> and
/// <see cref="IRecoveryStateScanner"/>.
/// <para>
/// NATS KV applies a single <c>MaxAge</c> per bucket rather than a TTL per key, so each entry also
/// carries an absolute <see cref="StoredRecoveryState.ExpiresAtUtc"/>: reads and scans treat an entry
/// past its expiry as absent (and delete it best-effort), giving precise per-correlation expiry while
/// the bucket's <c>MaxAge</c> acts as a garbage-collection ceiling for orphans.
/// </para>
/// </summary>
internal sealed class NatsRecoveryStateStore : IRecoveryStateStore, IRecoveryStateScanner
{
    private readonly INatsKvStore _store;
    private readonly ILogger<NatsRecoveryStateStore> _logger;
    private readonly TimeProvider _timeProvider;

    public NatsRecoveryStateStore(
        INatsKvStore store,
        IOptions<NatsAsyncResponseChannelOptions> options,
        ILogger<NatsRecoveryStateStore> logger,
        TimeProvider? timeProvider = null)
    {
        options.Value.Validate();
        _store = store;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

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

        var stored = new StoredRecoveryState
        {
            State = state,
            ExpiresAtUtc = _timeProvider.GetUtcNow() + ttl
        };

        return _store.PutAsync(
            NatsSubjectSchema.RecoveryKey(correlationId),
            JsonSerializer.Serialize(stored),
            cancellationToken);
    }

    public async Task<RecoveryState?> GetAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();

        var key = NatsSubjectSchema.RecoveryKey(correlationId);
        var json = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (json is null)
            return null;

        var stored = TryDeserialize(json, key);
        if (stored?.State is null)
            return null;

        if (IsExpired(stored))
        {
            // Past its logical expiry but still physically present (bucket MaxAge has not collected it
            // yet): treat as gone and remove it best-effort so it never resurfaces.
            await TryDeleteSilentlyAsync(key, cancellationToken).ConfigureAwait(false);
            return null;
        }

        if (!IsSchemaReadable(stored.State, key))
            return null;

        if (string.IsNullOrWhiteSpace(stored.State.CorrelationId))
            stored.State.CorrelationId = correlationId;

        return stored.State;
    }

    public Task<bool> TryDeleteAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();
        return _store.DeleteAsync(NatsSubjectSchema.RecoveryKey(correlationId), cancellationToken);
    }

    public async IAsyncEnumerable<RecoveryState> ScanAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var key in _store.GetKeysAsync(cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var json = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (json is null)
                continue;

            var stored = TryDeserialize(json, key);
            if (stored?.State is null)
                continue;

            if (IsExpired(stored))
            {
                await TryDeleteSilentlyAsync(key, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (!IsSchemaReadable(stored.State, key))
                continue;

            // Older entries may predate the persisted correlation id; recover it from the key.
            if (string.IsNullOrWhiteSpace(stored.State.CorrelationId))
                stored.State.CorrelationId = NatsSubjectSchema.CorrelationIdFromRecoveryKey(key);

            yield return stored.State;
        }
    }

    private bool IsExpired(StoredRecoveryState stored) => stored.ExpiresAtUtc <= _timeProvider.GetUtcNow();

    private bool IsSchemaReadable(RecoveryState state, string key)
    {
        if (RecoveryStateSchema.IsReadable(state.SchemaVersion))
            return true;

        _logger.LogWarning(
            "Recovery state at key {RecoveryKey} has schema version {SchemaVersion}, newer than this build supports ({Current}); rejecting it instead of risking a misinterpreted recovery.",
            key, state.SchemaVersion, RecoveryStateSchema.Current);
        return false;
    }

    private StoredRecoveryState? TryDeserialize(string json, string key)
    {
        try
        {
            return JsonSerializer.Deserialize<StoredRecoveryState>(json);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Unreadable recovery state at key {RecoveryKey}; skipping.", key);
            return null;
        }
    }

    private async Task TryDeleteSilentlyAsync(string key, CancellationToken cancellationToken)
    {
        try
        {
            await _store.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Best-effort delete of expired recovery state at key {RecoveryKey} failed.", key);
        }
    }

    /// <summary>The stored envelope: the recovery state plus its absolute expiry, for per-key logical TTL.</summary>
    internal sealed class StoredRecoveryState
    {
        public RecoveryState? State { get; set; }
        public DateTimeOffset ExpiresAtUtc { get; set; }
    }
}
