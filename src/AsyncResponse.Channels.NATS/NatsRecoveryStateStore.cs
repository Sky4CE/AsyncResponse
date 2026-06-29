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

    /// <summary>Creates a NATS JetStream Key-Value recovery state store.</summary>
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

    /// <inheritdoc />
    public async Task SaveAsync(
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

        var key = NatsSubjectSchema.RecoveryKey(correlationId);
        var stored = await LoadStoredAsync(key, cancellationToken).ConfigureAwait(false);
        var states = stored is not null && !IsExpired(stored)
            ? StatesFrom(stored)
            : [];
        states.RemoveAll(existing => existing.RegistrationId == state.RegistrationId);
        states.Add(state);

        await PutStatesAsync(key, states, _timeProvider.GetUtcNow() + ttl, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<RecoveryState?> GetAsync(string correlationId, CancellationToken cancellationToken = default)
        => (await GetAllAsync(correlationId, cancellationToken).ConfigureAwait(false)).FirstOrDefault();

    /// <inheritdoc />
    public async Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();

        var key = NatsSubjectSchema.RecoveryKey(correlationId);
        var stored = await LoadStoredAsync(key, cancellationToken).ConfigureAwait(false);
        if (stored is null)
            return [];

        if (IsExpired(stored))
        {
            // Past its logical expiry but still physically present (bucket MaxAge has not collected it
            // yet): treat as gone and remove it best-effort so it never resurfaces.
            await TryDeleteSilentlyAsync(key, cancellationToken).ConfigureAwait(false);
            return [];
        }

        var states = StatesFrom(stored);
        for (var i = states.Count - 1; i >= 0; i--)
        {
            var state = states[i];
            if (!IsSchemaReadable(state, key))
            {
                states.RemoveAt(i);
                continue;
            }

            if (string.IsNullOrWhiteSpace(state.CorrelationId))
                state.CorrelationId = correlationId;
        }

        return states;
    }

    /// <inheritdoc />
    public Task<bool> TryDeleteAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();
        return _store.DeleteAsync(NatsSubjectSchema.RecoveryKey(correlationId), cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();

        var key = NatsSubjectSchema.RecoveryKey(correlationId);
        var stored = await LoadStoredAsync(key, cancellationToken).ConfigureAwait(false);
        if (stored is null)
            return false;

        if (IsExpired(stored))
        {
            await TryDeleteSilentlyAsync(key, cancellationToken).ConfigureAwait(false);
            return false;
        }

        var states = StatesFrom(stored);
        var removed = states.RemoveAll(state => state.RegistrationId == registrationId) > 0;
        if (!removed)
            return false;

        if (states.Count == 0)
            return await _store.DeleteAsync(key, cancellationToken).ConfigureAwait(false);

        await PutStatesAsync(key, states, stored.ExpiresAtUtc, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RecoveryState> ScanAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var key in _store.GetKeysAsync(cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var stored = await LoadStoredAsync(key, cancellationToken).ConfigureAwait(false);
            if (stored is null)
                continue;

            if (IsExpired(stored))
            {
                await TryDeleteSilentlyAsync(key, cancellationToken).ConfigureAwait(false);
                continue;
            }

            foreach (var state in StatesFrom(stored))
            {
                if (!IsSchemaReadable(state, key))
                    continue;

                // Older entries may predate the persisted correlation id; recover it from the key.
                if (string.IsNullOrWhiteSpace(state.CorrelationId))
                    state.CorrelationId = NatsSubjectSchema.CorrelationIdFromRecoveryKey(key);

                yield return state;
            }
        }
    }

    private async Task<StoredRecoveryState?> LoadStoredAsync(string key, CancellationToken cancellationToken)
    {
        var json = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
        return json is null ? null : TryDeserialize(json, key);
    }

    private Task PutStatesAsync(
        string key,
        List<RecoveryState> states,
        DateTimeOffset expiresAtUtc,
        CancellationToken cancellationToken)
    {
        var stored = new StoredRecoveryState
        {
            State = states[0],
            States = states,
            ExpiresAtUtc = expiresAtUtc
        };

        return _store.PutAsync(
            key,
            JsonSerializer.Serialize(stored),
            cancellationToken);
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

    private static List<RecoveryState> StatesFrom(StoredRecoveryState stored)
        => stored.States is { Count: > 0 }
            ? [.. stored.States]
            : stored.State is null ? [] : [stored.State];

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
        public List<RecoveryState>? States { get; set; }
        public DateTimeOffset ExpiresAtUtc { get; set; }
    }
}
