using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;

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
        if (!string.Equals(state.CorrelationId, correlationId, StringComparison.Ordinal))
            throw new ArgumentException("The recovery-state correlation id must match the store key.", nameof(state));
        if (state.SchemaVersion != RecoveryStateSchema.Current)
            throw new ArgumentException("The recovery state must use the current schema version.", nameof(state));

        cancellationToken.ThrowIfCancellationRequested();
        if (state.RegistrationId == Guid.Empty)
            state.RegistrationId = Guid.NewGuid();

        var key = NatsSubjectSchema.RecoveryKey(correlationId);

        // Revision-conditioned read-modify-write: two waiters registering the same correlation id
        // concurrently must both survive.
        for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var entry = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            var stored = entry is { } existing ? TryDeserialize(existing.Value, key) : null;
            var states = stored is not null && !IsExpired(stored)
                ? StatesFrom(stored)
                : [];
            states.RemoveAll(existingState => !IsStateReadable(existingState, key, correlationId));
            states.RemoveAll(existingState => existingState.RegistrationId == state.RegistrationId);
            states.Add(state);
            var json = SerializeStates(states, _timeProvider.GetUtcNow() + ttl);

            var written = entry is { } current
                ? await _store.TryUpdateAsync(key, json, current.Revision, cancellationToken).ConfigureAwait(false)
                : await _store.TryCreateAsync(key, json, cancellationToken).ConfigureAwait(false);
            if (written)
                return;
        }

        throw new InvalidOperationException(
            $"Recovery-state save for correlationId '{correlationId}' could not commit after {MaxCasAttempts} optimistic attempts.");
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();

        var key = NatsSubjectSchema.RecoveryKey(correlationId);
        var loaded = await LoadStoredAsync(key, cancellationToken).ConfigureAwait(false);
        if (loaded is not { } found)
            return [];

        if (IsExpired(found.Stored))
        {
            // Past its logical expiry but still physically present (bucket MaxAge has not collected it
            // yet): treat as gone and remove it best-effort so it never resurfaces.
            await TryDeleteSilentlyAsync(key, found.Revision, cancellationToken).ConfigureAwait(false);
            return [];
        }

        var states = StatesFrom(found.Stored);
        var stored = states.Count;
        for (var i = states.Count - 1; i >= 0; i--)
        {
            var state = states[i];
            if (!IsStateReadable(state, key, correlationId))
            {
                states.RemoveAt(i);
                continue;
            }
        }

        // Registrations existed and none of them survived. An empty list reads as "no recovery
        // callback was ever armed", which the dispatcher answers by acknowledging the response —
        // so a corrupt or newer-schema registration consumed a terminal response its callback never
        // saw. Fail the delivery instead; a partially readable batch deliberately does not (see
        // RecoveryStateUnreadableException).
        if (stored > 0 && states.Count == 0)
            throw new RecoveryStateUnreadableException(correlationId, stored);

        return states;
    }

    /// <inheritdoc />
    public async Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        if (registrationId == Guid.Empty)
            throw new ArgumentException("Registration id cannot be empty.", nameof(registrationId));
        cancellationToken.ThrowIfCancellationRequested();

        var key = NatsSubjectSchema.RecoveryKey(correlationId);

        // Revision-conditioned removal: deleting one registration must not clobber a registration
        // that another writer appended between our read and our write.
        for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var entry = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (entry is not { } existing)
                return false;

            var stored = TryDeserialize(existing.Value, key);
            if (stored is null)
                return false;

            if (IsExpired(stored))
            {
                await TryDeleteSilentlyAsync(key, existing.Revision, cancellationToken).ConfigureAwait(false);
                return false;
            }

            var states = StatesFrom(stored);
            states.RemoveAll(state => !IsStateReadable(state, key, correlationId));
            var removed = states.RemoveAll(state => state.RegistrationId == registrationId) > 0;
            if (!removed)
                return false;

            var succeeded = states.Count == 0
                ? await _store.TryDeleteAsync(key, existing.Revision, cancellationToken).ConfigureAwait(false)
                : await _store.TryUpdateAsync(key, SerializeStates(states, stored.ExpiresAtUtc), existing.Revision, cancellationToken).ConfigureAwait(false);
            if (succeeded)
                return true;
        }

        _logger.LogWarning(
            "Recovery-state delete for correlationId {CorrelationId} registration {RegistrationId} exhausted {Attempts} optimistic attempts; leaving the registration for expiry.",
            correlationId, registrationId, MaxCasAttempts);
        return false;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RecoveryState> ScanAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var key in _store.GetKeysAsync(cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var loaded = await LoadStoredAsync(key, cancellationToken).ConfigureAwait(false);
            if (loaded is not { } found)
                continue;

            if (IsExpired(found.Stored))
            {
                await TryDeleteSilentlyAsync(key, found.Revision, cancellationToken).ConfigureAwait(false);
                continue;
            }

            foreach (var state in StatesFrom(found.Stored))
            {
                var correlationId = NatsSubjectSchema.CorrelationIdFromRecoveryKey(key);
                if (!IsStateReadable(state, key, correlationId))
                    continue;

                yield return state;
            }
        }
    }

    private const int MaxCasAttempts = 4;

    private async Task<(StoredRecoveryState Stored, ulong Revision)?> LoadStoredAsync(string key, CancellationToken cancellationToken)
    {
        var entry = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (entry is not { } existing)
            return null;

        var stored = TryDeserialize(existing.Value, key);
        return stored is null ? null : (stored, existing.Revision);
    }

    private static string SerializeStates(List<RecoveryState> states, DateTimeOffset expiresAtUtc)
        => JsonSerializer.Serialize(new StoredRecoveryState
        {
            States = states,
            ExpiresAtUtc = expiresAtUtc
        }, NatsChannelJsonContext.Default.StoredRecoveryState);

    private bool IsExpired(StoredRecoveryState stored) => stored.ExpiresAtUtc <= _timeProvider.GetUtcNow();

    private bool IsStateReadable(RecoveryState? state, string key, string correlationId)
    {
        if (state is null || state.RegistrationId == Guid.Empty)
        {
            _logger.LogWarning(
                "Recovery state at key {RecoveryKey} has no registration id; rejecting it because it cannot be deleted safely.",
                key);
            return false;
        }

        if (!RecoveryStateSchema.IsReadable(state.SchemaVersion))
        {
            _logger.LogWarning(
                "Recovery state at key {RecoveryKey} has unsupported schema version {SchemaVersion} (current: {Current}); rejecting it instead of risking a misinterpreted recovery.",
                key, state.SchemaVersion, RecoveryStateSchema.Current);
            return false;
        }

        if (string.Equals(state.CorrelationId, correlationId, StringComparison.Ordinal))
            return true;

        _logger.LogWarning(
            "Recovery state at key {RecoveryKey} has correlationId {StoredCorrelationId}, expected {CorrelationId}; rejecting it.",
            key, state.CorrelationId, correlationId);
        return false;
    }

    private StoredRecoveryState? TryDeserialize(string json, string key)
    {
        try
        {
            return JsonSerializer.Deserialize(json, NatsChannelJsonContext.Default.StoredRecoveryState);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Unreadable recovery state at key {RecoveryKey}; skipping.", key);
            return null;
        }
    }

    private static List<RecoveryState> StatesFrom(StoredRecoveryState stored)
        => stored.States is { Count: > 0 } ? [.. stored.States] : [];

    private async Task TryDeleteSilentlyAsync(string key, ulong revision, CancellationToken cancellationToken)
    {
        try
        {
            // Revision-conditioned like every other write in this store: an unconditional delete
            // could destroy a FRESH registration a concurrent SaveAsync committed between our
            // expired read and this cleanup. On a revision conflict the delete is simply skipped —
            // the new writer owns the key now.
            await _store.TryDeleteAsync(key, revision, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Best-effort delete of expired recovery state at key {RecoveryKey} failed.", key);
        }
    }

    /// <summary>The stored envelope: the recovery state plus its absolute expiry, for per-key logical TTL.</summary>
    internal sealed class StoredRecoveryState
    {
        public List<RecoveryState>? States { get; set; }
        public DateTimeOffset ExpiresAtUtc { get; set; }
    }
}

/// <summary>
/// Source-generated metadata for the package-local KV envelope (trim/AOT-safe; the wire format is
/// unchanged — Metadata-mode generation with default options matches the previous reflection-based
/// serialization exactly).
/// </summary>
[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(NatsRecoveryStateStore.StoredRecoveryState))]
internal sealed partial class NatsChannelJsonContext : JsonSerializerContext;
