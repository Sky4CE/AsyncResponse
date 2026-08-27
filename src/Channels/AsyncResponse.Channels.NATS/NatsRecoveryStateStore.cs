using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

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
            // Deliberately NOT pruned by readability: a registration this build cannot INTERPRET
            // (a newer schema version written by a host mid-rolling-upgrade) must be carried
            // through untouched. Rewriting the shared envelope from the readable subset silently
            // deleted that sibling — the write path treating "unreadable" as "missing", which is
            // exactly what GetAllAsync was hardened to refuse.
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

        // The ENVELOPE itself is unreadable — a truncated or corrupt value, or a shape a newer
        // build wrote whose JSON this one cannot parse at all (so the per-registration schema check
        // below never gets to classify it). That is the same "unreadable is not missing" case the
        // per-registration branch guards, one level up: returning [] here would read as "no
        // recovery callback was ever armed" and the dispatcher would acknowledge the terminal
        // response, consuming it for a callback that never ran. Refuse so redelivery can reach a
        // build that can read it.
        if (loaded.Unreadable)
            throw new RecoveryStateUnreadableException(correlationId, 1);

        if (loaded.Stored is not { } stored)
            return [];

        var found = (Stored: stored, loaded.Revision);

        if (IsExpired(found.Stored))
        {
            // Past its logical expiry but still physically present (bucket MaxAge has not collected it
            // yet): treat as gone and remove it best-effort so it never resurfaces.
            await TryDeleteSilentlyAsync(key, found.Revision, cancellationToken).ConfigureAwait(false);
            return [];
        }

        var states = StatesFrom(found.Stored);
        var unreadable = 0;
        for (var i = states.Count - 1; i >= 0; i--)
        {
            var state = states[i];
            if (!IsStateReadable(state, key, correlationId, ref unreadable))
            {
                states.RemoveAt(i);
                continue;
            }
        }

        // Registrations existed and none this build could INTERPRET survived. An empty list reads
        // as "no recovery callback was ever armed", which the dispatcher answers by acknowledging
        // the response — so a corrupt or newer-schema registration would consume a terminal response
        // its callback never saw. A partially readable batch deliberately does not throw (see
        // RecoveryStateUnreadableException), and neither does a row rejected for carrying ANOTHER
        // correlation id: that row is readable and simply belongs elsewhere, so for the id actually
        // asked about it is absence, not corruption.
        if (unreadable > 0 && states.Count == 0)
            throw new RecoveryStateUnreadableException(correlationId, unreadable);

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
            // Same rule as SaveAsync: remove only the targeted registration, never a sibling this
            // build merely cannot read — dropping those here also let the key be deleted outright
            // when they were the only survivors.
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

            // The watchdog scan reports; it does not settle a delivery, so an unreadable envelope
            // is skipped here rather than thrown (TryDeserialize already logged it). GetAllAsync —
            // the path whose answer decides whether a terminal response is acknowledged — refuses.
            if (loaded.Stored is not { } storedState)
                continue;

            var found = (Stored: storedState, loaded.Revision);

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

    /// <summary>
    /// Tri-state load: the key is absent, or it holds an envelope this build can read, or it holds
    /// an envelope it cannot. The third case is NOT absence — see the note in <see cref="GetAllAsync"/>.
    /// </summary>
    private async Task<(StoredRecoveryState? Stored, ulong Revision, bool Unreadable)> LoadStoredAsync(string key, CancellationToken cancellationToken)
    {
        var entry = await _store.GetAsync(key, cancellationToken).ConfigureAwait(false);
        if (entry is not { } existing)
            return (null, 0, false);

        var stored = TryDeserialize(existing.Value, key);
        return stored is null ? (null, existing.Revision, true) : (stored, existing.Revision, false);
    }

    /// <summary>
    /// The package-local envelope metadata CHAINED behind the library's resolver, not used alone.
    /// A callback argument is <see cref="CallbackParam"/>.Value, typed <c>object</c>, so it
    /// serializes by runtime type — and the source generator only emitted what this envelope
    /// references transitively (string, int, Guid, DateTime). On its own the context therefore
    /// threw NotSupportedException at waiter registration for a perfectly ordinary literal
    /// (a bool, a long, an enum, a DTO), on this channel and Redis only, and bypassed
    /// AsyncResponseJsonSerialization.RegisterResolver — the documented trim/AOT seam — entirely.
    /// The wire format is unchanged: the envelope's own metadata still resolves first.
    /// </summary>
    private static readonly JsonSerializerOptions _envelopeOptions = new()
    {
        TypeInfoResolver = JsonTypeInfoResolver.Combine(NatsChannelJsonContext.Default, AsyncResponseJson.Resolver)
    };

    /// <summary>The envelope's metadata off the chained options — the JsonTypeInfo overloads keep this trim/AOT-clean.</summary>
    private static readonly JsonTypeInfo<StoredRecoveryState> _envelopeTypeInfo =
        AsyncResponseJson.GetTypeInfo<StoredRecoveryState>(_envelopeOptions);

    private static string SerializeStates(List<RecoveryState> states, DateTimeOffset expiresAtUtc)
        => JsonSerializer.Serialize(new StoredRecoveryState
        {
            States = states,
            ExpiresAtUtc = expiresAtUtc
        }, _envelopeTypeInfo);

    private bool IsExpired(StoredRecoveryState stored) => stored.ExpiresAtUtc <= _timeProvider.GetUtcNow();

    /// <summary>Readability check for paths that prune without judging a read.</summary>
    private bool IsStateReadable(RecoveryState? state, string key, string correlationId)
    {
        var ignored = 0;
        return IsStateReadable(state, key, correlationId, ref ignored);
    }

    /// <summary>
    /// Readability check that also counts rows this build could not INTERPRET. A row carrying
    /// another correlation id is deliberately NOT counted: it is readable and belongs elsewhere.
    /// </summary>
    private bool IsStateReadable(RecoveryState? state, string key, string correlationId, ref int unreadable)
    {
        if (state is null || state.RegistrationId == Guid.Empty)
        {
            _logger.LogWarning(
                "Recovery state at key {RecoveryKey} has no registration id; rejecting it because it cannot be deleted safely.",
                key);
            unreadable++;
            return false;
        }

        if (!RecoveryStateSchema.IsReadable(state.SchemaVersion))
        {
            _logger.LogWarning(
                "Recovery state at key {RecoveryKey} has unsupported schema version {SchemaVersion} (current: {Current}); rejecting it instead of risking a misinterpreted recovery.",
                key, state.SchemaVersion, RecoveryStateSchema.Current);
            unreadable++;
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
            return JsonSerializer.Deserialize(json, _envelopeTypeInfo);
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
