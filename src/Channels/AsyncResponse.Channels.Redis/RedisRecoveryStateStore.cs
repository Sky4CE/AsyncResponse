using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace AsyncResponse.Channels.Redis;

/// <summary>
/// Redis-backed implementation of <see cref="IRecoveryStateStore"/>.
/// <para>
/// Every registration for a correlation id shares one key, but Redis applies a single TTL per key,
/// so each entry also carries an absolute <see cref="StoredRegistration.ExpiresAtUtc"/>: reads and
/// scans treat an entry past its expiry as absent, saves prune expired entries, and the key TTL is
/// always the longest remaining entry lifetime — a stream of fresh registrations can therefore
/// never keep a dead sibling registration recoverable (nor truncate a longer-lived one).
/// </para>
/// </summary>
internal sealed class RedisRecoveryStateStore : IRecoveryStateStore, IRecoveryStateScanner
{
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly IDatabase _database;
    private readonly RedisKeySchema _keys;
    private readonly ILogger<RedisRecoveryStateStore> _logger;
    private readonly TimeProvider _timeProvider;

    /// <summary>Creates a Redis-backed recovery state store.</summary>
    public RedisRecoveryStateStore(
        IConnectionMultiplexer multiplexer,
        IOptions<RedisAsyncResponseOptions> options,
        ILogger<RedisRecoveryStateStore> logger,
        TimeProvider? timeProvider = null)
    {
        _multiplexer = multiplexer;
        _database = multiplexer.GetDatabase();
        _keys = new RedisKeySchema(options.Value.KeyPrefix);
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

        var recoveryKey = _keys.RecoveryKey(correlationId);

        // Optimistic read-modify-write: two waiters registering the same correlation id
        // concurrently must both survive, so each write commits only while the stored value is
        // still the one we read (transaction condition), retrying on a conflict.
        for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var nowUtc = _timeProvider.GetUtcNow();
            var previous = await _database.StringGetAsync(recoveryKey).ConfigureAwait(false);
            var (entries, legacy) = previous.IsNullOrEmpty
                ? (new List<StoredRegistration>(), false)
                : DeserializeEntries(previous, recoveryKey, correlationId, logAsError: true, nowUtc, preserveUnreadable: true);
            if (legacy)
            {
                // Legacy blobs (a bare state list) carry no per-entry expiry — under that format
                // every save re-armed the whole key with a full TTL anyway, so re-stamping each
                // entry with this save's full TTL preserves that ceiling exactly once; from here
                // on the blob is enveloped and expiry is per entry.
                foreach (var entry in entries)
                    entry.ExpiresAtUtc = nowUtc + ttl;
            }

            entries.RemoveAll(existing => existing.State?.RegistrationId == state.RegistrationId);
            entries.Add(new StoredRegistration { State = state, ExpiresAtUtc = nowUtc + ttl });

            var transaction = _database.CreateTransaction();
            transaction.AddCondition(previous.IsNull
                ? Condition.KeyNotExists(recoveryKey)
                : Condition.StringEqual(recoveryKey, previous));
            // The key must outlive its longest-lived entry and no more: a fresh full TTL here
            // would re-extend every co-located registration's physical lifetime on each save.
            _ = transaction.StringSetAsync(recoveryKey, SerializeEntries(entries), MaxRemaining(entries, nowUtc));
            if (await transaction.ExecuteAsync().ConfigureAwait(false))
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

        var recoveryKey = _keys.RecoveryKey(correlationId);
        return await LoadStatesAsync(recoveryKey, correlationId).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        if (registrationId == Guid.Empty)
            throw new ArgumentException("Registration id cannot be empty.", nameof(registrationId));
        cancellationToken.ThrowIfCancellationRequested();

        var recoveryKey = _keys.RecoveryKey(correlationId);

        // Optimistic removal: deleting one registration must not clobber a registration that a
        // concurrent writer appended between our read and our write.
        for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var nowUtc = _timeProvider.GetUtcNow();
            var previous = await _database.StringGetAsync(recoveryKey).ConfigureAwait(false);
            if (previous.IsNullOrEmpty)
                return false;

            var (entries, legacy) = DeserializeEntries(previous, recoveryKey, correlationId, logAsError: true, nowUtc, preserveUnreadable: true);
            var removed = entries.RemoveAll(entry => entry.State?.RegistrationId == registrationId) > 0;
            if (!removed)
                return false;

            var transaction = _database.CreateTransaction();
            transaction.AddCondition(Condition.StringEqual(recoveryKey, previous));
            if (entries.Count == 0)
            {
                _ = transaction.KeyDeleteAsync(recoveryKey);
            }
            else if (legacy)
            {
                // Legacy blobs keep their shape and key TTL here: only SaveAsync migrates to the
                // enveloped shape, because it alone has a TTL to stamp the survivors with.
                _ = transaction.StringSetAsync(
                    recoveryKey,
                    AsyncResponseJson.Serialize(entries.ConvertAll(entry => entry.State!).FindAll(static state => state is not null)),
                    Expiration.KeepTtl,
                    ValueCondition.Always);
            }
            else
            {
                // Shrink the key to its longest surviving entry: keeping the previous TTL would
                // hold the key alive long after the removed registration — possibly the only
                // long-lived one — is gone.
                _ = transaction.StringSetAsync(recoveryKey, SerializeEntries(entries), MaxRemaining(entries, nowUtc));
            }
            if (await transaction.ExecuteAsync().ConfigureAwait(false))
                return true;
        }

        // Leave the registration for expiry rather than risking a lost concurrent registration
        // with an unconditional rewrite; the caller treats false as "nothing deleted".
        _logger.LogWarning(
            "Recovery-state delete for correlationId {CorrelationId} registration {RegistrationId} exhausted {Attempts} optimistic attempts; leaving the registration for expiry.",
            correlationId, registrationId, MaxCasAttempts);
        return false;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RecoveryState> ScanAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Primaries only. Every replica holds a copy of the same keys, so scanning them too walked
        // the keyspace once per node and produced nothing the primary had not already yielded —
        // the dedupe below hid the duplicate entries but not the round trips. It also aimed a full
        // keyspace scan at nodes that exist to serve reads cheaply. On a single-node deployment
        // this changes nothing: that node is the primary.
        var connectedServers = _multiplexer.GetEndPoints()
            .Select(endPoint => _multiplexer.GetServer(endPoint))
            .Where(server => server.IsConnected && !server.IsReplica)
            .ToList();

        var seenKeys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var server in connectedServers)
        {
            foreach (var key in server.Keys(pattern: _keys.RecoveryKeyPattern, pageSize: 250))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var recoveryKey = key.ToString();
                if (!seenKeys.Add(recoveryKey))
                    continue;

                var value = await _database.StringGetAsync(recoveryKey).ConfigureAwait(false);
                if (value.IsNullOrEmpty)
                    continue;

                var correlationId = _keys.CorrelationIdFromRecoveryKey(recoveryKey);
                var (entries, _) = DeserializeEntries(value, recoveryKey, correlationId, logAsError: false, _timeProvider.GetUtcNow());
                foreach (var entry in entries)
                    yield return entry.State!;
            }
        }
    }

    private const int MaxCasAttempts = 4;

    private async Task<List<RecoveryState>> LoadStatesAsync(string recoveryKey, string correlationId)
    {
        var value = await _database.StringGetAsync(recoveryKey).ConfigureAwait(false);
        if (value.IsNullOrEmpty)
            return [];

        var now = _timeProvider.GetUtcNow();
        var (entries, _) = DeserializeEntries(value, recoveryKey, correlationId, logAsError: true, now);
        if (entries.Count > 0)
            return entries.ConvertAll(entry => entry.State!);

        // The key held a blob and nothing readable came out of it. That must not read as "no
        // registration was ever armed" — the dispatcher acknowledges the response on that answer,
        // consuming a terminal response whose callback never ran.
        //
        // Expiry is the exception and has to be told apart here, because DeserializeEntries drops
        // lapsed entries by the same route it drops unreadable ones: a registration past its expiry
        // is legitimately gone, and failing on it would redeliver forever against a record that is
        // supposed to disappear.
        if (CountStoredRegistrations(value, out var stored) && stored > 0)
            throw new RecoveryStateUnreadableException(correlationId, stored);

        return [];
    }

    /// <summary>
    /// Counts the registrations physically present in the blob that are NOT past their expiry,
    /// without applying the readability rules. The gap between this and what
    /// <see cref="DeserializeEntries"/> returned is exactly the unreadable set. Returns
    /// <c>false</c> when the blob itself will not parse at all — in which case every registration it
    /// held is unreadable by definition, and the caller is told so via <paramref name="stored"/>.
    /// </summary>
    private bool CountStoredRegistrations(RedisValue value, out int stored)
    {
        var json = value.ToString();
        var now = _timeProvider.GetUtcNow();
        try
        {
            if (IsLegacyShape(json))
            {
                // Legacy blobs carry no per-entry expiry; every element is a live registration.
                stored = AsyncResponseJson.Deserialize<List<RecoveryState>>(json)?.Count ?? 0;
                return true;
            }

            var parsed = JsonSerializer.Deserialize(json, _envelopeTypeInfo);
            var registrations = parsed?.Registrations;
            if (registrations is null)
            {
                stored = 0;
                return true;
            }

            stored = 0;
            foreach (var entry in registrations)
            {
                if (entry is not null && entry.ExpiresAtUtc > now)
                    stored++;
            }

            return true;
        }
        catch (JsonException)
        {
            // Unparseable at the top level: the blob exists and holds an unknown number of
            // registrations, all of them unreadable. One is enough to fail the delivery.
            stored = 1;
            return true;
        }
    }

    /// <summary>
    /// The package-local envelope metadata CHAINED behind the library's resolver, not used alone.
    /// A callback argument is <see cref="CallbackParam"/>.Value, typed <c>object</c>, so it
    /// serializes by runtime type — and the source generator only emitted what this envelope
    /// references transitively (string, int, Guid, DateTime). On its own the context therefore
    /// threw NotSupportedException at waiter registration for a perfectly ordinary literal
    /// (a bool, a long, an enum, a DTO), on these two channels only, and bypassed
    /// AsyncResponseJsonSerialization.RegisterResolver — the documented trim/AOT seam — entirely.
    /// The wire format is unchanged: the envelope's own metadata still resolves first.
    /// </summary>
    private static readonly JsonSerializerOptions _envelopeOptions = new()
    {
        TypeInfoResolver = JsonTypeInfoResolver.Combine(RedisChannelJsonContext.Default, AsyncResponseJson.Resolver)
    };

    /// <summary>The envelope's metadata off the chained options — the JsonTypeInfo overloads keep this trim/AOT-clean.</summary>
    private static readonly JsonTypeInfo<StoredRecoveryState> _envelopeTypeInfo =
        AsyncResponseJson.GetTypeInfo<StoredRecoveryState>(_envelopeOptions);

    private static RedisValue SerializeEntries(List<StoredRegistration> entries)
        => JsonSerializer.Serialize(
            new StoredRecoveryState { Registrations = entries },
            _envelopeTypeInfo);

    private static TimeSpan MaxRemaining(List<StoredRegistration> entries, DateTimeOffset nowUtc)
    {
        var maxExpiresAtUtc = DateTimeOffset.MinValue;
        foreach (var entry in entries)
        {
            if (entry.ExpiresAtUtc > maxExpiresAtUtc)
                maxExpiresAtUtc = entry.ExpiresAtUtc;
        }

        return maxExpiresAtUtc - nowUtc;
    }

    /// <summary>
    /// Deserializes the stored blob. <c>preserveUnreadable</c>: write paths pass true. A read filters out an entry this build cannot INTERPRET (a newer
    /// schema version, a null state, a blank registration id), but a read-modify-write must carry
    /// it through untouched: rewriting the shared blob from the readable subset silently deleted a
    /// sibling registration written by a newer host mid-rolling-upgrade — the write path treating
    /// "unreadable" as "missing", which is exactly what the read path was hardened to refuse.
    /// Expired entries are still pruned either way; those are genuinely gone.
    /// </summary>
    private (List<StoredRegistration> Entries, bool Legacy) DeserializeEntries(
        RedisValue value,
        string recoveryKey,
        string correlationId,
        bool logAsError,
        DateTimeOffset nowUtc,
        bool preserveUnreadable = false)
    {
        var json = value.ToString();
        try
        {
            // Legacy blobs are a bare JSON array of states; enveloped blobs are an object. The
            // first significant character tells the shapes apart without a speculative parse.
            if (IsLegacyShape(json))
            {
                var states = AsyncResponseJson.Deserialize<List<RecoveryState>>(json) ?? [];
                var legacyEntries = new List<StoredRegistration>(states.Count);
                foreach (var state in states)
                {
                    // A legacy entry has no expiry of its own; it lives until the key's TTL, as it
                    // always did (SaveAsync stamps one when it rewrites the blob enveloped).
                    if (preserveUnreadable || IsStateReadable(state, recoveryKey, correlationId))
                        legacyEntries.Add(new StoredRegistration { State = state });
                }

                return (legacyEntries, true);
            }

            var stored = JsonSerializer.Deserialize(json, _envelopeTypeInfo);
            var entries = stored?.Registrations ?? [];
            entries.RemoveAll(entry => entry is null || (!preserveUnreadable && !IsStateReadable(entry.State, recoveryKey, correlationId)));
            // An entry past its per-entry expiry is logically gone even while a longer-lived
            // sibling keeps the key alive; surfacing it would fire recovery callbacks for a
            // registration that lapsed long ago.
            entries.RemoveAll(entry => entry.ExpiresAtUtc <= nowUtc);
            return (entries, false);
        }
        catch (JsonException ex)
        {
            if (logAsError)
                _logger.LogError(ex, "Failed to deserialize recovery state at {RecoveryKey}.", recoveryKey);
            else
                _logger.LogWarning(ex, "Unreadable recovery state at {RecoveryKey}; skipping.", recoveryKey);
            return ([], false);
        }
    }

    private static bool IsLegacyShape(string json)
    {
        foreach (var ch in json)
        {
            if (char.IsWhiteSpace(ch))
                continue;
            return ch == '[';
        }

        return false;
    }

    private bool IsStateReadable(RecoveryState? state, string recoveryKey, string correlationId)
    {
        if (state is null || state.RegistrationId == Guid.Empty)
        {
            _logger.LogWarning(
                "Recovery state at {RecoveryKey} has no registration id; rejecting it because it cannot be deleted safely.",
                recoveryKey);
            return false;
        }

        if (!RecoveryStateSchema.IsReadable(state.SchemaVersion))
        {
            _logger.LogWarning(
                "Recovery state at {RecoveryKey} has unsupported schema version {SchemaVersion} (current: {Current}); rejecting it instead of risking a misinterpreted recovery.",
                recoveryKey, state.SchemaVersion, RecoveryStateSchema.Current);
            return false;
        }

        if (string.Equals(state.CorrelationId, correlationId, StringComparison.Ordinal))
            return true;

        _logger.LogWarning(
            "Recovery state at {RecoveryKey} has correlationId {StoredCorrelationId}, expected {CorrelationId}; rejecting it.",
            recoveryKey, state.CorrelationId, correlationId);
        return false;
    }

    /// <summary>
    /// The stored envelope: an object (legacy blobs were a bare array, which is how the two
    /// shapes are told apart) holding every registration with its own absolute expiry.
    /// </summary>
    internal sealed class StoredRecoveryState
    {
        public List<StoredRegistration>? Registrations { get; set; }
    }

    /// <summary>One registration and the absolute expiry of the save that wrote it.</summary>
    internal sealed class StoredRegistration
    {
        public RecoveryState? State { get; set; }
        public DateTimeOffset ExpiresAtUtc { get; set; }
    }
}

/// <summary>
/// Source-generated metadata for the package-local recovery envelope (trim/AOT-safe; Metadata-mode
/// generation with default options serializes the nested <see cref="RecoveryState"/> exactly like
/// the reflection-based path did).
/// </summary>
[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(RedisRecoveryStateStore.StoredRecoveryState))]
internal sealed partial class RedisChannelJsonContext : JsonSerializerContext;
