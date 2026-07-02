using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace AsyncResponse.Channels.Redis;

/// <summary>
/// Redis-backed implementation of <see cref="IRecoveryStateStore"/>.
/// </summary>
internal sealed class RedisRecoveryStateStore : IRecoveryStateStore, IRecoveryStateScanner
{
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly IDatabase _database;
    private readonly RedisKeySchema _keys;
    private readonly ILogger<RedisRecoveryStateStore> _logger;

    /// <summary>Creates a Redis-backed recovery state store.</summary>
    public RedisRecoveryStateStore(
        IConnectionMultiplexer multiplexer,
        IOptions<RedisAsyncResponseOptions> options,
        ILogger<RedisRecoveryStateStore> logger)
    {
        _multiplexer = multiplexer;
        _database = multiplexer.GetDatabase();
        _keys = new RedisKeySchema(options.Value.KeyPrefix);
        _logger = logger;
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

        var recoveryKey = _keys.RecoveryKey(correlationId);

        // Optimistic read-modify-write: two waiters registering the same correlation id
        // concurrently must both survive, so each write commits only while the stored value is
        // still the one we read (transaction condition), retrying on a conflict.
        for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var previous = await _database.StringGetAsync(recoveryKey).ConfigureAwait(false);
            var states = previous.IsNullOrEmpty
                ? []
                : DeserializeStates(previous, recoveryKey, correlationId, logAsError: true);
            states.RemoveAll(existing => existing.RegistrationId == state.RegistrationId);
            states.Add(state);

            var transaction = _database.CreateTransaction();
            transaction.AddCondition(previous.IsNull
                ? Condition.KeyNotExists(recoveryKey)
                : Condition.StringEqual(recoveryKey, previous));
            _ = transaction.StringSetAsync(recoveryKey, JsonSerializer.Serialize(states), ttl);
            if (await transaction.ExecuteAsync().ConfigureAwait(false))
                return;
        }

        // Registering recovery must not fail the wait: fall back to last-writer-wins after
        // sustained contention (pathological for a single correlation id) and say so.
        _logger.LogWarning(
            "Recovery-state save for correlationId {CorrelationId} exhausted {Attempts} optimistic attempts; falling back to an unconditional write.",
            correlationId, MaxCasAttempts);
        var fallbackStates = await LoadStatesAsync(recoveryKey, correlationId).ConfigureAwait(false);
        fallbackStates.RemoveAll(existing => existing.RegistrationId == state.RegistrationId);
        fallbackStates.Add(state);
        await _database.StringSetAsync(
            recoveryKey,
            JsonSerializer.Serialize(fallbackStates),
            ttl).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<RecoveryState?> GetAsync(string correlationId, CancellationToken cancellationToken = default)
        => (await GetAllAsync(correlationId, cancellationToken).ConfigureAwait(false)).FirstOrDefault();

    /// <inheritdoc />
    public async Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();

        var recoveryKey = _keys.RecoveryKey(correlationId);
        return await LoadStatesAsync(recoveryKey, correlationId).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<bool> TryDeleteAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();
        return _database.KeyDeleteAsync(_keys.RecoveryKey(correlationId));
    }

    /// <inheritdoc />
    public async Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();

        var recoveryKey = _keys.RecoveryKey(correlationId);

        // Optimistic removal: deleting one registration must not clobber a registration that a
        // concurrent writer appended between our read and our write.
        for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var previous = await _database.StringGetAsync(recoveryKey).ConfigureAwait(false);
            if (previous.IsNullOrEmpty)
                return false;

            var states = DeserializeStates(previous, recoveryKey, correlationId, logAsError: true);
            var removed = states.RemoveAll(state => state.RegistrationId == registrationId) > 0;
            if (!removed)
                return false;

            var transaction = _database.CreateTransaction();
            transaction.AddCondition(Condition.StringEqual(recoveryKey, previous));
            if (states.Count == 0)
                _ = transaction.KeyDeleteAsync(recoveryKey);
            else
                _ = transaction.StringSetAsync(recoveryKey, JsonSerializer.Serialize(states), Expiration.KeepTtl, ValueCondition.Always);
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
        var connectedServers = _multiplexer.GetEndPoints()
            .Select(endPoint => _multiplexer.GetServer(endPoint))
            .Where(server => server.IsConnected)
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
                foreach (var state in DeserializeStates(value, recoveryKey, correlationId, logAsError: false))
                    yield return state;
            }
        }
    }

    private const int MaxCasAttempts = 4;

    private async Task<List<RecoveryState>> LoadStatesAsync(string recoveryKey, string correlationId)
    {
        var value = await _database.StringGetAsync(recoveryKey).ConfigureAwait(false);
        return value.IsNullOrEmpty
            ? []
            : DeserializeStates(value, recoveryKey, correlationId, logAsError: true);
    }

    private List<RecoveryState> DeserializeStates(RedisValue value, string recoveryKey, string correlationId, bool logAsError)
    {
        try
        {
            using var document = JsonDocument.Parse(value.ToString());
            List<RecoveryState> states;
            if (document.RootElement.ValueKind == JsonValueKind.Array)
            {
                states = JsonSerializer.Deserialize<List<RecoveryState>>(document.RootElement.GetRawText()) ?? [];
            }
            else if (JsonSerializer.Deserialize<RecoveryState>(document.RootElement.GetRawText()) is { } state)
            {
                states = [state];
            }
            else
            {
                states = [];
            }

            for (var i = states.Count - 1; i >= 0; i--)
            {
                var state = states[i];
                if (!RecoveryStateSchema.IsReadable(state.SchemaVersion))
                {
                    _logger.LogWarning(
                        "Recovery state at {RecoveryKey} has schema version {SchemaVersion}, newer than this build supports ({Current}); rejecting it instead of risking a misinterpreted recovery.",
                        recoveryKey, state.SchemaVersion, RecoveryStateSchema.Current);
                    states.RemoveAt(i);
                    continue;
                }

                if (string.IsNullOrWhiteSpace(state.CorrelationId))
                    state.CorrelationId = correlationId;
            }

            return states;
        }
        catch (JsonException ex)
        {
            if (logAsError)
                _logger.LogError(ex, "Failed to deserialize recovery state at {RecoveryKey}.", recoveryKey);
            else
                _logger.LogWarning(ex, "Unreadable recovery state at {RecoveryKey}; skipping.", recoveryKey);
            return [];
        }
    }

}
