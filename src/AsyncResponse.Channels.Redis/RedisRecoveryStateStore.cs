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
        var states = await LoadStatesAsync(recoveryKey, correlationId).ConfigureAwait(false);
        states.RemoveAll(existing => existing.RegistrationId == state.RegistrationId);
        states.Add(state);

        // ponytail: read-modify-write can lose a genuinely concurrent same-cid registration; use
        // a Lua append/remove script if this becomes a measured production race.
        await _database.StringSetAsync(
            recoveryKey,
            JsonSerializer.Serialize(states),
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
        var states = await LoadStatesAsync(recoveryKey, correlationId).ConfigureAwait(false);
        if (states.Count == 0)
            return false;

        var removed = states.RemoveAll(state => state.RegistrationId == registrationId) > 0;
        if (!removed)
            return false;

        if (states.Count == 0)
            return await _database.KeyDeleteAsync(recoveryKey).ConfigureAwait(false);

        await _database.StringSetAsync(
            recoveryKey,
            JsonSerializer.Serialize(states),
            Expiration.KeepTtl,
            ValueCondition.Always).ConfigureAwait(false);
        return true;
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
