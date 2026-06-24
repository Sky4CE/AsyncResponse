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

        return _database.StringSetAsync(
            _keys.RecoveryKey(correlationId),
            JsonSerializer.Serialize(state),
            ttl);
    }

    public async Task<RecoveryState?> GetAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();

        var recoveryKey = _keys.RecoveryKey(correlationId);
        var value = await _database.StringGetAsync(recoveryKey).ConfigureAwait(false);
        if (value.IsNullOrEmpty)
            return null;

        try
        {
            var state = JsonSerializer.Deserialize<RecoveryState>(value.ToString());
            if (state is not null && !RecoveryStateSchema.IsReadable(state.SchemaVersion))
            {
                _logger.LogWarning(
                    "Recovery state at {RecoveryKey} has schema version {SchemaVersion}, newer than this build supports ({Current}); rejecting it instead of risking a misinterpreted recovery.",
                    recoveryKey.ToString(), state.SchemaVersion, RecoveryStateSchema.Current);
                return null;
            }

            return state;
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Failed to deserialize recovery state at {RecoveryKey}.", recoveryKey.ToString());
            return null;
        }
    }

    public Task<bool> TryDeleteAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();
        return _database.KeyDeleteAsync(_keys.RecoveryKey(correlationId));
    }

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

                RecoveryState? state;
                try
                {
                    state = JsonSerializer.Deserialize<RecoveryState>(value.ToString());
                }
                catch (JsonException ex)
                {
                    _logger.LogWarning(ex, "Unreadable recovery state at {RecoveryKey}; skipping.", recoveryKey);
                    continue;
                }

                if (state is null)
                    continue;

                if (!RecoveryStateSchema.IsReadable(state.SchemaVersion))
                {
                    _logger.LogWarning(
                        "Recovery state at {RecoveryKey} has schema version {SchemaVersion}, newer than this build supports ({Current}); skipping it during scan.",
                        recoveryKey, state.SchemaVersion, RecoveryStateSchema.Current);
                    continue;
                }

                // Older entries may predate the persisted correlation id; recover it from the key.
                if (string.IsNullOrWhiteSpace(state.CorrelationId))
                    state.CorrelationId = _keys.CorrelationIdFromRecoveryKey(recoveryKey);

                yield return state;
            }
        }
    }

}
