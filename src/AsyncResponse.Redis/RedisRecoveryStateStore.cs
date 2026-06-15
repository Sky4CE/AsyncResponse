using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Text.Json;

namespace AsyncResponse.Redis;

/// <summary>
/// Redis-backed implementation of <see cref="IRecoveryStateStore"/>.
/// </summary>
internal sealed class RedisRecoveryStateStore : IRecoveryStateStore
{
    private const string SERVICE_NAME = nameof(RedisRecoveryStateStore);

    private readonly IDatabase _database;
    private readonly RedisKeySchema _keys;
    private readonly ILogger<RedisRecoveryStateStore> _logger;

    public RedisRecoveryStateStore(
        IConnectionMultiplexer multiplexer,
        IOptions<RedisAsyncResponseOptions> options,
        ILogger<RedisRecoveryStateStore> logger)
    {
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
            return JsonSerializer.Deserialize<RecoveryState>(value.ToString());
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "{ServiceName}: Failed to deserialize recovery state at {RecoveryKey}.",
                SERVICE_NAME, recoveryKey);
            return null;
        }
    }

    public Task<bool> TryDeleteAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        cancellationToken.ThrowIfCancellationRequested();
        return _database.KeyDeleteAsync(_keys.RecoveryKey(correlationId));
    }
}
