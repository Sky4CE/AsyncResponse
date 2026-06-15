using StackExchange.Redis;

namespace AsyncResponse.Channels.Redis;

/// <summary>
/// The single source of truth for Redis key/channel shapes, shared by the channel and the
/// watchdog. Key shapes are a storage contract: changing them orphans in-flight recovery state.
/// </summary>
internal sealed class RedisKeySchema(string _keyPrefix)
{
    public string RecoveryKeyPattern => $"{_keyPrefix}:recovery:*";

    public RedisChannel Channel(string correlationId)
        => new($"{_keyPrefix}:response:{correlationId}", RedisChannel.PatternMode.Literal);

    public string RecoveryKey(string correlationId) => $"{_keyPrefix}:recovery:{correlationId}";

    public string CorrelationIdFromRecoveryKey(string recoveryKey)
        => recoveryKey[$"{_keyPrefix}:recovery:".Length..];
}
