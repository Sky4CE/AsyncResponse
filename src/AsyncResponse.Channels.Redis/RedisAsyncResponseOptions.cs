namespace AsyncResponse.Channels.Redis;

/// <summary>
/// Options for the Redis async-response channel. Recovery-state expiry, default timeout, and the
/// remote stack-trace policy are inherited from <see cref="DurableAsyncResponseChannelOptions"/>.
/// </summary>
public sealed class RedisAsyncResponseOptions : DurableAsyncResponseChannelOptions
{
    /// <summary>
    /// Prefix for every Redis key and pub/sub channel created by the channel:
    /// response channels are <c>{KeyPrefix}:response:{correlationId}</c> and recovery state lives
    /// at <c>{KeyPrefix}:recovery:{correlationId}</c>. Change it to isolate multiple
    /// applications or environments sharing one Redis. Treat as a deployment-wide contract:
    /// publishers and subscribers must agree on it, and changing it orphans existing
    /// recovery state.
    /// </summary>
    public string KeyPrefix { get; set; } = "asyncresponse";
}
