namespace AsyncResponse.Channels.Redis;

/// <summary>
/// Options for the Redis async-response channel.
/// </summary>
public sealed class RedisAsyncResponseOptions
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

    /// <summary>
    /// How long persisted <see cref="RecoveryState"/> entries live. This bounds how long after a
    /// crash/redeploy a late response can still trigger the lost-subscriber callbacks.
    /// Set it comfortably above your longest-running flow. Default: 7 days.
    /// </summary>
    public TimeSpan RecoveryStateExpiry { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Default timeout applied to waiters that do not specify <c>WithTimeout</c>. When
    /// <c>null</c> (the default), <see cref="RecoveryStateExpiry"/> is used — once the recovery
    /// state has expired, waiting longer is meaningless. Waits are never infinite: a response
    /// that never arrives faults the waiter with a <see cref="TimeoutException"/> so the flow
    /// fails visibly instead of hanging forever.
    /// </summary>
    public TimeSpan? DefaultTimeout { get; set; }
}
