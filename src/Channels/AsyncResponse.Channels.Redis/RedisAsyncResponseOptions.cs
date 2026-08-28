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

    /// <summary>
    /// Validates the options, throwing <see cref="InvalidOperationException"/> on a
    /// misconfiguration. Called by the channel so a bad configuration fails fast rather than at
    /// first use.
    /// </summary>
    public void Validate()
    {
        // Shared channel knobs (RecoveryStateExpiry, DefaultTimeout, DisposalDrainTimeout) go
        // through the ONE base guard set.
        ValidateShared(nameof(RedisAsyncResponseOptions));

        // Sibling-channel parity: RemoteStackTrace.Cap treats any non-positive cap as "leave the
        // trace unchanged", so a negative value (a plausible "unlimited", or a bad configuration
        // binding) silently disabled the DoS bound in both directions. The other four channels
        // reject it at startup; Redis was the only one that accepted it.
        if (MaxRemoteStackTraceLength < 0)
            throw new InvalidOperationException($"{nameof(RedisAsyncResponseOptions)}.{nameof(MaxRemoteStackTraceLength)} must not be negative.");
    }
}
