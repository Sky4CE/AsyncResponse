namespace AsyncResponse.Transports.RabbitMQ;

internal static class RabbitMqOptionsValidator
{
    /// <summary>Validates the supplied options.</summary>
    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(RabbitMqAsyncResponseOptions)}.{name} must be configured.");

    /// <summary>
    /// Bounds the knobs copied verbatim into the RabbitMQ <c>ConnectionFactory</c>, which would
    /// otherwise fail at the FIRST connect — long after registration. AMQP 0-9-1 heartbeats are
    /// 16-bit seconds (zero disables them). The recovery interval is used directly by the
    /// client's automatic-recovery loop as a <c>Task.Delay</c>: a negative value faults (and
    /// TERMINATES) that loop and zero spins it, so the interval must be strictly positive and
    /// under the timer ceiling — there is no client-side fallback.
    /// </summary>
    public static void ValidateConnection(RabbitMqAsyncResponseOptions options)
    {
        if (options.RequestedHeartbeat < TimeSpan.Zero || options.RequestedHeartbeat > TimeSpan.FromSeconds(ushort.MaxValue))
            throw new InvalidOperationException(
                $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(options.RequestedHeartbeat)} must be between zero (disabled) and {ushort.MaxValue} seconds — AMQP heartbeats are 16-bit seconds.");

        AsyncResponseChannelOptions.EnsureTimerBacked(options.NetworkRecoveryInterval, nameof(RabbitMqAsyncResponseOptions), nameof(options.NetworkRecoveryInterval));
    }

    /// <summary>
    /// The mirrored broker <c>consumer_timeout</c> is advertised as the transport's in-flight ceiling
    /// (<see cref="IWorkerTransportInFlightLimit.MaxInFlightDuration"/>, whose contract is "positive"),
    /// and the durable-flow engine plans in-process waits inside it: zero or a negative value leaves
    /// no room for any wait. <c>null</c> (the broker's timeout is disabled) is the way to say "no
    /// ceiling".
    /// </summary>
    public static void ValidateConsumerTimeout(RabbitMqAsyncResponseOptions options)
    {
        if (options.BrokerConsumerTimeout is { } consumerTimeout && consumerTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException(
                $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(options.BrokerConsumerTimeout)} must be positive, or null when the broker's consumer_timeout is disabled.");
    }
}
