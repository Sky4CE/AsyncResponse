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
    /// 16-bit seconds (zero disables them); the recovery interval arms the client's reconnect
    /// timer (non-positive values fall back to the 5-second default, so only positive values are
    /// bounded).
    /// </summary>
    public static void ValidateConnection(RabbitMqAsyncResponseOptions options)
    {
        if (options.RequestedHeartbeat < TimeSpan.Zero || options.RequestedHeartbeat > TimeSpan.FromSeconds(ushort.MaxValue))
            throw new InvalidOperationException(
                $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(options.RequestedHeartbeat)} must be between zero (disabled) and {ushort.MaxValue} seconds — AMQP heartbeats are 16-bit seconds.");

        if (options.NetworkRecoveryInterval > TimeSpan.Zero)
            AsyncResponseChannelOptions.EnsureTimerBacked(options.NetworkRecoveryInterval, nameof(RabbitMqAsyncResponseOptions), nameof(options.NetworkRecoveryInterval));
    }
}
