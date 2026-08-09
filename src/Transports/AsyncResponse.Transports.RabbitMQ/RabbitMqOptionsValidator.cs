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
}
