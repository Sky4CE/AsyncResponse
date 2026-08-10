namespace AsyncResponse.Transports.Redis;

internal static class RedisTransportOptionsValidator
{
    /// <summary>Validates the supplied options.</summary>
    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(RedisAsyncResponseTransportOptions)}.{name} must be configured.");

    /// <summary>Validates the supplied options.</summary>
    public static void PositiveOrNull(long? value, string name)
    {
        if (value is <= 0)
            throw new InvalidOperationException($"{nameof(RedisAsyncResponseTransportOptions)}.{name} must be positive when set.");
    }

    /// <summary>Validates the supplied options.</summary>
    public static void ValidateCommon(RedisAsyncResponseTransportOptions options)
    {
        _ = Required(options.KeyPrefix, nameof(options.KeyPrefix));
        _ = Required(options.WorkerConsumerGroup, nameof(options.WorkerConsumerGroup));
        _ = Required(options.ResponseConsumerGroup, nameof(options.ResponseConsumerGroup));
        _ = Required(options.CorrelationIdField, nameof(options.CorrelationIdField));
        _ = Required(options.PayloadField, nameof(options.PayloadField));
        _ = Required(options.DefaultReplyTargetName, nameof(options.DefaultReplyTargetName));

        // OperationTimeout arms a CancellationTokenSource per command; the retry delays feed
        // Task.Delay — all timer-armed, so all carry the .NET timer ceiling.
        AsyncResponseChannelOptions.EnsureTimerBacked(options.OperationTimeout, nameof(RedisAsyncResponseTransportOptions), nameof(options.OperationTimeout));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryBaseDelay, nameof(RedisAsyncResponseTransportOptions), nameof(options.PublishRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryMaxDelay, nameof(RedisAsyncResponseTransportOptions), nameof(options.PublishRetryMaxDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryBaseDelay, nameof(RedisAsyncResponseTransportOptions), nameof(options.SubscriberRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryMaxDelay, nameof(RedisAsyncResponseTransportOptions), nameof(options.SubscriberRetryMaxDelay));
        PositiveOrNull(options.StreamMaxLength, nameof(options.StreamMaxLength));
        PositiveOrNull(options.DeadLetterStreamMaxLength, nameof(options.DeadLetterStreamMaxLength));

        if (options.PublishMaxAttempts <= 0)
            throw new InvalidOperationException($"{nameof(RedisAsyncResponseTransportOptions)}.{nameof(options.PublishMaxAttempts)} must be positive.");

        if (options.PublishRetryBaseDelay > options.PublishRetryMaxDelay)
        {
            throw new InvalidOperationException(
                $"{nameof(RedisAsyncResponseTransportOptions)}.{nameof(options.PublishRetryBaseDelay)} cannot exceed " +
                $"{nameof(RedisAsyncResponseTransportOptions)}.{nameof(options.PublishRetryMaxDelay)}.");
        }

        if (options.SubscriberRetryBaseDelay > options.SubscriberRetryMaxDelay)
        {
            throw new InvalidOperationException(
                $"{nameof(RedisAsyncResponseTransportOptions)}.{nameof(options.SubscriberRetryBaseDelay)} cannot exceed " +
                $"{nameof(RedisAsyncResponseTransportOptions)}.{nameof(options.SubscriberRetryMaxDelay)}.");
        }

        if (options.HostShutdownTimeout is { } hostShutdownTimeout && hostShutdownTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(RedisAsyncResponseTransportOptions)}.{nameof(options.HostShutdownTimeout)} must be positive when set.");
    }
}
