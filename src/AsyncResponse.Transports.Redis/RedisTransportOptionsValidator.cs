namespace AsyncResponse.Transports.Redis;

internal static class RedisTransportOptionsValidator
{
    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(RedisAsyncResponseTransportOptions)}.{name} must be configured.");

    public static void Positive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(RedisAsyncResponseTransportOptions)}.{name} must be positive.");
    }

    public static void PositiveOrNull(long? value, string name)
    {
        if (value is <= 0)
            throw new InvalidOperationException($"{nameof(RedisAsyncResponseTransportOptions)}.{name} must be positive when set.");
    }

    public static void ValidateCommon(RedisAsyncResponseTransportOptions options)
    {
        _ = Required(options.KeyPrefix, nameof(options.KeyPrefix));
        _ = Required(options.WorkerConsumerGroup, nameof(options.WorkerConsumerGroup));
        _ = Required(options.ResponseConsumerGroup, nameof(options.ResponseConsumerGroup));
        _ = Required(options.CorrelationIdField, nameof(options.CorrelationIdField));
        _ = Required(options.PayloadField, nameof(options.PayloadField));
        _ = Required(options.DefaultReplyTargetName, nameof(options.DefaultReplyTargetName));

        Positive(options.OperationTimeout, nameof(options.OperationTimeout));
        Positive(options.PublishRetryBaseDelay, nameof(options.PublishRetryBaseDelay));
        Positive(options.PublishRetryMaxDelay, nameof(options.PublishRetryMaxDelay));
        Positive(options.SubscriberRetryBaseDelay, nameof(options.SubscriberRetryBaseDelay));
        Positive(options.SubscriberRetryMaxDelay, nameof(options.SubscriberRetryMaxDelay));
        Positive(options.ShutdownTimeout, nameof(options.ShutdownTimeout));
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
