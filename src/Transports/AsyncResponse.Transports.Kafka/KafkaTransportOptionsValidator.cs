namespace AsyncResponse.Transports.Kafka;

internal static class KafkaTransportOptionsValidator
{
    /// <summary>Validates the supplied options.</summary>
    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(KafkaAsyncResponseTransportOptions)}.{name} must be configured.");

    /// <summary>Validates the supplied options.</summary>
    public static void Positive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(KafkaAsyncResponseTransportOptions)}.{name} must be positive.");
    }

    /// <summary>Validates the supplied options.</summary>
    public static void ValidateCommon(KafkaAsyncResponseTransportOptions options)
    {
        _ = Required(options.BootstrapServers, nameof(options.BootstrapServers));
        _ = Required(options.TopicPrefix, nameof(options.TopicPrefix));
        _ = Required(options.WorkerConsumerGroup, nameof(options.WorkerConsumerGroup));
        _ = Required(options.ResponseConsumerGroup, nameof(options.ResponseConsumerGroup));
        _ = Required(options.CorrelationIdHeader, nameof(options.CorrelationIdHeader));
        _ = Required(options.DefaultReplyTargetName, nameof(options.DefaultReplyTargetName));

        if (options.DeadLetterEnabled && string.IsNullOrWhiteSpace(options.DeadLetterTopic))
            _ = Required(options.DeadLetterTopicSuffix, nameof(options.DeadLetterTopicSuffix));

        Positive(options.OffsetCommitInterval, nameof(options.OffsetCommitInterval));
        Positive(options.OperationTimeout, nameof(options.OperationTimeout));
        Positive(options.PublishRetryBaseDelay, nameof(options.PublishRetryBaseDelay));
        Positive(options.PublishRetryMaxDelay, nameof(options.PublishRetryMaxDelay));
        Positive(options.SubscriberRetryBaseDelay, nameof(options.SubscriberRetryBaseDelay));
        Positive(options.SubscriberRetryMaxDelay, nameof(options.SubscriberRetryMaxDelay));
        Positive(options.ShutdownTimeout, nameof(options.ShutdownTimeout));

        if (options.PublishMaxAttempts <= 0)
            throw new InvalidOperationException($"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(options.PublishMaxAttempts)} must be positive.");

        if (options.PublishRetryBaseDelay > options.PublishRetryMaxDelay)
        {
            throw new InvalidOperationException(
                $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(options.PublishRetryBaseDelay)} cannot exceed " +
                $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(options.PublishRetryMaxDelay)}.");
        }

        if (options.SubscriberRetryBaseDelay > options.SubscriberRetryMaxDelay)
        {
            throw new InvalidOperationException(
                $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(options.SubscriberRetryBaseDelay)} cannot exceed " +
                $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(options.SubscriberRetryMaxDelay)}.");
        }

        if (options.TopicNumPartitions is not -1 and <= 0)
            throw new InvalidOperationException($"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(options.TopicNumPartitions)} must be positive or -1 (broker default).");

        if (options.TopicReplicationFactor is not (-1) and <= 0)
            throw new InvalidOperationException($"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(options.TopicReplicationFactor)} must be positive or -1 (broker default).");

        if (options.HostShutdownTimeout is { } hostShutdownTimeout && hostShutdownTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(options.HostShutdownTimeout)} must be positive when set.");
    }
}
