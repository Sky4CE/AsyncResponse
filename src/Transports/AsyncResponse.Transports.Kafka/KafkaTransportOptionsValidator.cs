namespace AsyncResponse.Transports.Kafka;

internal static class KafkaTransportOptionsValidator
{
    /// <summary>Validates the supplied options.</summary>
    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(KafkaAsyncResponseTransportOptions)}.{name} must be configured.");

    /// <summary>
    /// librdkafka's allowed range for <c>auto.commit.interval.ms</c> tops out at 86,400,000 ms;
    /// a larger value fails consumer CONSTRUCTION inside the subscriber loop, not validation.
    /// </summary>
    private static readonly TimeSpan MaxOffsetCommitInterval = TimeSpan.FromDays(1);

    /// <summary>
    /// The Kafka client passes timeouts to librdkafka as 32-bit millisecond values
    /// (<c>Flush(TimeSpan)</c>, <c>Consume(TimeSpan)</c>, admin request timeouts); anything above
    /// int.MaxValue ms (~24.8 days) overflows there mid-operation.
    /// </summary>
    internal static void EnsureIntMilliseconds(TimeSpan value, string optionsName, string name)
    {
        if (value <= TimeSpan.Zero || value.TotalMilliseconds > int.MaxValue)
            throw new InvalidOperationException(
                $"{optionsName}.{name} must be positive and at most {TimeSpan.FromMilliseconds(int.MaxValue).TotalDays:0.#} days " +
                "(the Kafka client passes it to librdkafka as a 32-bit millisecond value).");
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

        if (options.OffsetCommitInterval <= TimeSpan.Zero || options.OffsetCommitInterval > MaxOffsetCommitInterval)
            throw new InvalidOperationException(
                $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(options.OffsetCommitInterval)} must be positive and at most 1 day " +
                "(the librdkafka auto.commit.interval.ms range; larger values fail consumer construction).");
        EnsureIntMilliseconds(options.OperationTimeout, nameof(KafkaAsyncResponseTransportOptions), nameof(options.OperationTimeout));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryBaseDelay, nameof(KafkaAsyncResponseTransportOptions), nameof(options.PublishRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryMaxDelay, nameof(KafkaAsyncResponseTransportOptions), nameof(options.PublishRetryMaxDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryBaseDelay, nameof(KafkaAsyncResponseTransportOptions), nameof(options.SubscriberRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryMaxDelay, nameof(KafkaAsyncResponseTransportOptions), nameof(options.SubscriberRetryMaxDelay));

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
