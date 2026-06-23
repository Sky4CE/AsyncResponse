namespace AsyncResponse.Transports.NATS;

internal static class NatsTransportOptionsValidator
{
    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(NatsAsyncResponseTransportOptions)}.{name} must be configured.");

    public static void Positive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(NatsAsyncResponseTransportOptions)}.{name} must be positive.");
    }

    public static void PositiveOrNull(long? value, string name)
    {
        if (value is <= 0)
            throw new InvalidOperationException($"{nameof(NatsAsyncResponseTransportOptions)}.{name} must be positive when set.");
    }

    public static void ValidateCommon(NatsAsyncResponseTransportOptions options)
    {
        _ = Required(options.SubjectPrefix, nameof(options.SubjectPrefix));
        _ = Required(options.WorkerConsumer, nameof(options.WorkerConsumer));
        _ = Required(options.ResponseConsumer, nameof(options.ResponseConsumer));
        _ = Required(options.CorrelationIdHeader, nameof(options.CorrelationIdHeader));
        _ = Required(options.DefaultReplyTargetName, nameof(options.DefaultReplyTargetName));

        // A subject prefix becomes leading tokens of every transport subject; it must not contain
        // whitespace or the NATS subject wildcards.
        if (options.SubjectPrefix.IndexOfAny([' ', '\t', '*', '>', '\r', '\n']) >= 0)
            throw new InvalidOperationException(
                $"{nameof(NatsAsyncResponseTransportOptions)}.{nameof(options.SubjectPrefix)} '{options.SubjectPrefix}' must not contain whitespace or the NATS wildcards '*'/'>'.");

        Positive(options.AckWait, nameof(options.AckWait));
        Positive(options.PublishRetryBaseDelay, nameof(options.PublishRetryBaseDelay));
        Positive(options.PublishRetryMaxDelay, nameof(options.PublishRetryMaxDelay));
        Positive(options.SubscriberRetryBaseDelay, nameof(options.SubscriberRetryBaseDelay));
        Positive(options.SubscriberRetryMaxDelay, nameof(options.SubscriberRetryMaxDelay));
        Positive(options.ShutdownTimeout, nameof(options.ShutdownTimeout));
        PositiveOrNull(options.StreamMaxMessages, nameof(options.StreamMaxMessages));
        PositiveOrNull(options.DeadLetterStreamMaxMessages, nameof(options.DeadLetterStreamMaxMessages));

        if (options.PublishMaxAttempts <= 0)
            throw new InvalidOperationException($"{nameof(NatsAsyncResponseTransportOptions)}.{nameof(options.PublishMaxAttempts)} must be positive.");

        if (options.PublishRetryBaseDelay > options.PublishRetryMaxDelay)
            throw new InvalidOperationException(
                $"{nameof(NatsAsyncResponseTransportOptions)}.{nameof(options.PublishRetryBaseDelay)} cannot exceed " +
                $"{nameof(NatsAsyncResponseTransportOptions)}.{nameof(options.PublishRetryMaxDelay)}.");

        if (options.SubscriberRetryBaseDelay > options.SubscriberRetryMaxDelay)
            throw new InvalidOperationException(
                $"{nameof(NatsAsyncResponseTransportOptions)}.{nameof(options.SubscriberRetryBaseDelay)} cannot exceed " +
                $"{nameof(NatsAsyncResponseTransportOptions)}.{nameof(options.SubscriberRetryMaxDelay)}.");
    }

    public static void ValidateSubscriber(NatsSubscriberOptions subscriber, string role)
    {
        if (subscriber.BatchSize <= 0)
            throw new InvalidOperationException($"{nameof(NatsSubscriberOptions)}.{nameof(subscriber.BatchSize)} ({role}) must be positive.");

        if (subscriber.MaxDeliveryAttempts < 0)
            throw new InvalidOperationException($"{nameof(NatsSubscriberOptions)}.{nameof(subscriber.MaxDeliveryAttempts)} ({role}) cannot be negative.");

        Positive(subscriber.RedeliveryDelay, $"{nameof(subscriber.RedeliveryDelay)} ({role})");

        if (subscriber.AckMode is NatsAckMode.AckAfterReceive)
        {
            if (subscriber.BackgroundWorkerCount <= 0)
                throw new InvalidOperationException($"{nameof(NatsSubscriberOptions)}.{nameof(subscriber.BackgroundWorkerCount)} ({role}) must be positive for AckAfterReceive.");
            if (subscriber.BackgroundQueueCapacity <= 0)
                throw new InvalidOperationException($"{nameof(NatsSubscriberOptions)}.{nameof(subscriber.BackgroundQueueCapacity)} ({role}) must be positive for AckAfterReceive.");
            Positive(subscriber.BackgroundDrainTimeout, $"{nameof(subscriber.BackgroundDrainTimeout)} ({role})");
        }
    }
}
