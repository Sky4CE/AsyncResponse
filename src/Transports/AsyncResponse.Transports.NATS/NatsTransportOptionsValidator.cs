namespace AsyncResponse.Transports.NATS;

internal static class NatsTransportOptionsValidator
{
    /// <summary>Validates the supplied options.</summary>
    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(NatsAsyncResponseTransportOptions)}.{name} must be configured.");

    /// <summary>Validates the supplied options.</summary>
    public static void PositiveOrNull(long? value, string name)
    {
        if (value is <= 0)
            throw new InvalidOperationException($"{nameof(NatsAsyncResponseTransportOptions)}.{name} must be positive when set.");
    }

    /// <summary>Validates the supplied options.</summary>
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

        // AckWait is a server-side JetStream consumer deadline carried as nanoseconds on the wire
        // (persistence bound keeps it representable); the retry delays arm in-process Task.Delay
        // timers (timer ceiling).
        AsyncResponseChannelOptions.EnsurePersistedTtl(options.AckWait, nameof(NatsAsyncResponseTransportOptions), nameof(options.AckWait));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryBaseDelay, nameof(NatsAsyncResponseTransportOptions), nameof(options.PublishRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryMaxDelay, nameof(NatsAsyncResponseTransportOptions), nameof(options.PublishRetryMaxDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryBaseDelay, nameof(NatsAsyncResponseTransportOptions), nameof(options.SubscriberRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryMaxDelay, nameof(NatsAsyncResponseTransportOptions), nameof(options.SubscriberRetryMaxDelay));
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

    /// <summary>Validates the supplied subscriber options together with the transport-wide shutdown budget.</summary>
    public static void ValidateSubscriber(
        NatsAsyncResponseTransportOptions transportOptions,
        NatsSubscriberOptions subscriber,
        string role)
    {
        ValidateSubscriber(subscriber, role);

        if (subscriber.AckMode is not NatsAckMode.AckAfterEnqueue)
            return;

        // NATS subscribers spend only the background drain at shutdown; the consume loop stops
        // with the host token and the connection teardown is not separately bounded.
        ShutdownBudgetValidator.Validate(
            "NATS",
            $"{nameof(NatsAsyncResponseTransportOptions)}.{nameof(transportOptions.HostShutdownTimeout)}",
            transportOptions.HostShutdownTimeout,
            ($"{nameof(NatsSubscriberOptions)}.{nameof(subscriber.BackgroundDrainTimeout)} ({role})", subscriber.BackgroundDrainTimeout));
    }

    /// <summary>Validates the supplied options.</summary>
    public static void ValidateSubscriber(NatsSubscriberOptions subscriber, string role)
    {
        if (subscriber.BatchSize <= 0)
            throw new InvalidOperationException($"{nameof(NatsSubscriberOptions)}.{nameof(subscriber.BatchSize)} ({role}) must be positive.");

        if (subscriber.MaxDeliveryAttempts < 0)
            throw new InvalidOperationException($"{nameof(NatsSubscriberOptions)}.{nameof(subscriber.MaxDeliveryAttempts)} ({role}) cannot be negative.");

        // The NAK redelivery delay rides the wire as nanoseconds and is honored server-side —
        // persistence bound, not the (smaller) in-process timer ceiling.
        AsyncResponseChannelOptions.EnsurePersistedTtl(subscriber.RedeliveryDelay, nameof(NatsSubscriberOptions), $"{nameof(subscriber.RedeliveryDelay)} ({role})");

        switch (subscriber.AckMode)
        {
            case NatsAckMode.AckAfterHandlerCompletes:
                return;

            case NatsAckMode.AckAfterEnqueue:
                if (subscriber.BackgroundWorkerCount <= 0)
                    throw new InvalidOperationException($"{nameof(NatsSubscriberOptions)}.{nameof(subscriber.BackgroundWorkerCount)} ({role}) must be positive for AckAfterEnqueue.");
                if (subscriber.BackgroundQueueCapacity <= 0)
                    throw new InvalidOperationException($"{nameof(NatsSubscriberOptions)}.{nameof(subscriber.BackgroundQueueCapacity)} ({role}) must be positive for AckAfterEnqueue.");
                AsyncResponseChannelOptions.EnsureTimerBacked(subscriber.BackgroundDrainTimeout, nameof(NatsSubscriberOptions), $"{nameof(subscriber.BackgroundDrainTimeout)} ({role})");
                return;

            default:
                throw new InvalidOperationException(
                    $"{nameof(NatsSubscriberOptions)}.{nameof(subscriber.AckMode)} ({role}) has unsupported value '{subscriber.AckMode}'.");
        }
    }
}
