namespace AsyncResponse.Transports.MongoDB;

internal static class MongoDbTransportOptionsValidator
{
    public static void ValidateCommon(MongoDbAsyncResponseTransportOptions options)
    {
        ValidateCollectionName(options.MessageCollection, nameof(options.MessageCollection));
        Required(options.WorkerQueue, nameof(options.WorkerQueue));
        Required(options.ResponseQueue, nameof(options.ResponseQueue));
        Required(options.DeadLetterQueue, nameof(options.DeadLetterQueue));
        Required(options.CorrelationIdHeader, nameof(options.CorrelationIdHeader));
        Required(options.DefaultReplyTargetName, nameof(options.DefaultReplyTargetName));

        // All three logical queues share one collection, distinguished only by the queue field. Equal
        // names would make subscribers consume each other's documents (or re-consume dead letters).
        if (StringComparer.Ordinal.Equals(options.WorkerQueue, options.ResponseQueue)
            || StringComparer.Ordinal.Equals(options.WorkerQueue, options.DeadLetterQueue)
            || StringComparer.Ordinal.Equals(options.ResponseQueue, options.DeadLetterQueue))
        {
            throw new InvalidOperationException(
                $"{nameof(MongoDbAsyncResponseTransportOptions)}.{nameof(options.WorkerQueue)}, " +
                $"{nameof(options.ResponseQueue)}, and {nameof(options.DeadLetterQueue)} must be distinct; they share one queue collection.");
        }

        // Timer-armed knobs get the .NET timer ceiling (LockTimeout also drives the in-process
        // lease-renewal Task.Delay at half its value); DeadLetterRetention is a server-side
        // "now - retention" prune cutoff and gets the persistence bound instead.
        if (options.DeadLetterRetention is { } deadLetterRetention)
            AsyncResponseChannelOptions.EnsurePersistedTtl(deadLetterRetention, nameof(MongoDbAsyncResponseTransportOptions), nameof(options.DeadLetterRetention));

        AsyncResponseChannelOptions.EnsureTimerBacked(options.LockTimeout, nameof(MongoDbAsyncResponseTransportOptions), nameof(options.LockTimeout));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryBaseDelay, nameof(MongoDbAsyncResponseTransportOptions), nameof(options.PublishRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryMaxDelay, nameof(MongoDbAsyncResponseTransportOptions), nameof(options.PublishRetryMaxDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryBaseDelay, nameof(MongoDbAsyncResponseTransportOptions), nameof(options.SubscriberRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryMaxDelay, nameof(MongoDbAsyncResponseTransportOptions), nameof(options.SubscriberRetryMaxDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.ShutdownTimeout, nameof(MongoDbAsyncResponseTransportOptions), nameof(options.ShutdownTimeout));

        if (options.PublishMaxAttempts <= 0)
            throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseTransportOptions)}.{nameof(options.PublishMaxAttempts)} must be positive.");
        if (options.PublishRetryBaseDelay > options.PublishRetryMaxDelay)
            throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseTransportOptions)}.{nameof(options.PublishRetryBaseDelay)} cannot exceed {nameof(options.PublishRetryMaxDelay)}.");
        if (options.SubscriberRetryBaseDelay > options.SubscriberRetryMaxDelay)
            throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseTransportOptions)}.{nameof(options.SubscriberRetryBaseDelay)} cannot exceed {nameof(options.SubscriberRetryMaxDelay)}.");
    }

    /// <summary>Validates the supplied subscriber options together with the transport-wide shutdown budget.</summary>
    public static void ValidateSubscriber(
        MongoDbAsyncResponseTransportOptions transportOptions,
        MongoDbSubscriberOptions subscriber,
        string role)
    {
        ValidateSubscriber(subscriber, role);

        if (subscriber.AckMode is not MongoDbAckMode.AckAfterEnqueue)
            return;

        // MongoDB spends the background drain plus the change-stream listen-task join
        // (ShutdownTimeout) at shutdown; both must fit inside the host budget.
        ShutdownBudgetValidator.Validate(
            "MongoDB",
            $"{nameof(MongoDbAsyncResponseTransportOptions)}.{nameof(transportOptions.HostShutdownTimeout)}",
            transportOptions.HostShutdownTimeout,
            ($"{nameof(MongoDbSubscriberOptions)}.{nameof(subscriber.BackgroundDrainTimeout)} ({role})", subscriber.BackgroundDrainTimeout),
            ($"{nameof(MongoDbAsyncResponseTransportOptions)}.{nameof(transportOptions.ShutdownTimeout)}", transportOptions.ShutdownTimeout));
    }

    public static void ValidateSubscriber(MongoDbSubscriberOptions subscriber, string role)
    {
        if (subscriber.BatchSize <= 0)
            throw new InvalidOperationException($"{nameof(MongoDbSubscriberOptions)}.{nameof(subscriber.BatchSize)} ({role}) must be positive.");
        if (subscriber.MaxDeliveryAttempts < 0)
            throw new InvalidOperationException($"{nameof(MongoDbSubscriberOptions)}.{nameof(subscriber.MaxDeliveryAttempts)} ({role}) cannot be negative.");

        // RedeliveryDelay is a server-side "$$NOW + delay" visibility stamp (persistence bound);
        // EmptyPollDelay arms the idle-poll Task.Delay (timer ceiling).
        AsyncResponseChannelOptions.EnsurePersistedTtl(subscriber.RedeliveryDelay, nameof(MongoDbSubscriberOptions), $"{nameof(subscriber.RedeliveryDelay)} ({role})");
        AsyncResponseChannelOptions.EnsureTimerBacked(subscriber.EmptyPollDelay, nameof(MongoDbSubscriberOptions), $"{nameof(subscriber.EmptyPollDelay)} ({role})");

        switch (subscriber.AckMode)
        {
            case MongoDbAckMode.AckAfterHandlerCompletes:
                return;
            case MongoDbAckMode.AckAfterEnqueue:
                if (subscriber.BackgroundWorkerCount <= 0)
                    throw new InvalidOperationException($"{nameof(MongoDbSubscriberOptions)}.{nameof(subscriber.BackgroundWorkerCount)} ({role}) must be positive for AckAfterEnqueue.");
                if (subscriber.BackgroundQueueCapacity <= 0)
                    throw new InvalidOperationException($"{nameof(MongoDbSubscriberOptions)}.{nameof(subscriber.BackgroundQueueCapacity)} ({role}) must be positive for AckAfterEnqueue.");
                AsyncResponseChannelOptions.EnsureTimerBacked(subscriber.BackgroundDrainTimeout, nameof(MongoDbSubscriberOptions), $"{nameof(subscriber.BackgroundDrainTimeout)} ({role})");
                return;
            default:
                throw new InvalidOperationException($"{nameof(MongoDbSubscriberOptions)}.{nameof(subscriber.AckMode)} ({role}) has unsupported value '{subscriber.AckMode}'.");
        }
    }

    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseTransportOptions)}.{name} must be configured.");

    public static void ValidateCollectionName(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{nameof(MongoDbAsyncResponseTransportOptions)}.{name} must be configured.");
        if (value.Contains('$') || value.Contains('\0')
            || value.StartsWith("system.", StringComparison.Ordinal) || value.Contains(".system.", StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"{nameof(MongoDbAsyncResponseTransportOptions)}.{name} '{value}' must be a valid MongoDB collection name (no '$' or NUL characters, not in or containing the reserved system namespace).");
        if (string.Equals(value, AsyncResponse.Internal.MongoOwnershipLedger.CollectionName, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"{nameof(MongoDbAsyncResponseTransportOptions)}.{name} '{value}' is reserved for the cross-component ownership ledger.");
    }

}
