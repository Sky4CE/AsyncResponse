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

    private static void ValidateSubjectToken(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            return;

        if (value.IndexOfAny([' ', '\t', '*', '>', '\r', '\n']) >= 0)
            throw new InvalidOperationException(
                $"{nameof(NatsAsyncResponseTransportOptions)}.{name} '{value}' must not contain whitespace or the NATS wildcards '*'/'>'.");

        // Dots namespace a subject, but a leading, trailing or doubled '.' yields an EMPTY token
        // — a subject nats-server rejects with a non-fatal -ERR that NATS.Net never surfaces, so
        // the failure showed up as silent NoResponders at runtime rather than here (channel-options
        // parity).
        if (value.StartsWith('.') || value.EndsWith('.') || value.Contains("..", StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"{nameof(NatsAsyncResponseTransportOptions)}.{name} '{value}' must not begin or end with '.' or contain '..' (an empty NATS subject token).");
    }

    /// <summary>
    /// nats-server caps JetStream stream/consumer names at 255 characters (its subject-length
    /// default is far larger, but names derived from subjects share the cap). A longer value fails
    /// stream/consumer creation at first use — deep inside the subscriber retry loop as an opaque
    /// broker error retried forever — and derived stream names size a stack buffer from the
    /// subject, so the bound is enforced here as a named startup error.
    /// </summary>
    private const int NameLengthCap = 255;

    private static void EnsureNameLength(string? value, string name)
    {
        if (value is not null && value.Length > NameLengthCap)
            throw new InvalidOperationException(
                $"{nameof(NatsAsyncResponseTransportOptions)}.{name} resolves to {value.Length} characters; " +
                $"NATS limits subjects and JetStream stream/consumer names to {NameLengthCap} characters, " +
                "so longer values fail stream/consumer creation at first use instead of at startup.");
    }

    private static void EnsureDistinct(string left, string right, string kind, string leftName, string rightName)
    {
        if (StringComparer.Ordinal.Equals(left, right))
            throw new InvalidOperationException(
                $"{nameof(NatsAsyncResponseTransportOptions)}.{leftName} and " +
                $"{nameof(NatsAsyncResponseTransportOptions)}.{rightName} must resolve to distinct {kind}s " +
                $"(both resolve to '{left}') so worker, response, and dead-letter traffic do not consume each other's messages.");
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
        // whitespace, the NATS subject wildcards, or an empty token.
        ValidateSubjectToken(options.SubjectPrefix, nameof(options.SubjectPrefix));

        // An explicitly configured subject must satisfy the same token rules as the prefix-derived
        // defaults: whitespace or a wildcard fails stream/consumer creation at first use — deep
        // inside the subscriber retry loop as an opaque broker error retried forever — instead of
        // as a named startup error here.
        ValidateSubjectToken(options.WorkerSubject, nameof(options.WorkerSubject));
        ValidateSubjectToken(options.ResponseSubject, nameof(options.ResponseSubject));
        ValidateSubjectToken(options.DeadLetterSubject, nameof(options.DeadLetterSubject));

        // Length caps must run BEFORE the schema below resolves anything: stream defaulting sizes
        // a stack buffer from the subject, so the raw inputs are bounded before code derives from
        // them.
        EnsureNameLength(options.SubjectPrefix, nameof(options.SubjectPrefix));
        EnsureNameLength(options.WorkerSubject, nameof(options.WorkerSubject));
        EnsureNameLength(options.ResponseSubject, nameof(options.ResponseSubject));
        EnsureNameLength(options.DeadLetterSubject, nameof(options.DeadLetterSubject));
        EnsureNameLength(options.WorkerStream, nameof(options.WorkerStream));
        EnsureNameLength(options.ResponseStream, nameof(options.ResponseStream));
        EnsureNameLength(options.DeadLetterStream, nameof(options.DeadLetterStream));
        EnsureNameLength(options.WorkerConsumer, nameof(options.WorkerConsumer));
        EnsureNameLength(options.ResponseConsumer, nameof(options.ResponseConsumer));

        // Worker, response, and dead-letter traffic must never share a subject or a stream: the
        // durable consumers are unfiltered, so a shared stream feeds every role every message (and
        // a dead-letter republish landing back in the worker stream loops poison forever). Compare
        // the RESOLVED names, as the Redis sibling does: stream defaulting sanitizes every
        // non-[A-Za-z0-9-_] char to '_', so even distinct subjects ('a.b' vs 'a_b') can collide on
        // one stream — which EnsureStreamAsync would then silently repoint to whichever role ran
        // last.
        var schema = new NatsTransportSubjectSchema(options);

        // Re-check the RESOLVED names: a prefix inside the cap can still derive an over-cap
        // subject/stream once the role suffix is appended.
        EnsureNameLength(schema.WorkerSubject, nameof(options.WorkerSubject));
        EnsureNameLength(schema.ResponseSubject, nameof(options.ResponseSubject));
        EnsureNameLength(schema.DeadLetterSubject, nameof(options.DeadLetterSubject));
        EnsureNameLength(schema.WorkerStream, nameof(options.WorkerStream));
        EnsureNameLength(schema.ResponseStream, nameof(options.ResponseStream));
        EnsureNameLength(schema.DeadLetterStream, nameof(options.DeadLetterStream));

        EnsureDistinct(schema.WorkerSubject, schema.ResponseSubject, "subject", nameof(options.WorkerSubject), nameof(options.ResponseSubject));
        EnsureDistinct(schema.WorkerSubject, schema.DeadLetterSubject, "subject", nameof(options.WorkerSubject), nameof(options.DeadLetterSubject));
        EnsureDistinct(schema.ResponseSubject, schema.DeadLetterSubject, "subject", nameof(options.ResponseSubject), nameof(options.DeadLetterSubject));
        EnsureDistinct(schema.WorkerStream, schema.ResponseStream, "stream", nameof(options.WorkerStream), nameof(options.ResponseStream));
        EnsureDistinct(schema.WorkerStream, schema.DeadLetterStream, "stream", nameof(options.WorkerStream), nameof(options.DeadLetterStream));
        EnsureDistinct(schema.ResponseStream, schema.DeadLetterStream, "stream", nameof(options.ResponseStream), nameof(options.DeadLetterStream));

        // AckWait is a server-side JetStream consumer deadline carried as nanoseconds on the wire,
        // but it ALSO arms the in-process ack-extension heartbeat's Task.Delay at one third of its
        // value, so its real sink is the timer ceiling — under the persistence bound a legal
        // multi-month value passed validation and then killed every batch with
        // ArgumentOutOfRangeException from the heartbeat's delay. The retry delays arm in-process
        // Task.Delay timers too (timer ceiling).
        AsyncResponseChannelOptions.EnsureTimerBacked(options.AckWait, nameof(NatsAsyncResponseTransportOptions), nameof(options.AckWait));
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
