using AsyncResponse.Internal;

namespace AsyncResponse.Transports.PostgreSQL;

internal static class PostgreSqlTransportOptionsValidator
{
    public static void ValidateCommon(PostgreSqlAsyncResponseTransportOptions options)
    {
        ValidateIdentifier(options.SchemaName, nameof(options.SchemaName));
        ValidateIdentifier(options.MessageTable, nameof(options.MessageTable));
        ValidateIdentifier(options.NotificationChannel, nameof(options.NotificationChannel));
        Required(options.WorkerQueue, nameof(options.WorkerQueue));
        Required(options.ResponseQueue, nameof(options.ResponseQueue));
        Required(options.DeadLetterQueue, nameof(options.DeadLetterQueue));
        Required(options.CorrelationIdHeader, nameof(options.CorrelationIdHeader));
        Required(options.DefaultReplyTargetName, nameof(options.DefaultReplyTargetName));

        // All three logical queues share one table, distinguished only by the queue column. Equal
        // names would make subscribers consume each other's rows (or re-consume dead letters).
        if (StringComparer.Ordinal.Equals(options.WorkerQueue, options.ResponseQueue)
            || StringComparer.Ordinal.Equals(options.WorkerQueue, options.DeadLetterQueue)
            || StringComparer.Ordinal.Equals(options.ResponseQueue, options.DeadLetterQueue))
        {
            throw new InvalidOperationException(
                $"{nameof(PostgreSqlAsyncResponseTransportOptions)}.{nameof(options.WorkerQueue)}, " +
                $"{nameof(options.ResponseQueue)}, and {nameof(options.DeadLetterQueue)} must be distinct; they share one queue table.");
        }

        // The queue table's derived index names reserve suffix space, but a table whose name ends
        // exactly where a reserved stem truncates can still derive its own name, silently skipping
        // index creation (indexes and tables share PostgreSQL's relation namespace).
        (string Role, string Name)[] namePlan =
        [
            ($"{nameof(options.MessageTable)} table", options.MessageTable),
            ("claim index (derived from MessageTable)", PostgreSqlTransportStore.IndexName(options.MessageTable, "claim")),
            ("created index (derived from MessageTable)", PostgreSqlTransportStore.IndexName(options.MessageTable, "created")),
        ];
        RelationalNamePlan.RequireDistinct(
            namePlan,
            nameof(PostgreSqlAsyncResponseTransportOptions),
            $"; rename {nameof(options.MessageTable)} so the derived index names stay distinct.");

        // Timer-armed knobs get the .NET timer ceiling (LockTimeout also drives the in-process
        // lease-renewal Task.Delay at half its value); DeadLetterRetention and RedeliveryDelay are
        // database-side "now + value" stamps and get the persistence bound instead.
        if (options.DeadLetterRetention is { } deadLetterRetention)
            AsyncResponseChannelOptions.EnsurePersistedTtl(deadLetterRetention, nameof(PostgreSqlAsyncResponseTransportOptions), nameof(options.DeadLetterRetention));

        AsyncResponseChannelOptions.EnsureTimerBacked(options.LockTimeout, nameof(PostgreSqlAsyncResponseTransportOptions), nameof(options.LockTimeout));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryBaseDelay, nameof(PostgreSqlAsyncResponseTransportOptions), nameof(options.PublishRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryMaxDelay, nameof(PostgreSqlAsyncResponseTransportOptions), nameof(options.PublishRetryMaxDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryBaseDelay, nameof(PostgreSqlAsyncResponseTransportOptions), nameof(options.SubscriberRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryMaxDelay, nameof(PostgreSqlAsyncResponseTransportOptions), nameof(options.SubscriberRetryMaxDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.ShutdownTimeout, nameof(PostgreSqlAsyncResponseTransportOptions), nameof(options.ShutdownTimeout));

        if (options.PublishMaxAttempts <= 0)
            throw new InvalidOperationException($"{nameof(PostgreSqlAsyncResponseTransportOptions)}.{nameof(options.PublishMaxAttempts)} must be positive.");
        if (options.PublishRetryBaseDelay > options.PublishRetryMaxDelay)
            throw new InvalidOperationException($"{nameof(PostgreSqlAsyncResponseTransportOptions)}.{nameof(options.PublishRetryBaseDelay)} cannot exceed {nameof(options.PublishRetryMaxDelay)}.");
        if (options.SubscriberRetryBaseDelay > options.SubscriberRetryMaxDelay)
            throw new InvalidOperationException($"{nameof(PostgreSqlAsyncResponseTransportOptions)}.{nameof(options.SubscriberRetryBaseDelay)} cannot exceed {nameof(options.SubscriberRetryMaxDelay)}.");
    }

    /// <summary>Validates the supplied subscriber options together with the transport-wide shutdown budget.</summary>
    public static void ValidateSubscriber(
        PostgreSqlAsyncResponseTransportOptions transportOptions,
        PostgreSqlSubscriberOptions subscriber,
        string role)
    {
        ValidateSubscriber(subscriber, role);

        if (subscriber.AckMode is not PostgreSqlAckMode.AckAfterEnqueue)
            return;

        // PostgreSQL spends the background drain plus the LISTEN-task join (ShutdownTimeout)
        // at shutdown; both must fit inside the host budget or ACKed work is truncated.
        ShutdownBudgetValidator.Validate(
            "PostgreSQL",
            $"{nameof(PostgreSqlAsyncResponseTransportOptions)}.{nameof(transportOptions.HostShutdownTimeout)}",
            transportOptions.HostShutdownTimeout,
            ($"{nameof(PostgreSqlSubscriberOptions)}.{nameof(subscriber.BackgroundDrainTimeout)} ({role})", subscriber.BackgroundDrainTimeout),
            ($"{nameof(PostgreSqlAsyncResponseTransportOptions)}.{nameof(transportOptions.ShutdownTimeout)}", transportOptions.ShutdownTimeout));
    }

    public static void ValidateSubscriber(PostgreSqlSubscriberOptions subscriber, string role)
    {
        if (subscriber.BatchSize <= 0)
            throw new InvalidOperationException($"{nameof(PostgreSqlSubscriberOptions)}.{nameof(subscriber.BatchSize)} ({role}) must be positive.");
        if (subscriber.MaxDeliveryAttempts < 0)
            throw new InvalidOperationException($"{nameof(PostgreSqlSubscriberOptions)}.{nameof(subscriber.MaxDeliveryAttempts)} ({role}) cannot be negative.");

        // RedeliveryDelay is a database-side "now + delay" visibility stamp (persistence bound);
        // EmptyPollDelay arms the idle-poll Task.Delay (timer ceiling).
        AsyncResponseChannelOptions.EnsurePersistedTtl(subscriber.RedeliveryDelay, nameof(PostgreSqlSubscriberOptions), $"{nameof(subscriber.RedeliveryDelay)} ({role})");
        AsyncResponseChannelOptions.EnsureTimerBacked(subscriber.EmptyPollDelay, nameof(PostgreSqlSubscriberOptions), $"{nameof(subscriber.EmptyPollDelay)} ({role})");

        switch (subscriber.AckMode)
        {
            case PostgreSqlAckMode.AckAfterHandlerCompletes:
                return;
            case PostgreSqlAckMode.AckAfterEnqueue:
                if (subscriber.BackgroundWorkerCount <= 0)
                    throw new InvalidOperationException($"{nameof(PostgreSqlSubscriberOptions)}.{nameof(subscriber.BackgroundWorkerCount)} ({role}) must be positive for AckAfterEnqueue.");
                if (subscriber.BackgroundQueueCapacity <= 0)
                    throw new InvalidOperationException($"{nameof(PostgreSqlSubscriberOptions)}.{nameof(subscriber.BackgroundQueueCapacity)} ({role}) must be positive for AckAfterEnqueue.");
                AsyncResponseChannelOptions.EnsureTimerBacked(subscriber.BackgroundDrainTimeout, nameof(PostgreSqlSubscriberOptions), $"{nameof(subscriber.BackgroundDrainTimeout)} ({role})");
                return;
            default:
                throw new InvalidOperationException($"{nameof(PostgreSqlSubscriberOptions)}.{nameof(subscriber.AckMode)} ({role}) has unsupported value '{subscriber.AckMode}'.");
        }
    }

    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(PostgreSqlAsyncResponseTransportOptions)}.{name} must be configured.");

    public static void ValidateIdentifier(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{nameof(PostgreSqlAsyncResponseTransportOptions)}.{name} must be configured.");
        if (!IsIdentifier(value))
            throw new InvalidOperationException(
                $"{nameof(PostgreSqlAsyncResponseTransportOptions)}.{name} '{value}' must be a simple PostgreSQL identifier (letters, digits, and underscores; not starting with a digit).");
        // PostgreSQL TRUNCATES over-limit identifiers silently (a NOTICE, not an error), so an
        // over-limit configured name would create/address an object under a different name.
        if (value.Length > 63)
            throw new InvalidOperationException(
                $"{nameof(PostgreSqlAsyncResponseTransportOptions)}.{name} '{value}' is {value.Length} characters; PostgreSQL identifiers are limited to 63 and longer names are silently truncated.");
    }

    private static bool IsIdentifier(string value)
    {
        if (value.Length == 0 || !(char.IsAsciiLetter(value[0]) || value[0] == '_'))
            return false;
        foreach (var c in value)
        {
            if (!(char.IsAsciiLetterOrDigit(c) || c == '_'))
                return false;
        }
        return true;
    }
}
