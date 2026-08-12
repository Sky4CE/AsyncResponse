namespace AsyncResponse.Transports.SqlServer;

internal static class SqlServerTransportOptionsValidator
{
    public static void ValidateCommon(SqlServerAsyncResponseTransportOptions options)
    {
        Required(options.ConnectionString, nameof(options.ConnectionString));
        ValidateIdentifier(options.SchemaName, nameof(options.SchemaName));
        ValidateIdentifier(options.MessageTable, nameof(options.MessageTable));
        Required(options.WorkerQueue, nameof(options.WorkerQueue));
        Required(options.ResponseQueue, nameof(options.ResponseQueue));
        Required(options.DeadLetterQueue, nameof(options.DeadLetterQueue));
        Required(options.CorrelationIdHeader, nameof(options.CorrelationIdHeader));
        Required(options.DefaultReplyTargetName, nameof(options.DefaultReplyTargetName));

        // The three logical queues share one table, distinguished only by the queue column, so
        // names that the DATABASE considers equal are as dangerous as identical ones. SQL Server
        // pads the shorter operand of an `=` comparison with spaces before comparing — even under
        // a binary collation — so 'worker' and 'worker ' match each other's rows and both
        // subscribers would consume both queues. Ordinal distinctness alone does not see that.
        ValidateQueueName(options.WorkerQueue, nameof(options.WorkerQueue));
        ValidateQueueName(options.ResponseQueue, nameof(options.ResponseQueue));
        ValidateQueueName(options.DeadLetterQueue, nameof(options.DeadLetterQueue));

        if (StringComparer.Ordinal.Equals(options.WorkerQueue, options.ResponseQueue)
            || StringComparer.Ordinal.Equals(options.WorkerQueue, options.DeadLetterQueue)
            || StringComparer.Ordinal.Equals(options.ResponseQueue, options.DeadLetterQueue))
        {
            throw new InvalidOperationException(
                $"{nameof(SqlServerAsyncResponseTransportOptions)}.{nameof(options.WorkerQueue)}, " +
                $"{nameof(options.ResponseQueue)}, and {nameof(options.DeadLetterQueue)} must be distinct; they share one queue table.");
        }

        // Timer-armed knobs get the .NET timer ceiling (LockTimeout also drives the in-process
        // lease-renewal Task.Delay at half its value); DeadLetterRetention is a database-side
        // "now - retention" prune cutoff and gets the persistence bound instead.
        if (options.DeadLetterRetention is { } deadLetterRetention)
            AsyncResponseChannelOptions.EnsurePersistedTtl(deadLetterRetention, nameof(SqlServerAsyncResponseTransportOptions), nameof(options.DeadLetterRetention));

        AsyncResponseChannelOptions.EnsureTimerBacked(options.LockTimeout, nameof(SqlServerAsyncResponseTransportOptions), nameof(options.LockTimeout));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryBaseDelay, nameof(SqlServerAsyncResponseTransportOptions), nameof(options.PublishRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryMaxDelay, nameof(SqlServerAsyncResponseTransportOptions), nameof(options.PublishRetryMaxDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryBaseDelay, nameof(SqlServerAsyncResponseTransportOptions), nameof(options.SubscriberRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryMaxDelay, nameof(SqlServerAsyncResponseTransportOptions), nameof(options.SubscriberRetryMaxDelay));

        if (options.PublishMaxAttempts <= 0)
            throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseTransportOptions)}.{nameof(options.PublishMaxAttempts)} must be positive.");
        if (options.PublishRetryBaseDelay > options.PublishRetryMaxDelay)
            throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseTransportOptions)}.{nameof(options.PublishRetryBaseDelay)} cannot exceed {nameof(options.PublishRetryMaxDelay)}.");
        if (options.SubscriberRetryBaseDelay > options.SubscriberRetryMaxDelay)
            throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseTransportOptions)}.{nameof(options.SubscriberRetryBaseDelay)} cannot exceed {nameof(options.SubscriberRetryMaxDelay)}.");
    }

    /// <summary>Validates the supplied subscriber options together with the transport-wide shutdown budget.</summary>
    public static void ValidateSubscriber(
        SqlServerAsyncResponseTransportOptions transportOptions,
        SqlServerSubscriberOptions subscriber,
        string role)
    {
        ValidateSubscriber(subscriber, role);

        if (subscriber.AckMode is not SqlServerAckMode.AckAfterEnqueue)
            return;

        // SQL Server subscribers spend only the background drain at shutdown; the polling loop
        // stops with the host token and holds no connection that needs a bounded close.
        ShutdownBudgetValidator.Validate(
            "SQL Server",
            $"{nameof(SqlServerAsyncResponseTransportOptions)}.{nameof(transportOptions.HostShutdownTimeout)}",
            transportOptions.HostShutdownTimeout,
            ($"{nameof(SqlServerSubscriberOptions)}.{nameof(subscriber.BackgroundDrainTimeout)} ({role})", subscriber.BackgroundDrainTimeout));
    }

    public static void ValidateSubscriber(SqlServerSubscriberOptions subscriber, string role)
    {
        if (subscriber.BatchSize <= 0)
            throw new InvalidOperationException($"{nameof(SqlServerSubscriberOptions)}.{nameof(subscriber.BatchSize)} ({role}) must be positive.");
        if (subscriber.MaxDeliveryAttempts < 0)
            throw new InvalidOperationException($"{nameof(SqlServerSubscriberOptions)}.{nameof(subscriber.MaxDeliveryAttempts)} ({role}) cannot be negative.");

        // RedeliveryDelay is a database-side "now + delay" visibility stamp (persistence bound);
        // EmptyPollDelay arms the idle-poll Task.Delay (timer ceiling).
        AsyncResponseChannelOptions.EnsurePersistedTtl(subscriber.RedeliveryDelay, nameof(SqlServerSubscriberOptions), $"{nameof(subscriber.RedeliveryDelay)} ({role})");
        AsyncResponseChannelOptions.EnsureTimerBacked(subscriber.EmptyPollDelay, nameof(SqlServerSubscriberOptions), $"{nameof(subscriber.EmptyPollDelay)} ({role})");

        switch (subscriber.AckMode)
        {
            case SqlServerAckMode.AckAfterHandlerCompletes:
                return;
            case SqlServerAckMode.AckAfterEnqueue:
                if (subscriber.BackgroundWorkerCount <= 0)
                    throw new InvalidOperationException($"{nameof(SqlServerSubscriberOptions)}.{nameof(subscriber.BackgroundWorkerCount)} ({role}) must be positive for AckAfterEnqueue.");
                if (subscriber.BackgroundQueueCapacity <= 0)
                    throw new InvalidOperationException($"{nameof(SqlServerSubscriberOptions)}.{nameof(subscriber.BackgroundQueueCapacity)} ({role}) must be positive for AckAfterEnqueue.");
                AsyncResponseChannelOptions.EnsureTimerBacked(subscriber.BackgroundDrainTimeout, nameof(SqlServerSubscriberOptions), $"{nameof(subscriber.BackgroundDrainTimeout)} ({role})");
                return;
            default:
                throw new InvalidOperationException($"{nameof(SqlServerSubscriberOptions)}.{nameof(subscriber.AckMode)} ({role}) has unsupported value '{subscriber.AckMode}'.");
        }
    }

    /// <summary>
    /// A queue name is a database KEY, not free text: it is stored in <c>nvarchar(200)</c> and
    /// matched with <c>=</c>, whose space padding makes trailing blanks invisible to the database
    /// while the library still treats the names as distinct. Leading blanks are rejected on the
    /// same principle — they survive comparison but make two names indistinguishable in logs.
    /// </summary>
    public static void ValidateQueueName(string value, string name)
    {
        if (value.Length > MaxQueueNameLength)
        {
            throw new InvalidOperationException(
                $"{nameof(SqlServerAsyncResponseTransportOptions)}.{name} is {value.Length} characters; the queue column is " +
                $"nvarchar({MaxQueueNameLength}), so a longer name is truncated or rejected at the first publish.");
        }

        if (value[0] == ' ' || value[^1] == ' ')
        {
            throw new InvalidOperationException(
                $"{nameof(SqlServerAsyncResponseTransportOptions)}.{name} '{value}' begins or ends with a space. SQL Server pads " +
                "the shorter operand of an equality comparison, so a trailing space is invisible to the queue lookup — two names " +
                "differing only in trailing spaces would consume each other's messages. Remove the surrounding spaces.");
        }
    }

    /// <summary>The queue column's width: <c>queue nvarchar(200)</c> in the transport's DDL.</summary>
    public const int MaxQueueNameLength = 200;

    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseTransportOptions)}.{name} must be configured.");

    public static void ValidateIdentifier(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseTransportOptions)}.{name} must be configured.");
        if (!IsIdentifier(value))
            throw new InvalidOperationException(
                $"{nameof(SqlServerAsyncResponseTransportOptions)}.{name} '{value}' must be a simple SQL Server identifier (letters, digits, and underscores; not starting with a digit).");
        // sysname caps identifiers at 128; an over-limit name fails at DDL time with a raw
        // "identifier too long" error instead of an actionable configuration error.
        if (value.Length > 128)
            throw new InvalidOperationException(
                $"{nameof(SqlServerAsyncResponseTransportOptions)}.{name} '{value}' is {value.Length} characters; SQL Server identifiers are limited to 128.");
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
