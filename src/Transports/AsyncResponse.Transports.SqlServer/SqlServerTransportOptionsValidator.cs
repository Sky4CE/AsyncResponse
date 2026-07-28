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

        // All three logical queues share one table, distinguished only by the queue column. Equal
        // names would make subscribers consume each other's rows (or re-consume dead letters).
        if (StringComparer.Ordinal.Equals(options.WorkerQueue, options.ResponseQueue)
            || StringComparer.Ordinal.Equals(options.WorkerQueue, options.DeadLetterQueue)
            || StringComparer.Ordinal.Equals(options.ResponseQueue, options.DeadLetterQueue))
        {
            throw new InvalidOperationException(
                $"{nameof(SqlServerAsyncResponseTransportOptions)}.{nameof(options.WorkerQueue)}, " +
                $"{nameof(options.ResponseQueue)}, and {nameof(options.DeadLetterQueue)} must be distinct; they share one queue table.");
        }

        if (options.DeadLetterRetention is { } deadLetterRetention && deadLetterRetention <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseTransportOptions)}.{nameof(options.DeadLetterRetention)} must be positive when set.");

        Positive(options.LockTimeout, nameof(options.LockTimeout));
        Positive(options.PublishRetryBaseDelay, nameof(options.PublishRetryBaseDelay));
        Positive(options.PublishRetryMaxDelay, nameof(options.PublishRetryMaxDelay));
        Positive(options.SubscriberRetryBaseDelay, nameof(options.SubscriberRetryBaseDelay));
        Positive(options.SubscriberRetryMaxDelay, nameof(options.SubscriberRetryMaxDelay));
        Positive(options.ShutdownTimeout, nameof(options.ShutdownTimeout));

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

        if (transportOptions.HostShutdownTimeout is { } hostShutdownTimeout)
        {
            if (hostShutdownTimeout <= TimeSpan.Zero)
                throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseTransportOptions)}.{nameof(transportOptions.HostShutdownTimeout)} must be positive when set.");

            var requiredShutdownBudget = transportOptions.ShutdownTimeout + subscriber.BackgroundDrainTimeout;
            if (requiredShutdownBudget > hostShutdownTimeout)
            {
                throw new InvalidOperationException(
                    $"{nameof(SqlServerSubscriberOptions)}.{nameof(subscriber.BackgroundDrainTimeout)} ({role}) plus " +
                    $"{nameof(SqlServerAsyncResponseTransportOptions)}.{nameof(transportOptions.ShutdownTimeout)} " +
                    $"requires {requiredShutdownBudget}, which exceeds " +
                    $"{nameof(SqlServerAsyncResponseTransportOptions)}.{nameof(transportOptions.HostShutdownTimeout)} " +
                    $"({hostShutdownTimeout}). Increase Microsoft.Extensions.Hosting.HostOptions.ShutdownTimeout " +
                    $"and mirror that value in {nameof(SqlServerAsyncResponseTransportOptions)}.{nameof(transportOptions.HostShutdownTimeout)}, " +
                    "or reduce the SQL Server shutdown/drain timeouts.");
            }
        }
    }

    public static void ValidateSubscriber(SqlServerSubscriberOptions subscriber, string role)
    {
        if (subscriber.BatchSize <= 0)
            throw new InvalidOperationException($"{nameof(SqlServerSubscriberOptions)}.{nameof(subscriber.BatchSize)} ({role}) must be positive.");
        if (subscriber.MaxDeliveryAttempts < 0)
            throw new InvalidOperationException($"{nameof(SqlServerSubscriberOptions)}.{nameof(subscriber.MaxDeliveryAttempts)} ({role}) cannot be negative.");

        Positive(subscriber.RedeliveryDelay, $"{nameof(subscriber.RedeliveryDelay)} ({role})");
        Positive(subscriber.EmptyPollDelay, $"{nameof(subscriber.EmptyPollDelay)} ({role})");

        switch (subscriber.AckMode)
        {
            case SqlServerAckMode.AckAfterHandlerCompletes:
                return;
            case SqlServerAckMode.AckAfterEnqueue:
                if (subscriber.BackgroundWorkerCount <= 0)
                    throw new InvalidOperationException($"{nameof(SqlServerSubscriberOptions)}.{nameof(subscriber.BackgroundWorkerCount)} ({role}) must be positive for AckAfterEnqueue.");
                if (subscriber.BackgroundQueueCapacity <= 0)
                    throw new InvalidOperationException($"{nameof(SqlServerSubscriberOptions)}.{nameof(subscriber.BackgroundQueueCapacity)} ({role}) must be positive for AckAfterEnqueue.");
                Positive(subscriber.BackgroundDrainTimeout, $"{nameof(subscriber.BackgroundDrainTimeout)} ({role})");
                return;
            default:
                throw new InvalidOperationException($"{nameof(SqlServerSubscriberOptions)}.{nameof(subscriber.AckMode)} ({role}) has unsupported value '{subscriber.AckMode}'.");
        }
    }

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
    }

    private static void Positive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseTransportOptions)}.{name} must be positive.");
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
