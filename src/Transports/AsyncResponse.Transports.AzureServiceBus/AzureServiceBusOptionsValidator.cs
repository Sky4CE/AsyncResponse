namespace AsyncResponse.Transports.AzureServiceBus;

internal static class AzureServiceBusOptionsValidator
{
    public static void ValidateCommon(AzureServiceBusAsyncResponseOptions options)
    {
        Required(options.WorkerQueue, nameof(options.WorkerQueue));
        Required(options.ResponseQueue, nameof(options.ResponseQueue));
        Required(options.CorrelationIdProperty, nameof(options.CorrelationIdProperty));
        Required(options.DefaultReplyTargetName, nameof(options.DefaultReplyTargetName));

        if (StringComparer.Ordinal.Equals(options.WorkerQueue, options.ResponseQueue))
        {
            throw new InvalidOperationException(
                $"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(options.WorkerQueue)} and " +
                $"{nameof(options.ResponseQueue)} must be distinct so worker and response subscribers do not consume each other's messages.");
        }

        if (options.MaxMessagesPerReceive <= 0)
            throw new InvalidOperationException($"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(options.MaxMessagesPerReceive)} must be positive.");
        if (options.PublishMaxAttempts <= 0)
            throw new InvalidOperationException($"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(options.PublishMaxAttempts)} must be positive.");

        Positive(options.ReceiveWaitTime, nameof(options.ReceiveWaitTime));
        Positive(options.PublishRetryBaseDelay, nameof(options.PublishRetryBaseDelay));
        Positive(options.PublishRetryMaxDelay, nameof(options.PublishRetryMaxDelay));
        Positive(options.SubscriberRetryBaseDelay, nameof(options.SubscriberRetryBaseDelay));
        Positive(options.SubscriberRetryMaxDelay, nameof(options.SubscriberRetryMaxDelay));
        Positive(options.ShutdownTimeout, nameof(options.ShutdownTimeout));

        if (options.PublishRetryBaseDelay > options.PublishRetryMaxDelay)
            throw new InvalidOperationException($"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(options.PublishRetryBaseDelay)} cannot exceed {nameof(options.PublishRetryMaxDelay)}.");
        if (options.SubscriberRetryBaseDelay > options.SubscriberRetryMaxDelay)
            throw new InvalidOperationException($"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(options.SubscriberRetryBaseDelay)} cannot exceed {nameof(options.SubscriberRetryMaxDelay)}.");
    }

    public static void ValidateSubscriber(
        AzureServiceBusAsyncResponseOptions transportOptions,
        AzureServiceBusSubscriberOptions subscriberOptions,
        AzureServiceBusSubscriberRole role)
    {
        var optionPath = role is AzureServiceBusSubscriberRole.Worker
            ? $"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(AzureServiceBusAsyncResponseOptions.WorkerSubscriber)}"
            : $"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(AzureServiceBusAsyncResponseOptions.ResponseSubscriber)}";

        if (subscriberOptions.MaxDeliveryAttempts < 0)
            throw new InvalidOperationException($"{optionPath}.{nameof(AzureServiceBusSubscriberOptions.MaxDeliveryAttempts)} cannot be negative.");
        if (subscriberOptions.PrefetchCount < 0)
            throw new InvalidOperationException($"{optionPath}.{nameof(AzureServiceBusSubscriberOptions.PrefetchCount)} cannot be negative.");
        if (subscriberOptions.LockRenewalInterval is { } lockRenewalInterval && lockRenewalInterval <= TimeSpan.Zero)
            throw new InvalidOperationException($"{optionPath}.{nameof(AzureServiceBusSubscriberOptions.LockRenewalInterval)} must be positive when set.");

        switch (subscriberOptions.AckMode)
        {
            case AzureServiceBusAckMode.AckAfterHandlerCompletes:
                return;

            case AzureServiceBusAckMode.AckAfterEnqueue:
                if (subscriberOptions.BackgroundWorkerCount <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(AzureServiceBusSubscriberOptions.BackgroundWorkerCount)} must be explicitly configured " +
                        $"when {nameof(AzureServiceBusSubscriberOptions.AckMode)} is {nameof(AzureServiceBusAckMode.AckAfterEnqueue)}.");
                }

                if (subscriberOptions.BackgroundQueueCapacity <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(AzureServiceBusSubscriberOptions.BackgroundQueueCapacity)} must be explicitly configured " +
                        $"when {nameof(AzureServiceBusSubscriberOptions.AckMode)} is {nameof(AzureServiceBusAckMode.AckAfterEnqueue)}.");
                }

                if (subscriberOptions.BackgroundDrainTimeout <= TimeSpan.Zero)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(AzureServiceBusSubscriberOptions.BackgroundDrainTimeout)} must be positive.");
                }

                // Service Bus spends the background drain plus receiver close / renewal-task join
                // (ShutdownTimeout) at shutdown; both must fit inside the host budget.
                ShutdownBudgetValidator.Validate(
                    "Azure Service Bus",
                    $"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(AzureServiceBusAsyncResponseOptions.HostShutdownTimeout)}",
                    transportOptions.HostShutdownTimeout,
                    ($"{optionPath}.{nameof(AzureServiceBusSubscriberOptions.BackgroundDrainTimeout)}", subscriberOptions.BackgroundDrainTimeout),
                    ($"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(AzureServiceBusAsyncResponseOptions.ShutdownTimeout)}", transportOptions.ShutdownTimeout));

                return;

            default:
                throw new InvalidOperationException(
                    $"{optionPath}.{nameof(AzureServiceBusSubscriberOptions.AckMode)} has unsupported value '{subscriberOptions.AckMode}'.");
        }
    }

    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(AzureServiceBusAsyncResponseOptions)}.{name} must be configured.");

    private static void Positive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(AzureServiceBusAsyncResponseOptions)}.{name} must be positive.");
    }
}
