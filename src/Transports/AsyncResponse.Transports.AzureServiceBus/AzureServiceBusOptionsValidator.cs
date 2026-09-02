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

        // All of these arm timers: ReceiveWaitTime inside the SDK's receive call, the retry
        // delays via Task.Delay, and ShutdownTimeout as a CancellationTokenSource budget.
        AsyncResponseChannelOptions.EnsureTimerBacked(options.ReceiveWaitTime, nameof(AzureServiceBusAsyncResponseOptions), nameof(options.ReceiveWaitTime));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryBaseDelay, nameof(AzureServiceBusAsyncResponseOptions), nameof(options.PublishRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryMaxDelay, nameof(AzureServiceBusAsyncResponseOptions), nameof(options.PublishRetryMaxDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryBaseDelay, nameof(AzureServiceBusAsyncResponseOptions), nameof(options.SubscriberRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryMaxDelay, nameof(AzureServiceBusAsyncResponseOptions), nameof(options.SubscriberRetryMaxDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.ShutdownTimeout, nameof(AzureServiceBusAsyncResponseOptions), nameof(options.ShutdownTimeout));

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
        if (subscriberOptions.LockRenewalInterval is { } lockRenewalInterval)
            AsyncResponseChannelOptions.EnsureTimerBacked(lockRenewalInterval, optionPath, nameof(AzureServiceBusSubscriberOptions.LockRenewalInterval));

        switch (subscriberOptions.AckMode)
        {
            case AzureServiceBusAckMode.AckAfterHandlerCompletes:
                // ShutdownTimeout is spent at shutdown even without a background drain (SQS
                // parity, and what this option's own XML doc and transport-semantics.md promise
                // is validated): the lock-renewal join on the final batch, then the receiver
                // close, run SEQUENTIALLY on the stop path, so with renewal on the budget must
                // fit two of them. Returning here without summing anything let a raised
                // ShutdownTimeout overrun the host's stop budget, force-terminating mid-close and
                // redelivering handled-but-uncompleted messages whose locks then lapsed.
                if (subscriberOptions.LockRenewalInterval is not null)
                {
                    ShutdownBudgetValidator.Validate(
                        "Azure Service Bus",
                        $"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(AzureServiceBusAsyncResponseOptions.HostShutdownTimeout)}",
                        transportOptions.HostShutdownTimeout,
                        ($"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(AzureServiceBusAsyncResponseOptions.ShutdownTimeout)} (lock-renewal join)", transportOptions.ShutdownTimeout),
                        ($"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(AzureServiceBusAsyncResponseOptions.ShutdownTimeout)} (receiver close)", transportOptions.ShutdownTimeout));
                }
                else
                {
                    ShutdownBudgetValidator.Validate(
                        "Azure Service Bus",
                        $"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(AzureServiceBusAsyncResponseOptions.HostShutdownTimeout)}",
                        transportOptions.HostShutdownTimeout,
                        ($"{nameof(AzureServiceBusAsyncResponseOptions)}.{nameof(AzureServiceBusAsyncResponseOptions.ShutdownTimeout)} (receiver close)", transportOptions.ShutdownTimeout));
                }

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

                AsyncResponseChannelOptions.EnsureTimerBacked(subscriberOptions.BackgroundDrainTimeout, optionPath, nameof(AzureServiceBusSubscriberOptions.BackgroundDrainTimeout));

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
}
