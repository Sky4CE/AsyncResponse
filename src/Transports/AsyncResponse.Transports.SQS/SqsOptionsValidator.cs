namespace AsyncResponse.Transports.SQS;

internal static class SqsOptionsValidator
{
    private static readonly TimeSpan MaxReceiveWaitTime = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan MaxVisibilityTimeout = TimeSpan.FromHours(12);

    public static void ValidateCommon(SqsAsyncResponseOptions options)
    {
        Required(options.WorkerQueue, nameof(options.WorkerQueue));
        Required(options.ResponseQueue, nameof(options.ResponseQueue));
        Required(options.CorrelationIdAttribute, nameof(options.CorrelationIdAttribute));
        Required(options.DefaultReplyTargetName, nameof(options.DefaultReplyTargetName));

        if (StringComparer.Ordinal.Equals(options.WorkerQueue, options.ResponseQueue))
        {
            throw new InvalidOperationException(
                $"{nameof(SqsAsyncResponseOptions)}.{nameof(options.WorkerQueue)} and " +
                $"{nameof(options.ResponseQueue)} must be distinct so worker and response subscribers do not consume each other's messages.");
        }

        if (options.MaxMessagesPerReceive is < 1 or > 10)
        {
            throw new InvalidOperationException(
                $"{nameof(SqsAsyncResponseOptions)}.{nameof(options.MaxMessagesPerReceive)} must be between 1 and 10 (the SQS ReceiveMessage limit).");
        }

        if (options.ReceiveWaitTime < TimeSpan.Zero || options.ReceiveWaitTime > MaxReceiveWaitTime)
        {
            throw new InvalidOperationException(
                $"{nameof(SqsAsyncResponseOptions)}.{nameof(options.ReceiveWaitTime)} must be between 0 and 20 seconds (the SQS long-poll limit).");
        }

        if (options.PublishMaxAttempts <= 0)
            throw new InvalidOperationException($"{nameof(SqsAsyncResponseOptions)}.{nameof(options.PublishMaxAttempts)} must be positive.");

        if (options.CreateQueues)
        {
            Required(options.DeadLetterQueueSuffix, nameof(options.DeadLetterQueueSuffix));
            if (options.MaxReceiveCount is < 1 or > 1000)
            {
                throw new InvalidOperationException(
                    $"{nameof(SqsAsyncResponseOptions)}.{nameof(options.MaxReceiveCount)} must be between 1 and 1000 (the SQS redrive policy limit).");
            }
        }

        if (SqsQueueAddress.IsFifo(options.WorkerQueue))
            Required(options.FifoMessageGroupIdFallback, nameof(options.FifoMessageGroupIdFallback));

        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryBaseDelay, nameof(SqsAsyncResponseOptions), nameof(options.PublishRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryMaxDelay, nameof(SqsAsyncResponseOptions), nameof(options.PublishRetryMaxDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryBaseDelay, nameof(SqsAsyncResponseOptions), nameof(options.SubscriberRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryMaxDelay, nameof(SqsAsyncResponseOptions), nameof(options.SubscriberRetryMaxDelay));

        if (options.PublishRetryBaseDelay > options.PublishRetryMaxDelay)
            throw new InvalidOperationException($"{nameof(SqsAsyncResponseOptions)}.{nameof(options.PublishRetryBaseDelay)} cannot exceed {nameof(options.PublishRetryMaxDelay)}.");
        if (options.SubscriberRetryBaseDelay > options.SubscriberRetryMaxDelay)
            throw new InvalidOperationException($"{nameof(SqsAsyncResponseOptions)}.{nameof(options.SubscriberRetryBaseDelay)} cannot exceed {nameof(options.SubscriberRetryMaxDelay)}.");
    }

    public static void ValidateSubscriber(
        SqsAsyncResponseOptions transportOptions,
        SqsSubscriberOptions subscriberOptions,
        SqsSubscriberRole role)
    {
        var optionPath = role is SqsSubscriberRole.Worker
            ? $"{nameof(SqsAsyncResponseOptions)}.{nameof(SqsAsyncResponseOptions.WorkerSubscriber)}"
            : $"{nameof(SqsAsyncResponseOptions)}.{nameof(SqsAsyncResponseOptions.ResponseSubscriber)}";

        if (subscriberOptions.VisibilityTimeout is { } visibilityTimeout
            && (visibilityTimeout <= TimeSpan.Zero || visibilityTimeout > MaxVisibilityTimeout))
        {
            throw new InvalidOperationException(
                $"{optionPath}.{nameof(SqsSubscriberOptions.VisibilityTimeout)} must be positive and at most 12 hours (the SQS limit).");
        }

        if (subscriberOptions.RedeliveryDelay is { } redeliveryDelay
            && (redeliveryDelay < TimeSpan.Zero || redeliveryDelay > MaxVisibilityTimeout))
        {
            throw new InvalidOperationException(
                $"{optionPath}.{nameof(SqsSubscriberOptions.RedeliveryDelay)} must be between zero and 12 hours (the SQS visibility limit).");
        }

        if (subscriberOptions.VisibilityRenewalInterval is { } renewalInterval)
        {
            // The renewal heartbeat arms Task.Delay, so the interval carries the timer ceiling
            // (in practice the shorter-than-visibility rule below is far tighter).
            AsyncResponseChannelOptions.EnsureTimerBacked(renewalInterval, optionPath, nameof(SqsSubscriberOptions.VisibilityRenewalInterval));

            if (subscriberOptions.VisibilityTimeout is not { } renewedVisibility)
            {
                throw new InvalidOperationException(
                    $"{optionPath}.{nameof(SqsSubscriberOptions.VisibilityRenewalInterval)} requires " +
                    $"{nameof(SqsSubscriberOptions.VisibilityTimeout)} so the heartbeat knows how far to extend each message.");
            }

            if (renewalInterval >= renewedVisibility)
            {
                throw new InvalidOperationException(
                    $"{optionPath}.{nameof(SqsSubscriberOptions.VisibilityRenewalInterval)} must be shorter than " +
                    $"{nameof(SqsSubscriberOptions.VisibilityTimeout)}, or messages become visible between heartbeats.");
            }
        }

        switch (subscriberOptions.AckMode)
        {
            case SqsAckMode.AckAfterHandlerCompletes:
                return;

            case SqsAckMode.AckAfterEnqueue:
                if (subscriberOptions.BackgroundWorkerCount <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(SqsSubscriberOptions.BackgroundWorkerCount)} must be explicitly configured " +
                        $"when {nameof(SqsSubscriberOptions.AckMode)} is {nameof(SqsAckMode.AckAfterEnqueue)}.");
                }

                if (subscriberOptions.BackgroundQueueCapacity <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(SqsSubscriberOptions.BackgroundQueueCapacity)} must be explicitly configured " +
                        $"when {nameof(SqsSubscriberOptions.AckMode)} is {nameof(SqsAckMode.AckAfterEnqueue)}.");
                }

                AsyncResponseChannelOptions.EnsureTimerBacked(subscriberOptions.BackgroundDrainTimeout, optionPath, nameof(SqsSubscriberOptions.BackgroundDrainTimeout));

                // SQS subscribers spend only the background drain at shutdown; the receive loop
                // stops with the host token and the SDK client needs no bounded close.
                ShutdownBudgetValidator.Validate(
                    "SQS",
                    $"{nameof(SqsAsyncResponseOptions)}.{nameof(SqsAsyncResponseOptions.HostShutdownTimeout)}",
                    transportOptions.HostShutdownTimeout,
                    ($"{optionPath}.{nameof(SqsSubscriberOptions.BackgroundDrainTimeout)}", subscriberOptions.BackgroundDrainTimeout));

                return;

            default:
                throw new InvalidOperationException(
                    $"{optionPath}.{nameof(SqsSubscriberOptions.AckMode)} has unsupported value '{subscriberOptions.AckMode}'.");
        }
    }

    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(SqsAsyncResponseOptions)}.{name} must be configured.");

}
