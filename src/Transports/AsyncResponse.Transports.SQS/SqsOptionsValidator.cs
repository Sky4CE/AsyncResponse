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

        // Every correlated publish writes this attribute name, and SQS rejects a SendMessage with
        // an invalid one — non-retryably — so a name it can never accept failed every correlated
        // publish (and every external responder told to use it) after a clean startup.
        if (!SqsWorkerTransport.IsValidMessageAttributeName(options.CorrelationIdAttribute))
        {
            throw new InvalidOperationException(
                $"{nameof(SqsAsyncResponseOptions)}.{nameof(options.CorrelationIdAttribute)} '{options.CorrelationIdAttribute}' is not a valid SQS " +
                "message-attribute name: use at most 256 ASCII letters, digits, '_', '-' and '.', with no leading, trailing or consecutive " +
                "periods, and no 'AWS.' or 'Amazon.' prefix (reserved in any casing).");
        }

        // A queue string that is not a URL goes to GetQueueUrl as a NAME. Without CreateQueues
        // nothing checked it against the SQS name rule, so a string that can never resolve — an
        // ARN, a dotted name, a stray space — passed startup: the subscriber then retried
        // GetQueueUrl forever and every publish failed.
        foreach (var (queue, optionName) in new[]
                 {
                     (options.WorkerQueue!, nameof(options.WorkerQueue)),
                     (options.ResponseQueue!, nameof(options.ResponseQueue))
                 })
        {
            if (!SqsQueueAddress.IsUrl(queue) && !SqsQueueAddress.IsValidQueueName(queue))
            {
                throw new InvalidOperationException(
                    $"{nameof(SqsAsyncResponseOptions)}.{optionName} '{queue}' is neither a queue URL nor a valid SQS queue name: " +
                    "configure the queue URL (https://…) or its name — at most 80 characters of ASCII letters, digits, '-' and '_' " +
                    "(a FIFO queue's '.fifo' suffix included in the 80). Queue ARNs are not accepted.");
            }
        }

        // Either side may be a name or a URL: two URLs compare normalized. A name against a URL
        // naming it may be another account's queue, so that only warns (PossibleQueueCollisions).
        if (SqsQueueAddress.SameQueue(options.WorkerQueue, options.ResponseQueue))
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

            // The derived dead-letter names must not collide with a LIVE queue (the guard every
            // sibling transport applies to its dead-letter destination): a redrive policy aimed at
            // the live response queue moves poison worker jobs into the ingress, where any
            // parseable JSON completes a real waiter — and provisioning would also silently
            // reconfigure the live queue's attributes.
            foreach (var queue in new[] { options.WorkerQueue!, options.ResponseQueue! })
            {
                if (SqsQueueAddress.IsUrl(queue))
                    continue;

                var deadLetterQueue = SqsQueueAddress.DeriveDeadLetterQueueName(queue, options.DeadLetterQueueSuffix!);
                if (SqsQueueAddress.SameQueue(deadLetterQueue, options.WorkerQueue)
                    || SqsQueueAddress.SameQueue(deadLetterQueue, options.ResponseQueue))
                {
                    throw new InvalidOperationException(
                        $"{nameof(SqsAsyncResponseOptions)}: the dead-letter queue derived for '{queue}' with " +
                        $"{nameof(options.DeadLetterQueueSuffix)} '{options.DeadLetterQueueSuffix}' is '{deadLetterQueue}', " +
                        "which collides with a live worker/response queue. Rename the queues or change the suffix.");
                }

                // Provisioning also creates the derived name (the queue's own name is checked
                // above for every mode), and SQS rejects a bad one with a deterministic 400 — a
                // 77-character name plus "-dlq" (81), or a suffix with a '.' in it — which
                // surfaced only after the whole provisioning retry budget, minutes into host
                // startup.
                if (!SqsQueueAddress.IsValidQueueName(deadLetterQueue))
                {
                    throw new InvalidOperationException(
                        $"{nameof(SqsAsyncResponseOptions)}: {nameof(options.CreateQueues)} would create the queue '{deadLetterQueue}'" +
                        $" (the dead-letter queue derived for '{queue}' with {nameof(options.DeadLetterQueueSuffix)} '{options.DeadLetterQueueSuffix}')" +
                        ", which SQS rejects: a queue name is at most 80 characters of ASCII letters, digits, '-' and '_' (a FIFO queue's '.fifo' suffix included in the 80).");
                }
            }
        }

        if (SqsQueueAddress.IsFifo(options.WorkerQueue))
        {
            Required(options.FifoMessageGroupIdFallback, nameof(options.FifoMessageGroupIdFallback));

            // Correlation ids go through ToMessageGroupId; the fallback is sent as it is, so a
            // value SQS rejects failed every uncorrelated FIFO publish — every durable-flow job
            // among them — with a non-retryable 400, after a clean startup.
            if (!SqsWorkerTransport.IsValidMessageGroupId(options.FifoMessageGroupIdFallback))
            {
                throw new InvalidOperationException(
                    $"{nameof(SqsAsyncResponseOptions)}.{nameof(options.FifoMessageGroupIdFallback)} '{options.FifoMessageGroupIdFallback}' is not a valid SQS " +
                    "MessageGroupId: use 1–128 ASCII letters, digits and punctuation (no spaces).");
            }
        }

        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryBaseDelay, nameof(SqsAsyncResponseOptions), nameof(options.PublishRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.PublishRetryMaxDelay, nameof(SqsAsyncResponseOptions), nameof(options.PublishRetryMaxDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryBaseDelay, nameof(SqsAsyncResponseOptions), nameof(options.SubscriberRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryMaxDelay, nameof(SqsAsyncResponseOptions), nameof(options.SubscriberRetryMaxDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.ShutdownTimeout, nameof(SqsAsyncResponseOptions), nameof(options.ShutdownTimeout));

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
                // ShutdownTimeout is spent at shutdown even without a background drain: the
                // visibility-renewal join on the final batch waits it out against a degraded
                // endpoint, exactly as its own XML doc says ("at shutdown it counts against the
                // host's budget").
                ShutdownBudgetValidator.Validate(
                    "SQS",
                    $"{nameof(SqsAsyncResponseOptions)}.{nameof(SqsAsyncResponseOptions.HostShutdownTimeout)}",
                    transportOptions.HostShutdownTimeout,
                    ($"{nameof(SqsAsyncResponseOptions)}.{nameof(SqsAsyncResponseOptions.ShutdownTimeout)}", transportOptions.ShutdownTimeout));
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

                // The receive loop stops with the host token and the SDK client needs no bounded
                // close, but shutdown spends BOTH the background drain and the visibility-renewal
                // join's ShutdownTimeout (Azure Service Bus parity) — validating only the drain let
                // a raised ShutdownTimeout blow the host budget and force-terminate mid-join,
                // redelivering handled-but-undeleted work.
                ShutdownBudgetValidator.Validate(
                    "SQS",
                    $"{nameof(SqsAsyncResponseOptions)}.{nameof(SqsAsyncResponseOptions.HostShutdownTimeout)}",
                    transportOptions.HostShutdownTimeout,
                    ($"{optionPath}.{nameof(SqsSubscriberOptions.BackgroundDrainTimeout)}", subscriberOptions.BackgroundDrainTimeout),
                    ($"{nameof(SqsAsyncResponseOptions)}.{nameof(SqsAsyncResponseOptions.ShutdownTimeout)}", transportOptions.ShutdownTimeout));

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

    /// <summary>
    /// The pairs the exact collision guards cannot decide: a queue name and a queue URL sharing its
    /// queue name (<see cref="SqsQueueAddress.MayBeSameQueue"/>), among the worker and response
    /// queues, the dead-letter queues <see cref="SqsAsyncResponseOptions.CreateQueues"/> derives,
    /// and the named reply targets. Failing on them rejected a legitimate cross-account pair — at
    /// startup, or from <c>GetReplyTarget</c> on every enqueue — so the worker subscriber warns
    /// about each at startup instead.
    /// </summary>
    public static IEnumerable<string> PossibleQueueCollisions(SqsAsyncResponseOptions options)
    {
        var workerQueue = options.WorkerQueue;
        var responseQueue = options.ResponseQueue;
        if (SqsQueueAddress.MayBeSameQueue(workerQueue, responseQueue))
            yield return $"{nameof(SqsAsyncResponseOptions.WorkerQueue)} '{workerQueue}' and {nameof(SqsAsyncResponseOptions.ResponseQueue)} '{responseQueue}'";

        string[] derivedDeadLetterQueues = string.IsNullOrWhiteSpace(options.DeadLetterQueueSuffix)
            ? []
            :
            [
                SqsQueueAddress.DeriveDeadLetterQueueName(workerQueue, options.DeadLetterQueueSuffix),
                SqsQueueAddress.DeriveDeadLetterQueueName(responseQueue, options.DeadLetterQueueSuffix)
            ];
        if (options.CreateQueues)
        {
            foreach (var deadLetterQueue in derivedDeadLetterQueues.Where(queue => !SqsQueueAddress.IsUrl(queue)))
            {
                foreach (var liveQueue in new[] { workerQueue, responseQueue })
                {
                    if (SqsQueueAddress.MayBeSameQueue(deadLetterQueue, liveQueue))
                        yield return $"the derived dead-letter queue '{deadLetterQueue}' and the live queue '{liveQueue}'";
                }
            }
        }

        foreach (var (targetName, target) in options.ReplyTargets)
        {
            if (string.IsNullOrWhiteSpace(target.Queue))
                continue;

            foreach (var guarded in derivedDeadLetterQueues.Prepend(workerQueue))
            {
                if (SqsQueueAddress.MayBeSameQueue(target.Queue, guarded))
                    yield return $"reply target '{targetName}' ('{target.Queue}') and '{guarded}' ({nameof(SqsAsyncResponseOptions.WorkerQueue)} or a derived dead-letter queue)";
            }
        }
    }

}
