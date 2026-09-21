using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System.Collections;
using System.Diagnostics;
using System.Threading.Channels;

namespace AsyncResponse.Transports.RabbitMQ;

internal enum RabbitMqSubscriberRole
{
    Worker,
    ResponseIngress
}

internal abstract class RabbitMqMessageDispatcher : IAsyncDisposable
{
    private readonly Func<RabbitMqDelivery, CancellationToken, Task> _handler;
    private readonly RabbitMqSubscriberOptions _subscriberOptions;
    private readonly string _queue;
    private readonly RabbitMqSubscriberRole _role;

    /// <summary>Runs the RabbitMqMessageDispatcher operation.</summary>
    protected RabbitMqMessageDispatcher(
        Func<RabbitMqDelivery, CancellationToken, Task> handler,
        RabbitMqAsyncResponseOptions transportOptions,
        RabbitMqSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        RabbitMqSubscriberRole role)
    {
        _handler = handler;
        TransportOptions = transportOptions;
        _subscriberOptions = subscriberOptions;
        Logger = logger;
        _queue = queue;
        _role = role;
    }

    protected RabbitMqAsyncResponseOptions TransportOptions { get; }
    protected ILogger Logger { get; }
    protected string QueueName => _queue;

    /// <summary>
    /// Maximum delivery attempts before a failing <see cref="RabbitMqAckMode.AckAfterHandlerCompletes"/> handler
    /// rejects without requeue. <c>0</c> means unlimited (requeue forever).
    /// </summary>
    protected int MaxDeliveryAttempts => _subscriberOptions.MaxDeliveryAttempts;

    /// <summary>
    /// Resolves the 1-based delivery attempt for a message from the broker's <c>x-death</c> count and the
    /// <c>redelivered</c> flag. A message seen for the first time is attempt 1.
    /// </summary>
    internal static int ResolveDeliveryAttempt(RabbitMqDelivery delivery)
    {
        var priorAttempts = Math.Max(ReadDeathCount(delivery.BasicProperties), delivery.Redelivered ? 1L : 0L);

        // Saturate BEFORE the +1. x-death is a message header, so any publisher can forge it: a count
        // of long.MaxValue wrapped to long.MinValue on the increment, slipped under the int.MaxValue
        // range check and cast to attempt 0 — a message below every cap forever, never parked.
        return priorAttempts >= int.MaxValue ? int.MaxValue : (int)(priorAttempts + 1);
    }

    /// <summary>
    /// The cap this delivery can actually be judged against. A plain <c>basic.nack</c> requeue does
    /// NOT increment <c>x-death</c>, so without a dead-letter hop the resolved attempt saturates at
    /// 2 — and a configured cap above 2 was therefore never reachable: <c>attempt &lt; cap</c> stayed
    /// true on every redelivery and the message requeued forever, exactly as if the cap were 0.
    /// That is strictly worse than the documented "behaves like 2", which is what this restores:
    /// once the broker has actually dead-lettered the message at least once (x-death present), its
    /// attempts are countable and the operator's full cap applies.
    /// </summary>
    internal int EffectiveDeliveryCap(RabbitMqDelivery delivery)
        => MaxDeliveryAttempts > 2 && ReadDeathCount(delivery.BasicProperties) == 0
            ? 2
            : MaxDeliveryAttempts;

    protected static long ReadDeathCount(IReadOnlyBasicProperties properties)
    {
        if (properties.Headers is null
            || !properties.Headers.TryGetValue("x-death", out var raw)
            || raw is not IEnumerable entries)
        {
            return 0;
        }

        long max = 0;
        foreach (var entry in entries)
        {
            if (entry is not IDictionary fields || !fields.Contains("count") || fields["count"] is not { } countValue)
                continue;

            try
            {
                max = Math.Max(max, Convert.ToInt64(countValue));
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                // Ignore malformed x-death entries; fall back to the redelivered flag.
            }
        }

        return max;
    }

    /// <summary>Longest <c>AR-DeadLetter-Reason</c> header value, in UTF-16 code units.</summary>
    internal const int MaxDeadLetterReasonLength = 512;

    /// <summary>
    /// Builds the properties for a dead-letter copy of <paramref name="delivery"/>: the original
    /// headers plus the <c>AR-DeadLetter-*</c> forensic headers.
    /// </summary>
    protected BasicProperties BuildDeadLetterProperties(RabbitMqDelivery delivery, Exception exception)
    {
        var headers = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (delivery.BasicProperties.Headers is { } original)
        {
            foreach (var header in original)
                headers[header.Key] = header.Value;
        }

        // Surrogate-aware cut (see PortableText.TruncateWellFormed): a fixed-index slice through a
        // non-BMP character left a lone high surrogate in a header the client encodes as UTF-8.
        headers["AR-DeadLetter-Reason"] = PortableText.TruncateWellFormed(exception.Message, MaxDeadLetterReasonLength);
        headers["AR-DeadLetter-Source-Queue"] = _queue;
        headers["AR-DeadLetter-Role"] = _role.ToString();

        return new BasicProperties
        {
            Persistent = true,
            ContentType = delivery.BasicProperties.ContentType,
            MessageId = delivery.BasicProperties.MessageId,
            CorrelationId = delivery.BasicProperties.CorrelationId,
            Headers = headers
        };
    }

    /// <summary>Creates the configured dispatcher.</summary>
    public static RabbitMqMessageDispatcher Create(
        Func<RabbitMqDelivery, CancellationToken, Task> handler,
        RabbitMqAsyncResponseOptions transportOptions,
        RabbitMqSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        RabbitMqSubscriberRole role)
    {
        ValidateOptions(transportOptions, subscriberOptions, role);

        return subscriberOptions.AckMode == RabbitMqAckMode.AckAfterHandlerCompletes
            ? new AwaitingRabbitMqMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                queue,
                role)
            : new QueuedRabbitMqMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                queue,
                role);
    }

    /// <summary>Validates the supplied options.</summary>
    public static void ValidateOptions(
        RabbitMqAsyncResponseOptions transportOptions,
        RabbitMqSubscriberOptions subscriberOptions,
        RabbitMqSubscriberRole role)
    {
        var optionPath = role is RabbitMqSubscriberRole.Worker
            ? $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.WorkerSubscriber)}"
            : $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.ResponseSubscriber)}";

        RabbitMqOptionsValidator.ValidateConnection(transportOptions);

        // Both arm Task.Delay timers in the subscriber restart loop.
        AsyncResponseChannelOptions.EnsureTimerBacked(transportOptions.SubscriberRetryBaseDelay, nameof(RabbitMqAsyncResponseOptions), nameof(RabbitMqAsyncResponseOptions.SubscriberRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(transportOptions.SubscriberRetryMaxDelay, nameof(RabbitMqAsyncResponseOptions), nameof(RabbitMqAsyncResponseOptions.SubscriberRetryMaxDelay));
        if (transportOptions.SubscriberRetryBaseDelay > transportOptions.SubscriberRetryMaxDelay)
        {
            throw new InvalidOperationException(
                $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.SubscriberRetryBaseDelay)} cannot exceed " +
                $"{nameof(RabbitMqAsyncResponseOptions.SubscriberRetryMaxDelay)}.");
        }

        if (StringComparer.Ordinal.Equals(transportOptions.WorkerQueue, transportOptions.ResponseQueue))
        {
            throw new InvalidOperationException(
                $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.WorkerQueue)} and " +
                $"{nameof(RabbitMqAsyncResponseOptions.ResponseQueue)} must be distinct so worker and response subscribers do not consume each other's messages.");
        }

        // The publish address is (exchange, routingKey), not the queue name: a direct exchange
        // fans one routing key out to EVERY queue bound with it, so two distinct queues sharing
        // one address both receive every publish — worker envelopes delivered to the response
        // queue complete real waiters through the response ingress.
        if (StringComparer.Ordinal.Equals(transportOptions.WorkerExchange, transportOptions.ResponseExchange)
            && StringComparer.Ordinal.Equals(transportOptions.WorkerRoutingKey, transportOptions.ResponseRoutingKey))
        {
            throw new InvalidOperationException(
                $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.WorkerExchange)}+" +
                $"{nameof(RabbitMqAsyncResponseOptions.WorkerRoutingKey)} and " +
                $"{nameof(RabbitMqAsyncResponseOptions.ResponseExchange)}+{nameof(RabbitMqAsyncResponseOptions.ResponseRoutingKey)} " +
                "must not form the same publish address: the exchange fans that routing key out to both queues, so worker " +
                "jobs would also be delivered to the response queue (and responses to the worker queue).");
        }

        // Parity with the Kafka/NATS validators: a dead-letter destination aimed at a live one
        // turns reject-without-requeue into a broker-rate loop. A rejected message re-enters the
        // dead-letter exchange under DeadLetterRoutingKey — or its ORIGINAL routing key when that
        // is blank, which by definition matches the binding of the queue that rejected it. It
        // loops (or crosses into the other role's queue) only when that (exchange, routing key)
        // pair is a live binding; a distinct DeadLetterRoutingKey on a shared exchange is a
        // legitimate topology and must keep starting.
        if (!string.IsNullOrWhiteSpace(transportOptions.DeadLetterExchange))
        {
            var deadLetterAddressIsLive = string.IsNullOrWhiteSpace(transportOptions.DeadLetterRoutingKey)
                ? StringComparer.Ordinal.Equals(transportOptions.DeadLetterExchange, transportOptions.WorkerExchange)
                    || StringComparer.Ordinal.Equals(transportOptions.DeadLetterExchange, transportOptions.ResponseExchange)
                : (StringComparer.Ordinal.Equals(transportOptions.DeadLetterExchange, transportOptions.WorkerExchange)
                        && StringComparer.Ordinal.Equals(transportOptions.DeadLetterRoutingKey, transportOptions.WorkerRoutingKey))
                    || (StringComparer.Ordinal.Equals(transportOptions.DeadLetterExchange, transportOptions.ResponseExchange)
                        && StringComparer.Ordinal.Equals(transportOptions.DeadLetterRoutingKey, transportOptions.ResponseRoutingKey));
            if (deadLetterAddressIsLive)
            {
                throw new InvalidOperationException(
                    $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.DeadLetterExchange)} " +
                    $"'{transportOptions.DeadLetterExchange}' plus {nameof(RabbitMqAsyncResponseOptions.DeadLetterRoutingKey)} " +
                    $"'{transportOptions.DeadLetterRoutingKey ?? "(blank — the message's original routing key)"}' targets a live " +
                    $"binding ({nameof(RabbitMqAsyncResponseOptions.WorkerExchange)}/{nameof(RabbitMqAsyncResponseOptions.ResponseExchange)}): " +
                    "dead-lettered messages would re-enter live routing and loop instead of parking. " +
                    $"Set a {nameof(RabbitMqAsyncResponseOptions.DeadLetterRoutingKey)} that no live queue is bound with, or use a dedicated exchange.");
            }
        }

        if (!string.IsNullOrWhiteSpace(transportOptions.DeadLetterQueue)
            && (StringComparer.Ordinal.Equals(transportOptions.DeadLetterQueue, transportOptions.WorkerQueue)
                || StringComparer.Ordinal.Equals(transportOptions.DeadLetterQueue, transportOptions.ResponseQueue)))
        {
            throw new InvalidOperationException(
                $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.DeadLetterQueue)} " +
                $"'{transportOptions.DeadLetterQueue}' must not be a live queue " +
                $"({nameof(RabbitMqAsyncResponseOptions.WorkerQueue)}/{nameof(RabbitMqAsyncResponseOptions.ResponseQueue)}): " +
                "dead-lettered messages would be consumed as live traffic.");
        }

        // The park publish goes through the default exchange, whose routing key IS the queue name:
        // parked into a live queue, a capped message is redelivered straight back to the subscriber
        // that parked it — past its cap, so it is parked again, in a loop at broker rate.
        if (!string.IsNullOrWhiteSpace(transportOptions.ParkQueue)
            && (StringComparer.Ordinal.Equals(transportOptions.ParkQueue, transportOptions.WorkerQueue)
                || StringComparer.Ordinal.Equals(transportOptions.ParkQueue, transportOptions.ResponseQueue)))
        {
            throw new InvalidOperationException(
                $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.ParkQueue)} " +
                $"'{transportOptions.ParkQueue}' must not be a live queue " +
                $"({nameof(RabbitMqAsyncResponseOptions.WorkerQueue)}/{nameof(RabbitMqAsyncResponseOptions.ResponseQueue)}): " +
                "parked messages would be consumed as live traffic.");
        }

        RabbitMqOptionsValidator.ValidateConsumerTimeout(transportOptions);

        if (subscriberOptions.PrefetchCount == 0)
            throw new InvalidOperationException($"{optionPath}.{nameof(RabbitMqSubscriberOptions.PrefetchCount)} must be positive.");

        switch (subscriberOptions.AckMode)
        {
            case RabbitMqAckMode.AckAfterHandlerCompletes:
                return;

            case RabbitMqAckMode.AckAfterEnqueue:
                if (subscriberOptions.BackgroundWorkerCount <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(RabbitMqSubscriberOptions.BackgroundWorkerCount)} must be explicitly configured " +
                        $"when {nameof(RabbitMqSubscriberOptions.AckMode)} is {nameof(RabbitMqAckMode.AckAfterEnqueue)}.");
                }

                if (subscriberOptions.BackgroundQueueCapacity <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(RabbitMqSubscriberOptions.BackgroundQueueCapacity)} must be explicitly configured " +
                        $"when {nameof(RabbitMqSubscriberOptions.AckMode)} is {nameof(RabbitMqAckMode.AckAfterEnqueue)}.");
                }

                AsyncResponseChannelOptions.EnsureTimerBacked(subscriberOptions.BackgroundDrainTimeout, optionPath, nameof(RabbitMqSubscriberOptions.BackgroundDrainTimeout));
                AsyncResponseChannelOptions.EnsureTimerBacked(transportOptions.ShutdownTimeout, nameof(RabbitMqAsyncResponseOptions), nameof(RabbitMqAsyncResponseOptions.ShutdownTimeout));

                // RabbitMQ arms ShutdownTimeout TWICE on the stop path — once for BasicCancel,
                // then (after the background drain) a fresh budget for the channel and connection
                // closes — so the worst case is ShutdownTimeout + drain + ShutdownTimeout, and all
                // three must fit inside the host budget. Summing only one close term let a
                // configuration that overran the host by a full ShutdownTimeout start.
                ShutdownBudgetValidator.Validate(
                    "RabbitMQ",
                    $"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.HostShutdownTimeout)}",
                    transportOptions.HostShutdownTimeout,
                    ($"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.ShutdownTimeout)} (consumer cancel)", transportOptions.ShutdownTimeout),
                    ($"{optionPath}.{nameof(RabbitMqSubscriberOptions.BackgroundDrainTimeout)}", subscriberOptions.BackgroundDrainTimeout),
                    ($"{nameof(RabbitMqAsyncResponseOptions)}.{nameof(RabbitMqAsyncResponseOptions.ShutdownTimeout)} (channel/connection close)", transportOptions.ShutdownTimeout));

                return;

            default:
                throw new InvalidOperationException(
                    $"{optionPath}.{nameof(RabbitMqSubscriberOptions.AckMode)} has unsupported value '{subscriberOptions.AckMode}'.");
        }
    }

    /// <summary>Handles the delivered message.</summary>
    public abstract Task HandleAsync(
        RabbitMqDelivery delivery,
        IRabbitMqChannel channel,
        CancellationToken subscriberCancellationToken);

    /// <summary>
    /// Binds the channel of the subscriber attempt that is about to consume; disposing the returned
    /// handle unbinds it when that attempt ends. The dispatcher lives as long as the hosted
    /// subscriber and is fed by every attempt, so only a dispatcher that works after the delivery
    /// callback returns needs to know which channel is the live one.
    /// </summary>
    public virtual IDisposable AttachChannel(IRabbitMqChannel channel) => NoChannelAttachment.Instance;

    /// <summary>Releases resources held by this instance.</summary>
    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private sealed class NoChannelAttachment : IDisposable
    {
        public static readonly NoChannelAttachment Instance = new();

        public void Dispose()
        {
        }
    }

    /// <summary>Runs the ExecuteHandlerAsync operation.</summary>
    protected async Task ExecuteHandlerAsync(
        RabbitMqDelivery delivery,
        CancellationToken cancellationToken,
        bool logFailures = true)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.rabbitmq.receive",
            ActivityKind.Consumer);
        activity?.SetTag("asyncresponse.transport", "rabbitmq");
        activity?.SetTag("asyncresponse.rabbitmq.role", _role.ToString());
        activity?.SetTag("asyncresponse.rabbitmq.ack_mode", _subscriberOptions.AckMode.ToString());
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.destination.name", _queue);
        activity?.SetTag("messaging.rabbitmq.exchange", delivery.Exchange);
        activity?.SetTag("messaging.rabbitmq.routing_key", delivery.RoutingKey);
        activity?.SetTag("messaging.rabbitmq.delivery_tag", delivery.DeliveryTag);
        activity?.SetTag("messaging.message.id", delivery.BasicProperties.MessageId);

        if (!string.IsNullOrWhiteSpace(delivery.BasicProperties.CorrelationId))
            AsyncResponseDiagnostics.SetCorrelationId(activity, delivery.BasicProperties.CorrelationId);

        try
        {
            await _handler(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (logFailures)
                Logger.LogError(ex, "RabbitMQ message handling failed for delivery {DeliveryTag}.", delivery.DeliveryTag);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>Runs the NotifyBackgroundFailureAsync operation.</summary>
    protected async ValueTask NotifyBackgroundFailureAsync(
        RabbitMqDelivery delivery,
        Exception exception,
        string queue,
        RabbitMqSubscriberRole role)
    {
        var callback = _subscriberOptions.OnBackgroundFailure;
        if (callback is null)
            return;

        try
        {
            await callback(new RabbitMqBackgroundFailureContext(
                queue,
                role.ToString(),
                delivery.Exchange,
                delivery.RoutingKey,
                delivery.DeliveryTag,
                exception)).ConfigureAwait(false);
        }
        catch (Exception callbackException)
        {
            Logger.LogError(
                callbackException,
                "RabbitMQ background failure callback failed for already-ACKed delivery {DeliveryTag} on {Queue}.",
                delivery.DeliveryTag,
                queue);
        }
    }
}

internal sealed class AwaitingRabbitMqMessageDispatcher(
    Func<RabbitMqDelivery, CancellationToken, Task> handler,
    RabbitMqAsyncResponseOptions transportOptions,
    RabbitMqSubscriberOptions subscriberOptions,
    ILogger logger,
    string queue,
    RabbitMqSubscriberRole role)
    : RabbitMqMessageDispatcher(handler, transportOptions, subscriberOptions, logger, queue, role)
{
    /// <summary>Failed parks in a row; paces the requeue of the next one (reset by a park that lands).</summary>
    private int _consecutiveParkFailures;

    /// <summary>Handles the delivered message.</summary>
    public override async Task HandleAsync(
        RabbitMqDelivery delivery,
        IRabbitMqChannel channel,
        CancellationToken subscriberCancellationToken)
    {
        // Pre-execution cap (NATS/DB-transport parity), BEFORE the handler runs: a delivery whose
        // previous attempt ended WITHOUT a thrown exception — the process OOM-killed mid-handler,
        // FailFast, a hang that tripped the broker's consumer_timeout — is requeued by the broker
        // and never reaches the catch below, so nothing ever judged it against the cap and one
        // poison message crash-looped every replica in turn whatever MaxDeliveryAttempts said.
        if (MaxDeliveryAttempts > 0 && ResolveDeliveryAttempt(delivery) > EffectiveDeliveryCap(delivery))
        {
            if (ReadDeathCount(delivery.BasicProperties) == 0)
            {
                await TryNackAsync(delivery, channel, requeue: false).ConfigureAwait(false);
            }
            else
            {
                await ParkAtCapAsync(
                    delivery,
                    channel,
                    new InvalidOperationException($"RabbitMQ delivery exceeded {MaxDeliveryAttempts} delivery attempts before its handler ran."),
                    subscriberCancellationToken).ConfigureAwait(false);
            }

            return;
        }

        try
        {
            await ExecuteHandlerAsync(delivery, subscriberCancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (subscriberCancellationToken.IsCancellationRequested)
        {
            // Shutdown, not a handler failure: NACKing here would count a healthy delivery against
            // the cap — and at the cap reject it without requeue, dropping (or dead-lettering)
            // work whose side effects never ran. Leave it un-ACKed; the broker redelivers it when
            // the channel closes.
            return;
        }
        catch (Exception ex)
        {
            // Requeue for redelivery, unless a delivery cap is configured and this delivery has reached it —
            // then reject without requeue so the broker dead-letters (or drops) it instead of hot-looping.
            // Once the broker has dead-lettered the message (x-death present), a plain requeue can never
            // advance the attempt again — x-death only counts dead-letter hops and `redelivered` is already
            // set — so every retry BELOW the cap must ride the dead-letter cycle, which is what makes the
            // operator's cap countable at all. AT the cap with x-death present that same cycle is exactly
            // what must not run again: the dead-letter exchange already brought this message back once,
            // so a reject re-entered it at the cycle's TTL rate forever and the cap never parked anything
            // (EffectiveDeliveryCap was unreachable on the very path it was written for). Park it here.
            var deathCount = ReadDeathCount(delivery.BasicProperties);
            var belowCap = ResolveDeliveryAttempt(delivery) < EffectiveDeliveryCap(delivery);
            if (MaxDeliveryAttempts <= 0 || (deathCount == 0 && belowCap))
                await TryNackAsync(delivery, channel, requeue: true).ConfigureAwait(false);
            else if (deathCount == 0 || belowCap)
                await TryNackAsync(delivery, channel, requeue: false).ConfigureAwait(false);
            else
                await ParkAtCapAsync(delivery, channel, ex, subscriberCancellationToken).ConfigureAwait(false);
            return;
        }

        // The ACK sits outside the handler's try/catch: a transient BasicAck failure after a
        // successful handler must not be NACKed — the broker would redeliver and re-run side
        // effects that already completed. The un-ACKed delivery is redelivered anyway when the
        // channel closes, which is the unavoidable at-least-once floor. Settlement deliberately
        // ignores cancellation (as every sibling transport does): a shutdown racing the ACK would
        // abort the settle and redeliver work whose handler already ran.
        try
        {
            await channel.BasicAckAsync(delivery.DeliveryTag, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogError(
                ex,
                "Failed to ACK RabbitMQ delivery {DeliveryTag} for {Queue} after a successful handler; the broker will redeliver it when the channel closes.",
                delivery.DeliveryTag,
                QueueName);
        }
    }

    /// <summary>
    /// Terminal settlement for a delivery at its cap whose <c>x-death</c> shows the dead-letter
    /// exchange already returned it once: another reject would only re-enter that cycle. The
    /// message is copied to <see cref="RabbitMqAsyncResponseOptions.ParkQueue"/> (or, without one,
    /// <see cref="RabbitMqAsyncResponseOptions.DeadLetterQueue"/>) through the default exchange —
    /// bypassing the exchange that cycles — and the delivery is ACKed so the loop ends; without a
    /// queue the drop is logged as an error. A failed copy is handed back to the broker with a
    /// requeue after a backoff, so it is redelivered and the park retries.
    /// </summary>
    private async Task ParkAtCapAsync(
        RabbitMqDelivery delivery,
        IRabbitMqChannel channel,
        Exception exception,
        CancellationToken subscriberCancellationToken)
    {
        // A closed channel already requeued every un-ACKed delivery; it comes back with the same
        // x-death count and parks on its next attempt.
        if (!channel.IsOpen)
            return;

        var parkQueue = !string.IsNullOrWhiteSpace(TransportOptions.ParkQueue)
            ? TransportOptions.ParkQueue
            : TransportOptions.DeadLetterQueue;
        if (!string.IsNullOrWhiteSpace(parkQueue))
        {
            try
            {
                await channel.BasicPublishAsync(
                    string.Empty,
                    parkQueue,
                    BuildDeadLetterProperties(delivery, exception),
                    delivery.Body,
                    CancellationToken.None).ConfigureAwait(false);
                Volatile.Write(ref _consecutiveParkFailures, 0);
                Logger.LogWarning(
                    exception,
                    "RabbitMQ delivery {DeliveryTag} on {Queue} reached {MaxDeliveryAttempts} delivery attempts after riding the dead-letter cycle; parked in {DeadLetterQueue}.",
                    delivery.DeliveryTag,
                    QueueName,
                    MaxDeliveryAttempts,
                    parkQueue);
            }
            catch (Exception publishException)
            {
                // Hand the delivery back explicitly. AMQP never redelivers an un-ACKed delivery
                // while its channel stays open — leaving it unsettled only pinned one prefetch
                // credit, and PrefetchCount failed parks later the consumer received nothing at
                // all, most likely during the very incident that is filling the park queue. The
                // pause keeps the requeue → redeliver → failed-park loop off broker rate; it runs
                // inside the delivery callback, so this channel's deliveries wait with it.
                var retryDelay = AsyncResponseRetry.Backoff(
                    Interlocked.Increment(ref _consecutiveParkFailures),
                    TransportOptions.SubscriberRetryBaseDelay,
                    TransportOptions.SubscriberRetryMaxDelay);
                Logger.LogError(
                    publishException,
                    "Failed to park capped RabbitMQ delivery {DeliveryTag} on {Queue} in {DeadLetterQueue}; requeueing it in {RetryDelay} so the broker redelivers it and the park is retried.",
                    delivery.DeliveryTag,
                    QueueName,
                    parkQueue,
                    retryDelay);

                try
                {
                    await Task.Delay(retryDelay, subscriberCancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Stopping: skip the rest of the pause, not the requeue.
                }

                await TryNackAsync(delivery, channel, requeue: true).ConfigureAwait(false);
                return;
            }
        }
        else
        {
            Logger.LogError(
                exception,
                "RabbitMQ delivery {DeliveryTag} on {Queue} reached {MaxDeliveryAttempts} delivery attempts after riding the dead-letter cycle and neither ParkQueue nor DeadLetterQueue is configured; ACKing it so the cycle ends — the message is dropped.",
                delivery.DeliveryTag,
                QueueName,
                MaxDeliveryAttempts);
        }

        try
        {
            await channel.BasicAckAsync(delivery.DeliveryTag, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ackException)
        {
            Logger.LogWarning(
                ackException,
                "Failed to ACK parked RabbitMQ delivery {DeliveryTag} on {Queue}; the broker redelivers it when the channel closes.",
                delivery.DeliveryTag,
                QueueName);
        }
    }

    private async ValueTask TryNackAsync(RabbitMqDelivery delivery, IRabbitMqChannel channel, bool requeue)
    {
        // Never throw from here: this runs inside the client's delivery callback, and an escaped
        // exception would leave the delivery neither ACKed nor NACKed — a prefetch credit pinned
        // with no app-visible trace and the requeue/reject decision silently lost.
        // A closed channel already returned every un-ACKed delivery to the queue; NACKing it would throw.
        if (!channel.IsOpen)
        {
            Logger.LogWarning(
                "Skipping NACK ({NackDecision}) of RabbitMQ delivery {DeliveryTag} for {Queue}: the channel is closed, so the broker has already requeued it.",
                requeue ? "requeue" : "reject",
                delivery.DeliveryTag,
                QueueName);
            return;
        }

        try
        {
            await channel.BasicNackAsync(delivery.DeliveryTag, requeue, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(
                ex,
                "Failed to NACK ({NackDecision}) RabbitMQ delivery {DeliveryTag} for {Queue}; the broker redelivers it when the channel closes.",
                requeue ? "requeue" : "reject",
                delivery.DeliveryTag,
                QueueName);
        }
    }
}

internal sealed class QueuedRabbitMqMessageDispatcher : RabbitMqMessageDispatcher
{
    private readonly Channel<RabbitMqDelivery> _queue;
    private readonly Task[] _workers;
    private readonly CancellationTokenSource _drainCancellation = new();

    /// <summary>Serializes best-effort dead-letter publishes from concurrent background workers.</summary>
    private readonly SemaphoreSlim _deadLetterPublishGate = new(1, 1);

    /// <summary>
    /// The subscriber's channel, captured per delivery: background workers need it to publish an
    /// already-ACKed failed delivery to the dead-letter exchange (the native reject-without-requeue
    /// DLX route is unreachable once the ACK happened at enqueue).
    /// </summary>
    private volatile IRabbitMqChannel? _channel;

    /// <summary>
    /// The live subscriber attempt's channel. This dispatcher outlives attempts — queued work that
    /// was already ACKed must survive a channel shutdown or connection blip instead of being
    /// drained as if the host were stopping — so a background failure that lands while an attempt
    /// is being rebuilt publishes through the NEXT attempt's channel, never the dead one.
    /// </summary>
    private readonly object _attachGate = new();
    private ChannelAttachment? _attachment;
    private TaskCompletionSource _attached = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Signalled when disposal starts: no further attempt will attach a channel.</summary>
    private readonly CancellationTokenSource _stopping = new();
    private readonly CancellationToken _stoppingToken;
    private readonly TimeSpan _deadLetterChannelWait;
    private readonly TimeSpan _drainTimeout;
    private readonly string _queueName;
    private readonly RabbitMqSubscriberRole _role;
    private int _pendingCount;
    private int _runningCount;
    private int _disposeStarted;

    /// <summary>Runs the QueuedRabbitMqMessageDispatcher operation.</summary>
    public QueuedRabbitMqMessageDispatcher(
        Func<RabbitMqDelivery, CancellationToken, Task> handler,
        RabbitMqAsyncResponseOptions transportOptions,
        RabbitMqSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        RabbitMqSubscriberRole role)
        : base(handler, transportOptions, subscriberOptions, logger, queue, role)
    {
        _drainTimeout = subscriberOptions.BackgroundDrainTimeout;
        _stoppingToken = _stopping.Token;

        // Twice the supervisor's longest backoff: a reachable broker yields the next attempt's
        // channel well inside it, and an unreachable one must not hold a background worker (and
        // the queued work behind it) for the length of the outage.
        var channelWait = transportOptions.SubscriberRetryMaxDelay + transportOptions.SubscriberRetryMaxDelay;
        _deadLetterChannelWait = channelWait > AsyncResponseChannelOptions.MaxTimerBackedTimeout
            ? AsyncResponseChannelOptions.MaxTimerBackedTimeout
            : channelWait;
        _queueName = queue;
        _role = role;
        _queue = Channel.CreateBounded<RabbitMqDelivery>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
        {
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = subscriberOptions.BackgroundWorkerCount == 1,
            SingleWriter = false
        });

        _workers = Enumerable.Range(0, subscriberOptions.BackgroundWorkerCount)
            .Select(workerIndex => Task.Run(() => RunWorkerAsync(workerIndex)))
            .ToArray();

        Logger.LogInformation(
            "Created RabbitMQ ACK-after-enqueue dispatcher for {Queue} with {WorkerCount} worker(s), queue capacity {QueueCapacity}, drain timeout {DrainTimeout}.",
            _queueName,
            subscriberOptions.BackgroundWorkerCount,
            subscriberOptions.BackgroundQueueCapacity,
            _drainTimeout);
    }

    internal int PendingCount => Volatile.Read(ref _pendingCount);
    internal int RunningCount => Volatile.Read(ref _runningCount);

    /// <summary>Handles the delivered message.</summary>
    public override async Task HandleAsync(
        RabbitMqDelivery delivery,
        IRabbitMqChannel channel,
        CancellationToken subscriberCancellationToken)
    {
        // The client owns the delivery body's memory only until the consumer callback returns
        // ("Accessing the body at a later point is unsafe as its memory can be already
        // released" — RabbitMQ.Client v7). This dispatcher hands the delivery to background
        // workers that read the body after the callback, so materialize a private copy now.
        // The awaiting dispatcher consumes the body inside the callback and stays zero-copy.
        delivery = delivery with { Body = delivery.Body.ToArray() };
        _channel = channel;

        Interlocked.Increment(ref _pendingCount);
        if (!_queue.Writer.TryWrite(delivery))
        {
            // Saturated: wait for a worker to free a slot instead of NACKing. The early ACK below has
            // already released the prefetch credit, so QoS cannot bound a NACK/redeliver cycle — the
            // broker would redeliver within ~1 RTT and spin at network rate. RabbitMQ.Client dispatches
            // a channel's deliveries sequentially, so blocking here pauses this channel's delivery
            // loop, which is the actual backpressure (mirrors the Kafka pause and the NATS wait).
            Logger.LogDebug(
                "RabbitMQ background queue for {Queue} is full; pausing the delivery loop until capacity frees. Pending={PendingCount}, Running={RunningCount}.",
                _queueName,
                PendingCount,
                RunningCount);
            // The park also ends with the attempt that delivered it. The queue outlives attempts,
            // so a write parked under a channel that has since died would otherwise land later —
            // after the broker already requeued that un-ACKed delivery for the next attempt — and
            // the job would run twice.
            using var parked = CancellationTokenSource.CreateLinkedTokenSource(
                subscriberCancellationToken,
                AttachmentEnded(channel));
            try
            {
                await _queue.Writer.WriteAsync(delivery, parked.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is OperationCanceledException or ChannelClosedException)
            {
                // Subscriber stopping, its attempt ending, or dispatcher draining while parked: the
                // delivery was never enqueued (and never ACKed), so hand it back to the broker — one
                // NACK, not a spin. A closed channel requeues the un-ACKed delivery on its own;
                // never throw from here, this runs inside the client's delivery callback.
                Interlocked.Decrement(ref _pendingCount);
                await TryRequeueAsync(delivery, channel).ConfigureAwait(false);
                return;
            }
        }

        // The delivery now belongs to a background worker, which decrements _pendingCount when it dequeues.
        // Do not touch the counter or NACK here, even if the ACK below fails — the message is already
        // executing in-process and a NACK would trigger a duplicate execution via requeue. Settlement
        // deliberately ignores cancellation (as every sibling transport does): a shutdown racing the
        // ACK would abort the settle and redeliver a job the background worker is still running.
        try
        {
            await channel.BasicAckAsync(delivery.DeliveryTag, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogError(
                ex,
                "Failed to ACK RabbitMQ delivery {DeliveryTag} for {Queue} after enqueue; it is being processed but the broker will redeliver it when the channel closes.",
                delivery.DeliveryTag,
                _queueName);
        }
    }

    /// <summary>
    /// Publishes an already-ACKed failed delivery to the configured dead-letter exchange
    /// (Kafka/Redis dispatcher parity). The native reject-without-requeue DLX route is
    /// unreachable here — the ACK happened at enqueue — so without this copy a permanently
    /// failing job vanished with one log line: no requeue, no DLX record, no forensic trail.
    /// Best-effort: on any failure the loss stays observable via the log and OnBackgroundFailure,
    /// exactly as before. Mirrors native dead-lettering's routing: the original routing key,
    /// unless DeadLetterRoutingKey overrides it (the same rule the topology binds the
    /// dead-letter queue with).
    /// </summary>
    private async Task TryDeadLetterAlreadyAckedAsync(RabbitMqDelivery delivery, Exception exception)
    {
        if (string.IsNullOrWhiteSpace(TransportOptions.DeadLetterExchange))
            return;

        var properties = BuildDeadLetterProperties(delivery, exception);

        var routingKey = string.IsNullOrWhiteSpace(TransportOptions.DeadLetterRoutingKey)
            ? delivery.RoutingKey
            : TransportOptions.DeadLetterRoutingKey;

        // The channel this delivery arrived on may be gone: the dispatcher outlives subscriber
        // attempts. Publish through the live attempt's channel, waiting for the next one while the
        // subscriber is being rebuilt — bounded, and not at all once disposal has started (no
        // further attempt follows).
        using var channelWait = CancellationTokenSource.CreateLinkedTokenSource(_stoppingToken);
        channelWait.CancelAfter(_deadLetterChannelWait);

        while (true)
        {
            var channel = await WaitForOpenChannelAsync(channelWait.Token).ConfigureAwait(false);
            if (channel is null)
            {
                Logger.LogError(
                    "Cannot dead-letter already-ACKed RabbitMQ delivery {DeliveryTag} on {Queue}: the subscriber channel is closed and no new one was attached. The failure is only observable via logs and OnBackgroundFailure.",
                    delivery.DeliveryTag,
                    _queueName);
                return;
            }

            // Serialized: multiple background workers can fail concurrently, and they share the
            // subscriber's one channel.
            await _deadLetterPublishGate.WaitAsync().ConfigureAwait(false);
            try
            {
                await channel.BasicPublishAsync(
                    TransportOptions.DeadLetterExchange!,
                    routingKey,
                    properties,
                    delivery.Body,
                    CancellationToken.None).ConfigureAwait(false);
                Logger.LogInformation(
                    "Dead-lettered already-ACKed RabbitMQ delivery {DeliveryTag} from {Queue} to exchange {DeadLetterExchange}.",
                    delivery.DeliveryTag,
                    _queueName,
                    TransportOptions.DeadLetterExchange);
                return;
            }
            catch (Exception publishException) when (!channel.IsOpen && !channelWait.IsCancellationRequested)
            {
                // The channel died under the publish (a broker nack or an unroutable return leaves
                // it open): the next attempt's channel takes the copy.
                Logger.LogDebug(
                    publishException,
                    "The subscriber channel closed while dead-lettering already-ACKed RabbitMQ delivery {DeliveryTag} on {Queue}; retrying on the next attempt's channel.",
                    delivery.DeliveryTag,
                    _queueName);
            }
            catch (Exception publishException)
            {
                Logger.LogError(
                    publishException,
                    "Failed to dead-letter already-ACKed RabbitMQ delivery {DeliveryTag} on {Queue}; the failure is only observable via logs and OnBackgroundFailure.",
                    delivery.DeliveryTag,
                    _queueName);
                return;
            }
            finally
            {
                _deadLetterPublishGate.Release();
            }
        }
    }

    /// <summary>Binds the live subscriber attempt's channel.</summary>
    public override IDisposable AttachChannel(IRabbitMqChannel channel)
    {
        var attachment = new ChannelAttachment(this, channel);
        lock (_attachGate)
        {
            _attachment = attachment;
            _attached.TrySetResult();
        }

        return attachment;
    }

    private void Detach(ChannelAttachment attachment)
    {
        lock (_attachGate)
        {
            if (ReferenceEquals(_attachment, attachment))
                _attachment = null;
        }
    }

    /// <summary>
    /// Cancelled when the attempt that attached <paramref name="channel"/> ends; never, for a
    /// channel no attempt attached (a caller driving <see cref="HandleAsync"/> directly).
    /// </summary>
    private CancellationToken AttachmentEnded(IRabbitMqChannel channel)
    {
        lock (_attachGate)
        {
            return _attachment is { } attachment && ReferenceEquals(attachment.Channel, channel)
                ? attachment.Ended
                : CancellationToken.None;
        }
    }

    /// <summary>
    /// The open channel to publish through — the attached attempt's, else the one the last delivery
    /// arrived on — or <c>null</c> once <paramref name="cancellationToken"/> fires with none open.
    /// </summary>
    private async ValueTask<IRabbitMqChannel?> WaitForOpenChannelAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            Task attached;
            lock (_attachGate)
            {
                if ((_attachment?.Channel ?? _channel) is { IsOpen: true } channel)
                    return channel;

                // A closed channel whose attempt has not unwound yet still counts as attached:
                // wait for the NEXT attach, not the one that already happened.
                if (_attached.Task.IsCompleted)
                    _attached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                attached = _attached.Task;
            }

            try
            {
                await attached.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }
    }

    private sealed class ChannelAttachment : IDisposable
    {
        private readonly QueuedRabbitMqMessageDispatcher _owner;
        private readonly CancellationTokenSource _ended = new();
        private int _disposed;

        public ChannelAttachment(QueuedRabbitMqMessageDispatcher owner, IRabbitMqChannel channel)
        {
            _owner = owner;
            Channel = channel;
            Ended = _ended.Token;
        }

        public IRabbitMqChannel Channel { get; }
        public CancellationToken Ended { get; }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            _owner.Detach(this);
            _ended.Cancel();
            _ended.Dispose();
        }
    }

    private async ValueTask TryRequeueAsync(RabbitMqDelivery delivery, IRabbitMqChannel channel)
    {
        // A closed channel already returned every un-ACKed delivery to the queue; NACKing it would throw.
        if (!channel.IsOpen)
            return;

        try
        {
            await channel.BasicNackAsync(delivery.DeliveryTag, requeue: true, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogDebug(
                ex,
                "Failed to NACK delivery {DeliveryTag} for {Queue} during shutdown; the broker requeues it when the channel closes.",
                delivery.DeliveryTag,
                _queueName);
        }
    }

    /// <summary>Releases resources held by this instance.</summary>
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;

        Logger.LogInformation(
            "Draining RabbitMQ ACK-after-enqueue dispatcher for {Queue}. Pending={PendingCount}, Running={RunningCount}.",
            _queueName,
            PendingCount,
            RunningCount);
        _queue.Writer.TryComplete();

        // Workers read the token captured at construction, never the source, so disposing it
        // under them is safe.
        _stopping.Cancel();
        _stopping.Dispose();

        try
        {
            await Task.WhenAll(_workers).WaitAsync(_drainTimeout).ConfigureAwait(false);
            _drainCancellation.Dispose();
        }
        catch (TimeoutException ex)
        {
            _drainCancellation.Cancel();
            Logger.LogWarning(
                ex,
                "Timed out while draining RabbitMQ ACK-after-enqueue dispatcher for {Queue}. Pending={PendingCount}, Running={RunningCount}. Already ACKed work may be interrupted by host shutdown.",
                _queueName,
                PendingCount,
                RunningCount);

            // The workers are still running and read _drainCancellation.Token each loop, so disposing it now
            // would throw ObjectDisposedException inside them. Dispose once they actually finish, off the
            // shutdown path, so the source is not leaked either.
            _ = Task.WhenAll(_workers).ContinueWith(
                _ => _drainCancellation.Dispose(),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
        catch (Exception ex)
        {
            // A worker faulted outside its own handler guard (DB/NATS dispatcher parity). WhenAll
            // only completes once every worker has finished, so the source is safe to dispose here
            // — and the fault must not escape DisposeAsync and mask the real shutdown path.
            Logger.LogDebug(ex, "RabbitMQ ACK-after-enqueue dispatcher drain for {Queue} ended with an error.", _queueName);
            _drainCancellation.Dispose();
        }
    }

    private async Task RunWorkerAsync(int workerIndex)
    {
        await foreach (var delivery in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            Interlocked.Decrement(ref _pendingCount);

            // Once the drain budget has lapsed, STOP executing (DB/Redis/Pub-Sub parity). The
            // token below cannot stop the real handler — it is
            // `_ingress.HandleWorkerMessageAsync(payload)`, whose target takes no
            // CancellationToken — so past the budget the loop kept starting fresh work beyond the
            // host's shutdown budget, and every entry still queued at process exit vanished with
            // no record (ACKed at enqueue, so the broker never redelivers it).
            if (_drainCancellation.IsCancellationRequested)
            {
                var lapsed = new OperationCanceledException(
                    "The ACK-after-enqueue drain budget lapsed before this already-ACKed message was handled.");
                Logger.LogWarning(
                    "RabbitMQ background handler for already-ACKed delivery {DeliveryTag} on {Queue} was not started: the drain budget had lapsed. Dead-lettering and surfacing via OnBackgroundFailure.",
                    delivery.DeliveryTag,
                    _queueName);
                await NotifyBackgroundFailureAsync(delivery, lapsed, _queueName, _role).ConfigureAwait(false);
                await TryDeadLetterAlreadyAckedAsync(delivery, lapsed).ConfigureAwait(false);
                continue;
            }

            Interlocked.Increment(ref _runningCount);

            try
            {
                await ExecuteHandlerAsync(
                    delivery,
                    _drainCancellation.Token,
                    logFailures: false).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogError(
                    ex,
                    "RabbitMQ background handler failed for already-ACKed delivery {DeliveryTag} on {Queue}.",
                    delivery.DeliveryTag,
                    _queueName);
                await NotifyBackgroundFailureAsync(
                    delivery,
                    ex,
                    _queueName,
                    _role).ConfigureAwait(false);
                await TryDeadLetterAlreadyAckedAsync(delivery, ex).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _runningCount);
            }
        }
    }
}
