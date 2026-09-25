using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Threading.Channels;

namespace AsyncResponse.Transports.Kafka;

internal enum KafkaSubscriberRole
{
    Worker,
    ResponseIngress
}

internal sealed record KafkaDelivery(
    string Topic,
    int Partition,
    long Offset,
    string Payload,
    string? CorrelationId,
    IReadOnlyList<KafkaTransportHeader> Headers);

/// <summary>
/// Turns consumed Kafka messages into AsyncResponse handler invocations under one of the two ACK
/// modes. Kafka offsets cannot NACK a single message, so failed handlers are retried in-process
/// with bounded backoff; a message that exhausts its attempts is produced to the dead-letter topic
/// and its offset stored so the partition keeps moving.
/// </summary>
/// <summary>
/// A message exhausted its handling and its dead-letter publish failed for good, so it is
/// neither buried nor committable. Thrown out of the poll loop ON PURPOSE: Kafka commits a
/// partition <em>position</em>, not per-record acknowledgements, so merely leaving this message's
/// offset unstored (the previous behavior) protected nothing — the next successful settlement on
/// the same partition stored a higher offset, the auto-committer committed past the failed
/// message, and a restart skipped it with no dead-letter copy anywhere. Faulting the subscriber
/// instead stops the partition at the unresolved message: the consumer closes without ever
/// storing past it, the supervisor rebuilds it after its backoff, and the message is re-consumed
/// and its burial retried until the dead-letter topic is back. That is a loud, bounded-rate loop
/// (every restart logs this failure) and a stalled subscriber — the at-least-once outcome — rather
/// than a silent loss.
/// </summary>
internal sealed class KafkaDeadLetterPublishFailedException(string topic, int partition, long offset, Exception innerException)
    : Exception(
        $"Kafka message {topic}[{partition}]@{offset} could not be dead-lettered after exhausting its handling attempts. " +
        "Its offset is left unstored and the subscriber is restarted so no later settlement on the partition commits past it; " +
        "fix the dead-letter topic to let the partition advance.",
        innerException)
{
    public string Topic { get; } = topic;
    public int Partition { get; } = partition;
    public long Offset { get; } = offset;
}

internal abstract class KafkaMessageDispatcher : IAsyncDisposable
{
    private readonly Func<KafkaDelivery, CancellationToken, Task> _handler;
    private readonly KafkaSubscriberOptions _subscriberOptions;

    /// <summary>
    /// The consumer settlements store offsets on. Fixed for a per-attempt dispatcher; a dispatcher
    /// that outlives attempts (the queued one) is re-pointed at each attempt's consumer by
    /// <see cref="AttachConsumer"/> — only ever from the poll thread's side of the loop.
    /// </summary>
    private volatile IKafkaConsumerClient? _consumer;
    private readonly IKafkaProducerClient _producer;
    private int _messageMaxBytes;
    private readonly KafkaTransportTopicSchema _topics;
    private readonly string _topic;
    private readonly string _consumerGroup;
    private readonly KafkaSubscriberRole _role;

    /// <summary>Runs the KafkaMessageDispatcher operation.</summary>
    protected KafkaMessageDispatcher(
        Func<KafkaDelivery, CancellationToken, Task> handler,
        IKafkaConsumerClient? consumer,
        IKafkaProducerClient producer,
        KafkaAsyncResponseTransportOptions transportOptions,
        KafkaSubscriberOptions subscriberOptions,
        ILogger logger,
        string topic,
        string consumerGroup,
        KafkaSubscriberRole role)
    {
        _handler = handler;
        _consumer = consumer;
        _producer = producer;
        TransportOptions = transportOptions;
        _subscriberOptions = subscriberOptions;
        _topics = new KafkaTransportTopicSchema(transportOptions);
        Logger = logger;
        _topic = topic;
        _consumerGroup = consumerGroup;
        _role = role;
    }

    protected KafkaAsyncResponseTransportOptions TransportOptions { get; }
    protected ILogger Logger { get; }
    protected IKafkaConsumerClient Consumer
        => _consumer ?? throw new InvalidOperationException("No Kafka consumer is attached to this dispatcher.");

    protected int MaxDeliveryAttempts => _subscriberOptions.MaxDeliveryAttempts;

    /// <summary>
    /// Points the dispatcher at the consumer of the subscriber attempt that is about to poll;
    /// disposing the returned handle detaches it when that attempt ends. Only a dispatcher that
    /// outlives attempts (the ACK-after-enqueue one: created once per hosted subscriber, drained
    /// only at host stop) is created without a consumer.
    /// </summary>
    public IDisposable AttachConsumer(IKafkaConsumerClient consumer)
    {
        _consumer = consumer;
        return new ConsumerAttachment(this, consumer);
    }

    private sealed class ConsumerAttachment(KafkaMessageDispatcher owner, IKafkaConsumerClient consumer) : IDisposable
    {
        public void Dispose()
            => Interlocked.CompareExchange(ref owner._consumer, null, consumer);
    }

    /// <summary>Creates the configured dispatcher.</summary>
    public static KafkaMessageDispatcher Create(
        Func<KafkaDelivery, CancellationToken, Task> handler,
        IKafkaConsumerClient? consumer,
        IKafkaProducerClient producer,
        KafkaAsyncResponseTransportOptions transportOptions,
        KafkaSubscriberOptions subscriberOptions,
        ILogger logger,
        string topic,
        string consumerGroup,
        KafkaSubscriberRole role)
    {
        ValidateOptions(transportOptions, subscriberOptions, role);

        if (subscriberOptions.AckMode is KafkaAckMode.AckAfterEnqueue)
        {
            return new QueuedKafkaMessageDispatcher(
                handler,
                consumer,
                producer,
                transportOptions,
                subscriberOptions,
                logger,
                topic,
                consumerGroup,
                role);
        }

        return new AwaitingKafkaMessageDispatcher(
            handler,
            // Per attempt: its detached handlers are bound to the consumer they were consumed on.
            consumer ?? throw new ArgumentNullException(nameof(consumer), "The ack-after-handler dispatcher needs its attempt's consumer."),
            producer,
            transportOptions,
            subscriberOptions,
            logger,
            topic,
            consumerGroup,
            role);
    }

    /// <summary>Validates the supplied options.</summary>
    public static void ValidateOptions(
        KafkaAsyncResponseTransportOptions transportOptions,
        KafkaSubscriberOptions subscriberOptions,
        KafkaSubscriberRole role)
    {
        KafkaTransportOptionsValidator.ValidateCommon(transportOptions);

        var optionPath = role is KafkaSubscriberRole.Worker
            ? $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(KafkaAsyncResponseTransportOptions.WorkerSubscriber)}"
            : $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(KafkaAsyncResponseTransportOptions.ResponseSubscriber)}";

        // PollTimeout and BackpressurePollDelay both go to Consume(TimeSpan), which librdkafka
        // takes as 32-bit milliseconds; the handler-retry delays arm in-process Task.Delay timers.
        KafkaTransportOptionsValidator.EnsureIntMilliseconds(subscriberOptions.PollTimeout, optionPath, nameof(KafkaSubscriberOptions.PollTimeout));
        KafkaTransportOptionsValidator.EnsureIntMilliseconds(subscriberOptions.BackpressurePollDelay, optionPath, nameof(KafkaSubscriberOptions.BackpressurePollDelay));
        if (subscriberOptions.MaxDeliveryAttempts < 0)
            throw new InvalidOperationException($"{optionPath}.{nameof(KafkaSubscriberOptions.MaxDeliveryAttempts)} cannot be negative.");
        AsyncResponseChannelOptions.EnsureTimerBacked(subscriberOptions.HandlerRetryBaseDelay, optionPath, nameof(KafkaSubscriberOptions.HandlerRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(subscriberOptions.HandlerRetryMaxDelay, optionPath, nameof(KafkaSubscriberOptions.HandlerRetryMaxDelay));
        if (subscriberOptions.HandlerRetryBaseDelay > subscriberOptions.HandlerRetryMaxDelay)
        {
            throw new InvalidOperationException(
                $"{optionPath}.{nameof(KafkaSubscriberOptions.HandlerRetryBaseDelay)} cannot exceed " +
                $"{optionPath}.{nameof(KafkaSubscriberOptions.HandlerRetryMaxDelay)}.");
        }

        KafkaTransportOptionsValidator.EnsureMaxPollInterval(subscriberOptions.MaxPollInterval, optionPath, nameof(KafkaSubscriberOptions.MaxPollInterval));
        AsyncResponseChannelOptions.EnsureTimerBackedAllowZero(subscriberOptions.DetachHandlerAfter, optionPath, nameof(KafkaSubscriberOptions.DetachHandlerAfter));
        AsyncResponseChannelOptions.EnsureTimerBackedAllowZero(subscriberOptions.FaultDrainTimeout, optionPath, nameof(KafkaSubscriberOptions.FaultDrainTimeout));

        switch (subscriberOptions.AckMode)
        {
            case KafkaAckMode.AckAfterHandlerCompletes:
                // The poll thread's longest gap in this mode is one inline handler wait plus one
                // poll; a gap reaching max.poll.interval.ms gets the consumer evicted from its
                // group and its partitions redelivered elsewhere. Half the interval is the margin.
                // Handler execution time and the in-process retry ladder no longer count: past
                // DetachHandlerAfter the handler runs detached while the poll thread keeps polling
                // (the earlier rule bounded the retry DELAYS for that reason, and left real handler
                // time — a flow step awaiting a remote response — to overrun the interval anyway).
                var pollGapMs = subscriberOptions.DetachHandlerAfter.TotalMilliseconds + subscriberOptions.PollTimeout.TotalMilliseconds;
                if (pollGapMs * 2 > subscriberOptions.MaxPollInterval.TotalMilliseconds)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}: {nameof(KafkaSubscriberOptions.DetachHandlerAfter)} ({subscriberOptions.DetachHandlerAfter}) plus " +
                        $"{nameof(KafkaSubscriberOptions.PollTimeout)} ({subscriberOptions.PollTimeout}) must fit within half of " +
                        $"{nameof(KafkaSubscriberOptions.MaxPollInterval)} ({subscriberOptions.MaxPollInterval}) — that sum is the poll thread's " +
                        "longest gap, and a gap reaching max.poll.interval.ms gets the consumer evicted from its group. Lower " +
                        $"{nameof(KafkaSubscriberOptions.DetachHandlerAfter)} or raise {nameof(KafkaSubscriberOptions.MaxPollInterval)}.");
                }

                return;

            case KafkaAckMode.AckAfterEnqueue:
                if (subscriberOptions.BackgroundWorkerCount <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(KafkaSubscriberOptions.BackgroundWorkerCount)} must be explicitly configured " +
                        $"when {nameof(KafkaSubscriberOptions.AckMode)} is {nameof(KafkaAckMode.AckAfterEnqueue)}.");
                }

                if (subscriberOptions.BackgroundQueueCapacity <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(KafkaSubscriberOptions.BackgroundQueueCapacity)} must be explicitly configured " +
                        $"when {nameof(KafkaSubscriberOptions.AckMode)} is {nameof(KafkaAckMode.AckAfterEnqueue)}.");
                }

                AsyncResponseChannelOptions.EnsureTimerBacked(subscriberOptions.BackgroundDrainTimeout, optionPath, nameof(KafkaSubscriberOptions.BackgroundDrainTimeout));

                // Kafka subscribers spend only the background drain at shutdown; the poll loop
                // stops with the host token and the consumer close is not separately bounded.
                ShutdownBudgetValidator.Validate(
                    "Kafka",
                    $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(KafkaAsyncResponseTransportOptions.HostShutdownTimeout)}",
                    transportOptions.HostShutdownTimeout,
                    ($"{optionPath}.{nameof(KafkaSubscriberOptions.BackgroundDrainTimeout)}", subscriberOptions.BackgroundDrainTimeout));

                return;

            default:
                throw new InvalidOperationException(
                    $"{optionPath}.{nameof(KafkaSubscriberOptions.AckMode)} has unsupported value '{subscriberOptions.AckMode}'.");
        }
    }

    /// <summary>Handles the delivered message through to settlement, offset store included.</summary>
    public abstract Task HandleAsync(KafkaDelivery delivery, CancellationToken subscriberCancellationToken);

    /// <summary>
    /// The poll thread's entry point for a consumed message. Returns once the message is settled
    /// (offset stored, or dead-lettered and stored) or — for the awaiting dispatcher — once its
    /// handler has been detached to run on while polling continues. Throws when the message cannot
    /// be settled (a permanently failing burial, cancellation), which faults the poll loop so the
    /// subscriber is rebuilt without ever committing past the message.
    /// </summary>
    public virtual void Accept(KafkaDelivery delivery, CancellationToken subscriberCancellationToken)
        => HandleAsync(delivery, subscriberCancellationToken).GetAwaiter().GetResult();

    /// <summary>
    /// The poll thread's entry point for a consumed message that could not be turned into a
    /// delivery (<see cref="DiscardUnprocessableAsync"/> describes the settlement). Default:
    /// settled at once — the queued dispatcher stores every offset at enqueue, in consumption
    /// order, so nothing earlier on the partition is still unresolved. The awaiting dispatcher
    /// overrides it to hold the message behind a detached handler of the same partition: its
    /// offset must not be stored — and so committed — ahead of a message consumed before it that
    /// is still being handled.
    /// </summary>
    public virtual void AcceptUnprocessable(KafkaIncomingMessage message, Exception failure, CancellationToken subscriberCancellationToken)
        => DiscardUnprocessableAsync(message, failure, subscriberCancellationToken).GetAwaiter().GetResult();

    /// <summary>
    /// Poll-thread tick: settles detached handlers that have finished — offset stored, partition
    /// resumed, the next held message started. Throws when one of them failed for good (the poll
    /// loop faults, exactly as an inline failure would).
    /// </summary>
    public virtual void SettleCompleted()
    {
    }

    /// <summary>
    /// The poll loop FAILED (as opposed to a stop) and the consumer is about to be closed and
    /// rebuilt by the supervisor. Default: the graceful drain. The awaiting dispatcher overrides
    /// it with a bounded wait (<see cref="KafkaSubscriberOptions.FaultDrainTimeout"/>) so the
    /// reconnect is not parked behind an unrelated long handler.
    /// </summary>
    public virtual ValueTask TeardownAfterFaultAsync() => DisposeAsync();

    /// <summary>
    /// Whether detached handlers are in flight. The poll loop then polls in
    /// <see cref="KafkaSubscriberOptions.BackpressurePollDelay"/> slices so a completion is settled
    /// promptly instead of after a full <see cref="KafkaSubscriberOptions.PollTimeout"/>.
    /// </summary>
    public virtual bool HasDetachedWork => false;

    /// <summary>
    /// Whether the dispatcher can accept more deliveries right now. Awaiting dispatchers always can
    /// (a partition with a detached handler is paused, so nothing arrives for it); the queued
    /// dispatcher returns <c>false</c> while its bounded queue is saturated so the subscriber
    /// pauses partition fetching instead of buffering an unbounded backlog in-process.
    /// </summary>
    public virtual bool CanAcceptMore => true;

    /// <summary>Releases resources held by this instance.</summary>
    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <summary>Runs the ExecuteHandlerAsync operation.</summary>
    protected async Task ExecuteHandlerAsync(
        KafkaDelivery delivery,
        int attempt,
        CancellationToken cancellationToken,
        bool logFailures = true)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.kafka.receive",
            ActivityKind.Consumer,
            delivery.CorrelationId);
        activity?.SetTag("asyncresponse.transport", "kafka");
        activity?.SetTag("asyncresponse.kafka.role", _role.ToString());
        activity?.SetTag("asyncresponse.kafka.ack_mode", _subscriberOptions.AckMode.ToString());
        activity?.SetTag("asyncresponse.kafka.delivery_attempt", attempt);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", delivery.Topic);
        activity?.SetTag("messaging.kafka.consumer.group", _consumerGroup);
        activity?.SetTag("messaging.kafka.destination.partition", delivery.Partition);
        activity?.SetTag("messaging.kafka.message.offset", delivery.Offset);

        try
        {
            await _handler(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // A stop is not a handler failure: neither this subscriber's own cancellation nor the
            // flow engine's host-stop hand-back belongs in an error log.
            // Nor, for the same reason, an error span (RabbitMQ parity).
            var stopped = ex is DurableFlowInterruptedException
                || (ex is OperationCanceledException && cancellationToken.IsCancellationRequested);
            if (logFailures && !stopped)
            {
                Logger.LogError(
                    ex,
                    "Kafka message handling failed for {Topic}[{Partition}]@{Offset} (attempt {Attempt}).",
                    delivery.Topic,
                    delivery.Partition,
                    delivery.Offset,
                    attempt);
            }

            if (!stopped)
                AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>
    /// Marks the delivered message resolved by storing its next offset; the consumer's
    /// auto-committer flushes stored offsets on the configured interval.
    /// </summary>
    protected void StoreOffset(KafkaDelivery delivery)
        => Consumer.StoreOffset(delivery.Topic, delivery.Partition, delivery.Offset);

    /// <summary>
    /// Stores the offset after the message is settled (handler success or dead-letter publish),
    /// swallowing failures: StoreOffset throws when a rebalance revoked the partition — routine in
    /// a consumer group — and the only consequence is redelivery after the rebalance. A thrown
    /// settlement must never be misread as a handler failure that re-runs (or dead-letters)
    /// already-settled work.
    /// </summary>
    protected void StoreOffsetAfterSettlement(KafkaDelivery delivery)
        => StoreOffsetAfterSettlement(delivery.Topic, delivery.Partition, delivery.Offset);

    /// <summary>
    /// Coordinate overload, for settlement paths that never built a <see cref="KafkaDelivery"/> —
    /// the unprocessable-message discard runs before projection succeeds.
    /// </summary>
    protected void StoreOffsetAfterSettlement(string topic, int partition, long offset)
    {
        try
        {
            Consumer.StoreOffset(topic, partition, offset);
        }
        catch (KafkaPartitionNotAssignedException ex)
        {
            // Routine in a consumer group, not an error: a rebalance took the partition while the
            // message was being settled, and its new owner re-consumes it from the group's
            // committed offset (sibling precedent: the pause/resume of a revoked partition).
            Logger.LogInformation(
                ex,
                "Did not store the offset of Kafka message {Topic}[{Partition}]@{Offset} after settlement: a rebalance revoked the partition, so its new owner redelivers it.",
                topic,
                partition,
                offset);
        }
        catch (Exception ex)
        {
            Logger.LogError(
                ex,
                "Failed to store offset for Kafka message {Topic}[{Partition}]@{Offset} after settlement; it will be redelivered after a restart or rebalance.",
                topic,
                partition,
                offset);
        }
    }

    /// <summary>Runs the ReachedDeliveryAttempts operation.</summary>
    protected bool ReachedDeliveryAttempts(int attempt)
        => MaxDeliveryAttempts > 0 && attempt >= MaxDeliveryAttempts;

    /// <summary>Computes the delay before the next in-process handler retry.</summary>
    protected TimeSpan RetryBackoff(int completedAttempts)
        => AsyncResponseRetry.Backoff(
            completedAttempts,
            _subscriberOptions.HandlerRetryBaseDelay,
            _subscriberOptions.HandlerRetryMaxDelay);

    /// <summary>
    /// Produces the failing message to the dead-letter topic (when enabled), preserving the
    /// original payload and headers and attaching failure-detail headers.
    /// </summary>
    protected async Task DeadLetterAsync(
        KafkaDelivery delivery,
        Exception exception,
        string reason,
        int attempts,
        CancellationToken cancellationToken)
        => await DeadLetterCoreAsync(
            delivery.Topic,
            delivery.Partition,
            delivery.Offset,
            Encoding.UTF8.GetBytes(delivery.Payload),
            delivery.Headers,
            delivery.CorrelationId,
            exception,
            reason,
            attempts,
            cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Dead-letters (when enabled) and stores the offset of a message that could not be turned into
    /// a delivery — for example a foreign message with an empty payload. Without this, such a
    /// message would fail before <see cref="HandleAsync"/> runs on every subscriber restart and its
    /// partition would never advance.
    /// </summary>
    public async Task DiscardUnprocessableAsync(
        KafkaIncomingMessage message,
        Exception failure,
        CancellationToken cancellationToken)
    {
        Logger.LogError(
            failure,
            "Kafka message {Topic}[{Partition}]@{Offset} could not be parsed into a delivery; dead-lettering and committing it to avoid a poison-message loop.",
            message.Topic,
            message.Partition,
            message.Offset);

        // Settlement ignores the stopping token, as every sibling settlement path does: a shutdown
        // landing between the dead-letter publish and the offset store would abort the publish
        // mid-flight and leave the poison message neither buried nor committed. A burial that
        // fails for good FAULTS the poll loop (see KafkaDeadLetterPublishFailedException): an
        // earlier round swallowed it and left the offset unstored, which looked safe but was not —
        // the next settlement on the same partition committed past this message. The restart loop
        // it replaces is bounded by the supervisor's backoff and is the at-least-once outcome.
        try
        {
            await DeadLetterCoreAsync(
                message.Topic,
                message.Partition,
                message.Offset,
                message.Payload ?? [],
                message.Headers,
                KafkaCorrelationIdExtractor.TryReadHeader(message.Headers, TransportOptions.CorrelationIdHeader),
                failure,
                "unprocessable_message",
                attempts: 0,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception deadLetterException)
        {
            Logger.LogError(
                deadLetterException,
                "Failed to dead-letter unprocessable Kafka message {Topic}[{Partition}]@{Offset}; its offset is left unstored and the subscriber restarts so no later settlement commits past it. The partition is stalled until the dead-letter topic is fixed.",
                message.Topic,
                message.Partition,
                message.Offset);
            throw new KafkaDeadLetterPublishFailedException(message.Topic, message.Partition, message.Offset, deadLetterException);
        }

        // Guarded like every other settlement: a rebalance revoking this partition makes
        // StoreOffset throw, and here that throw originates INSIDE the poll loop's catch arm, so
        // nothing could catch it — it faulted the poll loop after the message was already produced
        // to the dead-letter topic, and the restart dead-lettered it a second time.
        StoreOffsetAfterSettlement(message.Topic, message.Partition, message.Offset);
    }

    /// <summary>
    /// Longest <c>exceptionType</c> / <c>exceptionMessage</c> dead-letter header value, in UTF-16
    /// code units.
    /// </summary>
    internal const int MaxDeadLetterHeaderLength = 4096;

    /// <summary>The headers a dead-letter copy stamps (see <see cref="DeadLetterCoreAsync"/>).</summary>
    internal static readonly IReadOnlySet<string> BurialHeaderKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "sourceTopic",
        "sourcePartition",
        "sourceOffset",
        "consumerGroup",
        "subscriberRole",
        "attempts",
        "reason",
        "exceptionType",
        "exceptionMessage",
        "occurredAtUtc"
    };

    /// <summary>
    /// Upper bounds on the record-format framing librdkafka adds when it checks a record against
    /// <c>message.max.bytes</c> (length/offset/timestamp varints and attributes per record; the two
    /// length varints per header). Over-estimated on purpose: they only shorten diagnostics.
    /// </summary>
    private const int RecordOverheadBytes = 64;
    private const int HeaderOverheadBytes = 10;

    /// <summary>The producer's <c>message.max.bytes</c>, resolved on first use (a burial is rare).</summary>
    private int MessageMaxBytes
    {
        get
        {
            var resolved = Volatile.Read(ref _messageMaxBytes);
            if (resolved == 0)
            {
                resolved = KafkaProducerClientAdapter.ResolveMessageMaxBytes(TransportOptions);
                Volatile.Write(ref _messageMaxBytes, resolved);
            }

            return resolved;
        }
    }

    private static bool HasHeader(IReadOnlyList<KafkaTransportHeader> headers, string key)
    {
        foreach (var header in headers)
        {
            if (string.Equals(header.Key, key, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private static long EstimateRecordSize(string? key, byte[] payload, IReadOnlyList<KafkaTransportHeader> headers)
    {
        long size = RecordOverheadBytes + payload.Length + (key is null ? 0 : Encoding.UTF8.GetByteCount(key));
        foreach (var header in headers)
            size += HeaderOverheadBytes + Encoding.UTF8.GetByteCount(header.Key) + (header.Value?.Length ?? 0);

        return size;
    }

    /// <summary>The longest well-formed prefix of <paramref name="value"/> that encodes to at most <paramref name="maxBytes"/> UTF-8 bytes.</summary>
    private static string TruncateToUtf8Bytes(string value, long maxBytes)
    {
        if (maxBytes <= 0)
            return string.Empty;
        if (Encoding.UTF8.GetByteCount(value) <= maxBytes)
            return value;

        // Every UTF-16 unit encodes to at least one byte, so the answer is at most maxBytes units.
        int low = 0, high = (int)Math.Min(value.Length, maxBytes);
        while (low < high)
        {
            var mid = low + ((high - low + 1) / 2);
            if (Encoding.UTF8.GetByteCount(PortableText.TruncateWellFormed(value, mid)) <= maxBytes)
                low = mid;
            else
                high = mid - 1;
        }

        return PortableText.TruncateWellFormed(value, low);
    }

    private async Task DeadLetterCoreAsync(
        string sourceTopic,
        int partition,
        long offset,
        byte[] payload,
        IReadOnlyList<KafkaTransportHeader> originalHeaders,
        string? correlationId,
        Exception exception,
        string reason,
        int attempts,
        CancellationToken cancellationToken)
    {
        if (!TransportOptions.DeadLetterEnabled)
            return;

        // A record replayed from a dead-letter topic already carries a burial set: replaced, not
        // stacked — a second sourceTopic/reason/... behind the first left every first-match reader
        // (tooling, a human with kcat) looking at the stale values, and each replay cycle grew the
        // record toward the size limit. Only a record that carries one (sourceTopic AND
        // sourceOffset) is stripped: the names are generic, and a user's own "reason" or
        // "attempts" header on a record that was never buried is part of the message.
        var replayedBurial = HasHeader(originalHeaders, "sourceTopic") && HasHeader(originalHeaders, "sourceOffset");
        var headers = new List<KafkaTransportHeader>(originalHeaders.Count + BurialHeaderKeys.Count);
        foreach (var header in originalHeaders)
        {
            if (!replayedBurial || !BurialHeaderKeys.Contains(header.Key))
                headers.Add(header);
        }

        headers.Add(KafkaTransportHeader.Utf8("sourceTopic", sourceTopic));
        headers.Add(KafkaTransportHeader.Utf8("sourcePartition", partition.ToString(CultureInfo.InvariantCulture)));
        headers.Add(KafkaTransportHeader.Utf8("sourceOffset", offset.ToString(CultureInfo.InvariantCulture)));
        headers.Add(KafkaTransportHeader.Utf8("consumerGroup", _consumerGroup));
        headers.Add(KafkaTransportHeader.Utf8("subscriberRole", _role.ToString()));
        headers.Add(KafkaTransportHeader.Utf8("attempts", attempts.ToString(CultureInfo.InvariantCulture)));
        headers.Add(KafkaTransportHeader.Utf8("reason", reason));
        headers.Add(KafkaTransportHeader.Utf8("occurredAtUtc", DateTimeOffset.UtcNow.ToString("O")));

        // Capped: these two are the only dead-letter headers whose size the failing code decides
        // (an exception message can quote a whole payload; a closed generic's name nests without
        // bound), and an uncapped one pushed the dead-letter record past message.max.bytes — a
        // burial that then fails on every retry, for a message that was itself within the limit.
        // Surrogate-aware cut (see PortableText.TruncateWellFormed). The per-header cap alone did
        // not close that: a record within ~24 KB of the producer's limit still could not be buried
        // (and restarted the subscriber for ever, see KafkaDeadLetterPublishFailedException), so
        // both are also cut to what the record's own size leaves — the message first.
        var exceptionType = PortableText.TruncateWellFormed(exception.GetType().FullName!, MaxDeadLetterHeaderLength);
        var exceptionMessage = PortableText.TruncateWellFormed(exception.Message, MaxDeadLetterHeaderLength);
        var room = MessageMaxBytes - EstimateRecordSize(correlationId, payload, headers)
            - (2 * HeaderOverheadBytes) - Encoding.UTF8.GetByteCount("exceptionType") - Encoding.UTF8.GetByteCount("exceptionMessage");
        var typeBytes = Encoding.UTF8.GetByteCount(exceptionType);
        if (typeBytes + Encoding.UTF8.GetByteCount(exceptionMessage) > room)
        {
            exceptionMessage = TruncateToUtf8Bytes(exceptionMessage, room - typeBytes);
            exceptionType = TruncateToUtf8Bytes(exceptionType, room - Encoding.UTF8.GetByteCount(exceptionMessage));
        }

        headers.Add(KafkaTransportHeader.Utf8("exceptionType", exceptionType));
        headers.Add(KafkaTransportHeader.Utf8("exceptionMessage", exceptionMessage));

        // The unprocessable-message discard blocks the poll thread on this (the awaiting
        // dispatcher's burial runs inside the detached handler task now, but keeps the same bound
        // so a partition is not parked on an undeliverable dead-letter topic for message.timeout.ms
        // per attempt either): a produce to an undeliverable dead-letter topic waits out
        // librdkafka's message.timeout.ms (5 min by default) PER attempt — past
        // max.poll.interval.ms, which evicted the consumer mid-burial and rebalanced the partition
        // to a peer that hit the same message: a rebalance storm at zero throughput. Bound the whole
        // ladder to a quarter of the poll interval; every caller already treats a failed burial as
        // "offset left unstored, retried after restart/rebalance".
        using var pollBudget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        pollBudget.CancelAfter(TimeSpan.FromTicks(_subscriberOptions.MaxPollInterval.Ticks / 4));

        await KafkaTransportRetry.ExecuteAsync(
            token => _producer.PublishAsync(
                _topics.DeadLetterTopicFor(sourceTopic),
                correlationId,
                payload,
                headers,
                token),
            TransportOptions.PublishMaxAttempts,
            TransportOptions.PublishRetryBaseDelay,
            TransportOptions.PublishRetryMaxDelay,
            pollBudget.Token).ConfigureAwait(false);
    }

    /// <summary>Runs the NotifyBackgroundFailureAsync operation.</summary>
    protected async ValueTask NotifyBackgroundFailureAsync(
        KafkaDelivery delivery,
        Exception exception)
    {
        var callback = _subscriberOptions.OnBackgroundFailure;
        if (callback is null)
            return;

        try
        {
            await callback(new KafkaBackgroundFailureContext(
                delivery.Topic,
                _consumerGroup,
                _role.ToString(),
                delivery.Partition,
                delivery.Offset,
                delivery.CorrelationId,
                exception)).ConfigureAwait(false);
        }
        catch (Exception callbackException)
        {
            Logger.LogError(
                callbackException,
                "Kafka background failure callback failed for already-committed message {Topic}[{Partition}]@{Offset}.",
                delivery.Topic,
                delivery.Partition,
                delivery.Offset);
        }
    }
}

/// <summary>
/// Ack-after-handler mode. A message's handler is started the moment it is consumed and awaited
/// inline for up to <see cref="KafkaSubscriberOptions.DetachHandlerAfter"/>; a handler still
/// running past that is <em>detached</em>: its partition is paused (Kafka's own ordering primitive
/// — nothing for it is fetched, nothing is buffered in-process), the handler and its retry ladder
/// run on, and the poll thread returns to polling. The earlier design awaited the whole handler on
/// the poll thread: a durable-flow step awaiting a remote response or a timer for longer than
/// <c>max.poll.interval.ms</c> (5 minutes by default) got the consumer evicted from its group, its
/// partitions rebalanced, the message redelivered to a peer that started the same work again,
/// and every other partition assigned to this consumer stalled behind it.
/// <para>
/// The consumer is touched only from the poll thread: detached handlers never store offsets or
/// resume partitions themselves. The poll loop calls <see cref="SettleCompleted"/> every tick,
/// which observes finished handlers exactly as the inline path would — success stores the offset,
/// cancellation leaves it unstored for redelivery, a burial that failed for good faults the poll
/// loop so nothing is ever committed past the message — then starts the next message held for the
/// partition, or resumes it. Disposal (the poll loop has exited by then) waits for the remaining
/// detached handlers and settles them before the consumer's close commits, so finished work is
/// not redelivered by a routine stop.
/// </para>
/// </summary>
internal sealed class AwaitingKafkaMessageDispatcher : KafkaMessageDispatcher
{
    private readonly TimeSpan _detachAfter;
    private readonly TimeSpan _faultDrainTimeout;
    private readonly string _topic;

    // Poll-thread-only: the loop is the sole caller of Accept/SettleCompleted, and DisposeAsync
    // runs after it has exited. No lock.
    private readonly Dictionary<int, DetachedPartition> _detached = [];

    /// <summary>
    /// Partitions whose message was handed back by the flow engine because the host is stopping
    /// (<see cref="DurableFlowInterruptedException"/>). Their offset is left unstored and the
    /// partition stays paused: storing any LATER offset of the partition would commit past the
    /// handed-back message (Kafka commits a position), so nothing more of it is settled until the
    /// subscriber stops. Poll-thread-only, like <see cref="_detached"/>.
    /// </summary>
    private readonly HashSet<int> _handedBack = [];

    /// <summary>Runs the AwaitingKafkaMessageDispatcher operation.</summary>
    public AwaitingKafkaMessageDispatcher(
        Func<KafkaDelivery, CancellationToken, Task> handler,
        IKafkaConsumerClient consumer,
        IKafkaProducerClient producer,
        KafkaAsyncResponseTransportOptions transportOptions,
        KafkaSubscriberOptions subscriberOptions,
        ILogger logger,
        string topic,
        string consumerGroup,
        KafkaSubscriberRole role)
        : base(handler, consumer, producer, transportOptions, subscriberOptions, logger, topic, consumerGroup, role)
    {
        _detachAfter = subscriberOptions.DetachHandlerAfter;
        _faultDrainTimeout = subscriberOptions.FaultDrainTimeout;
        _topic = topic;
    }

    /// <summary>Partitions whose handler is currently detached (test observability).</summary>
    internal int DetachedCount => _detached.Count;

    public override bool HasDetachedWork => _detached.Count > 0;

    /// <summary>Handles the delivered message inline through to the offset store (the unit-test and inline-path contract).</summary>
    public override async Task HandleAsync(KafkaDelivery delivery, CancellationToken subscriberCancellationToken)
    {
        await SettleAsync(delivery, subscriberCancellationToken).ConfigureAwait(false);
        StoreOffsetAfterSettlement(delivery);
    }

    /// <inheritdoc />
    public override void Accept(KafkaDelivery delivery, CancellationToken subscriberCancellationToken)
    {
        if (SkipHandedBackPartition(delivery.Partition, delivery.Offset))
            return;

        if (TryHoldBehindDetached(delivery.Partition, HeldMessage.For(delivery)))
            return;

        // Started on the pool, not inline: the inline wait below is a real bound on the poll
        // thread's gap even for a handler whose synchronous prefix is long.
        var settlement = Task.Run(() => SettleAsync(delivery, subscriberCancellationToken), CancellationToken.None);
        if (WaitInline(settlement))
        {
            if (settlement.IsCanceled)
            {
                // A stop — this subscriber's own, or the flow engine's host-stop hand-back, which
                // arrives while this subscriber's token may still be live. Not a failure to retry
                // or bury, and never a skip: the offset stays unstored and the partition parked.
                HandBack(delivery.Partition, delivery.Offset, held: null);
                return;
            }

            // The fast path, unchanged: settle in place and consume the next message.
            settlement.GetAwaiter().GetResult();
            StoreOffsetAfterSettlement(delivery);
            return;
        }

        // Read only now, for a handler that detaches — the fast path above never needs it — and
        // still the generation the message was consumed under: rebalance callbacks run only inside
        // Consume, which the poll thread has not called since.
        var generation = Consumer.GetAssignmentGeneration(delivery.Topic, delivery.Partition);
        PausePartition(delivery.Partition);
        _detached[delivery.Partition] = new DetachedPartition(delivery, settlement, subscriberCancellationToken, generation);
        Logger.LogDebug(
            "Kafka handler for {Topic}[{Partition}]@{Offset} is still running after {DetachAfter}; detached it and paused the partition while polling continues.",
            delivery.Topic,
            delivery.Partition,
            delivery.Offset,
            _detachAfter);
    }

    /// <inheritdoc />
    public override void AcceptUnprocessable(KafkaIncomingMessage message, Exception failure, CancellationToken subscriberCancellationToken)
    {
        if (SkipHandedBackPartition(message.Partition, message.Offset))
            return;

        // Same rule as a valid delivery for the partition: the message consumed before it is
        // still being handled, so this one waits its turn. Settling it now would store — and
        // let the auto-committer commit — an offset PAST the unfinished message; a crash
        // after that commit skipped the unfinished message for good, and the dead-letter
        // copy this discard produces is of the malformed record, not of the work that was
        // lost. Held, it is buried and its offset stored in order, once the handler settles.
        if (TryHoldBehindDetached(message.Partition, HeldMessage.Unprocessable(message, failure)))
        {
            Logger.LogDebug(
                "Kafka message {Topic}[{Partition}]@{Offset} could not be parsed into a delivery and is held behind the partition's detached handler; it is dead-lettered in order once that handler settles.",
                message.Topic,
                message.Partition,
                message.Offset);
            return;
        }

        // Nothing earlier on the partition is unresolved (every earlier message settled inline
        // or would be in _detached), so the discard is safe to settle at once.
        base.AcceptUnprocessable(message, failure, subscriberCancellationToken);
    }

    /// <summary>
    /// A message for a partition whose earlier message was handed back at host stop: dropped
    /// unsettled (offset unstored, so it redelivers with the partition) and the pause re-asserted
    /// — a rebalance hands a partition back with its pause state reset.
    /// </summary>
    private bool SkipHandedBackPartition(int partition, long offset)
    {
        if (!_handedBack.Contains(partition))
            return false;

        PausePartition(partition);
        Logger.LogDebug(
            "Kafka message {Topic}[{Partition}]@{Offset} is left unsettled: an earlier message of the partition was handed back because the host is stopping.",
            _topic,
            partition,
            offset);
        return true;
    }

    /// <summary>
    /// Holds <paramref name="message"/> behind the partition's detached handler when that handler
    /// still belongs to the partition's CURRENT assignment. A handler whose partition was revoked
    /// since it detached is judged by this first message of the new assignment, which starts at
    /// the group's committed offset: one PAST the handler's message means another member committed
    /// past it, so the handler is orphaned — its offset can never be stored (that would rewind the
    /// group), and the new message is settled normally while the orphan runs on unobserved by the
    /// consumer, however long it still takes. One AT OR BEFORE it means nobody moved past it — an
    /// eager rebalance revokes every partition and typically hands most straight back — so the
    /// handler is re-adopted into the current assignment and the redelivered message waits behind
    /// it, instead of a second copy of the same message running alongside the first.
    /// </summary>
    private bool TryHoldBehindDetached(int partition, HeldMessage message)
    {
        if (!_detached.TryGetValue(partition, out var inFlight))
            return false;

        var generation = Consumer.GetAssignmentGeneration(inFlight.Delivery.Topic, partition);
        if (generation != inFlight.Generation)
        {
            if (message.Offset > inFlight.Delivery.Offset)
            {
                _detached.Remove(partition);
                ObserveOrphaned(inFlight, committedPast: true);
                return false;
            }

            inFlight.Generation = generation;
            Logger.LogDebug(
                "Kafka partition {Topic}[{Partition}] came back to this consumer at offset {Offset}, at or before its still-running detached handler @{RunningOffset}; the handler keeps the partition and the redelivered messages wait behind it.",
                _topic,
                partition,
                message.Offset,
                inFlight.Delivery.Offset);
        }

        // A message for a partition whose handler is still running: a rebalance handed the
        // partition back with its pause reset (the consumer adapter lifts it on every revoke),
        // or the client delivered a message it had fetched before the pause. Hold it behind
        // the running one — the partition's order is the contract — and re-assert the pause
        // so nothing more arrives; the hold is therefore bounded by what was already in
        // flight, never a queue that grows.
        (inFlight.Held ??= new Queue<HeldMessage>()).Enqueue(message);
        PausePartition(partition);
        return true;
    }

    /// <summary>Whether the partition was revoked (or lost) since <paramref name="work"/> detached.</summary>
    private bool IsOrphaned(DetachedPartition work)
        => Consumer.GetAssignmentGeneration(work.Delivery.Topic, work.Delivery.Partition) != work.Generation;

    /// <summary>
    /// Ends a settlement that was stopped rather than finished: the offset stays unstored, the
    /// messages held behind it are dropped unstarted (their offsets unstored too), and the
    /// partition stays paused and parked for the rest of this consumer's life — the host is
    /// stopping, and storing any later offset of the partition would commit past this message.
    /// </summary>
    private void HandBack(int partition, long offset, Queue<HeldMessage>? held)
    {
        _handedBack.Add(partition);
        PausePartition(partition);
        Logger.LogInformation(
            "Kafka message {Topic}[{Partition}]@{Offset} was handed back because the host is stopping; its offset is left unstored and the partition parked{Held}, so the message redelivers after the restart.",
            _topic,
            partition,
            offset,
            held is { Count: > 0 } ? $" ({held.Count} message(s) held behind it are left unsettled too)" : string.Empty);
    }

    /// <inheritdoc />
    public override void SettleCompleted()
    {
        if (_detached.Count == 0)
            return;

        List<int>? finished = null;
        foreach (var (partition, work) in _detached)
        {
            if (work.Settlement.IsCompleted)
                (finished ??= []).Add(partition);
        }

        if (finished is null)
            return;

        foreach (var partition in finished)
        {
            var work = _detached[partition];
            // Removed BEFORE it is observed: a settlement that throws faults the poll loop, and the
            // entry must not be settled a second time by disposal.
            _detached.Remove(partition);

            if (IsOrphaned(work))
            {
                // Its partition moved away (and may have come back) while it ran, and no message of
                // a new assignment re-adopted it: never store its offset, never start what was held
                // behind it — the partition's current owner re-consumes all of it from the group's
                // committed offset.
                ObserveOrphaned(work);
                continue;
            }

            if (work.Settlement.IsCanceled)
            {
                HandBack(partition, work.Delivery.Offset, work.Held);
                continue;
            }

            work.Settlement.GetAwaiter().GetResult();
            StoreOffsetAfterSettlement(work.Delivery);
            ContinueHeld(partition, work.Held, work.Delivery.Offset, work.SubscriberCancellationToken);
        }
    }

    /// <summary>
    /// Works through the messages held behind a settled handler, in consumption order: one at or
    /// below the settled offset is a redelivery of work this consumer has already settled (a
    /// re-adopting assignment re-fetched from the group's committed offset) and is dropped; an
    /// unprocessable one is dead-lettered and its offset stored right here (its turn has come —
    /// never ahead of the handler it was consumed behind); the first valid delivery is started
    /// detached with the rest still held behind it (the partition stays paused); an empty hold
    /// resumes the partition.
    /// </summary>
    private void ContinueHeld(int partition, Queue<HeldMessage>? held, long settledOffset, CancellationToken subscriberCancellationToken)
    {
        while (held is { Count: > 0 })
        {
            var next = held.Dequeue();
            if (next.Offset <= settledOffset)
            {
                Logger.LogDebug(
                    "Kafka message {Topic}[{Partition}]@{Offset} is a redelivery of work this consumer already settled (up to @{SettledOffset}); dropped.",
                    _topic,
                    partition,
                    next.Offset,
                    settledOffset);
                continue;
            }

            if (next.Delivery is { } delivery)
            {
                var generation = Consumer.GetAssignmentGeneration(delivery.Topic, delivery.Partition);
                var settlement = Task.Run(() => SettleAsync(delivery, subscriberCancellationToken), CancellationToken.None);
                _detached[partition] = new DetachedPartition(delivery, settlement, subscriberCancellationToken, generation) { Held = held.Count > 0 ? held : null };
                return;
            }

            // A burial that fails for good throws out of here and faults the poll loop, exactly
            // as an inline discard would; whatever is still held redelivers with the partition.
            DiscardUnprocessableAsync(next.Message!, next.Failure!, subscriberCancellationToken).GetAwaiter().GetResult();
        }

        ResumePartition(partition);
    }

    /// <summary>
    /// The poll loop has exited (a stop, or a fault). Detached handlers run on — the handler takes
    /// no cancellation token the ingress would honor — so wait for each and settle it exactly as the
    /// poll thread would have: an offset stored here is committed by the consumer close that
    /// follows, and finished work is not redelivered by a routine stop. Unbounded, as the inline
    /// path was (the host's shutdown budget bounds the stop as a whole). Messages still held behind
    /// a detached handler are dropped unstarted: their offsets are unstored, so they redeliver.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        if (_detached.Count == 0)
            return;

        Logger.LogInformation(
            "Waiting for {Count} detached Kafka handler(s) on {Topic} to settle before the consumer closes.",
            _detached.Count,
            _topic);

        foreach (var (partition, work) in _detached.ToArray())
        {
            _detached.Remove(partition);
            await SettleAfterLoopExitAsync(work).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The poll loop FAILED and the consumer is about to be closed and rebuilt. Waits at most
    /// <see cref="KafkaSubscriberOptions.FaultDrainTimeout"/> for the detached handlers: those
    /// that settled get their offsets stored, exactly as the poll thread would have (the close
    /// that follows commits them); the rest are abandoned — offsets unstored, so their messages
    /// redeliver on the rebuilt consumer while the abandoned handler may still be running — and
    /// observed, so each one's eventual outcome is logged instead of vanishing. Messages held
    /// behind a detached handler are dropped unstarted, as on a stop. The unbounded wait this
    /// replaces on the fault path let one long handler (a durable-flow step awaiting a remote
    /// response) hold the subscriber's reconnect for its whole duration, so a transient broker
    /// failure disabled every partition of the subscriber for as long as that step took and the
    /// configured reconnect policy never ran.
    /// </summary>
    public override async ValueTask TeardownAfterFaultAsync()
    {
        if (_detached.Count == 0)
            return;

        Logger.LogInformation(
            "Kafka poll loop for {Topic} failed with {Count} detached handler(s) still running; waiting up to {FaultDrainTimeout} for them before the consumer is rebuilt.",
            _topic,
            _detached.Count,
            _faultDrainTimeout);

        if (_faultDrainTimeout > TimeSpan.Zero)
        {
            var settlements = new Task[_detached.Count];
            var index = 0;
            foreach (var work in _detached.Values)
                settlements[index++] = work.Settlement;

            try
            {
                await Task.WhenAll(settlements).WaitAsync(_faultDrainTimeout).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // A timeout, or a settlement that faulted or was canceled: each one is observed
                // individually below.
            }
        }

        foreach (var (partition, work) in _detached.ToArray())
        {
            _detached.Remove(partition);
            if (work.Settlement.IsCompleted)
            {
                await SettleAfterLoopExitAsync(work).ConfigureAwait(false);
                continue;
            }

            Logger.LogWarning(
                "Abandoning detached Kafka handler for {Topic}[{Partition}]@{Offset}: still running {FaultDrainTimeout} after the poll loop failed. Its offset is left unstored, so the message redelivers on the rebuilt consumer — possibly while this handler is still running; its outcome is logged when it settles.",
                work.Delivery.Topic,
                work.Delivery.Partition,
                work.Delivery.Offset,
                _faultDrainTimeout);
            ObserveAbandoned(work);
        }
    }

    /// <summary>
    /// Settles a detached handler after the poll loop has exited (a stop, or a fault whose budget
    /// it finished within): its offset is stored for the consumer close to commit, a cancellation
    /// or failure leaves it unstored so the message redelivers.
    /// </summary>
    private async Task SettleAfterLoopExitAsync(DetachedPartition work)
    {
        if (IsOrphaned(work))
        {
            // Its partition was revoked while it ran: there is no offset of ours to store, so the
            // stop does not wait for it either.
            ObserveOrphaned(work);
            return;
        }

        try
        {
            await work.Settlement.ConfigureAwait(false);
            StoreOffsetAfterSettlement(work.Delivery);
        }
        catch (OperationCanceledException)
        {
            Logger.LogInformation(
                "Detached Kafka handler for {Topic}[{Partition}]@{Offset} was canceled by the stop; its offset is left unstored and the message redelivers.",
                work.Delivery.Topic,
                work.Delivery.Partition,
                work.Delivery.Offset);
        }
        catch (Exception ex)
        {
            Logger.LogError(
                ex,
                "Detached Kafka handler for {Topic}[{Partition}]@{Offset} failed while the subscriber was stopping; its offset is left unstored and the message redelivers.",
                work.Delivery.Topic,
                work.Delivery.Partition,
                work.Delivery.Offset);
        }
    }

    /// <summary>
    /// Logs the eventual outcome of a handler whose partition was revoked while it ran. Like an
    /// abandoned one it never touches the consumer: the message belongs to the partition's current
    /// owner, which re-consumes it from the group's committed offset — storing this one's offset
    /// would rewind that owner's commits. <paramref name="committedPast"/>: the new assignment
    /// started past the message — the group has already committed past it, so nobody re-consumes it.
    /// </summary>
    private void ObserveOrphaned(DetachedPartition work, bool committedPast = false)
    {
        if (committedPast)
        {
            Logger.LogInformation(
                "Detached Kafka handler for {Topic}[{Partition}]@{Offset} outlived its partition's assignment (a rebalance revoked it) and the group has already committed past its message; its offset will not be stored and nothing is held behind it — the message is not redelivered.",
                work.Delivery.Topic,
                work.Delivery.Partition,
                work.Delivery.Offset);
        }
        else
        {
            Logger.LogInformation(
                "Detached Kafka handler for {Topic}[{Partition}]@{Offset} outlived its partition's assignment (a rebalance revoked it); its offset will not be stored and nothing is held behind it — the partition's current owner redelivers the message.",
                work.Delivery.Topic,
                work.Delivery.Partition,
                work.Delivery.Offset);
        }

        ObserveAbandoned(work, revoked: true, committedPast);
    }

    /// <summary>
    /// Logs the eventual outcome of a handler the fault teardown abandoned (or whose partition a
    /// rebalance revoked, <paramref name="revoked"/>). It never touches the consumer — the one it
    /// was consumed on is closed by then, or no longer holds its assignment — so the outcome is
    /// informational: the message has already been handed back to the group for redelivery —
    /// unless <paramref name="committedPast"/>, when the group has already committed past it.
    /// </summary>
    private void ObserveAbandoned(DetachedPartition work, bool revoked = false, bool committedPast = false)
        => _ = work.Settlement.ContinueWith(
            static (settlement, state) =>
            {
                var (logger, delivery, revoked, committedPast) = ((ILogger, KafkaDelivery, bool, bool))state!;
                var kind = revoked ? "Orphaned" : "Abandoned";
                var where = revoked
                    ? "after a rebalance revoked its partition"
                    : "after the consumer it was consumed on was rebuilt";
                var redelivery = committedPast
                    ? "the group had already committed past the message, so it is not redelivered"
                    : revoked
                        ? "its offset was never stored, so the partition's current owner redelivers the message"
                        : "its offset was never stored, so the message redelivers on the rebuilt consumer";
                if (settlement.IsCanceled)
                {
                    logger.LogInformation(
                        "{Kind} Kafka handler for {Topic}[{Partition}]@{Offset} stopped on cancellation {Where}; {Redelivery}.",
                        kind,
                        delivery.Topic,
                        delivery.Partition,
                        delivery.Offset,
                        where,
                        redelivery);
                }
                else if (settlement.IsFaulted)
                {
                    logger.LogWarning(
                        settlement.Exception!.GetBaseException(),
                        "{Kind} Kafka handler for {Topic}[{Partition}]@{Offset} failed {Where}; {Redelivery}.",
                        kind,
                        delivery.Topic,
                        delivery.Partition,
                        delivery.Offset,
                        where,
                        redelivery);
                }
                else
                {
                    logger.LogInformation(
                        "{Kind} Kafka handler for {Topic}[{Partition}]@{Offset} completed {Where}; {Redelivery} (handlers are at-least-once).",
                        kind,
                        delivery.Topic,
                        delivery.Partition,
                        delivery.Offset,
                        where,
                        redelivery);
                }
            },
            (Logger, work.Delivery, revoked, committedPast),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

    /// <summary>
    /// Runs the handler with the in-process retry ladder and, at the delivery cap, the dead-letter
    /// burial — everything but the offset store, which the poll thread performs once this returns.
    /// Returns normally when the message is settled (handled, or buried); throws on cancellation
    /// (offset not stored: redelivered after restart or rebalance) and when the burial fails for
    /// good (<see cref="KafkaDeadLetterPublishFailedException"/>).
    /// </summary>
    private async Task SettleAsync(KafkaDelivery delivery, CancellationToken subscriberCancellationToken)
    {
        var attempt = 0;
        while (true)
        {
            attempt++;
            try
            {
                await ExecuteHandlerAsync(delivery, attempt, subscriberCancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex) when (subscriberCancellationToken.IsCancellationRequested || ex is DurableFlowInterruptedException)
            {
                // Offset not stored: the message is redelivered after restart or rebalance. The
                // flow engine's host-stop hand-back is recognized by TYPE: the host fires
                // ApplicationStopping before it stops any hosted service, so it arrives while this
                // subscriber's own token is still live — and treated as a failure it ran the
                // retry ladder against a stopping host and, past it, dead-lettered a flow's only
                // wake-up and stored its offset.
                throw;
            }
            catch (Exception ex)
            {
                if (ReachedDeliveryAttempts(attempt))
                {
                    Logger.LogWarning(
                        ex,
                        "Kafka message {Topic}[{Partition}]@{Offset} reached max delivery attempts ({MaxDeliveryAttempts}); producing to dead-letter topic.",
                        delivery.Topic,
                        delivery.Partition,
                        delivery.Offset,
                        MaxDeliveryAttempts);
                    // A permanently failing dead-letter topic (UnknownTopicOrPart with auto-create
                    // off, an over-sized payload) burns the publish retries and then throws. That
                    // throw is deliberately NOT swallowed: an earlier round swallowed it, leaving
                    // the offset unstored and consumption running, and the next successful
                    // settlement on the same partition then stored a higher offset — the
                    // auto-committer committed past this message and a restart skipped it with no
                    // dead-letter copy. Faulting the subscriber (KafkaDeadLetterPublishFailedException)
                    // stalls the partition AT this message: the consumer closes without storing
                    // past it, the supervisor restarts it after its backoff, and the handler and
                    // burial are retried per restart — a loud, bounded-rate loop until the
                    // dead-letter topic is fixed, which is the at-least-once outcome.
                    try
                    {
                        await DeadLetterAsync(
                            delivery,
                            ex,
                            "handler_failed_max_attempts",
                            attempt,
                            CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception deadLetterException)
                    {
                        Logger.LogError(
                            deadLetterException,
                            "Failed to dead-letter Kafka message {Topic}[{Partition}]@{Offset} at the delivery cap; its offset is left unstored and the subscriber restarts so no later settlement commits past it. The partition is stalled until the dead-letter topic is fixed.",
                            delivery.Topic,
                            delivery.Partition,
                            delivery.Offset);
                        throw new KafkaDeadLetterPublishFailedException(delivery.Topic, delivery.Partition, delivery.Offset, deadLetterException);
                    }

                    return;
                }

                // Kafka offsets cannot NACK one message, so retry in-process with backoff. This
                // stalls the message's partition (head-of-line), which is inherent to classic
                // consumer groups — and only that partition: past DetachHandlerAfter the ladder
                // runs detached from the poll thread.
                await Task.Delay(RetryBackoff(attempt), subscriberCancellationToken).ConfigureAwait(false);
                continue;
            }

            // Settlement sits OUTSIDE the handler try (parity with the queued dispatcher and every
            // sibling transport): a StoreOffset failure after a successful handler — routine when a
            // rebalance revoked the partition mid-handler — must not be misread as a handler
            // failure that re-runs, or dead-letters, work that already succeeded.
            return;
        }
    }

    /// <summary>
    /// Blocks the poll thread for at most the inline budget. <c>true</c> when the settlement task
    /// finished (in any state — the caller observes it); <c>false</c> when it is still running.
    /// </summary>
    private bool WaitInline(Task settlement)
    {
        if (settlement.IsCompleted)
            return true;
        if (_detachAfter <= TimeSpan.Zero)
            return false;

        try
        {
            return settlement.Wait(_detachAfter);
        }
        catch (AggregateException)
        {
            // Completed, faulted: the caller re-awaits it and gets the original exception.
            return true;
        }
    }

    private void PausePartition(int partition)
    {
        try
        {
            Consumer.PausePartition(_topic, partition);
        }
        catch (Exception ex)
        {
            // Not assigned any more (a rebalance took it): nothing to pause, nothing arrives for it,
            // and the running handler's outcome is settled like any other when it finishes.
            Logger.LogDebug(ex, "Could not pause {Topic}[{Partition}] behind its detached handler; the partition is no longer assigned to this consumer.", _topic, partition);
        }
    }

    private void ResumePartition(int partition)
    {
        try
        {
            Consumer.ResumePartition(_topic, partition);
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "Could not resume {Topic}[{Partition}] after its detached handler settled; the partition is no longer assigned to this consumer.", _topic, partition);
        }
    }

    private sealed class DetachedPartition(KafkaDelivery delivery, Task settlement, CancellationToken subscriberCancellationToken, long generation)
    {
        public KafkaDelivery Delivery { get; } = delivery;
        public Task Settlement { get; } = settlement;
        public CancellationToken SubscriberCancellationToken { get; } = subscriberCancellationToken;

        /// <summary>
        /// The partition's assignment generation when the message was consumed (see
        /// <see cref="IKafkaConsumerClient.GetAssignmentGeneration"/>), or the later one that
        /// re-adopted the handler (see <see cref="TryHoldBehindDetached"/>).
        /// </summary>
        public long Generation { get; set; } = generation;

        /// <summary>Messages consumed for the partition while its handler was detached, in order.</summary>
        public Queue<HeldMessage>? Held { get; set; }
    }

    /// <summary>
    /// One message consumed behind a detached handler: a valid delivery, or one that could not
    /// be projected (kept with the failure that rejected it, for the dead-letter headers).
    /// </summary>
    private readonly record struct HeldMessage(KafkaDelivery? Delivery, KafkaIncomingMessage? Message, Exception? Failure)
    {
        public static HeldMessage For(KafkaDelivery delivery) => new(delivery, null, null);

        public static HeldMessage Unprocessable(KafkaIncomingMessage message, Exception failure) => new(null, message, failure);

        public long Offset => Delivery?.Offset ?? Message!.Offset;
    }
}

internal sealed class QueuedKafkaMessageDispatcher : KafkaMessageDispatcher
{
    private readonly Channel<KafkaDelivery> _queue;
    private readonly Task[] _workers;
    private readonly CancellationTokenSource _drainCancellation = new();
    private readonly TimeSpan _drainTimeout;
    private readonly int _capacity;
    private readonly string _topic;
    private int _pendingCount;
    private int _runningCount;
    private int _disposeStarted;

    /// <summary>Runs the QueuedKafkaMessageDispatcher operation.</summary>
    public QueuedKafkaMessageDispatcher(
        Func<KafkaDelivery, CancellationToken, Task> handler,
        IKafkaConsumerClient? consumer,
        IKafkaProducerClient producer,
        KafkaAsyncResponseTransportOptions transportOptions,
        KafkaSubscriberOptions subscriberOptions,
        ILogger logger,
        string topic,
        string consumerGroup,
        KafkaSubscriberRole role)
        : base(handler, consumer, producer, transportOptions, subscriberOptions, logger, topic, consumerGroup, role)
    {
        _topic = topic;
        _drainTimeout = subscriberOptions.BackgroundDrainTimeout;
        _capacity = subscriberOptions.BackgroundQueueCapacity;
        _queue = Channel.CreateBounded<KafkaDelivery>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
        {
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.Wait,
            // Never single-reader: once the drain budget lapses, DisposeAsync reads the queue
            // alongside the workers to route what is still queued.
            SingleReader = false,
            SingleWriter = false
        });

        _workers = Enumerable.Range(0, subscriberOptions.BackgroundWorkerCount)
            .Select(workerIndex => Task.Run(() => RunWorkerAsync(workerIndex)))
            .ToArray();

        Logger.LogInformation(
            "Created Kafka ACK-after-enqueue dispatcher for {Topic} with {WorkerCount} worker(s), queue capacity {QueueCapacity}, drain timeout {DrainTimeout}.",
            _topic,
            subscriberOptions.BackgroundWorkerCount,
            subscriberOptions.BackgroundQueueCapacity,
            _drainTimeout);
    }

    internal int PendingCount => Volatile.Read(ref _pendingCount);
    internal int RunningCount => Volatile.Read(ref _runningCount);

    public override bool CanAcceptMore => Volatile.Read(ref _pendingCount) < _capacity;

    // The queue and its workers belong to the hosted service, not to one supervised attempt: the
    // workers never touch the consumer (only the poll thread's enqueue stores an offset, through
    // the attached one), so nothing in here is per attempt, and disposing it IS the stop-time
    // drain — owned per attempt, every poll-loop fault on a healthy host paused the whole
    // subscriber for the drain budget and then dead-lettered committed work unstarted as
    // "drain budget lapsed" (or lost it, when the fault was the dead-letter topic itself).

    /// <summary>
    /// A poll-loop fault is not a stop: the queued work keeps running and the next attempt's
    /// consumer is attached to this same instance, so there is nothing to tear down (the attempt's
    /// consumer attachment is released by the subscriber).
    /// </summary>
    public override ValueTask TeardownAfterFaultAsync() => ValueTask.CompletedTask;

    /// <summary>Handles the delivered message.</summary>
    public override async Task HandleAsync(
        KafkaDelivery delivery,
        CancellationToken subscriberCancellationToken)
    {
        Interlocked.Increment(ref _pendingCount);
        if (!_queue.Writer.TryWrite(delivery))
        {
            // The subscriber pauses partition fetching while CanAcceptMore is false, so this wait
            // only covers the race between its capacity check and this write. Unlike Redis there is
            // no pending-entry list to defer to: the message is already consumed, so it must be
            // enqueued before the loop may continue.
            try
            {
                await _queue.Writer.WriteAsync(delivery, subscriberCancellationToken).ConfigureAwait(false);
            }
            catch
            {
                Interlocked.Decrement(ref _pendingCount);
                throw;
            }
        }

        // The message now belongs to a background worker, which decrements _pendingCount when it
        // dequeues. Do not touch the counter again here, even if the offset store below fails.
        try
        {
            StoreOffset(delivery);
        }
        catch (KafkaPartitionNotAssignedException ex)
        {
            // Routine in a consumer group, as on the ack-after-handler settlement path: a
            // rebalance took the partition, and its new owner re-consumes the message.
            Logger.LogInformation(
                ex,
                "Did not store the offset of Kafka message {Topic}[{Partition}]@{Offset} after enqueue: a rebalance revoked the partition, so its new owner redelivers it while this copy is processed (at-least-once).",
                delivery.Topic,
                delivery.Partition,
                delivery.Offset);
        }
        catch (Exception ex)
        {
            Logger.LogError(
                ex,
                "Failed to store offset for Kafka message {Topic}[{Partition}]@{Offset} after enqueue; it is being processed but will be redelivered after a restart or rebalance.",
                delivery.Topic,
                delivery.Partition,
                delivery.Offset);
        }
    }

    /// <summary>Releases resources held by this instance.</summary>
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;

        Logger.LogInformation(
            "Draining Kafka ACK-after-enqueue dispatcher for {Topic}. Pending={PendingCount}, Running={RunningCount}.",
            _topic,
            PendingCount,
            RunningCount);
        _queue.Writer.TryComplete();

        // BackgroundDrainTimeout is the whole spend the shutdown-budget validator sums for this
        // dispatcher, so it is split rather than exceeded (DbTransportShared parity): most of it
        // lets queued and running handlers finish, and a quarter is RESERVED for burying what is
        // still queued once it lapses. That burial used to be left to the worker loop's lapse
        // branch alone, which only runs when a busy worker frees up — with every worker still
        // inside a long handler nothing ran it before this method returned, and the committed
        // entries still queued raced the host disposing the shared producer and the process
        // exiting: no dead-letter copy, no OnBackgroundFailure, no per-message log.
        var routingReserve = TimeSpan.FromTicks(_drainTimeout.Ticks / 4);
        var drainBudget = _drainTimeout - routingReserve;
        try
        {
            await Task.WhenAll(_workers).WaitAsync(drainBudget).ConfigureAwait(false);
            _drainCancellation.Dispose();
        }
        catch (TimeoutException ex)
        {
            _drainCancellation.Cancel();
            Logger.LogWarning(
                ex,
                "Timed out while draining Kafka ACK-after-enqueue dispatcher for {Topic}. Pending={PendingCount}, Running={RunningCount}. Dead-lettering the messages still queued; already committed work that is running may be interrupted by host shutdown.",
                _topic,
                PendingCount,
                RunningCount);

            await RouteUndrainedAsync(routingReserve).ConfigureAwait(false);

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
            Logger.LogDebug(ex, "Kafka ACK-after-enqueue dispatcher drain for {Topic} ended with an error.", _topic);
            _drainCancellation.Dispose();
        }
    }

    /// <summary>
    /// Runs inside <see cref="DisposeAsync"/> once the drain budget has lapsed, within the reserved
    /// quarter of it and before the subscriber (and then the host) disposes the shared producer:
    /// every message still queued is surfaced through
    /// <see cref="KafkaSubscriberOptions.OnBackgroundFailure"/> and dead-lettered here, instead of
    /// waiting for a worker that is still inside a long handler. Whatever the reserve cannot cover
    /// is counted in one error; workers that free up later keep routing it through their own lapse
    /// branch.
    /// </summary>
    private async Task RouteUndrainedAsync(TimeSpan reserve)
    {
        using var budget = new CancellationTokenSource(reserve);
        while (!budget.IsCancellationRequested && _queue.Reader.TryRead(out var delivery))
        {
            Interlocked.Decrement(ref _pendingCount);
            await RouteLapsedAsync(delivery, budget.Token).ConfigureAwait(false);
        }

        // A worker the cancellation cut short (mid-handler, or mid-retry-backoff) settles its own
        // delivery on the way out; the rest of the reserve is theirs, so that copy lands before
        // this method returns and the producer goes away.
        try
        {
            await Task.WhenAll(_workers).WaitAsync(budget.Token).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The reserve lapsed with a worker still inside a handler that ignores the token (or a
            // worker faulted outside its guard, which the caller's drain path already reports).
        }

        var remaining = _queue.Reader.Count;
        if (remaining > 0)
        {
            Logger.LogError(
                "{Remaining} already-committed Kafka messages on {Topic} were neither handled nor dead-lettered within the {Reserve} reserved after the drain budget lapsed; any still queued at process exit are lost (their offsets are committed).",
                remaining,
                _topic,
                reserve);
        }
    }

    /// <summary>
    /// Settlement of an already-committed message that will never be handled because the drain
    /// budget lapsed: surfaced through the callback and dead-lettered, since Kafka will not
    /// redeliver it.
    /// </summary>
    private async Task RouteLapsedAsync(KafkaDelivery delivery, CancellationToken cancellationToken)
    {
        var lapsed = new OperationCanceledException(
            "The ACK-after-enqueue drain budget lapsed before this already-committed message was handled.");
        Logger.LogWarning(
            "Kafka background handler for already-committed message {Topic}[{Partition}]@{Offset} was not started: the drain budget had lapsed. Dead-lettering and surfacing via OnBackgroundFailure.",
            delivery.Topic,
            delivery.Partition,
            delivery.Offset);
        await NotifyBackgroundFailureAsync(delivery, lapsed).ConfigureAwait(false);
        await TryDeadLetterAfterCommitAsync(delivery, lapsed, "drain_budget_lapsed_after_commit", 0, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Best-effort burial of an already-committed message: a failure is only logged (the offset is committed either way).</summary>
    private async Task TryDeadLetterAfterCommitAsync(
        KafkaDelivery delivery,
        Exception exception,
        string reason,
        int attempts,
        CancellationToken cancellationToken)
    {
        try
        {
            await DeadLetterAsync(delivery, exception, reason, attempts, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception deadLetterException)
        {
            Logger.LogError(
                deadLetterException,
                "Failed to dead-letter already-committed Kafka message {Topic}[{Partition}]@{Offset}.",
                delivery.Topic,
                delivery.Partition,
                delivery.Offset);
        }
    }

    private async Task RunWorkerAsync(int workerIndex)
    {
        await foreach (var delivery in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            Interlocked.Decrement(ref _pendingCount);

            // Once the drain budget has lapsed, STOP executing (DB/Redis/Pub-Sub parity). The
            // drain token cannot stop the real handler — it is
            // `_ingress.HandleWorkerMessageAsync(payload)`, whose target takes no
            // CancellationToken — so past the budget the loop kept starting fresh work beyond the
            // host's shutdown budget, and every entry still queued at process exit vanished with
            // no record (its offset was stored at enqueue, so Kafka never redelivers it).
            if (_drainCancellation.IsCancellationRequested)
            {
                await RouteLapsedAsync(delivery, CancellationToken.None).ConfigureAwait(false);
                continue;
            }

            Interlocked.Increment(ref _runningCount);

            try
            {
                await ExecuteWithRetryAsync(delivery).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex) when (_drainCancellation.IsCancellationRequested)
            {
                // The drain budget lapsed with this already-committed message still unsettled (its
                // handler failed and the retry backoff was cut short): Kafka will not redeliver it,
                // so surface the drop through OnBackgroundFailure AND bury it — an unstarted entry
                // is dead-lettered on this same lapse, and one that already failed once has at
                // least as much reason to leave a record.
                Logger.LogWarning(
                    "Kafka background handler for already-committed message {Topic}[{Partition}]@{Offset} was canceled during dispatcher shutdown; dead-lettering and surfacing via OnBackgroundFailure.",
                    delivery.Topic,
                    delivery.Partition,
                    delivery.Offset);
                await NotifyBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
                await TryDeadLetterAfterCommitAsync(delivery, ex, "drain_budget_lapsed_after_commit", 0, CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _runningCount);
            }
        }
    }

    private async Task ExecuteWithRetryAsync(KafkaDelivery delivery)
    {
        var attempt = 0;
        while (true)
        {
            attempt++;
            try
            {
                await ExecuteHandlerAsync(
                    delivery,
                    attempt,
                    _drainCancellation.Token,
                    logFailures: false).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (_drainCancellation.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // Unlimited retries (0) on this path spun a background worker on an already-
                // committed message forever: the bounded queue filled, the poll loop pinned the
                // assignment paused, and the subscriber wedged with no dead-letter record and no
                // OnBackgroundFailure, because both live inside this block. After an early ACK
                // the sibling transports never retry at all, so 0 means a single attempt here.
                //
                // The flow engine's host-stop hand-back ends the ladder at once, too: the offset was
                // committed at enqueue, so there is no delivery to hand back, and every retry would
                // be interrupted by the same stopping host — only the record below is left to write.
                if (ex is DurableFlowInterruptedException)
                {
                    // A hand-back, not a failure: a Warning (an Error alerted on every deploy that
                    // stopped a host mid-flow), surfaced and dead-lettered under its own reason —
                    // early ACK can never redeliver, and the flow's checkpoints make a replay of
                    // the copy safe. With dead-lettering disabled no copy is written and the
                    // wake-up is lost: that is an Error, and the log must not claim a copy.
                    if (TransportOptions.DeadLetterEnabled)
                    {
                        Logger.LogWarning(
                            ex,
                            "Kafka background handler for already-committed message {Topic}[{Partition}]@{Offset} was handed back because the host is stopping; its offset is already committed, so Kafka will not redeliver it. Dead-lettering a copy (handed_back_after_commit) and surfacing via OnBackgroundFailure.",
                            delivery.Topic,
                            delivery.Partition,
                            delivery.Offset);
                    }
                    else
                    {
                        Logger.LogError(
                            ex,
                            "Kafka background handler for already-committed message {Topic}[{Partition}]@{Offset} was handed back because the host is stopping; its offset is already committed and no dead-letter destination is configured, so the wake-up is lost unless OnBackgroundFailure records it (resume the flow explicitly).",
                            delivery.Topic,
                            delivery.Partition,
                            delivery.Offset);
                    }

                    await NotifyBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
                    await TryDeadLetterAfterCommitAsync(delivery, ex, "handed_back_after_commit", attempt, CancellationToken.None).ConfigureAwait(false);
                    return;
                }

                if (MaxDeliveryAttempts <= 0 || ReachedDeliveryAttempts(attempt))
                {
                    Logger.LogError(
                        ex,
                        "Kafka background handler failed for already-committed message {Topic}[{Partition}]@{Offset} after {Attempts} attempt(s).",
                        delivery.Topic,
                        delivery.Partition,
                        delivery.Offset,
                        attempt);
                    await NotifyBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
                    await TryDeadLetterAfterCommitAsync(delivery, ex, "background_handler_failed_after_commit", attempt, CancellationToken.None).ConfigureAwait(false);
                    return;
                }

                await Task.Delay(RetryBackoff(attempt), _drainCancellation.Token).ConfigureAwait(false);
            }
        }
    }
}
