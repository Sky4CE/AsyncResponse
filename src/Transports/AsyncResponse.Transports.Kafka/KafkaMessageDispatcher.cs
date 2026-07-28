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
internal abstract class KafkaMessageDispatcher : IAsyncDisposable
{
    private readonly Func<KafkaDelivery, CancellationToken, Task> _handler;
    private readonly KafkaSubscriberOptions _subscriberOptions;
    private readonly IKafkaConsumerClient _consumer;
    private readonly IKafkaProducerClient _producer;
    private readonly KafkaTransportTopicSchema _topics;
    private readonly string _topic;
    private readonly string _consumerGroup;
    private readonly KafkaSubscriberRole _role;

    /// <summary>Runs the KafkaMessageDispatcher operation.</summary>
    protected KafkaMessageDispatcher(
        Func<KafkaDelivery, CancellationToken, Task> handler,
        IKafkaConsumerClient consumer,
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

    protected int MaxDeliveryAttempts => _subscriberOptions.MaxDeliveryAttempts;

    /// <summary>Creates the configured dispatcher.</summary>
    public static KafkaMessageDispatcher Create(
        Func<KafkaDelivery, CancellationToken, Task> handler,
        IKafkaConsumerClient consumer,
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
            consumer,
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

        if (subscriberOptions.PollTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException($"{optionPath}.{nameof(KafkaSubscriberOptions.PollTimeout)} must be positive.");
        if (subscriberOptions.BackpressurePollDelay <= TimeSpan.Zero)
            throw new InvalidOperationException($"{optionPath}.{nameof(KafkaSubscriberOptions.BackpressurePollDelay)} must be positive.");
        if (subscriberOptions.MaxDeliveryAttempts < 0)
            throw new InvalidOperationException($"{optionPath}.{nameof(KafkaSubscriberOptions.MaxDeliveryAttempts)} cannot be negative.");
        if (subscriberOptions.HandlerRetryBaseDelay <= TimeSpan.Zero)
            throw new InvalidOperationException($"{optionPath}.{nameof(KafkaSubscriberOptions.HandlerRetryBaseDelay)} must be positive.");
        if (subscriberOptions.HandlerRetryMaxDelay <= TimeSpan.Zero)
            throw new InvalidOperationException($"{optionPath}.{nameof(KafkaSubscriberOptions.HandlerRetryMaxDelay)} must be positive.");
        if (subscriberOptions.HandlerRetryBaseDelay > subscriberOptions.HandlerRetryMaxDelay)
        {
            throw new InvalidOperationException(
                $"{optionPath}.{nameof(KafkaSubscriberOptions.HandlerRetryBaseDelay)} cannot exceed " +
                $"{optionPath}.{nameof(KafkaSubscriberOptions.HandlerRetryMaxDelay)}.");
        }

        switch (subscriberOptions.AckMode)
        {
            case KafkaAckMode.AckAfterHandlerCompletes:
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

                if (subscriberOptions.BackgroundDrainTimeout <= TimeSpan.Zero)
                    throw new InvalidOperationException($"{optionPath}.{nameof(KafkaSubscriberOptions.BackgroundDrainTimeout)} must be positive.");

                if (transportOptions.HostShutdownTimeout is { } hostShutdownTimeout)
                {
                    var requiredShutdownBudget = transportOptions.ShutdownTimeout + subscriberOptions.BackgroundDrainTimeout;
                    if (requiredShutdownBudget > hostShutdownTimeout)
                    {
                        throw new InvalidOperationException(
                            $"{optionPath}.{nameof(KafkaSubscriberOptions.BackgroundDrainTimeout)} plus " +
                            $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(KafkaAsyncResponseTransportOptions.ShutdownTimeout)} " +
                            $"requires {requiredShutdownBudget}, which exceeds " +
                            $"{nameof(KafkaAsyncResponseTransportOptions)}.{nameof(KafkaAsyncResponseTransportOptions.HostShutdownTimeout)} " +
                            $"({hostShutdownTimeout}). Increase Microsoft.Extensions.Hosting.HostOptions.ShutdownTimeout " +
                            $"and mirror that value in {nameof(KafkaAsyncResponseTransportOptions)}.{nameof(KafkaAsyncResponseTransportOptions.HostShutdownTimeout)}, " +
                            "or reduce the Kafka shutdown/drain timeouts.");
                    }
                }

                return;

            default:
                throw new InvalidOperationException(
                    $"{optionPath}.{nameof(KafkaSubscriberOptions.AckMode)} has unsupported value '{subscriberOptions.AckMode}'.");
        }
    }

    /// <summary>Handles the delivered message.</summary>
    public abstract Task HandleAsync(KafkaDelivery delivery, CancellationToken subscriberCancellationToken);

    /// <summary>
    /// Whether the dispatcher can accept more deliveries right now. Awaiting dispatchers always can
    /// (handlers run inline); the queued dispatcher returns <c>false</c> while its bounded queue is
    /// saturated so the subscriber pauses partition fetching instead of buffering an unbounded
    /// backlog in-process.
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
            if (logFailures)
            {
                Logger.LogError(
                    ex,
                    "Kafka message handling failed for {Topic}[{Partition}]@{Offset} (attempt {Attempt}).",
                    delivery.Topic,
                    delivery.Partition,
                    delivery.Offset,
                    attempt);
            }

            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>
    /// Marks the delivered message resolved by storing its next offset; the consumer's
    /// auto-committer flushes stored offsets on the configured interval.
    /// </summary>
    protected void StoreOffset(KafkaDelivery delivery)
        => _consumer.StoreOffset(delivery.Topic, delivery.Partition, delivery.Offset);

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
            cancellationToken).ConfigureAwait(false);

        _consumer.StoreOffset(message.Topic, message.Partition, message.Offset);
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

        var headers = new List<KafkaTransportHeader>(originalHeaders.Count + 10);
        headers.AddRange(originalHeaders);
        headers.Add(KafkaTransportHeader.Utf8("sourceTopic", sourceTopic));
        headers.Add(KafkaTransportHeader.Utf8("sourcePartition", partition.ToString(CultureInfo.InvariantCulture)));
        headers.Add(KafkaTransportHeader.Utf8("sourceOffset", offset.ToString(CultureInfo.InvariantCulture)));
        headers.Add(KafkaTransportHeader.Utf8("consumerGroup", _consumerGroup));
        headers.Add(KafkaTransportHeader.Utf8("subscriberRole", _role.ToString()));
        headers.Add(KafkaTransportHeader.Utf8("attempts", attempts.ToString(CultureInfo.InvariantCulture)));
        headers.Add(KafkaTransportHeader.Utf8("reason", reason));
        headers.Add(KafkaTransportHeader.Utf8("exceptionType", exception.GetType().FullName!));
        headers.Add(KafkaTransportHeader.Utf8("exceptionMessage", exception.Message));
        headers.Add(KafkaTransportHeader.Utf8("occurredAtUtc", DateTimeOffset.UtcNow.ToString("O")));

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
            cancellationToken).ConfigureAwait(false);
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

internal sealed class AwaitingKafkaMessageDispatcher(
    Func<KafkaDelivery, CancellationToken, Task> handler,
    IKafkaConsumerClient consumer,
    IKafkaProducerClient producer,
    KafkaAsyncResponseTransportOptions transportOptions,
    KafkaSubscriberOptions subscriberOptions,
    ILogger logger,
    string topic,
    string consumerGroup,
    KafkaSubscriberRole role)
    : KafkaMessageDispatcher(handler, consumer, producer, transportOptions, subscriberOptions, logger, topic, consumerGroup, role)
{
    /// <summary>Handles the delivered message.</summary>
    public override async Task HandleAsync(
        KafkaDelivery delivery,
        CancellationToken subscriberCancellationToken)
    {
        var attempt = 0;
        while (true)
        {
            attempt++;
            try
            {
                await ExecuteHandlerAsync(delivery, attempt, subscriberCancellationToken).ConfigureAwait(false);
                StoreOffset(delivery);
                return;
            }
            catch (OperationCanceledException) when (subscriberCancellationToken.IsCancellationRequested)
            {
                // Offset not stored: the message is redelivered after restart or rebalance.
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
                    await DeadLetterAsync(
                        delivery,
                        ex,
                        "handler_failed_max_attempts",
                        attempt,
                        CancellationToken.None).ConfigureAwait(false);
                    StoreOffset(delivery);
                    return;
                }

                // Kafka offsets cannot NACK one message, so retry in-process with backoff. This
                // stalls the message's partition (head-of-line), which is inherent to classic
                // consumer groups.
                await Task.Delay(RetryBackoff(attempt), subscriberCancellationToken).ConfigureAwait(false);
            }
        }
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
        _topic = topic;
        _drainTimeout = subscriberOptions.BackgroundDrainTimeout;
        _capacity = subscriberOptions.BackgroundQueueCapacity;
        _queue = Channel.CreateBounded<KafkaDelivery>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
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
            "Created Kafka ACK-after-enqueue dispatcher for {Topic} with {WorkerCount} worker(s), queue capacity {QueueCapacity}, drain timeout {DrainTimeout}.",
            _topic,
            subscriberOptions.BackgroundWorkerCount,
            subscriberOptions.BackgroundQueueCapacity,
            _drainTimeout);
    }

    internal int PendingCount => Volatile.Read(ref _pendingCount);
    internal int RunningCount => Volatile.Read(ref _runningCount);

    public override bool CanAcceptMore => Volatile.Read(ref _pendingCount) < _capacity;

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
                "Timed out while draining Kafka ACK-after-enqueue dispatcher for {Topic}. Pending={PendingCount}, Running={RunningCount}. Already committed work may be interrupted by host shutdown.",
                _topic,
                PendingCount,
                RunningCount);

            _ = Task.WhenAll(_workers).ContinueWith(
                _ => _drainCancellation.Dispose(),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    private async Task RunWorkerAsync(int workerIndex)
    {
        await foreach (var delivery in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            Interlocked.Decrement(ref _pendingCount);
            Interlocked.Increment(ref _runningCount);

            try
            {
                await ExecuteWithRetryAsync(delivery).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex) when (_drainCancellation.IsCancellationRequested)
            {
                // The drain budget lapsed with this already-committed message still unprocessed:
                // Kafka will not redeliver it, so surface the drop through OnBackgroundFailure
                // instead of losing it silently.
                Logger.LogWarning(
                    "Kafka background handler for already-committed message {Topic}[{Partition}]@{Offset} was canceled during dispatcher shutdown; surfacing via OnBackgroundFailure.",
                    delivery.Topic,
                    delivery.Partition,
                    delivery.Offset);
                await NotifyBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
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
                if (ReachedDeliveryAttempts(attempt))
                {
                    Logger.LogError(
                        ex,
                        "Kafka background handler failed for already-committed message {Topic}[{Partition}]@{Offset} after {Attempts} attempt(s).",
                        delivery.Topic,
                        delivery.Partition,
                        delivery.Offset,
                        attempt);
                    await NotifyBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);

                    try
                    {
                        await DeadLetterAsync(
                            delivery,
                            ex,
                            "background_handler_failed_after_commit",
                            attempt,
                            CancellationToken.None).ConfigureAwait(false);
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

                    return;
                }

                await Task.Delay(RetryBackoff(attempt), _drainCancellation.Token).ConfigureAwait(false);
            }
        }
    }
}
