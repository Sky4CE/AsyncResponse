using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text;

namespace AsyncResponse.Transports.Kafka;

internal abstract class KafkaSubscriberService : BackgroundService
{
    /// <summary>
    /// Whether the payload is within the engine's inbound size budget. Overridden by the response
    /// ingress subscriber, which is the only role that parses the BODY to find a correlation id —
    /// the worker role reads a header and never touches payload size. Default true so a role
    /// without a budget behaves exactly as before.
    /// </summary>
    protected virtual bool IsWithinInboundBudget(string payload) => true;

    private readonly IKafkaConsumerClientFactory _consumerFactory;
    private readonly IKafkaProducerClient _producer;
    private readonly IKafkaAdminClient _adminClient;

    /// <summary>Runs the KafkaSubscriberService operation.</summary>
    protected KafkaSubscriberService(
        IOptions<KafkaAsyncResponseTransportOptions> options,
        IKafkaConsumerClientFactory consumerFactory,
        IKafkaProducerClient producer,
        IKafkaAdminClient adminClient,
        ILogger logger)
    {
        Options = options.Value;
        KafkaTransportOptionsValidator.ValidateCommon(Options);
        _consumerFactory = consumerFactory;
        _producer = producer;
        _adminClient = adminClient;
        Logger = logger;
    }

    protected KafkaAsyncResponseTransportOptions Options { get; }
    protected ILogger Logger { get; }

    protected abstract string Topic { get; }
    protected abstract string ConsumerGroup { get; }
    protected abstract KafkaSubscriberOptions SubscriberOptions { get; }
    protected abstract KafkaSubscriberRole SubscriberRole { get; }
    /// <summary>Handles the delivered message.</summary>
    protected abstract Task HandleMessageAsync(KafkaDelivery delivery, CancellationToken cancellationToken);

    /// <summary>Runs this background operation until cancellation is requested.</summary>
    /// <summary>
    /// Validates subscriber options here rather than at the top of <c>ExecuteAsync</c>: since
    /// Microsoft.Extensions.Hosting.Abstractions 10.0.10, <c>BackgroundService.StartAsync</c> no
    /// longer runs <c>ExecuteAsync</c> inline, so a throw there surfaces only through the host's
    /// background-exception handling — or never, when a fast stop discards the queued work —
    /// instead of failing host startup synchronously.
    /// </summary>
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        KafkaMessageDispatcher.ValidateOptions(Options, SubscriberOptions, SubscriberRole);
        return base.StartAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => SubscriberSupervisor.RunAsync(
            RunSubscriberAsync,
            stoppingToken,
            failures => AsyncResponseRetry.Backoff(
                failures,
                Options.SubscriberRetryBaseDelay,
                Options.SubscriberRetryMaxDelay),
            (ex, retryDelay) => Logger.LogWarning(
                ex,
                "Kafka subscriber failed for topic {Topic} ({Role}); retrying in {RetryDelay}.",
                Topic,
                SubscriberRole,
                retryDelay));

    private async Task RunSubscriberAsync(CancellationToken stoppingToken)
    {
        if (Options.CreateTopics)
            await EnsureTopicsAsync(stoppingToken).ConfigureAwait(false);

        var consumer = _consumerFactory.Create(SubscriberRole);
        try
        {
            consumer.Subscribe(Topic);

            // One session token per consumer, linked to the host's: a stop cancels it as before.
            // A poll-loop FAULT cancels it too — after the bounded fault teardown below — so a
            // detached handler abandoned by that teardown stops retrying a message whose offset
            // this session can no longer store, instead of running its whole retry ladder for a
            // consumer that is gone.
            using var session = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            var dispatcher = KafkaMessageDispatcher.Create(
                HandleMessageAsync,
                consumer,
                _producer,
                Options,
                SubscriberOptions,
                Logger,
                Topic,
                ConsumerGroup,
                SubscriberRole);

            Logger.LogInformation(
                "Kafka subscriber started. Topic: {Topic}. Group: {ConsumerGroup}. Role: {Role}. AckMode: {AckMode}.",
                Topic,
                ConsumerGroup,
                SubscriberRole,
                SubscriberOptions.AckMode);

            var faulted = false;
            try
            {
                // Consume() blocks the calling thread, so the poll loop runs on a dedicated thread
                // instead of starving the thread pool; the dispatcher's settlements happen on it too
                // (the consumer is touched from no other thread while the loop runs).
                await Task.Factory.StartNew(
                    () => RunPollLoop(consumer, dispatcher, session.Token),
                    stoppingToken,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default).ConfigureAwait(false);
            }
            catch (Exception) when (!stoppingToken.IsCancellationRequested)
            {
                // The poll loop failed (a consume error, a dropped connection, a burial that
                // failed for good) and the supervisor will rebuild the consumer after its backoff.
                // Teardown is BOUNDED here, unlike the graceful stop's: waiting for every detached
                // handler with no limit parked the reconnect behind an unrelated long handler — a
                // durable-flow step awaiting a remote response — and the configured retry policy
                // never ran. Handlers that settle within the budget get their offsets stored
                // (the close below commits them); the rest are abandoned with their offsets
                // unstored, so their messages redeliver on the rebuilt consumer.
                faulted = true;
                await dispatcher.TeardownAfterFaultAsync().ConfigureAwait(false);
                session.Cancel();
                throw;
            }
            finally
            {
                // Graceful stop (or a fault racing one): the drain waits for the ACK-after-enqueue
                // background queue — or ack-after-handler mode's detached handlers, storing their
                // offsets — before the consumer commits its final stored offsets below. The host's
                // shutdown budget bounds it.
                if (!faulted)
                    await dispatcher.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            CloseQuietly(consumer);
            consumer.Dispose();
        }
    }

    private void RunPollLoop(
        IKafkaConsumerClient consumer,
        KafkaMessageDispatcher dispatcher,
        CancellationToken stoppingToken)
    {
        var paused = false;
        while (!stoppingToken.IsCancellationRequested)
        {
            // Handlers that outlived their inline budget are settled here, on the poll thread —
            // the only thread that touches the consumer — before the next poll: offset stored,
            // partition resumed. A settlement that failed for good throws and faults the loop.
            dispatcher.SettleCompleted();

            KafkaIncomingMessage? message;
            if (!dispatcher.CanAcceptMore)
            {
                // Backpressure: stop fetching from the assigned partitions while the bounded
                // in-process queue is saturated. Keep calling Consume so the broker still sees the
                // consumer polling (max.poll.interval.ms) and rebalance callbacks keep firing.
                // Re-assert the pause on EVERY saturated tick, not only on the edge: Pause()
                // snapshots the CURRENT assignment, and a rebalance during backpressure hands this
                // member partitions with their pause state reset — an edge-triggered pause would
                // leave those fetching into the full queue and park the poll thread on the bounded
                // write. Pause is a local librdkafka call (no broker round trip), so the per-tick
                // re-assert is cheap.
                consumer.PauseAssignment();
                if (!paused)
                {
                    paused = true;
                    Logger.LogDebug(
                        "Kafka subscriber for {Topic} paused its assignment: the in-process queue is full.",
                        Topic);
                }

                message = consumer.Consume(SubscriberOptions.BackpressurePollDelay);
            }
            else
            {
                if (paused)
                {
                    consumer.ResumeAssignment();
                    paused = false;
                    Logger.LogDebug(
                        "Kafka subscriber for {Topic} resumed its assignment: in-process queue capacity freed.",
                        Topic);
                }

                // With detached handlers in flight, poll in short slices so a completion is
                // settled within BackpressurePollDelay instead of after a full PollTimeout; the
                // slice is what bounds the resume latency of the paused partition.
                message = consumer.Consume(dispatcher.HasDetachedWork
                    ? SubscriberOptions.BackpressurePollDelay
                    : SubscriberOptions.PollTimeout);
            }

            if (message is null)
                continue;

            KafkaDelivery delivery;
            try
            {
                delivery = CreateDelivery(message);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A foreign/malformed message can never be handled; dead-letter and commit it so
                // its partition advances instead of re-failing on every subscriber restart.
                //
                // Deliberately every non-cancellation exception, not just InvalidDataException:
                // CreateDelivery also runs correlation-id extraction, and anything that escapes
                // here faults the poll loop with the offset unstored, so the same message re-throws
                // after every supervisor restart and the whole subscriber (all assigned partitions)
                // stops advancing — MaxDeliveryAttempts cannot help, because it is keyed on a
                // delivery this path never constructed.
                //
                // Through the dispatcher's partition ordering, not a direct discard: storing this
                // message's offset while an earlier message of the same partition is still being
                // handled (detached) commits the partition PAST that unfinished message, and a
                // crash after the commit skips it for good with no dead-letter copy anywhere.
                dispatcher.AcceptUnprocessable(message, ex, stoppingToken);
                continue;
            }

            // Settles inline (queued mode, and ack-after-handler mode within DetachHandlerAfter)
            // or detaches the handler and returns; either way the poll thread is back here within
            // the validated poll gap.
            dispatcher.Accept(delivery, stoppingToken);
        }
    }

    private async Task EnsureTopicsAsync(CancellationToken cancellationToken)
    {
        var topics = new List<string>(2) { Topic };
        if (Options.DeadLetterEnabled)
            topics.Add(new KafkaTransportTopicSchema(Options).DeadLetterTopicFor(Topic));

        await _adminClient.EnsureTopicsAsync(
            topics,
            Options.TopicNumPartitions,
            Options.TopicReplicationFactor,
            cancellationToken).ConfigureAwait(false);
    }

    private KafkaDelivery CreateDelivery(KafkaIncomingMessage message)
    {
        var payload = message.Payload is { Length: > 0 }
            ? Encoding.UTF8.GetString(message.Payload)
            : null;
        if (string.IsNullOrWhiteSpace(payload))
        {
            throw new InvalidDataException(
                $"Kafka message {message.Topic}[{message.Partition}]@{message.Offset} does not contain a payload.");
        }

        // Body-path extraction parses the whole payload, so it is gated on the inbound budget;
        // the field/header path reads metadata only and is unaffected by payload size.
        var correlationId = SubscriberRole is KafkaSubscriberRole.ResponseIngress
            ? IsWithinInboundBudget(payload)
                ? KafkaCorrelationIdExtractor.Extract(message.Headers, payload, Options)
                : null
            : KafkaCorrelationIdExtractor.TryReadHeader(message.Headers, Options.CorrelationIdHeader);

        return new KafkaDelivery(
            message.Topic,
            message.Partition,
            message.Offset,
            payload,
            correlationId,
            message.Headers);
    }

    private void CloseQuietly(IKafkaConsumerClient consumer)
    {
        try
        {
            // Commits stored offsets and leaves the group cleanly so partitions rebalance
            // immediately instead of waiting for the session timeout.
            consumer.Close();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(
                ex,
                "Kafka consumer for topic {Topic} ({Role}) failed to close cleanly; uncommitted offsets will be redelivered.",
                Topic,
                SubscriberRole);
        }
    }
}

internal sealed class KafkaWorkerSubscriber : KafkaSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;
    private readonly KafkaTransportTopicSchema _topics;

    /// <summary>Runs the KafkaWorkerSubscriber operation.</summary>
    public KafkaWorkerSubscriber(
        IOptions<KafkaAsyncResponseTransportOptions> options,
        IKafkaConsumerClientFactory consumerFactory,
        IKafkaProducerClient producer,
        IKafkaAdminClient adminClient,
        IAsyncResponseIngress ingress,
        ILogger<KafkaWorkerSubscriber> logger)
        : base(options, consumerFactory, producer, adminClient, logger)
    {
        _ingress = ingress;
        _topics = new KafkaTransportTopicSchema(options.Value);
    }

    protected override string Topic => _topics.WorkerTopic;
    protected override string ConsumerGroup => Options.WorkerConsumerGroup;
    protected override KafkaSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override KafkaSubscriberRole SubscriberRole => KafkaSubscriberRole.Worker;

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(KafkaDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(delivery.Payload);
}

internal sealed class KafkaResponseIngressSubscriber : KafkaSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;
    private readonly KafkaTransportTopicSchema _topics;

    /// <summary>Runs the KafkaResponseIngressSubscriber operation.</summary>
    public KafkaResponseIngressSubscriber(
        IOptions<KafkaAsyncResponseTransportOptions> options,
        IKafkaConsumerClientFactory consumerFactory,
        IKafkaProducerClient producer,
        IKafkaAdminClient adminClient,
        IAsyncResponseIngress ingress,
        ILogger<KafkaResponseIngressSubscriber> logger)
        : base(options, consumerFactory, producer, adminClient, logger)
    {
        _ingress = ingress;
        _topics = new KafkaTransportTopicSchema(options.Value);
    }

    protected override string Topic => _topics.ResponseTopic;
    protected override string ConsumerGroup => Options.ResponseConsumerGroup;
    protected override KafkaSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
    protected override KafkaSubscriberRole SubscriberRole => KafkaSubscriberRole.ResponseIngress;

    /// <inheritdoc />
    protected override bool IsWithinInboundBudget(string payload) => !_ingress.IsOverInboundBudget(payload);

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(KafkaDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleResponseMessageAsync(delivery.Payload, delivery.CorrelationId);
}
