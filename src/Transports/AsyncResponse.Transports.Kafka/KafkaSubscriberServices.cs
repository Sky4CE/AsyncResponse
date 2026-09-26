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

    /// <summary>
    /// The host-stop signal the subscriber stops taking new deliveries at (see
    /// <see cref="WorkerIntakeGate"/>): the worker role only — a response subscriber keeps
    /// delivering to the waiters host stop deliberately does not interrupt.
    /// </summary>
    protected virtual WorkerIntakeGate? IntakeGate => null;

    /// <summary>Handles the delivered message.</summary>
    protected abstract Task HandleMessageAsync(KafkaDelivery delivery, CancellationToken cancellationToken);

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
        KafkaTransportOptionsValidator.EnsureConsumerConfigAccepted(Options, SubscriberRole);
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // The ACK-after-enqueue dispatcher — its queue and workers — belongs to the hosted
        // service, not to one supervised attempt (sibling parity: NATS, SQS, the database
        // transports): each attempt attaches its consumer, and only the host stop drains it. The
        // ack-after-handler dispatcher stays per attempt: its detached handlers are bound to the
        // consumer (and the partition assignment) they were consumed on.
        await using var serviceDispatcher = SubscriberOptions.AckMode is KafkaAckMode.AckAfterEnqueue
            ? CreateDispatcher(consumer: null)
            : null;

        await SubscriberSupervisor.RunAsync(
            attemptToken => RunSubscriberAsync(serviceDispatcher, attemptToken),
            stoppingToken,
            failures => AsyncResponseRetry.Backoff(
                failures,
                Options.SubscriberRetryBaseDelay,
                Options.SubscriberRetryMaxDelay),
            // Guarded: a throwing logging provider escaping this callback ended the supervisor —
            // and with it the subscriber — instead of retrying.
            (ex, retryDelay) => SafeLog.Try(() => Logger.LogWarning(
                ex,
                "Kafka subscriber failed for topic {Topic} ({Role}); retrying in {RetryDelay}.",
                Topic,
                SubscriberRole,
                retryDelay)),
            healthyRunThreshold: AsyncResponseRetry.MaxAttainableDelay(Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay)).ConfigureAwait(false);
    }

    private KafkaMessageDispatcher CreateDispatcher(IKafkaConsumerClient? consumer)
        => KafkaMessageDispatcher.Create(
            HandleMessageAsync,
            consumer,
            _producer,
            Options,
            SubscriberOptions,
            Logger,
            Topic,
            ConsumerGroup,
            SubscriberRole,
            IntakeGate);

    private async Task RunSubscriberAsync(KafkaMessageDispatcher? serviceDispatcher, CancellationToken stoppingToken)
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
            var dispatcher = serviceDispatcher ?? CreateDispatcher(consumer);

            // The service-lifetime dispatcher stores offsets through THIS attempt's consumer until
            // the attempt ends (released before the consumer closes).
            using var attached = serviceDispatcher?.AttachConsumer(consumer);

            SafeLog.Try(() => Logger.LogInformation(
                "Kafka subscriber started. Topic: {Topic}. Group: {ConsumerGroup}. Role: {Role}. AckMode: {AckMode}.",
                Topic,
                ConsumerGroup,
                SubscriberRole,
                SubscriberOptions.AckMode));

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
                // unstored, so their messages redeliver on the rebuilt consumer. The service-level
                // ACK-after-enqueue dispatcher is not torn down at all: its queued work keeps
                // running and the rebuilt consumer is attached to it.
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
                // shutdown budget bounds it. DisposeAsync is idempotent, so the service-level
                // `await using` of the ACK-after-enqueue dispatcher is a no-op after this.
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
        var intakeStopLogged = false;
        while (!stoppingToken.IsCancellationRequested)
        {
            // Handlers that outlived their inline budget are settled here, on the poll thread —
            // the only thread that touches the consumer — before the next poll: offset stored,
            // partition resumed. A settlement that failed for good throws and faults the loop.
            dispatcher.SettleCompleted();

            KafkaIncomingMessage? message;
            if (!dispatcher.CanAcceptMore)
            {
                // Backpressure — the bounded in-process queue is saturated — or host stop (the
                // worker's intake gate closed, and nothing lifts it again): stop fetching from the
                // assigned partitions. Keep calling Consume so the broker still sees the consumer
                // polling (max.poll.interval.ms) and rebalance callbacks keep firing.
                // Re-assert the pause on EVERY such tick, not only on the edge: Pause() snapshots
                // the CURRENT assignment, and a partition this member gains meanwhile (from a peer
                // leaving the group) arrives unpaused — an edge-triggered pause would leave it
                // fetching into the full queue and park the poll thread on the bounded write. (A
                // partition the pause already covered keeps it across a revoke and a
                // re-assignment: librdkafka holds an application pause on the partition, and the
                // adapter lifts it on one that comes back after the resume below.) Pause is a
                // local librdkafka call (no broker round trip), so the per-tick re-assert is cheap.
                consumer.PauseAssignment();
                if (dispatcher.IntakeClosed && !intakeStopLogged)
                {
                    intakeStopLogged = true;
                    SafeLog.Try(() => Logger.LogInformation(
                        "Kafka worker subscriber for {Topic} stopped taking new deliveries: the host is stopping. Its assignment is paused and the consumer keeps polling until the subscriber stops; anything consumed from here on is left unsettled for the partition's next owner.",
                        Topic));
                }
                else if (!paused)
                {
                    SafeLog.Try(() => Logger.LogDebug(
                        "Kafka subscriber for {Topic} paused its assignment: the in-process queue is full.",
                        Topic));
                }

                paused = true;

                // Once the host is stopping and nothing detached is left to settle, nothing can
                // lift the pause any more: a full poll timeout per tick keeps the member alive.
                message = consumer.Consume(dispatcher.IntakeClosed && !dispatcher.HasDetachedWork
                    ? SubscriberOptions.PollTimeout
                    : SubscriberOptions.BackpressurePollDelay);
            }
            else
            {
                if (paused)
                {
                    consumer.ResumeAssignment();
                    paused = false;
                    SafeLog.Try(() => Logger.LogDebug(
                        "Kafka subscriber for {Topic} resumed its assignment: in-process queue capacity freed.",
                        Topic));
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

            if (dispatcher.IntakeClosed)
            {
                // Consumed after host stop began — fetched before the pause took hold, or in the
                // very poll the gate closed during. Neither started nor settled: its offset is not
                // stored (and nothing later of its partition is, since nothing more is taken), so
                // the partition's next owner redelivers it. Taken, early ACK committed it at
                // enqueue, and a flow wake-up — the very kind this host's own timer hand-overs just
                // published for a live replica — ran into the stopping host's hand-back and became
                // a dead-letter copy stranding its flow; under ack-after-handler it spent a
                // delivery and made the graceful stop wait for it.
                SafeLog.Try((Logger, Message: message), static state => state.Logger.LogDebug(
                    "Kafka message {Topic}[{Partition}]@{Offset} was consumed after the host began stopping; left unsettled for the partition's next owner.",
                    state.Message.Topic,
                    state.Message.Partition,
                    state.Message.Offset));
                continue;
            }

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
            SafeLog.Try(() => Logger.LogWarning(
                ex,
                "Kafka consumer for topic {Topic} ({Role}) failed to close cleanly; uncommitted offsets will be redelivered.",
                Topic,
                SubscriberRole));
        }
    }
}

internal sealed class KafkaWorkerSubscriber : KafkaSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;
    private readonly KafkaTransportTopicSchema _topics;
    private readonly WorkerIntakeGate _intakeGate;

    /// <summary>Runs the KafkaWorkerSubscriber operation.</summary>
    public KafkaWorkerSubscriber(
        IOptions<KafkaAsyncResponseTransportOptions> options,
        IKafkaConsumerClientFactory consumerFactory,
        IKafkaProducerClient producer,
        IKafkaAdminClient adminClient,
        IAsyncResponseIngress ingress,
        ILogger<KafkaWorkerSubscriber> logger,
        IHostApplicationLifetime? hostLifetime = null)
        : base(options, consumerFactory, producer, adminClient, logger)
    {
        _ingress = ingress;
        _topics = new KafkaTransportTopicSchema(options.Value);
        _intakeGate = new WorkerIntakeGate(hostLifetime);
    }

    protected override string Topic => _topics.WorkerTopic;
    protected override string ConsumerGroup => Options.WorkerConsumerGroup;
    protected override KafkaSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override KafkaSubscriberRole SubscriberRole => KafkaSubscriberRole.Worker;
    protected override WorkerIntakeGate IntakeGate => _intakeGate;

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
