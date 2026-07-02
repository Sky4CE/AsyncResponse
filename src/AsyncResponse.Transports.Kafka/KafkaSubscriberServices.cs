using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text;

namespace AsyncResponse.Transports.Kafka;

internal abstract class KafkaSubscriberService : BackgroundService
{
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
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        KafkaMessageDispatcher.ValidateOptions(Options, SubscriberOptions, SubscriberRole);

        var failures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunSubscriberAsync(stoppingToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                failures++;
                var retryDelay = AsyncResponseRetry.Backoff(
                    failures,
                    Options.SubscriberRetryBaseDelay,
                    Options.SubscriberRetryMaxDelay);
                Logger.LogWarning(
                    ex,
                    "Kafka subscriber failed for topic {Topic} ({Role}); retrying in {RetryDelay}.",
                    Topic,
                    SubscriberRole,
                    retryDelay);
                await Task.Delay(retryDelay, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private async Task RunSubscriberAsync(CancellationToken stoppingToken)
    {
        if (Options.CreateTopics)
            await EnsureTopicsAsync(stoppingToken).ConfigureAwait(false);

        var consumer = _consumerFactory.Create(SubscriberRole);
        try
        {
            consumer.Subscribe(Topic);
            await using var dispatcher = KafkaMessageDispatcher.Create(
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

            // Consume() blocks the calling thread, so the poll loop runs on a dedicated thread
            // instead of starving the thread pool; the dispatcher's async work is awaited from it.
            await Task.Factory.StartNew(
                () => RunPollLoop(consumer, dispatcher, stoppingToken),
                stoppingToken,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default).ConfigureAwait(false);

            // Leaving the await-using scope drains the ACK-after-enqueue background queue before
            // the consumer commits its final stored offsets below.
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
            KafkaIncomingMessage? message;
            if (!dispatcher.CanAcceptMore)
            {
                // Backpressure: stop fetching from the assigned partitions while the bounded
                // in-process queue is saturated. Keep calling Consume so the broker still sees the
                // consumer polling (max.poll.interval.ms) and rebalance callbacks keep firing.
                if (!paused)
                {
                    consumer.PauseAssignment();
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

                message = consumer.Consume(SubscriberOptions.PollTimeout);
            }

            if (message is null)
                continue;

            KafkaDelivery delivery;
            try
            {
                delivery = CreateDelivery(message);
            }
            catch (InvalidDataException ex)
            {
                // A foreign/malformed message can never be handled; dead-letter and commit it so
                // its partition advances instead of re-failing on every subscriber restart.
                dispatcher.DiscardUnprocessableAsync(message, ex, stoppingToken).GetAwaiter().GetResult();
                continue;
            }

            dispatcher.HandleAsync(delivery, stoppingToken).GetAwaiter().GetResult();
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

        var correlationId = SubscriberRole is KafkaSubscriberRole.ResponseIngress
            ? KafkaCorrelationIdExtractor.Extract(message.Headers, payload, Options)
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

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(KafkaDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleResponseMessageAsync(delivery.Payload, delivery.CorrelationId);
}
