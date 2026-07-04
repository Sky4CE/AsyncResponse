using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.SQS;

internal abstract class SqsSubscriberService : BackgroundService
{
    private readonly ISqsClient _client;

    protected SqsSubscriberService(
        IOptions<SqsAsyncResponseOptions> options,
        ISqsClient client,
        ILogger logger)
    {
        Options = options.Value;
        SqsOptionsValidator.ValidateCommon(Options);
        _client = client;
        Logger = logger;
    }

    protected SqsAsyncResponseOptions Options { get; }
    protected ILogger Logger { get; }

    protected abstract string QueueName { get; }
    protected abstract SqsSubscriberOptions SubscriberOptions { get; }
    protected abstract SqsSubscriberRole SubscriberRole { get; }
    /// <summary>Handles the delivered message.</summary>
    protected abstract Task HandleMessageAsync(SqsTransportDelivery delivery, CancellationToken cancellationToken);

    /// <summary>Runs this background operation until cancellation is requested.</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queue = QueueName;
        SqsMessageDispatcher.ValidateOptions(Options, SubscriberOptions, SubscriberRole);
        var failures = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunSubscriberAsync(queue, stoppingToken).ConfigureAwait(false);
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
                    "SQS subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.",
                    queue,
                    SubscriberRole,
                    retryDelay);
                await Task.Delay(retryDelay, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private async Task RunSubscriberAsync(string queue, CancellationToken stoppingToken)
    {
        // A queue configured by name resolves through GetQueueUrl; failures here (queue not yet
        // provisioned, endpoint still starting) surface to the retry loop above.
        var queueUrl = SqsQueueAddress.IsUrl(queue)
            ? queue
            : await _client.GetQueueUrlAsync(queue, stoppingToken).ConfigureAwait(false);

        await using var dispatcher = SqsMessageDispatcher.Create(
            HandleMessageAsync,
            Options,
            SubscriberOptions,
            Logger,
            queue,
            SubscriberRole);

        Logger.LogInformation(
            "SQS subscriber started. Queue: {Queue}. Role: {Role}. AckMode: {AckMode}.",
            queue,
            SubscriberRole,
            SubscriberOptions.AckMode);

        while (!stoppingToken.IsCancellationRequested)
        {
            var deliveries = await _client.ReceiveMessagesAsync(
                new SqsReceiveRequest(
                    queueUrl,
                    Options.MaxMessagesPerReceive,
                    Options.ReceiveWaitTime,
                    SubscriberOptions.VisibilityTimeout),
                stoppingToken).ConfigureAwait(false);

            foreach (var delivery in deliveries)
                await dispatcher.HandleAsync(delivery, stoppingToken).ConfigureAwait(false);
        }
    }
}

internal sealed class SqsWorkerSubscriber : SqsSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    /// <summary>Creates a worker subscriber for the configured SQS worker queue.</summary>
    public SqsWorkerSubscriber(
        IOptions<SqsAsyncResponseOptions> options,
        ISqsClient client,
        IAsyncResponseIngress ingress,
        ILogger<SqsWorkerSubscriber> logger)
        : base(options, client, logger)
    {
        _ingress = ingress;
    }

    protected override string QueueName
        => SqsOptionsValidator.Required(Options.WorkerQueue, nameof(Options.WorkerQueue));

    protected override SqsSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override SqsSubscriberRole SubscriberRole => SqsSubscriberRole.Worker;

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(SqsTransportDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(delivery.Body);
}

internal sealed class SqsResponseIngressSubscriber : SqsSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    /// <summary>Creates a response subscriber for the configured SQS response queue.</summary>
    public SqsResponseIngressSubscriber(
        IOptions<SqsAsyncResponseOptions> options,
        ISqsClient client,
        IAsyncResponseIngress ingress,
        ILogger<SqsResponseIngressSubscriber> logger)
        : base(options, client, logger)
    {
        _ingress = ingress;
    }

    protected override string QueueName
        => SqsOptionsValidator.Required(Options.ResponseQueue, nameof(Options.ResponseQueue));

    protected override SqsSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
    protected override SqsSubscriberRole SubscriberRole => SqsSubscriberRole.ResponseIngress;

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(SqsTransportDelivery delivery, CancellationToken cancellationToken)
    {
        var correlationId = SqsCorrelationIdExtractor.Extract(delivery, delivery.Body, Options);
        return _ingress.HandleResponseMessageAsync(delivery.Body, correlationId);
    }
}
