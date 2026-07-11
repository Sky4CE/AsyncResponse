using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.AzureServiceBus;

internal abstract class AzureServiceBusSubscriberService : BackgroundService
{
    private readonly IAzureServiceBusClient _client;

    protected AzureServiceBusSubscriberService(
        IOptions<AzureServiceBusAsyncResponseOptions> options,
        IAzureServiceBusClient client,
        ILogger logger)
    {
        Options = options.Value;
        AzureServiceBusOptionsValidator.ValidateCommon(Options);
        _client = client;
        Logger = logger;
    }

    protected AzureServiceBusAsyncResponseOptions Options { get; }
    protected ILogger Logger { get; }

    protected abstract string QueueName { get; }
    protected abstract AzureServiceBusSubscriberOptions SubscriberOptions { get; }
    protected abstract AzureServiceBusSubscriberRole SubscriberRole { get; }
    /// <summary>Handles the delivered message.</summary>
    protected abstract Task HandleMessageAsync(AzureServiceBusTransportDelivery delivery, CancellationToken cancellationToken);

    /// <summary>Runs this background operation until cancellation is requested.</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queue = QueueName;
        AzureServiceBusMessageDispatcher.ValidateOptions(Options, SubscriberOptions, SubscriberRole);
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
                var retryDelay = RetryDelay(failures);
                Logger.LogWarning(
                    ex,
                    "Azure Service Bus subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.",
                    queue,
                    SubscriberRole,
                    retryDelay);
                await Task.Delay(retryDelay, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private async Task RunSubscriberAsync(string queue, CancellationToken stoppingToken)
    {
        await using var receiver = _client.CreateReceiver(queue, SubscriberOptions);
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            HandleMessageAsync,
            Options,
            SubscriberOptions,
            Logger,
            queue,
            SubscriberRole);

        Logger.LogInformation(
            "Azure Service Bus subscriber started. Queue: {Queue}. Role: {Role}. AckMode: {AckMode}.",
            queue,
            SubscriberRole,
            SubscriberOptions.AckMode);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var messages = await receiver.ReceiveMessagesAsync(
                    Options.MaxMessagesPerReceive,
                    Options.ReceiveWaitTime,
                    stoppingToken).ConfigureAwait(false);

                foreach (var message in messages)
                    await dispatcher.HandleAsync(message, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            using var shutdown = new CancellationTokenSource(Options.ShutdownTimeout);
            await receiver.CloseAsync(shutdown.Token).ConfigureAwait(false);
        }
    }

    private TimeSpan RetryDelay(int failures)
    {
        var exponent = Math.Max(0, failures - 1);
        var milliseconds = Options.SubscriberRetryBaseDelay.TotalMilliseconds * Math.Pow(2, exponent);
        return TimeSpan.FromMilliseconds(Math.Min(milliseconds, Options.SubscriberRetryMaxDelay.TotalMilliseconds));
    }
}

internal sealed class AzureServiceBusWorkerSubscriber : AzureServiceBusSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    /// <summary>Creates a worker subscriber for the configured Service Bus worker queue.</summary>
    public AzureServiceBusWorkerSubscriber(
        IOptions<AzureServiceBusAsyncResponseOptions> options,
        IAzureServiceBusClient client,
        IAsyncResponseIngress ingress,
        ILogger<AzureServiceBusWorkerSubscriber> logger)
        : base(options, client, logger)
    {
        _ingress = ingress;
    }

    protected override string QueueName
        => AzureServiceBusOptionsValidator.Required(Options.WorkerQueue, nameof(Options.WorkerQueue));

    protected override AzureServiceBusSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override AzureServiceBusSubscriberRole SubscriberRole => AzureServiceBusSubscriberRole.Worker;

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(AzureServiceBusTransportDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(delivery.Body);
}

internal sealed class AzureServiceBusResponseIngressSubscriber : AzureServiceBusSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    /// <summary>Creates a response subscriber for the configured Service Bus response queue.</summary>
    public AzureServiceBusResponseIngressSubscriber(
        IOptions<AzureServiceBusAsyncResponseOptions> options,
        IAzureServiceBusClient client,
        IAsyncResponseIngress ingress,
        ILogger<AzureServiceBusResponseIngressSubscriber> logger)
        : base(options, client, logger)
    {
        _ingress = ingress;
    }

    protected override string QueueName
        => AzureServiceBusOptionsValidator.Required(Options.ResponseQueue, nameof(Options.ResponseQueue));

    protected override AzureServiceBusSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
    protected override AzureServiceBusSubscriberRole SubscriberRole => AzureServiceBusSubscriberRole.ResponseIngress;

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(AzureServiceBusTransportDelivery delivery, CancellationToken cancellationToken)
    {
        var correlationId = AzureServiceBusCorrelationIdExtractor.Extract(delivery, delivery.Body, Options);
        return _ingress.HandleResponseMessageAsync(delivery.Body, correlationId);
    }
}
