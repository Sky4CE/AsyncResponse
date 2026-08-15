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
    /// <summary>
    /// Validates subscriber options here rather than at the top of <c>ExecuteAsync</c>: since
    /// Microsoft.Extensions.Hosting.Abstractions 10.0.10, <c>BackgroundService.StartAsync</c> no
    /// longer runs <c>ExecuteAsync</c> inline, so a throw there surfaces only through the host's
    /// background-exception handling — or never, when a fast stop discards the queued work —
    /// instead of failing host startup synchronously.
    /// </summary>
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _ = QueueName; // Resolving the name enforces its Required check at startup too.
        AzureServiceBusMessageDispatcher.ValidateOptions(Options, SubscriberOptions, SubscriberRole);
        return base.StartAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queue = QueueName;
        return SubscriberSupervisor.RunAsync(
            ct => RunSubscriberAsync(queue, ct),
            stoppingToken,
            failures => AsyncResponseRetry.Backoff(failures, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay),
            (ex, retryDelay) => Logger.LogWarning(
                ex,
                "Azure Service Bus subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.",
                queue,
                SubscriberRole,
                retryDelay));
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
                // In early-ACK mode, receiving while the background queue is saturated would burn
                // DeliveryCount via queue-full abandons, so wait for free capacity and never request
                // more messages than the dispatcher can accept.
                await dispatcher.WaitForCapacityAsync(stoppingToken).ConfigureAwait(false);
                var maxMessages = Math.Min(Options.MaxMessagesPerReceive, dispatcher.FreeCapacity);

                var messages = await receiver.ReceiveMessagesAsync(
                    maxMessages,
                    Options.ReceiveWaitTime,
                    stoppingToken).ConfigureAwait(false);

                await DispatchBatchAsync(dispatcher, messages, queue, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            using var shutdown = new CancellationTokenSource(Options.ShutdownTimeout);
            await receiver.CloseAsync(shutdown.Token).ConfigureAwait(false);
        }
    }

    private async Task DispatchBatchAsync(
        AzureServiceBusMessageDispatcher dispatcher,
        IReadOnlyList<AzureServiceBusTransportDelivery> messages,
        string queue,
        CancellationToken stoppingToken)
    {
        if (messages.Count == 0)
            return;

        if (SubscriberOptions.AckMode is not AzureServiceBusAckMode.AckAfterHandlerCompletes
            || SubscriberOptions.LockRenewalInterval is not { } renewalInterval)
        {
            foreach (var message in messages)
                await dispatcher.HandleAsync(message, stoppingToken).ConfigureAwait(false);
            return;
        }

        // The batch is processed serially, so a slow handler lets the peek locks of the later (still
        // unsettled) messages expire and Service Bus redelivers them to a competing consumer while
        // they are still queued here — systematic duplicate processing. While the batch is in flight,
        // a background loop renews the lock of every unsettled message each interval.
        var progress = new BatchProgress();
        using var renewalCancellation = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var renewalTask = RenewLocksLoopAsync(messages, progress, renewalInterval, queue, renewalCancellation.Token);
        try
        {
            foreach (var message in messages)
            {
                try
                {
                    await dispatcher.HandleAsync(message, stoppingToken).ConfigureAwait(false);
                }
                finally
                {
                    progress.MarkSettled();
                }
            }
        }
        finally
        {
            renewalCancellation.Cancel();
            try
            {
                // Cancellation exits the sweep between messages and interrupts the in-flight renew
                // call, so this normally completes immediately. The bound is the hard backstop: an
                // unbounded await here would let a degraded namespace hold the receive loop (and, at
                // shutdown, the whole host budget) hostage for the SDK retry budget per message.
                await renewalTask.WaitAsync(Options.ShutdownTimeout).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                Logger.LogWarning(
                    "Azure Service Bus lock renewal for {Queue} ({Role}) did not stop within the shutdown budget ({ShutdownTimeout}); abandoning the renewal task.",
                    queue,
                    SubscriberRole,
                    Options.ShutdownTimeout);
            }
        }
    }

    private async Task RenewLocksLoopAsync(
        IReadOnlyList<AzureServiceBusTransportDelivery> messages,
        BatchProgress progress,
        TimeSpan renewalInterval,
        string queue,
        CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                await Task.Delay(renewalInterval, cancellationToken).ConfigureAwait(false);

                // Renew from the first unsettled message onward: that covers the message currently in
                // the handler plus everything still waiting its turn. A renewal racing a just-settled
                // message merely fails and is logged; redelivery keeps at-least-once intact.
                for (var i = progress.SettledCount; i < messages.Count; i++)
                {
                    // The batch finished or the subscriber is stopping: exit quietly between messages
                    // instead of spending up to a full SDK retry budget on each remaining renew.
                    if (cancellationToken.IsCancellationRequested)
                        return;

                    var message = messages[i];
                    try
                    {
                        await message.RenewLockAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        // Our token interrupted the in-flight renew; not a renewal failure.
                        return;
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(
                            ex,
                            "Failed to renew the lock of Azure Service Bus message {MessageId} on {Queue}; it may redeliver while still being processed (at-least-once preserved).",
                            message.MessageId,
                            queue);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // The batch finished or the subscriber is stopping.
        }
    }

    private sealed class BatchProgress
    {
        private int _settledCount;

        public int SettledCount => Volatile.Read(ref _settledCount);

        public void MarkSettled() => Interlocked.Increment(ref _settledCount);
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
