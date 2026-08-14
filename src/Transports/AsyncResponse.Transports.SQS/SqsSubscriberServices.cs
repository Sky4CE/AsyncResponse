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
        SqsMessageDispatcher.ValidateOptions(Options, SubscriberOptions, SubscriberRole);
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queue = QueueName;
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
            // In early-ACK mode, receiving while the background queue is saturated would burn the
            // queue's redrive policy (SQS counts every receive), so wait for free capacity and never
            // request more messages than the dispatcher can accept.
            await dispatcher.WaitForCapacityAsync(stoppingToken).ConfigureAwait(false);
            var maxMessages = Math.Min(Options.MaxMessagesPerReceive, dispatcher.FreeCapacity);

            var deliveries = await _client.ReceiveMessagesAsync(
                new SqsReceiveRequest(
                    queueUrl,
                    maxMessages,
                    Options.ReceiveWaitTime,
                    SubscriberOptions.VisibilityTimeout),
                stoppingToken).ConfigureAwait(false);

            await DispatchBatchAsync(dispatcher, deliveries, queue, stoppingToken).ConfigureAwait(false);
        }
    }

    private async Task DispatchBatchAsync(
        SqsMessageDispatcher dispatcher,
        IReadOnlyList<SqsTransportDelivery> deliveries,
        string queue,
        CancellationToken stoppingToken)
    {
        if (deliveries.Count == 0)
            return;

        if (SubscriberOptions.AckMode is not SqsAckMode.AckAfterHandlerCompletes
            || SubscriberOptions.VisibilityRenewalInterval is not { } renewalInterval
            || SubscriberOptions.VisibilityTimeout is not { } visibilityTimeout)
        {
            foreach (var delivery in deliveries)
                await dispatcher.HandleAsync(delivery, stoppingToken).ConfigureAwait(false);
            return;
        }

        // The batch is processed serially, so a slow handler lets the visibility timeout of the later
        // (still unprocessed) messages lapse and a competing consumer processes them a second time.
        // While the batch is in flight, a heartbeat resets every unsettled message's invisibility to
        // the configured visibility timeout.
        var progress = new BatchProgress(deliveries.Count);
        using var renewalCancellation = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var renewalTask = RenewVisibilityLoopAsync(
            deliveries,
            progress,
            renewalInterval,
            visibilityTimeout,
            queue,
            renewalCancellation.Token);
        try
        {
            for (var index = 0; index < deliveries.Count; index++)
            {
                var delivery = deliveries[index];
                var batchIndex = index;
                // The dispatcher's failure path shortens visibility to RedeliveryDelay while the
                // heartbeat still counts the message as unsettled (MarkSettled runs only after
                // HandleAsync returns). Routing the dispatcher's visibility changes through a
                // suppression mark — set before the change itself — keeps a racing heartbeat from
                // stretching that fast retry back out to the full visibility timeout.
                var tracked = delivery with
                {
                    ChangeVisibilityAsync = timeout =>
                    {
                        progress.SuppressRenewal(batchIndex);
                        return delivery.ChangeVisibilityAsync(timeout);
                    }
                };
                try
                {
                    await dispatcher.HandleAsync(tracked, stoppingToken).ConfigureAwait(false);
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
            await renewalTask.ConfigureAwait(false);
        }
    }

    private async Task RenewVisibilityLoopAsync(
        IReadOnlyList<SqsTransportDelivery> deliveries,
        BatchProgress progress,
        TimeSpan renewalInterval,
        TimeSpan visibilityTimeout,
        string queue,
        CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                await Task.Delay(renewalInterval, cancellationToken).ConfigureAwait(false);

                // Renew from the first unsettled message onward: that covers the message currently in
                // the handler plus everything still waiting its turn. Two settle paths race this
                // sweep, and only one of them is harmless. A handled message was deleted, so a late
                // renewal merely fails and is logged — SQS redelivery keeps at-least-once intact. A
                // failed message was NOT deleted (its receipt handle stays live) and already carries
                // the failure path's shortened RedeliveryDelay, so a late renewal here would SUCCEED
                // and stretch that fast retry back out to the full visibility timeout — the
                // suppression mark and the per-message re-read of the settled prefix keep the sweep
                // away from it.
                for (var i = progress.SettledCount; i < deliveries.Count; i++)
                {
                    if (i < progress.SettledCount || progress.IsRenewalSuppressed(i))
                        continue;

                    var delivery = deliveries[i];
                    try
                    {
                        await delivery.ChangeVisibilityAsync(visibilityTimeout).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(
                            ex,
                            "Failed to renew visibility of SQS message {MessageId} on {Queue}; it may redeliver while still being processed (at-least-once preserved).",
                            delivery.MessageId,
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
        // One slot per batch message. Settled and suppressed only ever transition false→true, so
        // monotonic volatile writes/reads are enough — no lock, and a stale read only delays a
        // skip by one sweep pass.
        private readonly bool[] _renewalSuppressed;
        private int _settledCount;

        public BatchProgress(int batchSize) => _renewalSuppressed = new bool[batchSize];

        public int SettledCount => Volatile.Read(ref _settledCount);

        public void MarkSettled() => Interlocked.Increment(ref _settledCount);

        /// <summary>Marks the message at <paramref name="index"/> as owning its own visibility; the renewal sweep must leave it alone.</summary>
        public void SuppressRenewal(int index) => Volatile.Write(ref _renewalSuppressed[index], true);

        public bool IsRenewalSuppressed(int index) => Volatile.Read(ref _renewalSuppressed[index]);
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
