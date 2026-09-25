using Azure.Messaging.ServiceBus;
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

    /// <summary>Clocks the stop path's shutdown budget; replaced by tests to drive it virtually.</summary>
    internal TimeProvider Clock { get; set; } = TimeProvider.System;

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
        if (SubscriberOptions is { AckMode: AzureServiceBusAckMode.AckAfterHandlerCompletes, PrefetchCount: > 0 })
        {
            // Service Bus locks a prefetched message the moment it lands in the client buffer, and
            // the renewal heartbeat covers only what a receive has returned — so behind a slow
            // handler the buffered locks lapse, those messages run a second time on a peer, fail
            // their late Complete here, and burn DeliveryCount toward MaxDeliveryAttempts.
            Logger.LogWarning(
                "Azure Service Bus {Role} subscriber for {Queue} prefetches {PrefetchCount} message(s) in AckAfterHandlerCompletes mode: buffered messages are locked but never renewed while they wait behind the running handler, so a slow handler lets their locks expire and they run twice. Keep PrefetchCount × handler latency well under the queue's LockDuration, or set PrefetchCount = 0.",
                SubscriberRole,
                QueueName,
                SubscriberOptions.PrefetchCount);
        }

        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queue = QueueName;

        // The dispatcher outlives the supervised attempts below (SQS/Pub-Sub/NATS parity). In
        // complete-after-enqueue mode it holds work Service Bus already COMPLETED, and disposing
        // it runs the stop-time drain — so scoped to one attempt, any receive fault that outlived
        // the SDK's retries (throttling, a communication problem) on a host that was NOT stopping
        // paused consumption for the drain budget and then surfaced the still-queued work as
        // lapsed, work Service Bus can never redeliver. It captures nothing per attempt: every
        // settlement runs inside HandleAsync against the delivery's own receiver, so only host
        // stop drains it.
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            HandleMessageAsync,
            Options,
            SubscriberOptions,
            Logger,
            queue,
            SubscriberRole);

        await SubscriberSupervisor.RunAsync(
            ct => RunSubscriberAsync(queue, dispatcher, ct),
            stoppingToken,
            failures => AsyncResponseRetry.Backoff(failures, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay),
            (ex, retryDelay) => Logger.LogWarning(
                ex,
                "Azure Service Bus subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.",
                queue,
                SubscriberRole,
                retryDelay),
            healthyRunThreshold: Options.SubscriberRetryMaxDelay).ConfigureAwait(false);
    }

    private async Task RunSubscriberAsync(string queue, AzureServiceBusMessageDispatcher dispatcher, CancellationToken stoppingToken)
    {
        await using var receiver = _client.CreateReceiver(queue, SubscriberOptions);

        Logger.LogInformation(
            "Azure Service Bus subscriber started. Queue: {Queue}. Role: {Role}. AckMode: {AckMode}.",
            queue,
            SubscriberRole,
            SubscriberOptions.AckMode);

        // Service Bus locks — and, once a lock lapses, counts — every message a receive hands over,
        // not the one a handler starts. ACK-after-handler works a batch serially, so every worker
        // job received behind a handler that kills the process (stack overflow, OOM, FailFast)
        // came back with its DeliveryCount bumped without ever having run, eating healthy
        // batch-mates' MaxDeliveryAttempts budget, and a long handler kept up to
        // MaxMessagesPerReceive - 1 jobs locked under the heartbeat while idle peers could have run
        // them. One message per receive leaves the rest on the entity, where any peer can take it
        // (NATS parity). Early ACK settles each message as it is accepted, and the response
        // ingress runs the library's own short handler, so both keep the batch — one receive
        // round trip per response would cut response throughput for no such risk.
        var receiveSize = SubscriberOptions.AckMode is AzureServiceBusAckMode.AckAfterHandlerCompletes
            && SubscriberRole is AzureServiceBusSubscriberRole.Worker
                ? 1
                : Options.MaxMessagesPerReceive;
        var stopBudget = new SharedStopBudget(Options.ShutdownTimeout, Clock);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                // In early-ACK mode, receiving while the background queue is saturated would burn
                // DeliveryCount via queue-full abandons, so wait for free capacity and never request
                // more messages than the dispatcher can accept.
                await dispatcher.WaitForCapacityAsync(stoppingToken).ConfigureAwait(false);
                var maxMessages = Math.Min(receiveSize, dispatcher.FreeCapacity);

                var messages = await receiver.ReceiveMessagesAsync(
                    maxMessages,
                    Options.ReceiveWaitTime,
                    stoppingToken).ConfigureAwait(false);

                if (await DispatchBatchAsync(dispatcher, messages, queue, stopBudget, stoppingToken).ConfigureAwait(false))
                {
                    // The flow engine handed a delivery back: ApplicationStopping has fired and
                    // this subscriber's own stop is next. Every job received from now on would be
                    // interrupted the same way and left locked for a whole LockDuration — among
                    // them the wake-ups the engine hands over for a replica still running to take
                    // at once. So stop receiving for the rest of the attempt (Redis/NATS rule) and
                    // wait for the stop.
                    Logger.LogInformation(
                        "Azure Service Bus {Role} subscriber for {Queue} stops receiving: the flow engine handed a delivery back because the host is stopping.",
                        SubscriberRole,
                        queue);
                    await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            // Whenever the host stop ends the attempt — an OCE out of the receive, or the loop
            // condition after a batch whose handler outlived the stop — close under the budget
            // the validator sums for it. Left to `await using`, ServiceBusReceiver.DisposeAsync
            // closes with no token at all, so a namespace stalling the link drain or detach held
            // the stop past the host budget. (Closed here, the later dispose is a no-op.)
            if (stoppingToken.IsCancellationRequested)
                await CloseReceiverAsync(receiver, queue, stopBudget.Remaining()).ConfigureAwait(false);
        }
    }

    private async Task CloseReceiverAsync(IAzureServiceBusReceiver receiver, string queue, TimeSpan budget)
    {
        var shutdown = new CancellationTokenSource(budget, Clock);
        try
        {
            // The token bounds the link drain; WaitAsync bounds the rest, since the SDK detaches
            // the link with CancellationToken.None. Called even with nothing left: the close marks
            // the receiver closed, so the later `await using` dispose cannot close it unbounded.
            await receiver.CloseAsync(shutdown.Token).WaitAsync(budget, Clock).ConfigureAwait(false);
            shutdown.Dispose();
        }
        catch (Exception ex)
        {
            // The source stays undisposed: an abandoned close may still hold its token.
            Logger.LogWarning(
                ex,
                "Azure Service Bus receiver for {Queue} ({Role}) did not close cleanly within the shutdown budget ({ShutdownTimeout}); abandoning it.",
                queue,
                SubscriberRole,
                budget);
        }
    }

    /// <summary>
    /// Dispatches one received batch; returns <c>true</c> when the flow engine handed a delivery
    /// back because the host is stopping, which ends the batch the way the stop itself does.
    /// </summary>
    private async Task<bool> DispatchBatchAsync(
        AzureServiceBusMessageDispatcher dispatcher,
        IReadOnlyList<AzureServiceBusTransportDelivery> messages,
        string queue,
        SharedStopBudget stopBudget,
        CancellationToken stoppingToken)
    {
        if (messages.Count == 0)
            return false;

        var progress = new BatchProgress(messages.Count);
        var next = 0;
        var handedBack = false;
        if (SubscriberOptions.AckMode is not AzureServiceBusAckMode.AckAfterHandlerCompletes
            || SubscriberOptions.LockRenewalInterval is not { } renewalInterval)
        {
            try
            {
                for (; next < messages.Count; next++)
                {
                    // The handler takes no token, so a stop cannot interrupt the message in hand —
                    // but it must not START the rest of the batch: every fresh handler runs against
                    // the host's shutdown budget, and in early-ACK mode every further enqueue
                    // completes work the drain budget may then have to refuse.
                    if (stoppingToken.IsCancellationRequested)
                        break;

                    try
                    {
                        handedBack = await dispatcher.HandleAsync(messages[next], stoppingToken).ConfigureAwait(false)
                            is AzureServiceBusDispatchOutcome.HandedBack;
                    }
                    finally
                    {
                        progress.MarkSettled();
                    }

                    if (handedBack)
                        break;
                }
            }
            finally
            {
                // No renewal join to overlap here, so on the stop path the hand-back and the
                // receiver close after it share the ONE ShutdownTimeout the validator sums for
                // them — each taking a whole one overran the host budget by a ShutdownTimeout. A
                // hand-back while the subscriber is still live (a flow hand-back ended the batch)
                // keeps its own bound: the close comes later, with the stop.
                await HandBackUnstartedAsync(
                    messages,
                    Math.Max(next, progress.SettledCount),
                    queue,
                    stoppingToken.IsCancellationRequested ? stopBudget : null).ConfigureAwait(false);
            }

            return handedBack;
        }

        // The batch is processed serially, so a slow handler lets the peek locks of the later (still
        // unsettled) messages expire and Service Bus redelivers them to a competing consumer while
        // they are still queued here — systematic duplicate processing. While the batch is in flight,
        // a background loop renews the lock of every unsettled message each interval.
        // NOT linked to the stop token: the handler in flight when the host stops keeps running
        // (it takes no token), and ending its renewal at that moment let its lock lapse under a
        // live handler — a peer then ran the same job a second time and the late Complete failed
        // with MessageLockLost. The renewal ends when the batch loop does (SQS/NATS parity).
        using var renewalCancellation = new CancellationTokenSource();
        var renewalTask = RenewLocksLoopAsync(messages, progress, renewalInterval, queue, renewalCancellation.Token);
        try
        {
            for (; next < messages.Count; next++)
            {
                // Same stop rule as the renewal-free path above.
                if (stoppingToken.IsCancellationRequested)
                    break;

                try
                {
                    handedBack = await dispatcher.HandleAsync(messages[next], stoppingToken).ConfigureAwait(false)
                        is AzureServiceBusDispatchOutcome.HandedBack;
                }
                finally
                {
                    progress.MarkSettled();
                }

                if (handedBack)
                    break;
            }
        }
        finally
        {
            renewalCancellation.Cancel();

            // Hand back whatever never started — a stop, a flow hand-back, or a handler that exited
            // through the stop's cancellation, cut the batch short (NATS rule: a message whose
            // handler ran is past `next`). Started before the join below and bounded by its own
            // ShutdownTimeout, so the two overlap: the stop path spends one ShutdownTimeout here,
            // not two, and the validator's renewal-join + receiver-close sum stays true.
            var handBack = HandBackUnstartedAsync(messages, Math.Max(next, progress.SettledCount), queue, stopBudget: null);
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

            await handBack.ConfigureAwait(false);
        }

        return handedBack;
    }

    /// <summary>
    /// Abandons the batch messages that were never started, so a peer can take them at once.
    /// Left alone they stay locked for the rest of their lock although nothing is processing them,
    /// stalling that work on every rolling deploy; the abandon counts the one delivery that lock
    /// expiry would have counted anyway. Best-effort and bounded by
    /// <see cref="AzureServiceBusAsyncResponseOptions.ShutdownTimeout"/> — or, when it shares one
    /// with the receiver close, by what is left of <paramref name="stopBudget"/>: a message that
    /// cannot be handed back simply redelivers when its lock lapses.
    /// </summary>
    private async Task HandBackUnstartedAsync(
        IReadOnlyList<AzureServiceBusTransportDelivery> messages,
        int firstUnstarted,
        string queue,
        SharedStopBudget? stopBudget)
    {
        if (firstUnstarted >= messages.Count)
            return;

        var budget = stopBudget?.Remaining() ?? Options.ShutdownTimeout;
        var releases = new Task[messages.Count - firstUnstarted];
        for (var index = firstUnstarted; index < messages.Count; index++)
            releases[index - firstUnstarted] = ReleaseAsync(messages[index]);

        try
        {
            await Task.WhenAll(releases).WaitAsync(budget, Clock).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            Logger.LogWarning(
                "Handing unstarted Azure Service Bus messages back to {Queue} ({Role}) did not finish within the shutdown budget ({ShutdownTimeout}); they redeliver when their locks lapse.",
                queue,
                SubscriberRole,
                budget);
        }

        async Task ReleaseAsync(AzureServiceBusTransportDelivery delivery)
        {
            try
            {
                await delivery.AbandonAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(
                    ex,
                    "Failed to hand unstarted Azure Service Bus message {MessageId} back to {Queue}; it redelivers when its lock lapses.",
                    delivery.MessageId,
                    queue);
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

                    if (progress.IsRenewalSuppressed(i))
                        continue;

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
                    catch (ServiceBusException ex) when (ex.Reason is ServiceBusFailureReason.MessageLockLost)
                    {
                        // Definitive, not transient: a lost lock token can never be renewed again,
                        // and the message stays unsettled until its handler returns — hours, for an
                        // awaited durable-flow step. Say so once and stop asking (SQS ceiling
                        // parity) instead of spending a failing RPC ahead of the healthy messages,
                        // plus a warning, on every remaining beat.
                        progress.SuppressRenewal(i);
                        Logger.LogWarning(
                            ex,
                            "The lock of Azure Service Bus message {MessageId} on {Queue} is lost and can no longer be renewed; Service Bus redelivers it while it is still being processed (at-least-once preserved). Renewal stops for this message.",
                            message.MessageId,
                            queue);
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

    /// <summary>
    /// One <see cref="AzureServiceBusAsyncResponseOptions.ShutdownTimeout"/> shared by the stop
    /// path's spends that the validator sums as a single term — the renewal-free hand-back of
    /// unstarted batch messages and the receiver close after it. Its clock starts at the first
    /// spend drawn from it; each later spend gets only what is left.
    /// </summary>
    private sealed class SharedStopBudget(TimeSpan budget, TimeProvider clock)
    {
        private long? _startedAt;

        /// <summary>What is left of the budget; the first call starts its clock.</summary>
        public TimeSpan Remaining()
        {
            _startedAt ??= clock.GetTimestamp();
            var left = budget - clock.GetElapsedTime(_startedAt.Value);
            return left > TimeSpan.Zero ? left : TimeSpan.Zero;
        }
    }

    private sealed class BatchProgress(int batchSize)
    {
        private readonly bool[] _renewalSuppressed = new bool[batchSize];
        private int _settledCount;

        public int SettledCount => Volatile.Read(ref _settledCount);

        public void MarkSettled() => Interlocked.Increment(ref _settledCount);

        /// <summary>Marks the message at <paramref name="index"/> as beyond renewal; the sweep leaves it alone.</summary>
        public void SuppressRenewal(int index) => Volatile.Write(ref _renewalSuppressed[index], true);

        public bool IsRenewalSuppressed(int index) => Volatile.Read(ref _renewalSuppressed[index]);
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
        var correlationId = !_ingress.IsOverInboundBudget(delivery.Body)
            ? AzureServiceBusCorrelationIdExtractor.Extract(delivery, delivery.Body, Options)
            : null;
        return _ingress.HandleResponseMessageAsync(delivery.Body, correlationId);
    }
}
