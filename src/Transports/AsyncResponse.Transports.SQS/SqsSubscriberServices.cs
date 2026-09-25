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

    /// <summary>Measures how long a delivery has been in flight; replaced by tests to reach the 12-hour ceiling.</summary>
    internal TimeProvider Clock { get; set; } = TimeProvider.System;

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

        // The dispatcher outlives the supervised attempts below. In ACK-after-enqueue mode it
        // holds work that was already DELETED at the broker, and disposing it runs the stop-time
        // drain — so scoped to one attempt, any receive fault (throttling, a network blip) on a
        // host that is NOT stopping paused consumption for the drain budget and then surfaced
        // the still-queued work as lapsed, work SQS can never redeliver. It captures nothing
        // per-attempt (deliveries carry their own settlement), so only host stop drains it.
        await using var dispatcher = SqsMessageDispatcher.Create(
            HandleMessageAsync,
            Options,
            SubscriberOptions,
            Logger,
            queue,
            SubscriberRole);

        await SubscriberSupervisor.RunAsync(
            ct => RunSubscriberAsync(queue, dispatcher, ct),
            stoppingToken,
            failures => AsyncResponseRetry.Backoff(
                failures,
                Options.SubscriberRetryBaseDelay,
                Options.SubscriberRetryMaxDelay),
            (ex, retryDelay) => Logger.LogWarning(
                ex,
                "SQS subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.",
                queue,
                SubscriberRole,
                retryDelay),
            healthyRunThreshold: Options.SubscriberRetryMaxDelay).ConfigureAwait(false);
    }

    private async Task RunSubscriberAsync(string queue, SqsMessageDispatcher dispatcher, CancellationToken stoppingToken)
    {
        // A queue configured by name resolves through GetQueueUrl; failures here (queue not yet
        // provisioned, endpoint still starting) surface to the retry loop above.
        var queueUrl = SqsQueueAddress.IsUrl(queue)
            ? queue
            : await _client.GetQueueUrlAsync(queue, stoppingToken).ConfigureAwait(false);

        Logger.LogInformation(
            "SQS subscriber started. Queue: {Queue}. Role: {Role}. AckMode: {AckMode}.",
            queue,
            SubscriberRole,
            SubscriberOptions.AckMode);

        // SQS counts every message a receive hands over (ApproximateReceiveCount) and starts its
        // in-flight clock at the receive, not when a handler starts. ACK-after-handler works a
        // batch serially, so every worker job received behind a handler that kills the process
        // (stack overflow, OOM, FailFast) came back with its count bumped without ever having
        // run — the redrive policy then buried healthy batch-mates of a poison message — and the
        // later batch positions spent their visibility (and the 12-hour ceiling) waiting, or were
        // started after it had already lapsed. On FIFO a failed message's later same-group
        // batch-mates also ran ahead of its redelivery. One message per receive leaves the rest on
        // the queue, where any peer can take it (NATS parity). Early ACK deletes each message as
        // it is accepted, and the response ingress runs the library's own short handler, so both
        // keep the batch — a billed ReceiveMessage per response would cost ten times the calls for
        // no such risk.
        var receiveSize = SubscriberOptions.AckMode is SqsAckMode.AckAfterHandlerCompletes
            && SubscriberRole is SqsSubscriberRole.Worker
                ? 1
                : Options.MaxMessagesPerReceive;
        var emptyShortPolls = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            // In early-ACK mode, receiving while the background queue is saturated would burn the
            // queue's redrive policy (SQS counts every receive), so wait for free capacity and never
            // request more messages than the dispatcher can accept.
            await dispatcher.WaitForCapacityAsync(stoppingToken).ConfigureAwait(false);
            var maxMessages = Math.Min(receiveSize, dispatcher.FreeCapacity);

            // Stamped BEFORE the call: the 12-hour in-flight ceiling counts from the broker-side
            // receive, which happens somewhere inside the long poll, so measuring from here can
            // only over-estimate a delivery's age — the safe direction for the renewal clamp.
            var receiveStarted = Clock.GetTimestamp();
            var deliveries = await _client.ReceiveMessagesAsync(
                new SqsReceiveRequest(
                    queueUrl,
                    maxMessages,
                    Options.ReceiveWaitTime,
                    SubscriberOptions.VisibilityTimeout),
                stoppingToken).ConfigureAwait(false);

            if (deliveries.Count == 0)
            {
                // A long poll holds an empty receive for the whole wait (any positive
                // ReceiveWaitTime reaches the wire as at least one second). Short polling —
                // ReceiveWaitTime = 0 — answers at once, so on an idle queue the loop re-polled
                // once per network round trip: billed ReceiveMessage calls around the clock, per
                // subscriber and replica. Back off on the subscriber retry schedule instead, and
                // start over on the first message (NATS fast-empty-poll parity).
                if (Options.ReceiveWaitTime == TimeSpan.Zero)
                {
                    var delay = AsyncResponseRetry.Backoff(++emptyShortPolls, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay);
                    await Task.Delay(delay, Clock, stoppingToken).ConfigureAwait(false);
                }

                continue;
            }

            emptyShortPolls = 0;
            if (await DispatchBatchAsync(dispatcher, deliveries, queue, receiveStarted, stoppingToken).ConfigureAwait(false))
            {
                // The flow engine handed a delivery back: ApplicationStopping has fired and this
                // subscriber's own stop is next. Every job received from now on would be
                // interrupted the same way and left invisible for a whole visibility timeout —
                // among them the wake-ups the engine hands over for a replica still running to take
                // at once. So stop receiving for the rest of the attempt (Redis/NATS rule) and wait
                // for the stop.
                Logger.LogInformation(
                    "SQS {Role} subscriber for {Queue} stops receiving: the flow engine handed a delivery back because the host is stopping.",
                    SubscriberRole,
                    queue);
                await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Dispatches one received batch; returns <c>true</c> when the flow engine handed a delivery
    /// back because the host is stopping, which ends the batch the way the stop itself does.
    /// </summary>
    private async Task<bool> DispatchBatchAsync(
        SqsMessageDispatcher dispatcher,
        IReadOnlyList<SqsTransportDelivery> deliveries,
        string queue,
        long receiveStarted,
        CancellationToken stoppingToken)
    {
        if (deliveries.Count == 0)
            return false;

        var next = 0;
        var handedBack = false;
        if (SubscriberOptions.AckMode is not SqsAckMode.AckAfterHandlerCompletes
            || SubscriberOptions.VisibilityRenewalInterval is not { } renewalInterval
            || SubscriberOptions.VisibilityTimeout is not { } visibilityTimeout)
        {
            var settled = 0;
            try
            {
                for (; next < deliveries.Count; next++)
                {
                    // The handler takes no token, so a stop cannot interrupt the message in hand —
                    // but it must not START the rest of the batch: every fresh handler runs against
                    // the host's shutdown budget and is killed mid-flight when that lapses.
                    if (stoppingToken.IsCancellationRequested)
                        break;

                    try
                    {
                        handedBack = await dispatcher.HandleAsync(deliveries[next], stoppingToken).ConfigureAwait(false)
                            is SqsDispatchOutcome.HandedBack;
                    }
                    finally
                    {
                        settled++;
                    }

                    if (handedBack)
                        break;
                }
            }
            finally
            {
                // Hand back whatever never started: a stop, a flow hand-back, or a handler that
                // exited through the stop's cancellation — a parked flow's interruption unwinds
                // past the stop check above — cut the batch short (NATS rule: a message whose
                // handler ran is past `next`).
                await HandBackUnstartedAsync(deliveries, Math.Max(next, settled), progress: null, queue).ConfigureAwait(false);
            }

            return handedBack;
        }

        // The batch is processed serially, so a slow handler lets the visibility timeout of the later
        // (still unprocessed) messages lapse and a competing consumer processes them a second time.
        // While the batch is in flight, a heartbeat resets every unsettled message's invisibility to
        // the configured visibility timeout.
        var progress = new BatchProgress(deliveries.Count);
        // NOT linked to the stop token: the handler in flight when the host stops keeps running
        // (it takes no token), and ending its heartbeat at that moment let its visibility lapse
        // under a live handler — a competing consumer then ran the same job a second time on
        // every rolling deploy. The heartbeat ends when the batch loop does.
        using var renewalCancellation = new CancellationTokenSource();
        var renewalTask = RenewVisibilityLoopAsync(
            deliveries,
            progress,
            renewalInterval,
            visibilityTimeout,
            queue,
            receiveStarted,
            renewalCancellation.Token);
        try
        {
            for (; next < deliveries.Count; next++)
            {
                // Same stop rule as the renewal-free path above. Without it the loop kept
                // starting the rest of the batch serially after the stop — with the heartbeat
                // already cancelled, so those handlers outlived their visibility.
                if (stoppingToken.IsCancellationRequested)
                    break;

                var delivery = deliveries[next];
                var batchIndex = next;
                // The dispatcher's failure path shortens visibility to RedeliveryDelay while the
                // heartbeat still counts the message as unsettled (MarkSettled runs only after
                // HandleAsync returns). Routing the dispatcher's visibility changes through a
                // suppression mark blocks future renewals; the per-message gate joins any renewal
                // already in flight before applying the shorter retry delay.
                var tracked = delivery with
                {
                    ChangeVisibilityAsync = async (timeout, token) =>
                    {
                        progress.SuppressRenewal(batchIndex);
                        // A renewal may already be in flight. Its reply must settle before the
                        // retry delay is applied, otherwise it can overwrite that shorter delay.
                        var gate = progress.VisibilityGate(batchIndex);
                        // Not the stop token: since the handler in flight at the stop keeps
                        // running, its failure can land after the stop, and an already-cancelled
                        // token made the wait throw at once — even with the gate free — so the
                        // retry delay was silently skipped and the message sat out its full
                        // visibility. Settlement ignores cancellation; the timeout bounds it.
                        if (!await gate.WaitAsync(Options.ShutdownTimeout, CancellationToken.None).ConfigureAwait(false))
                            throw new TimeoutException("SQS visibility renewal did not settle before the retry-delay update budget elapsed.");
                        try
                        {
                            await delivery.ChangeVisibilityAsync(timeout, token).ConfigureAwait(false);
                        }
                        finally
                        {
                            gate.Release();
                        }
                    }
                };
                try
                {
                    handedBack = await dispatcher.HandleAsync(tracked, stoppingToken).ConfigureAwait(false)
                        is SqsDispatchOutcome.HandedBack;
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
            // Hand back whatever never started — a stop, a flow hand-back, or a handler that
            // exited through the stop's cancellation, cut the batch short (NATS rule) — while the
            // suppression marks and per-message gates still order it after any renewal in flight.
            var handBack = HandBackUnstartedAsync(deliveries, Math.Max(next, progress.SettledCount), progress, queue);
            renewalCancellation.Cancel();
            try
            {
                // Cancellation exits the sweep between messages and reaches the in-flight
                // ChangeVisibility call through its token, so this join normally completes at
                // once. The bound covers the one case the token cannot end promptly — an SDK call
                // mid-retry against a degraded endpoint — which would otherwise stall the receive
                // loop (and, at shutdown, the host's stop budget) for the SDK retry budget.
                await renewalTask.WaitAsync(Options.ShutdownTimeout).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                Logger.LogWarning(
                    "SQS visibility renewal for {Queue} ({Role}) did not stop within the shutdown budget ({ShutdownTimeout}); abandoning the renewal task.",
                    queue,
                    SubscriberRole,
                    Options.ShutdownTimeout);
            }

            // Started before the join above and bounded by the same ShutdownTimeout, so the two
            // overlap: the stop path still spends one ShutdownTimeout here, not two.
            await handBack.ConfigureAwait(false);
        }

        return handedBack;
    }

    /// <summary>
    /// Makes the batch messages that were never started visible again at once. Left alone they
    /// stay invisible for the rest of their visibility timeout although nothing is processing
    /// them, stalling that work for as long on every rolling deploy; the receive already counted
    /// toward the redrive policy either way. Best-effort and bounded by
    /// <see cref="SqsAsyncResponseOptions.ShutdownTimeout"/>: a message that cannot be handed back
    /// simply reappears when its visibility timeout lapses.
    /// </summary>
    private async Task HandBackUnstartedAsync(
        IReadOnlyList<SqsTransportDelivery> deliveries,
        int firstUnstarted,
        BatchProgress? progress,
        string queue)
    {
        if (firstUnstarted >= deliveries.Count)
            return;

        var budget = new CancellationTokenSource(Options.ShutdownTimeout);
        var releases = new Task[deliveries.Count - firstUnstarted];
        for (var index = firstUnstarted; index < deliveries.Count; index++)
            releases[index - firstUnstarted] = ReleaseAsync(index);

        try
        {
            await Task.WhenAll(releases).WaitAsync(Options.ShutdownTimeout).ConfigureAwait(false);
            budget.Dispose();
        }
        catch (TimeoutException)
        {
            // An SDK call mid-retry ignored the budget token. The source stays undisposed: the
            // abandoned calls still hold its token.
            Logger.LogWarning(
                "Handing unstarted SQS messages back to {Queue} ({Role}) did not finish within the shutdown budget ({ShutdownTimeout}); they reappear when their visibility timeout lapses.",
                queue,
                SubscriberRole,
                Options.ShutdownTimeout);
        }

        async Task ReleaseAsync(int index)
        {
            var delivery = deliveries[index];
            SemaphoreSlim? gate = null;
            try
            {
                if (progress is not null)
                {
                    // Same ordering rule as the retry delay: a renewal already in flight for this
                    // receipt must settle first, or its reply overwrites the release.
                    progress.SuppressRenewal(index);
                    await progress.VisibilityGate(index).WaitAsync(budget.Token).ConfigureAwait(false);
                    gate = progress.VisibilityGate(index);
                }

                await delivery.ChangeVisibilityAsync(TimeSpan.Zero, budget.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogWarning(
                    ex,
                    "Failed to hand unstarted SQS message {MessageId} back to {Queue} while stopping; it reappears when its visibility timeout lapses.",
                    delivery.MessageId,
                    queue);
            }
            finally
            {
                gate?.Release();
            }
        }
    }

    /// <summary>
    /// The visibility a renewal may still request for a delivery received
    /// <paramref name="inFlight"/> ago, or <c>null</c> once nothing is left. SQS never keeps a
    /// message invisible for more than 12 hours from its receive and REJECTS — it does not
    /// truncate — a <c>ChangeMessageVisibility</c> that would cross that, so an unclamped renewal
    /// fails on every beat from <c>12 h − VisibilityTimeout</c> onward and forfeits the tail of
    /// the ceiling. Rounded down to whole seconds because the adapter rounds requests up.
    /// </summary>
    internal static TimeSpan? ClampRenewalToInFlightCeiling(TimeSpan visibilityTimeout, TimeSpan inFlight)
    {
        var remaining = TimeSpan.FromSeconds(Math.Floor((SqsWorkerTransport.SqsMaxInFlightDuration - inFlight).TotalSeconds));
        if (remaining <= TimeSpan.Zero)
            return null;

        return visibilityTimeout < remaining ? visibilityTimeout : remaining;
    }

    private async Task RenewVisibilityLoopAsync(
        IReadOnlyList<SqsTransportDelivery> deliveries,
        BatchProgress progress,
        TimeSpan renewalInterval,
        TimeSpan visibilityTimeout,
        string queue,
        long receiveStarted,
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
                    // The batch finished or the subscriber is stopping: exit quietly between
                    // messages instead of spending up to a full SDK retry budget on each remaining
                    // renew (ASB-twin parity).
                    if (cancellationToken.IsCancellationRequested)
                        return;

                    if (i < progress.SettledCount || progress.IsRenewalSuppressed(i))
                        continue;

                    var delivery = deliveries[i];
                    try
                    {
                        var gate = progress.VisibilityGate(i);
                        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
                        try
                        {
                            if (i < progress.SettledCount || progress.IsRenewalSuppressed(i))
                                continue;

                            var extension = ClampRenewalToInFlightCeiling(visibilityTimeout, Clock.GetElapsedTime(receiveStarted));
                            if (extension is { } clamped)
                                await delivery.ChangeVisibilityAsync(clamped, cancellationToken).ConfigureAwait(false);

                            if (extension != visibilityTimeout)
                            {
                                // The ceiling, not a renewal fault: nothing can extend this
                                // delivery any further, so say so once and stop asking instead of
                                // logging a rejected renewal on every remaining beat.
                                progress.SuppressRenewal(i);
                                Logger.LogWarning(
                                    "SQS message {MessageId} on {Queue} has reached the 12-hour SQS in-flight ceiling; its visibility cannot be extended further and SQS will redeliver it while its handler is still running.",
                                    delivery.MessageId,
                                    queue);
                            }
                        }
                        finally
                        {
                            gate.Release();
                        }
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                    {
                        // Only OUR token ends the sweep. The AWS SDK surfaces its own client-side
                        // HTTP timeout as TaskCanceledException with the caller's token untouched,
                        // and excluding every OperationCanceledException let that escape to the
                        // outer catch — whose body is just a comment — silently ending renewal for
                        // the whole remaining batch. Messages 3..N then went visible mid-processing
                        // and a peer re-ran them: systematic duplicate execution with no log line.
                        // Same idiom the durable-flow start ladder and the DB channel already use.
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
        // Gates are per message: a stuck renewal for one receipt cannot block another receipt's
        // retry. Do not dispose gates while an abandoned SDK call may still release one.
        private readonly bool[] _renewalSuppressed;
        private readonly SemaphoreSlim[] _visibilityGates;
        private int _settledCount;

        public BatchProgress(int batchSize)
        {
            _renewalSuppressed = new bool[batchSize];
            _visibilityGates = Enumerable.Range(0, batchSize).Select(_ => new SemaphoreSlim(1, 1)).ToArray();
        }

        public SemaphoreSlim VisibilityGate(int index) => _visibilityGates[index];

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
    private readonly bool _durableFlowsRegistered;

    /// <summary>Creates a worker subscriber for the configured SQS worker queue.</summary>
    public SqsWorkerSubscriber(
        IOptions<SqsAsyncResponseOptions> options,
        ISqsClient client,
        IAsyncResponseIngress ingress,
        ILogger<SqsWorkerSubscriber> logger,
        IEnumerable<DurableFlowOptions>? durableFlowOptions = null)
        : base(options, client, logger)
    {
        _ingress = ingress;
        _durableFlowsRegistered = durableFlowOptions?.Any() == true;
    }

    /// <summary>
    /// Validates like every subscriber, then warns about queue pairs that may be one queue (once
    /// per process — the worker subscriber speaks for the transport) and about the two SQS worker
    /// configurations in which durable flows run against limits the engine cannot see (interim
    /// guidance; see docs/transport-semantics.md).
    /// </summary>
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        var start = base.StartAsync(cancellationToken);
        foreach (var collision in SqsOptionsValidator.PossibleQueueCollisions(Options))
        {
            Logger.LogWarning(
                "SQS {Collision} may be one queue: a queue name resolves in the client's own account and region, so the two collide unless the URL names another account's or region's queue. Configure both as URLs to make the comparison exact.",
                collision);
        }

        if (_durableFlowsRegistered)
            WarnAboutDurableFlowLimits();
        return start;
    }

    private void WarnAboutDurableFlowLimits()
    {
        if (SqsQueueAddress.IsFifo(Options.WorkerQueue))
        {
            // Flow start, resume and wake-up jobs carry no correlation id unless the flow was
            // started inside a request scope, and FIFO keeps every flow timer in process.
            Logger.LogWarning(
                "The SQS worker queue {Queue} is a FIFO queue and durable flows are registered: every job without a correlation id — durable-flow start, resume and wake-up jobs among them — shares the single MessageGroupId '{FallbackGroup}' ({FallbackOption}), which SQS delivers strictly one at a time across all consumers. FIFO also keeps flow timers in process, so one flow parked on a timer or an awaited step holds that group — and with it every other flow — for as long as it waits. Prefer a standard worker queue for durable flows.",
                Options.WorkerQueue,
                Options.FifoMessageGroupIdFallback,
                $"{nameof(SqsAsyncResponseOptions)}.{nameof(SqsAsyncResponseOptions.FifoMessageGroupIdFallback)}");
        }

        if (SubscriberOptions is { AckMode: SqsAckMode.AckAfterHandlerCompletes, VisibilityRenewalInterval: null, VisibilityTimeout: null })
        {
            // The transport cannot read the queue's own visibility timeout, so it advertises the
            // 12-hour SQS maximum as the in-flight ceiling the engine plans in-process waits
            // against — while SQS redelivers after the queue's visibility timeout (30 s unless
            // configured otherwise).
            Logger.LogWarning(
                "The SQS worker subscriber for {Queue} sets neither {VisibilityTimeout} nor {RenewalInterval}, so the queue's own visibility timeout (30 seconds unless configured otherwise) decides when SQS redelivers an in-flight job — a value the transport cannot read, so it advertises the 12-hour SQS maximum to the durable-flow engine instead. An in-process timer park or awaited step longer than the real visibility timeout then runs a second copy of the job on a peer, without a warning. Set the visibility timeout option to the queue's value, or enable renewal.",
                Options.WorkerQueue,
                $"{nameof(SqsAsyncResponseOptions.WorkerSubscriber)}.{nameof(SqsSubscriberOptions.VisibilityTimeout)}",
                $"{nameof(SqsAsyncResponseOptions.WorkerSubscriber)}.{nameof(SqsSubscriberOptions.VisibilityRenewalInterval)}");
        }
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
        var correlationId = !_ingress.IsOverInboundBudget(delivery.Body)
            ? SqsCorrelationIdExtractor.Extract(delivery, delivery.Body, Options)
            : null;
        return _ingress.HandleResponseMessageAsync(delivery.Body, correlationId);
    }
}
