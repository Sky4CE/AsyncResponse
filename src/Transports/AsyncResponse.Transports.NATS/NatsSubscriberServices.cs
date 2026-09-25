using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Net;

namespace AsyncResponse.Transports.NATS;

/// <summary>
/// Base hosted service that consumes a JetStream subject through a durable consumer and routes each
/// message to the AsyncResponse ingress with the configured acknowledgement/redelivery/dead-letter
/// policy. A failed consume loop is retried with bounded backoff so a transient NATS outage does not
/// kill the subscriber.
/// </summary>
internal abstract class NatsSubscriberService : BackgroundService
{
    /// <summary>
    /// Re-arm period of the idle long-poll fetch (the NATS.Net default fetch period). Purely how
    /// often an empty wait returns to re-check cancellation — not a delivery deadline.
    /// </summary>
    private static readonly TimeSpan LongPollExpires = TimeSpan.FromSeconds(30);

    /// <summary>
    /// A long poll that comes back empty sooner than this did not expire: the server holds a pull
    /// request for the whole <see cref="LongPollExpires"/> when there is nothing to deliver. Half
    /// the period leaves room for a poll cut short by a reconnect without ever mistaking a
    /// millisecond answer for an expiry.
    /// </summary>
    private static readonly TimeSpan FastEmptyPollThreshold = LongPollExpires / 2;

    /// <summary>
    /// Consecutive fast-empty long polls after which the attempt is handed back to the supervisor,
    /// so the stream/consumer provisioning runs again.
    /// </summary>
    private const int MaxConsecutiveFastEmptyPolls = 5;

    private readonly INatsJetStreamTransport _jetStream;
    private readonly TimeProvider _timeProvider;

    /// <summary>Runs the NatsSubscriberService operation.</summary>
    protected NatsSubscriberService(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsConnection connection,
        ILogger logger)
        : this(options, new NatsJetStreamTransportAdapter(connection.CreateJetStreamContext(), logger, options.Value.StreamReplicas), logger)
    {
    }

    /// <summary>Runs the NatsSubscriberService operation.</summary>
    protected NatsSubscriberService(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsJetStreamTransport jetStream,
        ILogger logger,
        TimeProvider? timeProvider = null)
    {
        Options = options.Value;
        NatsTransportOptionsValidator.ValidateCommon(Options);
        _jetStream = jetStream;
        Logger = logger;
        Schema = new NatsTransportSubjectSchema(Options);

        // Times the idle long poll, paces its fast-empty backoff and the in-progress heartbeat.
        // The system clock in production — what is measured is a real server's answer; the seam
        // exists for tests.
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    protected NatsAsyncResponseTransportOptions Options { get; }
    protected ILogger Logger { get; }
    protected NatsTransportSubjectSchema Schema { get; }

    protected abstract string Subject { get; }
    protected abstract string Stream { get; }
    protected abstract string Consumer { get; }
    protected abstract NatsSubscriberOptions SubscriberOptions { get; }
    protected abstract NatsSubscriberRole Role { get; }
    /// <summary>Handles the delivered message.</summary>
    protected abstract Task HandleMessageAsync(NatsJobDelivery delivery, CancellationToken cancellationToken);

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
        NatsTransportOptionsValidator.ValidateSubscriber(Options, SubscriberOptions, Role.ToString());
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // The dispatcher — and with it the ACK-after-enqueue queue and its workers — belongs to
        // the hosted service, not to one supervised attempt. Disposing it IS the stop-time drain
        // (wait BackgroundDrainTimeout, then cancel and dead-letter whatever is still queued), so
        // owning it per attempt ran that drain on every fetch-loop failure of a host that was NOT
        // stopping: a NATS blip or a JetStream leader election paused consumption for the drain
        // budget and then buried queued, already-ACKed work as "drain budget lapsed" — or lost it
        // outright when the dead-letter publish rode the same failing connection. Nothing in it
        // is per attempt (the JetStream adapter wraps the host's reconnecting connection, and each
        // delivery carries its own settlement handles), so every rebuilt attempt feeds this one
        // instance and only the host stop drains it.
        await using var dispatcher = new NatsMessageDispatcher(
            HandleMessageAsync,
            _jetStream,
            Options,
            SubscriberOptions,
            Schema,
            Logger,
            Role,
            Consumer);

        await SubscriberSupervisor.RunAsync(
            attemptToken => RunSubscriberAsync(dispatcher, attemptToken),
            stoppingToken,
            failures => AsyncResponseRetry.Backoff(failures, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay),
            (ex, retryDelay) => Logger.LogWarning(ex, "NATS subscriber failed for subject {Subject} ({Role}); retrying in {RetryDelay}.", Subject, Role, retryDelay),
            healthyRunThreshold: Options.SubscriberRetryMaxDelay).ConfigureAwait(false);
    }

    private async Task RunSubscriberAsync(NatsMessageDispatcher dispatcher, CancellationToken stoppingToken)
    {
        if (Options.CreateStreams)
        {
            await _jetStream.EnsureStreamAsync(Stream, Subject, Options.StreamMaxMessages, stoppingToken).ConfigureAwait(false);
            if (Options.DeadLetterEnabled)
                await _jetStream.EnsureDeadLetterStreamAsync(Schema.DeadLetterStream, Schema.DeadLetterSubject, Options.DeadLetterStreamMaxMessages, stoppingToken).ConfigureAwait(false);
        }

        var liveAckWait = await _jetStream.EnsureConsumerAsync(Stream, Consumer, Options.AckWait, SubscriberOptions.MaxDeliveryAttempts, stoppingToken).ConfigureAwait(false);

        // The heartbeat must land inside the window the server actually enforces: the consumer is
        // never modified, so after AckWait is raised (or on an operator-provisioned durable) the
        // live consumer's ack wait can be the shorter one — renewing from the options alone let it
        // lapse between renewals and redeliver messages under live handlers.
        var renewalInterval = RenewalIntervalFor(liveAckWait < Options.AckWait ? liveAckWait : Options.AckWait);

        Logger.LogInformation(
            "NATS subscriber started. Subject: {Subject}. Stream: {Stream}. Consumer: {Consumer}. Role: {Role}. AckMode: {AckMode}.",
            Subject, Stream, Consumer, Role, SubscriberOptions.AckMode);

        // JetStream counts a delivery when it hands the message over, not when a handler starts.
        // ACK-after-handler runs its batch serially, so every message prefetched behind a handler
        // that kills the process (stack overflow, OOM, FailFast) came back with its count bumped
        // without ever having run — and MaxDeliveryAttempts crashes later the pre-execution cap
        // dead-lettered up to BatchSize-1 healthy batch-mates along with the poison one. One
        // message per fetch leaves the rest on the stream, where nothing is counted and any peer
        // can take it. ACK-after-enqueue settles each message as it is accepted, so it keeps the
        // batch.
        var fetchSize = SubscriberOptions.AckMode is NatsAckMode.AckAfterEnqueue ? SubscriberOptions.BatchSize : 1;
        var batch = new List<NatsJobDelivery>(fetchSize);
        var fastEmptyPolls = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            // The flow engine handed a delivery back: the host IS stopping — the engine reacts to
            // ApplicationStopping, which fires before this subscriber's token — so fetch nothing
            // more; wait for the stop. Ending only the delivery kept the loop fetching through the
            // whole stop window, and every flow wake-up fetched there was handed back too: an
            // attempt spent on a stopping host (a live peer would have taken it at once) — or, in
            // early ACK, already ACKed, so each one became a dead-letter copy.
            if (dispatcher.HandBackSignalled)
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken).ConfigureAwait(false);
                break;
            }

            batch.Clear();

            // Drain whatever is already available, up to the fetch size. The batch is
            // materialized out of the client buffer BEFORE dispatch so the in-progress heartbeat
            // below can reach every waiting message: the server starts each message's AckWait
            // clock at delivery, and an open-ended consume buffered the whole prefetch
            // client-side — a serial batch whose handlers together outlast AckWait had its tail
            // redelivered to a competing consumer (and NumDelivered climbed toward the Term cap)
            // while it was still queued here.
            await foreach (var delivery in _jetStream.FetchNoWaitAsync(Stream, Consumer, fetchSize, stoppingToken).ConfigureAwait(false))
                batch.Add(delivery);

            if (batch.Count == 0)
            {
                // Nothing waiting: long-poll for a single message so idle delivery latency stays
                // push-like. Siblings arriving behind the long-polled message stay ON the stream
                // — where AckWait has not started — until the next no-wait drain.
                var pollStarted = _timeProvider.GetTimestamp();
                await foreach (var delivery in _jetStream.FetchAsync(Stream, Consumer, maxMessages: 1, LongPollExpires, stoppingToken).ConfigureAwait(false))
                    batch.Add(delivery);

                if (batch.Count == 0)
                {
                    fastEmptyPolls = await BackOffAfterEmptyLongPollAsync(_timeProvider.GetElapsedTime(pollStarted), fastEmptyPolls, stoppingToken).ConfigureAwait(false);
                    continue;
                }
            }

            fastEmptyPolls = 0;
            await DispatchBatchAsync(dispatcher, batch, renewalInterval, stoppingToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Tells an expired long poll (re-arm at once) from one the server never held. A pull request
    /// that reaches no live consumer — the durable or its stream was deleted, or JetStream has no
    /// leader for it — is answered "503 no responders", which the client ends WITHOUT an
    /// exception: exactly what an expiry looks like, only in a millisecond. Re-arming on that spun
    /// the loop thousands of times a second against a cluster that was already in trouble, and
    /// since nothing ever threw, the supervisor never reran the provisioning that would have
    /// recreated the consumer — the subscriber stayed dead until the process restarted. (A poll
    /// in flight when the consumer is deleted does throw; the silent case is the replica that was
    /// inside a handler at that moment.) Returns the updated consecutive fast-empty count.
    /// </summary>
    private async Task<int> BackOffAfterEmptyLongPollAsync(TimeSpan pollDuration, int fastEmptyPolls, CancellationToken stoppingToken)
    {
        if (pollDuration >= FastEmptyPollThreshold || stoppingToken.IsCancellationRequested)
            return 0; // the long poll expired empty; re-arm

        fastEmptyPolls++;
        if (fastEmptyPolls >= MaxConsecutiveFastEmptyPolls)
        {
            throw new InvalidOperationException(
                $"NATS consumer '{Consumer}' on stream '{Stream}' answered {fastEmptyPolls} consecutive long polls empty without holding them " +
                $"(the last one returned after {pollDuration.TotalMilliseconds:F0} ms of a {LongPollExpires.TotalSeconds:F0} s wait): " +
                "the pull requests are not reaching a live consumer — it or its stream was deleted, or JetStream has no leader for it. Rebuilding the subscriber.");
        }

        var delay = AsyncResponseRetry.Backoff(fastEmptyPolls, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay);
        Logger.LogDebug(
            "NATS long poll for {Role} returned empty after {PollDuration} instead of being held; backing off {Delay} before polling again ({FastEmptyPolls}/{MaxFastEmptyPolls}).",
            Role,
            pollDuration,
            delay,
            fastEmptyPolls,
            MaxConsecutiveFastEmptyPolls);
        await Task.Delay(delay, _timeProvider, stoppingToken).ConfigureAwait(false);
        return fastEmptyPolls;
    }

    private async Task DispatchBatchAsync(
        NatsMessageDispatcher dispatcher,
        List<NatsJobDelivery> batch,
        TimeSpan renewalInterval,
        CancellationToken stoppingToken)
    {
        // The batch is dispatched serially, so a slow handler lets the server-side AckWait of the
        // later (still unsettled) messages lapse into redelivery. While the batch is in flight, a
        // heartbeat signals in-progress for every unsettled message to reset its AckWait window.
        // The heartbeat is NOT tied to the stop token: a handler takes no token, so it outlives
        // the stop signal, and cancelling its renewal there let AckWait lapse under the live
        // handler on every rolling deploy — the job was redelivered to a peer and ran twice. It
        // ends only when this loop has let go of every message.
        var progress = new BatchProgress();
        using var renewalCancellation = new CancellationTokenSource();
        var renewalTask = RenewInProgressLoopAsync(batch, progress, renewalInterval, renewalCancellation.Token);
        var next = 0;
        try
        {
            for (; next < batch.Count; next++)
            {
                // Stopping: do not start what has not started. The rest of the batch used to run
                // on, handler after handler, past the stop signal. A hand-back from the flow
                // engine (inline, or in an early-ACK worker) is the same signal, arriving before
                // the token.
                if (stoppingToken.IsCancellationRequested || dispatcher.HandBackSignalled)
                    break;

                try
                {
                    await dispatcher.HandleAsync(batch[next], stoppingToken).ConfigureAwait(false);
                }
                finally
                {
                    progress.MarkSettled();
                }
            }
        }
        finally
        {
            // Hand back whatever never started (a stop, or a handler cancelled by it, cut the
            // batch short) while the heartbeat still covers it.
            for (var i = Math.Max(next, progress.SettledCount); i < batch.Count; i++)
                await ReleaseUnstartedAsync(batch[i]).ConfigureAwait(false);

            renewalCancellation.Cancel();
            try
            {
                // Cancellation exits the sweep between messages and aborts the in-flight
                // heartbeat (the token reaches the SDK call), so this normally completes at once.
                // The bound is the hard backstop for a heartbeat the client cannot abort — a
                // write wedged on a dead socket: an unbounded join here held the loop after every
                // message in the batch had settled, so no further batch was fetched and a stop
                // never completed, with nothing for the supervisor to restart. Past one heartbeat
                // interval the loop is abandoned; the server's AckWait settles whatever it left.
                await renewalTask.WaitAsync(renewalInterval, _timeProvider).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                Logger.LogWarning(
                    "NATS in-progress heartbeat for {Role} did not stop within {RenewalInterval} after its batch settled; abandoning it — unsettled deliveries fall back to the server-side AckWait.",
                    Role,
                    renewalInterval);
                _ = renewalTask.ContinueWith(
                    static (task, state) => ((ILogger)state!).LogWarning(task.Exception, "Abandoned NATS in-progress heartbeat faulted."),
                    Logger,
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
        }
    }

    /// <summary>
    /// Hands a prefetched message that never started back to the server, with no redelivery delay
    /// so an idle peer can take it at once. Leaving it unsettled instead would pin it for the rest
    /// of its AckWait window — the stop that cut the batch short is usually a rolling deploy, and
    /// the messages behind the handler are exactly the work the surviving replicas should pick up.
    /// A failure here is not worth failing the stop over: the window lapses and the server
    /// redelivers anyway, which is the same outcome one AckWait later.
    /// </summary>
    private async Task ReleaseUnstartedAsync(NatsJobDelivery delivery)
    {
        try
        {
            await delivery.NakAsync(TimeSpan.Zero).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogDebug(
                ex,
                "Failed to hand back an unstarted NATS message on subject {Subject} ({Role}); it redelivers when its AckWait lapses.",
                delivery.Subject,
                Role);
        }
    }

    /// <summary>
    /// ~AckWait/3: two chances to land a renewal inside every AckWait window even when one sweep
    /// is delayed by a slow round trip. Also the bound on joining the renewal loop after a batch.
    /// </summary>
    private static TimeSpan RenewalIntervalFor(TimeSpan ackWait) => TimeSpan.FromMilliseconds(Math.Max(1, ackWait.TotalMilliseconds / 3));

    private async Task RenewInProgressLoopAsync(
        List<NatsJobDelivery> batch,
        BatchProgress progress,
        TimeSpan interval,
        CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                await Task.Delay(interval, _timeProvider, cancellationToken).ConfigureAwait(false);

                // Renew from the first unsettled message onward: that covers the message
                // currently in the handler plus everything still waiting its turn. A settle
                // racing this sweep is harmless — Ack/Nak/Term has already consumed the delivery
                // server-side, and a late in-progress signal for it is ignored rather than
                // un-settling anything, so no suppression mark is needed (unlike the SQS twin,
                // whose failure path re-arms a visibility a late renewal could stretch).
                for (var i = progress.SettledCount; i < batch.Count; i++)
                {
                    // The batch finished or the subscriber is stopping: exit quietly between
                    // messages instead of spending a round trip on each remaining renewal.
                    if (cancellationToken.IsCancellationRequested)
                        return;

                    if (i < progress.SettledCount)
                        continue;

                    var delivery = batch[i];
                    try
                    {
                        await delivery.ProgressAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        Logger.LogWarning(
                            ex,
                            "Failed to signal in-progress for NATS message on subject {Subject} ({Role}); its AckWait may lapse and it may redeliver while still queued (at-least-once preserved).",
                            delivery.Subject,
                            Role);
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
        // Settled only ever increments, so a monotonic volatile read is enough — no lock, and a
        // stale read only renews an already-settled message once more (which the server ignores).
        private int _settledCount;

        public int SettledCount => Volatile.Read(ref _settledCount);

        public void MarkSettled() => Interlocked.Increment(ref _settledCount);
    }
}

/// <summary>Consumes worker-job messages and executes them through the AsyncResponse ingress.</summary>
internal sealed class NatsWorkerSubscriber : NatsSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    /// <summary>Runs the NatsWorkerSubscriber operation.</summary>
    public NatsWorkerSubscriber(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsConnection connection,
        IAsyncResponseIngress ingress,
        ILogger<NatsWorkerSubscriber> logger)
        : base(options, connection, logger)
        => _ingress = ingress;

    internal NatsWorkerSubscriber(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsJetStreamTransport jetStream,
        IAsyncResponseIngress ingress,
        ILogger<NatsWorkerSubscriber> logger,
        TimeProvider? timeProvider = null)
        : base(options, jetStream, logger, timeProvider)
        => _ingress = ingress;

    protected override string Subject => Schema.WorkerSubject;
    protected override string Stream => Schema.WorkerStream;
    protected override string Consumer => Options.WorkerConsumer;
    protected override NatsSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override NatsSubscriberRole Role => NatsSubscriberRole.Worker;

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(NatsJobDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(delivery.Payload);
}

/// <summary>Consumes response messages and feeds them into the AsyncResponse ingress, correlated by header or JSON body.</summary>
internal sealed class NatsResponseIngressSubscriber : NatsSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    /// <summary>Runs the NatsResponseIngressSubscriber operation.</summary>
    public NatsResponseIngressSubscriber(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsConnection connection,
        IAsyncResponseIngress ingress,
        ILogger<NatsResponseIngressSubscriber> logger)
        : base(options, connection, logger)
        => _ingress = ingress;

    internal NatsResponseIngressSubscriber(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsJetStreamTransport jetStream,
        IAsyncResponseIngress ingress,
        ILogger<NatsResponseIngressSubscriber> logger,
        TimeProvider? timeProvider = null)
        : base(options, jetStream, logger, timeProvider)
        => _ingress = ingress;

    protected override string Subject => Schema.ResponseSubject;
    protected override string Stream => Schema.ResponseStream;
    protected override string Consumer => Options.ResponseConsumer;
    protected override NatsSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
    protected override NatsSubscriberRole Role => NatsSubscriberRole.ResponseIngress;

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(NatsJobDelivery delivery, CancellationToken cancellationToken)
    {
        var correlationId = !_ingress.IsOverInboundBudget(delivery.Payload)
            ? NatsCorrelationIdExtractor.Extract(delivery.Headers, delivery.Payload, Options)
            : null;
        return _ingress.HandleResponseMessageAsync(delivery.Payload, correlationId);
    }
}
