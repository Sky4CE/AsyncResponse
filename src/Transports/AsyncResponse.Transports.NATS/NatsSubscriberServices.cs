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

    private readonly INatsJetStreamTransport _jetStream;

    /// <summary>Runs the NatsSubscriberService operation.</summary>
    protected NatsSubscriberService(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsConnection connection,
        ILogger logger)
        : this(options, new NatsJetStreamTransportAdapter(connection.CreateJetStreamContext(), logger), logger)
    {
    }

    /// <summary>Runs the NatsSubscriberService operation.</summary>
    protected NatsSubscriberService(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsJetStreamTransport jetStream,
        ILogger logger)
    {
        Options = options.Value;
        NatsTransportOptionsValidator.ValidateCommon(Options);
        _jetStream = jetStream;
        Logger = logger;
        Schema = new NatsTransportSubjectSchema(Options);
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

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => SubscriberSupervisor.RunAsync(
            RunSubscriberAsync,
            stoppingToken,
            failures => AsyncResponseRetry.Backoff(failures, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay),
            (ex, retryDelay) => Logger.LogWarning(ex, "NATS subscriber failed for subject {Subject} ({Role}); retrying in {RetryDelay}.", Subject, Role, retryDelay));

    private async Task RunSubscriberAsync(CancellationToken stoppingToken)
    {
        if (Options.CreateStreams)
        {
            await _jetStream.EnsureStreamAsync(Stream, Subject, Options.StreamMaxMessages, stoppingToken).ConfigureAwait(false);
            if (Options.DeadLetterEnabled)
                await _jetStream.EnsureDeadLetterStreamAsync(Schema.DeadLetterStream, Schema.DeadLetterSubject, Options.DeadLetterStreamMaxMessages, stoppingToken).ConfigureAwait(false);
        }

        await _jetStream.EnsureConsumerAsync(Stream, Consumer, Options.AckWait, stoppingToken).ConfigureAwait(false);

        await using var dispatcher = new NatsMessageDispatcher(
            HandleMessageAsync,
            _jetStream,
            Options,
            SubscriberOptions,
            Schema,
            Logger,
            Role,
            Consumer);

        Logger.LogInformation(
            "NATS subscriber started. Subject: {Subject}. Stream: {Stream}. Consumer: {Consumer}. Role: {Role}. AckMode: {AckMode}.",
            Subject, Stream, Consumer, Role, SubscriberOptions.AckMode);

        var batch = new List<NatsJobDelivery>(SubscriberOptions.BatchSize);
        while (!stoppingToken.IsCancellationRequested)
        {
            batch.Clear();

            // Drain whatever is already available, up to the batch size. The batch is
            // materialized out of the client buffer BEFORE dispatch so the in-progress heartbeat
            // below can reach every waiting message: the server starts each message's AckWait
            // clock at delivery, and an open-ended consume buffered the whole prefetch
            // client-side — a serial batch whose handlers together outlast AckWait had its tail
            // redelivered to a competing consumer (and NumDelivered climbed toward the Term cap)
            // while it was still queued here.
            await foreach (var delivery in _jetStream.FetchNoWaitAsync(Stream, Consumer, SubscriberOptions.BatchSize, stoppingToken).ConfigureAwait(false))
                batch.Add(delivery);

            if (batch.Count == 0)
            {
                // Nothing waiting: long-poll for a single message so idle delivery latency stays
                // push-like. Siblings arriving behind the long-polled message stay ON the stream
                // — where AckWait has not started — until the next no-wait drain.
                await foreach (var delivery in _jetStream.FetchAsync(Stream, Consumer, maxMessages: 1, LongPollExpires, stoppingToken).ConfigureAwait(false))
                    batch.Add(delivery);

                if (batch.Count == 0)
                    continue; // the long poll expired empty; re-arm
            }

            await DispatchBatchAsync(dispatcher, batch, stoppingToken).ConfigureAwait(false);
        }
    }

    private async Task DispatchBatchAsync(
        NatsMessageDispatcher dispatcher,
        List<NatsJobDelivery> batch,
        CancellationToken stoppingToken)
    {
        // The batch is dispatched serially, so a slow handler lets the server-side AckWait of the
        // later (still unsettled) messages lapse into redelivery. While the batch is in flight, a
        // heartbeat signals in-progress for every unsettled message to reset its AckWait window.
        var progress = new BatchProgress();
        using var renewalCancellation = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var renewalTask = RenewInProgressLoopAsync(batch, progress, renewalCancellation.Token);
        try
        {
            foreach (var delivery in batch)
            {
                try
                {
                    await dispatcher.HandleAsync(delivery, stoppingToken).ConfigureAwait(false);
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

    private async Task RenewInProgressLoopAsync(
        List<NatsJobDelivery> batch,
        BatchProgress progress,
        CancellationToken cancellationToken)
    {
        // ~AckWait/3: two chances to land a renewal inside every AckWait window even when one
        // sweep is delayed by a slow round trip.
        var interval = TimeSpan.FromMilliseconds(Math.Max(1, Options.AckWait.TotalMilliseconds / 3));
        try
        {
            while (true)
            {
                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);

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
                        await delivery.ProgressAsync().ConfigureAwait(false);
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
        ILogger<NatsWorkerSubscriber> logger)
        : base(options, jetStream, logger)
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
        ILogger<NatsResponseIngressSubscriber> logger)
        : base(options, jetStream, logger)
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
