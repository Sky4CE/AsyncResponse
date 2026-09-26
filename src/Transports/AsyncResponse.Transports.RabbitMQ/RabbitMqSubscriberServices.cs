using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text;

namespace AsyncResponse.Transports.RabbitMQ;

internal abstract class RabbitMqSubscriberService : BackgroundService
{
    private readonly IRabbitMqConnectionFactory _connectionFactory;

    /// <summary>Runs the RabbitMqSubscriberService operation.</summary>
    protected RabbitMqSubscriberService(
        IOptions<RabbitMqAsyncResponseOptions> options,
        ILogger logger)
        : this(options, logger, new RabbitMqConnectionFactoryAdapter(options.Value))
    {
    }

    /// <summary>Runs the RabbitMqSubscriberService operation.</summary>
    protected RabbitMqSubscriberService(
        IOptions<RabbitMqAsyncResponseOptions> options,
        ILogger logger,
        IRabbitMqConnectionFactory connectionFactory)
    {
        Options = options.Value;
        Logger = logger;
        _connectionFactory = connectionFactory;
    }

    protected RabbitMqAsyncResponseOptions Options { get; }
    protected ILogger Logger { get; }

    protected abstract string QueueName { get; }
    protected abstract RabbitMqSubscriberOptions SubscriberOptions { get; }
    protected abstract RabbitMqSubscriberRole SubscriberRole { get; }

    /// <summary>
    /// The host lifetime whose stop closes this subscriber's intake — the worker subscriber's only;
    /// <c>null</c> for the response subscriber, which keeps serving waiters through the stop.
    /// </summary>
    protected virtual IHostApplicationLifetime? IntakeLifetime => null;
    /// <summary>Ensures the required resource exists.</summary>
    protected abstract Task EnsureTopologyAsync(IRabbitMqChannel channel, CancellationToken cancellationToken);
    /// <summary>Handles the delivered message.</summary>
    protected abstract Task HandleMessageAsync(RabbitMqDelivery delivery, CancellationToken cancellationToken);

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
        RabbitMqMessageDispatcher.ValidateOptions(Options, SubscriberOptions, SubscriberRole);
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queue = QueueName;

        // basic.nack requeue does not increment the x-death header, so the resolved attempt for a plain
        // requeued delivery never exceeds 2. Warn once at startup instead of silently never enforcing the cap.
        if (SubscriberOptions.AckMode is RabbitMqAckMode.AckAfterHandlerCompletes
            && SubscriberOptions.MaxDeliveryAttempts > 2)
        {
            SafeLog.Try(() => Logger.LogWarning(
                "RabbitMQ {OptionName} is {MaxDeliveryAttempts} for queue {Queue} ({Role}), but attempts beyond 2 cannot be counted: "
                + "basic.nack requeue does not increment x-death, so the cap only takes effect once a TTL-retry dead-letter cycle "
                + "re-delivers the message through a dead-letter exchange. Until then the effective cap is 2 — a failing message is "
                + "rejected on its second delivery rather than requeued without limit.",
                nameof(RabbitMqSubscriberOptions.MaxDeliveryAttempts),
                SubscriberOptions.MaxDeliveryAttempts,
                queue,
                SubscriberRole));
        }

        // The dispatcher — and with it the ACK-after-enqueue queue and its workers — belongs to the
        // hosted service, not to one supervised attempt (sibling parity: NATS, SQS, the database
        // transports). Disposing it IS the stop-time drain, so owning it per attempt ran that drain
        // on every channel fault of a host that was NOT stopping: a broker restart, a network blip
        // or a channel-level protocol close paused consumption for the whole drain budget and then
        // dead-lettered already-ACKed work as "drain budget lapsed" — or lost it, because the
        // dead-letter copy rode the channel that had just died. Each attempt attaches its channel
        // instead (AttachChannel), so background dead-letter copies ride the live one, and only the
        // host stop drains.
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            HandleMessageAsync,
            Options,
            SubscriberOptions,
            Logger,
            queue,
            SubscriberRole,
            IntakeLifetime);

        await SubscriberSupervisor.RunAsync(
            ct => RunSubscriberAsync(dispatcher, queue, ct),
            stoppingToken,
            // Covers failed startup and a mid-run consumer/channel termination alike. Jittered
            // backoff, not NetworkRecoveryInterval (which paces the CLIENT's automatic recovery
            // of an existing connection): a broker restart drops every consumer on every
            // replica at once, and a flat shared delay reconnects them all on the same tick.
            failures => AsyncResponseRetry.Backoff(failures, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay),
            // Guarded: a throwing logging provider must not end the restart loop it reports on.
            (ex, retryDelay) => SafeLog.Try(() => Logger.LogWarning(
                ex,
                "RabbitMQ subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.",
                queue,
                SubscriberRole,
                retryDelay)),
            healthyRunThreshold: AsyncResponseRetry.MaxAttainableDelay(Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay)).ConfigureAwait(false);
    }

    /// <summary>
    /// Whether this subscriber's channel ever publishes: the early-ACK dead-letter copy (to
    /// <see cref="RabbitMqAsyncResponseOptions.DeadLetterExchange"/>, or parked) and the
    /// ack-after-handler park at the cap (into <see cref="RabbitMqAsyncResponseOptions.ParkQueue"/>
    /// or <see cref="RabbitMqAsyncResponseOptions.DeadLetterQueue"/> — reachable with no
    /// dead-letter exchange configured here at all, when a broker policy supplies it).
    /// </summary>
    internal static bool PublishesFromSubscriberChannel(RabbitMqAsyncResponseOptions options)
        => !string.IsNullOrWhiteSpace(options.DeadLetterExchange)
            || !string.IsNullOrWhiteSpace(options.DeadLetterQueue)
            || !string.IsNullOrWhiteSpace(options.ParkQueue);

    private async Task RunSubscriberAsync(RabbitMqMessageDispatcher dispatcher, string queue, CancellationToken stoppingToken)
    {
        await using var connection = await _connectionFactory.CreateConnectionAsync(stoppingToken).ConfigureAwait(false);
        // Publisher confirmations whenever this channel can publish (dead-letter copy or park):
        // without confirmation tracking an unroutable (mandatory) return raises only an unobserved
        // basic.return — the publish "succeeded", a successful burial (or park, followed by the
        // ACK that ends the delivery) was logged for a message the broker discarded. With confirms
        // the publish throws, and the existing catches log the failure honestly (a failed park is
        // requeued). Acks/nacks are unaffected by confirm mode, so channels that never publish pay
        // nothing.
        await using var channel = await connection.CreateChannelAsync(
            publisherConfirmations: PublishesFromSubscriberChannel(Options),
            cancellationToken: stoppingToken).ConfigureAwait(false);
        await EnsureTopologyAsync(channel, stoppingToken).ConfigureAwait(false);
        await channel.BasicQosAsync(SubscriberOptions.PrefetchCount, stoppingToken).ConfigureAwait(false);

        // Bound before the first delivery can arrive, released when this attempt ends: background
        // work that outlives the attempt publishes through the NEXT attempt's channel.
        using var attached = dispatcher.AttachChannel(channel);

        SafeLog.Try(() => Logger.LogInformation(
            "RabbitMQ subscriber started. Queue: {Queue}. Role: {Role}. AckMode: {AckMode}.",
            queue,
            SubscriberRole,
            SubscriberOptions.AckMode));

        var consumer = await channel.BasicConsumeAsync(
            queue,
            delivery => dispatcher.HandleAsync(delivery, channel, stoppingToken),
            stoppingToken).ConfigureAwait(false);

        // Park until host shutdown or consumer termination. The termination task is the only signal
        // that deliveries stopped (broker-side basic.cancel and channel-level closes raise no
        // exception here), so parking on the stopping token alone would keep a dead subscription
        // alive forever. A registration-fed TCS instead of an infinite Task.Delay: a faulted
        // iteration must not leak one timer + token registration per rebuild.
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using (stoppingToken.Register(() => stopped.TrySetResult()))
        {
            var first = await Task.WhenAny(consumer.Terminated, stopped.Task).ConfigureAwait(false);

            // A client-initiated cancel during shutdown also completes Terminated (cancel-ok raises
            // UnregisteredAsync), so termination is a failure only while the host is still running.
            // Throwing hands control to the ExecuteAsync retry loop, which disposes this
            // connection/channel (via await using) and rebuilds both plus the consumer after backoff.
            // The dispatcher is NOT drained on this path: it outlives the attempt, its queued
            // already-ACKed work keeps running, and the next attempt's channel is attached to it.
            if (first == consumer.Terminated && !stoppingToken.IsCancellationRequested)
            {
                var reason = await consumer.Terminated.ConfigureAwait(false);
                throw new InvalidOperationException(
                    $"RabbitMQ consumer for queue '{queue}' ({SubscriberRole}) stopped receiving: {reason}.");
            }
        }

        using var shutdown = new CancellationTokenSource(Options.ShutdownTimeout);
        try
        {
            await channel.BasicCancelAsync(consumer.ConsumerTag, shutdown.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (stoppingToken.IsCancellationRequested)
        {
            // A cancel that fails or outlives ShutdownTimeout must not skip the drain below: thrown
            // out of here, the channel and connection were disposed first and the drain ran later,
            // from the service-level `await using` — dead-letter copies found no channel, and the
            // in-flight wait ran after the close it exists to precede. A consumer still registered
            // meanwhile is harmless: the awaiting dispatcher starts nothing once stoppingToken is
            // cancelled, and the queued one leaves whatever arrives after its drain began un-ACKed
            // until the channel close below requeues it. Guarded, for the same reason: a throwing
            // logging provider must not skip the drain either.
            SafeLog.Try(() => Logger.LogWarning(
                ex,
                "RabbitMQ consumer cancel for queue {Queue} ({Role}) failed while stopping; draining and closing the channel anyway.",
                queue,
                SubscriberRole));
        }

        // Host stop only (the termination path above threw): drain BEFORE closing the channel and
        // connection (Kafka parity: "leaving the await-using scope drains ... before the consumer
        // commits"). ACK-after-enqueue: the background queue, whose dead-letter publishes ride this
        // consumer channel — closing it first made TryDeadLetterAlreadyAckedAsync find the channel
        // closed on every graceful shutdown and each already-ACKed failure during the drain lost
        // its DLX record. ACK-after-handler: the handler still running in a delivery callback, so
        // its ACK lands before the close instead of the close requeueing a finished job for a peer
        // to run again. DisposeAsync is idempotent, so the service-level `await using` unwind is a
        // no-op.
        await dispatcher.DisposeAsync().ConfigureAwait(false);

        // A fresh budget for the closes (ASB/SQS parity: arm the source right before the call it
        // bounds). The drain above can run up to BackgroundDrainTimeout, longer than the 5 s
        // ShutdownTimeout, so the token armed for BasicCancel was already cancelled by the time
        // it reached CloseAsync — which threw, skipped the connection close, and left both to
        // the unbounded await-using unwind on every early-ACK shutdown.
        using var closeBudget = new CancellationTokenSource(Options.ShutdownTimeout);
        await channel.CloseAsync(closeBudget.Token).ConfigureAwait(false);
        await connection.CloseAsync(Options.ShutdownTimeout, closeBudget.Token).ConfigureAwait(false);
    }
}

internal sealed class RabbitMqWorkerSubscriber : RabbitMqSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;
    private readonly IHostApplicationLifetime? _hostLifetime;

    /// <summary>Runs the RabbitMqWorkerSubscriber operation.</summary>
    public RabbitMqWorkerSubscriber(
        IOptions<RabbitMqAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<RabbitMqWorkerSubscriber> logger,
        IHostApplicationLifetime? hostLifetime = null)
        : base(options, logger)
    {
        _ingress = ingress;
        _hostLifetime = hostLifetime;
    }

    internal RabbitMqWorkerSubscriber(
        IOptions<RabbitMqAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<RabbitMqWorkerSubscriber> logger,
        IRabbitMqConnectionFactory connectionFactory,
        IHostApplicationLifetime? hostLifetime = null)
        : base(options, logger, connectionFactory)
    {
        _ingress = ingress;
        _hostLifetime = hostLifetime;
    }

    /// <summary>
    /// Host stop closes the worker intake (<see cref="WorkerIntakeGate"/>): from
    /// <see cref="IHostApplicationLifetime.ApplicationStopping"/> on, the dispatcher starts and
    /// settles no new delivery, so the wake-ups this host's own hand-overs just published go to a
    /// live replica instead of being handed back here.
    /// </summary>
    protected override IHostApplicationLifetime? IntakeLifetime => _hostLifetime;

    /// <summary>
    /// Validates like every subscriber, then warns about the worker configurations in which durable
    /// flows run against RabbitMQ limits the engine cannot see (SQS parity). Unconditionally: every
    /// host registers a durable-flow state store (startup fails without one) and durable-flow jobs
    /// ride this queue whenever a flow runs, which registration cannot tell in advance
    /// (<c>WithDurableFlow</c> is optional on JIT).
    /// </summary>
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        var start = base.StartAsync(cancellationToken);
        if (SubscriberOptions.AckMode == RabbitMqAckMode.AckAfterHandlerCompletes)
            WarnAboutDurableFlowLimits();
        return start;
    }

    private void WarnAboutDurableFlowLimits()
    {
        if (SubscriberOptions.MaxDeliveryAttempts == 1)
        {
            // Every requeue the broker makes on its own comes back `redelivered`, which resolves to
            // attempt 2 — past a cap of 1, so the pre-execution check rejects it unrun. A host-stop
            // hand-back cannot avoid that: it leaves the delivery un-ACKed precisely so the broker
            // redelivers it; a requeue NACK sets `redelivered` too, and an ACK-and-republish is what
            // the flow engine's hand-over already tried — the hand-back is its fallback when that
            // publish failed, or when the wake-up would only come straight back to this stopping host.
            SafeLog.Try(() => Logger.LogWarning(
                "The RabbitMQ worker subscriber for {Queue} has {OptionName} = 1, and durable-flow jobs ride this queue: every delivery the broker requeues on its own — a flow handed back at host stop, a delivery prefetched but not yet started when host stop began, a channel closed under a running handler (a stop that outlives its in-flight wait, a connection loss, consumer_timeout) — comes back redelivered, resolves to attempt 2 and is rejected before its handler runs (dead-lettered, or dropped without a dead-letter exchange). A durable flow's wake-up can be lost that way on a routine deploy. Set it to 2 or more, or 0 for unlimited.",
                Options.WorkerQueue,
                $"{nameof(RabbitMqAsyncResponseOptions.WorkerSubscriber)}.{nameof(RabbitMqSubscriberOptions.MaxDeliveryAttempts)}"));
        }

        if (Options.BrokerConsumerTimeout is { } consumerTimeout
            && SubscriberOptions.PrefetchCount > 0
            && RabbitMqWorkerTransport.ResolveMaxInFlightDuration(Options) is { } ceiling
            && consumerTimeout / SubscriberOptions.PrefetchCount < ceiling)
        {
            // The transport advertises the floor rather than a share that would hop timers every
            // few seconds (or not at all); what the floor gives up is the guarantee that the last
            // buffered delivery stays inside the timeout when most of the buffer is parked flows.
            SafeLog.Try(() => Logger.LogWarning(
                "The RabbitMQ worker subscriber for {Queue} prefetches {PrefetchCount} deliveries, which leaves each {Share} of BrokerConsumerTimeout ({ConsumerTimeout}) — below the {InFlightCeiling} the transport advertises to the durable-flow engine as its in-flight ceiling instead (timers wait in process for at most half of it per delivery). When most prefetched deliveries are parked flows, the last one buffered can outlive the broker's consumer_timeout, which closes the channel and requeues every unacknowledged delivery. Lower {PrefetchOption} to {MaxPrefetch} or less, or raise the broker's consumer_timeout together with BrokerConsumerTimeout.",
                Options.WorkerQueue,
                SubscriberOptions.PrefetchCount,
                consumerTimeout / SubscriberOptions.PrefetchCount,
                consumerTimeout,
                ceiling,
                $"{nameof(RabbitMqAsyncResponseOptions.WorkerSubscriber)}.{nameof(RabbitMqSubscriberOptions.PrefetchCount)}",
                Math.Max(1L, consumerTimeout.Ticks / ceiling.Ticks)));
        }
    }

    protected override string QueueName
        => RabbitMqOptionsValidator.Required(Options.WorkerQueue, nameof(Options.WorkerQueue));

    protected override RabbitMqSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override RabbitMqSubscriberRole SubscriberRole => RabbitMqSubscriberRole.Worker;

    /// <summary>Ensures the required resource exists.</summary>
    protected override Task EnsureTopologyAsync(IRabbitMqChannel channel, CancellationToken cancellationToken)
        => RabbitMqTopology.EnsureWorkerAsync(channel, Options, cancellationToken);

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(RabbitMqDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(Encoding.UTF8.GetString(delivery.Body.Span));
}

internal sealed class RabbitMqResponseIngressSubscriber : RabbitMqSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    /// <summary>Runs the RabbitMqResponseIngressSubscriber operation.</summary>
    public RabbitMqResponseIngressSubscriber(
        IOptions<RabbitMqAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<RabbitMqResponseIngressSubscriber> logger)
        : base(options, logger)
    {
        _ingress = ingress;
    }

    internal RabbitMqResponseIngressSubscriber(
        IOptions<RabbitMqAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<RabbitMqResponseIngressSubscriber> logger,
        IRabbitMqConnectionFactory connectionFactory)
        : base(options, logger, connectionFactory)
    {
        _ingress = ingress;
    }

    protected override string QueueName
        => RabbitMqOptionsValidator.Required(Options.ResponseQueue, nameof(Options.ResponseQueue));

    protected override RabbitMqSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
    protected override RabbitMqSubscriberRole SubscriberRole => RabbitMqSubscriberRole.ResponseIngress;

    /// <summary>Ensures the required resource exists.</summary>
    protected override Task EnsureTopologyAsync(IRabbitMqChannel channel, CancellationToken cancellationToken)
        => RabbitMqTopology.EnsureResponseAsync(channel, Options, cancellationToken);

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(RabbitMqDelivery delivery, CancellationToken cancellationToken)
    {
        var messageJson = Encoding.UTF8.GetString(delivery.Body.Span);
        var correlationId = !_ingress.IsOverInboundBudget(messageJson)
            ? RabbitMqCorrelationIdExtractor.Extract(delivery, messageJson, Options)
            : null;
        return _ingress.HandleResponseMessageAsync(messageJson, correlationId);
    }
}
