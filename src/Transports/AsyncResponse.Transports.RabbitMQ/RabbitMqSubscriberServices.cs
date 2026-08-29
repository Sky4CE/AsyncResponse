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
    /// <summary>Ensures the required resource exists.</summary>
    protected abstract Task EnsureTopologyAsync(IRabbitMqChannel channel, CancellationToken cancellationToken);
    /// <summary>Handles the delivered message.</summary>
    protected abstract Task HandleMessageAsync(RabbitMqDelivery delivery, CancellationToken cancellationToken);

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
        RabbitMqMessageDispatcher.ValidateOptions(Options, SubscriberOptions, SubscriberRole);
        return base.StartAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queue = QueueName;

        // basic.nack requeue does not increment the x-death header, so the resolved attempt for a plain
        // requeued delivery never exceeds 2. Warn once at startup instead of silently never enforcing the cap.
        if (SubscriberOptions.AckMode is RabbitMqAckMode.AckAfterHandlerCompletes
            && SubscriberOptions.MaxDeliveryAttempts > 2)
        {
            Logger.LogWarning(
                "RabbitMQ {OptionName} is {MaxDeliveryAttempts} for queue {Queue} ({Role}), but attempts beyond 2 cannot be counted: "
                + "basic.nack requeue does not increment x-death, so the cap only takes effect once a TTL-retry dead-letter cycle "
                + "re-delivers the message through a dead-letter exchange. Until then the effective cap is 2 — a failing message is "
                + "rejected on its second delivery rather than requeued without limit.",
                nameof(RabbitMqSubscriberOptions.MaxDeliveryAttempts),
                SubscriberOptions.MaxDeliveryAttempts,
                queue,
                SubscriberRole);
        }

        return SubscriberSupervisor.RunAsync(
            ct => RunSubscriberAsync(queue, ct),
            stoppingToken,
            // Covers failed startup and a mid-run consumer/channel termination alike. Jittered
            // backoff, not NetworkRecoveryInterval (which paces the CLIENT's automatic recovery
            // of an existing connection): a broker restart drops every consumer on every
            // replica at once, and a flat shared delay reconnects them all on the same tick.
            failures => AsyncResponseRetry.Backoff(failures, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay),
            (ex, retryDelay) => Logger.LogWarning(
                ex,
                "RabbitMQ subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.",
                queue,
                SubscriberRole,
                retryDelay));
    }

    private async Task RunSubscriberAsync(string queue, CancellationToken stoppingToken)
    {
        await using var connection = await _connectionFactory.CreateConnectionAsync(stoppingToken).ConfigureAwait(false);
        // Publisher confirmations when a dead-letter exchange is configured: the already-ACKed
        // dead-letter copy is published on THIS channel, and without confirmation tracking an
        // unroutable (mandatory) return raises only an unobserved basic.return — the publish
        // "succeeded" and a successful burial was logged for a message the broker discarded.
        // With confirms the publish throws, and the existing catch logs the failure honestly.
        // Acks/nacks are unaffected by confirm mode, so channels that never publish pay nothing.
        await using var channel = await connection.CreateChannelAsync(
            publisherConfirmations: !string.IsNullOrWhiteSpace(Options.DeadLetterExchange),
            cancellationToken: stoppingToken).ConfigureAwait(false);
        await EnsureTopologyAsync(channel, stoppingToken).ConfigureAwait(false);
        await channel.BasicQosAsync(SubscriberOptions.PrefetchCount, stoppingToken).ConfigureAwait(false);

        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            HandleMessageAsync,
            Options,
            SubscriberOptions,
            Logger,
            queue,
            SubscriberRole);

        Logger.LogInformation(
            "RabbitMQ subscriber started. Queue: {Queue}. Role: {Role}. AckMode: {AckMode}.",
            queue,
            SubscriberRole,
            SubscriberOptions.AckMode);

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
            if (first == consumer.Terminated && !stoppingToken.IsCancellationRequested)
            {
                var reason = await consumer.Terminated.ConfigureAwait(false);
                throw new InvalidOperationException(
                    $"RabbitMQ consumer for queue '{queue}' ({SubscriberRole}) stopped receiving: {reason}.");
            }
        }

        using var shutdown = new CancellationTokenSource(Options.ShutdownTimeout);
        await channel.BasicCancelAsync(consumer.ConsumerTag, shutdown.Token).ConfigureAwait(false);

        // Drain the ACK-after-enqueue background queue BEFORE closing the channel and connection
        // (Kafka parity: "leaving the await-using scope drains ... before the consumer commits").
        // The drain's dead-letter publishes ride this consumer channel, so closing it first made
        // TryDeadLetterAlreadyAckedAsync find the channel closed on every graceful shutdown and
        // each already-ACKed failure during the drain lost its DLX record. DisposeAsync is
        // idempotent, so the `await using` unwind after the closes is a no-op.
        await dispatcher.DisposeAsync().ConfigureAwait(false);

        await channel.CloseAsync(shutdown.Token).ConfigureAwait(false);
        await connection.CloseAsync(Options.ShutdownTimeout, shutdown.Token).ConfigureAwait(false);
    }
}

internal sealed class RabbitMqWorkerSubscriber : RabbitMqSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    /// <summary>Runs the RabbitMqWorkerSubscriber operation.</summary>
    public RabbitMqWorkerSubscriber(
        IOptions<RabbitMqAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<RabbitMqWorkerSubscriber> logger)
        : base(options, logger)
    {
        _ingress = ingress;
    }

    internal RabbitMqWorkerSubscriber(
        IOptions<RabbitMqAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<RabbitMqWorkerSubscriber> logger,
        IRabbitMqConnectionFactory connectionFactory)
        : base(options, logger, connectionFactory)
    {
        _ingress = ingress;
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
