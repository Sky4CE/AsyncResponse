using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Channels;

namespace AsyncResponse.Transports.SqlServer;

/// <summary>
/// Base hosted service that consumes one SQL Server queue and routes rows to AsyncResponse ingress
/// with configured acknowledgement, redelivery, and dead-letter behavior.
/// </summary>
internal abstract class SqlServerSubscriberService : BackgroundService
{
    private readonly SqlServerTransportStore _store;
    private readonly Channel<bool> _signals = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
    {
        SingleReader = true,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.DropWrite
    });

    protected SqlServerSubscriberService(
        IOptions<SqlServerAsyncResponseTransportOptions> options,
        SqlServerTransportStore store,
        ILogger logger)
    {
        Options = options.Value;
        SqlServerTransportOptionsValidator.ValidateCommon(Options);
        _store = store;
        Logger = logger;
    }

    protected SqlServerAsyncResponseTransportOptions Options { get; }
    protected ILogger Logger { get; }

    protected abstract string Queue { get; }
    protected abstract SqlServerSubscriberOptions SubscriberOptions { get; }
    protected abstract SqlServerSubscriberRole Role { get; }
    protected abstract Task HandleMessageAsync(SqlServerTransportDelivery delivery, CancellationToken cancellationToken);

    /// <inheritdoc />
    /// <summary>
    /// Validates subscriber options here rather than at the top of <c>ExecuteAsync</c>: since
    /// Microsoft.Extensions.Hosting.Abstractions 10.0.10, <c>BackgroundService.StartAsync</c> no
    /// longer runs <c>ExecuteAsync</c> inline, so a throw there surfaces only through the host's
    /// background-exception handling — or never, when a fast stop discards the queued work —
    /// instead of failing host startup synchronously.
    /// </summary>
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        SqlServerTransportOptionsValidator.ValidateSubscriber(Options, SubscriberOptions, Role.ToString());
        return base.StartAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => SubscriberSupervisor.RunAsync(
            RunSubscriberAsync,
            stoppingToken,
            failures => AsyncResponseRetry.Backoff(failures, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay),
            (ex, delay) => Logger.LogWarning(ex, "SQL Server subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.", Queue, Role, delay));

    private async Task RunSubscriberAsync(CancellationToken stoppingToken)
    {
        await _store.EnsureCreatedAsync(stoppingToken).ConfigureAwait(false);

        // Same-process wake: publishes to this queue (or a NAK release, queue == null) signal the
        // loop directly since SQL Server has no LISTEN/NOTIFY; cross-process publishes are picked up
        // by the EmptyPollDelay poll below.
        Action<string?> onPublished = queue =>
        {
            if (queue is null || string.Equals(queue, Queue, StringComparison.Ordinal))
                _signals.Writer.TryWrite(true);
        };

        // The subscription happens inside the try so ANY escape — the dispatcher's constructor
        // included — runs the unsubscribing finally: the store is a singleton, so a handler leaked
        // by one failed run survives every retry and every later publish invokes it. A -= that the
        // += never preceded is a harmless no-op.
        try
        {
            _store.MessagePublished += onPublished;

            await using var dispatcher = new SqlServerMessageDispatcher(
                HandleMessageAsync,
                Options,
                SubscriberOptions,
                Logger,
                Role);

            Logger.LogInformation(
                "SQL Server subscriber started. Queue: {Queue}. Role: {Role}. AckMode: {AckMode}.",
                Queue,
                Role,
                SubscriberOptions.AckMode);

            while (!stoppingToken.IsCancellationRequested)
            {
                var claimed = 0;
                await foreach (var delivery in _store.ClaimBatchAsync(Queue, SubscriberOptions.BatchSize, Options.LockTimeout, stoppingToken).ConfigureAwait(false))
                {
                    claimed++;
                    await dispatcher.HandleAsync(delivery, stoppingToken).ConfigureAwait(false);
                }

                if (claimed > 0)
                    continue;

                await WaitForSignalOrDelayAsync(stoppingToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _store.MessagePublished -= onPublished;
        }
    }

    private async Task WaitForSignalOrDelayAsync(CancellationToken cancellationToken)
    {
        // The WhenAny loser is cancelled via the per-iteration linked source (mirroring the
        // channel-side CollectDispatchScopeAsync): an abandoned WaitToReadAsync would otherwise
        // stay parked in the channel's blocked-reader list until the next signal — one per empty
        // poll, accumulating without bound on an idle queue.
        using var iteration = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var delay = Task.Delay(SubscriberOptions.EmptyPollDelay, iteration.Token);
        var signal = _signals.Reader.WaitToReadAsync(iteration.Token).AsTask();
        var completed = await Task.WhenAny(delay, signal).ConfigureAwait(false);
        iteration.Cancel();
        if (completed == signal)
        {
            await signal.ConfigureAwait(false);
            while (_signals.Reader.TryRead(out _))
            {
            }
        }
    }
}

/// <summary>Consumes worker-job rows and executes them through the AsyncResponse ingress.</summary>
internal sealed class SqlServerWorkerSubscriber : SqlServerSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    public SqlServerWorkerSubscriber(
        IOptions<SqlServerAsyncResponseTransportOptions> options,
        SqlServerTransportStore store,
        IAsyncResponseIngress ingress,
        ILogger<SqlServerWorkerSubscriber> logger)
        : base(options, store, logger)
        => _ingress = ingress;

    protected override string Queue => Options.WorkerQueue;
    protected override SqlServerSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override SqlServerSubscriberRole Role => SqlServerSubscriberRole.Worker;

    protected override Task HandleMessageAsync(SqlServerTransportDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(delivery.Payload);
}

/// <summary>Consumes response rows and feeds them into the AsyncResponse ingress.</summary>
internal sealed class SqlServerResponseIngressSubscriber : SqlServerSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    public SqlServerResponseIngressSubscriber(
        IOptions<SqlServerAsyncResponseTransportOptions> options,
        SqlServerTransportStore store,
        IAsyncResponseIngress ingress,
        ILogger<SqlServerResponseIngressSubscriber> logger)
        : base(options, store, logger)
        => _ingress = ingress;

    protected override string Queue => Options.ResponseQueue;
    protected override SqlServerSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
    protected override SqlServerSubscriberRole Role => SqlServerSubscriberRole.ResponseIngress;

    protected override Task HandleMessageAsync(SqlServerTransportDelivery delivery, CancellationToken cancellationToken)
    {
        var correlationId = !_ingress.IsOverInboundBudget(delivery.Payload)
            ? SqlServerCorrelationIdExtractor.Extract(delivery.Headers, delivery.Payload, Options)
            : null;
        return _ingress.HandleResponseMessageAsync(delivery.Payload, correlationId);
    }
}
