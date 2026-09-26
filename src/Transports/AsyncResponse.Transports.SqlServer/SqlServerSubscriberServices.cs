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
    private readonly WorkerIntakeGate? _intakeGate;
    private readonly Channel<bool> _signals = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
    {
        SingleReader = true,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.DropWrite
    });

    /// <param name="options">The transport options.</param>
    /// <param name="store">The queue store.</param>
    /// <param name="logger">The subscriber's logger.</param>
    /// <param name="intakeGate">The worker subscriber's host-stop intake gate; <c>null</c> for the
    /// response subscriber, which keeps delivering to waiters through host stop.</param>
    protected SqlServerSubscriberService(
        IOptions<SqlServerAsyncResponseTransportOptions> options,
        SqlServerTransportStore store,
        ILogger logger,
        WorkerIntakeGate? intakeGate = null)
    {
        Options = options.Value;
        SqlServerTransportOptionsValidator.ValidateCommon(Options);
        _store = store;
        Logger = logger;
        _intakeGate = intakeGate;
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

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // ONE dispatcher for the service's lifetime, outliving every supervised attempt below; only
        // the host stopping disposes it. In early-ACK mode it owns the queued and running work
        // whose queue items the ACK already deleted, and its DisposeAsync IS the stop-time drain:
        // wait out BackgroundDrainTimeout, then cancel and dead-letter whatever is still queued.
        // Built inside the attempt, every poll fault — a claim timeout, a deadlock victim, a
        // failover: routine for a loop that polls the database several times a second — ran that
        // drain on a host that was NOT stopping: consumption paused for the whole budget, then
        // healthy already-ACKed work was dead-lettered as "drain budget lapsed" — or, when the
        // dead-letter write needed the same failing database, survived only as an Error log line.
        await using var dispatcher = new SqlServerMessageDispatcher(
            HandleMessageAsync,
            Options,
            SubscriberOptions,
            Logger,
            Role,
            hostStopping: _intakeGate?.HostStopping ?? CancellationToken.None);

        await SubscriberSupervisor.RunAsync(
            attemptToken => RunSubscriberAsync(dispatcher, attemptToken),
            stoppingToken,
            failures => AsyncResponseRetry.Backoff(failures, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay),
            (ex, delay) => Logger.LogWarning(ex, "SQL Server subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.", Queue, Role, delay),
            healthyRunThreshold: AsyncResponseRetry.MaxAttainableDelay(Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay)).ConfigureAwait(false);
    }

    private async Task RunSubscriberAsync(SqlServerMessageDispatcher dispatcher, CancellationToken stoppingToken)
    {
        await _store.EnsureCreatedAsync(stoppingToken).ConfigureAwait(false);

        // Same-process wake: publishes to this queue signal the loop directly since SQL Server has
        // no LISTEN/NOTIFY (a null queue still wakes every subscriber, though the store no longer
        // raises one); cross-process publishes and NAK releases are picked up by the
        // EmptyPollDelay poll below.
        Action<string?> onPublished = queue =>
        {
            if (queue is null || string.Equals(queue, Queue, StringComparison.Ordinal))
                _signals.Writer.TryWrite(true);
        };

        // The subscription happens inside the try so ANY escape — a throwing logger provider
        // included — runs the unsubscribing finally: the store is a singleton, so a handler leaked
        // by one failed run survives every retry and every later publish invokes it. A -= that the
        // += never preceded is a harmless no-op.
        try
        {
            _store.MessagePublished += onPublished;

            Logger.LogInformation(
                "SQL Server subscriber started. Queue: {Queue}. Role: {Role}. AckMode: {AckMode}.",
                Queue,
                Role,
                SubscriberOptions.AckMode);

            while (!stoppingToken.IsCancellationRequested)
            {
                // Worker intake stops at host stop (WorkerIntakeGate): once ApplicationStopping has
                // fired, claim nothing more — the rows stay in the table for a live replica — and
                // wait for this subscriber's own stop. Claiming on through the stop window took the
                // very wake-ups this host's own flow hand-overs had just published (the same-process
                // wake above claims them before any peer has polled) and handed them back: an
                // attempt spent and the row locked until its lease lapsed (or, under early ACK, a
                // settled wake-up turned into a dead-letter copy), where a live peer would have run
                // it at once.
                if (_intakeGate?.IsClosed == true)
                {
                    await ParkAtHostStopAsync(stoppingToken).ConfigureAwait(false);
                    break;
                }

                var claimed = 0;
                await foreach (var delivery in _store.ClaimBatchAsync(Queue, SubscriberOptions.BatchSize, Options.LockTimeout, stoppingToken).ConfigureAwait(false))
                {
                    claimed++;
                    await dispatcher.HandleAsync(delivery, stoppingToken).ConfigureAwait(false);

                    // The batch claims lazily, one row per step, so leaving it here claims nothing more.
                    if (_intakeGate?.IsClosed == true)
                        break;
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

    // The worker loop's host-stop park: no claim until this subscriber's own stop (in-process
    // publish wakes keep signalling into the one-slot, drop-on-full channel, which nothing reads
    // any more).
    private async Task ParkAtHostStopAsync(CancellationToken stoppingToken)
    {
        SafeLog.Try((Logger, Queue), static s => s.Logger.LogDebug(
            "SQL Server worker subscriber for queue {Queue} stopped claiming: the host is stopping, so the rows are left for a live replica.",
            s.Queue));
        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }
}

/// <summary>Consumes worker-job rows and executes them through the AsyncResponse ingress.</summary>
internal sealed class SqlServerWorkerSubscriber : SqlServerSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    /// <param name="options">The transport options.</param>
    /// <param name="store">The queue store.</param>
    /// <param name="ingress">The ingress the worker jobs run through.</param>
    /// <param name="logger">The subscriber's logger.</param>
    /// <param name="hostLifetime">The host lifetime whose <c>ApplicationStopping</c> stops worker
    /// intake (see <see cref="WorkerIntakeGate"/>); without one the gate never closes.</param>
    public SqlServerWorkerSubscriber(
        IOptions<SqlServerAsyncResponseTransportOptions> options,
        SqlServerTransportStore store,
        IAsyncResponseIngress ingress,
        ILogger<SqlServerWorkerSubscriber> logger,
        IHostApplicationLifetime? hostLifetime = null)
        : base(options, store, logger, new WorkerIntakeGate(hostLifetime))
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
