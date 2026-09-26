using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Channels;

namespace AsyncResponse.Transports.MongoDB;

/// <summary>
/// Base hosted service that consumes one MongoDB queue and routes documents to AsyncResponse ingress
/// with configured acknowledgement, redelivery, and dead-letter behavior.
/// </summary>
internal abstract class MongoDbSubscriberService : BackgroundService
{
    private readonly MongoDbTransportStore _store;
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
    protected MongoDbSubscriberService(
        IOptions<MongoDbAsyncResponseTransportOptions> options,
        MongoDbTransportStore store,
        ILogger logger,
        WorkerIntakeGate? intakeGate = null)
    {
        Options = options.Value;
        MongoDbTransportOptionsValidator.ValidateCommon(Options);
        _store = store;
        Logger = logger;
        _intakeGate = intakeGate;
    }

    protected MongoDbAsyncResponseTransportOptions Options { get; }
    protected ILogger Logger { get; }

    protected abstract string Queue { get; }
    protected abstract MongoDbSubscriberOptions SubscriberOptions { get; }
    protected abstract MongoDbSubscriberRole Role { get; }
    protected abstract Task HandleMessageAsync(MongoDbTransportDelivery delivery, CancellationToken cancellationToken);

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
        MongoDbTransportOptionsValidator.ValidateSubscriber(Options, SubscriberOptions, Role.ToString());
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
        await using var dispatcher = new MongoDbMessageDispatcher(
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
            (ex, delay) => Logger.LogWarning(ex, "MongoDB subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.", Queue, Role, delay),
            healthyRunThreshold: AsyncResponseRetry.MaxAttainableDelay(Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay)).ConfigureAwait(false);
    }

    private async Task RunSubscriberAsync(MongoDbMessageDispatcher dispatcher, CancellationToken stoppingToken)
    {
        await _store.EnsureCreatedAsync(stoppingToken).ConfigureAwait(false);
        using var signalCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        // The wake task starts inside the try so ANY escape — a throwing logger provider
        // included — runs the cancelling finally. Disposing signalCts does NOT cancel it, so an
        // escape before the finally would otherwise leave ListenLoopAsync parked on the change
        // stream, leaking a cursor per retry.
        Task? listenTask = null;
        try
        {
            listenTask = Options.UseChangeStreamWake
                ? Task.Run(() => ListenLoopAsync(signalCts.Token), signalCts.Token)
                : Task.CompletedTask;

            Logger.LogInformation(
                "MongoDB subscriber started. Queue: {Queue}. Role: {Role}. AckMode: {AckMode}.",
                Queue,
                Role,
                SubscriberOptions.AckMode);

            while (!stoppingToken.IsCancellationRequested)
            {
                // Worker intake stops at host stop (WorkerIntakeGate): once ApplicationStopping has
                // fired, claim nothing more — the documents stay in the collection for a live
                // replica — and wait for this subscriber's own stop. Claiming on through the stop
                // window took the very wake-ups this host's own flow hand-overs had just published
                // and handed them back: an attempt spent and the document locked until its lease
                // lapsed (or, under early ACK, a settled wake-up turned into a dead-letter copy),
                // where a live peer would have run it at once.
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

                    // The batch claims lazily, one document per step, so leaving it here claims
                    // nothing more.
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
            await signalCts.CancelAsync().ConfigureAwait(false);
            if (listenTask is not null)
            {
                try
                {
                    await listenTask.WaitAsync(Options.ShutdownTimeout).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is OperationCanceledException or TimeoutException)
                {
                }
            }
        }
    }

    private async Task ListenLoopAsync(CancellationToken cancellationToken)
    {
        // Retry with backoff, mirroring the channel-side listener: a transient watch failure
        // (network blip, replica-set stepdown) must not permanently degrade this subscriber from
        // push wake to poll-only latency for the rest of the process's uptime.
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _store.WatchQueueAsync(Queue, () =>
                {
                    _signals.Writer.TryWrite(true);
                    return Task.CompletedTask;
                }, cancellationToken).ConfigureAwait(false);
                failures = 0;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (MongoDbTransportStore.IsChangeStreamUnsupported(ex))
            {
                // Structural, not transient: a standalone server never grows change streams, so
                // retrying is pointless — the poll loop is the permanent delivery path here.
                SafeLog.Try(() => Logger.LogInformation(
                    "MongoDB change streams are unavailable for queue {Queue} (the server is not a replica set); polling continues at {PollDelay}.",
                    Queue,
                    SubscriberOptions.EmptyPollDelay));
                return;
            }
            catch (Exception ex)
            {
                failures++;
                var delay = AsyncResponseRetry.Backoff(failures, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay);
                // Guarded: a throwing logging provider ended this loop for good (poll-only wakes
                // for the rest of the process's uptime).
                SafeLog.Try(() => Logger.LogWarning(ex, "MongoDB change-stream wake for queue {Queue} failed; retrying in {RetryDelay} (polling continues meanwhile).", Queue, delay));
                try
                {
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
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

    // The worker loop's host-stop park: no claim until this subscriber's own stop (the
    // change-stream wake keeps signalling into the one-slot, drop-on-full channel, which nothing
    // reads any more).
    private async Task ParkAtHostStopAsync(CancellationToken stoppingToken)
    {
        SafeLog.Try((Logger, Queue), static s => s.Logger.LogDebug(
            "MongoDB worker subscriber for queue {Queue} stopped claiming: the host is stopping, so the documents are left for a live replica.",
            s.Queue));
        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }
}

/// <summary>Consumes worker-job documents and executes them through the AsyncResponse ingress.</summary>
internal sealed class MongoDbWorkerSubscriber : MongoDbSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    /// <param name="options">The transport options.</param>
    /// <param name="store">The queue store.</param>
    /// <param name="ingress">The ingress the worker jobs run through.</param>
    /// <param name="logger">The subscriber's logger.</param>
    /// <param name="hostLifetime">The host lifetime whose <c>ApplicationStopping</c> stops worker
    /// intake (see <see cref="WorkerIntakeGate"/>); without one the gate never closes.</param>
    public MongoDbWorkerSubscriber(
        IOptions<MongoDbAsyncResponseTransportOptions> options,
        MongoDbTransportStore store,
        IAsyncResponseIngress ingress,
        ILogger<MongoDbWorkerSubscriber> logger,
        IHostApplicationLifetime? hostLifetime = null)
        : base(options, store, logger, new WorkerIntakeGate(hostLifetime))
        => _ingress = ingress;

    protected override string Queue => Options.WorkerQueue;
    protected override MongoDbSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override MongoDbSubscriberRole Role => MongoDbSubscriberRole.Worker;

    protected override Task HandleMessageAsync(MongoDbTransportDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(delivery.Payload);
}

/// <summary>Consumes response documents and feeds them into the AsyncResponse ingress.</summary>
internal sealed class MongoDbResponseIngressSubscriber : MongoDbSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    public MongoDbResponseIngressSubscriber(
        IOptions<MongoDbAsyncResponseTransportOptions> options,
        MongoDbTransportStore store,
        IAsyncResponseIngress ingress,
        ILogger<MongoDbResponseIngressSubscriber> logger)
        : base(options, store, logger)
        => _ingress = ingress;

    protected override string Queue => Options.ResponseQueue;
    protected override MongoDbSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
    protected override MongoDbSubscriberRole Role => MongoDbSubscriberRole.ResponseIngress;

    protected override Task HandleMessageAsync(MongoDbTransportDelivery delivery, CancellationToken cancellationToken)
    {
        var correlationId = !_ingress.IsOverInboundBudget(delivery.Payload)
            ? MongoDbCorrelationIdExtractor.Extract(delivery.Headers, delivery.Payload, Options)
            : null;
        return _ingress.HandleResponseMessageAsync(delivery.Payload, correlationId);
    }
}
