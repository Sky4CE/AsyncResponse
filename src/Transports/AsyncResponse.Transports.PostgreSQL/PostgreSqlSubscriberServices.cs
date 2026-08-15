using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Channels;

namespace AsyncResponse.Transports.PostgreSQL;

/// <summary>
/// Base hosted service that consumes one PostgreSQL queue and routes rows to AsyncResponse ingress
/// with configured acknowledgement, redelivery, and dead-letter behavior.
/// </summary>
internal abstract class PostgreSqlSubscriberService : BackgroundService
{
    private readonly PostgreSqlTransportStore _store;
    private readonly Channel<bool> _signals = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
    {
        SingleReader = true,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.DropWrite
    });

    protected PostgreSqlSubscriberService(
        IOptions<PostgreSqlAsyncResponseTransportOptions> options,
        PostgreSqlTransportStore store,
        ILogger logger)
    {
        Options = options.Value;
        PostgreSqlTransportOptionsValidator.ValidateCommon(Options);
        _store = store;
        Logger = logger;
    }

    protected PostgreSqlAsyncResponseTransportOptions Options { get; }
    protected ILogger Logger { get; }

    protected abstract string Queue { get; }
    protected abstract PostgreSqlSubscriberOptions SubscriberOptions { get; }
    protected abstract PostgreSqlSubscriberRole Role { get; }
    protected abstract Task HandleMessageAsync(PostgreSqlTransportDelivery delivery, CancellationToken cancellationToken);

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
        PostgreSqlTransportOptionsValidator.ValidateSubscriber(Options, SubscriberOptions, Role.ToString());
        return base.StartAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => SubscriberSupervisor.RunAsync(
            RunSubscriberAsync,
            stoppingToken,
            failures => AsyncResponseRetry.Backoff(failures, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay),
            (ex, delay) => Logger.LogWarning(ex, "PostgreSQL subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.", Queue, Role, delay));

    private async Task RunSubscriberAsync(CancellationToken stoppingToken)
    {
        await _store.EnsureCreatedAsync(stoppingToken).ConfigureAwait(false);
        using var signalCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        // The LISTEN task starts inside the try so ANY escape — the dispatcher's constructor
        // included — runs the cancelling finally. Disposing signalCts does NOT cancel it, so an
        // escape before the finally would otherwise leave ListenLoopAsync parked in
        // connection.WaitAsync holding a pooled connection, one per retry until pool exhaustion.
        Task? listenTask = null;
        try
        {
            listenTask = Task.Run(() => ListenLoopAsync(signalCts.Token), signalCts.Token);

            await using var dispatcher = new PostgreSqlMessageDispatcher(
                HandleMessageAsync,
                Options,
                SubscriberOptions,
                Logger,
                Role);

            Logger.LogInformation(
                "PostgreSQL subscriber started. Queue: {Queue}. Role: {Role}. AckMode: {AckMode}.",
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
        // Retry with backoff, mirroring the channel-side listener: a transient LISTEN failure
        // (network blip, failover) must not permanently degrade this subscriber from push wake
        // to poll-only latency for the rest of the process's uptime.
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _store.ExecuteListenAsync(() =>
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
            catch (Exception ex)
            {
                failures++;
                var delay = AsyncResponseRetry.Backoff(failures, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay);
                Logger.LogWarning(ex, "PostgreSQL LISTEN helper for queue {Queue} failed; retrying in {RetryDelay} (polling continues meanwhile).", Queue, delay);
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
}

/// <summary>Consumes worker-job rows and executes them through the AsyncResponse ingress.</summary>
internal sealed class PostgreSqlWorkerSubscriber : PostgreSqlSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    public PostgreSqlWorkerSubscriber(
        IOptions<PostgreSqlAsyncResponseTransportOptions> options,
        PostgreSqlTransportStore store,
        IAsyncResponseIngress ingress,
        ILogger<PostgreSqlWorkerSubscriber> logger)
        : base(options, store, logger)
        => _ingress = ingress;

    protected override string Queue => Options.WorkerQueue;
    protected override PostgreSqlSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override PostgreSqlSubscriberRole Role => PostgreSqlSubscriberRole.Worker;

    protected override Task HandleMessageAsync(PostgreSqlTransportDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(delivery.Payload);
}

/// <summary>Consumes response rows and feeds them into the AsyncResponse ingress.</summary>
internal sealed class PostgreSqlResponseIngressSubscriber : PostgreSqlSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    public PostgreSqlResponseIngressSubscriber(
        IOptions<PostgreSqlAsyncResponseTransportOptions> options,
        PostgreSqlTransportStore store,
        IAsyncResponseIngress ingress,
        ILogger<PostgreSqlResponseIngressSubscriber> logger)
        : base(options, store, logger)
        => _ingress = ingress;

    protected override string Queue => Options.ResponseQueue;
    protected override PostgreSqlSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
    protected override PostgreSqlSubscriberRole Role => PostgreSqlSubscriberRole.ResponseIngress;

    protected override Task HandleMessageAsync(PostgreSqlTransportDelivery delivery, CancellationToken cancellationToken)
    {
        var correlationId = PostgreSqlCorrelationIdExtractor.Extract(delivery.Headers, delivery.Payload, Options);
        return _ingress.HandleResponseMessageAsync(delivery.Payload, correlationId);
    }
}
