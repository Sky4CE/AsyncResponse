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
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        PostgreSqlTransportOptionsValidator.ValidateSubscriber(SubscriberOptions, Role.ToString());

        var failures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunSubscriberAsync(stoppingToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                failures++;
                var delay = AsyncResponseRetry.Backoff(failures, Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay);
                Logger.LogWarning(ex, "PostgreSQL subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.", Queue, Role, delay);
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private async Task RunSubscriberAsync(CancellationToken stoppingToken)
    {
        await _store.EnsureCreatedAsync(stoppingToken).ConfigureAwait(false);
        using var signalCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var listenTask = Task.Run(() => ListenLoopAsync(signalCts.Token), signalCts.Token);

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

        try
        {
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
            try
            {
                await listenTask.WaitAsync(Options.ShutdownTimeout).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is OperationCanceledException or TimeoutException)
            {
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private async Task ListenLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _store.ExecuteListenAsync(() =>
            {
                _signals.Writer.TryWrite(true);
                return Task.CompletedTask;
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "PostgreSQL LISTEN helper for queue {Queue} stopped; polling continues.", Queue);
        }
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private async Task WaitForSignalOrDelayAsync(CancellationToken cancellationToken)
    {
        var delay = Task.Delay(SubscriberOptions.EmptyPollDelay, cancellationToken);
        var signal = _signals.Reader.WaitToReadAsync(cancellationToken).AsTask();
        var completed = await Task.WhenAny(delay, signal).ConfigureAwait(false);
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
