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
    private readonly Channel<bool> _signals = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
    {
        SingleReader = true,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.DropWrite
    });

    protected MongoDbSubscriberService(
        IOptions<MongoDbAsyncResponseTransportOptions> options,
        MongoDbTransportStore store,
        ILogger logger)
    {
        Options = options.Value;
        MongoDbTransportOptionsValidator.ValidateCommon(Options);
        _store = store;
        Logger = logger;
    }

    protected MongoDbAsyncResponseTransportOptions Options { get; }
    protected ILogger Logger { get; }

    protected abstract string Queue { get; }
    protected abstract MongoDbSubscriberOptions SubscriberOptions { get; }
    protected abstract MongoDbSubscriberRole Role { get; }
    protected abstract Task HandleMessageAsync(MongoDbTransportDelivery delivery, CancellationToken cancellationToken);

    /// <inheritdoc />
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        MongoDbTransportOptionsValidator.ValidateSubscriber(SubscriberOptions, Role.ToString());

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
                Logger.LogWarning(ex, "MongoDB subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.", Queue, Role, delay);
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private async Task RunSubscriberAsync(CancellationToken stoppingToken)
    {
        await _store.EnsureCreatedAsync(stoppingToken).ConfigureAwait(false);
        using var signalCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var listenTask = Options.UseChangeStreamWake
            ? Task.Run(() => ListenLoopAsync(signalCts.Token), signalCts.Token)
            : Task.CompletedTask;

        await using var dispatcher = new MongoDbMessageDispatcher(
            HandleMessageAsync,
            Options,
            SubscriberOptions,
            Logger,
            Role);

        Logger.LogInformation(
            "MongoDB subscriber started. Queue: {Queue}. Role: {Role}. AckMode: {AckMode}.",
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
            await _store.WatchQueueAsync(Queue, () =>
            {
                _signals.Writer.TryWrite(true);
                return Task.CompletedTask;
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex) when (MongoDbTransportStore.IsChangeStreamUnsupported(ex))
        {
            Logger.LogInformation(
                "MongoDB change streams are unavailable for queue {Queue} (the server is not a replica set); polling continues at {PollDelay}.",
                Queue,
                SubscriberOptions.EmptyPollDelay);
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "MongoDB change-stream wake for queue {Queue} stopped; polling continues.", Queue);
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

/// <summary>Consumes worker-job documents and executes them through the AsyncResponse ingress.</summary>
internal sealed class MongoDbWorkerSubscriber : MongoDbSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    public MongoDbWorkerSubscriber(
        IOptions<MongoDbAsyncResponseTransportOptions> options,
        MongoDbTransportStore store,
        IAsyncResponseIngress ingress,
        ILogger<MongoDbWorkerSubscriber> logger)
        : base(options, store, logger)
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
        var correlationId = MongoDbCorrelationIdExtractor.Extract(delivery.Headers, delivery.Payload, Options);
        return _ingress.HandleResponseMessageAsync(delivery.Payload, correlationId);
    }
}
