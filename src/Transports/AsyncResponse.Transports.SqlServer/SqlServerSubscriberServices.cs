using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;
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

    /// <summary>Starts a consumer-side receive span carrying the standard messaging attributes.</summary>
    protected Activity? StartReceiveActivity(string name, SqlServerTransportDelivery delivery)
    {
        delivery.Headers.TryGetValue(Options.CorrelationIdHeader, out var correlationId);
        var activity = AsyncResponseDiagnostics.StartActivity(name, ActivityKind.Consumer, correlationId);
        activity?.SetTag("asyncresponse.transport", "sqlserver");
        activity?.SetTag("messaging.system", "sqlserver");
        activity?.SetTag("messaging.destination.name", delivery.Queue);
        activity?.SetTag("messaging.operation.type", "process");
        activity?.SetTag("messaging.message.delivery_attempt", delivery.Attempt);
        return activity;
    }

    /// <inheritdoc />
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        SqlServerTransportOptionsValidator.ValidateSubscriber(SubscriberOptions, Role.ToString());

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
                Logger.LogWarning(ex, "SQL Server subscriber failed for queue {Queue} ({Role}); retrying in {RetryDelay}.", Queue, Role, delay);
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
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
            _store.MessagePublished -= onPublished;
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

    protected override async Task HandleMessageAsync(SqlServerTransportDelivery delivery, CancellationToken cancellationToken)
    {
        using var activity = StartReceiveActivity("asyncresponse.worker.receive", delivery);
        try
        {
            await _ingress.HandleWorkerMessageAsync(delivery.Payload).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }
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

    protected override async Task HandleMessageAsync(SqlServerTransportDelivery delivery, CancellationToken cancellationToken)
    {
        var correlationId = SqlServerCorrelationIdExtractor.Extract(delivery.Headers, delivery.Payload, Options);
        using var activity = StartReceiveActivity("asyncresponse.response.receive", delivery);
        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);
        try
        {
            await _ingress.HandleResponseMessageAsync(delivery.Payload, correlationId).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }
}
