using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.GooglePubSub;

internal abstract class GooglePubSubSubscriberService : BackgroundService
{
    private readonly Func<SubscriptionName, GooglePubSubSubscriberOptions, Task<IGooglePubSubSubscriberClient>> _subscriberFactory;

    /// <summary>Runs the GooglePubSubSubscriberService operation.</summary>
    protected GooglePubSubSubscriberService(
        IOptions<GooglePubSubAsyncResponseOptions> options,
        ILogger logger)
        : this(options, logger, CreateSubscriberAsync)
    {
    }

    /// <summary>Runs the GooglePubSubSubscriberService operation.</summary>
    protected GooglePubSubSubscriberService(
        IOptions<GooglePubSubAsyncResponseOptions> options,
        ILogger logger,
        Func<SubscriptionName, GooglePubSubSubscriberOptions, Task<IGooglePubSubSubscriberClient>> subscriberFactory)
    {
        Options = options.Value;
        Logger = logger;
        _subscriberFactory = subscriberFactory;
    }

    protected GooglePubSubAsyncResponseOptions Options { get; }
    protected ILogger Logger { get; }

    protected abstract string SubscriptionId { get; }
    protected abstract GooglePubSubSubscriberOptions SubscriberOptions { get; }
    protected abstract GooglePubSubSubscriberRole SubscriberRole { get; }
    /// <summary>Handles the delivered message.</summary>
    protected abstract Task HandleMessageAsync(PubsubMessage message, CancellationToken cancellationToken);

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static async Task<IGooglePubSubSubscriberClient> CreateSubscriberAsync(
        SubscriptionName subscriptionName,
        GooglePubSubSubscriberOptions subscriberOptions)
    {
        // EmulatorOrProduction honors PUBSUB_EMULATOR_HOST when present (local dev / tests) and uses
        // real Google Cloud otherwise — no behavior change in production.
        var builder = new SubscriberClientBuilder
        {
            SubscriptionName = subscriptionName,
            EmulatorDetection = EmulatorDetection.EmulatorOrProduction
        };

        // In early-ACK mode, bound the streaming pull to the background queue capacity so the client
        // never holds more un-ACKed messages than the dispatcher can accept. Combined with the
        // dispatcher's write-side backpressure this keeps queue-full NACKs (which burn a configured
        // DeadLetterPolicy's delivery attempts) out of steady-state operation.
        if (subscriberOptions.AckMode is GooglePubSubAckMode.AckAfterEnqueue)
        {
            builder.Settings = new SubscriberClient.Settings
            {
                FlowControlSettings = new Google.Api.Gax.FlowControlSettings(
                    maxOutstandingElementCount: subscriberOptions.BackgroundQueueCapacity,
                    maxOutstandingByteCount: null)
            };
        }

        var subscriber = await builder.BuildAsync().ConfigureAwait(false);
        return new GooglePubSubSubscriberClientAdapter(subscriber);
    }

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
        _ = GooglePubSubOptionsValidator.Required(Options.ProjectId, nameof(Options.ProjectId));
        _ = SubscriptionId; // Resolving the id enforces its Required check at startup too.
        GooglePubSubMessageDispatcher.ValidateOptions(Options, SubscriberOptions, SubscriberRole);
        return base.StartAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var projectId = GooglePubSubOptionsValidator.Required(Options.ProjectId, nameof(Options.ProjectId));
        var subscriptionId = SubscriptionId;
        var subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);

        // The transport intentionally has no MaxDeliveryAttempts and no library-managed dead-letter
        // queue for Pub/Sub: capping redelivery is delegated to the subscription's native
        // DeadLetterPolicy. The client cannot cheaply probe whether one is configured, so tell the
        // operator unconditionally instead of failing silently forever on a poison message.
        Logger.LogWarning(
            "Pub/Sub redelivery is unbounded for subscription {Subscription} ({Role}): the transport enforces no delivery-attempt cap and has no library dead-letter queue. "
            + "Configure a DeadLetterPolicy on the subscription to cap redeliveries of failing messages.",
            subscriptionName.ToString(),
            SubscriberRole);

        return SubscriberSupervisor.RunAsync(
            ct => RunSubscriberAsync(subscriptionName, subscriptionId, ct),
            stoppingToken,
            failures => AsyncResponseRetry.Backoff(
                failures,
                Options.SubscriberRetryBaseDelay,
                Options.SubscriberRetryMaxDelay),
            (ex, retryDelay) => Logger.LogWarning(
                ex,
                "Pub/Sub subscriber failed for subscription {Subscription} ({Role}); retrying in {RetryDelay}.",
                subscriptionName.ToString(),
                SubscriberRole,
                retryDelay));
    }

    private async Task RunSubscriberAsync(
        SubscriptionName subscriptionName,
        string subscriptionId,
        CancellationToken stoppingToken)
    {
        var subscriber = await _subscriberFactory(subscriptionName, SubscriberOptions).ConfigureAwait(false);
        try
        {
            await using var dispatcher = GooglePubSubMessageDispatcher.Create(
                HandleMessageAsync,
                Options,
                SubscriberOptions,
                Logger,
                subscriptionId,
                SubscriberRole);

            Logger.LogInformation(
                "Pub/Sub subscriber started. Subscription: {Subscription}. Role: {Role}. AckMode: {AckMode}.",
                subscriptionName.ToString(),
                SubscriberRole,
                SubscriberOptions.AckMode);

            var runTask = subscriber.StartAsync(dispatcher.HandleAsync);

            try
            {
                await runTask.WaitAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                await subscriber.StopAsync(
                    new SubscriberClient.ShutdownOptions
                    {
                        Timeout = Options.ShutdownTimeout
                    },
                    CancellationToken.None).ConfigureAwait(false);
                await runTask.ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Graceful shutdown: the cancellation branch above already stopped the client.
            throw;
        }
        catch
        {
            // A non-shutdown failure abandons the streaming pull: release the client BEFORE the
            // retry loop builds a replacement, or its gRPC channels, pull connection and
            // ack-extension timers stay alive — one leaked client per rebuild.
            await StopSubscriberQuietlyAsync(subscriber).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Best-effort stop of a failed subscriber client, swallowing stop errors: StopAsync is the
    /// seam's only release primitive, and the caller is already propagating the original failure.
    /// </summary>
    private async Task StopSubscriberQuietlyAsync(IGooglePubSubSubscriberClient subscriber)
    {
        try
        {
            await subscriber.StopAsync(
                new SubscriberClient.ShutdownOptions
                {
                    Timeout = Options.ShutdownTimeout
                },
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "Best-effort stop of a failed Pub/Sub subscriber client did not complete cleanly.");
        }
    }

}

internal sealed class GooglePubSubWorkerSubscriber : GooglePubSubSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    /// <summary>Runs the GooglePubSubWorkerSubscriber operation.</summary>
    public GooglePubSubWorkerSubscriber(
        IOptions<GooglePubSubAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<GooglePubSubWorkerSubscriber> logger)
        : base(options, logger)
    {
        _ingress = ingress;
    }

    internal GooglePubSubWorkerSubscriber(
        IOptions<GooglePubSubAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<GooglePubSubWorkerSubscriber> logger,
        Func<SubscriptionName, GooglePubSubSubscriberOptions, Task<IGooglePubSubSubscriberClient>> subscriberFactory)
        : base(options, logger, subscriberFactory)
    {
        _ingress = ingress;
    }

    protected override string SubscriptionId
        => GooglePubSubOptionsValidator.Required(Options.WorkerSubscriptionId, nameof(Options.WorkerSubscriptionId));

    protected override GooglePubSubSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override GooglePubSubSubscriberRole SubscriberRole => GooglePubSubSubscriberRole.Worker;

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(PubsubMessage message, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(message.Data.ToStringUtf8());
}

internal sealed class GooglePubSubResponseIngressSubscriber : GooglePubSubSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

    /// <summary>Runs the GooglePubSubResponseIngressSubscriber operation.</summary>
    public GooglePubSubResponseIngressSubscriber(
        IOptions<GooglePubSubAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<GooglePubSubResponseIngressSubscriber> logger)
        : base(options, logger)
    {
        _ingress = ingress;
    }

    internal GooglePubSubResponseIngressSubscriber(
        IOptions<GooglePubSubAsyncResponseOptions> options,
        IAsyncResponseIngress ingress,
        ILogger<GooglePubSubResponseIngressSubscriber> logger,
        Func<SubscriptionName, GooglePubSubSubscriberOptions, Task<IGooglePubSubSubscriberClient>> subscriberFactory)
        : base(options, logger, subscriberFactory)
    {
        _ingress = ingress;
    }

    protected override string SubscriptionId
        => GooglePubSubOptionsValidator.Required(Options.ResponseSubscriptionId, nameof(Options.ResponseSubscriptionId));

    protected override GooglePubSubSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
    protected override GooglePubSubSubscriberRole SubscriberRole => GooglePubSubSubscriberRole.ResponseIngress;

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(PubsubMessage message, CancellationToken cancellationToken)
    {
        var messageJson = message.Data.ToStringUtf8();
        var correlationId = !_ingress.IsOverInboundBudget(messageJson)
            ? GooglePubSubCorrelationIdExtractor.Extract(message, messageJson, Options)
            : null;
        return _ingress.HandleResponseMessageAsync(messageJson, correlationId);
    }
}
