using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.GooglePubSub;

internal abstract class GooglePubSubSubscriberService : BackgroundService
{
    private readonly Func<SubscriptionName, Task<IGooglePubSubSubscriberClient>> _subscriberFactory;

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
        Func<SubscriptionName, Task<IGooglePubSubSubscriberClient>> subscriberFactory)
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

    private static async Task<IGooglePubSubSubscriberClient> CreateSubscriberAsync(
        SubscriptionName subscriptionName)
    {
        // EmulatorOrProduction honors PUBSUB_EMULATOR_HOST when present (local dev / tests) and uses
        // real Google Cloud otherwise — no behavior change in production.
        var subscriber = await new SubscriberClientBuilder
        {
            SubscriptionName = subscriptionName,
            EmulatorDetection = EmulatorDetection.EmulatorOrProduction
        }.BuildAsync().ConfigureAwait(false);
        return new GooglePubSubSubscriberClientAdapter(subscriber);
    }

    /// <summary>Runs this background operation until cancellation is requested.</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var projectId = GooglePubSubOptionsValidator.Required(Options.ProjectId, nameof(Options.ProjectId));
        var subscriptionId = SubscriptionId;
        GooglePubSubMessageDispatcher.ValidateOptions(Options, SubscriberOptions, SubscriberRole);
        var subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);

        var failures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunSubscriberAsync(subscriptionName, subscriptionId, stoppingToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                failures++;
                var retryDelay = AsyncResponseRetry.Backoff(
                    failures,
                    Options.SubscriberRetryBaseDelay,
                    Options.SubscriberRetryMaxDelay);
                Logger.LogWarning(
                    ex,
                    "Pub/Sub subscriber failed for subscription {Subscription} ({Role}); retrying in {RetryDelay}.",
                    subscriptionName.ToString(),
                    SubscriberRole,
                    retryDelay);
                await Task.Delay(retryDelay, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private async Task RunSubscriberAsync(
        SubscriptionName subscriptionName,
        string subscriptionId,
        CancellationToken stoppingToken)
    {
        var subscriber = await _subscriberFactory(subscriptionName).ConfigureAwait(false);
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
        Func<SubscriptionName, Task<IGooglePubSubSubscriberClient>> subscriberFactory)
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
        Func<SubscriptionName, Task<IGooglePubSubSubscriberClient>> subscriberFactory)
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
        var correlationId = GooglePubSubCorrelationIdExtractor.Extract(message, messageJson, Options);
        return _ingress.HandleResponseMessageAsync(messageJson, correlationId);
    }
}
