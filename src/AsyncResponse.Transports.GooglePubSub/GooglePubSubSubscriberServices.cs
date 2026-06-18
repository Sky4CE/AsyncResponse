using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace AsyncResponse.Transports.GooglePubSub;

internal abstract class GooglePubSubSubscriberService : BackgroundService
{
    private readonly Func<SubscriptionName, Task<IGooglePubSubSubscriberClient>> _subscriberFactory;

    protected GooglePubSubSubscriberService(
        IOptions<GooglePubSubAsyncResponseOptions> options,
        ILogger logger)
        : this(options, logger, CreateSubscriberAsync)
    {
    }

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

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var projectId = GooglePubSubOptionsValidator.Required(Options.ProjectId, nameof(Options.ProjectId));
        var subscriptionId = SubscriptionId;
        var subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);
        var subscriber = await _subscriberFactory(subscriptionName).ConfigureAwait(false);

        Logger.LogInformation("Pub/Sub subscriber started. Subscription: {Subscription}.", subscriptionName.ToString());

        var runTask = subscriber.StartAsync(async (message, cancellationToken) =>
        {
            using var activity = AsyncResponseDiagnostics.StartActivity(
                "asyncresponse.pubsub.receive",
                ActivityKind.Consumer);
            activity?.SetTag("asyncresponse.transport", "google_pubsub");
            activity?.SetTag("messaging.system", "gcp_pubsub");
            activity?.SetTag("messaging.destination.name", subscriptionId);
            activity?.SetTag("messaging.message.id", message.MessageId);

            if (message.Attributes.TryGetValue(Options.CorrelationIdAttribute, out var correlationId))
                AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

            try
            {
                await HandleMessageAsync(message, cancellationToken).ConfigureAwait(false);
                return SubscriberClient.Reply.Ack;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Pub/Sub message handling failed; NACKing message {MessageId}.", message.MessageId);
                AsyncResponseDiagnostics.SetError(activity, ex);
                return SubscriberClient.Reply.Nack;
            }
        });

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

    protected override Task HandleMessageAsync(PubsubMessage message, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(message.Data.ToStringUtf8());
}

internal sealed class GooglePubSubResponseIngressSubscriber : GooglePubSubSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;

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

    protected override Task HandleMessageAsync(PubsubMessage message, CancellationToken cancellationToken)
    {
        var messageJson = message.Data.ToStringUtf8();
        var correlationId = GooglePubSubCorrelationIdExtractor.Extract(message, messageJson, Options);
        return _ingress.HandleResponseMessageAsync(messageJson, correlationId);
    }
}
