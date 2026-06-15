using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AsyncResponse.GooglePubSub;

internal abstract class GooglePubSubSubscriberService(
    IOptions<GooglePubSubAsyncResponseOptions> options,
    ILogger logger) : BackgroundService
{
    protected GooglePubSubAsyncResponseOptions Options { get; } = options.Value;
    protected ILogger Logger { get; } = logger;

    protected abstract string SubscriptionId { get; }
    protected abstract string ServiceName { get; }
    protected abstract Task HandleMessageAsync(PubsubMessage message, CancellationToken cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var projectId = GooglePubSubOptionsValidator.Required(Options.ProjectId, nameof(Options.ProjectId));
        var subscriptionName = SubscriptionName.FromProjectSubscription(projectId, SubscriptionId);
        var subscriber = await SubscriberClient.CreateAsync(subscriptionName).ConfigureAwait(false);

        Logger.LogInformation("{ServiceName}: started. Subscription: {Subscription}.",
            ServiceName, subscriptionName.ToString());

        var runTask = subscriber.StartAsync(async (message, cancellationToken) =>
        {
            try
            {
                await HandleMessageAsync(message, cancellationToken).ConfigureAwait(false);
                return SubscriberClient.Reply.Ack;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "{ServiceName}: message handling failed; NACKing message {MessageId}.",
                    ServiceName, message.MessageId);
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

internal sealed class GooglePubSubWorkerSubscriber(
    IOptions<GooglePubSubAsyncResponseOptions> options,
    IAsyncResponseIngress ingress,
    ILogger<GooglePubSubWorkerSubscriber> logger) : GooglePubSubSubscriberService(options, logger)
{
    protected override string SubscriptionId
        => GooglePubSubOptionsValidator.Required(Options.WorkerSubscriptionId, nameof(Options.WorkerSubscriptionId));

    protected override string ServiceName => nameof(GooglePubSubWorkerSubscriber);

    protected override Task HandleMessageAsync(PubsubMessage message, CancellationToken cancellationToken)
        => ingress.HandleWorkerMessageAsync(message.Data.ToStringUtf8());
}

internal sealed class GooglePubSubResponseIngressSubscriber(
    IOptions<GooglePubSubAsyncResponseOptions> options,
    IAsyncResponseIngress ingress,
    ILogger<GooglePubSubResponseIngressSubscriber> logger) : GooglePubSubSubscriberService(options, logger)
{
    protected override string SubscriptionId
        => GooglePubSubOptionsValidator.Required(Options.ResponseSubscriptionId, nameof(Options.ResponseSubscriptionId));

    protected override string ServiceName => nameof(GooglePubSubResponseIngressSubscriber);

    protected override Task HandleMessageAsync(PubsubMessage message, CancellationToken cancellationToken)
    {
        message.Attributes.TryGetValue(Options.CorrelationIdAttribute, out var correlationId);
        return ingress.HandleResponseMessageAsync(message.Data.ToStringUtf8(), correlationId);
    }
}
