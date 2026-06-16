using AsyncResponse.Transports.GooglePubSub;
using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Microsoft.Extensions.Options;

namespace AsyncResponse.IntegrationTests.App;

/// <summary>
/// Creates the worker/response topics + subscriptions in the Google Pub/Sub <em>emulator</em> before the
/// transport's subscribers start. The emulator boots empty, whereas the library assumes the
/// topics/subscriptions already exist. This runs <b>only</b> when <c>PUBSUB_EMULATOR_HOST</c> is set, so
/// it is a no-op against real Google Cloud (where they are provisioned out of band).
/// </summary>
/// <remarks>
/// Registered before <c>AddAsyncResponse()</c> so it is the first hosted service: hosted services start
/// in registration order, and its <see cref="StartAsync"/> completes (awaiting provisioning) before the
/// transport's subscribers begin attaching to the subscriptions.
/// </remarks>
internal sealed class PubSubEmulatorProvisioner(
    IOptions<GooglePubSubAsyncResponseOptions> options,
    ILogger<PubSubEmulatorProvisioner> logger) : IHostedService
{
    private const int MaxAttempts = 30;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")))
            return; // real Google Cloud: topics/subscriptions are provisioned out of band.

        var o = options.Value;
        var projectId = o.ProjectId!;

        var publisher = await new PublisherServiceApiClientBuilder
        {
            EmulatorDetection = EmulatorDetection.EmulatorOnly
        }.BuildAsync(cancellationToken).ConfigureAwait(false);
        var subscriber = await new SubscriberServiceApiClientBuilder
        {
            EmulatorDetection = EmulatorDetection.EmulatorOnly
        }.BuildAsync(cancellationToken).ConfigureAwait(false);

        await CreateTopicAsync(publisher, projectId, o.WorkerTopicId!, cancellationToken).ConfigureAwait(false);
        await CreateTopicAsync(publisher, projectId, o.ResponseTopicId!, cancellationToken).ConfigureAwait(false);
        await CreateSubscriptionAsync(subscriber, projectId, o.WorkerSubscriptionId!, o.WorkerTopicId!, cancellationToken).ConfigureAwait(false);
        await CreateSubscriptionAsync(subscriber, projectId, o.ResponseSubscriptionId!, o.ResponseTopicId!, cancellationToken).ConfigureAwait(false);

        logger.LogInformation(
            "Pub/Sub emulator provisioned: topics [{WorkerTopic}, {ResponseTopic}], subscriptions [{WorkerSub}, {ResponseSub}].",
            o.WorkerTopicId, o.ResponseTopicId, o.WorkerSubscriptionId, o.ResponseSubscriptionId);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    // The emulator may not be serving the instant its container reports "running" — retry transient
    // RPC failures (Unavailable) with a short backoff; treat AlreadyExists as success (idempotent).
    private static async Task CreateTopicAsync(
        PublisherServiceApiClient publisher, string projectId, string topicId, CancellationToken ct)
    {
        var name = TopicName.FromProjectTopic(projectId, topicId);
        for (var attempt = 1; ; attempt++)
        {
            try { await publisher.CreateTopicAsync(name, ct).ConfigureAwait(false); return; }
            catch (RpcException e) when (e.StatusCode == StatusCode.AlreadyExists) { return; }
            catch (RpcException) when (attempt < MaxAttempts) { await Task.Delay(TimeSpan.FromMilliseconds(500), ct).ConfigureAwait(false); }
        }
    }

    private static async Task CreateSubscriptionAsync(
        SubscriberServiceApiClient subscriber, string projectId, string subscriptionId, string topicId, CancellationToken ct)
    {
        var subscription = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);
        var topic = TopicName.FromProjectTopic(projectId, topicId);
        for (var attempt = 1; ; attempt++)
        {
            try { await subscriber.CreateSubscriptionAsync(subscription, topic, pushConfig: null, ackDeadlineSeconds: 60, ct).ConfigureAwait(false); return; }
            catch (RpcException e) when (e.StatusCode == StatusCode.AlreadyExists) { return; }
            catch (RpcException) when (attempt < MaxAttempts) { await Task.Delay(TimeSpan.FromMilliseconds(500), ct).ConfigureAwait(false); }
        }
    }
}
