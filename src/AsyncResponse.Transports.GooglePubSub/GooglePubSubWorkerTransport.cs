using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Text.Json;

namespace AsyncResponse.Transports.GooglePubSub;

/// <summary>
/// Publishes <see cref="WorkerJobEnvelope"/> messages to a Google Pub/Sub topic.
/// </summary>
public sealed class GooglePubSubWorkerTransport : IWorkerTransport, IAsyncDisposable
{
    private readonly GooglePubSubAsyncResponseOptions _options;
    private readonly Lazy<Task<IGooglePubSubPublisherClient>> _publisher;

    public GooglePubSubWorkerTransport(IOptions<GooglePubSubAsyncResponseOptions> options)
        : this(options, () => CreatePublisherAsync(options.Value))
    {
    }

    internal GooglePubSubWorkerTransport(
        IOptions<GooglePubSubAsyncResponseOptions> options,
        Func<Task<IGooglePubSubPublisherClient>> publisherFactory)
    {
        _options = options.Value;
        _ = GooglePubSubOptionsValidator.Required(_options.ProjectId, nameof(_options.ProjectId));
        _ = GooglePubSubOptionsValidator.Required(_options.WorkerTopicId, nameof(_options.WorkerTopicId));
        _publisher = new Lazy<Task<IGooglePubSubPublisherClient>>(publisherFactory);
    }

    private static async Task<IGooglePubSubPublisherClient> CreatePublisherAsync(
        GooglePubSubAsyncResponseOptions options)
    {
        var projectId = GooglePubSubOptionsValidator.Required(options.ProjectId, nameof(options.ProjectId));
        var topicId = GooglePubSubOptionsValidator.Required(options.WorkerTopicId, nameof(options.WorkerTopicId));
        var topicName = TopicName.FromProjectTopic(projectId, topicId);
        // EmulatorOrProduction honors PUBSUB_EMULATOR_HOST when present (local dev / tests) and uses
        // real Google Cloud otherwise — no behavior change in production.
        var publisher = await new PublisherClientBuilder
        {
            TopicName = topicName,
            EmulatorDetection = EmulatorDetection.EmulatorOrProduction
        }.BuildAsync().ConfigureAwait(false);
        return new GooglePubSubPublisherClientAdapter(publisher);
    }

    public async Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);

        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.worker.publish",
            ActivityKind.Producer,
            job.CorrelationId);
        activity?.SetTag("asyncresponse.transport", "google_pubsub");
        activity?.SetTag("messaging.system", "gcp_pubsub");
        activity?.SetTag("messaging.destination.name", _options.WorkerTopicId);
        AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
        AsyncResponseDiagnostics.SetWorker(activity, job.Call);

        try
        {
            var message = new PubsubMessage
            {
                Data = ByteString.CopyFromUtf8(JsonSerializer.Serialize(job))
            };

            if (!string.IsNullOrWhiteSpace(job.CorrelationId))
                message.Attributes[_options.CorrelationIdAttribute] = job.CorrelationId;

            var publisher = await _publisher.Value.ConfigureAwait(false);
            var messageId = await publisher.PublishAsync(message).WaitAsync(cancellationToken).ConfigureAwait(false);
            activity?.SetTag("messaging.message.id", messageId);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_publisher.IsValueCreated)
            return;

        var publisher = await _publisher.Value.ConfigureAwait(false);
        await publisher.ShutdownAsync(_options.ShutdownTimeout).ConfigureAwait(false);
    }
}
