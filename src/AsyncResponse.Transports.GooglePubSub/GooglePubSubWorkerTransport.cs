using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace AsyncResponse.Transports.GooglePubSub;

/// <summary>
/// Publishes <see cref="WorkerJobEnvelope"/> messages to a Google Pub/Sub topic.
/// </summary>
public sealed class GooglePubSubWorkerTransport : IWorkerTransport, IAsyncDisposable
{
    private readonly GooglePubSubAsyncResponseOptions _options;
    private readonly Lazy<Task<PublisherClient>> _publisher;

    public GooglePubSubWorkerTransport(IOptions<GooglePubSubAsyncResponseOptions> options)
    {
        _options = options.Value;
        var projectId = GooglePubSubOptionsValidator.Required(_options.ProjectId, nameof(_options.ProjectId));
        var topicId = GooglePubSubOptionsValidator.Required(_options.WorkerTopicId, nameof(_options.WorkerTopicId));
        var topicName = TopicName.FromProjectTopic(projectId, topicId);
        _publisher = new Lazy<Task<PublisherClient>>(() => PublisherClient.CreateAsync(topicName));
    }

    public async Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);

        var message = new PubsubMessage
        {
            Data = ByteString.CopyFromUtf8(JsonSerializer.Serialize(job))
        };

        if (!string.IsNullOrWhiteSpace(job.CorrelationId))
            message.Attributes[_options.CorrelationIdAttribute] = job.CorrelationId;

        var publisher = await _publisher.Value.ConfigureAwait(false);
        await publisher.PublishAsync(message).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (!_publisher.IsValueCreated)
            return;

        var publisher = await _publisher.Value.ConfigureAwait(false);
        await publisher.ShutdownAsync(_options.ShutdownTimeout).ConfigureAwait(false);
    }
}
