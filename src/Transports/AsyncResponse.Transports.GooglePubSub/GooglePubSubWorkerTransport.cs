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
/// <remarks>
/// The publisher client is created lazily and re-created on demand: a transient build failure when the
/// first job is published does not permanently break the transport (a faulted build attempt is not cached).
/// </remarks>
public sealed class GooglePubSubWorkerTransport : IWorkerTransport, IAsyncDisposable
{
    private readonly GooglePubSubAsyncResponseOptions _options;
    private readonly Func<Task<IGooglePubSubPublisherClient>> _publisherFactory;
    private readonly SemaphoreSlim _publisherGate = new(1, 1);
    private IGooglePubSubPublisherClient? _publisher;
    private int _disposeGate;
    private bool _disposed;

    /// <summary>Runs the GooglePubSubWorkerTransport operation.</summary>
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
        _publisherFactory = publisherFactory;
    }

    private async Task<IGooglePubSubPublisherClient> GetPublisherAsync(CancellationToken cancellationToken)
    {
        var publisher = Volatile.Read(ref _publisher);
        if (publisher is not null)
            return publisher;

        await _publisherGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_publisher is not null)
                return _publisher;

            // Assign only after the await succeeds, so a faulted build attempt is not cached and the next
            // publish retries instead of awaiting a permanently faulted task.
            var created = await _publisherFactory().ConfigureAwait(false);
            _publisher = created;
            return created;
        }
        finally
        {
            _publisherGate.Release();
        }
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
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

    /// <summary>Publishes the supplied message.</summary>
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

            var publisher = await GetPublisherAsync(cancellationToken).ConfigureAwait(false);
            var messageId = await publisher.PublishAsync(message).WaitAsync(cancellationToken).ConfigureAwait(false);
            activity?.SetTag("messaging.message.id", messageId);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>Releases resources held by this instance.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeGate, 1) != 0)
            return;

        await _publisherGate.WaitAsync().ConfigureAwait(false);
        try
        {
            _disposed = true;
            if (_publisher is not null)
                await _publisher.ShutdownAsync(_options.ShutdownTimeout).ConfigureAwait(false);
        }
        finally
        {
            _publisherGate.Release();
            _publisherGate.Dispose();
        }
    }
}
