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
    private readonly Func<CancellationToken, Task<IGooglePubSubPublisherClient>> _publisherFactory;
    private readonly SemaphoreSlim _publisherGate = new(1, 1);
    private IGooglePubSubPublisherClient? _publisher;
    private int _disposeGate;
    private bool _disposed;

    /// <summary>Runs the GooglePubSubWorkerTransport operation.</summary>
    public GooglePubSubWorkerTransport(IOptions<GooglePubSubAsyncResponseOptions> options)
        : this(options, cancellationToken => CreatePublisherAsync(options.Value, cancellationToken))
    {
    }

    internal GooglePubSubWorkerTransport(
        IOptions<GooglePubSubAsyncResponseOptions> options,
        Func<CancellationToken, Task<IGooglePubSubPublisherClient>> publisherFactory)
    {
        _options = options.Value;
        _ = GooglePubSubOptionsValidator.Required(_options.ProjectId, nameof(_options.ProjectId));
        _ = GooglePubSubOptionsValidator.Required(_options.WorkerTopicId, nameof(_options.WorkerTopicId));
        GooglePubSubOptionsValidator.ValidateTimeouts(_options);
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
            // publish retries instead of awaiting a permanently faulted task. The token reaches the
            // build itself (sibling parity): a stalled credential/metadata lookup or gRPC handshake
            // under this gate otherwise ignored the caller's — and the host's stopping — token.
            var created = await _publisherFactory(cancellationToken).ConfigureAwait(false);
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
        GooglePubSubAsyncResponseOptions options,
        CancellationToken cancellationToken)
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
        }.BuildAsync(cancellationToken).ConfigureAwait(false);
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
                Data = ByteString.CopyFromUtf8(AsyncResponseJson.Serialize(job))
            };

            if (!string.IsNullOrWhiteSpace(job.CorrelationId))
                message.Attributes[_options.CorrelationIdAttribute] = job.CorrelationId;

            var publisher = await GetPublisherAsync(cancellationToken).ConfigureAwait(false);
            var messageId = await publisher.PublishAsync(message, cancellationToken).ConfigureAwait(false);
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
            {
                // Best effort, like the RabbitMQ/Azure Service Bus worker transports' closes: the
                // SDK CANCELS the returned task when the timeout expires before the backlog
                // flushes, and this is a container-created singleton — a throw here escapes
                // ServiceProvider.DisposeAsync and aborts the disposal of every service after it.
                try
                {
                    await _publisher.ShutdownAsync(_options.ShutdownTimeout).ConfigureAwait(false);
                }
                catch
                {
                    // Best effort.
                }
            }
        }
        finally
        {
            // Release, never Dispose: SemaphoreSlim.Dispose does not complete pending WaitAsync
            // waiters, so disposing here would strand publishers parked on the gate forever (and
            // the first woken waiter's finally would throw trying to Release a disposed
            // semaphore, never handing the permit on). Released, each parked waiter wakes in
            // turn and observes _disposed; the gate holds no unmanaged resources, so leaving it
            // undisposed leaks nothing.
            _publisherGate.Release();
        }
    }
}
