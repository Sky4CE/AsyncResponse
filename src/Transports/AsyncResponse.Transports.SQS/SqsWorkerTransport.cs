using Amazon.SQS;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Net;
using System.Text.Json;

namespace AsyncResponse.Transports.SQS;

/// <summary>Publishes <see cref="WorkerJobEnvelope"/> messages to an AWS SQS queue.</summary>
/// <remarks>
/// The worker queue URL is resolved lazily (a queue configured by name goes through
/// <c>GetQueueUrl</c> once) and cached for the lifetime of the transport; a transient resolution
/// failure on the first publish is not cached, so the next publish retries. When the worker queue
/// is a FIFO queue (name or URL ending in <c>.fifo</c>), the correlation id becomes the
/// <c>MessageGroupId</c> so one flow's jobs stay ordered, and every message carries a unique
/// <c>MessageDeduplicationId</c> so distinct jobs of the same flow are never deduplicated away.
/// </remarks>
public sealed class SqsWorkerTransport : IWorkerTransport, IDelayedWorkerTransport, IAsyncDisposable
{
    /// <summary>The SQS per-message <c>DelaySeconds</c> ceiling (15 minutes).</summary>
    internal static readonly TimeSpan SqsMaxDelay = TimeSpan.FromSeconds(900);

    private readonly SqsAsyncResponseOptions _options;
    private readonly ISqsClient _client;
    private readonly bool _disposeClient;
    private readonly bool _isFifoQueue;
    private readonly SemaphoreSlim _queueUrlGate = new(1, 1);
    private string? _queueUrl;
    private int _disposeGate;
    private bool _disposed;

    /// <summary>Creates a worker transport backed by a client built from the configured options.</summary>
    public SqsWorkerTransport(IOptions<SqsAsyncResponseOptions> options)
        : this(options, SqsClientFactory.Create(options.Value), disposeClient: true)
    {
    }

    internal SqsWorkerTransport(
        IOptions<SqsAsyncResponseOptions> options,
        ISqsClient client)
        : this(options, client, disposeClient: false)
    {
    }

    private SqsWorkerTransport(
        IOptions<SqsAsyncResponseOptions> options,
        ISqsClient client,
        bool disposeClient)
    {
        _options = options.Value;
        SqsOptionsValidator.ValidateCommon(_options);
        _client = client;
        _disposeClient = disposeClient;
        _isFifoQueue = SqsQueueAddress.IsFifo(_options.WorkerQueue);
    }

    private async Task<string> GetQueueUrlAsync(CancellationToken cancellationToken)
    {
        var queueUrl = Volatile.Read(ref _queueUrl);
        if (queueUrl is not null)
            return queueUrl;

        await _queueUrlGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_queueUrl is not null)
                return _queueUrl;

            // Assign only after the await succeeds, so a faulted resolution is not cached and the
            // next publish retries instead of reusing a permanently failed lookup.
            var resolved = SqsQueueAddress.IsUrl(_options.WorkerQueue)
                ? _options.WorkerQueue
                : await _client.GetQueueUrlAsync(_options.WorkerQueue, cancellationToken).ConfigureAwait(false);
            _queueUrl = resolved;
            return resolved;
        }
        finally
        {
            _queueUrlGate.Release();
        }
    }

    /// <summary>Publishes the supplied worker job.</summary>
    public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        => PublishCoreAsync(job, delay: null, cancellationToken);

    /// <inheritdoc/>
    /// <remarks>
    /// SQS caps a single hop at 15 minutes (<c>DelaySeconds</c> ≤ 900); longer waits ride the
    /// <see cref="WorkerJobEnvelope.NotBeforeUtc"/> re-publish chain, 15 minutes per hop.
    /// </remarks>
    public TimeSpan MaxPublishDelay => SqsMaxDelay;

    /// <inheritdoc/>
    public Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(delay, MaxPublishDelay);
        if (_isFifoQueue && delay > TimeSpan.Zero)
        {
            // SQS rejects per-message DelaySeconds on FIFO queues (queue-level delay only), and a
            // silently dropped delay would break every due-time above. Fail loudly with the way out.
            throw new InvalidOperationException(
                $"SQS FIFO queues do not support per-message delays, so delayed worker jobs (and suspended durable-flow timers) cannot use the FIFO worker queue '{_options.WorkerQueue}'. " +
                "Use a standard worker queue for delayed delivery, or keep timers in process by leaving the transport without delays.");
        }

        return PublishCoreAsync(job, delay > TimeSpan.Zero ? delay : null, cancellationToken);
    }

    private async Task PublishCoreAsync(WorkerJobEnvelope job, TimeSpan? delay, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(job);

        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.worker.publish",
            ActivityKind.Producer,
            job.CorrelationId);
        activity?.SetTag("asyncresponse.transport", "aws_sqs");
        activity?.SetTag("messaging.system", "aws_sqs");
        activity?.SetTag("messaging.destination.name", _options.WorkerQueue);
        AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
        AsyncResponseDiagnostics.SetWorker(activity, job.Call);

        try
        {
            var messageAttributes = new Dictionary<string, string>(StringComparer.Ordinal);
            if (!string.IsNullOrWhiteSpace(job.CorrelationId))
                messageAttributes[_options.CorrelationIdAttribute] = job.CorrelationId;

            var queueUrl = await GetQueueUrlAsync(cancellationToken).ConfigureAwait(false);
            var message = new SqsOutboundMessage(
                queueUrl,
                AsyncResponseJson.Serialize(job),
                string.IsNullOrWhiteSpace(job.CorrelationId) ? null : job.CorrelationId,
                MessageGroupId: _isFifoQueue
                    ? (string.IsNullOrWhiteSpace(job.CorrelationId) ? _options.FifoMessageGroupIdFallback : job.CorrelationId)
                    : null,
                MessageDeduplicationId: _isFifoQueue ? Guid.NewGuid().ToString("N") : null,
                messageAttributes,
                DelaySeconds: delay is { } pending ? (int)Math.Ceiling(pending.TotalSeconds) : null);
            if (delay is { } delayTag)
                activity?.SetTag("asyncresponse.worker.delay_seconds", delayTag.TotalSeconds);

            var messageId = await SendWithRetryAsync(message, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("messaging.message.id", messageId);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    private async Task<string> SendWithRetryAsync(
        SqsOutboundMessage message,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await _client.SendMessageAsync(message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (IsTransient(ex) && attempt < _options.PublishMaxAttempts && !cancellationToken.IsCancellationRequested)
            {
                var delay = AsyncResponseRetry.Backoff(attempt, _options.PublishRetryBaseDelay, _options.PublishRetryMaxDelay);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Classifies AWS SQS send failures worth retrying at the transport level.</summary>
    internal static bool IsTransient(Exception exception)
        => exception is AmazonSQSException sqsException
            && (sqsException.Retryable is not null
                || sqsException.StatusCode >= HttpStatusCode.InternalServerError
                || string.Equals(sqsException.ErrorCode, "RequestThrottled", StringComparison.Ordinal)
                || string.Equals(sqsException.ErrorCode, "ThrottlingException", StringComparison.Ordinal));

    /// <summary>Releases resources held by this instance.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeGate, 1) != 0)
            return;

        await _queueUrlGate.WaitAsync().ConfigureAwait(false);
        try
        {
            _disposed = true;
            if (_disposeClient)
                await _client.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _queueUrlGate.Release();
            _queueUrlGate.Dispose();
        }
    }
}
