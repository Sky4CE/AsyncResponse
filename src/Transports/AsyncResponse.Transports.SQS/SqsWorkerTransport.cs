using Amazon.SQS;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace AsyncResponse.Transports.SQS;

/// <summary>Publishes <see cref="WorkerJobEnvelope"/> messages to an AWS SQS queue.</summary>
/// <remarks>
/// The worker queue URL is resolved lazily (a queue configured by name goes through
/// <c>GetQueueUrl</c> once) and cached for the lifetime of the transport; a transient resolution
/// failure on the first publish is not cached, so the next publish retries. When the worker queue
/// is a FIFO queue (name or URL ending in <c>.fifo</c>), the correlation id becomes the
/// <c>MessageGroupId</c>, so jobs sharing a correlation id are delivered in order (an id SQS would
/// reject there — longer than 128 characters, or anything outside ASCII letters, digits and
/// punctuation — is replaced by a stable hash of itself), and every message carries a unique
/// <c>MessageDeduplicationId</c> so distinct jobs are never deduplicated away. Jobs without a
/// correlation id — durable-flow start, resume and wake-up jobs among them, unless the flow was
/// started inside a request scope — all share <see cref="SqsAsyncResponseOptions.FifoMessageGroupIdFallback"/>:
/// one group SQS delivers strictly one at a time across every consumer, not one group per flow.
/// </remarks>
public sealed class SqsWorkerTransport : IWorkerTransport, IDelayedWorkerTransport, IWorkerTransportInFlightLimit, IAsyncDisposable
{
    /// <summary>The SQS per-message <c>DelaySeconds</c> ceiling (15 minutes).</summary>
    internal static readonly TimeSpan SqsMaxDelay = TimeSpan.FromSeconds(900);

    /// <summary>
    /// The SQS in-flight ceiling: a message stays invisible for at most 12 hours counted from the
    /// <c>ReceiveMessage</c> that delivered it. Extending the visibility timeout does not reset
    /// that clock, and a <c>ChangeMessageVisibility</c> that would cross it is rejected.
    /// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
    /// </summary>
    internal static readonly TimeSpan SqsMaxInFlightDuration = TimeSpan.FromHours(12);

    /// <summary>The SQS <c>MessageGroupId</c> length limit.</summary>
    private const int MaxMessageGroupIdLength = 128;

    /// <summary>Marks a <c>MessageGroupId</c> derived by hashing a correlation id SQS would reject.</summary>
    private const string HashedMessageGroupIdPrefix = "sha256-";

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
        // The lock-free fast path checks disposal too: dispose leaves the cached URL in place, so
        // every publish after the first one skipped the gated check below — and with a shared,
        // DI-owned SQS client it even went through, after the transport was disposed.
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
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
    /// A FIFO worker queue reports <see cref="TimeSpan.Zero"/>: SQS rejects per-message
    /// <c>DelaySeconds</c> on FIFO queues, and advertising a capability the publish would then
    /// throw on lets a durable flow persist itself as sleeping before the enqueue fails —
    /// stranding the run. Zero routes the engine to its in-process fallback instead.
    /// </remarks>
    public TimeSpan MaxPublishDelay => _isFifoQueue ? TimeSpan.Zero : SqsMaxDelay;

    /// <inheritdoc/>
    /// <remarks>
    /// SQS redelivers a message the moment its visibility lapses, however alive its handler is.
    /// With <see cref="SqsSubscriberOptions.VisibilityRenewalInterval"/> set, the worker
    /// subscriber keeps extending the visibility until the 12-hour SQS maximum, which nothing can
    /// extend past. Without renewal an explicit <see cref="SqsSubscriberOptions.VisibilityTimeout"/>
    /// is itself the ceiling and is reported as such; when that is unset too, the queue's own
    /// visibility timeout governs — a value this transport never reads — so only the 12-hour
    /// upper bound can be reported: set <c>VisibilityTimeout</c> to advertise the real one (the
    /// worker subscriber warns at startup while durable flows run in that configuration).
    /// <c>null</c> in <see cref="SqsAckMode.AckAfterEnqueue"/>, where the message is deleted before
    /// its handler runs and nothing stays in flight at the broker.
    /// </remarks>
    public TimeSpan? MaxInFlightDuration
        => _options.WorkerSubscriber switch
        {
            { AckMode: SqsAckMode.AckAfterEnqueue } => null,
            // The positivity guard covers a publisher-only process, where the subscriber options
            // are never validated because no subscriber starts.
            { VisibilityRenewalInterval: null, VisibilityTimeout: { } visibilityTimeout } when visibilityTimeout > TimeSpan.Zero
                => visibilityTimeout,
            _ => SqsMaxInFlightDuration
        };

    /// <inheritdoc/>
    public Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default)
    {
        if (_isFifoQueue && delay > TimeSpan.Zero)
        {
            // SQS rejects per-message DelaySeconds on FIFO queues (queue-level delay only), and a
            // silently dropped delay would break every due-time above. Fail loudly with the way out.
            throw new InvalidOperationException(
                $"SQS FIFO queues do not support per-message delays, so delayed worker jobs (and suspended durable-flow timers) cannot use the FIFO worker queue '{_options.WorkerQueue}'. " +
                "Use a standard worker queue for delayed delivery, or keep timers in process by leaving the transport without delays.");
        }

        ArgumentOutOfRangeException.ThrowIfGreaterThan(delay, MaxPublishDelay);
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
            if (!string.IsNullOrWhiteSpace(job.CorrelationId) && IsValidAttributeValue(job.CorrelationId))
                messageAttributes[_options.CorrelationIdAttribute] = job.CorrelationId;

            var queueUrl = await GetQueueUrlAsync(cancellationToken).ConfigureAwait(false);
            var message = new SqsOutboundMessage(
                queueUrl,
                AsyncResponseJson.Serialize(job),
                string.IsNullOrWhiteSpace(job.CorrelationId) ? null : job.CorrelationId,
                MessageGroupId: _isFifoQueue
                    ? (string.IsNullOrWhiteSpace(job.CorrelationId) ? _options.FifoMessageGroupIdFallback : ToMessageGroupId(job.CorrelationId))
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

    /// <summary>
    /// Maps a correlation id onto a <c>MessageGroupId</c> SQS accepts. A correlation id is portable
    /// text — spaces, non-ASCII, up to 400 characters — while SQS allows at most 128 characters of
    /// ASCII letters, digits and punctuation there and rejects the whole <c>SendMessage</c>
    /// otherwise, so every FIFO publish for such an id failed. Conforming ids pass through
    /// unchanged (existing groups keep their ordering); the rest become a stable SHA-256 of the id,
    /// so one id still always lands in one group.
    /// </summary>
    internal static string ToMessageGroupId(string correlationId)
        => IsValidMessageGroupId(correlationId)
            ? correlationId
            // Uppercase hex: ToHexStringLower is .NET 9+, and this package still targets net8.0.
            : HashedMessageGroupIdPrefix + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(correlationId)));

    /// <summary>
    /// Whether SQS accepts the value in a <c>String</c> message attribute. SQS restricts message
    /// text to the XML character set (#x9, #xA, #xD, #x20–#xD7FF, #xE000–#xFFFD, #x10000+), and a
    /// portable correlation id — which rules out control characters and unpaired surrogates, but
    /// not U+FFFE/U+FFFF — can still fall outside it; SQS then rejected the whole
    /// <c>SendMessage</c>, so every publish for that id failed. The worker path reads the id from
    /// the body, so such an id simply travels without the (diagnostic) attribute.
    /// </summary>
    internal static bool IsValidAttributeValue(string value)
    {
        foreach (var character in value)
        {
            if (character is '￾' or '￿'
                || (character < ' ' && character is not ('\t' or '\n' or '\r')))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Whether SQS accepts the value as a <c>MessageGroupId</c> (or <c>MessageDeduplicationId</c>).</summary>
    internal static bool IsValidMessageGroupId(string value)
    {
        if (value.Length is 0 or > MaxMessageGroupIdLength)
            return false;

        foreach (var character in value)
        {
            // ASCII letters, digits and punctuation: exactly the printable range minus the space.
            if (character is <= ' ' or > '~')
                return false;
        }

        return true;
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
            // Release, never Dispose: SemaphoreSlim.Dispose does not complete pending WaitAsync
            // waiters, so disposing here would strand publishers parked on the gate forever (and
            // the first woken waiter's finally would throw trying to Release a disposed
            // semaphore, never handing the permit on). Released, each parked waiter wakes in
            // turn and observes _disposed; the gate holds no unmanaged resources, so leaving it
            // undisposed leaks nothing.
            _queueUrlGate.Release();
        }
    }
}
