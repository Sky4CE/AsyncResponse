using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Channels;

namespace AsyncResponse.Transports.SQS;

internal enum SqsSubscriberRole
{
    Worker,
    ResponseIngress
}

internal abstract class SqsMessageDispatcher : IAsyncDisposable
{
    private readonly Func<SqsTransportDelivery, CancellationToken, Task> _handler;
    private readonly SqsAsyncResponseOptions _transportOptions;
    private readonly SqsSubscriberOptions _subscriberOptions;
    private readonly string _queue;
    private readonly SqsSubscriberRole _role;

    protected SqsMessageDispatcher(
        Func<SqsTransportDelivery, CancellationToken, Task> handler,
        SqsAsyncResponseOptions transportOptions,
        SqsSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        SqsSubscriberRole role)
    {
        _handler = handler;
        _transportOptions = transportOptions;
        _subscriberOptions = subscriberOptions;
        Logger = logger;
        _queue = queue;
        _role = role;
    }

    protected ILogger Logger { get; }
    protected TimeSpan? RedeliveryDelay => _subscriberOptions.RedeliveryDelay;

    /// <summary>Creates the dispatcher configured by the subscriber options.</summary>
    public static SqsMessageDispatcher Create(
        Func<SqsTransportDelivery, CancellationToken, Task> handler,
        SqsAsyncResponseOptions transportOptions,
        SqsSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        SqsSubscriberRole role)
    {
        SqsOptionsValidator.ValidateSubscriber(transportOptions, subscriberOptions, role);

        return subscriberOptions.AckMode switch
        {
            SqsAckMode.AckAfterHandlerCompletes => new AwaitingSqsMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                queue,
                role),
            SqsAckMode.AckAfterEnqueue => new QueuedSqsMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                queue,
                role),
            _ => throw new InvalidOperationException(
                $"Unsupported SQS ACK mode '{subscriberOptions.AckMode}'.")
        };
    }

    /// <summary>Validates the supplied subscriber options.</summary>
    public static void ValidateOptions(
        SqsAsyncResponseOptions transportOptions,
        SqsSubscriberOptions subscriberOptions,
        SqsSubscriberRole role)
        => SqsOptionsValidator.ValidateSubscriber(transportOptions, subscriberOptions, role);

    /// <summary>Handles the delivered message.</summary>
    public abstract Task HandleAsync(
        SqsTransportDelivery delivery,
        CancellationToken subscriberCancellationToken);

    /// <summary>Releases resources held by this instance.</summary>
    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;

    protected async Task ExecuteHandlerAsync(
        SqsTransportDelivery delivery,
        CancellationToken cancellationToken,
        bool logFailures = true)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.sqs.receive",
            ActivityKind.Consumer);
        activity?.SetTag("asyncresponse.transport", "aws_sqs");
        activity?.SetTag("asyncresponse.sqs.role", _role.ToString());
        activity?.SetTag("asyncresponse.sqs.ack_mode", _subscriberOptions.AckMode.ToString());
        activity?.SetTag("messaging.system", "aws_sqs");
        activity?.SetTag("messaging.destination.name", _queue);
        activity?.SetTag("messaging.message.id", delivery.MessageId);
        activity?.SetTag("messaging.aws_sqs.receive_count", delivery.ReceiveCount);

        if (TryReadCorrelationId(delivery) is { } correlationId)
            AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        try
        {
            await _handler(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (logFailures)
                Logger.LogError(ex, "SQS message handling failed for message {MessageId}.", delivery.MessageId);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>
    /// Best-effort <c>ChangeMessageVisibility</c>: the receipt handle may already be expired or the
    /// message deleted by a competing consumer, and either way SQS redelivery still owns the retry.
    /// </summary>
    protected async ValueTask TryChangeVisibilityAsync(SqsTransportDelivery delivery, TimeSpan delay)
    {
        try
        {
            await delivery.ChangeVisibilityAsync(delay).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(
                ex,
                "Failed to change visibility of SQS message {MessageId} on {Queue}; it stays invisible until the visibility timeout expires.",
                delivery.MessageId,
                _queue);
        }
    }

    protected async ValueTask NotifyBackgroundFailureAsync(
        SqsTransportDelivery delivery,
        Exception exception,
        string queue,
        SqsSubscriberRole role)
    {
        var callback = _subscriberOptions.OnBackgroundFailure;
        if (callback is null)
            return;

        try
        {
            var context = new SqsBackgroundFailureContext(
                queue,
                role.ToString(),
                delivery.MessageId,
                delivery.ReceiveCount,
                TryReadCorrelationId(delivery),
                exception);
            await callback(context).ConfigureAwait(false);
        }
        catch (Exception callbackException)
        {
            Logger.LogError(
                callbackException,
                "SQS background failure callback failed for already-deleted message {MessageId} on {Queue}.",
                delivery.MessageId,
                queue);
        }
    }

    private string? TryReadCorrelationId(SqsTransportDelivery delivery)
        => !string.IsNullOrWhiteSpace(_transportOptions.CorrelationIdAttribute)
            && delivery.MessageAttributes.TryGetValue(_transportOptions.CorrelationIdAttribute, out var value)
            && !string.IsNullOrWhiteSpace(value)
                ? value
                : null;
}

internal sealed class AwaitingSqsMessageDispatcher(
    Func<SqsTransportDelivery, CancellationToken, Task> handler,
    SqsAsyncResponseOptions transportOptions,
    SqsSubscriberOptions subscriberOptions,
    ILogger logger,
    string queue,
    SqsSubscriberRole role)
    : SqsMessageDispatcher(handler, transportOptions, subscriberOptions, logger, queue, role)
{
    /// <summary>Handles the delivered message.</summary>
    public override async Task HandleAsync(
        SqsTransportDelivery delivery,
        CancellationToken subscriberCancellationToken)
    {
        try
        {
            await ExecuteHandlerAsync(delivery, subscriberCancellationToken).ConfigureAwait(false);
            await delivery.DeleteAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // SQS has no explicit NACK or dead-letter call: leaving the message undeleted lets it
            // reappear when its visibility timeout expires, ApproximateReceiveCount increments, and
            // the queue's redrive policy dead-letters it after maxReceiveCount receives.
            if (RedeliveryDelay is { } redeliveryDelay)
                await TryChangeVisibilityAsync(delivery, redeliveryDelay).ConfigureAwait(false);
        }
    }
}

internal sealed class QueuedSqsMessageDispatcher : SqsMessageDispatcher
{
    private readonly Channel<SqsTransportDelivery> _queue;
    private readonly Task[] _workers;
    private readonly CancellationTokenSource _drainCancellation = new();
    private readonly TimeSpan _drainTimeout;
    private readonly string _queueName;
    private readonly SqsSubscriberRole _role;
    private int _pendingCount;
    private int _runningCount;
    private int _disposeStarted;

    /// <summary>Creates an ACK-after-enqueue dispatcher with a bounded background queue.</summary>
    public QueuedSqsMessageDispatcher(
        Func<SqsTransportDelivery, CancellationToken, Task> handler,
        SqsAsyncResponseOptions transportOptions,
        SqsSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        SqsSubscriberRole role)
        : base(handler, transportOptions, subscriberOptions, logger, queue, role)
    {
        _drainTimeout = subscriberOptions.BackgroundDrainTimeout;
        _queueName = queue;
        _role = role;
        _queue = Channel.CreateBounded<SqsTransportDelivery>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
        {
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = subscriberOptions.BackgroundWorkerCount == 1,
            SingleWriter = false
        });

        _workers = Enumerable.Range(0, subscriberOptions.BackgroundWorkerCount)
            .Select(workerIndex => Task.Run(() => RunWorkerAsync(workerIndex)))
            .ToArray();

        Logger.LogInformation(
            "Created SQS ACK-after-enqueue dispatcher for {Queue} with {WorkerCount} worker(s), queue capacity {QueueCapacity}, drain timeout {DrainTimeout}.",
            _queueName,
            subscriberOptions.BackgroundWorkerCount,
            subscriberOptions.BackgroundQueueCapacity,
            _drainTimeout);
    }

    internal int PendingCount => Volatile.Read(ref _pendingCount);
    internal int RunningCount => Volatile.Read(ref _runningCount);

    /// <summary>Handles the delivered message.</summary>
    public override async Task HandleAsync(
        SqsTransportDelivery delivery,
        CancellationToken subscriberCancellationToken)
    {
        Interlocked.Increment(ref _pendingCount);
        if (!_queue.Writer.TryWrite(delivery))
        {
            Interlocked.Decrement(ref _pendingCount);
            Logger.LogWarning(
                "SQS background queue rejected message {MessageId} for {Queue}; releasing it for immediate redelivery. Pending={PendingCount}, Running={RunningCount}.",
                delivery.MessageId,
                _queueName,
                PendingCount,
                RunningCount);
            // Visibility zero is the SQS equivalent of an abandon: the message becomes receivable
            // again immediately instead of waiting out the full visibility timeout.
            await TryChangeVisibilityAsync(delivery, TimeSpan.Zero).ConfigureAwait(false);
            return;
        }

        // The delivery now belongs to a background worker, which decrements _pendingCount when it
        // dequeues. Do not touch the counter or release visibility here, even if the delete below
        // fails — the message is already executing in-process and releasing it would trigger a
        // duplicate execution via redelivery.
        try
        {
            await delivery.DeleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogError(
                ex,
                "Failed to delete SQS message {MessageId} for {Queue} after enqueue; it is being processed but SQS will redeliver it after the visibility timeout expires.",
                delivery.MessageId,
                _queueName);
        }
    }

    /// <summary>Releases resources held by this instance.</summary>
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;

        Logger.LogInformation(
            "Draining SQS ACK-after-enqueue dispatcher for {Queue}. Pending={PendingCount}, Running={RunningCount}.",
            _queueName,
            PendingCount,
            RunningCount);
        _queue.Writer.TryComplete();

        try
        {
            await Task.WhenAll(_workers).WaitAsync(_drainTimeout).ConfigureAwait(false);
            _drainCancellation.Dispose();
        }
        catch (TimeoutException ex)
        {
            _drainCancellation.Cancel();
            Logger.LogWarning(
                ex,
                "Timed out while draining SQS ACK-after-enqueue dispatcher for {Queue}. Pending={PendingCount}, Running={RunningCount}. Already-deleted work may be interrupted by host shutdown.",
                _queueName,
                PendingCount,
                RunningCount);

            _ = Task.WhenAll(_workers).ContinueWith(
                _ => _drainCancellation.Dispose(),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    private async Task RunWorkerAsync(int workerIndex)
    {
        await foreach (var delivery in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            Interlocked.Decrement(ref _pendingCount);
            Interlocked.Increment(ref _runningCount);

            try
            {
                Logger.LogDebug(
                    "SQS background worker {WorkerIndex} handling message {MessageId} for {Queue}. Pending={PendingCount}, Running={RunningCount}.",
                    workerIndex,
                    delivery.MessageId,
                    _queueName,
                    PendingCount,
                    RunningCount);
                await ExecuteHandlerAsync(
                    delivery,
                    _drainCancellation.Token,
                    logFailures: false).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.LogError(
                    ex,
                    "SQS background handler failed for already-deleted message {MessageId} on {Queue}.",
                    delivery.MessageId,
                    _queueName);
                await NotifyBackgroundFailureAsync(
                    delivery,
                    ex,
                    _queueName,
                    _role).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _runningCount);
            }
        }
    }
}
