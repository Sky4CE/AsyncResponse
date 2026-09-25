using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Channels;

namespace AsyncResponse.Transports.SQS;

internal enum SqsSubscriberRole
{
    Worker,
    ResponseIngress
}

internal enum SqsDispatchOutcome
{
    /// <summary>The delivery was settled, or accepted into the early-ACK queue.</summary>
    Processed,

    /// <summary>
    /// The flow engine handed the delivery back because the host is stopping
    /// (<see cref="DurableFlowInterruptedException"/>) while this subscriber's token was still
    /// live: its visibility was left untouched — and the receive loop must stop taking work.
    /// </summary>
    HandedBack
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

        return subscriberOptions.AckMode == SqsAckMode.AckAfterHandlerCompletes
            ? new AwaitingSqsMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                queue,
                role)
            : new QueuedSqsMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                queue,
                role);
    }

    /// <summary>Validates the supplied subscriber options.</summary>
    public static void ValidateOptions(
        SqsAsyncResponseOptions transportOptions,
        SqsSubscriberOptions subscriberOptions,
        SqsSubscriberRole role)
        => SqsOptionsValidator.ValidateSubscriber(transportOptions, subscriberOptions, role);

    /// <summary>Handles the delivered message.</summary>
    public abstract Task<SqsDispatchOutcome> HandleAsync(
        SqsTransportDelivery delivery,
        CancellationToken subscriberCancellationToken);

    /// <summary>
    /// Whether the dispatcher can accept more deliveries right now. Awaiting dispatchers always can
    /// (handlers run inline); the queued dispatcher returns <c>false</c> while its bounded queue is
    /// saturated so the receive loop stops pulling messages instead of receiving and releasing them —
    /// SQS counts every receive toward the queue's redrive policy.
    /// </summary>
    public virtual bool CanAcceptMore => true;

    /// <summary>
    /// Number of deliveries the dispatcher can accept right now. The receive loop requests at most
    /// this many messages per receive in early-ACK mode so a burst never overflows the background queue.
    /// </summary>
    public virtual int FreeCapacity => int.MaxValue;

    /// <summary>
    /// Waits until the dispatcher can accept at least one more delivery. Completes immediately for
    /// awaiting dispatchers; the queued dispatcher waits for a background worker to free a slot.
    /// </summary>
    public virtual ValueTask WaitForCapacityAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

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
        catch (Exception ex) when (ex is DurableFlowInterruptedException || (logFailures && IsHostStop(ex, cancellationToken)))
        {
            // The inline (ACK-after-handler) path hands the delivery back on a host stop, and the
            // flow engine's hand-back is not a failure on the early-ACK path either (its caller
            // warns and surfaces it): nothing failed, so no error log and no error span on every
            // rolling deploy.
            throw;
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
    /// Whether <paramref name="exception"/> is the host stopping rather than a handler failure: a
    /// cancellation once the subscriber's own token has fired, or the durable-flow engine's
    /// <see cref="DurableFlowInterruptedException"/>. The engine throws the latter on
    /// <c>ApplicationStopping</c>, which fires BEFORE any hosted service stops — so the worker
    /// subscriber's token is usually still live when it arrives, and keying on the token alone
    /// sent the flow's wake-up down the failure path (RedeliveryDelay, then the redrive policy's
    /// maxReceiveCount toward the dead-letter queue).
    /// </summary>
    protected static bool IsHostStop(Exception exception, CancellationToken subscriberCancellationToken)
        => exception is OperationCanceledException
            && (subscriberCancellationToken.IsCancellationRequested || exception is DurableFlowInterruptedException);

    /// <summary>
    /// Best-effort <c>ChangeMessageVisibility</c>: the receipt handle may already be expired or the
    /// message deleted by a competing consumer, and either way SQS redelivery still owns the retry.
    /// </summary>
    protected async ValueTask TryChangeVisibilityAsync(SqsTransportDelivery delivery, TimeSpan delay)
    {
        try
        {
            // Settlement deliberately ignores cancellation (as every sibling transport does).
            await delivery.ChangeVisibilityAsync(delay, CancellationToken.None).ConfigureAwait(false);
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
    public override async Task<SqsDispatchOutcome> HandleAsync(
        SqsTransportDelivery delivery,
        CancellationToken subscriberCancellationToken)
    {
        try
        {
            await ExecuteHandlerAsync(delivery, subscriberCancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsHostStop(ex, subscriberCancellationToken))
        {
            // Host shutdown, not a handler failure: shortening visibility would hasten a
            // redelivery of work that never ran as if it had failed. Leave the message
            // untouched — its visibility timeout lapses on its own and at-least-once
            // redelivery applies after restart (parity with the RabbitMQ/Redis/Kafka/DB
            // dispatchers).
            if (subscriberCancellationToken.IsCancellationRequested)
                throw;

            // The flow engine saw the host stop before this subscriber did. Its receive loop is
            // still running, so a released (or shortened) visibility would hand the wake-up
            // straight back to it; untouched, the message reappears once its visibility lapses —
            // by then to a peer or to this host after its restart. Returning (not rethrowing)
            // keeps the live receive loop out of the supervisor's failure path; the outcome tells
            // it to stop receiving.
            Logger.LogInformation(
                "SQS message {MessageId} was interrupted by the host stopping; leaving it for redelivery after its visibility timeout.",
                delivery.MessageId);
            return SqsDispatchOutcome.HandedBack;
        }
        catch (Exception)
        {
            // SQS has no explicit NACK or dead-letter call: leaving the message undeleted lets it
            // reappear when its visibility timeout expires, ApproximateReceiveCount increments, and
            // the queue's redrive policy dead-letters it after maxReceiveCount receives.
            if (RedeliveryDelay is { } redeliveryDelay)
                await TryChangeVisibilityAsync(delivery, redeliveryDelay).ConfigureAwait(false);
            return SqsDispatchOutcome.Processed;
        }

        // The delete sits outside the handler's try/catch: a transient DeleteMessage failure after
        // a successful handler must not be misread as a handler failure — shortening visibility
        // here would hasten a duplicate of work whose side effects already completed and burn
        // receives toward the redrive policy. Swallow and log instead; the message reappears when
        // its visibility timeout expires and at-least-once redelivery applies.
        try
        {
            await delivery.DeleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(
                ex,
                "Failed to delete SQS message {MessageId} after a successful handler; it may be redelivered after its visibility timeout.",
                delivery.MessageId);
        }

        return SqsDispatchOutcome.Processed;
    }
}

internal sealed class QueuedSqsMessageDispatcher : SqsMessageDispatcher
{
    private readonly Channel<SqsTransportDelivery> _queue;
    private readonly Task[] _workers;
    private readonly CancellationTokenSource _drainCancellation = new();
    private readonly TimeSpan _drainTimeout;
    private readonly int _capacity;
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
        _capacity = subscriberOptions.BackgroundQueueCapacity;
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

    public override bool CanAcceptMore => Volatile.Read(ref _pendingCount) < _capacity;

    public override int FreeCapacity => Math.Max(0, _capacity - Volatile.Read(ref _pendingCount));

    public override async ValueTask WaitForCapacityAsync(CancellationToken cancellationToken)
    {
        // WaitToWriteAsync completes when the bounded channel has room (or the channel is completed
        // during dispose, in which case there is nothing left to gate).
        while (!CanAcceptMore)
        {
            if (!await _queue.Writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
                return;
        }
    }

    /// <summary>Handles the delivered message.</summary>
    public override async Task<SqsDispatchOutcome> HandleAsync(
        SqsTransportDelivery delivery,
        CancellationToken subscriberCancellationToken)
    {
        Interlocked.Increment(ref _pendingCount);
        if (!_queue.Writer.TryWrite(delivery))
        {
            Interlocked.Decrement(ref _pendingCount);
            Logger.LogWarning(
                "SQS background queue rejected message {MessageId} for {Queue}; leaving it to redeliver via its visibility timeout. Pending={PendingCount}, Running={RunningCount}.",
                delivery.MessageId,
                _queueName,
                PendingCount,
                RunningCount);
            // Do not release visibility to zero here: SQS counts every receive toward the queue's
            // redrive policy, so an instantly re-receivable message that keeps hitting a full queue
            // would cross maxReceiveCount and dead-letter without ever being processed. Let the
            // visibility timeout lapse naturally (or shorten it via RedeliveryDelay when configured)
            // so redelivery lands after capacity has had time to free.
            if (RedeliveryDelay is { } redeliveryDelay)
                await TryChangeVisibilityAsync(delivery, redeliveryDelay).ConfigureAwait(false);
            return SqsDispatchOutcome.Processed;
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

        return SqsDispatchOutcome.Processed;
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

        // BackgroundDrainTimeout is the whole spend the shutdown-budget validator sums for this
        // dispatcher, so it is split rather than exceeded (database-transport parity): most of it
        // lets queued and running handlers finish, and the last quarter is RESERVED for surfacing
        // whatever is still queued once that lapses.
        var surfacingReserve = TimeSpan.FromTicks(_drainTimeout.Ticks / 4);
        try
        {
            await Task.WhenAll(_workers).WaitAsync(_drainTimeout - surfacingReserve).ConfigureAwait(false);
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

            // The workers surface a lapsed entry only once one of them frees up — and with every
            // worker still inside a handler that ignores the token, none does before this returns,
            // the host finishes stopping and the process exits: the entries still queued vanished
            // with no OnBackgroundFailure call at all. So the dispose surfaces them itself.
            await SurfaceUndrainedAsync(surfacingReserve).ConfigureAwait(false);

            _ = Task.WhenAll(_workers).ContinueWith(
                _ => _drainCancellation.Dispose(),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
        catch (Exception ex)
        {
            // A worker faulted outside its own handler guard (DB/NATS dispatcher parity). WhenAll
            // only completes once every worker has finished, so the source is safe to dispose here
            // — and the fault must not escape DisposeAsync and mask the real shutdown path.
            Logger.LogDebug(ex, "SQS ACK-after-enqueue dispatcher drain for {Queue} ended with an error.", _queueName);
            _drainCancellation.Dispose();
        }
    }

    /// <summary>
    /// Surfaces every entry still queued after the drain budget lapsed through
    /// <c>OnBackgroundFailure</c>, within <paramref name="reserve"/>, and logs the loss at Error
    /// with its count. Runs inline on the stop path, so a callback that is slow asynchronously is
    /// cut off at the reserve; entries left then are counted as lost.
    /// </summary>
    private async Task SurfaceUndrainedAsync(TimeSpan reserve)
    {
        var started = Stopwatch.GetTimestamp();
        var surfaced = 0;
        while (true)
        {
            var remaining = reserve - Stopwatch.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero || !_queue.Reader.TryRead(out var delivery))
                break;

            Interlocked.Decrement(ref _pendingCount);
            surfaced++;
            try
            {
                await SurfaceLapsedAsync(delivery).AsTask().WaitAsync(remaining).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                break;
            }
        }

        var lost = _queue.Reader.Count;
        if (surfaced == 0 && lost == 0)
            return;

        Logger.LogError(
            "The SQS ACK-after-enqueue drain budget for {Queue} lapsed with {Count} already-deleted message(s) never handled — SQS cannot redeliver them. Surfaced {Surfaced} via OnBackgroundFailure within the reserved {Reserve}; {Lost} could not be surfaced before shutdown.",
            _queueName,
            surfaced + lost,
            surfaced,
            reserve,
            lost);
    }

    private ValueTask SurfaceLapsedAsync(SqsTransportDelivery delivery)
    {
        var lapsed = new OperationCanceledException(
            "The ACK-after-enqueue drain budget lapsed before this already-deleted message was handled.");
        Logger.LogWarning(
            "SQS background handler for already-deleted message {MessageId} on {Queue} was not started: the drain budget had lapsed. Surfacing via OnBackgroundFailure.",
            delivery.MessageId,
            _queueName);
        return NotifyBackgroundFailureAsync(delivery, lapsed, _queueName, _role);
    }

    private async Task RunWorkerAsync(int workerIndex)
    {
        await foreach (var delivery in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            Interlocked.Decrement(ref _pendingCount);

            // Once the drain budget has lapsed, STOP executing (DB/Redis/Pub-Sub parity). The
            // token below cannot stop the real handler — it is
            // `_ingress.HandleWorkerMessageAsync(payload)`, whose target takes no
            // CancellationToken — so past the budget the loop kept starting fresh work beyond the
            // host's shutdown budget, and every entry still queued at process exit vanished with
            // no record (deleted at enqueue, so the redrive policy never sees it again). No DLQ
            // write is possible for a deleted message; OnBackgroundFailure is the record.
            if (_drainCancellation.IsCancellationRequested)
            {
                await SurfaceLapsedAsync(delivery).ConfigureAwait(false);
                continue;
            }

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
            catch (DurableFlowInterruptedException ex)
            {
                // The flow engine handed the job back because the host is stopping (Redis/NATS
                // parity): not a handler failure, so no Error — but the message was deleted at
                // enqueue and SQS cannot redeliver it, so surface the hand-back.
                Logger.LogWarning(
                    "SQS background handler for already-deleted message {MessageId} on {Queue} was handed back by the flow engine because the host is stopping; SQS will not redeliver it. Surfacing via OnBackgroundFailure.",
                    delivery.MessageId,
                    _queueName);
                await NotifyBackgroundFailureAsync(
                    delivery,
                    ex,
                    _queueName,
                    _role).ConfigureAwait(false);
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
