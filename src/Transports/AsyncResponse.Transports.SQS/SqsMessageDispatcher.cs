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

    /// <summary>The Generic Host's default stop budget, the hand-back bound when HostShutdownTimeout is validated externally.</summary>
    private static readonly TimeSpan DefaultHostShutdownTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The visibility a delivery the flow engine handed back at host stop is shortened to — the
    /// host shutdown budget (<see cref="SqsAsyncResponseOptions.HostShutdownTimeout"/>, or the
    /// host's 30-second default) — or <c>null</c> when its remaining visibility cannot outlast
    /// that bound: at most the configured <see cref="SqsSubscriberOptions.VisibilityTimeout"/>
    /// (each receive and each renewal set it), or the queue's own visibility timeout, which the
    /// transport cannot read (30 seconds unless configured otherwise), when none is configured.
    /// </summary>
    protected TimeSpan? HandedBackVisibility
    {
        get
        {
            var bound = _transportOptions.HostShutdownTimeout ?? DefaultHostShutdownTimeout;
            return _subscriberOptions.VisibilityTimeout is { } visibility && visibility > bound ? bound : null;
        }
    }

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
            AsyncResponseDiagnostics.SetError(activity, ex);
            if (logFailures)
            {
                SafeLog.Try(
                    (Logger, ex, delivery.MessageId),
                    static state => state.Logger.LogError(state.ex, "SQS message handling failed for message {MessageId}.", state.MessageId));
            }

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
            SafeLog.Try(
                (Logger, ex, delivery.MessageId, _queue),
                static state => state.Logger.LogWarning(
                    state.ex,
                    "Failed to change visibility of SQS message {MessageId} on {Queue}; it stays invisible until the visibility timeout expires.",
                    state.MessageId,
                    state._queue));
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
            SafeLog.Try(
                (Logger, callbackException, delivery.MessageId, queue),
                static state => state.Logger.LogError(
                    state.callbackException,
                    "SQS background failure callback failed for already-deleted message {MessageId} on {Queue}.",
                    state.MessageId,
                    state.queue));
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
            {
                // The handler takes no token, so a job in flight at this subscriber's own stop can
                // still reach a timer or lease wait and be handed back after it. That hand-back is
                // bounded like the one below (and the batch loop then leaves the message out of
                // its release): rethrown untouched, it sat out its whole configured visibility.
                if (ex is DurableFlowInterruptedException && HandedBackVisibility is { } stoppedVisibility)
                    await TryChangeVisibilityAsync(delivery, stoppedVisibility).ConfigureAwait(false);
                throw;
            }

            // The flow engine saw the host stop before this subscriber did. Its receive loop no
            // longer takes work (the intake gate, or this outcome, parks it), so the only reason
            // left not to release the message at once is the peers stopping alongside this host:
            // released now, it could bounce through replicas that have not parked yet, each
            // receive a step toward maxReceiveCount. Left untouched, though, it stayed invisible
            // for its whole remaining visibility — the configured VisibilityTimeout, which can be
            // hours. So it is shortened to the host shutdown budget when the visibility could
            // outlast it: by then every peer that began stopping with this host has stopped.
            // Returning (not rethrowing) keeps the live receive loop out of the supervisor's
            // failure path; the outcome tells it to stop receiving.
            if (HandedBackVisibility is { } handedBackVisibility)
                await TryChangeVisibilityAsync(delivery, handedBackVisibility).ConfigureAwait(false);

            SafeLog.Try(
                (Logger, delivery.MessageId),
                static state => state.Logger.LogInformation(
                    "SQS message {MessageId} was interrupted by the host stopping; leaving it for redelivery once its visibility lapses.",
                    state.MessageId));
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
            SafeLog.Try(
                (Logger, ex, delivery.MessageId),
                static state => state.Logger.LogWarning(
                    state.ex,
                    "Failed to delete SQS message {MessageId} after a successful handler; it may be redelivered after its visibility timeout.",
                    state.MessageId));
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
            // Never single-reader: once the drain budget lapses, DisposeAsync reads the queue
            // alongside the workers to surface what is still queued.
            SingleReader = false,
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
            // Do not release visibility to zero here: SQS counts every receive toward the queue's
            // redrive policy, so an instantly re-receivable message that keeps hitting a full queue
            // would cross maxReceiveCount and dead-letter without ever being processed. Let the
            // visibility timeout lapse naturally (or shorten it via RedeliveryDelay when configured)
            // so redelivery lands after capacity has had time to free.
            if (RedeliveryDelay is { } redeliveryDelay)
                await TryChangeVisibilityAsync(delivery, redeliveryDelay).ConfigureAwait(false);
            SafeLog.Try(
                (Logger, delivery.MessageId, _queueName, PendingCount, RunningCount),
                static state => state.Logger.LogWarning(
                    "SQS background queue rejected message {MessageId} for {Queue}; leaving it to redeliver via its visibility timeout. Pending={PendingCount}, Running={RunningCount}.",
                    state.MessageId,
                    state._queueName,
                    state.PendingCount,
                    state.RunningCount));
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
            SafeLog.Try(
                (Logger, ex, delivery.MessageId, _queueName),
                static state => state.Logger.LogError(
                    state.ex,
                    "Failed to delete SQS message {MessageId} for {Queue} after enqueue; it is being processed but SQS will redeliver it after the visibility timeout expires.",
                    state.MessageId,
                    state._queueName));
        }

        return SqsDispatchOutcome.Processed;
    }

    /// <summary>Releases resources held by this instance.</summary>
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;

        _queue.Writer.TryComplete();
        SafeLog.Try(
            (Logger, _queueName, PendingCount, RunningCount),
            static state => state.Logger.LogInformation(
                "Draining SQS ACK-after-enqueue dispatcher for {Queue}. Pending={PendingCount}, Running={RunningCount}.",
                state._queueName,
                state.PendingCount,
                state.RunningCount));

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
            SafeLog.Try(
                (Logger, ex, _queueName, PendingCount, RunningCount),
                static state => state.Logger.LogWarning(
                    state.ex,
                    "Timed out while draining SQS ACK-after-enqueue dispatcher for {Queue}. Pending={PendingCount}, Running={RunningCount}. Already-deleted work may be interrupted by host shutdown.",
                    state._queueName,
                    state.PendingCount,
                    state.RunningCount));

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
            _drainCancellation.Dispose();
            SafeLog.Try(
                (Logger, ex, _queueName),
                static state => state.Logger.LogDebug(state.ex, "SQS ACK-after-enqueue dispatcher drain for {Queue} ended with an error.", state._queueName));
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

        SafeLog.Try(
            (Logger, _queueName, surfaced, lost, reserve),
            static state => state.Logger.LogError(
                "The SQS ACK-after-enqueue drain budget for {Queue} lapsed with {Count} already-deleted message(s) never handled — SQS cannot redeliver them. Surfaced {Surfaced} via OnBackgroundFailure within the reserved {Reserve}; {Lost} could not be surfaced before shutdown.",
                state._queueName,
                state.surfaced + state.lost,
                state.surfaced,
                state.reserve,
                state.lost));
    }

    private ValueTask SurfaceLapsedAsync(SqsTransportDelivery delivery)
    {
        var lapsed = new OperationCanceledException(
            "The ACK-after-enqueue drain budget lapsed before this already-deleted message was handled.");
        SafeLog.Try(
            (Logger, delivery.MessageId, _queueName),
            static state => state.Logger.LogWarning(
                "SQS background handler for already-deleted message {MessageId} on {Queue} was not started: the drain budget had lapsed. Surfacing via OnBackgroundFailure.",
                state.MessageId,
                state._queueName));
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
                SafeLog.Try(
                    (Logger, workerIndex, delivery.MessageId, _queueName, PendingCount, RunningCount),
                    static state => state.Logger.LogDebug(
                        "SQS background worker {WorkerIndex} handling message {MessageId} for {Queue}. Pending={PendingCount}, Running={RunningCount}.",
                        state.workerIndex,
                        state.MessageId,
                        state._queueName,
                        state.PendingCount,
                        state.RunningCount));
                await ExecuteHandlerAsync(
                    delivery,
                    _drainCancellation.Token,
                    logFailures: false).ConfigureAwait(false);
            }
            catch (DurableFlowInterruptedException ex)
            {
                // The flow engine handed the job back because the host is stopping (Redis/NATS
                // parity): not a handler failure — but the message was deleted at enqueue, SQS
                // cannot redeliver it, and no dead-letter copy can be written for a deleted
                // message, so the wake-up is lost unless the report records it: an Error, as on
                // every sibling transport that cannot write a copy.
                SafeLog.Try(
                    (Logger, delivery.MessageId, _queueName),
                    static state => state.Logger.LogError(
                        "SQS background handler for already-deleted message {MessageId} on {Queue} was handed back by the flow engine because the host is stopping; SQS will not redeliver it, and no dead-letter copy can be written for a deleted message, so the wake-up is lost unless OnBackgroundFailure records it (resume the flow explicitly).",
                        state.MessageId,
                        state._queueName));
                await NotifyBackgroundFailureAsync(
                    delivery,
                    ex,
                    _queueName,
                    _role).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // A throwing logger must not end this loop: the workers are the only thing that
                // runs the already-deleted jobs queued behind this one.
                SafeLog.Try(
                    (Logger, ex, delivery.MessageId, _queueName),
                    static state => state.Logger.LogError(
                        state.ex,
                        "SQS background handler failed for already-deleted message {MessageId} on {Queue}.",
                        state.MessageId,
                        state._queueName));
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
