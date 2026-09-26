using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Channels;

namespace AsyncResponse.Transports.AzureServiceBus;

internal enum AzureServiceBusSubscriberRole
{
    Worker,
    ResponseIngress
}

internal enum AzureServiceBusDispatchOutcome
{
    /// <summary>The delivery was settled, or accepted into the early-ACK queue.</summary>
    Processed,

    /// <summary>
    /// The flow engine handed the delivery back because the host is stopping
    /// (<see cref="DurableFlowInterruptedException"/>) while this subscriber's token was still
    /// live: it was left locked, unsettled — and the receive loop must stop taking work.
    /// </summary>
    HandedBack
}

internal abstract class AzureServiceBusMessageDispatcher : IAsyncDisposable
{
    private readonly Func<AzureServiceBusTransportDelivery, CancellationToken, Task> _handler;
    private readonly AzureServiceBusAsyncResponseOptions _transportOptions;
    private readonly AzureServiceBusSubscriberOptions _subscriberOptions;
    private readonly string _queue;
    private readonly AzureServiceBusSubscriberRole _role;

    protected AzureServiceBusMessageDispatcher(
        Func<AzureServiceBusTransportDelivery, CancellationToken, Task> handler,
        AzureServiceBusAsyncResponseOptions transportOptions,
        AzureServiceBusSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        AzureServiceBusSubscriberRole role)
    {
        _handler = handler;
        _transportOptions = transportOptions;
        _subscriberOptions = subscriberOptions;
        Logger = logger;
        _queue = queue;
        _role = role;
    }

    protected ILogger Logger { get; }
    protected int MaxDeliveryAttempts => _subscriberOptions.MaxDeliveryAttempts;

    /// <summary>
    /// Service Bus rejects a dead-letter reason or description longer than 4096 characters with
    /// ArgumentOutOfRangeException, thrown client-side before any network call. The surrounding
    /// catch could not tell that apart from a lost lock, so a handler whose exception message ran
    /// long could never be dead-lettered at all: the library's MaxDeliveryAttempts cap went
    /// silently inoperative and the handler re-ran until the ENTITY's own MaxDeliveryCount.
    /// </summary>
    protected const int MaxDeadLetterDescriptionLength = 4096;

    protected static string TruncateDeadLetterDescription(string? description)
        // Surrogate-aware cut: an exception message is arbitrary text, and a fixed-index slice
        // through a non-BMP character left a lone high surrogate that the AMQP encoder replaces
        // with U+FFFD — corrupting the forensic text exactly where it was cut.
        => string.IsNullOrEmpty(description)
            ? string.Empty
            : PortableText.TruncateWellFormed(description, MaxDeadLetterDescriptionLength);

    /// <summary>Creates the dispatcher configured by the subscriber options.</summary>
    public static AzureServiceBusMessageDispatcher Create(
        Func<AzureServiceBusTransportDelivery, CancellationToken, Task> handler,
        AzureServiceBusAsyncResponseOptions transportOptions,
        AzureServiceBusSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        AzureServiceBusSubscriberRole role)
    {
        AzureServiceBusOptionsValidator.ValidateSubscriber(transportOptions, subscriberOptions, role);

        return subscriberOptions.AckMode == AzureServiceBusAckMode.AckAfterHandlerCompletes
            ? new AwaitingAzureServiceBusMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                queue,
                role)
            : new QueuedAzureServiceBusMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                queue,
                role);
    }

    /// <summary>Validates the supplied subscriber options.</summary>
    public static void ValidateOptions(
        AzureServiceBusAsyncResponseOptions transportOptions,
        AzureServiceBusSubscriberOptions subscriberOptions,
        AzureServiceBusSubscriberRole role)
        => AzureServiceBusOptionsValidator.ValidateSubscriber(transportOptions, subscriberOptions, role);

    /// <summary>Handles the delivered message.</summary>
    public abstract Task<AzureServiceBusDispatchOutcome> HandleAsync(
        AzureServiceBusTransportDelivery delivery,
        CancellationToken subscriberCancellationToken);

    /// <summary>
    /// Whether the dispatcher can accept more deliveries right now. Awaiting dispatchers always can
    /// (handlers run inline); the queued dispatcher returns <c>false</c> while its bounded queue is
    /// saturated so the receive loop stops pulling messages instead of receiving and abandoning them —
    /// every abandon burns <c>DeliveryCount</c> toward the entity's MaxDeliveryCount.
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
        AzureServiceBusTransportDelivery delivery,
        CancellationToken cancellationToken,
        bool logFailures = true)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.azure_service_bus.receive",
            ActivityKind.Consumer);
        activity?.SetTag("asyncresponse.transport", "azure_service_bus");
        activity?.SetTag("asyncresponse.azure_service_bus.role", _role.ToString());
        activity?.SetTag("asyncresponse.azure_service_bus.ack_mode", _subscriberOptions.AckMode.ToString());
        activity?.SetTag("messaging.system", "azure_service_bus");
        activity?.SetTag("messaging.destination.name", _queue);
        activity?.SetTag("messaging.message.id", delivery.MessageId);
        activity?.SetTag("messaging.azure_service_bus.sequence_number", delivery.SequenceNumber);

        if (!string.IsNullOrWhiteSpace(delivery.CorrelationId))
            AsyncResponseDiagnostics.SetCorrelationId(activity, delivery.CorrelationId);

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
                    static state => state.Logger.LogError(state.ex, "Azure Service Bus message handling failed for message {MessageId}.", state.MessageId));
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
    /// abandoned the flow's only wake-up, which this still-running receiver then pulled straight
    /// back, until the delivery-count cap dead-lettered it and stranded the run.
    /// </summary>
    protected static bool IsHostStop(Exception exception, CancellationToken subscriberCancellationToken)
        => exception is OperationCanceledException
            && (subscriberCancellationToken.IsCancellationRequested || exception is DurableFlowInterruptedException);

    protected async ValueTask NotifyBackgroundFailureAsync(
        AzureServiceBusTransportDelivery delivery,
        Exception exception,
        string queue,
        AzureServiceBusSubscriberRole role)
    {
        var callback = _subscriberOptions.OnBackgroundFailure;
        if (callback is null)
            return;

        try
        {
            var context = new AzureServiceBusBackgroundFailureContext(
                queue,
                role.ToString(),
                delivery.SequenceNumber,
                delivery.MessageId,
                delivery.CorrelationId ?? TryReadApplicationCorrelationId(delivery),
                exception);
            await callback(context).ConfigureAwait(false);
        }
        catch (Exception callbackException)
        {
            SafeLog.Try(
                (Logger, callbackException, delivery.MessageId, queue),
                static state => state.Logger.LogError(
                    state.callbackException,
                    "Azure Service Bus background failure callback failed for already-completed message {MessageId} on {Queue}.",
                    state.MessageId,
                    state.queue));
        }
    }

    private string? TryReadApplicationCorrelationId(AzureServiceBusTransportDelivery delivery)
    {
        if (!string.IsNullOrWhiteSpace(_transportOptions.CorrelationIdProperty)
            && delivery.ApplicationProperties.TryGetValue(_transportOptions.CorrelationIdProperty, out var value))
        {
            return AzureServiceBusCorrelationIdExtractor.TryConvertProperty(value);
        }

        return null;
    }
}

internal sealed class AwaitingAzureServiceBusMessageDispatcher(
    Func<AzureServiceBusTransportDelivery, CancellationToken, Task> handler,
    AzureServiceBusAsyncResponseOptions transportOptions,
    AzureServiceBusSubscriberOptions subscriberOptions,
    ILogger logger,
    string queue,
    AzureServiceBusSubscriberRole role)
    : AzureServiceBusMessageDispatcher(handler, transportOptions, subscriberOptions, logger, queue, role)
{
    /// <summary>Handles the delivered message.</summary>
    public override async Task<AzureServiceBusDispatchOutcome> HandleAsync(
        AzureServiceBusTransportDelivery delivery,
        CancellationToken subscriberCancellationToken)
    {
        try
        {
            await ExecuteHandlerAsync(delivery, subscriberCancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsHostStop(ex, subscriberCancellationToken))
        {
            // Host shutdown, not a handler failure: abandoning would burn a delivery count on
            // work that never ran, and at the cap the branch below would dead-letter healthy
            // work. Leave the delivery unsettled — the peek lock lapses on its own and
            // at-least-once redelivery applies after restart (parity with the
            // RabbitMQ/Redis/Kafka/DB dispatchers).
            if (subscriberCancellationToken.IsCancellationRequested)
                throw;

            // The flow engine saw the host stop before this subscriber did. Its receive loop no
            // longer takes work (the intake gate, or this outcome, parks it); an abandon now could
            // still bounce the wake-up through replicas stopping alongside this host that have
            // not parked yet, each abandon a DeliveryCount. Left locked, the delivery redelivers
            // once the lock lapses — within the entity's LockDuration (5 minutes at most), by then
            // to a peer or to this host after its restart. Returning (not rethrowing) keeps the
            // live receive loop out of the supervisor's failure path; the outcome tells it to stop
            // receiving.
            SafeLog.Try(
                (Logger, delivery.MessageId),
                static state => state.Logger.LogInformation(
                    "Azure Service Bus message {MessageId} was interrupted by the host stopping; leaving it unsettled for redelivery after its lock lapses.",
                    state.MessageId));
            return AzureServiceBusDispatchOutcome.HandedBack;
        }
        catch (Exception ex)
        {
            // Failure-path settlement is guarded like the Complete below: a slow handler that
            // outlived its peek lock makes DeadLetter/Abandon throw MessageLockLost, and an
            // escaping settlement would tear down the whole receiver — dropping the rest of the
            // already-received batch un-settled. On a lost settle the lock lapses on its own and
            // at-least-once redelivery applies (DeliveryCount still advances broker-side).
            if (MaxDeliveryAttempts > 0 && delivery.DeliveryCount >= MaxDeliveryAttempts)
            {
                try
                {
                    await delivery.DeadLetterAsync(
                        "AsyncResponseHandlerFailed",
                        TruncateDeadLetterDescription(ex.Message)).ConfigureAwait(false);
                }
                catch (Exception settleEx)
                {
                    SafeLog.Try(
                        (Logger, settleEx, delivery.MessageId),
                        static state => state.Logger.LogWarning(
                            state.settleEx,
                            "Failed to dead-letter Azure Service Bus message {MessageId} after a failed handler; the lock will lapse and the message will be redelivered.",
                            state.MessageId));
                }

                return AzureServiceBusDispatchOutcome.Processed;
            }

            try
            {
                await delivery.AbandonAsync().ConfigureAwait(false);
            }
            catch (Exception settleEx)
            {
                SafeLog.Try(
                    (Logger, settleEx, delivery.MessageId),
                    static state => state.Logger.LogWarning(
                        state.settleEx,
                        "Failed to abandon Azure Service Bus message {MessageId} after a failed handler; the lock will lapse and the message will be redelivered.",
                        state.MessageId));
            }

            return AzureServiceBusDispatchOutcome.Processed;
        }

        // The Complete sits outside the handler's try/catch: a transient settlement failure after
        // a successful handler must not be misread as a handler failure — dead-lettering or
        // abandoning here would redeliver (or bury) work whose side effects already completed.
        // Swallow and log instead; the peek-lock lapses on its own and at-least-once redelivery
        // applies.
        try
        {
            await delivery.CompleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SafeLog.Try(
                (Logger, ex, delivery.MessageId),
                static state => state.Logger.LogWarning(
                    state.ex,
                    "Failed to complete Azure Service Bus message {MessageId} after a successful handler; the lock will lapse and the message may be redelivered.",
                    state.MessageId));
        }

        return AzureServiceBusDispatchOutcome.Processed;
    }
}

internal sealed class QueuedAzureServiceBusMessageDispatcher : AzureServiceBusMessageDispatcher
{
    private readonly Channel<AzureServiceBusTransportDelivery> _queue;
    private readonly Task[] _workers;
    private readonly CancellationTokenSource _drainCancellation = new();
    private readonly TimeSpan _drainTimeout;
    private readonly int _capacity;
    private readonly string _queueName;
    private readonly AzureServiceBusSubscriberRole _role;
    private int _pendingCount;
    private int _runningCount;
    private int _disposeStarted;

    /// <summary>Creates an ACK-after-enqueue dispatcher with a bounded background queue.</summary>
    public QueuedAzureServiceBusMessageDispatcher(
        Func<AzureServiceBusTransportDelivery, CancellationToken, Task> handler,
        AzureServiceBusAsyncResponseOptions transportOptions,
        AzureServiceBusSubscriberOptions subscriberOptions,
        ILogger logger,
        string queue,
        AzureServiceBusSubscriberRole role)
        : base(handler, transportOptions, subscriberOptions, logger, queue, role)
    {
        _drainTimeout = subscriberOptions.BackgroundDrainTimeout;
        _capacity = subscriberOptions.BackgroundQueueCapacity;
        _queueName = queue;
        _role = role;
        _queue = Channel.CreateBounded<AzureServiceBusTransportDelivery>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
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
            "Created Azure Service Bus ACK-after-enqueue dispatcher for {Queue} with {WorkerCount} worker(s), queue capacity {QueueCapacity}, drain timeout {DrainTimeout}.",
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
    public override async Task<AzureServiceBusDispatchOutcome> HandleAsync(
        AzureServiceBusTransportDelivery delivery,
        CancellationToken subscriberCancellationToken)
    {
        Interlocked.Increment(ref _pendingCount);
        if (!_queue.Writer.TryWrite(delivery))
        {
            // The receive loop gates on free capacity, so this only covers the residual race between
            // its capacity check and this write. The abandon burns one DeliveryCount, but the loop
            // never receives while saturated, so a healthy message cannot repeat this path toward
            // the entity's MaxDeliveryCount.
            Interlocked.Decrement(ref _pendingCount);
            try
            {
                await delivery.AbandonAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Guarded like every other settlement: an escaping MessageLockLost would tear down
                // the receiver, and the lock lapsing redelivers the message on its own anyway.
                SafeLog.Try(
                    (Logger, ex, delivery.MessageId, _queueName),
                    static state => state.Logger.LogWarning(
                        state.ex,
                        "Azure Service Bus background queue rejected message {MessageId} for {Queue}, and abandoning it failed; the lock will lapse and the message will be redelivered.",
                        state.MessageId,
                        state._queueName));
                return AzureServiceBusDispatchOutcome.Processed;
            }

            SafeLog.Try(
                (Logger, delivery.MessageId, _queueName, PendingCount, RunningCount),
                static state => state.Logger.LogWarning(
                    "Azure Service Bus background queue rejected message {MessageId} for {Queue}; abandoned for redelivery. Pending={PendingCount}, Running={RunningCount}.",
                    state.MessageId,
                    state._queueName,
                    state.PendingCount,
                    state.RunningCount));
            return AzureServiceBusDispatchOutcome.Processed;
        }

        // The delivery now belongs to a background worker, which decrements _pendingCount when it dequeues.
        // Do not touch the counter or abandon here, even if the Complete below fails — the message is already
        // executing in-process and abandoning it would trigger a duplicate execution via redelivery.
        try
        {
            await delivery.CompleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SafeLog.Try(
                (Logger, ex, delivery.MessageId, _queueName),
                static state => state.Logger.LogError(
                    state.ex,
                    "Failed to complete Azure Service Bus message {MessageId} for {Queue} after enqueue; it is being processed but Service Bus will redeliver it after the lock expires.",
                    state.MessageId,
                    state._queueName));
        }

        return AzureServiceBusDispatchOutcome.Processed;
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
                "Draining Azure Service Bus ACK-after-enqueue dispatcher for {Queue}. Pending={PendingCount}, Running={RunningCount}.",
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
                    "Timed out while draining Azure Service Bus ACK-after-enqueue dispatcher for {Queue}. Pending={PendingCount}, Running={RunningCount}. Already-completed work may be interrupted by host shutdown.",
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
                static state => state.Logger.LogDebug(state.ex, "Azure Service Bus ACK-after-enqueue dispatcher drain for {Queue} ended with an error.", state._queueName));
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
                "The Azure Service Bus ACK-after-enqueue drain budget for {Queue} lapsed with {Count} already-completed message(s) never handled — Service Bus cannot redeliver them. Surfaced {Surfaced} via OnBackgroundFailure within the reserved {Reserve}; {Lost} could not be surfaced before shutdown.",
                state._queueName,
                state.surfaced + state.lost,
                state.surfaced,
                state.reserve,
                state.lost));
    }

    private ValueTask SurfaceLapsedAsync(AzureServiceBusTransportDelivery delivery)
    {
        var lapsed = new OperationCanceledException(
            "The ACK-after-enqueue drain budget lapsed before this already-completed message was handled.");
        SafeLog.Try(
            (Logger, delivery.MessageId, _queueName),
            static state => state.Logger.LogWarning(
                "Azure Service Bus background handler for already-completed message {MessageId} on {Queue} was not started: the drain budget had lapsed. Surfacing via OnBackgroundFailure.",
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
            // no record (completed at enqueue, so the broker never redelivers it). The settled
            // lock rules out a DLQ write; OnBackgroundFailure is the record.
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
                        "Azure Service Bus background worker {WorkerIndex} handling message {MessageId} for {Queue}. Pending={PendingCount}, Running={RunningCount}.",
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
                // parity): not a handler failure — but the message was completed at enqueue,
                // Service Bus cannot redeliver it, and a completed message cannot be dead-lettered,
                // so the wake-up is lost unless the report records it: an Error, as on every
                // sibling transport that cannot write a copy.
                SafeLog.Try(
                    (Logger, delivery.MessageId, _queueName),
                    static state => state.Logger.LogError(
                        "Azure Service Bus background handler for already-completed message {MessageId} on {Queue} was handed back by the flow engine because the host is stopping; Service Bus will not redeliver it, and a completed message cannot be dead-lettered, so the wake-up is lost unless OnBackgroundFailure records it (resume the flow explicitly).",
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
                // runs the already-completed jobs queued behind this one.
                SafeLog.Try(
                    (Logger, ex, delivery.MessageId, _queueName),
                    static state => state.Logger.LogError(
                        state.ex,
                        "Azure Service Bus background handler failed for already-completed message {MessageId} on {Queue}.",
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
