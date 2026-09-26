using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Channels;

namespace AsyncResponse.Transports.GooglePubSub;

internal enum GooglePubSubSubscriberRole
{
    Worker,
    ResponseIngress
}

internal abstract class GooglePubSubMessageDispatcher : IAsyncDisposable
{
    private readonly Func<PubsubMessage, CancellationToken, Task> _handler;
    private readonly GooglePubSubAsyncResponseOptions _transportOptions;
    private readonly GooglePubSubSubscriberOptions _subscriberOptions;
    private readonly string _subscriptionId;
    private readonly GooglePubSubSubscriberRole _role;
    private readonly TaskCompletionSource _clientStopping = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Runs the GooglePubSubMessageDispatcher operation.</summary>
    protected GooglePubSubMessageDispatcher(
        Func<PubsubMessage, CancellationToken, Task> handler,
        GooglePubSubAsyncResponseOptions transportOptions,
        GooglePubSubSubscriberOptions subscriberOptions,
        ILogger logger,
        string subscriptionId,
        GooglePubSubSubscriberRole role)
    {
        _handler = handler;
        _transportOptions = transportOptions;
        _subscriberOptions = subscriberOptions;
        Logger = logger;
        _subscriptionId = subscriptionId;
        _role = role;
    }

    protected ILogger Logger { get; }

    /// <summary>
    /// Creates the configured dispatcher. <paramref name="intakeGate"/> is the worker role's
    /// host-stop gate: from <c>ApplicationStopping</c> on, the early-ACK dispatcher holds every new
    /// delivery for the client stop instead of acknowledging it.
    /// </summary>
    public static GooglePubSubMessageDispatcher Create(
        Func<PubsubMessage, CancellationToken, Task> handler,
        GooglePubSubAsyncResponseOptions transportOptions,
        GooglePubSubSubscriberOptions subscriberOptions,
        ILogger logger,
        string subscriptionId,
        GooglePubSubSubscriberRole role,
        WorkerIntakeGate? intakeGate = null)
    {
        ValidateOptions(transportOptions, subscriberOptions, role);

        return subscriberOptions.AckMode == GooglePubSubAckMode.AckAfterHandlerCompletes
            ? new AwaitingGooglePubSubMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                subscriptionId,
                role)
            : new QueuedGooglePubSubMessageDispatcher(
                handler,
                transportOptions,
                subscriberOptions,
                logger,
                subscriptionId,
                role,
                intakeGate);
    }

    /// <summary>Validates the supplied options.</summary>
    public static void ValidateOptions(
        GooglePubSubAsyncResponseOptions transportOptions,
        GooglePubSubSubscriberOptions subscriberOptions,
        GooglePubSubSubscriberRole role)
    {
        var optionPath = role is GooglePubSubSubscriberRole.Worker
            ? $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.WorkerSubscriber)}"
            : $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.ResponseSubscriber)}";

        GooglePubSubOptionsValidator.ValidateTimeouts(transportOptions);
        GooglePubSubOptionsValidator.ValidateStreamingPull(subscriberOptions, optionPath);

        if (!string.IsNullOrWhiteSpace(transportOptions.WorkerSubscriptionId)
            && !string.IsNullOrWhiteSpace(transportOptions.ResponseSubscriptionId)
            && StringComparer.Ordinal.Equals(transportOptions.WorkerSubscriptionId, transportOptions.ResponseSubscriptionId))
        {
            throw new InvalidOperationException(
                $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.WorkerSubscriptionId)} and " +
                $"{nameof(GooglePubSubAsyncResponseOptions.ResponseSubscriptionId)} must be distinct so worker and response subscribers do not consume each other's messages.");
        }

        if (!string.IsNullOrWhiteSpace(transportOptions.WorkerTopicId)
            && !string.IsNullOrWhiteSpace(transportOptions.ResponseTopicId)
            && StringComparer.Ordinal.Equals(transportOptions.WorkerTopicId, transportOptions.ResponseTopicId))
        {
            throw new InvalidOperationException(
                $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.WorkerTopicId)} and " +
                $"{nameof(GooglePubSubAsyncResponseOptions.ResponseTopicId)} must be distinct so worker jobs and responses do not share one topic.");
        }

        switch (subscriberOptions.AckMode)
        {
            case GooglePubSubAckMode.AckAfterHandlerCompletes:
                // The stop drains the in-flight handlers for at most BackgroundDrainTimeout,
                // clamped (not validated) to what the host budget leaves after the bounded client
                // stop (ShutdownTimeout) — so ShutdownTimeout itself must fit: unvalidated, a
                // raised value passed startup and the host killed the process mid-stop (SQS/Azure
                // Service Bus parity).
                ShutdownBudgetValidator.Validate(
                    "Pub/Sub",
                    $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.HostShutdownTimeout)}",
                    transportOptions.HostShutdownTimeout,
                    ($"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.ShutdownTimeout)}", transportOptions.ShutdownTimeout));
                return;

            case GooglePubSubAckMode.AckAfterEnqueue:
                if (subscriberOptions.BackgroundWorkerCount <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(GooglePubSubSubscriberOptions.BackgroundWorkerCount)} must be explicitly configured " +
                        $"when {nameof(GooglePubSubSubscriberOptions.AckMode)} is {nameof(GooglePubSubAckMode.AckAfterEnqueue)}.");
                }

                if (subscriberOptions.BackgroundQueueCapacity <= 0)
                {
                    throw new InvalidOperationException(
                        $"{optionPath}.{nameof(GooglePubSubSubscriberOptions.BackgroundQueueCapacity)} must be explicitly configured " +
                        $"when {nameof(GooglePubSubSubscriberOptions.AckMode)} is {nameof(GooglePubSubAckMode.AckAfterEnqueue)}.");
                }

                AsyncResponseChannelOptions.EnsureTimerBacked(subscriberOptions.BackgroundDrainTimeout, optionPath, nameof(GooglePubSubSubscriberOptions.BackgroundDrainTimeout));

                // Pub/Sub spends the background drain plus the bounded subscriber-client stop
                // (ShutdownTimeout) at shutdown; both must fit inside the host budget.
                ShutdownBudgetValidator.Validate(
                    "Pub/Sub",
                    $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.HostShutdownTimeout)}",
                    transportOptions.HostShutdownTimeout,
                    ($"{optionPath}.{nameof(GooglePubSubSubscriberOptions.BackgroundDrainTimeout)}", subscriberOptions.BackgroundDrainTimeout),
                    ($"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(GooglePubSubAsyncResponseOptions.ShutdownTimeout)}", transportOptions.ShutdownTimeout));

                return;

            default:
                throw new InvalidOperationException(
                    $"{optionPath}.{nameof(GooglePubSubSubscriberOptions.AckMode)} has unsupported value '{subscriberOptions.AckMode}'.");
        }
    }

    /// <summary>Handles the delivered message.</summary>
    public abstract Task<SubscriberClient.Reply> HandleAsync(
        PubsubMessage message,
        CancellationToken subscriberCancellationToken);

    /// <summary>Releases resources held by this instance.</summary>
    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <summary>
    /// Host stop, before the subscriber client is stopped: stops starting deliveries (each one
    /// arriving from now on is held, unstarted, until <see cref="ReleaseHeldDeliveries"/>) and
    /// waits up to <paramref name="budget"/> on <paramref name="clock"/> for the handlers already
    /// running. A no-op for the early-ACK dispatcher, whose handlers run off the SDK callback.
    /// </summary>
    public virtual Task DrainInFlightAsync(TimeSpan budget, TimeProvider clock) => Task.CompletedTask;

    /// <summary>
    /// Hands back (Nack) every delivery held for the client stop — since
    /// <see cref="DrainInFlightAsync"/> began, handed back by the flow engine, or (early ACK) taken
    /// after the host stop began; called immediately before the subscriber client is stopped.
    /// </summary>
    public void ReleaseHeldDeliveries() => _clientStopping.TrySetResult();

    /// <summary>
    /// Holds a delivery that must not start — it arrived during the stop's drain, the flow engine
    /// handed it back at host stop, or (early ACK) it arrived after the host stop began — until the
    /// client stop, then hands it back. The streaming pull runs until the client is stopped, so a
    /// Nack returned at once freed its flow-control slot at once and Pub/Sub redelivered the
    /// message straight away — often to this same stream — for the whole stop window: a Nack storm
    /// that spent a DeadLetterPolicy's delivery attempts on healthy backlog and on the flow
    /// wake-ups handed over at the stop. Held, the delivery keeps its slot (the SDK keeps extending
    /// its lease), so the pull stalls once the slots are full, and the client stop's
    /// NackImmediately hands it back exactly once — one delivery attempt.
    /// </summary>
    protected async Task<SubscriberClient.Reply> HoldUntilClientStopAsync(CancellationToken subscriberCancellationToken)
    {
        try
        {
            await _clientStopping.Task.WaitAsync(subscriberCancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (subscriberCancellationToken.IsCancellationRequested)
        {
            // The SDK's own hard stop: hand it back now.
        }

        return SubscriberClient.Reply.Nack;
    }

    /// <summary>Runs the ExecuteHandlerAsync operation.</summary>
    protected async Task ExecuteHandlerAsync(
        PubsubMessage message,
        CancellationToken cancellationToken,
        bool logFailures = true)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.pubsub.receive",
            ActivityKind.Consumer);
        activity?.SetTag("asyncresponse.transport", "google_pubsub");
        activity?.SetTag("asyncresponse.pubsub.role", _role.ToString());
        activity?.SetTag("asyncresponse.pubsub.ack_mode", _subscriberOptions.AckMode.ToString());
        activity?.SetTag("messaging.system", "gcp_pubsub");
        activity?.SetTag("messaging.destination.name", _subscriptionId);
        activity?.SetTag("messaging.message.id", message.MessageId);

        if (message.Attributes.TryGetValue(_transportOptions.CorrelationIdAttribute, out var correlationId))
            AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        try
        {
            await _handler(message, cancellationToken).ConfigureAwait(false);
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
                    (Logger, ex, message.MessageId),
                    static state => state.Logger.LogError(state.ex, "Pub/Sub message handling failed for message {MessageId}.", state.MessageId));
            }

            throw;
        }
    }

    /// <summary>
    /// Whether <paramref name="exception"/> is the host stopping rather than a handler failure: a
    /// cancellation once the subscriber's own token has fired, or the durable-flow engine's
    /// <see cref="DurableFlowInterruptedException"/>, which it throws on <c>ApplicationStopping</c>
    /// — BEFORE any hosted service stops, so usually while the subscriber's token is still live.
    /// </summary>
    protected static bool IsHostStop(Exception exception, CancellationToken subscriberCancellationToken)
        => exception is OperationCanceledException
            && (subscriberCancellationToken.IsCancellationRequested || exception is DurableFlowInterruptedException);

    /// <summary>Runs the NotifyBackgroundFailureAsync operation.</summary>
    protected async ValueTask NotifyBackgroundFailureAsync(
        PubsubMessage message,
        Exception exception,
        string subscriptionId,
        GooglePubSubSubscriberRole role)
    {
        var callback = _subscriberOptions.OnBackgroundFailure;
        if (callback is null)
            return;

        try
        {
            await callback(new GooglePubSubBackgroundFailureContext(
                subscriptionId,
                role.ToString(),
                message,
                exception)).ConfigureAwait(false);
        }
        catch (Exception callbackException)
        {
            SafeLog.Try(
                (Logger, callbackException, message.MessageId, subscriptionId),
                static state => state.Logger.LogError(
                    state.callbackException,
                    "Pub/Sub background failure callback failed for already-ACKed message {MessageId} on {SubscriptionId}.",
                    state.MessageId,
                    state.subscriptionId));
        }
    }
}

internal sealed class AwaitingGooglePubSubMessageDispatcher(
    Func<PubsubMessage, CancellationToken, Task> handler,
    GooglePubSubAsyncResponseOptions transportOptions,
    GooglePubSubSubscriberOptions subscriberOptions,
    ILogger logger,
    string subscriptionId,
    GooglePubSubSubscriberRole role)
    : GooglePubSubMessageDispatcher(handler, transportOptions, subscriberOptions, logger, subscriptionId, role)
{
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _inFlight;
    private int _stopping;

    /// <summary>Handles the delivered message.</summary>
    public override async Task<SubscriberClient.Reply> HandleAsync(
        PubsubMessage message,
        CancellationToken subscriberCancellationToken)
    {
        // Counted BEFORE the stop flag is read (and the stop sets its flag with a full fence
        // before reading the count), so a delivery either sees the stop or is waited for.
        Interlocked.Increment(ref _inFlight);
        if (Volatile.Read(ref _stopping) != 0)
        {
            // The host is stopping: start nothing the stop budget cannot cover — and do not count
            // this delivery as running, so the drain never waits for it.
            LeaveInFlight();
            return await HoldUntilClientStopAsync(subscriberCancellationToken).ConfigureAwait(false);
        }

        try
        {
            await ExecuteHandlerAsync(message, subscriberCancellationToken).ConfigureAwait(false);
            return SubscriberClient.Reply.Ack;
        }
        catch (Exception ex) when (IsHostStop(ex, subscriberCancellationToken))
        {
            // Host shutdown, not a handler failure. Pub/Sub's handler contract offers no
            // "leave unsettled": the only redelivery primitive is Nack (an expired ack
            // deadline counts a delivery attempt exactly the same), so Nack is returned here
            // too — but through this explicit branch so shutdown cancellation is never
            // mistaken for (or later routed through) a failure policy.
            if (subscriberCancellationToken.IsCancellationRequested)
                return SubscriberClient.Reply.Nack;

            // The flow engine saw the host stop (ApplicationStopping) before this subscriber's
            // stop, while the pull is still live: a Nack now is redelivered at once, often to
            // this same stream, interrupted again — a Nack loop spending a DeadLetterPolicy's
            // attempts until the stop begins. Held below instead, like a delivery arriving during
            // the drain (ASB/SQS stop-receiving parity). Falls through once out of the in-flight
            // count, so the drain never waits for it.
        }
        catch
        {
            return SubscriberClient.Reply.Nack;
        }
        finally
        {
            LeaveInFlight();
        }

        return await HoldUntilClientStopAsync(subscriberCancellationToken).ConfigureAwait(false);
    }

    private void LeaveInFlight()
    {
        if (Interlocked.Decrement(ref _inFlight) == 0 && Volatile.Read(ref _stopping) != 0)
            _drained.TrySetResult();
    }

    /// <summary>
    /// Waits, before the client is stopped, for the handlers already running. The SDK's stop hands
    /// every message still in leasing back at once — any stop timeout under its 30-second
    /// hard-stop window skips WaitForProcessing — so a job whose handler was still running lost its
    /// ack-deadline extension, was redelivered to a peer mid-run, and its eventual Ack was dropped:
    /// every in-flight job ran twice on every rolling deploy. Draining first keeps each running
    /// handler's lease and lets its Ack reach the client's ack queue before the stop.
    /// </summary>
    public override async Task DrainInFlightAsync(TimeSpan budget, TimeProvider clock)
    {
        Interlocked.Exchange(ref _stopping, 1);
        var inFlight = Volatile.Read(ref _inFlight);
        if (inFlight == 0)
            _drained.TrySetResult();

        SafeLog.Try(
            (Logger, budget, inFlight),
            static state => state.Logger.LogInformation(
                "Pub/Sub subscriber is stopping: holding new deliveries for the client stop and waiting up to {Budget} for {InFlight} running handler(s) before stopping the client.",
                state.budget,
                state.inFlight));
        try
        {
            await _drained.Task.WaitAsync(budget, clock).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            SafeLog.Try(
                (Logger, budget),
                static state => state.Logger.LogWarning(
                    "Pub/Sub handlers still running on stop did not finish within {Budget} (BackgroundDrainTimeout, shortened to what the host shutdown budget leaves); stopping the subscriber client hands their messages back for redelivery.",
                    state.budget));
        }
    }

    /// <summary>Releases anything still held, should the client stop never have been reached.</summary>
    public override ValueTask DisposeAsync()
    {
        ReleaseHeldDeliveries();
        return ValueTask.CompletedTask;
    }
}

internal sealed class QueuedGooglePubSubMessageDispatcher : GooglePubSubMessageDispatcher
{
    private readonly Channel<PubsubMessage> _queue;
    private readonly Task[] _workers;
    private readonly CancellationTokenSource _drainCancellation = new();
    private readonly TimeSpan _drainTimeout;
    private readonly string _subscriptionId;
    private readonly GooglePubSubSubscriberRole _role;
    private readonly WorkerIntakeGate? _intakeGate;
    private int _pendingCount;
    private int _runningCount;
    private int _disposeStarted;
    private int _intakeClosedLogged;

    /// <summary>Runs the QueuedGooglePubSubMessageDispatcher operation.</summary>
    public QueuedGooglePubSubMessageDispatcher(
        Func<PubsubMessage, CancellationToken, Task> handler,
        GooglePubSubAsyncResponseOptions transportOptions,
        GooglePubSubSubscriberOptions subscriberOptions,
        ILogger logger,
        string subscriptionId,
        GooglePubSubSubscriberRole role,
        WorkerIntakeGate? intakeGate = null)
        : base(handler, transportOptions, subscriberOptions, logger, subscriptionId, role)
    {
        _drainTimeout = subscriberOptions.BackgroundDrainTimeout;
        _subscriptionId = subscriptionId;
        _role = role;
        _intakeGate = intakeGate;
        _queue = Channel.CreateBounded<PubsubMessage>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
        {
            AllowSynchronousContinuations = false,
            // Wait powers the queue-full backpressure path in HandleAsync: WriteAsync parks the
            // subscriber callback until a worker frees a slot instead of dropping or NACKing.
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
            "Created Pub/Sub ACK-after-enqueue dispatcher for {SubscriptionId} with {WorkerCount} worker(s), queue capacity {QueueCapacity}, drain timeout {DrainTimeout}.",
            _subscriptionId,
            subscriberOptions.BackgroundWorkerCount,
            subscriberOptions.BackgroundQueueCapacity,
            _drainTimeout);
    }

    internal int PendingCount => Volatile.Read(ref _pendingCount);
    internal int RunningCount => Volatile.Read(ref _runningCount);

    /// <summary>Handles the delivered message.</summary>
    public override async Task<SubscriberClient.Reply> HandleAsync(
        PubsubMessage message,
        CancellationToken subscriberCancellationToken)
    {
        // Host stop has begun (ApplicationStopping) and this is the worker: acknowledging now would
        // settle a delivery this stopping host may no longer run — among them the flow wake-ups the
        // engine's hand-overs had just published for a live replica, which reach their first timer
        // here, are handed back, and (ACKed) are lost. The streaming pull cannot stop short of the
        // client stop, so hold the delivery unstarted and hand it back then instead.
        if (_intakeGate?.IsClosed == true)
            return await HoldForHostStopAsync(message, subscriberCancellationToken).ConfigureAwait(false);

        try
        {
            Interlocked.Increment(ref _pendingCount);
            if (_queue.Writer.TryWrite(message))
            {
                // Enqueued: from here the reply is Ack whatever the log does — a throwing logger
                // reaching the catch below would NACK a message a worker is already running.
                SafeLog.Try(
                    (Logger, message.MessageId, _subscriptionId, PendingCount, RunningCount),
                    static state => state.Logger.LogDebug(
                        "Enqueued Pub/Sub message {MessageId} for background handling on {SubscriptionId}. Pending={PendingCount}, Running={RunningCount}.",
                        state.MessageId,
                        state._subscriptionId,
                        state.PendingCount,
                        state.RunningCount));
                return SubscriberClient.Reply.Ack;
            }

            // Queue full: apply backpressure instead of NACKing. A NACK burns one delivery attempt of
            // a DeadLetterPolicy configured on the subscription, so a saturated worker pool would
            // dead-letter healthy, never-executed messages. The streaming pull is flow-control-bounded
            // to the queue capacity, so at most capacity callbacks wait here; the await completes as
            // soon as a background worker frees a slot.
            SafeLog.Try(
                (Logger, _subscriptionId, message.MessageId, PendingCount, RunningCount),
                static state => state.Logger.LogDebug(
                    "Pub/Sub background queue is full for {SubscriptionId}; waiting for capacity before ACKing message {MessageId}. Pending={PendingCount}, Running={RunningCount}.",
                    state._subscriptionId,
                    state.MessageId,
                    state.PendingCount,
                    state.RunningCount));
            // The wait also ends when the host stop begins (the intake gate), so a delivery parked
            // here is never acknowledged after it: capacity freed by the drain would otherwise
            // enqueue — and settle — it then.
            using var intake = _intakeGate is { } gate
                ? CancellationTokenSource.CreateLinkedTokenSource(subscriberCancellationToken, gate.HostStopping)
                : null;
            await _queue.Writer.WriteAsync(message, intake?.Token ?? subscriberCancellationToken).ConfigureAwait(false);
            return SubscriberClient.Reply.Ack;
        }
        catch (OperationCanceledException) when (!subscriberCancellationToken.IsCancellationRequested && _intakeGate?.IsClosed == true)
        {
            // The host stop began while this delivery waited for capacity: never written, so hold it.
            Interlocked.Decrement(ref _pendingCount);
            return await HoldForHostStopAsync(message, subscriberCancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Cancellation (subscriber stopping) or a completed channel (dispatcher disposing):
            // NACK so Pub/Sub redelivers the message to the next subscriber instance.
            Interlocked.Decrement(ref _pendingCount);
            if (ex is OperationCanceledException or ChannelClosedException)
            {
                SafeLog.Try(
                    (Logger, message.MessageId, _subscriptionId),
                    static state => state.Logger.LogDebug(
                        "Pub/Sub message {MessageId} for {SubscriptionId} could not be enqueued during shutdown; returning NACK.",
                        state.MessageId,
                        state._subscriptionId));
            }
            else
            {
                SafeLog.Try(
                    (Logger, ex, message.MessageId, _subscriptionId),
                    static state => state.Logger.LogError(state.ex, "Failed to enqueue Pub/Sub message {MessageId} for {SubscriptionId}; returning NACK.", state.MessageId, state._subscriptionId));
            }

            return SubscriberClient.Reply.Nack;
        }
    }

    /// <summary>
    /// Holds a delivery taken after the host stop began, unstarted and unacknowledged, until the
    /// client stop hands it back (one delivery attempt; see <see cref="GooglePubSubMessageDispatcher.HoldUntilClientStopAsync"/>).
    /// </summary>
    private Task<SubscriberClient.Reply> HoldForHostStopAsync(PubsubMessage message, CancellationToken subscriberCancellationToken)
    {
        if (Interlocked.Exchange(ref _intakeClosedLogged, 1) == 0)
        {
            SafeLog.Try(
                (Logger, _role, _subscriptionId),
                static state => state.Logger.LogInformation(
                    "Pub/Sub {Role} subscriber for {SubscriptionId} stops taking deliveries: the host is stopping. Deliveries arriving from now on are held unstarted and handed back when the subscriber client stops.",
                    state._role,
                    state._subscriptionId));
        }

        SafeLog.Try(
            (Logger, message.MessageId, _subscriptionId),
            static state => state.Logger.LogDebug(
                "Holding Pub/Sub message {MessageId} for {SubscriptionId} unstarted until the subscriber client stops: the host is stopping.",
                state.MessageId,
                state._subscriptionId));
        return HoldUntilClientStopAsync(subscriberCancellationToken);
    }

    /// <summary>Releases resources held by this instance.</summary>
    public override async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;

        // Anything still held for a client stop that was never reached is handed back now.
        ReleaseHeldDeliveries();

        _queue.Writer.TryComplete();
        SafeLog.Try(
            (Logger, _subscriptionId, PendingCount, RunningCount),
            static state => state.Logger.LogInformation(
                "Draining Pub/Sub ACK-after-enqueue dispatcher for {SubscriptionId}. Pending={PendingCount}, Running={RunningCount}.",
                state._subscriptionId,
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
            SafeLog.Try(
                (Logger, _subscriptionId, PendingCount, RunningCount),
                static state => state.Logger.LogInformation(
                    "Drained Pub/Sub ACK-after-enqueue dispatcher for {SubscriptionId}. Pending={PendingCount}, Running={RunningCount}.",
                    state._subscriptionId,
                    state.PendingCount,
                    state.RunningCount));
        }
        catch (TimeoutException ex)
        {
            _drainCancellation.Cancel();
            SafeLog.Try(
                (Logger, ex, _subscriptionId, PendingCount, RunningCount),
                static state => state.Logger.LogWarning(
                    state.ex,
                    "Timed out while draining Pub/Sub ACK-after-enqueue dispatcher for {SubscriptionId}. Pending={PendingCount}, Running={RunningCount}. Already ACKed work may be interrupted by host shutdown.",
                    state._subscriptionId,
                    state.PendingCount,
                    state.RunningCount));

            // The workers surface a lapsed entry only once one of them frees up — and with every
            // worker still inside a handler that ignores the token, none does before this returns,
            // the host finishes stopping and the process exits: the entries still queued vanished
            // with no OnBackgroundFailure call at all. So the dispose surfaces them itself.
            await SurfaceUndrainedAsync(surfacingReserve).ConfigureAwait(false);

            // The workers are still running and read _drainCancellation.Token each loop, so disposing
            // it now would throw ObjectDisposedException inside them. Dispose once they actually finish,
            // off the shutdown path, so the source is not leaked either.
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
                (Logger, ex, _subscriptionId),
                static state => state.Logger.LogDebug(state.ex, "Pub/Sub ACK-after-enqueue dispatcher drain for {SubscriptionId} ended with an error.", state._subscriptionId));
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
            if (remaining <= TimeSpan.Zero || !_queue.Reader.TryRead(out var message))
                break;

            Interlocked.Decrement(ref _pendingCount);
            surfaced++;
            try
            {
                await SurfaceLapsedAsync(message).AsTask().WaitAsync(remaining).ConfigureAwait(false);
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
            (Logger, _subscriptionId, surfaced, lost, reserve),
            static state => state.Logger.LogError(
                "The Pub/Sub ACK-after-enqueue drain budget for {SubscriptionId} lapsed with {Count} already-ACKed message(s) never handled — Pub/Sub cannot redeliver them. Surfaced {Surfaced} via OnBackgroundFailure within the reserved {Reserve}; {Lost} could not be surfaced before shutdown.",
                state._subscriptionId,
                state.surfaced + state.lost,
                state.surfaced,
                state.reserve,
                state.lost));
    }

    private ValueTask SurfaceLapsedAsync(PubsubMessage message)
    {
        SafeLog.Try(
            (Logger, message.MessageId, _subscriptionId),
            static state => state.Logger.LogWarning(
                "Pub/Sub background handler for already-ACKed message {MessageId} on {SubscriptionId} was not started: the dispatcher's drain budget had lapsed. Surfacing via OnBackgroundFailure.",
                state.MessageId,
                state._subscriptionId));

        return NotifyBackgroundFailureAsync(
            message,
            new OperationCanceledException(
                "The ACK-after-enqueue drain budget lapsed before this already-ACKed message was handled."),
            _subscriptionId,
            _role);
    }

    private async Task RunWorkerAsync(int workerIndex)
    {
        await foreach (var message in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            Interlocked.Decrement(ref _pendingCount);
            Interlocked.Increment(ref _runningCount);

            // Once the drain budget has lapsed, STOP executing. The token below cannot stop the
            // real handler — it is the ingress, whose target takes no CancellationToken — so the
            // loop kept starting fresh work past the budget and every message still queued at
            // process exit vanished with no record (they were ACKed at enqueue, so Pub/Sub will not
            // redeliver them). Route them through OnBackgroundFailure instead of losing them.
            if (_drainCancellation.IsCancellationRequested)
            {
                await SurfaceLapsedAsync(message).ConfigureAwait(false);
                Interlocked.Decrement(ref _runningCount);
                continue;
            }

            try
            {
                SafeLog.Try(
                    (Logger, workerIndex, message.MessageId, _subscriptionId, PendingCount, RunningCount),
                    static state => state.Logger.LogDebug(
                        "Pub/Sub background worker {WorkerIndex} handling message {MessageId} for {SubscriptionId}. Pending={PendingCount}, Running={RunningCount}.",
                        state.workerIndex,
                        state.MessageId,
                        state._subscriptionId,
                        state.PendingCount,
                        state.RunningCount));
                await ExecuteHandlerAsync(
                    message,
                    _drainCancellation.Token,
                    logFailures: false).ConfigureAwait(false);
            }
            catch (DurableFlowInterruptedException ex)
            {
                // The flow engine handed the job back because the host is stopping (Redis/NATS
                // parity): not a handler failure — but the message was ACKed at enqueue, Pub/Sub
                // will not redeliver it, and the transport writes no dead-letter copy, so the
                // wake-up is lost unless the report records it: an Error, as on every sibling
                // transport that cannot write a copy.
                SafeLog.Try(
                    (Logger, message.MessageId, _subscriptionId),
                    static state => state.Logger.LogError(
                        "Pub/Sub background handler for already-ACKed message {MessageId} on {SubscriptionId} was handed back by the flow engine because the host is stopping; Pub/Sub will not redeliver it, and no dead-letter copy can be written for an ACKed message, so the wake-up is lost unless OnBackgroundFailure records it (resume the flow explicitly).",
                        state.MessageId,
                        state._subscriptionId));
                await NotifyBackgroundFailureAsync(
                    message,
                    ex,
                    _subscriptionId,
                    _role).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // A throwing logger must not end this loop: the workers are the only thing that
                // runs the already-ACKed jobs queued behind this one.
                SafeLog.Try(
                    (Logger, ex, message.MessageId, _subscriptionId),
                    static state => state.Logger.LogError(
                        state.ex,
                        "Pub/Sub background handler failed for already-ACKed message {MessageId} on {SubscriptionId}.",
                        state.MessageId,
                        state._subscriptionId));
                await NotifyBackgroundFailureAsync(
                    message,
                    ex,
                    _subscriptionId,
                    _role).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _runningCount);
            }
        }
    }
}
