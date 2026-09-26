using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Buffers;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace AsyncResponse.Channels.NATS;

/// <summary>
/// NATS-backed response channel:
/// <list type="bullet">
/// <item><description>Delivers responses over NATS Core request/reply on a subject keyed by
/// correlation id: a waiter subscribes and acks each message, and the publisher requests so the NATS
/// "no responders" signal reports precisely when nobody is listening.</description></item>
/// <item><description>Persists <see cref="RecoveryState"/> in a JetStream Key-Value bucket so a
/// response arriving after the waiter died (e.g. a redeploy) is routed through the lost-subscriber
/// dispatcher, which asks the payload's OnRecovery and invokes the resume or failure
/// callback with the materialized payload (or keeps the registration armed for a checkpoint).</description></item>
/// </list>
/// </summary>
internal sealed class NatsAsyncResponseChannel : IAsyncResponsePublisher, IRawAsyncResponsePublisher, IRecoverableAsyncResponseSubscriber, IActiveSubscriberProbe
{
    private readonly INatsResponseChannelClient _client;
    private readonly IRecoveryStateStore _recoveryStateStore;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly LostSubscriberCallbackDispatcher _lostSubscriberDispatcher;
    private readonly NatsSubjectSchema _subjects;
    private readonly NatsAsyncResponseChannelOptions _options;
    private readonly ILogger<NatsAsyncResponseChannel> _logger;
    private readonly TimeProvider _timeProvider;

    /// <summary>Creates a NATS-backed async-response channel.</summary>
    public NatsAsyncResponseChannel(
        IServiceScopeFactory scopeFactory,
        INatsResponseChannelClient client,
        IRecoveryStateStore recoveryStateStore,
        IOptions<NatsAsyncResponseChannelOptions> options,
        AsyncResponseContextPropagation propagation,
        ILogger<NatsAsyncResponseChannel> logger,
        TimeProvider? timeProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _options = options.Value;
        _options.Validate();
        _client = client;
        _recoveryStateStore = recoveryStateStore;
        _propagation = propagation;
        _subjects = new NatsSubjectSchema(_options.SubjectPrefix);
        _logger = logger;
        _lostSubscriberDispatcher = new LostSubscriberCallbackDispatcher(scopeFactory, propagation, logger, _timeProvider);

        // Recovery keys are not scoped by SubjectPrefix, so a prefix chosen to isolate a
        // deployment isolates its response subjects but not its registrations: every deployment
        // left on the default bucket shares one keyspace.
        if (!string.Equals(_options.SubjectPrefix, NatsAsyncResponseChannelOptions.DefaultSubjectPrefix, StringComparison.Ordinal)
            && string.Equals(_options.RecoveryBucket, NatsAsyncResponseChannelOptions.DefaultRecoveryBucket, StringComparison.Ordinal))
        {
            _logger.LogWarning(
                "The NATS channel's SubjectPrefix is '{SubjectPrefix}' but its RecoveryBucket is still the default '{RecoveryBucket}'. Recovery registrations are keyed by correlation id only, " +
                "so every deployment sharing this bucket sees the others' registrations (their long waits reported as stale by each watchdog, a shared correlation id's registration consumed by the wrong one). " +
                "Give each deployment its own RecoveryBucket as well.",
                _options.SubjectPrefix,
                _options.RecoveryBucket);
        }
    }

    /// <summary>Longest excerpt of a remote failure message copied into the wait span's status.</summary>
    private const int MaxRemoteFailureStatusLength = 256;

    // ---------------------------------------------------------------------------------------
    // IAsyncResponseSubscriber / IRecoverableAsyncResponseSubscriber

    /// <inheritdoc/>
    public Task<IAsyncResponseWaiter<T>> CreateResponseWaiter<T>(
        string correlationId,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null) where T : IAsyncResponsePayload
        => CreateResponseWaiterCore(correlationId, resumeCallback: null, failureCallback: null, completionPredicate, timeout);

    /// <inheritdoc/>
    public Task<IAsyncResponseWaiter<T>> CreateRecoverableResponseWaiter<T>(
        string correlationId,
        ReflectionCallDto? resumeCallback = null,
        ReflectionCallDto? failureCallback = null,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null) where T : IAsyncResponsePayload
        => CreateResponseWaiterCore(correlationId, resumeCallback, failureCallback, completionPredicate, timeout);

    /// <summary>
    /// The public <c>ResponseTask</c> surface: internal loop-fault settlements are marked with
    /// <see cref="NatsConsumeLoopException"/> so registration can abort atomically, but callers
    /// observe the original exception exactly as before.
    /// </summary>
    private static async Task<T> UnwrapConsumeLoopFaults<T>(Task<T> task) where T : IAsyncResponsePayload
    {
        try
        {
            return await task.ConfigureAwait(false);
        }
        catch (NatsConsumeLoopException loopFault)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(loopFault.InnerException!).Throw();
            throw; // unreachable
        }
    }

    private async Task<IAsyncResponseWaiter<T>> CreateResponseWaiterCore<T>(
        string correlationId,
        ReflectionCallDto? resumeCallback,
        ReflectionCallDto? failureCallback,
        Func<T, ValueTask<bool>>? completionPredicate,
        TimeSpan? timeout) where T : IAsyncResponsePayload
    {
        CorrelationIdGuard.ThrowIfUnusable(correlationId);

        // Recovery callbacks only make sense if the payload can say whether a late response should
        // resume or fail the flow. On this durable channel that decision is real (it survives a
        // redeploy), so require the override rather than letting the conservative default silently
        // route every recovered response to the failure callback.
        if ((resumeCallback is not null || failureCallback is not null)
            && !AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(T)))
        {
            throw new InvalidOperationException(
                $"Payload type '{typeof(T)}' registers lost-subscriber recovery callbacks on the NATS channel " +
                $"but does not override {nameof(IAsyncResponsePayload)}.{nameof(IAsyncResponsePayload.OnRecovery)}(). " +
                "Override it to declare what each response does to the flow — RecoveryAction.Resume, " +
                "RecoveryAction.Fail, or RecoveryAction.KeepWaiting for non-terminal checkpoints; the durable " +
                "channel needs this to route a response that arrives after the waiter was lost.");
        }

        // default: first envelope completes the wait
        completionPredicate ??= _ => new ValueTask<bool>(true);

        // Default timeout aligned with the recovery-state expiry: an infinite wait is never
        // meaningful, because once the recovery state expires the correlation id has no recovery
        // anyway. Timing out routes the flow through its normal failure handling instead of
        // leaving it stuck forever.
        timeout ??= _options.DefaultTimeout ?? _options.RecoveryStateExpiry;
        // BEFORE any side effect: an unsupported resolved timeout (non-positive, or past the
        // ~49.7-day BCL timer ceiling) used to throw only at timer arming — after the
        // subscription and recovery state existed, leaking both — and zero used to slip through
        // on some channels entirely, insta-timing-out a fully registered waiter.
        AsyncResponseChannelOptions.EnsureWaiterTimeoutSupported(timeout.Value);

        var storedCorrelationId = correlationId;
        // Capture the subscribe-time ExecutionContext so app AsyncLocals (trace, principal, logging
        // scope) flow into the message handler, which runs on a background consume-loop thread.
        var capturedContext = ExecutionContext.Capture();
        var subject = _subjects.ResponseSubject(correlationId);

        var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.wait", correlationId: correlationId);
        activity?.SetTag("asyncresponse.channel", "nats");
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));
        activity?.SetTag("asyncresponse.timeout_seconds", timeout.Value.TotalSeconds);

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Waiting for response on correlationId {CorrelationId} with timeout {Timeout}.", correlationId, timeout.Value);

        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registrationId = Guid.NewGuid();

        // Single-use cancellation token implementing the timeout. Armed only after subscribe + recovery
        // save succeed, but its callback is registered first so a very fast terminal message cleans up safely.
        // Clock-injected (DbChannelShared parity): CancelAfter on a default CTS is bound to the
        // system clock, so a virtual clock could never fire a production-sized waiter timeout.
        var cancellationTokenSource = new CancellationTokenSource(Timeout.InfiniteTimeSpan, _timeProvider);
        CancellationTokenRegistration timeoutRegistration = default;
        INatsChannelSubscription? subscription = null;

        // The subscription's lifetime, deliberately NOT the timeout source: NATS.Net ends a
        // subscription the moment its subscribe token is cancelled, so a subscription bound to
        // the timeout lost its server-side interest at CancelAfter — before the drain below had
        // deleted the recovery registration. A publish landing in that window saw "no responders,
        // registration present" and fired a recovery callback while the waiter reported a
        // timeout. Cancelled only as the teardown backstop.
        var subscriptionLifetime = new CancellationTokenSource();

        // -------------------------------------------------------------------------
        // Local: CleanupOnceAsync — ends the stream (which ends the consume loop), deletes
        // recovery state, and tears down the timeout, exactly once.
        int cleanupStarted = 0;
        int teardownBudgetSpent = 0;
        int registrationDeleted = 0;
        Task? drainDelete = null;
        int overloaded = 0;
        var subscriptionTornDown = false;
        var cleanupGate = new object();
        Task? cleanupTask = null;
        var streamEndGate = new object();
        Task? streamEndTask = null;
        var consumeLoop = Task.CompletedTask;

        // The ONE place the server-side subscription is disposed — the drain and the latched
        // cleanup both need the stream ended (whichever runs first), and having each dispose it
        // independently doubled the teardown for no benefit. TASK-latched and NEVER-faulting:
        // its failure is logged here exactly once, no matter how many latched callers observe
        // the task — and a caller that abandoned its bounded wait still gets the late outcome
        // recorded instead of it dying as a TaskScheduler.UnobservedTaskException. Callers read
        // "completed with subscriptionTornDown false" as teardown failure and backstop-cancel
        // (the cleanup core's finally, safe only after the timeout registration is gone).
        Task EndStreamOnce()
        {
            lock (streamEndGate)
            {
                return streamEndTask ??= EndStreamCoreAsync();
            }
        }

        async Task EndStreamCoreAsync()
        {
            if (subscription is null)
                return;

            try
            {
                await subscription.DisposeAsync().ConfigureAwait(false);
                subscriptionTornDown = true;
                SafeLog.Try((_logger, subject), static state => state._logger.LogDebug("Unsubscribed from subject {Subject}.", state.subject));
            }
            catch (Exception teardownEx)
            {
                SafeLog.Try(() => _logger.LogError(teardownEx, "Error during cleanup for subject {Subject}.", subject));
            }
        }

        // Task-latched so EVERY caller completes only when the one real cleanup has finished —
        // the previous fire-once int latch let a second caller (a disposing waiter racing the
        // timeout) return before the task was settled. The core itself never waits on the consume
        // loop: draining happens BEFORE the latch (DrainThenCleanupAsync), because the loop's own
        // finally also enters this latch — a join inside the core would make the loop await a core
        // that is joining the loop.
        ValueTask CleanupOnceAsync()
        {
            Task task;
            lock (cleanupGate)
            {
                task = cleanupTask ??= CleanupCoreAsync();
            }

            return task.IsCompletedSuccessfully ? ValueTask.CompletedTask : new ValueTask(task);
        }

        // Dispose-path cleanup: DRAINS the in-flight delivery before settling. The consume loop
        // may be mid Until-predicate holding a claimed terminal message; ending the stream and
        // joining the loop guarantees that by the time the latched core cancels, the task is
        // either settled by that delivery or genuinely undelivered. Never called from the loop
        // itself — loop-invoked cleanup uses CleanupOnceAsync directly, its task already settled
        // by the terminal dispatch.
        //
        // One DisposalDrainTimeout budget covers BOTH steps — a wedged client library can hang
        // the subscription dispose just as a wedged Until predicate can hang the loop join. The
        // core's cancel is only truthful once the JOIN below has proven the loop ended; any
        // drain outcome short of that — budget lapse, anything unforeseen — leaves a delivery
        // possibly mid-predicate holding a message already consumed from the stream, and
        // "canceled" would tell a re-attaching caller nothing was delivered. Those paths fault
        // the task with the explicit indeterminate contract instead (routing durable flows to a
        // fresh idempotent restart) and cancel the subscription token so the loop still ends
        // once the predicate returns. (A teardown FAILURE no longer throws — the latched
        // teardown logs it and leaves subscriptionTornDown false — so it backstop-cancels and
        // still proves settlement through the join.)
        //
        // waiterHandedOut is false only on the two registration-failure paths: no waiter was ever
        // returned, so nothing observes the task. There a lapse must not fault it indeterminate —
        // the fault would surface only as an UnobservedTaskException, and its Warning and span
        // status would report a delivery question about a wait that never started — and the
        // cleanup core settles it Canceled instead.
        async ValueTask DrainThenCleanupAsync(Exception? terminalIfUndelivered = null, bool waiterHandedOut = true)
        {
            if (Volatile.Read(ref cleanupStarted) == 0)
            {
                var drainTimeout = _options.DisposalDrainTimeout;
                var streamEndStarted = false;
                try
                {
                    using var budget = new CancellationTokenSource(drainTimeout);

                    // Delete the recovery registration FIRST, while the subscription is still
                    // live — the same "delete before unsubscribe" order the cleanup core keeps,
                    // for the same reason. This drain ends the stream before the core runs, so
                    // leaving the delete to the core reopened the window on the timeout and
                    // dispose paths: a publish landing after the UNSUB but before the delete saw
                    // "no responders, registration present" and fired a recovery callback while
                    // this waiter reported a timeout (or was cancelled). A response landing now
                    // reaches the live subscription instead. Inside the drain budget: during an
                    // outage the KV delete waits for the connection to come back, and awaited
                    // unbounded it held a timed-out waiter's ResponseTask pending for the whole
                    // outage. On a lapse registrationDeleted stays unset, and the cleanup core
                    // waits a bounded while more on THIS attempt — never a second delete beside
                    // it — after the waiter is settled, before the stream ends (the attempt never
                    // faults: it logs its own failure).
                    var deleteAttempt = DeleteRegistrationAsync();
                    Volatile.Write(ref drainDelete, deleteAttempt);
                    await deleteAttempt.WaitAsync(budget.Token).ConfigureAwait(false);

                    // ONE budget for the whole disposal: the latched cleanup below skips its own
                    // teardown wait when a drain already spent this budget on the same latched
                    // teardown task — a second full wait there made disposal cost double the
                    // configured DisposalDrainTimeout.
                    Volatile.Write(ref teardownBudgetSpent, 1);
                    streamEndStarted = true;
                    await EndStreamOnce().WaitAsync(budget.Token).ConfigureAwait(false);

                    // A failed teardown surfaces as "completed, subscriptionTornDown false" (the
                    // latched core logged it): backstop-cancel so the loop still ends, then FALL
                    // THROUGH to the join — a failed teardown proves nothing about a delivery
                    // mid-predicate, and skipping the join here let cleanup cancel a response the
                    // stream had already handed over.
                    if (!subscriptionTornDown && subscription is not null)
                        await DisarmThenCancelSubscriptionTokenAsync().ConfigureAwait(false);

                    // Settlement is PROVEN only by the loop having ended within the remaining
                    // budget — either it settled the task with the in-flight delivery, or it
                    // ended with nothing in flight and the cleanup's cancel below is truthful.
                    await consumeLoop.WaitAsync(budget.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!streamEndStarted && consumeLoop.IsCompleted)
                {
                    // The delete lapsed with nothing that could be in flight — no subscription
                    // (an abandoned registration) or a loop that already ended — so there is no
                    // settlement to prove. The cleanup core waits on the delete a while longer.
                    SafeLog.Try(() => _logger.LogDebug(
                        "Deleting the recovery registration for correlationId {CorrelationId} did not complete within {DrainTimeout}; the cleanup waits on it a while longer.",
                        correlationId, drainTimeout));
                }
                catch (Exception drainEx)
                {
                    if (waiterHandedOut)
                    {
                        // A TrySetResult from the late-finishing delivery loses against this and
                        // is dropped; the loop's own cleanup call is a no-op behind the latch. The
                        // non-cancellation exception case is unforeseen infrastructure failure —
                        // settlement is equally unproven there, so it must not fall back to cancel.
                        tcs.TrySetException(new AsyncResponseIndeterminateDeliveryException(correlationId, drainTimeout));
                        AsyncResponseDiagnostics.SetError(activity, "indeterminate_delivery", "Disposal drain did not prove settlement.");
                        SafeLog.Try(() => _logger.LogWarning(
                            "Disposal drain for correlationId {CorrelationId} did not prove settlement within {DrainTimeout}; faulting the waiter as indeterminate.",
                            correlationId, drainTimeout));
                    }
                    else
                    {
                        SafeLog.Try(() => _logger.LogDebug(
                            "Cleanup of the failed registration for correlationId {CorrelationId} did not finish within {DrainTimeout}; no waiter was handed out, so it ends canceled.",
                            correlationId, drainTimeout));
                    }

                    if (drainEx is not OperationCanceledException)
                        SafeLog.Try(() => _logger.LogDebug(drainEx, "Disposal drain failed for subject {Subject}.", subject));

                    // A lapse inside the delete leaves the registration in place: cancelling the
                    // subscription now would unsubscribe before it is gone — the window the delete
                    // runs first to close. The cleanup core waits on that delete (bounded), then
                    // ends the stream; only the timeout registration is disarmed here.
                    if (streamEndStarted)
                        await DisarmThenCancelSubscriptionTokenAsync().ConfigureAwait(false);
                    else
                        await timeoutRegistration.DisposeAsync().ConfigureAwait(false);
                }
            }

            // Settle AFTER the drain, never before it. The consume loop may already hold a message
            // the subscription received — the publisher was told "delivered", so it exists nowhere
            // else. Faulting first let a timeout beat that in-flight delivery and report a consumed
            // response as a timeout; TrySet loses here if the delivery won, which is the whole
            // point. (A lapsed drain budget has already faulted the task as indeterminate above,
            // and TrySet is a no-op behind it.)
            if (terminalIfUndelivered is not null)
                tcs.TrySetException(terminalIfUndelivered);

            await CleanupOnceAsync().ConfigureAwait(false);
        }

        async Task CleanupAbandonedRegistrationAsync()
        {
            try
            {
                await DrainThenCleanupAsync(waiterHandedOut: false).ConfigureAwait(false);
            }
            catch (Exception cleanupEx)
            {
                // Fire-and-forget: nothing awaits this task, so an escaped fault would vanish.
                SafeLog.Try(() => _logger.LogError(cleanupEx, "Cleanup of the abandoned registration for correlationId {CorrelationId} failed.", correlationId));
            }
        }

        async ValueTask DisarmThenCancelSubscriptionTokenAsync()
        {
            // Disarm the waiter-timeout registration BEFORE the backstop cancel: this runs on
            // disposal paths, and a timeout firing behind it would stamp a spurious
            // TimeoutException plus a waiter-timeout metric onto a disposal that is not a timeout.
            // Idempotent with the cleanup core's own registration disposal.
            await timeoutRegistration.DisposeAsync().ConfigureAwait(false);
            try
            {
                subscriptionLifetime.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // Cleanup already ran and disposed the source; the loop is ending regardless.
            }
        }

        // Best-effort: the KV entry expires on its own, and a transient store failure must not
        // skip the subscription teardown that follows. Marked once it succeeded so the cleanup
        // core does not repeat a delete the drain already made.
        async Task DeleteRegistrationAsync()
        {
            try
            {
                await _recoveryStateStore.TryDeleteAsync(correlationId, registrationId).ConfigureAwait(false);
                Volatile.Write(ref registrationDeleted, 1);
            }
            catch (Exception ex)
            {
                SafeLog.Try(() => _logger.LogError(ex, "Failed to delete recovery state for correlationId {CorrelationId}.", correlationId));
            }
        }

        async Task CleanupCoreAsync()
        {
            Interlocked.Exchange(ref cleanupStarted, 1);

            // ONE DisposalDrainTimeout for this core — the delete below and the teardown after it
            // — so a disposal spends at most the drain's budget plus this one.
            using var coreBudget = new CancellationTokenSource(_options.DisposalDrainTimeout);
            try
            {
                // Delete the recovery state BEFORE disposing the subscription. In the reverse
                // order a publish landing in the window sees "no responders, state present" and
                // fires a spurious recovery callback for a wait that already reached a terminal
                // state. In this order the window shows a subscriber that drops the message — a
                // late or duplicate terminal message is droppable; a resurrected recovery callback
                // is not. (A drain that preceded this core already deleted it, the same way.)
                //
                // Bounded: during a NATS outage the KV delete waits for the connection to come
                // back, and awaited unbounded here it held every disposing waiter — a flow step
                // holding its delivery and lease among them — for the whole outage. A drain whose
                // delete is still in flight is waited on, never duplicated; past the bound the
                // teardown goes ahead (the ordering is moot while disconnected: the server has
                // already dropped this client's interest), the abandoned attempt logs its own
                // outcome, and the entry's TTL plus the recovery watchdog back it.
                if (Volatile.Read(ref registrationDeleted) == 0)
                {
                    var pendingDelete = Volatile.Read(ref drainDelete);
                    var deletion = pendingDelete is { IsCompleted: false } ? pendingDelete : DeleteRegistrationAsync();
                    try
                    {
                        await deletion.WaitAsync(coreBudget.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (coreBudget.IsCancellationRequested)
                    {
                        SafeLog.Try(() => _logger.LogDebug(
                            "Deleting the recovery registration for correlationId {CorrelationId} did not complete within {DrainTimeout}; tearing the subscription down regardless (the registration expires with its TTL).",
                            correlationId, _options.DisposalDrainTimeout));
                    }
                }

                // End the stream if the drain has not already — dispatch-triggered cleanup (a
                // terminal delivery, a loop fault) reaches here without a drain. The task-latch
                // keeps the teardown single no matter which path got here first. Bounded like the
                // drain: this latched core is what a disposing waiter awaits when terminal
                // delivery started cleanup first (the drain skips itself on cleanupStarted), so
                // an unbudgeted teardown here let a wedged client library hold DisposeAsync
                // hostage past DisposalDrainTimeout. On the bound lapsing, the catch below logs
                // and the finally's backstop cancel still ends the consume loop; the abandoned
                // teardown task never faults (it logs its own late outcome). When a DRAIN
                // preceded this core, it already spent that budget on this same latched task —
                // waiting a second one here made disposal cost double the configured bound, so
                // the wait is skipped (the teardown is running and self-logging regardless).
                if (Volatile.Read(ref teardownBudgetSpent) == 0)
                {
                    var teardown = EndStreamOnce();

                    // The delete above can spend the whole budget (an outage): the teardown is
                    // started all the same and logs its own outcome, and the finally's backstop
                    // cancel ends the consume loop. Waiting on the spent budget threw at once and
                    // logged every waiter disposed during the outage as a cleanup Error.
                    if (!coreBudget.IsCancellationRequested)
                        await teardown.WaitAsync(coreBudget.Token).ConfigureAwait(false);
                }

                if (subscription is null)
                    subscriptionTornDown = true;
            }
            catch (Exception ex)
            {
                SafeLog.Try(() => _logger.LogError(ex, "Error during cleanup for subject {Subject}.", subject));
            }
            finally
            {
                await timeoutRegistration.DisposeAsync().ConfigureAwait(false);
                if (!subscriptionTornDown)
                {
                    // DisposeAsync did not complete, so the server-side subscription may still be
                    // pumping messages. Its lifetime is bound to this token (SubscribeAsync received
                    // it), and disposing a CTS never cancels — an explicit cancel is the backstop
                    // that ends the consume loop.
                    subscriptionLifetime.Cancel();
                }

                // A waiter disposed before any terminal signal must not leave ResponseTask pending
                // forever for callers that hold it directly — the timeout died above, so nothing
                // else could ever complete the task. A no-op after a normal completion, timeout,
                // fault, or a delivery drained by DrainThenCleanupAsync.
                tcs.TrySetCanceled();

                cancellationTokenSource.Dispose();
                subscriptionLifetime.Dispose();
                activity?.Dispose();
            }
        }

        // -------------------------------------------------------------------------
        // Local: ProcessResponseAsync — deserializes and handles a single envelope, completes the TCS when terminal.
        async Task ProcessResponseAsync(string? payload)
        {
            bool finished = false;
            try
            {
                if (string.IsNullOrEmpty(payload))
                {
                    // A non-probe message with no body cannot be a response; ignore it rather than fault.
                    _logger.LogWarning("Received empty response message for correlationId {CorrelationId}; ignoring.", correlationId);
                    return;
                }

                // JsonSafety, not the raw reader: a parse failure is logged below and handed to the
                // waiter, and the reader's own message quotes inbound property names and dictionary
                // keys (docs/security.md, "never logs a message body"). Size and position only.
                var envelope = JsonSafety.SafeDeserialize(payload, AsyncResponseEnvelopeJson.TypeInfo<T>());

                if (envelope == null)
                {
                    _logger.LogError("Failed to deserialize envelope for correlationId {CorrelationId}.", correlationId);
                    finished = true;
                    var deserializationError = new JsonException($"Failed to deserialize envelope for correlationId {correlationId}.");
                    AsyncResponseDiagnostics.SetError(activity, "deserialize_failure", deserializationError.Message);
                    if (!tcs.TrySetException(deserializationError))
                        _logger.LogWarning(deserializationError, "TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
                }
                else if (!AsyncResponseEnvelopeSchema.IsReadable(envelope.SchemaVersion))
                {
                    finished = true;
                    var schemaError = new InvalidOperationException(
                        $"Response envelope for correlationId {correlationId} has schema version {envelope.SchemaVersion}, " +
                        $"which this build does not support (current: {AsyncResponseEnvelopeSchema.Current}).");
                    AsyncResponseDiagnostics.SetError(activity, "schema_mismatch", schemaError.Message);
                    if (!tcs.TrySetException(schemaError))
                        _logger.LogWarning(schemaError, "TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
                }
                else if (!envelope.Success)
                {
                    finished = true;
                    var remoteFailure = new Exception(envelope.ExceptionMessage ?? "Unknown error during asynchronous processing.");
                    if (!string.IsNullOrEmpty(envelope.ExceptionStackTrace))
                        // Cap on receive too: the publish-side cap only bounds traces we emit, not what
                        // a remote we do not control can push at us.
                        remoteFailure.Data["RemoteStackTrace"] = RemoteStackTrace.Cap(envelope.ExceptionStackTrace, _options.MaxRemoteStackTraceLength);

                    // The remote's message is NOT logged (DB-channel parity) and reaches the span
                    // status only as a capped, escaped excerpt: like the stack trace above, it is
                    // text a remote we do not control chose — up to the whole inbound budget, with
                    // line breaks that forge log entries. The waiter's exception still carries it.
                    _logger.LogWarning("Received error response for correlationId {CorrelationId}.", correlationId);
                    AsyncResponseDiagnostics.SetError(activity, "remote_failure", DiagnosticText.EscapedExcerpt(remoteFailure.Message, MaxRemoteFailureStatusLength));
                    // Nor is it attached here: a duplicate or late error envelope would put the same
                    // remote-chosen text into the log after all (Redis parity).
                    if (!tcs.TrySetException(remoteFailure))
                        _logger.LogWarning("TaskCompletionSource already completed for correlationId {CorrelationId}; the error response was dropped.", correlationId);
                }
                else
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug("Received response for correlationId {CorrelationId}.", correlationId);
                    finished = await completionPredicate(envelope.Payload!).ConfigureAwait(false);
                    if (finished && !tcs.TrySetResult(envelope.Payload!))
                        _logger.LogWarning("TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message on subject {Subject} for correlationId {CorrelationId}.", subject, correlationId);
                finished = true;
                AsyncResponseDiagnostics.SetError(activity, ex);
                if (!tcs.TrySetException(ex))
                    _logger.LogWarning(ex, "TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
            }
            finally
            {
                if (finished)
                    await CleanupOnceAsync().ConfigureAwait(false);
            }
        }

        // -------------------------------------------------------------------------
        // Local: ProcessUnderCapturedContextAsync — restores the waiter's subscribe-time
        // ExecutionContext (app AsyncLocals) plus the correlation id before processing, since the
        // consume loop runs on a background thread that never had them.
        Task ProcessUnderCapturedContextAsync(string? payload)
        {
            async Task Process()
            {
                using var correlationScope = AsyncResponseContext.PushCorrelationId(storedCorrelationId);
                await ProcessResponseAsync(payload).ConfigureAwait(false);
            }

            if (capturedContext is null)
                return Process();

            Task? task = null;
            ExecutionContext.Run(capturedContext, _ => task = Process(), null);
            return task!;
        }

        // -------------------------------------------------------------------------
        // Local: ConsumeLoopAsync — reads messages serially from the subscription until it is disposed.
        async Task ConsumeLoopAsync(INatsChannelSubscription sub)
        {
            try
            {
                await foreach (var message in sub.ReadAsync(CancellationToken.None).ConfigureAwait(false))
                {
                    // Ack first so the publisher's request resolves quickly (delivery/liveness
                    // confirmed) even if processing the payload is slow — but never WAIT on it: the
                    // reply is a publish, and while the connection is reconnecting it waits for the
                    // reconnect. Awaited here, a response already received sat behind the outage,
                    // the waiter timed out (indeterminate), and the response was then processed
                    // into an already-settled wait and dropped. A failed ack must not abort the wait.
                    AcknowledgeWithoutWaiting(message);

                    // Past an overload fault the wait is settled as indeterminate: what is still
                    // buffered is skipped rather than run through the predicate on an outcome that
                    // cannot change.
                    if (message.IsProbe || Volatile.Read(ref overloaded) != 0)
                        continue;

                    await ProcessUnderCapturedContextAsync(message.Payload).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Response subscription loop failed for subject {Subject}.", subject);
                AsyncResponseDiagnostics.SetError(activity, ex);
                // The settlement itself carries its source: a loop death is a TRANSPORT failure,
                // not a delivered response, and the registration path must tell the two kinds of
                // faulted task apart ATOMICALLY — a side-band flag raced the settlement in both
                // directions (set-then-lose aborted a registration whose response had already
                // been delivered; set-after-win left a window that returned a dead waiter). Only
                // a loop fault that actually WINS the settlement marks the task; a fault that
                // loses to a terminal payload changes nothing. The wrapper never escapes: the
                // public ResponseTask unwraps it back to the original exception.
                if (!tcs.TrySetException(new NatsConsumeLoopException(ex)))
                    _logger.LogWarning(ex, "TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
                await CleanupOnceAsync().ConfigureAwait(false);
            }
        }

        void AcknowledgeWithoutWaiting(NatsInboundResponse message)
        {
            ValueTask reply;
            try
            {
                reply = message.ReplyAsync();
            }
            catch (Exception replyEx)
            {
                reply = ValueTask.FromException(replyEx);
            }

            if (reply.IsCompletedSuccessfully)
                reply.GetAwaiter().GetResult();
            else
                _ = ObserveReplyAsync(reply, subject);
        }

        // NATS.Net dropped an inbound message for this wait: its bounded subscription buffer was
        // full behind the serial consume loop (a slow Until predicate under a flood of responses).
        // The dropped message may have been the terminal one and the publisher was never told
        // otherwise, so nothing may be discarded silently: fault the wait with the overload form of
        // the indeterminate contract (Redis parity) and end it; durable flows restart the step.
        void OnMessagesDropped(int buffered)
        {
            if (Interlocked.Exchange(ref overloaded, 1) != 0)
                return;

            if (!tcs.TrySetException(new AsyncResponseIndeterminateDeliveryException(correlationId, buffered)))
                return; // settled already: the wait is over, and the dropped message changes nothing

            AsyncResponseDiagnostics.SetError(activity, "overloaded", "The wait's bounded response buffer overflowed.");
            AsyncResponseDiagnostics.RecordWaiterOverload("nats");
            // Off the client's event loop: cleanup waits on KV and teardown round trips.
            _ = Task.Run(async () => await CleanupOnceAsync().ConfigureAwait(false));
            SafeLog.Try(() => _logger.LogWarning(
                "Wait for correlationId {CorrelationId} is overloaded: {Buffered} responses were queued behind its serial processing and the NATS client dropped the next one. Faulting it as indeterminate and unsubscribing; the queued responses are discarded with it.",
                correlationId, buffered));
        }

        timeoutRegistration = cancellationTokenSource.Token.Register(() =>
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    _logger.LogWarning("Timed out waiting for response for correlationId {CorrelationId}.", correlationId);
                    AsyncResponseDiagnostics.SetError(activity, "timeout", $"Timed out waiting for response for correlationId {correlationId}.");
                    AsyncResponseDiagnostics.RecordWaiterTimeout("nats");
                    await DrainThenCleanupAsync(
                        new TimeoutException($"Timed out waiting for response for correlationId {correlationId}."))
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // Fire-and-forget: nothing awaits this task, so an escaped fault would vanish.
                    _logger.LogError(ex, "Error handling waiter timeout for correlationId {CorrelationId}.", correlationId);
                }
            });
        });

        // A delivery on the consume loop settled the wait: a response (result or error envelope).
        // Neither a loop death (a TRANSPORT failure, marked by NatsConsumeLoopException) nor an
        // indeterminate fault (the drain's own lapse, or an overload) is a delivered response.
        bool SettledByDelivery()
            => tcs.Task.IsCompletedSuccessfully
               || (tcs.Task.IsFaulted && tcs.Task.Exception!.InnerException is not (NatsConsumeLoopException or AsyncResponseIndeterminateDeliveryException));

        void LogSettledRegistrationFailure(Exception ex)
            => SafeLog.Try(() => _logger.LogWarning(ex,
                "Registration step failed after a delivery settled correlationId {CorrelationId}; returning the completed waiter.",
                correlationId));

        // Registration budget: the resolved waiter timeout. While the NATS connection is
        // reconnecting, subscribe, flush and the KV save all wait for it to reopen (NATS.Net
        // retries the reconnect forever) and the waiter timeout is only armed AFTER registration
        // — so an outage held CreateResponseWaiter for its whole duration, past any caller budget.
        // The subscribe is NOT handed this token: NATS.Net binds a subscribe token to the
        // subscription's lifetime, so a registration token there would end the live subscription
        // the moment the budget lapsed. It is bounded from outside instead.
        var lifetimeToken = subscriptionLifetime.Token;
        using var registrationBudget = new CancellationTokenSource(timeout.Value, _timeProvider);
        using var registrationCancellation = CancellationTokenSource.CreateLinkedTokenSource(lifetimeToken, registrationBudget.Token);
        Task<INatsChannelSubscription>? pendingSubscribe = null;

        try
        {
            pendingSubscribe = _client.SubscribeAsync(subject, OnMessagesDropped, lifetimeToken);
            subscription = await pendingSubscribe.WaitAsync(registrationBudget.Token).ConfigureAwait(false);
            consumeLoop = Task.Run(() => ConsumeLoopAsync(subscription));

            // Round-trip to the server so the subscription is guaranteed registered BEFORE the
            // recovery state is saved (and before the caller's trigger publishes the remote
            // request — closing the subscribe/trigger race). The order is the DB channels'
            // invariant: "recovery state visible ⇒ subscription visible". Saved first, a publish
            // landing in the window found the registration, probed the not-yet-visible
            // subscription, and consumed a live waiter's recovery arm — the waiter then resumed
            // twice (recovery callback now, live delivery to its timeout). Skipped once cleanup
            // started: the wait already settled terminally, so there is no trigger race left to
            // close — and cleanup may already have torn the subscription down (or backstop-
            // cancelled the lifetime this flush is linked to), so attempting it would be for nothing.
            if (Volatile.Read(ref cleanupStarted) == 0)
                await _client.FlushAsync(registrationCancellation.Token).ConfigureAwait(false);

            var recoveryState = new RecoveryState
            {
                RegistrationId = registrationId,
                ResumeCallback = resumeCallback,
                FailureCallback = failureCallback,
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(T).FullName,
                // The engine's clock, not the ambient one. The watchdog judges staleness as
                // "utcNow - RegisteredAtUtc" from whichever host scans, so an unsubstitutable
                // app-clock stamp made a skewed host's registrations either never age (skew ahead:
                // a genuinely stuck flow stays invisible and the health check stays green) or age
                // instantly (skew behind: healthy waits page the operator every scan). The DB
                // channels stamp the SERVER clock for exactly this reason; this at least puts the
                // stamp and the watchdog's "now" on one substitutable clock, and matches the
                // ExpiresAtUtc the recovery store writes for the same registration.
                RegisteredAtUtc = _timeProvider.GetUtcNow().UtcDateTime,
                Context = _propagation.Capture()
            };
            await _recoveryStateStore.SaveAsync(correlationId, recoveryState, _options.RecoveryStateExpiry, registrationCancellation.Token).ConfigureAwait(false);
            if (Volatile.Read(ref cleanupStarted) != 0)
            {
                // A terminal delivery on the already-running consume loop started cleanup while
                // this registration was still being written: cleanup's delete ran before the save
                // committed, so the save just orphaned a callback-armed registration that would
                // resurrect recovery for a wait that already reached a terminal state. Compensate
                // with a second delete (mirrors the in-memory channel's post-save check).
                // Best-effort: TTL and the watchdog back a failed delete.
                try
                {
                    await _recoveryStateStore.TryDeleteAsync(correlationId, registrationId).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Post-save recovery-state compensation delete failed for correlationId {CorrelationId}; the registration remains until TTL.", correlationId);
                }
            }

            _logger.LogDebug("Subscribed to subject {Subject} for correlationId {CorrelationId}.", subject, correlationId);
        }
        catch (Exception ex) when (SettledByDelivery())
        {
            // The wait already settled: a delivery on the consume loop completed the waiter while
            // this registration step was still in flight (cleanup marks cleanupStarted just after
            // setting the task, so the task is the race-free signal), and the step then failed
            // against the torn-down registration state (a failed save, a flush aborted by the
            // disposed lifetime source). The response in hand outranks the builder's
            // "throw so the trigger never fires" contract — rethrowing would discard a delivered
            // response, the exact loss this library exists to prevent, and the success path for
            // this same interleaving already returns the completed waiter. Cleanup runs on the
            // delivery path, so nothing is leaked; a save that still committed is compensated
            // above or expires via TTL, with the recovery watchdog behind it. The filter demands
            // an actual settlement (result or fault): a canceled task means NO response was
            // delivered — e.g. a future channel-wide teardown canceling in-flight registrations —
            // and takes the rethrow path below. Logged guarded: a throwing logging provider must
            // not turn the delivered response into a create failure after all.
            LogSettledRegistrationFailure(ex);
        }
        catch (Exception ex) when (registrationBudget.IsCancellationRequested && ex is OperationCanceledException)
        {
            // Clean up first, report second: a throwing logging provider (MEL rethrows provider
            // failures) skipped the lifetime cancel below, and the abandoned subscribe then
            // installed orphan interest after the reconnect — the leak this catch exists to prevent.
            AsyncResponseDiagnostics.SetError(activity, "subscribe_failure", "Registration did not complete within the waiter timeout.");

            if (subscription is null)
            {
                // The abandoned subscribe is still waiting for the connection. Cancel the lifetime
                // token it was handed so it fails instead of installing server-side interest after
                // the reconnect — orphan interest nobody reads, which later publishes would read
                // as "a subscriber received it" and drop. Cleanup only DISPOSES the lifetime source
                // when there is no subscription, and disposing never cancels.
                await DisarmThenCancelSubscriptionTokenAsync().ConfigureAwait(false);
                _ = DisposeLateSubscriptionAsync(pendingSubscribe, subject);
            }

            // Cleanup in the background: its recovery-state delete rides the same connection that
            // just failed to answer, so awaiting it here would hold the caller for the outage
            // after all. TTL and the watchdog back a delete that never lands.
            _ = CleanupAbandonedRegistrationAsync();

            SafeLog.Try(() => _logger.LogError(ex, "Registering the response waiter on subject {Subject} for correlationId {CorrelationId} did not complete within {Timeout}.", subject, correlationId, timeout.Value));

            // Throw so the builder never fires the trigger for a waiter that is not registered.
            throw new TimeoutException(
                $"Registering the NATS response waiter for correlationId {correlationId} did not complete within {timeout.Value}; " +
                "the NATS connection may be unavailable. No remote operation was triggered.",
                ex);
        }
        catch (Exception ex)
        {
            // Clean up first, report second: logging first left the subscription and registration
            // behind a throwing logging provider (MEL rethrows provider failures) — no timer armed,
            // read as a live waiter by the probe, silently consuming the next response for the id.
            AsyncResponseDiagnostics.SetError(activity, "subscribe_failure", ex.Message);
            await DrainThenCleanupAsync(waiterHandedOut: false).ConfigureAwait(false);

            // The drain JOINS the consume loop, so a delivery that was still inside the Until
            // predicate when this step failed (the filter above ran before it could settle) can
            // settle the wait during it. That response outranks the failure, exactly as in the
            // catch above — rethrowing here discarded a response the publisher was told was
            // delivered, with nothing left to recover it.
            if (SettledByDelivery())
            {
                LogSettledRegistrationFailure(ex);
            }
            else
            {
                SafeLog.Try(() => _logger.LogError(ex, "Failed to subscribe to subject {Subject} for correlationId {CorrelationId}.", subject, correlationId));

                // Rethrow instead of returning a pre-faulted waiter: the builder's contract is
                // that the trigger runs only once the subscription AND recovery state exist. A
                // returned waiter would still let the trigger fire the remote operation with no
                // registration left to receive (or recover) its response. Cleanup leaves nothing
                // behind and — no waiter having been handed out — settles the response task
                // Canceled, never faulted, so no unobserved fault lingers.
                throw;
            }
        }

        if (tcs.Task.IsFaulted && tcs.Task.Exception!.InnerException is NatsConsumeLoopException loopFault)
        {
            // The consume loop died AND won the settlement while this registration was in
            // flight: the fault is a TRANSPORT error, not a delivered response; no subscription
            // is live; and the loop's cleanup already ran (deleting any saved recovery state,
            // backed by the post-save compensation). Returning the waiter would let the builder
            // fire the trigger with nothing registered to receive — or recover — its response,
            // so the builder contract applies: throw, and the remote operation never starts. A
            // loop fault that LOST the settlement leaves no mark, so a wait a terminal payload
            // already settled is returned normally.
            throw new InvalidOperationException(
                $"The NATS response subscription for correlationId {correlationId} failed before registration completed.",
                loopFault.InnerException);
        }

        try
        {
            if (Volatile.Read(ref cleanupStarted) == 0)
                cancellationTokenSource.CancelAfter(timeout.Value);
        }
        catch (ObjectDisposedException)
        {
            // A response completed and cleaned up between the check and CancelAfter.
        }

        return new NatsAsyncResponseWaiter<T>(UnwrapConsumeLoopFaults(tcs.Task), () => DrainThenCleanupAsync());
    }

    /// <summary>
    /// Observes an acknowledgement the consume loop did not wait for, so its failure is logged
    /// instead of surfacing as an unobserved task exception.
    /// </summary>
    private async Task ObserveReplyAsync(ValueTask reply, string subject)
    {
        try
        {
            await reply.ConfigureAwait(false);
        }
        catch (Exception replyEx)
        {
            SafeLog.Try(() => _logger.LogDebug(replyEx, "Failed to acknowledge response on subject {Subject}.", subject));
        }
    }

    /// <summary>
    /// Settles the subscribe a lapsed registration budget abandoned. Its lifetime token is already
    /// cancelled, so it normally fails once the connection answers; one that completed anyway is
    /// disposed so its server-side interest ends. Observed either way, so the fault never surfaces
    /// as an unobserved task exception.
    /// </summary>
    private async Task DisposeLateSubscriptionAsync(Task<INatsChannelSubscription>? pending, string subject)
    {
        if (pending is null)
            return;

        try
        {
            var late = await pending.ConfigureAwait(false);
            await late.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Abandoned subscribe for subject {Subject} ended.", subject);
        }
    }

    // ---------------------------------------------------------------------------------------
    // IAsyncResponsePublisher

    /// <inheritdoc/>
    public Task SetResponse<T>(T response, string correlationId, CancellationToken cancellationToken = default) where T : IAsyncResponsePayload
        => SetResponseCore(response, correlationId, cancellationToken);

    Task IRawAsyncResponsePublisher.SetRawResponseJson(string responseJson, string correlationId, CancellationToken cancellationToken)
        => SetRawResponseJsonCore(responseJson, correlationId, cancellationToken);

    private async Task SetResponseCore<T>(T response, string correlationId, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "nats");
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));

        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (CorrelationIdGuard.IsUnpublishable(correlationId, _logger, activity, "the response"))
            return;

        var subject = _subjects.ResponseSubject(correlationId);
        try
        {
            var envelope = new AsyncResponseEnvelope<T> { Success = true, Payload = response };
            var json = AsyncResponseEnvelopeJson.Serialize(envelope);
            var outcome = await _client.RequestAsync(subject, json, probe: false, _options.DeliveryConfirmationTimeout, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.delivery", outcome.ToString());

            if (outcome == NatsDeliveryOutcome.NoResponders)
            {
                // Nobody was listening (the waiter died, e.g. with a redeploy): hand the response over
                // to the lost-subscriber dispatcher, which asks the payload whether to resume or fail.
                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostResponses(
                        _recoveryStateStore,
                        correlationId,
                        response,
                        subject,
                        cancellationToken,
                        hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                    .ConfigureAwait(false);
                if (dispatchResult.RetryLive)
                {
                    // A waiter subscribed between the request and the recovery-state read —
                    // re-attempt the live publish instead of consuming its registration; only a
                    // second no-responders consumes it.
                    outcome = await _client.RequestAsync(subject, json, probe: false, _options.DeliveryConfirmationTimeout, cancellationToken).ConfigureAwait(false);
                    activity?.SetTag("asyncresponse.delivery", outcome.ToString());
                    if (outcome != NatsDeliveryOutcome.NoResponders)
                        return;

                    dispatchResult = await _lostSubscriberDispatcher
                        .DispatchLostResponses(
                            _recoveryStateStore,
                            correlationId,
                            response,
                            subject,
                            cancellationToken,
                            hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                        .ConfigureAwait(false);
                    if (dispatchResult.RetryLive)
                    {
                        // Second contradiction: delivery keeps reporting no responders while the
                        // probe keeps getting an answer from a live subscriber (its interest not
                        // yet visible on the route the delivery took, e.g. still propagating
                        // across a cluster). Consuming registrations on this evidence would strip a
                        // live waiter of its recovery arm — leave all state intact and surface the
                        // non-delivery to the caller, whose retry/redelivery machinery re-attempts
                        // once the subscription is visible (or, once the waiter is gone, the probe
                        // answers no responders too and normal recovery takes over).
                        // Returning here instead would silently drop the payload: the caller
                        // reports success, the broker message is acked, and the response then
                        // exists nowhere.
                        _logger.LogWarning(
                            "Delivery for correlationId {CorrelationId} found no subscribers twice while the liveness probe kept reporting one; recovery registrations are left intact.",
                            correlationId);
                        activity?.SetTag("asyncresponse.recovery.liveness_contradiction", true);
                        throw new InvalidOperationException(
                            $"NATS delivery for correlationId '{correlationId}' found no responders twice while the liveness probe kept " +
                            "reporting a live subscriber; the payload was not delivered and recovery registrations were left intact. Retry " +
                            "the publish once the waiter's subscription is visible to the publishing endpoint.");
                    }
                }

                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.Action, dispatchResult.RouteMixed);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.Action, dispatchResult.CallbackInvoked, dispatchResult.RouteMixed);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
            }
            else if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Published response for correlationId {CorrelationId} on subject {Subject}. PayloadType: {PayloadType}. Outcome: {Outcome}.", correlationId, subject, typeof(T), outcome);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish response for correlationId {CorrelationId} on subject {Subject}.", correlationId, subject);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    private async Task SetRawResponseJsonCore(string responseJson, string correlationId, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.ingress.raw_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "nats");

        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (CorrelationIdGuard.IsUnpublishable(correlationId, _logger, activity, "the raw response", dropContractViolations: true))
            return;

        var subject = _subjects.ResponseSubject(correlationId);
        try
        {
            var json = SerializeRawSuccessEnvelope(responseJson);
            var outcome = await _client.RequestAsync(subject, json, probe: false, _options.DeliveryConfirmationTimeout, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.delivery", outcome.ToString());

            if (outcome == NatsDeliveryOutcome.NoResponders)
            {
                var response = new RawJsonResponse(responseJson).DeserializeUntyped();

                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostResponses(
                        _recoveryStateStore,
                        correlationId,
                        response,
                        subject,
                        cancellationToken,
                        hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                    .ConfigureAwait(false);
                if (dispatchResult.RetryLive)
                {
                    // A waiter subscribed between the request and the recovery-state read —
                    // re-attempt the live publish instead of consuming its registration; only a
                    // second no-responders consumes it.
                    outcome = await _client.RequestAsync(subject, json, probe: false, _options.DeliveryConfirmationTimeout, cancellationToken).ConfigureAwait(false);
                    activity?.SetTag("asyncresponse.delivery", outcome.ToString());
                    if (outcome != NatsDeliveryOutcome.NoResponders)
                        return;

                    dispatchResult = await _lostSubscriberDispatcher
                        .DispatchLostResponses(
                            _recoveryStateStore,
                            correlationId,
                            response,
                            subject,
                            cancellationToken,
                            hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                        .ConfigureAwait(false);
                    if (dispatchResult.RetryLive)
                    {
                        // Second contradiction: delivery keeps reporting no responders while the
                        // probe keeps getting an answer from a live subscriber (its interest not
                        // yet visible on the route the delivery took, e.g. still propagating
                        // across a cluster). Consuming registrations on this evidence would strip a
                        // live waiter of its recovery arm — leave all state intact and surface the
                        // non-delivery to the caller, whose retry/redelivery machinery re-attempts
                        // once the subscription is visible (or, once the waiter is gone, the probe
                        // answers no responders too and normal recovery takes over).
                        // Returning here instead would silently drop the payload: the caller
                        // reports success, the broker message is acked, and the response then
                        // exists nowhere.
                        _logger.LogWarning(
                            "Delivery for correlationId {CorrelationId} found no subscribers twice while the liveness probe kept reporting one; recovery registrations are left intact.",
                            correlationId);
                        activity?.SetTag("asyncresponse.recovery.liveness_contradiction", true);
                        throw new InvalidOperationException(
                            $"NATS delivery for correlationId '{correlationId}' found no responders twice while the liveness probe kept " +
                            "reporting a live subscriber; the payload was not delivered and recovery registrations were left intact. Retry " +
                            "the publish once the waiter's subscription is visible to the publishing endpoint.");
                    }
                }

                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.Action, dispatchResult.RouteMixed);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.Action, dispatchResult.CallbackInvoked, dispatchResult.RouteMixed);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
            }
            else if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Published raw response for correlationId {CorrelationId} on subject {Subject}. Outcome: {Outcome}.", correlationId, subject, outcome);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish raw response for correlationId {CorrelationId} on subject {Subject}.", correlationId, subject);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task SetException(Exception exception, string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(exception);

        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_exception", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "nats");
        activity?.SetTag("asyncresponse.exception_type", exception.GetType().FullName ?? exception.GetType().Name);

        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (CorrelationIdGuard.IsUnpublishable(correlationId, _logger, activity, "the exception", exception))
            return;

        var subject = _subjects.ResponseSubject(correlationId);
        try
        {
            var envelope = new AsyncResponseEnvelope<object>
            {
                Success = false,
                ExceptionMessage = exception.Message,
                ExceptionStackTrace = RemoteStackTrace.ForWire(exception.StackTrace, _options.IncludeRemoteStackTrace, _options.MaxRemoteStackTraceLength),
                Payload = null
            };
            var json = AsyncResponseEnvelopeJson.Serialize(envelope);
            var outcome = await _client.RequestAsync(subject, json, probe: false, _options.DeliveryConfirmationTimeout, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.delivery", outcome.ToString());

            if (outcome == NatsDeliveryOutcome.NoResponders)
            {
                // Nobody was listening: exception envelopes always go to the failure callback.
                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostExceptions(
                        _recoveryStateStore,
                        correlationId,
                        exception,
                        subject,
                        cancellationToken,
                        hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                    .ConfigureAwait(false);
                if (dispatchResult.RetryLive)
                {
                    // A waiter subscribed between the request and the recovery-state read —
                    // re-attempt the live publish instead of consuming its registration; only a
                    // second no-responders consumes it.
                    outcome = await _client.RequestAsync(subject, json, probe: false, _options.DeliveryConfirmationTimeout, cancellationToken).ConfigureAwait(false);
                    activity?.SetTag("asyncresponse.delivery", outcome.ToString());
                    if (outcome != NatsDeliveryOutcome.NoResponders)
                        return;

                    dispatchResult = await _lostSubscriberDispatcher
                        .DispatchLostExceptions(
                            _recoveryStateStore,
                            correlationId,
                            exception,
                            subject,
                            cancellationToken,
                            hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                        .ConfigureAwait(false);
                    if (dispatchResult.RetryLive)
                    {
                        // Second contradiction: delivery keeps reporting no responders while the
                        // probe keeps getting an answer from a live subscriber (its interest not
                        // yet visible on the route the delivery took, e.g. still propagating
                        // across a cluster). Consuming registrations on this evidence would strip a
                        // live waiter of its recovery arm — leave all state intact and surface the
                        // non-delivery to the caller, whose retry/redelivery machinery re-attempts
                        // once the subscription is visible (or, once the waiter is gone, the probe
                        // answers no responders too and normal recovery takes over).
                        // Returning here instead would silently drop the payload: the caller
                        // reports success, the broker message is acked, and the response then
                        // exists nowhere.
                        _logger.LogWarning(
                            "Delivery for correlationId {CorrelationId} found no subscribers twice while the liveness probe kept reporting one; recovery registrations are left intact.",
                            correlationId);
                        activity?.SetTag("asyncresponse.recovery.liveness_contradiction", true);
                        throw new InvalidOperationException(
                            $"NATS delivery for correlationId '{correlationId}' found no responders twice while the liveness probe kept " +
                            "reporting a live subscriber; the payload was not delivered and recovery registrations were left intact. Retry " +
                            "the publish once the waiter's subscription is visible to the publishing endpoint.");
                    }
                }

                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
                AsyncResponseDiagnostics.RecordLostSubscriber("exception", action: RecoveryAction.Fail, dispatchResult.CallbackInvoked);
            }
            else if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Published exception response for correlationId {CorrelationId} on subject {Subject}. Outcome: {Outcome}.", correlationId, subject, outcome);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish exception response for correlationId {CorrelationId} on subject {Subject}.", correlationId, subject);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    // ---------------------------------------------------------------------------------------
    // IActiveSubscriberProbe

    /// <inheritdoc/>
    public async ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(correlationId))
            return 0L;

        var subject = _subjects.ResponseSubject(correlationId);
        try
        {
            // NATS Core does not expose exact subscriber counts to clients, so the probe reports
            // presence. Only NoResponders is a definitive zero — the server told us nothing is
            // subscribed. NoReply means the OPPOSITE: interest existed and the ping was delivered,
            // it just was not acked inside PresenceProbeTimeout. That is routine for a LIVE waiter,
            // because the consume loop acks a probe only when it reads it, serially, after the
            // previous message's user Until predicate returns — so any predicate slower than the
            // 2s default made a healthy waiter look dead, which flagged it stale in the watchdog
            // and (worse) let the lost-subscriber dispatcher consume its recovery registration.
            var outcome = await _client.RequestAsync(subject, payload: null, probe: true, _options.PresenceProbeTimeout, cancellationToken).ConfigureAwait(false);
            return outcome switch
            {
                NatsDeliveryOutcome.Replied => 1L,
                NatsDeliveryOutcome.NoResponders => 0L,
                _ => -1L
            };
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to probe active subscribers for subject {Subject}.", subject);
            // Negative = "could not be probed" (the watchdog's unknown-liveness contract): 0 would
            // assert there is definitively no live waiter and flag every over-threshold
            // registration stale during a transient probe outage.
            return -1L;
        }
    }

    /// <summary>
    /// Re-probes waiter liveness for the lost-subscriber dispatcher's snapshot-race re-check,
    /// using the same presence probe the watchdog uses. An unprobeable result THROWS instead of
    /// reading as "no live waiter", so the failure propagates to the publisher's catch and the
    /// publish retries rather than consuming a live waiter's recovery registration (parity with
    /// the DB channels, whose re-check calls the store directly).
    /// </summary>
    private async ValueTask<bool> HasLiveSubscriberAsync(string correlationId, CancellationToken cancellationToken)
    {
        var subscribers = await CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
        if (subscribers < 0)
        {
            throw new InvalidOperationException(
                $"NATS subscriber liveness for correlationId '{correlationId}' could not be probed.");
        }

        return subscribers > 0;
    }

    private static string SerializeRawSuccessEnvelope(string payloadJson)
    {
        JsonSafety.ThrowIfClearlyNotJson(payloadJson);

        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("SchemaVersion", AsyncResponseEnvelopeSchema.Current);
            writer.WriteBoolean("Success", true);
            writer.WritePropertyName("Payload");
            writer.WriteRawValue(payloadJson);
            writer.WriteNull("ExceptionMessage");
            writer.WriteNull("ExceptionStackTrace");
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}

/// <summary>
/// Internal settlement marker: the consume loop faulted the wait (a transport failure — nothing
/// was delivered). Never escapes the channel: registration converts a marked settlement into a
/// thrown registration failure, and the public ResponseTask unwraps it to the original exception.
/// </summary>
internal sealed class NatsConsumeLoopException(Exception inner)
    : Exception("The NATS response subscription loop failed.", inner);
