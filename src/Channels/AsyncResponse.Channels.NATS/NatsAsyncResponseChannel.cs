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

    /// <summary>Creates a NATS-backed async-response channel.</summary>
    public NatsAsyncResponseChannel(
        IServiceScopeFactory scopeFactory,
        INatsResponseChannelClient client,
        IRecoveryStateStore recoveryStateStore,
        IOptions<NatsAsyncResponseChannelOptions> options,
        AsyncResponseContextPropagation propagation,
        ILogger<NatsAsyncResponseChannel> logger)
    {
        _options = options.Value;
        _options.Validate();
        _client = client;
        _recoveryStateStore = recoveryStateStore;
        _propagation = propagation;
        _subjects = new NatsSubjectSchema(_options.SubjectPrefix);
        _logger = logger;
        _lostSubscriberDispatcher = new LostSubscriberCallbackDispatcher(scopeFactory, propagation, logger);
    }

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

    private async Task<IAsyncResponseWaiter<T>> CreateResponseWaiterCore<T>(
        string correlationId,
        ReflectionCallDto? resumeCallback,
        ReflectionCallDto? failureCallback,
        Func<T, ValueTask<bool>>? completionPredicate,
        TimeSpan? timeout) where T : IAsyncResponsePayload
    {
        if (string.IsNullOrWhiteSpace(correlationId))
            throw new ArgumentNullException(nameof(correlationId), "CorrelationId must not be empty or whitespace.");

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
        var cancellationTokenSource = new CancellationTokenSource();
        CancellationTokenRegistration timeoutRegistration = default;
        INatsChannelSubscription? subscription = null;

        // -------------------------------------------------------------------------
        // Local: CleanupOnceAsync — ends the stream (which ends the consume loop), deletes
        // recovery state, and tears down the timeout, exactly once.
        int cleanupStarted = 0;
        int teardownBudgetSpent = 0;
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
                _logger.LogDebug("Unsubscribed from subject {Subject}.", subject);
            }
            catch (Exception teardownEx)
            {
                _logger.LogError(teardownEx, "Error during cleanup for subject {Subject}.", subject);
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
        async ValueTask DrainThenCleanupAsync()
        {
            if (Volatile.Read(ref cleanupStarted) == 0)
            {
                var drainTimeout = _options.DisposalDrainTimeout;
                // ONE budget for the whole disposal: the latched cleanup below skips its own
                // teardown wait when a drain already spent this budget on the same latched
                // teardown task — a second full wait there made disposal cost double the
                // configured DisposalDrainTimeout.
                Volatile.Write(ref teardownBudgetSpent, 1);
                try
                {
                    using var budget = new CancellationTokenSource(drainTimeout);
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
                catch (Exception drainEx)
                {
                    _logger.LogWarning(
                        "Disposal drain for correlationId {CorrelationId} did not prove settlement within {DrainTimeout}; faulting the waiter as indeterminate.",
                        correlationId, drainTimeout);
                    AsyncResponseDiagnostics.SetError(activity, "indeterminate_delivery", "Disposal drain did not prove settlement.");
                    // A TrySetResult from the late-finishing delivery loses against this and is
                    // dropped; the loop's own cleanup call is a no-op behind the latch. The
                    // non-cancellation exception case is unforeseen infrastructure failure —
                    // settlement is equally unproven there, so it must not fall back to cancel.
                    if (drainEx is not OperationCanceledException)
                        _logger.LogDebug(drainEx, "Disposal drain failed for subject {Subject}.", subject);
                    tcs.TrySetException(new AsyncResponseIndeterminateDeliveryException(correlationId, drainTimeout));
                    await DisarmThenCancelSubscriptionTokenAsync().ConfigureAwait(false);
                }
            }

            await CleanupOnceAsync().ConfigureAwait(false);
        }

        async ValueTask DisarmThenCancelSubscriptionTokenAsync()
        {
            // Disarm the waiter-timeout registration BEFORE the backstop cancel — the cancel
            // would otherwise fire it and stamp a spurious TimeoutException plus a waiter-timeout
            // metric onto a disposal that is not a timeout. Idempotent with the cleanup core's
            // own registration disposal.
            await timeoutRegistration.DisposeAsync().ConfigureAwait(false);
            try
            {
                cancellationTokenSource.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // Cleanup already ran and disposed the source; the loop is ending regardless.
            }
        }

        async Task CleanupCoreAsync()
        {
            Interlocked.Exchange(ref cleanupStarted, 1);

            try
            {
                try
                {
                    // Delete the recovery state BEFORE disposing the subscription. In the reverse
                    // order a publish landing in the window sees "no responders, state present" and
                    // fires a spurious recovery callback for a wait that already reached a terminal
                    // state. In this order the window shows a subscriber that drops the message — a
                    // late or duplicate terminal message is droppable; a resurrected recovery callback
                    // is not.
                    await _recoveryStateStore.TryDeleteAsync(correlationId, registrationId).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // Best-effort: the KV entry expires on its own, and a transient store failure
                    // must not skip the subscription teardown below.
                    _logger.LogError(ex, "Failed to delete recovery state for correlationId {CorrelationId}.", correlationId);
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
                    await EndStreamOnce().WaitAsync(_options.DisposalDrainTimeout).ConfigureAwait(false);
                if (subscription is null)
                    subscriptionTornDown = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during cleanup for subject {Subject}.", subject);
            }
            finally
            {
                await timeoutRegistration.DisposeAsync().ConfigureAwait(false);
                if (!subscriptionTornDown)
                {
                    // DisposeAsync did not complete, so the server-side subscription may still be
                    // pumping messages. Its lifetime is bound to this token (SubscribeAsync received
                    // it), and disposing a CTS never cancels — an explicit cancel is the backstop
                    // that ends the consume loop. Safe only after the timeout registration above is
                    // gone, or the cancel would fire a spurious waiter timeout.
                    cancellationTokenSource.Cancel();
                }

                // A waiter disposed before any terminal signal must not leave ResponseTask pending
                // forever for callers that hold it directly — the timeout died above, so nothing
                // else could ever complete the task. A no-op after a normal completion, timeout,
                // fault, or a delivery drained by DrainThenCleanupAsync.
                tcs.TrySetCanceled();

                cancellationTokenSource.Dispose();
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

                var envelope = JsonSerializer.Deserialize(payload, AsyncResponseEnvelopeJson.TypeInfo<T>());

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

                    _logger.LogWarning("Received error response for correlationId {CorrelationId}: {ErrorMessage}", correlationId, envelope.ExceptionMessage);
                    AsyncResponseDiagnostics.SetError(activity, "remote_failure", remoteFailure.Message);
                    if (!tcs.TrySetException(remoteFailure))
                        _logger.LogWarning(remoteFailure, "TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
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
                    // Ack first so the publisher's request resolves quickly (delivery/liveness confirmed)
                    // even if processing the payload is slow. A failed ack must not abort the wait.
                    try
                    {
                        await message.ReplyAsync().ConfigureAwait(false);
                    }
                    catch (Exception replyEx)
                    {
                        _logger.LogDebug(replyEx, "Failed to acknowledge response on subject {Subject}.", subject);
                    }

                    if (message.IsProbe)
                        continue;

                    await ProcessUnderCapturedContextAsync(message.Payload).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Response subscription loop failed for subject {Subject}.", subject);
                AsyncResponseDiagnostics.SetError(activity, ex);
                if (!tcs.TrySetException(ex))
                    _logger.LogWarning(ex, "TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
                await CleanupOnceAsync().ConfigureAwait(false);
            }
        }

        timeoutRegistration = cancellationTokenSource.Token.Register(() =>
        {
            _ = Task.Run(async () =>
            {
                _logger.LogWarning("Timed out waiting for response for correlationId {CorrelationId}.", correlationId);
                AsyncResponseDiagnostics.SetError(activity, "timeout", $"Timed out waiting for response for correlationId {correlationId}.");
                AsyncResponseDiagnostics.RecordWaiterTimeout("nats");
                tcs.TrySetException(new TimeoutException($"Timed out waiting for response for correlationId {correlationId}."));
                await DrainThenCleanupAsync().ConfigureAwait(false);
            });
        });

        try
        {
            subscription = await _client.SubscribeAsync(subject, cancellationTokenSource.Token).ConfigureAwait(false);
            consumeLoop = Task.Run(() => ConsumeLoopAsync(subscription));

            var recoveryState = new RecoveryState
            {
                RegistrationId = registrationId,
                ResumeCallback = resumeCallback,
                FailureCallback = failureCallback,
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(T).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                Context = _propagation.Capture()
            };
            await _recoveryStateStore.SaveAsync(correlationId, recoveryState, _options.RecoveryStateExpiry).ConfigureAwait(false);
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

            // Round-trip to the server so the subscription is guaranteed registered before the caller's
            // trigger publishes the remote request — closing the subscribe/trigger race. Skipped
            // once cleanup started: the wait already settled terminally, so there is no trigger
            // race left to close — and cleanup's teardown disposes the lifetime source this flush
            // reads its token from, so attempting it would throw for nothing.
            if (Volatile.Read(ref cleanupStarted) == 0)
                await _client.FlushAsync(cancellationTokenSource.Token).ConfigureAwait(false);

            _logger.LogDebug("Subscribed to subject {Subject} for correlationId {CorrelationId}.", subject, correlationId);
        }
        catch (Exception ex) when (tcs.Task.IsCompletedSuccessfully || tcs.Task.IsFaulted)
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
            // and takes the rethrow path below.
            _logger.LogWarning(ex,
                "Registration step failed after a delivery settled correlationId {CorrelationId}; returning the completed waiter.",
                correlationId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to subscribe to subject {Subject} for correlationId {CorrelationId}.", subject, correlationId);
            AsyncResponseDiagnostics.SetError(activity, "subscribe_failure", ex.Message);
            await DrainThenCleanupAsync().ConfigureAwait(false);

            // Rethrow instead of returning a pre-faulted waiter: the builder's contract is that
            // the trigger runs only once the subscription AND recovery state exist. A returned
            // waiter would still let the trigger fire the remote operation with no registration
            // left to receive (or recover) its response. Cleanup leaves nothing behind and cancels
            // the response task rather than faulting it, so no unobserved fault lingers.
            throw;
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

        return new NatsAsyncResponseWaiter<T>(tcs.Task, DrainThenCleanupAsync);
    }

    // ---------------------------------------------------------------------------------------
    // IAsyncResponsePublisher

    /// <inheritdoc/>
    public Task SetResponse<T>(T response, string correlationId, CancellationToken cancellationToken = default) where T : IAsyncResponsePayload
        => SetResponseCore(response, correlationId, cancellationToken);

    Task IRawAsyncResponsePublisher.SetRawResponse(object? response, string correlationId, CancellationToken cancellationToken)
        => SetResponseCore(response, correlationId, cancellationToken);

    Task IRawAsyncResponsePublisher.SetRawResponseJson(string responseJson, string correlationId, CancellationToken cancellationToken)
        => SetRawResponseJsonCore(responseJson, correlationId, cancellationToken);

    private async Task SetResponseCore<T>(T response, string correlationId, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "nats");
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));

        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the response.");
            AsyncResponseDiagnostics.SetError(activity, "correlation_id_null", "CorrelationId is null; cannot publish the response.");
            return;
        }

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
                        // probe keeps reporting a live subscriber (interest not yet visible
                        // server-side, or a stale heartbeat). Consuming registrations on this
                        // evidence would strip a live waiter of its recovery arm — leave all state
                        // intact for the next delivery or the waiter's own lifecycle.
                        _logger.LogWarning(
                            "Delivery for correlationId {CorrelationId} found no subscribers twice while the liveness probe kept reporting one; recovery registrations are left intact.",
                            correlationId);
                        activity?.SetTag("asyncresponse.recovery.liveness_contradiction", true);
                        return;
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

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the raw response.");
            AsyncResponseDiagnostics.SetError(activity, "correlation_id_null", "CorrelationId is null; cannot publish the raw response.");
            return;
        }

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
                        // probe keeps reporting a live subscriber (interest not yet visible
                        // server-side, or a stale heartbeat). Consuming registrations on this
                        // evidence would strip a live waiter of its recovery arm — leave all state
                        // intact for the next delivery or the waiter's own lifecycle.
                        _logger.LogWarning(
                            "Delivery for correlationId {CorrelationId} found no subscribers twice while the liveness probe kept reporting one; recovery registrations are left intact.",
                            correlationId);
                        activity?.SetTag("asyncresponse.recovery.liveness_contradiction", true);
                        return;
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

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            _logger.LogWarning("CorrelationId is null; cannot publish the exception. Exception: {ExceptionMessage}", exception.Message);
            AsyncResponseDiagnostics.SetError(activity, "correlation_id_null", "CorrelationId is null; cannot publish the exception.");
            return;
        }

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
                        // probe keeps reporting a live subscriber (interest not yet visible
                        // server-side, or a stale heartbeat). Consuming registrations on this
                        // evidence would strip a live waiter of its recovery arm — leave all state
                        // intact for the next delivery or the waiter's own lifecycle.
                        _logger.LogWarning(
                            "Delivery for correlationId {CorrelationId} found no subscribers twice while the liveness probe kept reporting one; recovery registrations are left intact.",
                            correlationId);
                        activity?.SetTag("asyncresponse.recovery.liveness_contradiction", true);
                        return;
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
            // presence: a live waiter answers the ping (1), no-responders or no timely answer means
            // none (0). The watchdog only needs "is anyone listening".
            var outcome = await _client.RequestAsync(subject, payload: null, probe: true, _options.PresenceProbeTimeout, cancellationToken).ConfigureAwait(false);
            return outcome == NatsDeliveryOutcome.Replied ? 1L : 0L;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to probe active subscribers for subject {Subject}.", subject);
            return 0L;
        }
    }

    /// <summary>
    /// Re-probes waiter liveness for the lost-subscriber dispatcher's snapshot-race re-check,
    /// using the same presence probe the watchdog uses.
    /// </summary>
    private async ValueTask<bool> HasLiveSubscriberAsync(string correlationId, CancellationToken cancellationToken)
        => await CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false) > 0;

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
