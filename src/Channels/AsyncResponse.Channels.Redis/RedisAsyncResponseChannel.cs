using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Buffers;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace AsyncResponse.Channels.Redis;

/// <summary>
/// Redis-backed response channel:
/// <list type="bullet">
/// <item><description>Publishes responses to Redis pub/sub channels keyed by correlation id.</description></item>
/// <item><description>Subscribes waiters to those channels with per-channel serialized handling.</description></item>
/// <item><description>Persists <see cref="RecoveryState"/> so responses arriving after the waiter
/// died (e.g. a redeploy) are routed through the lost-subscriber dispatcher, which asks the payload's
/// OnRecovery and invokes the resume or failure callback with the materialized payload
/// (or keeps the registration armed for a checkpoint).</description></item>
/// </list>
/// </summary>
internal sealed class RedisAsyncResponseChannel : IAsyncResponsePublisher, IRawAsyncResponsePublisher, IRecoverableAsyncResponseSubscriber, IActiveSubscriberProbe
{

    private readonly ISubscriber _subscriber;
    private readonly IRedisChannelSubscriber _channelSubscriber;
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly IRecoveryStateStore _recoveryStateStore;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly LostSubscriberCallbackDispatcher _lostSubscriberDispatcher;
    private readonly RedisKeySchema _keys;
    private readonly RedisAsyncResponseOptions _options;
    private readonly ILogger<RedisAsyncResponseChannel> _logger;

    private readonly SerialExecutorRegistry _executors;

    /// <summary>Creates a Redis-backed async-response channel.</summary>
    public RedisAsyncResponseChannel(
        IServiceScopeFactory scopeFactory,
        IConnectionMultiplexer multiplexer,
        IRecoveryStateStore recoveryStateStore,
        IOptions<RedisAsyncResponseOptions> options,
        AsyncResponseContextPropagation propagation,
        ILogger<RedisAsyncResponseChannel> logger,
        IRedisChannelSubscriber? channelSubscriber = null)
    {
        _subscriber = multiplexer.GetSubscriber();
        _channelSubscriber = channelSubscriber ?? new RedisChannelMessageQueueSubscriber(_subscriber);
        _multiplexer = multiplexer;
        _recoveryStateStore = recoveryStateStore;
        _propagation = propagation;
        _options = options.Value;
        _options.ValidateShared(nameof(RedisAsyncResponseOptions));
        _keys = new RedisKeySchema(_options.KeyPrefix);
        _logger = logger;
        _lostSubscriberDispatcher = new LostSubscriberCallbackDispatcher(scopeFactory, propagation, logger);
        _executors = new SerialExecutorRegistry(logger);
    }

    // ---------------------------------------------------------------------------------------
    // IAsyncResponseSubscriber / IRecoverableAsyncResponseSubscriber

    /// <inheritdoc/>
    public Task<IAsyncResponseWaiter<T>> CreateResponseWaiter<T>(
        string correlationId,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null) where T : IAsyncResponsePayload
        => CreateResponseWaiterCore(
            correlationId,
            resumeCallback: null,
            failureCallback: null,
            completionPredicate,
            timeout);

    /// <inheritdoc/>
    public Task<IAsyncResponseWaiter<T>> CreateRecoverableResponseWaiter<T>(
        string correlationId,
        ReflectionCallDto? resumeCallback = null,
        ReflectionCallDto? failureCallback = null,
        Func<T, ValueTask<bool>>? completionPredicate = null,
        TimeSpan? timeout = null) where T : IAsyncResponsePayload
        => CreateResponseWaiterCore(
            correlationId,
            resumeCallback,
            failureCallback,
            completionPredicate,
            timeout);

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
        // route every recovered response to the failure callback. The in-memory channel, which
        // cannot recover across a process restart, is deliberately not subject to this check.
        if ((resumeCallback is not null || failureCallback is not null)
            && !AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(T)))
        {
            throw new InvalidOperationException(
                $"Payload type '{typeof(T)}' registers lost-subscriber recovery callbacks on the Redis channel " +
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
        // scope) flow into the message handler, which runs on a foreign Redis subscriber thread.
        var capturedContext = ExecutionContext.Capture();
        var channel = _keys.Channel(correlationId);

        var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.wait", correlationId: correlationId);
        activity?.SetTag("asyncresponse.channel", "redis");
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));
        activity?.SetTag("asyncresponse.timeout_seconds", timeout.Value.TotalSeconds);

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Waiting for response on correlationId {CorrelationId} with timeout {Timeout}.", correlationId, timeout.Value);

        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registrationId = Guid.NewGuid();

        // Single-use cancellation token implementing the timeout. The timer is armed only
        // after subscribe + recovery-state save succeeds, but the callback is registered before
        // subscribing so a very fast terminal message can still clean up safely.
        var cancellationTokenSource = new CancellationTokenSource();
        CancellationTokenRegistration timeoutRegistration = default;
        IRedisChannelSubscription? subscription = null;
        var executorRegistered = false;

        // -------------------------------------------------------------------------
        // Local: CleanupOnceAsync
        // Ensures unsubscribe, recovery-state delete, timeout disposal, and executor cleanup
        // happen once no matter whether completion, timeout, or waiter disposal got there first.
        int cleanupStarted = 0;
        var cleanupGate = new object();
        Task? cleanupTask = null;

        // Task-latched so EVERY caller completes only when the one real cleanup has finished —
        // the previous fire-once int latch let a second caller (a disposing waiter racing the
        // timeout) return before the task was settled.
        ValueTask CleanupOnceAsync()
        {
            Task task;
            lock (cleanupGate)
            {
                task = cleanupTask ??= CleanupCoreAsync();
            }

            return task.IsCompletedSuccessfully ? ValueTask.CompletedTask : new ValueTask(task);
        }

        // Dispose-path cleanup: DRAINS the per-channel serial executor before settling. A delivery
        // may be mid Until-predicate holding a claimed terminal message; the marker work item
        // completes only after that in-flight item finished, so by the time cleanup cancels, the
        // task is either settled by the delivery or genuinely undelivered — never a cancellation
        // stealing a consumed response. Must NOT be called from dispatch code (which runs ON the
        // executor): the dispatch-triggered cleanup uses CleanupOnceAsync directly, its task
        // already settled.
        //
        // The drain is bounded by DisposalDrainTimeout — one budget covering marker ADMISSION too
        // (a full bounded queue behind a wedged item blocks the enqueue itself). A lapsed budget
        // must not fall back to the cleanup's cancel: the wedged delivery holds a message already
        // consumed from the stream, and "canceled" would tell a re-attaching caller nothing was
        // delivered. It faults the task with the explicit indeterminate contract instead, routing
        // durable flows to a fresh idempotent restart. A tombstone-suppressed enqueue is the
        // opposite case — the retired executor finished everything it ever admitted, so nothing
        // is in flight and the plain cancel is truthful.
        async ValueTask DrainThenCleanupAsync()
        {
            if (Volatile.Read(ref cleanupStarted) == 0 && executorRegistered)
            {
                var drainTimeout = _options.DisposalDrainTimeout;
                var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                try
                {
                    using var budget = new CancellationTokenSource(drainTimeout);
                    var accepted = await _executors.EnqueueAsync(channel.ToString()!, () =>
                    {
                        drained.TrySetResult();
                        return Task.CompletedTask;
                    }, budget.Token).ConfigureAwait(false);
                    if (accepted)
                        await drained.Task.WaitAsync(budget.Token).ConfigureAwait(false);
                }
                catch (Exception drainEx)
                {
                    // Budget lapse — or an unforeseen drain failure: either way the marker never
                    // ran, so an in-flight delivery cannot be ruled out (only accepted=false
                    // proves the executor finished everything). Settlement unproven means the
                    // cleanup's cancel below would be a false "nothing was delivered" — fault
                    // with the explicit indeterminate contract instead. A TrySetResult from the
                    // late-finishing dispatch loses against this and is dropped; its cleanup
                    // call is a no-op behind the latch.
                    _logger.LogWarning(
                        "Disposal drain for correlationId {CorrelationId} did not prove settlement within {DrainTimeout}; faulting the waiter as indeterminate.",
                        correlationId, drainTimeout);
                    AsyncResponseDiagnostics.SetError(activity, "indeterminate_delivery", "Disposal drain did not prove settlement.");
                    if (drainEx is not OperationCanceledException)
                        _logger.LogDebug(drainEx, "Dispatch drain failed for channel {Channel}.", channel.ToString()!);
                    tcs.TrySetException(new AsyncResponseIndeterminateDeliveryException(correlationId, drainTimeout));
                }
            }

            await CleanupOnceAsync().ConfigureAwait(false);
        }

        async Task CleanupCoreAsync()
        {
            Interlocked.Exchange(ref cleanupStarted, 1);

            // A waiter disposed before any terminal signal must not leave ResponseTask pending
            // forever for callers that hold it directly — the timeout dies with this cleanup, so
            // nothing else could ever complete the task. Cancellation is a no-op after a normal
            // completion, timeout, or fault (and after a delivery drained by DrainThenCleanupAsync).
            tcs.TrySetCanceled();

            try
            {
                try
                {
                    // Delete the recovery state BEFORE unsubscribing. In the reverse order a publish
                    // landing in the window sees "no subscriber, state present" and fires a spurious
                    // recovery callback for a wait that already reached a terminal state. In this
                    // order the window shows a subscriber that drops the message — a late or duplicate
                    // terminal message is droppable; a resurrected recovery callback is not.
                    await _recoveryStateStore.TryDeleteAsync(correlationId, registrationId).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // Best-effort: the state expires on its own, and a transient store failure must
                    // not skip the unsubscribe and executor teardown below.
                    _logger.LogError(ex, "Failed to delete recovery state for correlationId {CorrelationId}.", correlationId);
                }

                try
                {
                    // Bounded like the drain: this latched core is what a disposing waiter awaits
                    // when terminal delivery started cleanup first, so an unbudgeted unsubscribe
                    // would let a wedged client library hold DisposeAsync hostage past
                    // DisposalDrainTimeout. The quiet wrapper logs its own failure — including
                    // one that completes AFTER this wait was abandoned, which previously died as
                    // a TaskScheduler.UnobservedTaskException nobody logged.
                    if (subscription is not null)
                        await UnsubscribeQuietlyAsync(subscription).WaitAsync(_options.DisposalDrainTimeout).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    _logger.LogError(
                        "Unsubscribe for channel {Channel} did not finish within {DisposalDrainTimeout}; abandoning the wait (its outcome is logged when it completes).",
                        channel.ToString()!, _options.DisposalDrainTimeout);
                }
            }
            finally
            {
                // Purely local teardown runs no matter which network call above failed — the
                // cleanup latch is already set, so anything skipped here would leak until process
                // exit.
                if (executorRegistered)
                    _executors.OnSubscriptionRetired(channel.ToString()!);

                // Schedule the disposal on the thread pool; do not await directly to prevent
                // deadlocks with work currently running on the executor.
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await _executors.RemoveAsync(channel.ToString()!).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to retire the executor for channel {Channel}.", channel.ToString()!);
                    }
                });

                await timeoutRegistration.DisposeAsync().ConfigureAwait(false);
                cancellationTokenSource.Dispose();
                activity?.Dispose();
            }
        }

        // Never faults: the unsubscribe outcome is logged HERE, so a teardown outliving the
        // bounded wait above still records its failure instead of surfacing as an unobserved
        // task exception.
        async Task UnsubscribeQuietlyAsync(IRedisChannelSubscription liveSubscription)
        {
            try
            {
                await liveSubscription.DisposeAsync().ConfigureAwait(false);
                _logger.LogDebug("Unsubscribed from channel {Channel}.", channel.ToString()!);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during unsubscribe-once for channel {Channel}.", channel.ToString()!);
            }
        }

        // -------------------------------------------------------------------------
        // Local: ProcessRedisMessageAsync
        // Deserializes and handles a single incoming envelope, completes the TCS when terminal.
        async Task ProcessRedisMessageAsync(RedisChannel messageChannel, RedisValue messageValue)
        {
            _logger.LogDebug("Received message on channel {Channel}.", messageChannel.ToString()!);

            bool finished = false;
            try
            {
                var envelope = JsonSerializer.Deserialize(messageValue.ToString(), AsyncResponseEnvelopeJson.TypeInfo<T>());

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
                    {
                        // Cap on receive too: the publish-side cap only bounds traces we emit, not what
                        // a remote we do not control can push at us.
                        remoteFailure.Data["RemoteStackTrace"] = RemoteStackTrace.Cap(envelope.ExceptionStackTrace, _options.MaxRemoteStackTraceLength);
                    }

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
                _logger.LogError(ex, "Error processing message on channel {Channel} for correlationId {CorrelationId}.", messageChannel.ToString()!, correlationId);

                finished = true;
                AsyncResponseDiagnostics.SetError(activity, ex);
                if (!tcs.TrySetException(ex))
                    _logger.LogWarning(ex, "TaskCompletionSource already completed for correlationId {CorrelationId}.", correlationId);
            }
            finally
            {
                // Unsubscription also happens on dispose, but doing it immediately after the
                // terminal message releases resources sooner.
                if (finished)
                    await CleanupOnceAsync().ConfigureAwait(false);
            }
        }

        // -------------------------------------------------------------------------
        // Local: HandleMessageAsync
        // Receives pub/sub messages from the async subscription and enqueues them on the
        // per-channel executor, awaiting admission so executor backpressure reaches the
        // subscription's message loop instead of blocking a Redis reader thread.
        Task HandleMessageAsync(RedisChannel messageChannel, RedisValue messageValue)
        {
            // The registry coordinates create/enqueue/retire under one lock, so the message is never
            // enqueued onto an executor that is concurrently being torn down (no lost messages) and a
            // correlation-id reused mid-drain never produces two live executors for one channel.
            var enqueue = _executors.EnqueueAsync(
                messageChannel.ToString()!,
                () => ProcessUnderCapturedContextAsync(messageChannel, messageValue));
            return enqueue.IsCompletedSuccessfully ? Task.CompletedTask : enqueue.AsTask();
        }

        // -------------------------------------------------------------------------
        // Local: ProcessUnderCapturedContextAsync
        // Restores the waiter's subscribe-time ExecutionContext (app AsyncLocals: trace, principal,
        // logging scope) plus the correlation id before processing — the Redis subscriber callback
        // runs on a foreign thread-pool thread that never had them.
        Task ProcessUnderCapturedContextAsync(RedisChannel messageChannel, RedisValue messageValue)
        {
            async Task ProcessAsync()
            {
                using var correlationScope = AsyncResponseContext.PushCorrelationId(storedCorrelationId);
                await ProcessRedisMessageAsync(messageChannel, messageValue).ConfigureAwait(false);
            }

            if (capturedContext is null)
                return ProcessAsync();

            Task? task = null;
            ExecutionContext.Run(capturedContext, _ => task = ProcessAsync(), null);
            return task!;
        }

        timeoutRegistration = cancellationTokenSource.Token.Register(() =>
        {
            _ = Task.Run(async () =>
            {
                _logger.LogWarning("Timed out waiting for response for correlationId {CorrelationId}.", correlationId);
                AsyncResponseDiagnostics.SetError(activity, "timeout", $"Timed out waiting for response for correlationId {correlationId}.");
                AsyncResponseDiagnostics.RecordWaiterTimeout("redis");
                tcs.TrySetException(new TimeoutException($"Timed out waiting for response for correlationId {correlationId}."));
                await DrainThenCleanupAsync().ConfigureAwait(false);
            });
        });

        try
        {
            // Register the executor channel BEFORE the server-side SUBSCRIBE completes: the
            // subscriber attaches its message pump inside SubscribeAsync, so deliveries can start
            // before it returns, and on a correlation id reused within the tombstone lifetime the
            // registry would silently drop them as retirement stragglers until this registration
            // is visible. The subscribe-failure path below retires it again.
            _executors.OnSubscriptionRegistered(channel.ToString()!);
            executorRegistered = true;
            subscription = await _channelSubscriber.SubscribeAsync(channel, HandleMessageAsync).ConfigureAwait(false);
            if (Volatile.Read(ref cleanupStarted) != 0)
            {
                // A message pumped inside SubscribeAsync completed the waiter and ran cleanup to
                // the end before this assignment existed — its unsubscribe saw a null
                // subscription and the latched cleanup never re-runs. Compensate here, or the
                // server-side subscription outlives the waiter: NUMSUB and publish keep counting
                // a live waiter, and lost-subscriber recovery is suppressed for this correlation
                // id until process exit. Best-effort: the waiter already holds its response, so a
                // teardown fault must not fail the create.
                try
                {
                    await UnsubscribeQuietlyAsync(subscription).WaitAsync(_options.DisposalDrainTimeout).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Post-registration unsubscribe for channel {Channel} failed; the subscription may linger until process exit.", channel.ToString()!);
                }
            }

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
                // A terminal delivery started cleanup while this registration was still being
                // written: cleanup's delete ran before the save committed, so the save just
                // orphaned a callback-armed registration that would resurrect recovery for a wait
                // that already reached a terminal state. Compensate with a second delete
                // (mirrors the in-memory channel's post-save check). Best-effort: TTL and the
                // watchdog back a failed delete.
                try
                {
                    await _recoveryStateStore.TryDeleteAsync(correlationId, registrationId).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Post-save recovery-state compensation delete failed for correlationId {CorrelationId}; the registration remains until TTL.", correlationId);
                }
            }

            _logger.LogDebug("Subscribed to channel {Channel} for correlationId {CorrelationId}.", channel.ToString()!, correlationId);
        }
        catch (Exception ex) when (tcs.Task.IsCompletedSuccessfully || tcs.Task.IsFaulted)
        {
            // The wait already settled: a delivery completed the waiter while this registration
            // step was still in flight (cleanup marks cleanupStarted just after setting the task,
            // so the task is the race-free signal), and the step — the recovery-state save — then
            // failed. The response in hand outranks the builder's "throw so the trigger never
            // fires" contract: rethrowing would discard a delivered response, the exact loss this
            // library exists to prevent, and the success path for this same interleaving already
            // returns the completed waiter. Cleanup runs on the delivery path, so nothing is
            // leaked; a save that still committed is compensated above or expires via TTL, with
            // the recovery watchdog behind it. The filter demands an actual settlement (result
            // or fault): a canceled task means NO response was delivered — e.g. a future
            // channel-wide teardown canceling in-flight registrations — and takes the rethrow
            // path below.
            _logger.LogWarning(ex,
                "Registration step failed after a delivery settled correlationId {CorrelationId}; returning the completed waiter.",
                correlationId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to subscribe to channel {Channel} for correlationId {CorrelationId}.", channel.ToString()!, correlationId);
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

        return new RedisAsyncResponseWaiter<T>(tcs.Task, DrainThenCleanupAsync);
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

    // Intentionally duplicated with SetRawResponseJsonCore: this publish method is a latency hot
    // path, and earlier shared helper/delegate refactors regressed throughput in benchmarks.
    // Keep the typed Redis path inline unless a benchmark run proves a refactor is free.
    private async Task SetResponseCore<T>(T response, string correlationId, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "redis");
        AsyncResponseDiagnostics.SetPayloadType(activity, typeof(T));

        // When no correlation id is provided, fall back to the ambient context.
        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (CorrelationIdGuard.IsUnpublishable(correlationId, _logger, activity, "the response"))
            return;

        var channel = _keys.Channel(correlationId);
        try
        {
            var envelope = new AsyncResponseEnvelope<T>
            {
                Success = true,
                Payload = response
            };
            var json = AsyncResponseEnvelopeJson.Serialize(envelope);
            long numSubscribers = await _subscriber.PublishAsync(channel, json).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", numSubscribers);

            if (numSubscribers == 0)
            {
                // Nobody was listening (the waiter died, e.g. with a redeploy): hand the response
                // over to the lost-subscriber dispatcher, which asks the payload whether to resume
                // the flow or fail it, and invokes the matching callback.
                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostResponses(
                        _recoveryStateStore,
                        correlationId,
                        response,
                        channel.ToString()!,
                        cancellationToken,
                        hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                    .ConfigureAwait(false);
                if (dispatchResult.RetryLive)
                {
                    // A waiter subscribed between the publish and the recovery-state read —
                    // re-publish live instead of consuming its registration; only a second miss
                    // consumes it.
                    numSubscribers = await _subscriber.PublishAsync(channel, json).ConfigureAwait(false);
                    activity?.SetTag("asyncresponse.subscribers", numSubscribers);
                    if (numSubscribers > 0)
                        return;

                    dispatchResult = await _lostSubscriberDispatcher
                        .DispatchLostResponses(
                            _recoveryStateStore,
                            correlationId,
                            response,
                            channel.ToString()!,
                            cancellationToken,
                            hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                        .ConfigureAwait(false);
                    if (dispatchResult.RetryLive)
                    {
                        // Second contradiction: delivery keeps reporting no responders while the
                        // probe keeps reporting a live subscriber (interest not yet visible
                        // server-side, or a stale heartbeat). Consuming registrations on this
                        // evidence would strip a live waiter of its recovery arm — leave all state
                        // intact and surface the non-delivery to the caller, whose retry/redelivery
                        // machinery re-attempts once the subscription is visible (bounded by the
                        // heartbeat's liveness expiry, after which normal recovery takes over).
                        // Returning here instead would silently drop the payload.
                        _logger.LogWarning(
                            "Delivery for correlationId {CorrelationId} found no subscribers twice while the liveness probe kept reporting one; recovery registrations are left intact.",
                            correlationId);
                        activity?.SetTag("asyncresponse.recovery.liveness_contradiction", true);
                        throw new InvalidOperationException(
                            $"Redis delivery for correlationId '{correlationId}' found no subscribers twice while the liveness probe kept " +
                            "reporting one; the payload was not delivered and recovery registrations were left intact. Retry the publish " +
                            "once the waiter's subscription is visible to the publishing endpoint.");
                    }
                }

                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.Action, dispatchResult.RouteMixed);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.Action, dispatchResult.CallbackInvoked, dispatchResult.RouteMixed);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);

                await _executors.RemoveAsync(channel.ToString()!).ConfigureAwait(false);
            }
            else
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug("Published response for correlationId {CorrelationId} on channel {Channel}. PayloadType: {PayloadType}. Subscribers: {SubscriberCount}.", correlationId, channel.ToString()!, typeof(T), numSubscribers);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish response for correlationId {CorrelationId} on channel {Channel}.", correlationId, channel.ToString()!);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    // Intentionally duplicated with SetResponseCore: raw ingress uses pre-serialized payload JSON
    // and a different lost-subscriber materialization path, so avoiding shared indirection matters.
    private async Task SetRawResponseJsonCore(string responseJson, string correlationId, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.ingress.raw_response", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "redis");

        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (CorrelationIdGuard.IsUnpublishable(correlationId, _logger, activity, "the raw response", dropContractViolations: true))
            return;

        var channel = _keys.Channel(correlationId);
        try
        {
            var json = SerializeRawSuccessEnvelope(responseJson);
            long numSubscribers = await _subscriber.PublishAsync(channel, json).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", numSubscribers);

            if (numSubscribers == 0)
            {
                var response = new RawJsonResponse(responseJson).DeserializeUntyped();

                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostResponses(
                        _recoveryStateStore,
                        correlationId,
                        response,
                        channel.ToString()!,
                        cancellationToken,
                        hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                    .ConfigureAwait(false);
                if (dispatchResult.RetryLive)
                {
                    // A waiter subscribed between the publish and the recovery-state read —
                    // re-publish live instead of consuming its registration; only a second miss
                    // consumes it.
                    numSubscribers = await _subscriber.PublishAsync(channel, json).ConfigureAwait(false);
                    activity?.SetTag("asyncresponse.subscribers", numSubscribers);
                    if (numSubscribers > 0)
                        return;

                    dispatchResult = await _lostSubscriberDispatcher
                        .DispatchLostResponses(
                            _recoveryStateStore,
                            correlationId,
                            response,
                            channel.ToString()!,
                            cancellationToken,
                            hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                        .ConfigureAwait(false);
                    if (dispatchResult.RetryLive)
                    {
                        // Second contradiction: delivery keeps reporting no responders while the
                        // probe keeps reporting a live subscriber (interest not yet visible
                        // server-side, or a stale heartbeat). Consuming registrations on this
                        // evidence would strip a live waiter of its recovery arm — leave all state
                        // intact and surface the non-delivery to the caller, whose retry/redelivery
                        // machinery re-attempts once the subscription is visible (bounded by the
                        // heartbeat's liveness expiry, after which normal recovery takes over).
                        // Returning here instead would silently drop the payload.
                        _logger.LogWarning(
                            "Delivery for correlationId {CorrelationId} found no subscribers twice while the liveness probe kept reporting one; recovery registrations are left intact.",
                            correlationId);
                        activity?.SetTag("asyncresponse.recovery.liveness_contradiction", true);
                        throw new InvalidOperationException(
                            $"Redis delivery for correlationId '{correlationId}' found no subscribers twice while the liveness probe kept " +
                            "reporting one; the payload was not delivered and recovery registrations were left intact. Retry the publish " +
                            "once the waiter's subscription is visible to the publishing endpoint.");
                    }
                }

                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.Action, dispatchResult.RouteMixed);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.Action, dispatchResult.CallbackInvoked, dispatchResult.RouteMixed);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);

                await _executors.RemoveAsync(channel.ToString()!).ConfigureAwait(false);
            }
            else
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug("Published raw response for correlationId {CorrelationId} on channel {Channel}. Subscribers: {SubscriberCount}.", correlationId, channel.ToString()!, numSubscribers);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish raw response for correlationId {CorrelationId} on channel {Channel}.", correlationId, channel.ToString()!);
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task SetException(Exception exception, string correlationId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(exception);

        using var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.set_exception", ActivityKind.Producer);
        activity?.SetTag("asyncresponse.channel", "redis");
        activity?.SetTag("asyncresponse.exception_type", exception.GetType().FullName ?? exception.GetType().Name);

        AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        if (CorrelationIdGuard.IsUnpublishable(correlationId, _logger, activity, "the exception", exception))
            return;

        var channel = _keys.Channel(correlationId);
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
            long numSubscribers = await _subscriber.PublishAsync(channel, json).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.subscribers", numSubscribers);

            if (numSubscribers == 0)
            {
                // Nobody was listening: exception envelopes always go to the failure callback.
                var dispatchResult = await _lostSubscriberDispatcher
                    .DispatchLostExceptions(
                        _recoveryStateStore,
                        correlationId,
                        exception,
                        channel.ToString()!,
                        cancellationToken,
                        hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                    .ConfigureAwait(false);
                if (dispatchResult.RetryLive)
                {
                    // A waiter subscribed between the publish and the recovery-state read —
                    // re-publish live instead of consuming its registration; only a second miss
                    // consumes it.
                    numSubscribers = await _subscriber.PublishAsync(channel, json).ConfigureAwait(false);
                    activity?.SetTag("asyncresponse.subscribers", numSubscribers);
                    if (numSubscribers > 0)
                        return;

                    dispatchResult = await _lostSubscriberDispatcher
                        .DispatchLostExceptions(
                            _recoveryStateStore,
                            correlationId,
                            exception,
                            channel.ToString()!,
                            cancellationToken,
                            hasLiveSubscriber: () => HasLiveSubscriberAsync(correlationId, cancellationToken))
                        .ConfigureAwait(false);
                    if (dispatchResult.RetryLive)
                    {
                        // Second contradiction: delivery keeps reporting no responders while the
                        // probe keeps reporting a live subscriber (interest not yet visible
                        // server-side, or a stale heartbeat). Consuming registrations on this
                        // evidence would strip a live waiter of its recovery arm — leave all state
                        // intact and surface the non-delivery to the caller, whose retry/redelivery
                        // machinery re-attempts once the subscription is visible (bounded by the
                        // heartbeat's liveness expiry, after which normal recovery takes over).
                        // Returning here instead would silently drop the payload.
                        _logger.LogWarning(
                            "Delivery for correlationId {CorrelationId} found no subscribers twice while the liveness probe kept reporting one; recovery registrations are left intact.",
                            correlationId);
                        activity?.SetTag("asyncresponse.recovery.liveness_contradiction", true);
                        throw new InvalidOperationException(
                            $"Redis delivery for correlationId '{correlationId}' found no subscribers twice while the liveness probe kept " +
                            "reporting one; the payload was not delivered and recovery registrations were left intact. Retry the publish " +
                            "once the waiter's subscription is visible to the publishing endpoint.");
                    }
                }

                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
                AsyncResponseDiagnostics.RecordLostSubscriber("exception", action: RecoveryAction.Fail, dispatchResult.CallbackInvoked);

                await _executors.RemoveAsync(channel.ToString()!).ConfigureAwait(false);
            }
            else if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Published exception response for correlationId {CorrelationId} on channel {Channel}. Subscribers: {SubscriberCount}.", correlationId, channel.ToString()!, numSubscribers);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish exception response for correlationId {CorrelationId} on channel {Channel}.", correlationId, channel.ToString()!);
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

        var channel = _keys.Channel(correlationId);

        // Subscriptions live on whichever node the client subscribed through, so the live count is
        // the maximum reported across all connected endpoints. The async server call keeps large
        // watchdog probe sweeps off blocking thread-pool waits, and the per-endpoint token check
        // lets a shutdown abort the sweep between probes.
        long subscribers = 0;
        var probed = false;
        foreach (var endPoint in _multiplexer.GetEndPoints())
        {
            cancellationToken.ThrowIfCancellationRequested();

            var server = _multiplexer.GetServer(endPoint);
            if (!server.IsConnected)
                continue;

            try
            {
                subscribers = Math.Max(subscribers, await server.SubscriptionSubscriberCountAsync(channel).ConfigureAwait(false));
                probed = true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogDebug(ex, "Failed to read subscriber count for channel {Channel}.", channel.ToString()!);
            }
        }

        // Negative = "could not be probed" (the watchdog's unknown-liveness contract): when no
        // endpoint answered, 0 would assert there is definitively no live waiter and flag every
        // over-threshold registration stale during a transient probe outage.
        return probed ? subscribers : -1L;
    }

    /// <summary>
    /// Re-probes waiter liveness for the lost-subscriber dispatcher's snapshot-race re-check,
    /// using the same PUBSUB NUMSUB-based probe the watchdog uses. An unprobeable result THROWS
    /// instead of reading as "no live waiter", so the failure propagates to the publisher's catch
    /// and the publish retries rather than consuming a live waiter's recovery registration
    /// (parity with the DB channels, whose re-check calls the store directly).
    /// </summary>
    private async ValueTask<bool> HasLiveSubscriberAsync(string correlationId, CancellationToken cancellationToken)
    {
        var subscribers = await CountActiveSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);
        if (subscribers < 0)
        {
            throw new InvalidOperationException(
                $"Redis subscriber liveness for correlationId '{correlationId}' could not be probed on any connected endpoint.");
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
