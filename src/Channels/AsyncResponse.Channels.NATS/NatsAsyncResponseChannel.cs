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
/// dispatcher, which asks the payload's ShouldResumeOnRecovery and invokes the resume or failure
/// callback.</description></item>
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
            && !AsyncResponsePayloadReflection.OverridesShouldResumeOnRecovery(typeof(T)))
        {
            throw new InvalidOperationException(
                $"Payload type '{typeof(T)}' registers lost-subscriber recovery callbacks on the NATS channel " +
                $"but does not override {nameof(IAsyncResponsePayload)}.{nameof(IAsyncResponsePayload.ShouldResumeOnRecovery)}(). " +
                "Override it to declare which responses resume the flow (return true) versus fail it (return false); " +
                "the durable channel needs this to route a response that arrives after the waiter was lost.");
        }

        // default: first envelope completes the wait
        completionPredicate ??= _ => new ValueTask<bool>(true);

        // Default timeout aligned with the recovery-state expiry: an infinite wait is never
        // meaningful, because once the recovery state expires the correlation id has no recovery
        // anyway. Timing out routes the flow through its normal failure handling instead of
        // leaving it stuck forever.
        timeout ??= _options.DefaultTimeout ?? _options.RecoveryStateExpiry;

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
        // Local: CleanupOnceAsync — disposes the subscription (which ends the consume loop), deletes
        // recovery state, and tears down the timeout, exactly once.
        int cleanupStarted = 0;
        var subscriptionTornDown = false;
        async ValueTask CleanupOnceAsync()
        {
            if (Interlocked.Exchange(ref cleanupStarted, 1) != 0)
                return;

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

                if (subscription is not null)
                    await subscription.DisposeAsync().ConfigureAwait(false);
                subscriptionTornDown = true;
                _logger.LogDebug("Unsubscribed from subject {Subject}.", subject);
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
                await CleanupOnceAsync().ConfigureAwait(false);
            });
        });

        try
        {
            subscription = await _client.SubscribeAsync(subject, cancellationTokenSource.Token).ConfigureAwait(false);
            _ = Task.Run(() => ConsumeLoopAsync(subscription));

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

            // Round-trip to the server so the subscription is guaranteed registered before the caller's
            // trigger publishes the remote request — closing the subscribe/trigger race.
            await _client.FlushAsync(cancellationTokenSource.Token).ConfigureAwait(false);

            _logger.LogDebug("Subscribed to subject {Subject} for correlationId {CorrelationId}.", subject, correlationId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to subscribe to subject {Subject} for correlationId {CorrelationId}.", subject, correlationId);
            AsyncResponseDiagnostics.SetError(activity, "subscribe_failure", ex.Message);
            await CleanupOnceAsync().ConfigureAwait(false);

            // Rethrow instead of returning a pre-faulted waiter: the builder's contract is that
            // the trigger runs only once the subscription AND recovery state exist. A returned
            // waiter would still let the trigger fire the remote operation with no registration
            // left to receive (or recover) its response. Cleanup leaves nothing behind, and the
            // response task is cancelled rather than faulted so no unobserved fault lingers.
            tcs.TrySetCanceled();
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

        return new NatsAsyncResponseWaiter<T>(tcs.Task, CleanupOnceAsync);
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
                        .DispatchLostResponses(_recoveryStateStore, correlationId, response, subject, cancellationToken)
                        .ConfigureAwait(false);
                }

                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.ShouldResume);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.ShouldResume, dispatchResult.CallbackInvoked);
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
                        .DispatchLostResponses(_recoveryStateStore, correlationId, response, subject, cancellationToken)
                        .ConfigureAwait(false);
                }

                AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, dispatchResult.ShouldResume);
                AsyncResponseDiagnostics.RecordLostSubscriber("response", dispatchResult.ShouldResume, dispatchResult.CallbackInvoked);
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
                        .DispatchLostExceptions(_recoveryStateStore, correlationId, exception, subject, cancellationToken)
                        .ConfigureAwait(false);
                }

                activity?.SetTag("asyncresponse.recovery.callback_invoked", dispatchResult.CallbackInvoked);
                AsyncResponseDiagnostics.RecordLostSubscriber("exception", shouldResume: false, dispatchResult.CallbackInvoked);
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
