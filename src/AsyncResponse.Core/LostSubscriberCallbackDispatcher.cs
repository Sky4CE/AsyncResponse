using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Text.Json;

namespace AsyncResponse;

/// <summary>
/// Result of a lost-subscriber dispatch attempt.
/// </summary>
/// <param name="Action">
/// The recovery route the payload reported (<see cref="IAsyncResponsePayload.OnRecovery"/>):
/// <see cref="RecoveryAction.Resume"/>, <see cref="RecoveryAction.Fail"/>,
/// <see cref="RecoveryAction.KeepWaiting"/> (non-terminal checkpoint — nothing invoked, the
/// registration stays armed), or <c>null</c> when it could not be classified (no recovery state,
/// missing payload type, null payload, conversion failure) — treated as "do not resume".
/// </param>
/// <param name="CallbackInvoked">
/// <c>true</c> when a callback was invoked successfully — the recovery state is consumed and the
/// caller should delete it.
/// </param>
internal readonly record struct LostSubscriberDispatchResult(RecoveryAction? Action, bool CallbackInvoked)
{
    /// <summary>
    /// <c>true</c> when a live subscriber re-appeared between the caller's empty snapshot and the
    /// recovery-state read. No callback was invoked and no state was consumed — the caller should
    /// re-snapshot and dispatch live.
    /// </summary>
    public bool RetryLive { get; init; }

    /// <summary>
    /// <c>true</c> when shared-correlation registrations legitimately took DIFFERENT routes in
    /// this one dispatch (each registration classifies as the payload type IT registered).
    /// <see cref="Action"/> stays <c>null</c> — no single action describes the aggregate — but
    /// diagnostics report the route as <c>mixed</c> rather than <c>unclassified</c>; each
    /// registration's own dispatch activity carries its true route.
    /// </summary>
    public bool RouteMixed { get; init; }
}

/// <summary>
/// The single decision point of the lost-subscriber fallback: when an async response is published
/// and no subscriber is listening (the original waiter died, e.g. with a redeploy/restart), this
/// dispatcher chooses and invokes the callback persisted in the <see cref="RecoveryState"/>.
/// <para>
/// For payload envelopes (<c>SetResponse</c>) the payload's
/// <see cref="IAsyncResponsePayload.OnRecovery"/> decides the route:
/// <see cref="RecoveryAction.Resume"/> goes to the resume callback;
/// <see cref="RecoveryAction.Fail"/> (and any unclassifiable payload, conservatively) goes to the
/// failure callback wrapped in an <see cref="AsyncResponseDomainFailureException"/>; and
/// <see cref="RecoveryAction.KeepWaiting"/> — a non-terminal checkpoint — invokes nothing and
/// leaves the registration armed for a later response. For exception envelopes
/// (<c>SetException</c>) the failure callback is always used.
/// </para>
/// <para>
/// The chosen callback receives the <em>materialized</em> payload (the registered payload type),
/// not the raw broker JSON — an <c>object</c>-/interface-/base-typed callback parameter must get
/// the concrete instance, or every type guard in the consuming flow silently fails.
/// </para>
/// <para>
/// The publisher stays a plain transport: it only reports "published, but nobody was listening" and
/// hands over to this dispatcher. This decision is independent of the live waiter's <c>Until</c>
/// predicate, which no longer exists once the waiter is lost.
/// </para>
/// </summary>
internal sealed class LostSubscriberCallbackDispatcher(
    IServiceScopeFactory _scopeFactory,
    AsyncResponseContextPropagation _propagation,
    ILogger _logger)
{
    /// <summary>
    /// Loads every recovery registration for <paramref name="correlationId"/> and dispatches a lost
    /// response to each registration's resume/failure callback.
    /// </summary>
    public async Task<LostSubscriberDispatchResult> DispatchLostResponses<T>(
        IRecoveryStateStore recoveryStateStore,
        string correlationId,
        T response,
        string channel,
        CancellationToken cancellationToken,
        Func<ValueTask<bool>>? hasLiveSubscriber = null)
    {
        var recoveryStates = await recoveryStateStore.GetAllAsync(correlationId, cancellationToken).ConfigureAwait(false);

        // A waiter registers its subscription before saving its recovery state, so the snapshot
        // race has two shapes — and the re-check must run before the empty-state early return:
        // a recovery state visible here implies its subscription is visible too, and, inversely, a
        // freshly registered subscription may not have saved its state yet, in which case
        // recoveryStates is empty precisely because the waiter is about to go live. Either way a
        // live subscriber means the "nobody listening" premise was stale — hand the response back
        // for live delivery instead of consuming registrations or dropping it unrecoverably.
        if (hasLiveSubscriber is not null && await hasLiveSubscriber().ConfigureAwait(false))
            return new LostSubscriberDispatchResult(null, false) { RetryLive = true };

        // Classification and callbacks must see the payload exactly as a broker delivery would
        // have carried it, whichever process publishes.
        var wirePayload = WirePayload(response);

        if (recoveryStates.Count == 0)
            return await DispatchLostResponse(null, wirePayload, channel).ConfigureAwait(false);

        var callbackInvoked = false;
        RecoveryAction? action = null;
        var routeSet = false;
        var routeMixed = false;
        ExceptionDispatchInfo? firstException = null;

        foreach (var recoveryState in recoveryStates)
        {
            try
            {
                var result = await DispatchLostResponse(recoveryState, wirePayload, channel).ConfigureAwait(false);
                if (!routeSet)
                {
                    action = result.Action;
                    routeSet = true;
                }
                else if (action != result.Action)
                {
                    routeMixed = true;
                }

                if (!result.CallbackInvoked)
                    continue;

                callbackInvoked = true;
                await DeleteConsumedRegistrationAsync(recoveryStateStore, correlationId, recoveryState.RegistrationId, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
                    throw;

                // Capture rather than re-throw a bare variable so the original throw site's stack
                // trace survives the dispatch to the remaining registrations.
                firstException ??= ExceptionDispatchInfo.Capture(ex);
            }
        }

        firstException?.Throw();

        return new LostSubscriberDispatchResult(routeMixed ? null : action, callbackInvoked) { RouteMixed = routeMixed };
    }

    /// <summary>
    /// Loads every recovery registration for <paramref name="correlationId"/> and dispatches a lost
    /// exception to each registration's failure callback.
    /// </summary>
    public async Task<LostSubscriberDispatchResult> DispatchLostExceptions(
        IRecoveryStateStore recoveryStateStore,
        string correlationId,
        Exception exception,
        string channel,
        CancellationToken cancellationToken,
        Func<ValueTask<bool>>? hasLiveSubscriber = null)
    {
        var recoveryStates = await recoveryStateStore.GetAllAsync(correlationId, cancellationToken).ConfigureAwait(false);

        // Same snapshot-race re-check as DispatchLostResponses, and for the same reason it must
        // precede the empty-state early return: an empty snapshot may mean the waiter registered
        // its subscription but has not saved its recovery state yet. A live subscriber means the
        // exception should be delivered live instead of consumed here.
        if (hasLiveSubscriber is not null && await hasLiveSubscriber().ConfigureAwait(false))
            return new LostSubscriberDispatchResult(RecoveryAction.Fail, false) { RetryLive = true };

        if (recoveryStates.Count == 0)
            return new LostSubscriberDispatchResult(RecoveryAction.Fail, await DispatchLostException(null, exception, channel).ConfigureAwait(false));

        var callbackInvoked = false;
        ExceptionDispatchInfo? firstException = null;

        foreach (var recoveryState in recoveryStates)
        {
            try
            {
                if (!await DispatchLostException(recoveryState, exception, channel).ConfigureAwait(false))
                    continue;

                callbackInvoked = true;
                await DeleteConsumedRegistrationAsync(recoveryStateStore, correlationId, recoveryState.RegistrationId, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
                    throw;

                // Capture rather than re-throw a bare variable so the original throw site's stack
                // trace survives the dispatch to the remaining registrations.
                firstException ??= ExceptionDispatchInfo.Capture(ex);
            }
        }

        firstException?.Throw();

        // Exception envelopes always take the failure route, so the action is fixed at Fail.
        return new LostSubscriberDispatchResult(RecoveryAction.Fail, callbackInvoked);
    }

    /// <summary>Dispatches a successfully published payload that no subscriber received.</summary>
    public async Task<LostSubscriberDispatchResult> DispatchLostResponse<T>(RecoveryState? recoveryState, T response, string channel)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.lost_subscriber.dispatch",
            correlationId: recoveryState?.CorrelationId);
        activity?.SetTag("asyncresponse.lost_subscriber.kind", "response");
        activity?.SetTag("asyncresponse.channel_name", channel);
        if (response is not null)
            AsyncResponseDiagnostics.SetPayloadType(activity, response.GetType());

        try
        {
            // The recovering process has no live Until predicate — the payload itself decides
            // whether this late response resumes the flow, fails it, or is a non-terminal
            // checkpoint to wait past. Classification also MATERIALIZES the payload as the
            // registered type; the chosen callback must receive that instance, never the raw
            // broker JSON (an object-typed parameter otherwise gets a JsonElement and every type
            // guard in the consuming flow silently fails). A null (unclassifiable) verdict is
            // treated conservatively as "do not resume", so a payload that cannot be understood
            // never takes the happy path.
            var classification = recoveryState is null
                ? default
                : PayloadRecoveryClassifier.Classify(response, recoveryState.PayloadTypeFullName);
            var action = classification.Action;
            var callbackPayload = classification.MaterializedPayload ?? (object?)response;
            AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, action);

            if (action == RecoveryAction.KeepWaiting)
            {
                // A non-terminal checkpoint (progress report) with nobody listening: invoking the
                // resume callback here would spawn a worker per checkpoint, and the failure route
                // would fail a flow that is still running — both consume the registration and
                // leave the REAL terminal response with nothing to route against (the flow then
                // deadlocks re-attached to a correlation id nothing can answer). Invoke nothing;
                // the caller keeps the registration armed, bounded by its TTL and visible to the
                // watchdog.
                _logger.LogInformation(
                    "No subscribers for channel {Channel}; payload is a non-terminal checkpoint (KeepWaiting) — recovery registration retained.",
                    channel);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", false);
                return new LostSubscriberDispatchResult(action, false);
            }

            if (action != RecoveryAction.Resume)
            {
                if (recoveryState is null)
                {
                    _logger.LogWarning("No subscribers and no recovery state for channel {Channel}.", channel);
                    activity?.SetTag("asyncresponse.recovery.callback_invoked", false);
                    return new LostSubscriberDispatchResult(action, false);
                }

                var invoked = await DispatchToFailureCallback(recoveryState, callbackPayload, channel, activity).ConfigureAwait(false);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", invoked);
                return new LostSubscriberDispatchResult(action, invoked);
            }

            // action == Resume implies recoveryState is non-null (the verdict is null otherwise).
            if (recoveryState!.ResumeCallback == null)
            {
                // A resumable response with no resume callback registered: the flow cannot
                // proceed, which is exactly what the failure route reports — engage the armed
                // failure callback rather than repeatedly discarding the terminal signal until
                // the registration's TTL. Mirrors the unclassifiable route's conservatism; a
                // registration with NEITHER callback keeps the old warn-and-retain behavior.
                if (recoveryState.FailureCallback != null)
                {
                    _logger.LogWarning("No subscribers for channel {Channel}; payload is resumable but no resume callback is registered — routing to the failure callback.", channel);
                    var fallbackInvoked = await DispatchToFailureCallback(recoveryState, callbackPayload, channel, activity).ConfigureAwait(false);
                    activity?.SetTag("asyncresponse.recovery.callback_invoked", fallbackInvoked);
                    return new LostSubscriberDispatchResult(action, fallbackInvoked);
                }

                _logger.LogWarning("No subscribers for channel {Channel}; no resume callback available.", channel);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", false);
                return new LostSubscriberDispatchResult(action, false);
            }

            _logger.LogWarning("No subscribers for channel {Channel}; invoking resume callback.", channel);

            var invocation = ReflectionExtensions.ResolveCallback(
                recoveryState.ResumeCallback,
                payload: callbackPayload,
                exception: null,
                correlationId: recoveryState.CorrelationId
            );

            // Deliberately not swallowed: a failing resume propagates to the publisher's caller,
            // which can escalate it through SetException to the failure callback (the ingress does
            // exactly that). The catch below only marks the activity before rethrowing.
            await InvokeAsync(invocation, recoveryState.Context).ConfigureAwait(false);

            _logger.LogInformation("Resume callback invoked for channel {Channel}.", channel);
            activity?.SetTag("asyncresponse.recovery.callback_invoked", true);

            return new LostSubscriberDispatchResult(action, true);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>Dispatches an exception envelope that no subscriber received.</summary>
    public async Task<bool> DispatchLostException(RecoveryState? recoveryState, Exception exception, string channel)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.lost_subscriber.dispatch",
            correlationId: recoveryState?.CorrelationId);
        activity?.SetTag("asyncresponse.lost_subscriber.kind", "exception");
        activity?.SetTag("asyncresponse.channel_name", channel);
        activity?.SetTag("asyncresponse.exception_type", exception.GetType().FullName ?? exception.GetType().Name);
        AsyncResponseDiagnostics.SetLostSubscriberRoute(activity, RecoveryAction.Fail);

        try
        {
            if (recoveryState?.FailureCallback == null)
            {
                _logger.LogWarning("No subscribers for channel {Channel}; no failure callback available.", channel);
                activity?.SetTag("asyncresponse.recovery.callback_invoked", false);
                return false;
            }

            _logger.LogWarning("No subscribers for channel {Channel}; invoking failure callback.", channel);

            var invocation = ReflectionExtensions.ResolveCallback(
                recoveryState.FailureCallback,
                payload: null,
                exception: exception,
                correlationId: recoveryState.CorrelationId
            );

            await InvokeAsync(invocation, recoveryState.Context).ConfigureAwait(false);

            _logger.LogInformation("Failure callback invoked for channel {Channel}.", channel);
            activity?.SetTag("asyncresponse.recovery.callback_invoked", true);

            return true;
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>
    /// Routes a payload that declined to resume (<see cref="IAsyncResponsePayload.OnRecovery"/>
    /// returned <see cref="RecoveryAction.Fail"/>, or it could not be classified) to the failure
    /// callback, wrapped in an <see cref="AsyncResponseDomainFailureException"/> — so it takes the
    /// same path as a technical <c>SetException</c>. <paramref name="response"/> is the
    /// materialized payload when classification succeeded, the raw one otherwise.
    /// </summary>
    private async Task<bool> DispatchToFailureCallback(RecoveryState recoveryState, object? response, string channel, Activity? activity)
    {
        string? payloadJson = null;
        try
        {
            // The payload arrives as its wire representation (declared-type-normalized JSON or
            // raw ingress JSON); reuse it verbatim for diagnostics rather than re-serializing.
            payloadJson = response switch
            {
                string s => s,
                JsonElement je => je.GetRawText(),
                null => AsyncResponseJson.Serialize(response),
                _ => AsyncResponseJson.Serialize(response, response.GetType())
            };
        }
        catch (Exception)
        {
            // Ignore serialization failure here; the payload is only attached for diagnostics.
            // Under trimmed/AOT deployments this also covers payload types without registered
            // JSON metadata — the callback still fires, just without the diagnostic JSON.
        }

        if (recoveryState.FailureCallback == null)
        {
            _logger.LogError("No subscribers for channel {Channel} and the response declined to resume, but no failure callback is available; the response is NOT routed to resume. Payload: {Payload}", channel, payloadJson);
            return false;
        }

        _logger.LogWarning("No subscribers for channel {Channel}; response declined to resume, invoking failure callback. Payload: {Payload}", channel, payloadJson);

        var domainFailure = new AsyncResponseDomainFailureException(
            recoveryState.CorrelationId,
            recoveryState.PayloadTypeFullName,
            payloadJson);

        var invocation = ReflectionExtensions.ResolveCallback(
            recoveryState.FailureCallback,
            payload: response,
            exception: domainFailure,
            correlationId: recoveryState.CorrelationId
        );

        try
        {
            // Bounded in-process retry, mirroring the ingress's transient-fault policy: a failure
            // callback is re-invocable by contract (broker redelivery re-invokes it the same way),
            // and a one-shot invoke turned a transient dependency blip into a silently dropped
            // domain-failure signal that nothing ever revisited (the watchdog is report-only).
            await AsyncResponseRetry.ExecuteAsync(
                async _ =>
                {
                    await InvokeAsync(invocation, recoveryState.Context).ConfigureAwait(false);
                    return true;
                },
                isTransient: static _ => true,
                maxAttempts: 4,
                baseDelay: TimeSpan.FromMilliseconds(250),
                maxDelay: TimeSpan.FromSeconds(2),
                CancellationToken.None).ConfigureAwait(false);

            _logger.LogInformation("Failure callback invoked for channel {Channel}.", channel);

            return true;
        }
        catch (Exception ex)
        {
            // Deliberately not rethrown once the retries are exhausted — keep the swallow. An
            // exception would bubble up to the broker ingress, which reacts with SetException and
            // would invoke this same failure callback a second time; routing it to transport
            // redelivery instead would hot-loop a permanently-throwing callback on RabbitMQ's
            // unbounded default. The domain failure has already been dispatched (and retried
            // above), and the kept recovery row is surfaced by the watchdog's staleness report,
            // so the drop is operator-visible rather than silent.
            AsyncResponseDiagnostics.SetError(activity, ex);
            _logger.LogError(ex, "Failure callback failed for channel {Channel}.", channel);
            return false;
        }
    }

    /// <summary>
    /// Deletes a registration whose callback has already been invoked successfully. Best-effort by
    /// contract: the callback IS the outcome, and a cleanup fault must never be reinterpreted as a
    /// failed response — rethrowing here made the ingress retry the whole delivery (re-invoking
    /// the callback) and then publish the CLEANUP exception through SetException, invoking the
    /// failure callback for a flow whose resume had already succeeded. A failed delete leaves the
    /// registration to its TTL and the watchdog; recovery is at-least-once, so a later delivery
    /// re-invoking the callback is within contract.
    /// </summary>
    private async Task DeleteConsumedRegistrationAsync(
        IRecoveryStateStore recoveryStateStore,
        string correlationId,
        Guid registrationId,
        CancellationToken cancellationToken)
    {
        try
        {
            await recoveryStateStore.TryDeleteAsync(correlationId, registrationId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Recovery callback for correlationId {CorrelationId} succeeded but deleting its registration {RegistrationId} failed; the registration remains until its TTL or the next delivery.",
                correlationId,
                registrationId);
        }
    }

    /// <summary>
    /// Normalizes a typed payload to its WIRE representation — serialized as the publisher's
    /// DECLARED <typeparamref name="T"/>, exactly as <c>AsyncResponseEnvelope&lt;T&gt;</c> writes
    /// it. Reusing the live instance leaked state that never crosses the wire
    /// (<c>[JsonIgnore]</c>) into recovery routing, and serializing the RUNTIME type dropped the
    /// polymorphic discriminators only the declared-type contract emits — either way the verdict
    /// depended on which side of a serialization boundary the publisher sat. Raw ingress payloads
    /// (<see cref="JsonElement"/> / JSON string) already are wire representations.
    /// </summary>
    private static object? WirePayload<T>(T response)
    {
        if (response is null or JsonElement or string)
            return response;

        try
        {
            return AsyncResponseJson.Serialize(response);
        }
        catch
        {
            // No wire representation exists (unserializable payload — cycles, unregistered AOT
            // metadata). Hand the instance through: the wire-only classifier treats it as
            // unclassifiable, so it takes the conservative failure route with the instance
            // attached — a payload that could never have crossed the wire never resumes a flow.
            return response;
        }
    }

    private async Task InvokeAsync(ReflectionInvocationDto invocation, IReadOnlyDictionary<string, string>? context)
    {
        // The recovery callback may run in a different deployment than the original waiter, so
        // restore any ambient context captured at registration before resolving and invoking it.
        using var contextScope = _propagation.Restore(context);
        await using var serviceScope = _scopeFactory.CreateAsyncScope();
        await serviceScope.ServiceProvider.InvokeAsync(invocation).ConfigureAwait(false);
    }

}
