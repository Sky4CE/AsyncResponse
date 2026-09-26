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
    ILogger _logger,
    TimeProvider? _timeProvider = null)
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

        // Nothing to classify or invoke: no wire form is needed (building one serialized the
        // whole payload a second time on every typed publish that found nobody at all, a late
        // duplicate or progress after completion included).
        if (recoveryStates.Count == 0)
            return await DispatchLostResponse(null, response, channel).ConfigureAwait(false);

        // Classification and callbacks must see the payload exactly as a broker delivery would
        // have carried it, whichever process publishes.
        var wirePayload = WirePayload(response);

        var callbackInvoked = false;
        RecoveryAction? action = null;
        var routeSet = false;
        var routeMixed = false;
        List<ExceptionDispatchInfo>? failures = null;

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
                await DeleteConsumedRegistrationAsync(recoveryStateStore, correlationId, recoveryState.RegistrationId).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
                    throw;

                // Capture rather than re-throw a bare variable so the original throw site's stack
                // trace survives the dispatch to the remaining registrations. EVERY failure is
                // kept: settlement below classifies the whole set, not the first one.
                (failures ??= []).Add(ExceptionDispatchInfo.Capture(ex));
            }
        }

        if (failures is not null)
        {
            if (!callbackInvoked)
                ThrowUnsettled(failures, correlationId);

            SettleResidualFailures(failures, correlationId, channel, "response");
        }

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
        List<ExceptionDispatchInfo>? failures = null;

        foreach (var recoveryState in recoveryStates)
        {
            try
            {
                if (!await DispatchLostException(recoveryState, exception, channel).ConfigureAwait(false))
                    continue;

                callbackInvoked = true;
                await DeleteConsumedRegistrationAsync(recoveryStateStore, correlationId, recoveryState.RegistrationId).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
                    throw;

                // Capture rather than re-throw a bare variable so the original throw site's stack
                // trace survives the dispatch to the remaining registrations. EVERY failure is
                // kept: settlement below classifies the whole set, not the first one.
                (failures ??= []).Add(ExceptionDispatchInfo.Capture(ex));
            }
        }

        if (failures is not null)
        {
            if (!callbackInvoked)
                ThrowUnsettled(failures, correlationId);

            SettleResidualFailures(failures, correlationId, channel, "exception");
        }

        // Exception envelopes always take the failure route, so the action is fixed at Fail.
        return new LostSubscriberDispatchResult(RecoveryAction.Fail, callbackInvoked);
    }

    /// <summary>
    /// Rethrows for a fan-out in which NO registration's callback succeeded. A sibling whose
    /// failure-callback ladder was already exhausted (<see cref="RecoveryCallbackFailedException"/>)
    /// wins whatever its position: the ingress passes that shape through untouched, so the
    /// transport redelivers the still-unacknowledged signal to every registration — instead of
    /// the ingress burning its own retry ladder on an earlier sibling's failure and then escalating
    /// through <c>SetException</c>, which re-invokes the very failure callback that just gave up.
    /// Any other transient failure is wrapped for the same redelivery path. Only an entirely
    /// deterministic set is rethrown raw — a resume that can never be wired up — and the ingress
    /// escalates that through <c>SetException</c> on the first attempt: it excludes
    /// <see cref="IsPermanentCallbackFailure"/> from its retry ladder, since no later attempt can
    /// succeed. (The exception route never reaches this with a deterministic failure: its failure
    /// callback's deterministic faults are logged and acknowledged where they happen.)
    /// </summary>
    private static void ThrowUnsettled(List<ExceptionDispatchInfo> failures, string correlationId)
    {
        foreach (var failure in failures)
        {
            if (failure.SourceException is RecoveryCallbackFailedException)
                failure.Throw();
        }

        // No successful sibling does not make a transient resume failure a business failure.
        // In particular, RecoverAsync may have checkpointed the response before its wake-up
        // publish failed. Escalating through SetException then consumes its registration without
        // publishing that wake-up. Preserve the original signal for transport redelivery.
        foreach (var failure in failures)
        {
            if (!IsPermanentCallbackFailure(failure.SourceException))
                throw new RecoveryCallbackFailedException(correlationId, attempts: 1, failure.SourceException);
        }

        failures[0].Throw();
    }

    /// <summary>
    /// Settles a fan-out dispatch in which at least one registration's callback succeeded (and
    /// was consumed) while one or more others failed. Shared-correlation registrations are an
    /// expected shape — a worker that died mid-await leaves its registration beside the
    /// replacement's — and each carries its own delivery guarantee, so the verdict is taken over
    /// the WHOLE set of failures, never the first one alone.
    /// <para>
    /// A <b>deterministic</b> failure (the target is unauthorized, unresolvable, not registered,
    /// or no longer binds) is logged: redelivery cannot fix it and its registration stays for the
    /// watchdog to surface. A <b>transient</b> one — any failure that is not deterministic —
    /// propagates as <see cref="RecoveryCallbackFailedException"/>, which the ingress passes
    /// through untouched (no second retry ladder, no <c>SetException</c> escalation that would
    /// invoke the FAILURE callbacks of registrations whose resume merely blipped), so the
    /// transport redelivers the terminal signal; the consumed registrations are already deleted,
    /// so the redelivery reaches only the registrations that failed. The message is acknowledged
    /// only when EVERY failure was deterministic. Before round 39 the verdict was taken from the
    /// first failure alone: a deterministic failure first in the set hid a transient sibling
    /// behind it, the message was acknowledged, and the transient registration — a valid waiter
    /// whose dependency was briefly down — lost the only copy of its payload, with the outcome
    /// depending on the order the store returned the registrations in.
    /// </para>
    /// </summary>
    private void SettleResidualFailures(List<ExceptionDispatchInfo> failures, string correlationId, string channel, string kind)
    {
        ExceptionDispatchInfo? exhausted = null;
        Exception? transient = null;
        foreach (var failure in failures)
        {
            var residual = failure.SourceException;

            // Already the propagating shape (a sibling's failure-callback ladder was exhausted);
            // its own ladder logged it.
            if (residual is RecoveryCallbackFailedException)
            {
                exhausted ??= failure;
                continue;
            }

            if (IsPermanentCallbackFailure(residual))
            {
                _logger.LogError(
                    residual,
                    "Lost-{Kind} dispatch for correlationId {CorrelationId} on {Channel} failed with a deterministic fault for one registration after another registration's callback succeeded; redelivery cannot fix it, so that registration stays registered for watchdog visibility.",
                    kind,
                    correlationId,
                    channel);
                continue;
            }

            transient ??= residual;
        }

        if (exhausted is not null)
            exhausted.Throw();

        if (transient is null)
        {
            _logger.LogError(
                "Lost-{Kind} dispatch for correlationId {CorrelationId} on {Channel} partially failed with deterministic faults only; the message is acknowledged.",
                kind,
                correlationId,
                channel);
            return;
        }

        _logger.LogError(
            transient,
            "Lost-{Kind} dispatch for correlationId {CorrelationId} on {Channel} partially failed transiently after another registration's callback succeeded; the consumed registrations are deleted, the failed ones stay armed, and the message is left unacknowledged so the transport redelivers it to those registrations alone.",
            kind,
            correlationId,
            channel);
        throw new RecoveryCallbackFailedException(correlationId, attempts: 1, transient);
    }

    /// <summary>Dispatches a successfully published payload that no subscriber received.</summary>
    public async Task<LostSubscriberDispatchResult> DispatchLostResponse<T>(RecoveryState? recoveryState, T response, string channel)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.lost_subscriber.dispatch",
            correlationId: recoveryState?.CorrelationId);
        activity?.SetTag("asyncresponse.lost_subscriber.kind", "response");
        activity?.SetTag("asyncresponse.channel_name", channel);

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
            //
            // A payload type name past the resolution limits is never parsed (it can overflow the
            // stack inside the CLR's type-name parser — see IsWithinResolutionLimits), so it
            // classifies as unresolvable and takes the same conservative route. Said out loud,
            // because unlike a renamed type this is a recovery row nobody's code wrote.
            if (recoveryState?.PayloadTypeFullName is { } payloadTypeName
                && !AsyncResponseTypeResolution.IsWithinResolutionLimits(payloadTypeName))
            {
                _logger.LogError(
                    "The recovery registration for channel {Channel} names a payload type that is not resolved: {PayloadType}. The response is treated as unclassifiable (do not resume).",
                    channel,
                    AsyncResponseTypeResolution.DescribeForDiagnostics(payloadTypeName));
            }

            var classification = recoveryState is null
                ? default
                : PayloadRecoveryClassifier.Classify(response, recoveryState.PayloadTypeFullName);
            var action = classification.Action;
            var callbackPayload = classification.MaterializedPayload ?? (object?)response;
            TagPayloadType(activity, classification.MaterializedPayload, recoveryState, response);
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

                var invoked = await DispatchToFailureCallback(recoveryState, callbackPayload, response, channel, activity).ConfigureAwait(false);
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
                    var fallbackInvoked = await DispatchToFailureCallback(recoveryState, callbackPayload, response, channel, activity).ConfigureAwait(false);
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

            // The outer fan-out settlement wraps transient failures for transport redelivery,
            // including a single failed resume. Infrastructure failure must not become a
            // business-failure callback. This catch only marks the activity before rethrowing.
            await InvokeAsync(invocation, recoveryState.Context).ConfigureAwait(false);

            // The callback ran: a throwing logging provider must not report it as failed (the
            // registration would stay armed and the redelivery would invoke it again).
            SafeLog.Try((Logger: _logger, Channel: channel), static state => state.Logger.LogInformation("Resume callback invoked for channel {Channel}.", state.Channel));
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

            // The same ladder and the same settlement as the response route's failure callback:
            // a deterministic fault (unauthorized, unresolvable, malformed, no longer binding) is
            // logged and acknowledged with the registration kept — this route IS the SetException
            // escalation, so rethrowing it only made the transport redeliver the same fault
            // forever (RabbitMQ's default requeue has no cap) and handed a direct SetException
            // caller an internal exception type.
            var invoked = await InvokeFailureCallbackAsync(recoveryState, payload: null, exception, channel, activity).ConfigureAwait(false);
            activity?.SetTag("asyncresponse.recovery.callback_invoked", invoked);

            return invoked;
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
    /// materialized payload when classification succeeded, the raw one otherwise — what the
    /// callback receives; <paramref name="wirePayload"/> is the payload as the wire carried it,
    /// what <see cref="AsyncResponseDomainFailureException.PayloadJson"/> reports.
    /// </summary>
    private async Task<bool> DispatchToFailureCallback(RecoveryState recoveryState, object? response, object? wirePayload, string channel, Activity? activity)
    {
        string? payloadJson = null;
        try
        {
            // The wire representation (declared-type-normalized JSON or raw ingress JSON), reused
            // verbatim for diagnostics. Re-serializing the MATERIALIZED instance by its runtime
            // type dropped a polymorphic base's discriminator and every wire member the registered
            // type does not declare — PayloadJson then differed from what was published.
            payloadJson = wirePayload switch
            {
                string s => s,
                JsonElement je => je.GetRawText(),
                null => AsyncResponseJson.Serialize(wirePayload),
                // Only a payload with no wire form reaches here (WirePayload's backstop): this
                // attempt fails like its publish-side one did, and is ignored below.
                _ => AsyncResponseJson.Serialize(wirePayload, wirePayload.GetType())
            };
        }
        catch (Exception)
        {
            // Ignore serialization failure here; the payload is only attached for diagnostics.
            // Under trimmed/AOT deployments this also covers payload types without registered
            // JSON metadata — the callback still fires, just without the diagnostic JSON.
        }

        // Size only, never the JSON itself: these run at Error/Warning in production, and the
        // payload body is business data (the same rule the ingress applies — no content, not even
        // a hash). The full JSON still travels on AsyncResponseDomainFailureException.PayloadJson,
        // which deliberately keeps it out of Exception.Message and therefore out of generic
        // exception logging.
        if (recoveryState.FailureCallback == null)
        {
            _logger.LogError("No subscribers for channel {Channel} and the response declined to resume, but no failure callback is available; the response is NOT routed to resume. Payload: {PayloadLength} UTF-16 code units.", channel, payloadJson?.Length ?? 0);
            return false;
        }

        _logger.LogWarning("No subscribers for channel {Channel}; response declined to resume, invoking failure callback. Payload: {PayloadLength} UTF-16 code units.", channel, payloadJson?.Length ?? 0);

        // The type name goes into the exception's MESSAGE, which generic exception logging
        // sweeps up: bounded and escaped like every other quote of a persisted name. An ordinary
        // name passes through unchanged.
        var domainFailure = new AsyncResponseDomainFailureException(
            recoveryState.CorrelationId,
            recoveryState.PayloadTypeFullName is { } registeredTypeName
                ? AsyncResponseTypeResolution.DescribeForDiagnostics(registeredTypeName)
                : null,
            payloadJson);

        return await InvokeFailureCallbackAsync(recoveryState, response, domainFailure, channel, activity).ConfigureAwait(false);
    }

    /// <summary>
    /// Invokes <paramref name="recoveryState"/>'s failure callback and settles the outcome — the
    /// one policy BOTH failure routes share (a response that declined to resume, and an exception
    /// envelope). Returns <c>true</c> once the callback ran; <c>false</c> for a deterministic
    /// fault, which is logged and acknowledged with the registration kept for the watchdog; and
    /// throws <see cref="RecoveryCallbackFailedException"/> when a transient fault outlasted the
    /// in-process ladder, so the transport redelivers.
    /// </summary>
    private async Task<bool> InvokeFailureCallbackAsync(
        RecoveryState recoveryState,
        object? payload,
        Exception exception,
        string channel,
        Activity? activity)
    {
        try
        {
            // Inside the try: a malformed persisted descriptor is a deterministic wiring fault
            // like any other and takes the same log-and-acknowledge settlement below.
            var invocation = ReflectionExtensions.ResolveCallback(
                recoveryState.FailureCallback!,
                payload: payload,
                exception: exception,
                correlationId: recoveryState.CorrelationId
            );

            // Bounded in-process retry, mirroring the ingress's transient-fault policy: a failure
            // callback is re-invocable by contract (broker redelivery re-invokes it the same way),
            // and a one-shot invoke turned a transient dependency blip into a silently dropped
            // domain-failure signal that nothing ever revisited (the watchdog is report-only).
            //
            // Retry only what a retry can fix. A blanket "everything is transient" spent the full
            // ~1.75s backoff ladder on faults that are deterministic by construction — a callback
            // whose target is not registered in DI, or whose persisted type/method no longer
            // resolves, fails identically on attempt 4 — so a single misconfiguration taxed EVERY
            // lost-response dispatch on the publisher's thread and compounded with the transport's
            // own redelivery. Those cases now fail fast and loudly on the first attempt; genuine
            // dependency blips keep the full ladder.
            await AsyncResponseRetry.ExecuteAsync(
                async _ =>
                {
                    await InvokeAsync(invocation, recoveryState.Context).ConfigureAwait(false);
                    return true;
                },
                isTransient: static ex => !IsPermanentCallbackFailure(ex),
                maxAttempts: FailureCallbackAttempts,
                baseDelay: TimeSpan.FromMilliseconds(250),
                maxDelay: TimeSpan.FromSeconds(2),
                CancellationToken.None,
                _timeProvider).ConfigureAwait(false);

            SafeLog.Try((Logger: _logger, Channel: channel), static state => state.Logger.LogInformation("Failure callback invoked for channel {Channel}.", state.Channel));

            return true;
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);

            if (IsPermanentCallbackFailure(ex))
            {
                // Deterministic by construction: the same call fails the same way on every
                // delivery, so redelivery would only burn the transport's attempts (or hot-loop
                // on RabbitMQ's unbounded default). Swallow: the message is acknowledged, the
                // kept recovery row is surfaced by the watchdog's staleness report, and the
                // error log names the misconfiguration to fix.
                SafeLog.Try((Logger: _logger, Error: ex, Channel: channel), static state => state.Logger.LogError(
                    state.Error, "Failure callback for channel {Channel} cannot be invoked (deterministic fault); the message is acknowledged and the registration stays for the watchdog.", state.Channel));
                return false;
            }

            // Transient and exhausted: the response is a TERMINAL signal that, once acknowledged,
            // exists nowhere (the recovery row keeps the callback, not the payload; the watchdog
            // only reports). Propagate as a dedicated type the ingress passes through untouched —
            // no retry (the ladder above already ran) and no SetException escalation (that would
            // only re-invoke this same callback) — so the transport keeps the message for its own
            // bounded redelivery and dead-letter policy. On RabbitMQ's default MaxDeliveryAttempts
            // = 0 that is the documented unlimited requeue any failing handler gets; configure a
            // cap there as for worker jobs.
            SafeLog.Try((Logger: _logger, Error: ex, Channel: channel), static state => state.Logger.LogError(
                state.Error,
                "Failure callback for channel {Channel} failed on all {Attempts} attempts; the message is left unacknowledged for transport redelivery and the registration stays armed.",
                state.Channel,
                FailureCallbackAttempts));
            throw new RecoveryCallbackFailedException(recoveryState.CorrelationId ?? string.Empty, FailureCallbackAttempts, ex);
        }
    }

    /// <summary>In-process invocations of a failure callback per delivery before the delivery is handed back to the transport.</summary>
    internal const int FailureCallbackAttempts = 4;

    /// <summary>
    /// Whether a failed callback invocation is deterministic — the same call will fail the same
    /// way on every attempt, so retrying only burns the backoff ladder on the publish path.
    /// <para>
    /// Narrow on purpose: only faults raised while WIRING UP the call qualify — the target is
    /// unauthorized, its persisted descriptor is malformed, its persisted type no longer resolves,
    /// its service is not registered, or a persisted argument no longer converts to its parameter
    /// (<see cref="CallbackTargetUnresolvableException"/>), or its method no longer binds
    /// (<see cref="MissingMethodException"/>, <see cref="TypeLoadException"/>). A failure thrown by
    /// the callback BODY is never classified here, whatever its type: a handler that throws
    /// <see cref="InvalidOperationException"/> for a transient reason is ordinary application code
    /// and keeps the full retry ladder, which is why the marker type exists rather than a plain
    /// <c>is InvalidOperationException</c> test. The broker ingress consults the same predicate to
    /// escalate such a fault without its own retry ladder.
    /// </para>
    /// </summary>
    internal static bool IsPermanentCallbackFailure(Exception exception)
        => exception is CallbackTargetUnresolvableException
            or MissingMethodException
            or MissingMemberException
            or TypeLoadException;

    /// <summary>
    /// Deletes a registration whose callback has already been invoked successfully. Best-effort by
    /// contract: the callback IS the outcome, and a cleanup fault must never be reinterpreted as a
    /// failed response — rethrowing here made the ingress retry the whole delivery (re-invoking
    /// the callback) and then publish the CLEANUP exception through SetException, invoking the
    /// failure callback for a flow whose resume had already succeeded. A failed delete leaves the
    /// registration to its TTL and the watchdog; recovery is at-least-once, so a later delivery
    /// re-invoking the callback is within contract.
    /// <para>
    /// Deliberately NOT under the publisher's cancellation token: that token scopes the publish's
    /// own I/O (broker send, recovery-state lookup), and once the callback has run the delete is
    /// its bookkeeping. A caller cancelling late — an HTTP <c>RequestAborted</c> while the resume
    /// ran — left the consumed registration armed for its TTL, so the watchdog flagged a resumed
    /// flow as stuck and a retried publish re-invoked the callback. The store client's own
    /// timeouts bound the call, as they bound the channel's cleanup delete.
    /// </para>
    /// </summary>
    private async Task DeleteConsumedRegistrationAsync(
        IRecoveryStateStore recoveryStateStore,
        string correlationId,
        Guid registrationId)
    {
        try
        {
            await recoveryStateStore.TryDeleteAsync(correlationId, registrationId, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Best-effort means best-effort: a throwing logging provider here turned the
            // succeeded callback into a dispatch failure and a redelivery that invoked it again.
            SafeLog.Try((Logger: _logger, Error: ex, CorrelationId: correlationId, RegistrationId: registrationId), static state => state.Logger.LogWarning(
                state.Error,
                "Recovery callback for correlationId {CorrelationId} succeeded but deleting its registration {RegistrationId} failed; the registration remains until its TTL or the next delivery.",
                state.CorrelationId,
                state.RegistrationId));
        }
    }

    /// <summary>
    /// Tags <c>asyncresponse.payload_type</c> with the payload's real type. The dispatch receives
    /// the WIRE form (a JSON string or <see cref="JsonElement"/>), whose runtime type said nothing:
    /// the tag read <c>System.String</c> or <c>System.Text.Json.JsonElement</c> for every lost
    /// response. Prefers the materialized type, then the registration's persisted type name (store
    /// text, so bounded and escaped), then a typed instance; a raw payload with no registration
    /// leaves the tag unset rather than naming its JSON container.
    /// </summary>
    private static void TagPayloadType<T>(Activity? activity, object? materialized, RecoveryState? recoveryState, T response)
    {
        if (activity is null)
            return;

        if (materialized is not null)
            AsyncResponseDiagnostics.SetPayloadType(activity, materialized.GetType());
        else if (recoveryState?.PayloadTypeFullName is { } registeredTypeName)
            activity.SetTag("asyncresponse.payload_type", AsyncResponseTypeResolution.DescribeForDiagnostics(registeredTypeName));
        else if (response is not (null or JsonElement or string))
            AsyncResponseDiagnostics.SetPayloadType(activity, response.GetType());
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
            // metadata). Every bundled channel serializes a typed publish BEFORE dispatching here
            // and throws to the publisher, so this is a backstop for a caller that does not. Hand
            // the instance through: the wire-only classifier treats it as unclassifiable, so it
            // takes the conservative failure route — a payload that could never have crossed the
            // wire never resumes a flow.
            return response;
        }
    }

    private async Task InvokeAsync(ReflectionInvocationDto invocation, IReadOnlyDictionary<string, string>? context)
    {
        await using var serviceScope = _scopeFactory.CreateAsyncScope();

        // Authorize first, on the raw persisted descriptor. Both the callback target and the
        // context carrier come out of the recovery store, so anyone who can write a row there
        // supplies both — and restoring the context first would let the row pick the ambient
        // tenant/principal an authorizer consults to decide whether that same row's target may
        // run. The scope exists by now only to resolve the authorizer; nothing has been invoked.
        ReflectionExtensions.ThrowIfNotAuthorized(
            serviceScope.ServiceProvider.GetService<IAsyncResponseCallbackAuthorizer>(),
            invocation.ServiceInterfaceFullName,
            invocation.MethodName);

        // The recovery callback may run in a different deployment than the original waiter, so
        // restore any ambient context captured at registration before resolving and invoking it.
        using var contextScope = _propagation.Restore(context);
        await serviceScope.ServiceProvider.InvokeAsync(invocation).ConfigureAwait(false);
    }

}
