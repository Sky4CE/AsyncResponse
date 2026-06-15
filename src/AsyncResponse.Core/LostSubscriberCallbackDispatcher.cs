using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace AsyncResponse;

/// <summary>
/// Result of a lost-subscriber dispatch attempt.
/// </summary>
/// <param name="Outcome">
/// The classified domain outcome of the payload, or <c>null</c> when it could not be classified
/// (no recovery state, missing payload type, null payload, conversion failure).
/// </param>
/// <param name="CallbackInvoked">
/// <c>true</c> when a callback was invoked successfully — the recovery state is consumed and the
/// caller should delete it.
/// </param>
internal readonly record struct LostSubscriberDispatchResult(AsyncResponseOutcome? Outcome, bool CallbackInvoked);

/// <summary>
/// The single decision point of the lost-subscriber fallback: when an async response is published
/// and no subscriber is listening (the original waiter died, e.g. with a redeploy/restart), this
/// dispatcher chooses and invokes the callback persisted in the <see cref="RecoveryState"/>.
/// <para>
/// For payload envelopes (<c>SetResponse</c>) the domain outcome of the payload decides the route:
/// <see cref="AsyncResponseOutcome.Succeeded"/>/<see cref="AsyncResponseOutcome.InProgress"/> go to
/// the resume callback; <see cref="AsyncResponseOutcome.Failed"/>/<see cref="AsyncResponseOutcome.Unknown"/>
/// go to the failure callback wrapped in an <see cref="AsyncResponseDomainFailureException"/>;
/// unclassifiable payloads keep the resume routing. For exception envelopes
/// (<c>SetException</c>) the failure callback is always used.
/// </para>
/// <para>
/// The publisher stays a plain transport: it only reports "published, but nobody was listening"
/// and hands over to this dispatcher.
/// </para>
/// </summary>
internal sealed class LostSubscriberCallbackDispatcher(
    IServiceScopeFactory _scopeFactory,
    AsyncResponseContextPropagation _propagation,
    ILogger _logger)
{
    private const string SERVICE_NAME = nameof(LostSubscriberCallbackDispatcher);

    /// <summary>Dispatches a successfully published payload that no subscriber received.</summary>
    public async Task<LostSubscriberDispatchResult> DispatchLostResponse<T>(RecoveryState? recoveryState, T response, string channel)
    {
        const string MethodName = nameof(DispatchLostResponse);

        // A payload delivered through SetResponse is only a transport-level success: it may still
        // describe a failed business state (Status = Error, Success = false, ...). Classify it
        // as the payload type the original waiter registered for, so the flow is resumed only for
        // successful or in-progress responses while failed ones take the failure callback.
        var outcome = recoveryState is null
            ? null
            : PayloadOutcomeClassifier.TryClassify(response, recoveryState.PayloadTypeFullName);

        if (outcome is AsyncResponseOutcome.Failed or AsyncResponseOutcome.Unknown)
        {
            var invoked = await DispatchFailedDomainState(recoveryState!, response, outcome.Value, channel).ConfigureAwait(false);
            return new LostSubscriberDispatchResult(outcome, invoked);
        }

        if (recoveryState?.ResumeCallback == null)
        {
            _logger.LogWarning("{ServiceName}: {MethodName} No subscribers found for channel {Channel}. No resume callback available.",
                SERVICE_NAME, MethodName, channel);
            return new LostSubscriberDispatchResult(outcome, false);
        }

        _logger.LogWarning("{ServiceName}: {MethodName} No subscribers found for channel {Channel}. Invoking resume callback (domain outcome: {Outcome}).",
            SERVICE_NAME, MethodName, channel, outcome?.ToString() ?? "Unclassified");

        var invocation = ReflectionExtensions.ResolveCallback(
            recoveryState.ResumeCallback,
            payload: response,
            exception: null,
            correlationId: recoveryState.CorrelationId
        );

        // Deliberately not wrapped in try/catch: a failing resume propagates to the publisher's
        // caller, which can escalate it through SetException to the failure callback
        // (the ingress does exactly that).
        await InvokeAsync(invocation, recoveryState.Context).ConfigureAwait(false);

        _logger.LogInformation("{ServiceName}: {MethodName} Resume callback invoked for channel {Channel}.",
            SERVICE_NAME, MethodName, channel);

        return new LostSubscriberDispatchResult(outcome, true);
    }

    /// <summary>Dispatches an exception envelope that no subscriber received.</summary>
    public async Task<bool> DispatchLostException(RecoveryState? recoveryState, Exception exception, string channel)
    {
        const string MethodName = nameof(DispatchLostException);

        if (recoveryState?.FailureCallback == null)
        {
            _logger.LogWarning("{ServiceName}: {MethodName} No subscribers found for channel {Channel}. No failure callback available.",
                SERVICE_NAME, MethodName, channel);
            return false;
        }

        _logger.LogWarning("{ServiceName}: {MethodName} No subscribers found for channel {Channel}. Invoking failure callback.",
            SERVICE_NAME, MethodName, channel);

        var invocation = ReflectionExtensions.ResolveCallback(
            recoveryState.FailureCallback,
            payload: null,
            exception: exception,
            correlationId: recoveryState.CorrelationId
        );

        await InvokeAsync(invocation, recoveryState.Context).ConfigureAwait(false);

        _logger.LogInformation("{ServiceName}: {MethodName} Failure callback invoked for channel {Channel}.",
            SERVICE_NAME, MethodName, channel);

        return true;
    }

    /// <summary>
    /// Routes a payload that reported a failed (or unrecognized) domain state to the failure
    /// callback, wrapped in an <see cref="AsyncResponseDomainFailureException"/> — so the failure
    /// takes the same path as a technical <c>SetException</c>.
    /// </summary>
    private async Task<bool> DispatchFailedDomainState<T>(RecoveryState recoveryState, T response, AsyncResponseOutcome outcome, string channel)
    {
        const string MethodName = nameof(DispatchFailedDomainState);

        string? payloadJson = null;
        try
        {
            payloadJson = JsonSerializer.Serialize(response);
        }
        catch (Exception)
        {
            // Ignore serialization failure here; the payload is only attached for diagnostics.
        }

        if (recoveryState.FailureCallback == null)
        {
            _logger.LogError(
                "{ServiceName}: {MethodName} No subscribers found for channel {Channel} and the response reported domain outcome {Outcome}, but no failure callback is available. The response is NOT routed to the resume callback. Payload: {Payload}",
                SERVICE_NAME, MethodName, channel, outcome, payloadJson);
            return false;
        }

        _logger.LogWarning(
            "{ServiceName}: {MethodName} No subscribers found for channel {Channel}. Response reported domain outcome {Outcome}; invoking failure callback. Payload: {Payload}",
            SERVICE_NAME, MethodName, channel, outcome, payloadJson);

        var domainFailure = new AsyncResponseDomainFailureException(
            recoveryState.CorrelationId,
            outcome,
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
            await InvokeAsync(invocation, recoveryState.Context).ConfigureAwait(false);

            _logger.LogInformation("{ServiceName}: {MethodName} Failure callback invoked for channel {Channel} (domain outcome {Outcome}).",
                SERVICE_NAME, MethodName, channel, outcome);

            return true;
        }
        catch (Exception ex)
        {
            // Deliberately not rethrown: an exception would bubble up to the broker ingress,
            // which reacts with SetException — and that would invoke this same failure callback
            // a second time. The domain failure has already been dispatched.
            _logger.LogError(ex, "{ServiceName}: {MethodName} Failure callback failed for channel {Channel} (domain outcome {Outcome}).",
                SERVICE_NAME, MethodName, channel, outcome);
            return false;
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
