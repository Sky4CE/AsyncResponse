using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace AsyncResponse;

/// <summary>
/// Result of a lost-subscriber dispatch attempt.
/// </summary>
/// <param name="ShouldResume">
/// The recovery route the payload reported (<see cref="IAsyncResponsePayload.ShouldResumeOnRecovery"/>):
/// <c>true</c> resume, <c>false</c> fail, or <c>null</c> when it could not be classified (no recovery
/// state, missing payload type, null payload, conversion failure) — treated as "do not resume".
/// </param>
/// <param name="CallbackInvoked">
/// <c>true</c> when a callback was invoked successfully — the recovery state is consumed and the
/// caller should delete it.
/// </param>
internal readonly record struct LostSubscriberDispatchResult(bool? ShouldResume, bool CallbackInvoked);

/// <summary>
/// The single decision point of the lost-subscriber fallback: when an async response is published
/// and no subscriber is listening (the original waiter died, e.g. with a redeploy/restart), this
/// dispatcher chooses and invokes the callback persisted in the <see cref="RecoveryState"/>.
/// <para>
/// For payload envelopes (<c>SetResponse</c>) the payload's
/// <see cref="IAsyncResponsePayload.ShouldResumeOnRecovery"/> decides the route: <c>true</c> goes to
/// the resume callback; <c>false</c> (and any unclassifiable payload, conservatively) goes to the
/// failure callback wrapped in an <see cref="AsyncResponseDomainFailureException"/>. For exception
/// envelopes (<c>SetException</c>) the failure callback is always used.
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
    /// <summary>Dispatches a successfully published payload that no subscriber received.</summary>
    public async Task<LostSubscriberDispatchResult> DispatchLostResponse<T>(RecoveryState? recoveryState, T response, string channel)
    {
        // The recovering process has no live Until predicate — the payload itself decides whether
        // this late response resumes the flow or fails it. A null (unclassifiable) verdict is
        // treated conservatively as "do not resume", so a payload that cannot be understood never
        // takes the happy path.
        var shouldResume = recoveryState is null
            ? (bool?)null
            : PayloadRecoveryClassifier.ShouldResume(response, recoveryState.PayloadTypeFullName);

        if (shouldResume != true)
        {
            if (recoveryState is null)
            {
                _logger.LogWarning("No subscribers and no recovery state for channel {Channel}.", channel);
                return new LostSubscriberDispatchResult(shouldResume, false);
            }

            var invoked = await DispatchToFailureCallback(recoveryState, response, channel).ConfigureAwait(false);
            return new LostSubscriberDispatchResult(shouldResume, invoked);
        }

        // shouldResume == true implies recoveryState is non-null (the verdict is null otherwise).
        if (recoveryState!.ResumeCallback == null)
        {
            _logger.LogWarning("No subscribers for channel {Channel}; no resume callback available.", channel);
            return new LostSubscriberDispatchResult(shouldResume, false);
        }

        _logger.LogWarning("No subscribers for channel {Channel}; invoking resume callback.", channel);

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

        _logger.LogInformation("Resume callback invoked for channel {Channel}.", channel);

        return new LostSubscriberDispatchResult(shouldResume, true);
    }

    /// <summary>Dispatches an exception envelope that no subscriber received.</summary>
    public async Task<bool> DispatchLostException(RecoveryState? recoveryState, Exception exception, string channel)
    {
        if (recoveryState?.FailureCallback == null)
        {
            _logger.LogWarning("No subscribers for channel {Channel}; no failure callback available.", channel);
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

        return true;
    }

    /// <summary>
    /// Routes a payload that declined to resume (<see cref="IAsyncResponsePayload.ShouldResumeOnRecovery"/>
    /// returned <c>false</c>, or it could not be classified) to the failure callback, wrapped in an
    /// <see cref="AsyncResponseDomainFailureException"/> — so it takes the same path as a technical
    /// <c>SetException</c>.
    /// </summary>
    private async Task<bool> DispatchToFailureCallback<T>(RecoveryState recoveryState, T response, string channel)
    {
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
            await InvokeAsync(invocation, recoveryState.Context).ConfigureAwait(false);

            _logger.LogInformation("Failure callback invoked for channel {Channel}.", channel);

            return true;
        }
        catch (Exception ex)
        {
            // Deliberately not rethrown: an exception would bubble up to the broker ingress, which
            // reacts with SetException — and that would invoke this same failure callback a second
            // time. The domain failure has already been dispatched.
            _logger.LogError(ex, "Failure callback failed for channel {Channel}.", channel);
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
