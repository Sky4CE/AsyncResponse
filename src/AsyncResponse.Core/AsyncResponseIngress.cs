using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace AsyncResponse;

/// <summary>
/// Transport-neutral ingress implementation. Broker/webhook adapters can feed response payloads
/// and worker-job envelopes into this service without depending on a specific response channel.
/// </summary>
internal sealed class AsyncResponseIngress(
    IRawAsyncResponsePublisher _rawPublisher,
    IAsyncResponsePublisher _publisher,
    WorkerJobExecutor _workerJobExecutor,
    AsyncResponseContextPropagation _propagation,
    ILogger<AsyncResponseIngress> _logger,
    TimeProvider? _timeProvider = null,
    IAsyncResponseCallbackAuthorizer? _authorizer = null,
    Microsoft.Extensions.Options.IOptions<AsyncResponseOptions>? _options = null) : IAsyncResponseIngress
{
    /// <inheritdoc />
    public bool IsOverInboundBudget(string messageJson)
        => _options?.Value.MaxInboundMessageChars is { } limit
           && messageJson is not null
           && messageJson.Length > limit;

    /// <summary>
    /// Enforces <see cref="AsyncResponseOptions.MaxInboundMessageChars"/>. Returns <c>true</c> when
    /// the message was rejected, in which case the caller returns cleanly and the transport acks —
    /// see the option's remarks for why an oversized message is dropped rather than redelivered.
    /// Only the LENGTH is logged, never a prefix: an oversized body is still a body.
    /// </summary>
    private bool RejectIfOversized(string messageJson, string route, Activity? activity)
    {
        if (!IsOverInboundBudget(messageJson))
            return false;

        var limit = _options!.Value.MaxInboundMessageChars!.Value;

        _logger.LogError(
            "Ingress received an oversized {Route} message and acknowledged it without dispatch: {PayloadLength} UTF-16 code units exceeds the configured {Limit}.",
            route,
            messageJson.Length,
            limit);
        AsyncResponseDiagnostics.SetError(
            activity,
            "oversized_message",
            $"Inbound {route} message exceeds the configured size budget of {limit} UTF-16 code units.");
        AsyncResponseDiagnostics.RecordOversizedInboundMessage(route);
        return true;
    }

    /// <summary>Handles the delivered message.</summary>
    public async Task HandleResponseMessageAsync(string messageJson, string? correlationId)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.ingress.response",
            ActivityKind.Consumer,
            correlationId);

        // An id extracted from an untrusted broker message is unroutable in two ways — missing
        // outright, or present but outside the portable contract (over-long, or space-padded, which
        // a relational store treats as the SAME key as the trimmed form while the library compares
        // ids ordinally, so storing a payload under it could surface it at another conversation's
        // waiter). Here they get one answer, which is the OPPOSITE of the answer a public publisher
        // gives: deliberately acknowledged, not thrown, because the message can never route and
        // redelivery would retry it forever (RabbitMQ's default MaxDeliveryAttempts = 0 has no cap)
        // or burn dead-letter attempts on brokers that do. Error-level log + counter make the drop
        // loud — every occurrence is a producer-side contract violation. The ACTIVITY carries the
        // routing context (trace id, the id as extracted); nothing about the body is logged, not
        // even a hash of it — see the note on payload metadata below.
        if (RejectIfOversized(messageJson, "response", activity))
            return;

        if (CorrelationIdGuard.IsUnroutable(correlationId, out var unroutable))
        {
            _logger.LogError(
                "Ingress received a response message with an unusable correlation id ({UnroutableReason}); it cannot be routed and is acknowledged without dispatch. Payload: {PayloadLength} UTF-16 code units.",
                unroutable.Description,
                messageJson.Length);
            AsyncResponseDiagnostics.SetError(activity, unroutable.ErrorType, $"Inbound response message has an unusable correlation id: {unroutable.Description}.");
            AsyncResponseDiagnostics.RecordUnroutableResponse();
            return;
        }

        try
        {
            // Correlation id and size, and deliberately nothing derived from the CONTENT. A hash
            // prefix looks like harmless metadata but is a content oracle: it is deterministic, so
            // equal payloads are visibly equal across messages and hosts, and a low-entropy payload
            // (a status enum, a small id, a boolean result) can be confirmed outright by hashing
            // the guesses. Trace and correlation ids already tie an entry to its conversation.
            _logger.LogDebug(
                "Ingress received an inbound response message for {CorrelationId}. Payload: {PayloadLength} UTF-16 code units.",
                correlationId,
                messageJson.Length);

            // A transient infrastructure fault (channel store briefly unreachable, recovery-state
            // read hiccup, resume-callback dependency blip) must not finalize the waiter on the
            // first attempt — that would convert a recoverable response into a permanent business
            // failure. Retry briefly in-process before escalating. Parse failures are excluded:
            // an unparseable message never becomes parseable, so it escalates immediately.
            // Cancellation is excluded from BOTH the retry and the escalation below: it is not a
            // handler failure (a durable flow losing its execution lease mid-dispatch surfaces
            // here as an OperationCanceledException), so it propagates for the transport to
            // NAK/redeliver instead of terminally failing a waiter whose response was never lost.
            // Recovery resume callbacks may be re-invoked by these retries, which matches their
            // contract — broker redelivery re-invokes them the same way.
            await AsyncResponseRetry.ExecuteAsync(
                async _ =>
                {
                    await _rawPublisher.SetRawResponseJson(messageJson, correlationId).ConfigureAwait(false);
                    return true;
                },
                isTransient: static ex => ex is not (System.Text.Json.JsonException or InvalidDataException or OperationCanceledException),
                maxAttempts: 4,
                baseDelay: TimeSpan.FromMilliseconds(250),
                maxDelay: TimeSpan.FromSeconds(2),
                CancellationToken.None,
                _timeProvider).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Ingress failed to process the inbound response message.");
            AsyncResponseDiagnostics.SetError(activity, ex);
            try
            {
                await _publisher.SetException(ex, correlationId).ConfigureAwait(false);
            }
            catch (Exception innerEx)
            {
                _logger.LogError(innerEx, "Ingress failed to publish the exception for the inbound message (original error: {OriginalError}).", ex.Message);

                // Both the publish and the SetException escalation failed, so returning normally
                // would ack a response that now exists nowhere. Propagate instead: the transport's
                // redelivery/dead-letter policy retries the whole pipeline, and the recovery
                // registration stays valid for the redelivered attempt.
                throw;
            }
        }
    }

    /// <summary>Handles the delivered message.</summary>
    public async Task HandleWorkerMessageAsync(string messageJson)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.ingress.worker",
            ActivityKind.Consumer);

        // Before the parse, so an oversized envelope never becomes a DOM.
        if (RejectIfOversized(messageJson, "worker", activity))
            return;

        try
        {
            // The envelope is the WORST thing in the library to log whole: it carries the job's
            // arguments and whatever the context propagators captured (tenant, auth, trace baggage).
            // Size only, so a message that fails to even parse still leaves a trace, then the
            // routing metadata once it has been read.
            _logger.LogDebug("Ingress received a worker job. Payload: {PayloadLength} UTF-16 code units.", messageJson.Length);

            var job = JsonSafety.SafeDeserialize<WorkerJobEnvelope>(messageJson)
                ?? throw new InvalidDataException("Worker message deserialized to null.");
            AsyncResponseDiagnostics.SetCorrelationId(activity, job.CorrelationId);
            AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
            AsyncResponseDiagnostics.SetWorker(activity, job.Call);
            _logger.LogDebug(
                "Ingress worker job for {CorrelationId} targets {Service}.{Method}.",
                job.CorrelationId,
                job.Call?.ServiceInterfaceFullName,
                job.Call?.MethodName);

            // Authorize the target while the envelope is still inert data — BEFORE its propagated
            // context is restored. Both halves of this envelope are attacker-controlled to anyone
            // who can write to the worker transport: Call names the method to run, Context names
            // the ambient identity to run it under. Restoring Context first handed a custom
            // authorizer that consults ambient tenant/principal state the message's own answer to
            // the question it was about to be asked. ReflectionExtensions.InvokeAsync re-checks
            // downstream; this is the ordering, not the only gate.
            ReflectionExtensions.ThrowIfNotAuthorized(
                _authorizer,
                job.Call?.ServiceInterfaceFullName ?? string.Empty,
                job.Call?.MethodName ?? string.Empty);

            // The job crossed a serialization boundary (broker → ingress): restore any ambient
            // context its propagators captured before executing it.
            using (_propagation.Restore(job.Context))
                await _workerJobExecutor.ExecuteAsync(job).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is InvalidDataException or System.Text.Json.JsonException)
        {
            // An envelope NO build can ever parse, which is the same class the response path above
            // acknowledges rather than throws — and for the same reason: redelivery would retry it
            // forever (RabbitMQ's default MaxDeliveryAttempts = 0 has no cap) or burn dead-letter
            // attempts on brokers that do. Error log + counter make the drop loud; every occurrence
            // is a producer-side contract violation.
            //
            // Deliberately NOT the unsupported-schema rejection, which stays a throw: that envelope
            // is well-formed and a NEWER build can read it, so refusing lets it reach one instead
            // of being acknowledged away mid-rolling-deploy.
            _logger.LogError(
                ex,
                "Ingress received a worker envelope it cannot parse; it can never be executed and is acknowledged without dispatch. Payload: {PayloadLength} UTF-16 code units.",
                messageJson.Length);
            AsyncResponseDiagnostics.SetError(activity, ex);
            AsyncResponseDiagnostics.RecordWorkerOutcome("rejected");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ingress worker job execution failed.");
            AsyncResponseDiagnostics.SetError(activity, ex);

            // Propagate: the transport dispatcher owns the retry/dead-letter decision for worker
            // jobs (per its AckMode and MaxDeliveryAttempts). Swallowing here acknowledged failed
            // jobs as successes, which disabled redelivery entirely and left the waiter to burn
            // its full timeout.
            throw;
        }
    }
}
