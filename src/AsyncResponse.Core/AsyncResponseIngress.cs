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
    /// <para>
    /// The drop is decided and recorded before it is logged, and the log line cannot escape (see
    /// <see cref="SafeLog"/>): a logging provider that throws turned the deliberate acknowledge
    /// into an exception, and the transport redelivered the message forever — the loop this
    /// drop-and-ack exists to prevent. The same holds for every drop path below.
    /// </para>
    /// </summary>
    private bool RejectIfOversized(string messageJson, string route, Activity? activity)
    {
        if (!IsOverInboundBudget(messageJson))
            return false;

        var limit = _options!.Value.MaxInboundMessageChars!.Value;

        AsyncResponseDiagnostics.SetError(
            activity,
            "oversized_message",
            $"Inbound {route} message exceeds the configured size budget of {limit} UTF-16 code units.");
        AsyncResponseDiagnostics.RecordOversizedInboundMessage(route);
        SafeLog.Try((Logger: _logger, Route: route, messageJson.Length, Limit: limit), static state => state.Logger.LogError(
            "Ingress received an oversized {Route} message and acknowledged it without dispatch: {PayloadLength} UTF-16 code units exceeds the configured {Limit}.",
            state.Route,
            state.Length,
            state.Limit));
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
            AsyncResponseDiagnostics.SetError(activity, unroutable.ErrorType, $"Inbound response message has an unusable correlation id: {unroutable.Description}.");
            AsyncResponseDiagnostics.RecordUnroutableResponse();
            SafeLog.Try((Logger: _logger, Reason: unroutable.Description, messageJson.Length), static state => state.Logger.LogError(
                "Ingress received a response message with an unusable correlation id ({UnroutableReason}); it cannot be routed and is acknowledged without dispatch. Payload: {PayloadLength} UTF-16 code units.",
                state.Reason,
                state.Length));
            return;
        }

        try
        {
            // Correlation id and size, and deliberately nothing derived from the CONTENT. A hash
            // prefix looks like harmless metadata but is a content oracle: it is deterministic, so
            // equal payloads are visibly equal across messages and hosts, and a low-entropy payload
            // (a status enum, a small id, a boolean result) can be confirmed outright by hashing
            // the guesses. Trace and correlation ids already tie an entry to its conversation.
            // Guarded: inside this try a throwing provider was escalated as the RESPONSE's
            // failure, faulting the waiter with the logger's exception.
            SafeLog.Try((Logger: _logger, CorrelationId: correlationId, messageJson.Length), static state => state.Logger.LogDebug(
                "Ingress received an inbound response message for {CorrelationId}. Payload: {PayloadLength} UTF-16 code units.",
                state.CorrelationId,
                state.Length));

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
            //
            // RecoveryCallbackFailedException is excluded from both as well: the lost-subscriber
            // dispatcher already ran its own ladder against the failure callback, and escalating
            // through SetException would only invoke that same failing callback again. It
            // propagates so the transport redelivers the still-unacknowledged terminal signal.
            //
            // A deterministic callback fault (a resume target that is unauthorized, unresolvable,
            // malformed, or no longer binds) is excluded from the retry only: every attempt
            // re-dispatched and failed identically, so it paid the ~1.75 s ladder on the consumer
            // for nothing before escalating anyway. It escalates at once, like a parse failure.
            //
            // RecoveryStateUnreadableException is excluded from the escalation only: the store
            // holds registrations this build cannot interpret, and the escalation's own dispatch
            // reads them first and throws the same exception again. It propagates untouched so the
            // transport redelivers or dead-letters it — its contract — for a build or operator
            // that can. It still runs the ladder, pointless as a retry as that is: the ladder is
            // what paces each redelivery, and a broker with no delivery cap (RabbitMQ's default)
            // otherwise requeued it at broker speed, while capped ones (Service Bus, Pub/Sub) spent
            // their attempts in milliseconds — dead-lettering it before the newer build a rolling
            // deploy is bringing up could ever receive it.
            await AsyncResponseRetry.ExecuteAsync(
                async _ =>
                {
                    await _rawPublisher.SetRawResponseJson(messageJson, correlationId).ConfigureAwait(false);
                    return true;
                },
                isTransient: static ex => ex is not (System.Text.Json.JsonException or InvalidDataException or OperationCanceledException or RecoveryCallbackFailedException)
                                          && !LostSubscriberCallbackDispatcher.IsPermanentCallbackFailure(ex),
                maxAttempts: 4,
                baseDelay: TimeSpan.FromMilliseconds(250),
                maxDelay: TimeSpan.FromSeconds(2),
                CancellationToken.None,
                _timeProvider).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not (OperationCanceledException or RecoveryCallbackFailedException or RecoveryStateUnreadableException))
        {
            // Guarded: a throwing provider here skipped the escalation, so the waiter was never
            // told and the delivery was redelivered instead.
            SafeLog.Try((Logger: _logger, Error: ex), static state => state.Logger.LogError(state.Error, "Ingress failed to process the inbound response message."));
            AsyncResponseDiagnostics.SetError(activity, ex);
            try
            {
                await _publisher.SetException(ex, correlationId).ConfigureAwait(false);
            }
            catch (Exception innerEx)
            {
                SafeLog.Try((Logger: _logger, Error: innerEx, Original: ex.Message), static state => state.Logger.LogError(
                    state.Error, "Ingress failed to publish the exception for the inbound message (original error: {OriginalError}).", state.Original));

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

        WorkerJobEnvelope job;
        try
        {
            // The envelope is the WORST thing in the library to log whole: it carries the job's
            // arguments and whatever the context propagators captured (tenant, auth, trace baggage).
            // Size only, so a message that fails to even parse still leaves a trace, then the
            // routing metadata once it has been read. Guarded: a throwing provider here escaped
            // the parse filter below and the job was redelivered without ever running.
            SafeLog.Try((Logger: _logger, messageJson.Length), static state => state.Logger.LogDebug(
                "Ingress received a worker job. Payload: {PayloadLength} UTF-16 code units.", state.Length));

            job = JsonSafety.SafeDeserialize<WorkerJobEnvelope>(messageJson)
                ?? throw new InvalidDataException("Worker message deserialized to null.");

            // `required` on WorkerJobEnvelope.Call enforces presence on the wire, not non-null:
            // an explicit "call": null parses successfully yet can never be executed, so it is
            // the same producer-side contract violation as an unparseable envelope.
            if (job.Call is null)
                throw new InvalidDataException("Worker envelope carries a null call description.");

            // Same mechanism one level down: ReflectionCallDto's members are `required` too, so an
            // explicit "params": null (or a null element, or a null or blank target name) parses
            // yet can never resolve to a callback — it must take this drop-and-ack route, not
            // escape as an ArgumentNullException the transport would redeliver forever. The shape
            // rule is shared with the producer and the recovery dispatcher (ReflectionCallDtoGuard).
            if (ReflectionCallDtoGuard.FindDefect(job.Call) is { } defect)
                throw new InvalidDataException($"Worker envelope carries a malformed call description: {defect}.");

            // And beside the call: the reply target's members are `required` strings with the
            // same presence-only guarantee, and the executor validates them the moment it pushes
            // the job's context (whitespace-inclusive, exactly this rule) — outside this filter,
            // so a null or blank member threw on every delivery and was redelivered forever. A
            // null Properties map carries no data: the reply target's init accessor reads it back
            // as an empty map, so a handler never sees null there.
            if (job.ReplyTarget is { } replyTarget
                && (string.IsNullOrWhiteSpace(replyTarget.Name)
                    || string.IsNullOrWhiteSpace(replyTarget.Transport)
                    || string.IsNullOrWhiteSpace(replyTarget.Address)))
            {
                throw new InvalidDataException("Worker envelope carries a reply target with a null or blank member.");
            }
        }
        catch (Exception ex) when (ex is InvalidDataException or System.Text.Json.JsonException)
        {
            // An envelope NO build can ever parse, which is the same class the response path above
            // acknowledges rather than throws — and for the same reason: redelivery would retry it
            // forever (RabbitMQ's default MaxDeliveryAttempts = 0 has no cap) or burn dead-letter
            // attempts on brokers that do. Error log + counter make the drop loud; every occurrence
            // is a producer-side contract violation.
            //
            // This filter must cover ONLY the parse above: the job body can throw the same
            // exception types (a durable flow deserializing a persisted input, a handler parsing a
            // third-party response), and those must propagate below for the transport to
            // redeliver/dead-letter instead of being acknowledged away as a malformed envelope.
            //
            // Deliberately NOT the unsupported-schema rejection, which stays a throw: that envelope
            // is well-formed and a NEWER build can read it, so refusing lets it reach one instead
            // of being acknowledged away mid-rolling-deploy.
            AsyncResponseDiagnostics.SetError(activity, ex);
            AsyncResponseDiagnostics.RecordWorkerOutcome("rejected");
            SafeLog.Try((Logger: _logger, Error: ex, messageJson.Length), static state => state.Logger.LogError(
                state.Error,
                "Ingress received a worker envelope it cannot parse; it can never be executed and is acknowledged without dispatch. Payload: {PayloadLength} UTF-16 code units.",
                state.Length));
            return;
        }

        try
        {
            AsyncResponseDiagnostics.SetCorrelationId(activity, job.CorrelationId);
            AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
            AsyncResponseDiagnostics.SetWorker(activity, job.Call);

            // Stream-written text, logged before anything has validated or authorized it: the
            // names go through the same bounded, escaped quoting as every other persisted name,
            // and the correlation id through the escaped excerpt (it is checked against the
            // portable-id contract only later, by the executor). An ordinary value reads exactly
            // as before; CR/LF or megabytes of text can no longer forge or flood the log line.
            // Guarded: a throwing provider here failed the job before it ever ran.
            SafeLog.Try((Logger: _logger, Job: job), static state =>
            {
                if (state.Logger.IsEnabled(LogLevel.Debug))
                {
                    state.Logger.LogDebug(
                        "Ingress worker job for {CorrelationId} targets {Service}.{Method}.",
                        state.Job.CorrelationId is null ? null : DiagnosticText.EscapedExcerpt(state.Job.CorrelationId, AsyncResponseChannelOptions.MaxCorrelationIdLength),
                        AsyncResponseTypeResolution.DescribeForDiagnostics(state.Job.Call.ServiceInterfaceFullName),
                        DiagnosticText.EscapedExcerpt(state.Job.Call.MethodName, 256));
                }
            });

            // Authorize the target while the envelope is still inert data — BEFORE its propagated
            // context is restored. Both halves of this envelope are attacker-controlled to anyone
            // who can write to the worker transport: Call names the method to run, Context names
            // the ambient identity to run it under. Restoring Context first handed a custom
            // authorizer that consults ambient tenant/principal state the message's own answer to
            // the question it was about to be asked. ReflectionExtensions.InvokeAsync re-checks
            // downstream; this is the ordering, not the only gate.
            try
            {
                ReflectionExtensions.ThrowIfNotAuthorized(
                    _authorizer,
                    job.Call.ServiceInterfaceFullName ?? string.Empty,
                    job.Call.MethodName ?? string.Empty);
            }
            catch (CallbackTargetUnresolvableException)
            {
                // Refused without dispatching: counted "rejected", like the executor's
                // unsupported-schema refusal — and, like it, still thrown rather than
                // acknowledged. The allowlist is per-deployment configuration another replica (or
                // the next deploy) may accept, and an acknowledged job has no other copy; the
                // transport's redelivery/dead-letter policy decides. Pre-fix the refusal recorded
                // no worker outcome at all.
                AsyncResponseDiagnostics.RecordWorkerOutcome("rejected");
                throw;
            }

            // The job crossed a serialization boundary (broker → ingress): restore any ambient
            // context its propagators captured before executing it.
            using (_propagation.Restore(job.Context))
                await _workerJobExecutor.ExecuteAsync(job).ConfigureAwait(false);
        }
        catch (DurableFlowInterruptedException)
        {
            // A host-stop hand-back, not a failure: the executor records it as neither failed nor
            // an error span, and the transport leaves the delivery unsettled for redelivery.
            // Logging it at Error here raised an alert on every rolling deploy.
            throw;
        }
        catch (Exception ex)
        {
            // Guarded so the job's own exception is what propagates: transports classify it (a
            // lease hand-back, an oversized re-publish), and a throwing provider replaced it.
            SafeLog.Try((Logger: _logger, Error: ex), static state => state.Logger.LogError(state.Error, "Ingress worker job execution failed."));
            AsyncResponseDiagnostics.SetError(activity, ex);

            // Propagate: the transport dispatcher owns the retry/dead-letter decision for worker
            // jobs (per its AckMode and MaxDeliveryAttempts). Swallowing here acknowledged failed
            // jobs as successes, which disabled redelivery entirely and left the waiter to burn
            // its full timeout.
            throw;
        }
    }
}
