using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Text;

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
    ILogger<AsyncResponseIngress> _logger) : IAsyncResponseIngress
{
    /// <summary>
    /// Names why an id extracted from an untrusted broker message cannot route, or reports that it
    /// can. Two ways to fail, one answer: missing outright, or present but outside the portable
    /// contract — over-long, or space-padded, which a relational store treats as the SAME key as
    /// the trimmed form while the library compares ids ordinally, so storing a payload under it
    /// could surface that payload at another conversation's waiter.
    /// </summary>
    private static bool IsUnroutable([NotNullWhen(false)] string? correlationId, out UnroutableReason reason)
    {
        if (string.IsNullOrWhiteSpace(correlationId))
        {
            reason = new("correlation_id_null", "no correlation id");
            return true;
        }

        if (AsyncResponseChannelOptions.CorrelationIdNotPortable(correlationId) is { } rejection)
        {
            reason = new("correlation_id_not_portable", $"an unusable correlation id — {rejection}");
            return true;
        }

        reason = default;
        return false;
    }

    /// <summary>The span's <c>error.type</c> tag and the human-readable phrase, for one drop cause.</summary>
    private readonly record struct UnroutableReason(string ErrorType, string Description);

    /// <summary>Handles the delivered message.</summary>
    public async Task HandleResponseMessageAsync(string messageJson, string? correlationId)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.ingress.response",
            ActivityKind.Consumer,
            correlationId);

        if (IsUnroutable(correlationId, out var unroutable))
        {
            // Deliberately acknowledged, not thrown: the message can never route, so redelivery
            // would retry it forever (RabbitMQ's default MaxDeliveryAttempts = 0 has no cap) or
            // burn dead-letter attempts on brokers that do. Error-level log + counter make the drop
            // loud — every occurrence is a producer-side contract violation. Only metadata is
            // logged: response payloads may carry PII and stay out of logs by policy
            // (docs/security.md); the byte length and hash prefix are enough to correlate with
            // broker-side capture tooling.
            var payloadBytes = Encoding.UTF8.GetBytes(messageJson);
            _logger.LogError(
                "Ingress received a response message with {UnroutableReason}; it cannot be routed and is acknowledged without dispatch. Payload: {PayloadLength} bytes, sha256 {PayloadSha256Prefix}.",
                unroutable.Description,
                payloadBytes.Length,
                Convert.ToHexString(SHA256.HashData(payloadBytes).AsSpan(0, 8)));
            AsyncResponseDiagnostics.SetError(activity, unroutable.ErrorType, $"Inbound response message has {unroutable.Description}.");
            AsyncResponseDiagnostics.RecordUnroutableResponse();
            return;
        }

        try
        {
            _logger.LogDebug("Ingress received inbound response message: {Message}", messageJson);

            // A transient infrastructure fault (channel store briefly unreachable, recovery-state
            // read hiccup, resume-callback dependency blip) must not finalize the waiter on the
            // first attempt — that would convert a recoverable response into a permanent business
            // failure. Retry briefly in-process before escalating. Parse failures are excluded:
            // an unparseable message never becomes parseable, so it escalates immediately.
            // Recovery resume callbacks may be re-invoked by these retries, which matches their
            // contract — broker redelivery re-invokes them the same way.
            await AsyncResponseRetry.ExecuteAsync(
                async _ =>
                {
                    await _rawPublisher.SetRawResponseJson(messageJson, correlationId).ConfigureAwait(false);
                    return true;
                },
                isTransient: static ex => ex is not (System.Text.Json.JsonException or InvalidDataException),
                maxAttempts: 4,
                baseDelay: TimeSpan.FromMilliseconds(250),
                maxDelay: TimeSpan.FromSeconds(2),
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
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

        try
        {
            _logger.LogDebug("Ingress received worker job: {Payload}", messageJson);

            var job = JsonSafety.SafeDeserialize<WorkerJobEnvelope>(messageJson)
                ?? throw new InvalidDataException("Worker message deserialized to null.");
            AsyncResponseDiagnostics.SetCorrelationId(activity, job.CorrelationId);
            AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
            AsyncResponseDiagnostics.SetWorker(activity, job.Call);

            // The job crossed a serialization boundary (broker → ingress): restore any ambient
            // context its propagators captured before executing it.
            using (_propagation.Restore(job.Context))
                await _workerJobExecutor.ExecuteAsync(job).ConfigureAwait(false);
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
