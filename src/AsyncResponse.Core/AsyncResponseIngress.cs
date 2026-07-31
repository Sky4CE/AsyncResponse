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
    ILogger<AsyncResponseIngress> _logger) : IAsyncResponseIngress
{
    /// <summary>Handles the delivered message.</summary>
    public async Task HandleResponseMessageAsync(string messageJson, string? correlationId)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.ingress.response",
            ActivityKind.Consumer,
            correlationId);

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            // Deliberately acknowledged, not thrown: without a correlation id the message can
            // never route, so redelivery would retry it forever (RabbitMQ's default
            // MaxDeliveryAttempts = 0 has no cap) or burn dead-letter attempts on brokers that do.
            // Error-level log + counter make the drop loud — every occurrence is a producer-side
            // contract violation.
            _logger.LogError("Ingress received a response message with no correlation id; it cannot be routed and is acknowledged without dispatch. Message: {Message}", messageJson);
            AsyncResponseDiagnostics.SetError(activity, "correlation_id_null", "No correlation id on the inbound response message.");
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
