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
    public async Task HandleResponseMessageAsync(string messageJson, string? correlationId = null)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.ingress.response",
            ActivityKind.Consumer,
            correlationId);

        try
        {
            _logger.LogDebug("Ingress received inbound response message: {Message}", messageJson);

            var response = JsonSafety.SafeDeserialize<object?>(messageJson);
            await _rawPublisher.SetRawResponse(response, correlationId).ConfigureAwait(false);
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
            }
        }
    }

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
        }
    }
}
