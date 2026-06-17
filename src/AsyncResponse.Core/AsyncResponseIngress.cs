using Microsoft.Extensions.Logging;

namespace AsyncResponse;

/// <summary>
/// Transport-neutral ingress implementation. Broker/webhook adapters can feed response payloads
/// and worker-job envelopes into this service without depending on a specific response channel.
/// </summary>
internal sealed class AsyncResponseIngress(
    IAsyncResponsePublisher _publisher,
    WorkerJobExecutor _workerJobExecutor,
    AsyncResponseContextPropagation _propagation,
    ILogger<AsyncResponseIngress> _logger) : IAsyncResponseIngress
{
    public async Task HandleResponseMessageAsync(string messageJson, string? correlationId = null)
    {
        try
        {
            _logger.LogDebug("Ingress received inbound response message: {Message}", messageJson);

            var response = JsonSafety.SafeDeserialize<object?>(messageJson);
            await _publisher.SetResponse(response, correlationId).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ingress failed to process the inbound response message.");
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
        try
        {
            _logger.LogDebug("Ingress received worker job: {Payload}", messageJson);

            var job = JsonSafety.SafeDeserialize<WorkerJobEnvelope>(messageJson)
                ?? throw new InvalidDataException("Worker message deserialized to null.");

            // The job crossed a serialization boundary (broker → ingress): restore any ambient
            // context its propagators captured before executing it.
            using (_propagation.Restore(job.Context))
                await _workerJobExecutor.ExecuteAsync(job).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ingress worker job execution failed.");
        }
    }
}
