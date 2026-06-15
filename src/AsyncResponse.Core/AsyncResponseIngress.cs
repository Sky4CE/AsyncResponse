using Microsoft.Extensions.Logging;

namespace AsyncResponse;

/// <summary>
/// Transport-neutral ingress implementation. Broker/webhook adapters can feed response payloads
/// and worker-job envelopes into this service without depending on a specific response channel.
/// </summary>
internal sealed class AsyncResponseIngress(
    IAsyncResponsePublisher _publisher,
    WorkerJobExecutor _workerJobExecutor,
    ILogger<AsyncResponseIngress> _logger) : IAsyncResponseIngress
{
    private const string SERVICE_NAME = nameof(AsyncResponseIngress);

    public async Task HandleResponseMessageAsync(string messageJson, string? correlationId = null)
    {
        const string MethodName = nameof(HandleResponseMessageAsync);

        try
        {
            _logger.LogDebug("{ServiceName}: {MethodName} Received inbound response message: {Message}.",
                SERVICE_NAME, MethodName, messageJson);

            var response = JsonSafety.SafeDeserialize<object?>(messageJson);
            await _publisher.SetResponse(response, correlationId).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{ServiceName}: {MethodName} An error occurred while processing the inbound message. ErrorMessage: {ErrorMessage}",
                SERVICE_NAME, MethodName, ex.Message);
            try
            {
                await _publisher.SetException(ex, correlationId).ConfigureAwait(false);
            }
            catch (Exception innerEx)
            {
                _logger.LogError(innerEx, "{ServiceName}: {MethodName} Failed to publish the exception for the inbound message. ErrorMessage: {ErrorMessage}; InnerErrorMessage: {InnerErrorMessage}",
                    SERVICE_NAME, MethodName, ex.Message, innerEx.Message);
            }
        }
    }

    public async Task HandleWorkerMessageAsync(string messageJson)
    {
        const string MethodName = nameof(HandleWorkerMessageAsync);

        try
        {
            _logger.LogDebug("{ServiceName}: {MethodName} Received worker job: {Payload}",
                SERVICE_NAME, MethodName, messageJson);

            var job = JsonSafety.SafeDeserialize<WorkerJobEnvelope>(messageJson)
                ?? throw new InvalidDataException("Worker message deserialized to null.");

            await _workerJobExecutor.ExecuteAsync(job).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{ServiceName}: {MethodName} Worker job execution failed.",
                SERVICE_NAME, MethodName);
        }
    }
}
