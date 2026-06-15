using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AsyncResponse;

/// <summary>
/// Executes <see cref="WorkerJobEnvelope"/>s: restores the correlation context and invokes the
/// described service method through the DI container. Shared by the broker ingress
/// (<see cref="IAsyncResponseIngress.HandleWorkerMessageAsync"/>) and the in-process worker
/// transport, so every transport executes jobs identically.
/// </summary>
public sealed class WorkerJobExecutor(IServiceScopeFactory _scopeFactory, ILogger<WorkerJobExecutor> _logger)
{
    private const string SERVICE_NAME = nameof(WorkerJobExecutor);

    /// <summary>
    /// Executes the job. Exceptions propagate to the caller — transports decide whether to log,
    /// retry, or dead-letter.
    /// </summary>
    public async Task ExecuteAsync(WorkerJobEnvelope job)
    {
        ArgumentNullException.ThrowIfNull(job);

        _logger.LogDebug("{ServiceName}: executing job {Target}.{Method} (correlationId: {CorrelationId}).",
            SERVICE_NAME, job.Call.ServiceInterfaceFullName, job.Call.MethodName, job.CorrelationId);

        // Restore the ambient correlation id so publishes made by the job correlate to the
        // originating operation.
        if (!string.IsNullOrWhiteSpace(job.CorrelationId))
        {
            AsyncResponseContext.SetCorrelationId(job.CorrelationId!);
        }

        var invocation = ReflectionExtensions.ResolveCallback(
            job.Call,
            payload: null,
            exception: null,
            correlationId: job.CorrelationId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        await scope.ServiceProvider.InvokeAsync(invocation).ConfigureAwait(false);

        _logger.LogInformation("{ServiceName}: executed job {Target}.{Method} successfully.",
            SERVICE_NAME, job.Call.ServiceInterfaceFullName, job.Call.MethodName);
    }
}
