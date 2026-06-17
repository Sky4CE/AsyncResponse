using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AsyncResponse;

/// <summary>
/// Executes <see cref="WorkerJobEnvelope"/>s: restores the correlation context and invokes the
/// described service method through the DI container. Shared by the broker ingress
/// (<see cref="IAsyncResponseIngress.HandleWorkerMessageAsync"/>) and the in-process worker
/// transport, so every transport executes jobs identically.
/// </summary>
internal sealed class WorkerJobExecutor(IServiceScopeFactory _scopeFactory, ILogger<WorkerJobExecutor> _logger)
{
    /// <summary>
    /// Executes the job. Exceptions propagate to the caller — transports decide whether to log,
    /// retry, or dead-letter.
    /// </summary>
    public async Task ExecuteAsync(WorkerJobEnvelope job)
    {
        ArgumentNullException.ThrowIfNull(job);

        _logger.LogDebug("Executing worker job {Target}.{Method} (correlationId: {CorrelationId}, replyTarget: {ReplyTarget}).", job.Call.ServiceInterfaceFullName, job.Call.MethodName, job.CorrelationId, job.ReplyTarget?.Name);

        // Scope the restored ambient context so one job cannot inherit or leak another job's
        // correlation id or reply target.
        using var asyncResponseScope = AsyncResponseContext.PushContext(job.CorrelationId, job.ReplyTarget);

        var invocation = ReflectionExtensions.ResolveCallback(
            job.Call,
            payload: null,
            exception: null,
            correlationId: job.CorrelationId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        await scope.ServiceProvider.InvokeAsync(invocation).ConfigureAwait(false);

        _logger.LogInformation("Executed worker job {Target}.{Method} successfully.", job.Call.ServiceInterfaceFullName, job.Call.MethodName);
    }
}
