using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace AsyncResponse;

/// <summary>
/// Executes <see cref="WorkerJobEnvelope"/>s: restores the correlation context and invokes the
/// described service method through the DI container. Shared by the broker ingress
/// (<see cref="IAsyncResponseIngress.HandleWorkerMessageAsync"/>) and the in-process worker
/// transport, so every transport executes jobs identically.
/// </summary>
internal sealed class WorkerJobExecutor(
    IServiceScopeFactory _scopeFactory,
    ILogger<WorkerJobExecutor> _logger,
    IWorkerTransport? _workerTransport = null,
    TimeProvider? _timeProvider = null)
{
    /// <summary>
    /// Tolerance for early delivery of a due-time-stamped job. Broker delay resolution is one
    /// second at best (SQS DelaySeconds, visibility timestamps), so re-publishing for a
    /// sub-second remainder would spin a delivery loop that can never catch the instant.
    /// </summary>
    private static readonly TimeSpan NotBeforeTolerance = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Executes the job. Exceptions propagate to the caller — transports decide whether to log,
    /// retry, or dead-letter.
    /// </summary>
    public async Task ExecuteAsync(WorkerJobEnvelope job)
    {
        ArgumentNullException.ThrowIfNull(job);

        // Reject a job stamped with an unsupported schema rather than invoke a possibly-incompatible
        // method shape. Throwing routes the job through the transport's normal
        // failure/dead-letter handling. This is the single choke point every transport shares.
        if (!WorkerJobEnvelopeSchema.IsReadable(job.SchemaVersion))
        {
            _logger.LogWarning(
                "Worker job for correlationId {CorrelationId} has unsupported schema version {SchemaVersion} (current: {Current}); rejecting it.",
                job.CorrelationId, job.SchemaVersion, WorkerJobEnvelopeSchema.Current);
            AsyncResponseDiagnostics.RecordWorkerOutcome("rejected");
            throw new InvalidOperationException(
                $"Worker job schema version {job.SchemaVersion} is not supported by this build " +
                $"(current: {WorkerJobEnvelopeSchema.Current}) and cannot be executed safely.");
        }

        // Due-time guard, the shared half of delayed delivery (see IDelayedWorkerTransport): a job
        // delivered before its stamped due time — a chunked hop on a transport whose per-publish
        // delay is capped, or plain broker imprecision — is re-published for the remainder instead
        // of executed. Every transport funnels through here, so the chunk chain needs no
        // per-transport code.
        if (job.NotBeforeUtc is { } notBeforeUtc)
        {
            var remaining = notBeforeUtc - (_timeProvider ?? TimeProvider.System).GetUtcNow().UtcDateTime;
            if (remaining > NotBeforeTolerance)
            {
                if (_workerTransport is not IDelayedWorkerTransport delayedTransport)
                {
                    // The job was published by a delayed-capable producer, but THIS consumer's
                    // transport cannot re-delay it. Executing early would silently break the due
                    // time; throwing routes it through normal retry/DLQ where it is visible.
                    throw new InvalidOperationException(
                        $"Worker job for correlationId {job.CorrelationId} is due at {notBeforeUtc:O} ({remaining} from now), but the " +
                        $"registered worker transport ({_workerTransport?.GetType().Name ?? "none"}) does not support delayed delivery to re-schedule it.");
                }

                _logger.LogDebug(
                    "Worker job {Target}.{Method} delivered {Remaining} before its due time {NotBeforeUtc}; re-publishing the next hop.",
                    job.Call.ServiceInterfaceFullName, job.Call.MethodName, remaining, notBeforeUtc);
                AsyncResponseDiagnostics.RecordWorkerOutcome("redelayed");

                var hop = remaining <= delayedTransport.MaxPublishDelay ? remaining : delayedTransport.MaxPublishDelay;
                await delayedTransport.PublishAsync(job, hop).ConfigureAwait(false);
                return;
            }
        }

        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.worker.execute",
            ActivityKind.Consumer,
            job.CorrelationId);
        AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
        AsyncResponseDiagnostics.SetWorker(activity, job.Call);

        _logger.LogDebug("Executing worker job {Target}.{Method} (correlationId: {CorrelationId}, replyTarget: {ReplyTarget}).", job.Call.ServiceInterfaceFullName, job.Call.MethodName, job.CorrelationId, job.ReplyTarget?.Name);

        try
        {
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

            _logger.LogDebug("Executed worker job {Target}.{Method} successfully.", job.Call.ServiceInterfaceFullName, job.Call.MethodName);
            AsyncResponseDiagnostics.RecordWorkerOutcome("executed");
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            AsyncResponseDiagnostics.RecordWorkerOutcome("failed");
            throw;
        }
    }
}
