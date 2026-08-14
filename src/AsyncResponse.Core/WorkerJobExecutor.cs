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
    /// Minimum shrink of the remaining delay between two hops of the re-publish chain for the
    /// chain to count as making progress. A real hop shrinks the remainder by at least the
    /// broker's ~1s delay resolution; a hop redelivered with the SAME remainder means the gating
    /// clock disagrees with the stamping clock (skew) and re-publishing would loop forever.
    /// </summary>
    private static readonly TimeSpan RedelayProgressEpsilon = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Consecutive no-progress hops required before the stall fallback executes the job early.
    /// A single early redelivery can be a transient anomaly (an unhonored delay, a redrive
    /// surfacing the message) — executing on one sample would break the due-time contract by the
    /// whole remainder. Genuine skew stalls EVERY hop, so requiring a second consecutive stall
    /// keeps the anti-livelock property while a lone anomaly just re-publishes once more.
    /// </summary>
    private const int RedelayStallExecuteThreshold = 2;

    /// <summary>
    /// Executes the job. Exceptions propagate to the caller — transports decide whether to log,
    /// retry, or dead-letter.
    /// </summary>
    public async Task ExecuteAsync(WorkerJobEnvelope job)
    {
        ArgumentNullException.ThrowIfNull(job);

        // Armed only on the skew-proven early-execution path below; disposed with the invocation.
        IDisposable? forcedEarly = null;

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

        // The same portable-id contract the publishers enforce, applied to an id that arrived over
        // a broker. It has to happen HERE, before the redelay hop and before any handler runs: the
        // handler's implicit response publish would throw on this id, so the job would fail AFTER
        // its side effects and be redelivered to run them again. A null or blank id is left alone —
        // that is a fire-and-forget job, which has no response to publish.
        //
        // Drop, never throw — the same answer the ingress gives the identical id class on the
        // response path: the id can never become portable, so throwing turns the job into a
        // poison message that redelivers forever (RabbitMQ's default MaxDeliveryAttempts = 0 has
        // no cap) or burns dead-letter attempts on brokers that do. Returning cleanly lets the
        // transport ACK; the Error log + counter make the drop loud.
        if (!string.IsNullOrWhiteSpace(job.CorrelationId)
            && AsyncResponseChannelOptions.CorrelationIdNotPortable(job.CorrelationId) is { } rejection)
        {
            _logger.LogError(
                "Worker job carries a correlation id outside the portable contract; it cannot be executed and is acknowledged without dispatch. {Rejection}",
                rejection);
            AsyncResponseDiagnostics.RecordWorkerOutcome("rejected");
            return;
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
                // MaxPublishDelay <= zero: the capability is unavailable in the current
                // configuration (an SQS FIFO worker queue) — same as not implementing it.
                if (_workerTransport is not IDelayedWorkerTransport delayedTransport
                    || delayedTransport.MaxPublishDelay <= TimeSpan.Zero)
                {
                    // The job was published by a delayed-capable producer, but THIS consumer's
                    // transport cannot re-delay it. Executing early would silently break the due
                    // time; throwing routes it through normal retry/DLQ where it is visible.
                    throw new InvalidOperationException(
                        $"Worker job for correlationId {job.CorrelationId} is due at {notBeforeUtc:O} ({remaining} from now), but the " +
                        $"registered worker transport ({_workerTransport?.GetType().Name ?? "none"}) does not support delayed delivery to re-schedule it.");
                }

                // Progress check: on transports whose due time is gated by a different clock than
                // the one that stamped it (client-computed available_at / ScheduledEnqueueTime vs
                // the broker's own clock), a consumer running behind that clock is handed the job
                // back immediately and would re-publish the same remainder forever — each hop a
                // fresh message id, so no delivery counter ever reaches a DLQ. Executing early by
                // the skew beats never executing — but only after consecutive stalls prove the
                // skew is persistent, so a single anomalous early delivery cannot fire the job
                // arbitrarily ahead of its due time.
                var stalled = job.LastRedelayRemaining is { } lastRemaining && remaining >= lastRemaining - RedelayProgressEpsilon;
                job.RedelayStallCount = stalled ? job.RedelayStallCount + 1 : 0;

                if (stalled && job.RedelayStallCount >= RedelayStallExecuteThreshold)
                {
                    // The proof dies with this envelope. Anything the execution below re-publishes
                    // is a NEW message whose stall counters start at zero, so a durable timer that
                    // suspends again would rebuild the same proof from scratch on every lap and
                    // never finish. The marker lets such a step wait out its remainder in process
                    // instead — see WorkerJobSkewScope.
                    forcedEarly = WorkerJobSkewScope.Enter();

                    _logger.LogWarning(
                        "Worker job {Target}.{Method} was redelivered {Remaining} before its due time {NotBeforeUtc} with no progress over {StallCount} consecutive hops ({LastRemaining} previously); " +
                        "the publishing and delivery-gating clocks disagree (clock skew). Executing it now instead of re-publishing.",
                        job.Call.ServiceInterfaceFullName, job.Call.MethodName, remaining, notBeforeUtc, job.RedelayStallCount, job.LastRedelayRemaining);
                    // No outcome recorded here: the execution below records exactly one outcome
                    // ("executed"/"failed") for this delivery, like every other path.
                }
                else
                {
                    _logger.LogDebug(
                        "Worker job {Target}.{Method} delivered {Remaining} before its due time {NotBeforeUtc}; re-publishing the next hop.",
                        job.Call.ServiceInterfaceFullName, job.Call.MethodName, remaining, notBeforeUtc);
                    AsyncResponseDiagnostics.RecordWorkerOutcome("redelayed");

                    job.LastRedelayRemaining = remaining;
                    var hop = remaining <= delayedTransport.MaxPublishDelay ? remaining : delayedTransport.MaxPublishDelay;
                    await delayedTransport.PublishAsync(job, hop).ConfigureAwait(false);
                    return;
                }
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
        finally
        {
            forcedEarly?.Dispose();
        }
    }
}
