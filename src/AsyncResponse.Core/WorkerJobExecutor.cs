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

        // Every job starts unmarked. The skew marker is an AsyncLocal like WorkerJobScope's, and
        // the in-memory transport runs a job under its ENQUEUER's captured execution context: a
        // follow-up published by a job the stall guard released early inherited the unconsumed
        // marker, and a durable timer replayed in that lineage spent the one-shot proof that
        // belonged to the job that earned it (waiting in process, or failing on the timer ceiling).
        using var unmarked = WorkerJobSkewScope.EnterUnmarked();

        // Armed only on the skew-proven early-execution path below; disposed with the invocation.
        IDisposable? forcedEarly = null;

        // Reject a job stamped with an unsupported schema rather than invoke a possibly-incompatible
        // method shape. Throwing routes the job through the transport's normal
        // failure/dead-letter handling. This is the single choke point every transport shares.
        if (!WorkerJobEnvelopeSchema.IsReadable(job.SchemaVersion))
        {
            // Escaped: the id has not been through the portability check yet (that runs below, and
            // only for a readable envelope), so it is still raw wire text — a CR/LF in it would
            // forge a log line.
            _logger.LogWarning(
                "Worker job for correlationId {CorrelationId} has unsupported schema version {SchemaVersion} (current: {Current}); rejecting it.",
                job.CorrelationId is { } rawId ? DiagnosticText.EscapedExcerpt(rawId, AsyncResponseChannelOptions.MaxCorrelationIdLength) : null,
                job.SchemaVersion, WorkerJobEnvelopeSchema.Current);
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
        if (job.NotBeforeUtc is { } stampedNotBefore)
        {
            var notBeforeUtc = AsUtc(stampedNotBefore);
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
                // Both stall fields are wire values a foreign producer controls. The library only
                // ever stamps a strictly positive remainder, so a negative LastRedelayRemaining is
                // invalid (and TimeSpan.MinValue would overflow the checked subtraction below);
                // clamping the counter into [0, threshold] keeps a hostile int.MaxValue from
                // wrapping negative and disarming the stall fallback forever.
                var stalled = job.LastRedelayRemaining is { } lastRemaining
                    && lastRemaining >= TimeSpan.Zero
                    && remaining >= lastRemaining - RedelayProgressEpsilon;
                var stallCount = stalled ? Math.Clamp(job.RedelayStallCount, 0, RedelayStallExecuteThreshold) + 1 : 0;

                if (stalled && stallCount >= RedelayStallExecuteThreshold)
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
                        AsyncResponseTypeResolution.DescribeForDiagnostics(job.Call.ServiceInterfaceFullName), DiagnosticText.EscapedExcerpt(job.Call.MethodName, 256),
                        remaining, notBeforeUtc, stallCount, job.LastRedelayRemaining);
                    // No outcome recorded here: the execution below records exactly one outcome
                    // ("executed"/"failed") for this delivery, like every other path.
                }
                else
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                    {
                        _logger.LogDebug(
                            "Worker job {Target}.{Method} delivered {Remaining} before its due time {NotBeforeUtc}; re-publishing the next hop.",
                            AsyncResponseTypeResolution.DescribeForDiagnostics(job.Call.ServiceInterfaceFullName), DiagnosticText.EscapedExcerpt(job.Call.MethodName, 256),
                            remaining, notBeforeUtc);
                    }

                    AsyncResponseDiagnostics.RecordWorkerOutcome("redelayed");

                    // The next hop is a COPY carrying the new stall counters; the delivered
                    // envelope is never modified. The transport still owns it and may retry it as
                    // is — the in-memory ladder does when this publish throws (delayed capacity
                    // exhausted, host draining) — and counters stamped on it made each retry, 100
                    // ms later, read as a hop that made no progress: two retries "proved" clock
                    // skew and ran the job early, a durable timer spending the forced-early marker
                    // on it (and failing terminally when more than the timer ceiling was left).
                    var next = DurableFlowExecutor.CopyForRedelay(job, notBeforeUtc);
                    next.LastRedelayRemaining = remaining;
                    next.RedelayStallCount = stallCount;
                    var hop = remaining <= delayedTransport.MaxPublishDelay ? remaining : delayedTransport.MaxPublishDelay;
                    await delayedTransport.PublishAsync(next, hop).ConfigureAwait(false);
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

        // Escaped: the target is still unresolved wire text here (resolution and authorization run
        // below), and a blank correlation id skipped the portability check above.
        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug(
                "Executing worker job {Target}.{Method} (correlationId: {CorrelationId}, replyTarget: {ReplyTarget}).",
                AsyncResponseTypeResolution.DescribeForDiagnostics(job.Call.ServiceInterfaceFullName),
                DiagnosticText.EscapedExcerpt(job.Call.MethodName, 256),
                job.CorrelationId is { } correlationId ? DiagnosticText.EscapedExcerpt(correlationId, AsyncResponseChannelOptions.MaxCorrelationIdLength) : null,
                job.ReplyTarget?.Name is { } replyTarget ? DiagnosticText.EscapedExcerpt(replyTarget, 256) : null);
        }

        try
        {
            // Scope the restored ambient context so one job cannot inherit or leak another job's
            // correlation id or reply target.
            using var asyncResponseScope = AsyncResponseContext.PushContext(job.CorrelationId, job.ReplyTarget);

            // The executing job itself, for the one handler that needs it: a durable-flow
            // execution records the job's identity with its lease, and re-publishes THIS job when
            // the broker redelivers it under a handler that is still running. Entered in this
            // frame — the one that awaits the invocation — because an AsyncLocal written inside a
            // callee never flows back here, and entered even for a job without an id so a job the
            // in-memory transport runs under its enqueuer's captured context never reads as the
            // job that published it.
            using var jobScope = WorkerJobScope.Enter(job);

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
        // A DurableFlowInterruptedException passes through unrecorded: it is the flow engine
        // handing the delivery back at host stop — a cancellation by contract, never a job
        // failure — and each transport settles it as "not executed".
        catch (Exception ex) when (ex is not DurableFlowInterruptedException)
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

    /// <summary>
    /// <see cref="WorkerJobEnvelope.NotBeforeUtc"/> as a UTC instant. The value is wire data, and
    /// System.Text.Json reads a timestamp carrying an offset (<c>+00:00</c>, what most non-.NET
    /// producers write) as LOCAL time, while <see cref="DateTime"/> subtraction ignores
    /// <see cref="DateTime.Kind"/>: compared raw, the due time moved by the host's UTC offset — a
    /// job re-delayed hours too long east of Greenwich and run early west of it. An unspecified
    /// kind is taken as UTC, which is what the property promises.
    /// </summary>
    internal static DateTime AsUtc(DateTime value) => value.Kind switch
    {
        DateTimeKind.Local => value.ToUniversalTime(),
        DateTimeKind.Unspecified => DateTime.SpecifyKind(value, DateTimeKind.Utc),
        _ => value
    };
}
