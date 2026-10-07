using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;

namespace AsyncResponse;

/// <summary>
/// Executes durable flow runs. Its methods are the durable targets behind every flow: worker jobs
/// carry <see cref="ExecuteAsync"/>, and awaited steps register <see cref="RecoverAsync"/> /
/// <see cref="FailAsync(string, System.Exception, string)"/> as their lost-subscriber callbacks — invoked by whichever process
/// receives a late response, possibly a different deployment.
/// <para>
/// <b>Naming contract:</b> like all recovery callbacks, these targets are persisted as
/// interface/method name strings and live in stores for up to the configured expiry. The
/// interface and method names must stay stable across deployments.
/// </para>
/// </summary>
public interface IDurableFlowExecutor
{
    /// <summary>
    /// Runs the flow body for <paramref name="flowId"/> from the top: completed steps skip via
    /// their checkpoints, the in-flight awaited step re-attaches. No-op for terminal runs.
    /// </summary>
    Task ExecuteAsync(string flowId);

    /// <summary>
    /// Start target: the job <see cref="IDurableFlows.StartAsync{TFlow,TInput}"/> publishes. Creates
    /// the ledger from the serialized initial state the job carries when no ledger exists yet
    /// (insert-if-absent), then runs <see cref="ExecuteAsync"/>. The publish of this job — not the
    /// starter's own ledger write — is the start's commit point: a process that dies after the
    /// publish leaves a job whose execution creates the run, never a committed ledger that nothing
    /// will ever execute. An existing ledger for the same flow type and semantically identical
    /// input is executed as an idempotent re-start; one bound to different work is logged and the
    /// job dropped (the starter already reported the conflict to its caller). A start job whose
    /// carried state was created longer ago than <c>DurableFlowOptions.StateExpiry</c> and that
    /// finds no ledger is a replay of a run that has finished and expired: it is logged at Error
    /// and dropped rather than re-created, which would re-execute every completed step.
    /// </summary>
    Task CreateAndExecuteAsync(string flowId, string initialStateJson);

    /// <summary>
    /// Lost-subscriber resume target: re-enqueues <see cref="ExecuteAsync"/> on the worker
    /// transport (never runs the flow inline on a publisher's dispatch path).
    /// </summary>
    Task ResumeAsync(string flowId);

    /// <summary>
    /// Lost-subscriber success target: checkpoints the terminal payload into the matching pending
    /// step before re-enqueueing execution, so recovery does not wait for a consumed correlation id.
    /// A step that already faulted on <paramref name="correlationId"/> (it timed out, or its wait
    /// failed) is not pending on it any more: the late payload is not checkpointed into it — flow
    /// code may already have caught the fault and moved on — and the step restarts fresh instead.
    /// </summary>
    Task RecoverAsync(string flowId, object payload, string correlationId);

    /// <summary>Lost-subscriber failure target: marks the run terminally <see cref="FlowRunStatus.Failed"/>.</summary>
    Task FailAsync(string flowId, Exception exception);

    /// <summary>
    /// Correlation-scoped lost-subscriber failure target: marks the run terminally
    /// <see cref="FlowRunStatus.Failed"/> only while a step is still pending on
    /// <paramref name="correlationId"/>. A failure for a correlation id the flow has since settled
    /// or superseded (a dead worker's registration outliving the replacement's, a late error for a
    /// step that already restarted fresh, or one that already faulted on that id — timed out and
    /// caught by a best-effort flow, or about to restart from the transport's retry) is stale and
    /// is ignored — the same scoping <see cref="RecoverAsync"/> applies to the success target.
    /// </summary>
    Task FailAsync(string flowId, Exception exception, string correlationId);
}

/// <inheritdoc cref="IDurableFlowExecutor" />
internal sealed class DurableFlowExecutor : IDurableFlowExecutor
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IAsyncResponseBuilder _builder;
    private readonly IAsyncResponseSubscriber _subscriber;
    private readonly IRecoverableAsyncResponseSubscriber? _recoverableSubscriber;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly DurableFlowOptions _options;
    private readonly ILogger<DurableFlowExecutor> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly IDurableFlowExecutionObserver[] _observers;
    private readonly IWorkerTransport? _workerTransport;
    private readonly TimeSpan? _channelDefaultWaitTimeout;
    private readonly Dictionary<string, DurableFlowRegistration> _registrations;
    private readonly CancellationToken _hostStopping;

    /// <summary>Creates the flow executor.</summary>
    public DurableFlowExecutor(
        IServiceScopeFactory scopeFactory,
        IAsyncResponseBuilder builder,
        IAsyncResponseSubscriber subscriber,
        IRecoverableAsyncResponseSubscriber? recoverableSubscriber,
        AsyncResponseContextPropagation propagation,
        DurableFlowOptions options,
        ILogger<DurableFlowExecutor> logger,
        IEnumerable<DurableFlowRegistration>? registrations = null,
        Microsoft.Extensions.Hosting.IHostApplicationLifetime? hostLifetime = null,
        TimeProvider? timeProvider = null,
        IEnumerable<IDurableFlowExecutionObserver>? observers = null,
        IWorkerTransport? workerTransport = null,
        TimeSpan? channelDefaultWaitTimeout = null)
    {
        _workerTransport = workerTransport;
        _channelDefaultWaitTimeout = channelDefaultWaitTimeout;
        _scopeFactory = scopeFactory;
        _builder = builder;
        _subscriber = subscriber;
        _recoverableSubscriber = recoverableSubscriber;
        _propagation = propagation;
        _options = options;
        FlowStateConcurrency.ValidateOptions(_options);
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _observers = observers?.ToArray() ?? [];
        _hostStopping = hostLifetime?.ApplicationStopping ?? CancellationToken.None;
        _registrations = new Dictionary<string, DurableFlowRegistration>(StringComparer.Ordinal);
        foreach (var registration in registrations ?? [])
        {
            // Last registration wins, matching DI's usual override semantics. Keyed by type
            // identity (TypeNameIdentity): a generic flow class persisted by a build whose argument
            // assemblies carried another version still finds its registration.
            _registrations[TypeNameIdentity.Normalize(registration.FlowTypeFullName)!] = registration;
        }
    }

    /// <inheritdoc />
    public Task ExecuteAsync(string flowId) => ExecuteCoreAsync(flowId, _timeProvider.GetUtcNow().UtcDateTime);

    /// <param name="flowId">The run to execute.</param>
    /// <param name="deliveryStartedUtc">
    /// When this delivery's handler started — before any lease-contention wait — so an in-process
    /// timer can tell how much of the broker's in-flight ceiling the delivery has already used.
    /// </param>
    private async Task ExecuteCoreAsync(string flowId, DateTime deliveryStartedUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

        await using var lease = await AcquireExecutionLeaseWithRetryAsync(store, flowId).ConfigureAwait(false);
        if (lease is null)
            return;

        // A null load now means the run is genuinely gone (unknown, pruned, expired), which is the
        // one case where returning — and so acknowledging the wake-up — is correct. A ledger that
        // exists but cannot be read throws FlowStateUnreadableException instead and propagates to
        // the transport's retry/dead-letter policy, because acknowledging THAT abandons a Running
        // flow whose only remaining wake-up was this message.
        var state = await store.LoadAsync(flowId).ConfigureAwait(false);
        if (state is null)
        {
            // Every log line in this executor is guarded (SafeLog): each one sits between a
            // decision already made — often a durable write or publish already committed — and the
            // work or acknowledgement that must follow it. A throwing logging provider (MEL's
            // aggregate logger rethrows a provider's failure) turned an ack into a retried
            // delivery, a committed park into a failure, a Succeeded run into a "failed attempt".
            SafeLog.Try(
                (Logger: _logger, FlowId: flowId),
                static s => s.Logger.LogWarning("Durable flow {FlowId} has no state (unknown, pruned, or expired); nothing to execute.", s.FlowId));
            return;
        }

        if (state.Status != FlowRunStatus.Running)
            state = await ConfirmNotRunningAsync(store, flowId, state).ConfigureAwait(false);

        if (state.Status != FlowRunStatus.Running)
        {
            SafeLog.Try(
                (Logger: _logger, FlowId: flowId, state.Status),
                static s => s.Logger.LogDebug("Durable flow {FlowId} is already {Status}; skipping execution.", s.FlowId, s.Status));
            // Re-notify terminal runs on duplicate deliveries: the ORIGINAL delivery's
            // run-finished notification (below, outside the try/catch) may itself have thrown and
            // caused this redelivery — skipping here would lose the terminal event forever.
            // Observer delivery is at-least-once by contract; implementations tolerate duplicates.
            if (state.Status is FlowRunStatus.Succeeded or FlowRunStatus.Failed)
                await NotifyRunFinishedAsync(state).ConfigureAwait(false);
            await NotifyParentAsync(state).ConfigureAwait(false);
            return;
        }

        var activity = AsyncResponseDiagnostics.StartActivity("asyncresponse.flow.execute");
        using var spanStop = AsyncResponseDiagnostics.StopOnExit(activity);
        activity?.SetTag("asyncresponse.flow_id", flowId);
        activity?.SetTag("asyncresponse.flow_type", AsyncResponseTypeResolution.DescribeForDiagnostics(state.FlowTypeName));

        state.Attempts++;
        await lease.SaveAsync(state, _options.StateExpiry).ConfigureAwait(false);

        // The run may be resumed by a different deployment than the one that started it: restore
        // the ambient context captured at start before any flow code runs.
        using var ambientScope = _propagation.Restore(state.Context);

        DurableFlowContext? context = null;
        try
        {
            context = CreateContext(store, state, lease, deliveryStartedUtc);
            var suspended = await InvokeFlowAsync(scope.ServiceProvider, state, context).ConfigureAwait(false);
            if (suspended)
            {
                // The context persisted the suspended state BEFORE enqueueing the child; saving here
                // could overwrite newer checkpoints written by a parent re-execution the child has
                // already triggered on another worker. The park is committed (its wake-up is
                // published, its lease released): a throwing logger here used to reach the
                // general catch below, fail the delivery, and save through the released lease.
                SafeLog.Try(
                    (Logger: _logger, FlowId: flowId, state.LastMessage),
                    static s => s.Logger.LogDebug("Durable flow {FlowId} suspended: {Message}", s.FlowId, s.LastMessage));
                return;
            }

            state.Status = FlowRunStatus.Succeeded;
            state.LastMessage = "Flow completed.";
            await lease.SaveAsync(state, _options.StateExpiry).ConfigureAwait(false);

            // Nothing after the terminal save stays inside this try: an exception from here on
            // would enter the general catch below and rewrite a Succeeded ledger's message with
            // its own, report the attempt failed, and fail the delivery. The success line is
            // logged after the try/catch, once the outcome is settled.
        }
        catch (DurableFlowSuspendedException ex)
        {
            // Same as the IsSuspended return above: the suspended state is already persisted, and a
            // save here races the child-triggered parent re-execution.
            SafeLog.Try(
                (Logger: _logger, FlowId: flowId, ex.Message),
                static s => s.Logger.LogDebug("Durable flow {FlowId} suspended: {Message}", s.FlowId, s.Message));
            return;
        }
        catch (DurableFlowInterruptedException ex)
        {
            // Host stop handed the delivery back — an in-process timer reached on a stopping host,
            // or a hand-over that could not commit — and the transport redelivers it after the
            // restart. Nothing failed, so none of the failure path below: no error span, no
            // checkpoint of the interruption's message over the ledger's "sleeping until" (a
            // breadcrumb already persisted stays as it is; a timer handed back on entry, before
            // its first-pass save, persisted none, and its redelivery anchors the due time then,
            // as it always did), and above all no save that could throw and replace the
            // hand-back's TYPE, which is all a transport tells a hand-back from a failure by.
            // Before the lost-lease catch too, so a lease lapsing in the same instant does not
            // turn it into a failure either. Observers still hear that the attempt ended: a step
            // it reported waiting is no longer parked here.
            SafeLog.Try(
                (Logger: _logger, FlowId: flowId, ex.Message),
                static s => s.Logger.LogDebug("Durable flow {FlowId} handed its delivery back to the worker transport: {Message}", s.FlowId, s.Message));
            await NotifyRunAttemptFailedAsync(state).ConfigureAwait(false);
            throw;
        }
        catch (DurableFlowFailedException ex)
        {
            // Terminal by declaration: mark failed and swallow so the transport acks the job.
            state.Status = FlowRunStatus.Failed;
            state.LastMessage = ex.Message;
            try
            {
                await lease.SaveAsync(state, _options.StateExpiry, cause: ex).ConfigureAwait(false);
            }
            catch (Exception saveError)
            {
                // The terminal save was rejected (lease lost, store fault): the attempt ended
                // without a terminal outcome like every other failed attempt — observers hear it
                // and the span carries the error before the save's exception propagates. The
                // ledger still reads Running, and so does the event.
                state.Status = FlowRunStatus.Running;
                AsyncResponseDiagnostics.SetError(activity, saveError);
                await NotifyRunAttemptFailedAsync(state).ConfigureAwait(false);
                throw;
            }

            AsyncResponseDiagnostics.SetError(activity, ex);
            // Guarded: the terminal Failed save above is committed; a throw here failed the
            // delivery and deferred the run-finished and parent notifications below to a
            // redelivery that, once the attempts ran out, never came.
            SafeLog.Try(
                (Logger: _logger, Error: ex, FlowId: flowId),
                static s => s.Logger.LogWarning(s.Error, "Durable flow {FlowId} failed terminally: {Message}", s.FlowId, s.Error.Message));
        }
        catch (Exception ex) when (lease.LostToken.IsCancellationRequested)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            await NotifyRunAttemptFailedAsync(state).ConfigureAwait(false);
            throw;
        }
        catch (Exception ex)
        {
            state.LastMessage = ex.Message;
            AsyncResponseDiagnostics.SetError(activity, ex);

            // Before the save: a rejected checkpoint below must not lose the notification (the
            // attempt ended either way, and any step it parked is no longer parked).
            await NotifyRunAttemptFailedAsync(state).ConfigureAwait(false);

            await lease.SaveAsync(state, _options.StateExpiry, cause: ex).ConfigureAwait(false);

            // The failure's message can be the write that takes the ledger past a warning band.
            // Judged like the context's own saves — against the band seeded from the ledger as
            // this execution loaded it — so the redeliveries that fail the same way and write the
            // same message back do not warn again. Guarded: the attempt's failure propagates.
            if (context is not null)
                SafeLog.Try(context, static c => c.WarnIfLedgerLargeAfterExecutorSave());

            // Retriable: propagate so the worker transport redelivers the run with bounded
            // attempts and dead-letters it when they are exhausted — the "run is stuck" alarm.
            throw;
        }

        // The terminal outcome is persisted above; notify OUTSIDE the try/catch so a throwing
        // observer (throwing is the documented crash-injection contract) cannot re-enter those
        // catches and rewrite a Succeeded run as Failed — or overwrite a terminal ledger message
        // with a telemetry error — "the run's outcome is never at stake". A throw from here
        // propagates for redelivery; the replay sees the terminal status, RE-NOTIFIES (so the
        // event that just failed is not lost), and acks.
        if (state.Status == FlowRunStatus.Succeeded)
        {
            SafeLog.Try(
                (Logger: _logger, FlowId: flowId, state.Attempts),
                static s => s.Logger.LogInformation("Durable flow {FlowId} completed successfully (attempt {Attempts}).", s.FlowId, s.Attempts));
        }

        await NotifyRunFinishedAsync(state).ConfigureAwait(false);
        await NotifyParentAsync(state).ConfigureAwait(false);
    }

    /// <summary>
    /// The authoritative look behind a wake-up that is about to be acknowledged because the run
    /// does not read <see cref="FlowRunStatus.Running"/>. Skipping the execution writes nothing, so
    /// no revision or lease fence corrects a stale read behind it (see
    /// <see cref="IFlowStateStore.LoadCurrentAsync"/>): under a reused id a lagging copy can still
    /// show the previous run's terminal ledger while the new run's only wake-up is this delivery.
    /// A <c>null</c> look falls back to <paramref name="read"/> — a ledger that vanished in between
    /// makes skipping harmless, and a test double mocking the store (Moq answers the default
    /// interface member with <c>null</c>) keeps skipping a finished run.
    /// </summary>
    private static async Task<FlowState> ConfirmNotRunningAsync(IFlowStateStore store, string flowId, FlowState read)
        => await store.LoadCurrentAsync(flowId).ConfigureAwait(false) ?? read;

    private int _leaseContentionWaits;

    /// <summary>
    /// Deliveries asleep in the lease-contention poll below — a wait on the engine clock, not user
    /// code (AsyncResponse.Testing's quiescence probe). Counted only once the poll's timer exists.
    /// </summary>
    internal int LeaseContentionWaits => Volatile.Read(ref _leaseContentionWaits);

    /// <summary>
    /// Acquires the execution lease for <paramref name="flowId"/>, retrying while the current
    /// holder's lease window elapses. Returns <c>null</c> when this delivery is safe to ack without
    /// executing (flow terminal/absent, or the lease is held by a demonstrably live worker).
    /// <para>
    /// A held lease alone is NOT proof this delivery is a duplicate: the holder may have died
    /// inside its unexpired lease window, and acking would drop the only wake-up the flow has —
    /// wake-ups would silently become at-most-once and the <see cref="FlowRunStatus.Running"/> run
    /// would strand. Neither is a lease that outlasts THIS host's lease window: the lease in the
    /// way may have been issued by another deployment with a longer
    /// <see cref="DurableFlowOptions.ExecutionLeaseDuration"/>, so a successor configured with a
    /// shorter one used to give up — and ack — before the dead holder's lease had even expired.
    /// </para>
    /// <para>
    /// Proof therefore comes from the store, not from elapsed local configuration
    /// (<see cref="IFlowStateStore.ObserveLeaseAsync"/>): a lease whose owner or expiry changes
    /// while this delivery waits was acquired or renewed by a live worker in the meantime; a lease
    /// that never changes is a dead holder's, and is waited out to its PERSISTED expiry. When the
    /// wait ends with neither proof nor the lease, the delivery is not acknowledged —
    /// <see cref="DurableFlowLeaseContendedException"/> hands it back to the transport.
    /// </para>
    /// <para>
    /// A live holder makes this delivery redundant only when the holder's OWN job is a different
    /// one: that job stays unacknowledged at the broker and is redelivered if the holder dies. A
    /// broker with an in-flight ceiling (Pub/Sub <c>MaxTotalAckExtension</c>, RabbitMQ
    /// <c>consumer_timeout</c>, the SQS 12-hour visibility cap, a Kafka rebalance) redelivers the
    /// holder's own job while its handler is still running, and THAT delivery is the last copy of
    /// the wake-up: acknowledging it leaves nothing to redeliver when the holder's process ends.
    /// The lease records the job that drives it (<see cref="FlowLeaseContention"/>), so such a
    /// delivery is recognised and never acknowledged as a duplicate — it is re-published as the
    /// same job, delayed past the lease, where the transport can delay, and otherwise kept with
    /// the transport.
    /// </para>
    /// </summary>
    private async Task<FlowExecutionLease?> AcquireExecutionLeaseWithRetryAsync(IFlowStateStore store, string flowId)
    {
        // The job driving this execution (null for a direct call, tag-less for a job written
        // before WorkerJobEnvelope.JobId existed): recorded with the lease on acquire, compared
        // with the lease in the way on contention.
        var ownJob = WorkerJobScope.Current;
        var ownJobTag = FlowLeaseContention.JobTag(ownJob?.JobId);

        var lease = await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(
            store,
            flowId,
            _options,
            _logger,
            _timeProvider,
            jobTag: ownJobTag).ConfigureAwait(false);
        if (lease is not null)
            return lease;

        // This host's lease window bounds the wait only until the store says otherwise: the first
        // observation of the lease in the way moves the deadline to a full window past ITS expiry.
        // The 2s poll delay is capped by the renew interval so short test-sized leases still get polled.
        var window = _options.ExecutionLeaseDuration + _options.ExecutionLeaseRenewInterval;
        var startedWaitingUtc = _timeProvider.GetUtcNow().UtcDateTime;
        var deadline = FlowStateRetention.AddSaturating(startedWaitingUtc, window);

        // The store-driven extension below follows DATA this host does not control. A store clock
        // hours ahead of this one, or an expiry column read back shifted, used to move the deadline
        // as far out as the bad value said (saturating at DateTime.MaxValue, i.e. never): the
        // delivery then polled the store every pollDelay for good, pinning its worker slot, and
        // the contention exception whose message says "check for clock skew" was unreachable in
        // exactly the case it names. MaxLeaseContentionWait bounds the extension — never this
        // host's own window above, which is always waited.
        var extensionCeiling = FlowStateRetention.AddSaturating(startedWaitingUtc, _options.MaxLeaseContentionWait);
        var extensionCapped = false;
        var pollDelay = _options.ExecutionLeaseRenewInterval < TimeSpan.FromSeconds(2)
            ? _options.ExecutionLeaseRenewInterval
            : TimeSpan.FromSeconds(2);
        FlowLeaseObservation? baseline = null;
        var storeReportsLeases = true;
        string? ownJobHolderLeaseId = null;

        // The ledger is re-read on the first poll, on the poll after the lease was seen to change
        // (or to have no holder), and otherwise every ~30 s — not on every poll. Only its status is
        // needed, and a full-ledger load per 2 s poll cost a dead holder's every parked wake-up about
        // seventy whole-ledger reads while it waited the lease out. A lease that never changes
        // holds nothing that could turn the run terminal except a lease-less writer (a recovery
        // failure signal, an operator), which the periodic re-read still notices; a store that
        // reports no leases is re-read every poll, as before.
        var ledgerRereadEvery = Math.Max(1, (int)(TimeSpan.FromSeconds(30).Ticks / Math.Max(1, pollDelay.Ticks)));
        var pollsSinceLedgerRead = ledgerRereadEvery;
        FlowLeaseObservation? previousObservation = null;

        while (true)
        {
            if (pollsSinceLedgerRead >= ledgerRereadEvery)
            {
                pollsSinceLedgerRead = 0;

                // Between attempts, look at the state itself: a terminal or absent flow needs no
                // execution, and reporting it accurately beats a misleading "already executing" log.
                var state = await store.LoadAsync(flowId).ConfigureAwait(false);
                if (state is null)
                {
                    SafeLog.Try(
                        (Logger: _logger, FlowId: flowId),
                        static s => s.Logger.LogWarning("Durable flow {FlowId} has no state (unknown, expired, or unreadable); nothing to execute.", s.FlowId));
                    return null;
                }

                if (state.Status != FlowRunStatus.Running)
                    state = await ConfirmNotRunningAsync(store, flowId, state).ConfigureAwait(false);

                if (state.Status != FlowRunStatus.Running)
                {
                    SafeLog.Try(
                        (Logger: _logger, FlowId: flowId, state.Status),
                        static s => s.Logger.LogDebug("Durable flow {FlowId} is already {Status}; skipping duplicate delivery.", s.FlowId, s.Status));
                    // Same at-least-once re-notify as ExecuteAsync's terminal early return: the prior
                    // delivery may have died in the run-finished notification itself.
                    if (state.Status is FlowRunStatus.Succeeded or FlowRunStatus.Failed)
                        await NotifyRunFinishedAsync(state).ConfigureAwait(false);
                    await NotifyParentAsync(state).ConfigureAwait(false);
                    return null;
                }
            }

            pollsSinceLedgerRead++;

            lease = await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(
                store,
                flowId,
                _options,
                _logger,
                _timeProvider,
                jobTag: ownJobTag).ConfigureAwait(false);
            if (lease is not null)
                return lease;

            var observed = storeReportsLeases
                ? await store.ObserveLeaseAsync(flowId).ConfigureAwait(false)
                : null;
            if (observed?.LeaseId is null
                || previousObservation is null
                || !string.Equals(observed.LeaseId, previousObservation.LeaseId, StringComparison.Ordinal)
                || observed.ExpiresAtUtc != previousObservation.ExpiresAtUtc)
            {
                pollsSinceLedgerRead = ledgerRereadEvery;
            }

            previousObservation = observed;
            if (observed is null)
            {
                storeReportsLeases = false;
            }
            else if (observed.LeaseId is not null)
            {
                if (baseline is null)
                {
                    baseline = observed;
                    if (observed.ExpiresAtUtc is { } persistedExpiry)
                    {
                        // A dead holder's lease is acquirable once ITS expiry passes, whichever
                        // deployment's lease duration issued it; the extra window absorbs clock
                        // skew between this host and the store before the wait is declared stuck.
                        var persistedDeadline = FlowStateRetention.AddSaturating(persistedExpiry, window);
                        if (persistedDeadline > extensionCeiling)
                        {
                            persistedDeadline = extensionCeiling;
                            extensionCapped = true;
                        }

                        if (persistedDeadline > deadline)
                            deadline = persistedDeadline;
                    }
                }
                else
                {
                    switch (FlowLeaseContention.Judge(baseline, observed, ownJobTag))
                    {
                        case FlowLeaseContentionVerdict.AcknowledgeDuplicate:
                            // Only a worker that acquired or renewed the lease AFTER this delivery
                            // started waiting can have written that, and the job driving it is
                            // not this one. The premise that makes the ack safe: the holder's OWN
                            // job — a different job — is still unacknowledged at the broker, so
                            // if that holder crashes later the broker redelivers it. The retry
                            // loop exists to cover deliveries that arrive inside a DEAD holder's
                            // unexpired lease window, which broker redelivery alone cannot cover.
                            // Legacy caveat: when either side carries no job identity (a job or a
                            // lease written before WorkerJobEnvelope.JobId existed, mid rolling
                            // upgrade) the two cannot be told apart, and the evidence-based ack
                            // applies as it did before.
                            SafeLog.Try(
                                (Logger: _logger, FlowId: flowId, Evidence: string.Equals(observed.LeaseId, baseline.LeaseId, StringComparison.Ordinal) ? "renewed" : "taken over"),
                                static s => s.Logger.LogDebug(
                                    "Durable flow {FlowId} is executing on another live worker (its lease was {Evidence} while this delivery waited); skipping duplicate delivery.",
                                    s.FlowId,
                                    s.Evidence));
                            return null;

                        case FlowLeaseContentionVerdict.HolderOwnJobRedelivered:
                            // The live holder is executing THIS job: the broker handed it out a
                            // second time while its handler was still running, which only happens
                            // once an in-flight ceiling has lapsed. The first delivery can no
                            // longer be settled, so this one is the last copy of the wake-up —
                            // acknowledging it would leave nothing to redeliver when the holder's
                            // process ends, and the run would stay Running forever.
                            // MaxPublishDelay <= zero: the capability is unavailable in the current
                            // configuration (an SQS FIFO worker queue) — same as not implementing it.
                            var redelay = _workerTransport is IDelayedWorkerTransport delayedTransport
                                && delayedTransport.MaxPublishDelay > TimeSpan.Zero
                                    ? delayedTransport
                                    : null;
                            if (ownJobHolderLeaseId is null)
                            {
                                ownJobHolderLeaseId = observed.LeaseId;
                                AsyncResponseDiagnostics.RecordFlowOwnJobRedelivery(redelay is not null ? "redelayed" : "waiting");
                                SafeLog.Try(
                                    (Logger: _logger, FlowId: flowId, Resolution: redelay is not null
                                        ? "it is re-published as the same job, delayed past the holder's lease"
                                        : "it waits for the lease and is otherwise handed back to the transport"),
                                    static s => s.Logger.LogWarning(
                                        "Durable flow {FlowId} wake-up is a redelivery of the job its live lease holder is still executing: a broker in-flight ceiling lapsed under the running handler " +
                                        "(Google Pub/Sub MaxTotalAckExtension, RabbitMQ consumer_timeout, the SQS 12-hour visibility cap, a Kafka rebalance). It is the only copy of the wake-up the broker still has and is not acknowledged as a duplicate; {Resolution}.",
                                        s.FlowId,
                                        s.Resolution));
                            }

                            if (redelay is not null)
                            {
                                // Publish BEFORE the ack this return causes; a failed publish
                                // propagates and the delivery stays with the transport.
                                await RepublishOwnJobPastLeaseAsync(redelay, ownJob!, observed, flowId).ConfigureAwait(false);
                                return null;
                            }

                            // No delayed delivery: keep waiting on the deadline already set (a
                            // live holder's renewals never extend it) — the lease freeing or the
                            // ledger turning terminal resolves the wait, the deadline throws.
                            break;
                    }
                }
            }

            if (_timeProvider.GetUtcNow().UtcDateTime >= deadline)
                break;

            var poll = Task.Delay(pollDelay, _timeProvider, _hostStopping);
            Interlocked.Increment(ref _leaseContentionWaits);
            try
            {
                await poll.ConfigureAwait(false);
            }
            catch (OperationCanceledException ex)
            {
                // Host shutdown must not leave this delivery parked in the poll — but acking it
                // would silently drop the flow's only wake-up. Propagate as the engine's
                // hand-back signal so the transport treats the job as not executed and redelivers
                // it after restart. The TYPE is the signal: ApplicationStopping fires before any
                // hosted service stops, so the worker subscriber's own token is usually still
                // live here, and a plain cancellation read as a handler failure — a NAK, a retry
                // ladder, a dead-lettered wake-up.
                throw new DurableFlowInterruptedException(
                    $"Host is stopping; durable flow '{flowId}' wake-up is abandoned for redelivery.", ex);
            }
            finally
            {
                Interlocked.Decrement(ref _leaseContentionWaits);
            }
        }

        if (ownJobHolderLeaseId is not null)
        {
            // A live holder WAS proven — and it is executing this delivery's own job, so the proof
            // is no licence to ack. The transport keeps the wake-up; its retry policy paces the
            // redeliveries and its dead-letter queue is the alarm if the holder outlives them.
            throw new DurableFlowLeaseContendedException(
                flowId,
                $"the lease '{ownJobHolderLeaseId}' is held by a live execution of this same worker job: the broker redelivered a job whose handler is still running, so a broker in-flight ceiling lapsed under it " +
                "(Google Pub/Sub MaxTotalAckExtension, RabbitMQ consumer_timeout, the SQS 12-hour visibility cap, or a Kafka rebalance re-fetching an unstored offset). " +
                "This delivery is the only copy of the wake-up the broker still has, so it is never acknowledged as a duplicate, and the registered worker transport cannot re-publish it delayed past the holder's lease; " +
                "keep a single in-process wait shorter than the broker's in-flight ceiling, or raise the ceiling");
        }

        // No lease and no proof of a live holder. Acknowledging here is what stranded runs behind
        // a lease issued under a longer configuration; the transport keeps the wake-up instead.
        throw new DurableFlowLeaseContendedException(
            flowId,
            !storeReportsLeases
                ? $"the flow state store does not report leases ({nameof(IFlowStateStore)}.{nameof(IFlowStateStore.ObserveLeaseAsync)} returned null), and the lease stayed held through this host's whole lease window of {window}"
                : baseline is null
                    ? $"the lease stayed unacquirable through this host's whole lease window of {window} although the store reports no holder"
                    : extensionCapped
                        ? $"the lease held by '{baseline.LeaseId}' (persisted expiry {baseline.ExpiresAtUtc:O}) neither changed nor became acquirable within {nameof(DurableFlowOptions)}.{nameof(DurableFlowOptions.MaxLeaseContentionWait)} ({_options.MaxLeaseContentionWait}), and its persisted expiry lies further out than that budget lets one delivery wait; raise the budget if a deployment legitimately issues leases that long, otherwise check for clock skew between this host and the store"
                        : $"the lease held by '{baseline.LeaseId}' (persisted expiry {baseline.ExpiresAtUtc:O}) neither changed nor became acquirable within a full lease window past that expiry; check for clock skew between this host and the store");
    }

    /// <summary>
    /// Re-publishes <paramref name="job"/> — the job this delivery AND the live lease holder both
    /// carry — so that it comes back about when the holder's lease would lapse if the holder died
    /// now. The copy keeps the <see cref="WorkerJobEnvelope.JobId"/>: a fresh id would read as a
    /// different, redundant job at its next contention and be acknowledged on the holder's
    /// renewal, which is the loss this exists to prevent. The hop repeats at lease cadence while
    /// the holder lives; it finds a terminal ledger and acks once the holder finishes, or an
    /// expired lease it takes over once the holder dies.
    /// </summary>
    private async Task RepublishOwnJobPastLeaseAsync(
        IDelayedWorkerTransport transport,
        WorkerJobEnvelope job,
        FlowLeaseObservation holderLease,
        string flowId)
    {
        var nowUtc = _timeProvider.GetUtcNow().UtcDateTime;

        // One renew interval past the persisted expiry, so a holder that is still alive has
        // visibly renewed by the time the hop lands. The expiry is store data this host does not
        // control (see MaxLeaseContentionWait above): bounded by that budget and by what the
        // transport can delay in one publish. Coming back early costs one more hop, never the run.
        var delay = (holderLease.ExpiresAtUtc is { } expiresAtUtc && expiresAtUtc > nowUtc
                ? expiresAtUtc - nowUtc
                : TimeSpan.Zero)
            + _options.ExecutionLeaseRenewInterval;
        if (delay > _options.MaxLeaseContentionWait)
            delay = _options.MaxLeaseContentionWait;
        if (delay > transport.MaxPublishDelay)
            delay = transport.MaxPublishDelay;

        var hop = CopyForRedelay(job, FlowStateRetention.AddSaturating(nowUtc, delay));
        await transport.PublishAsync(hop, delay).ConfigureAwait(false);

        // Guarded: the copy is published. A throw here failed (NAKed) this delivery while its copy
        // already existed — both carry the holder's JobId, so neither is ever acknowledged as a
        // duplicate, and under a persistently throwing provider every lease cadence doubled them.
        SafeLog.Try(
            (Logger: _logger, FlowId: flowId, hop.NotBeforeUtc, Delay: delay),
            static s => s.Logger.LogInformation(
                "Durable flow {FlowId} wake-up re-published as the same job, due {NotBeforeUtc:O} ({Delay} from now, past the live holder's lease); acknowledging this delivery.",
                s.FlowId,
                s.NotBeforeUtc,
                s.Delay));
    }

    /// <summary>
    /// The same job with a new due time. A copy, not the delivered instance: the transport still
    /// owns that one (the in-memory transport retries it as-is), and a due time stamped on it
    /// would outlive a failed publish. The stall counters are left behind on purpose — they
    /// belong to the due-time chain that ended with this delivery.
    /// </summary>
    internal static WorkerJobEnvelope CopyForRedelay(WorkerJobEnvelope job, DateTime notBeforeUtc) => new()
    {
        SchemaVersion = job.SchemaVersion,
        Call = job.Call,
        CorrelationId = job.CorrelationId,
        ReplyTarget = job.ReplyTarget,
        Context = job.Context,
        JobId = job.JobId,
        NotBeforeUtc = notBeforeUtc
    };

    /// <inheritdoc />
    public async Task CreateAndExecuteAsync(string flowId, string initialStateJson)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentException.ThrowIfNullOrWhiteSpace(initialStateJson);
        var deliveryStartedUtc = _timeProvider.GetUtcNow().UtcDateTime;

        // The carrier is the ledger wire format itself. A carrier this build cannot read is
        // deterministic: FlowStateUnreadableException propagates to the transport's retry and
        // dead-letter policy, which is the alarm — the same treatment an unreadable stored ledger
        // gets in ExecuteAsync, and for the same reason (acknowledging it would lose the start).
        var initial = FlowStateJson.Deserialize(initialStateJson, flowId);
        if (!string.Equals(initial.FlowId, flowId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"The start job for durable flow '{DiagnosticText.EscapedExcerpt(flowId)}' carries initial state for '{DiagnosticText.EscapedExcerpt(initial.FlowId ?? string.Empty)}'; refusing to create a ledger under the wrong id.");
        }

        await using (var scope = _scopeFactory.CreateAsyncScope())
        {
            var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();
            var registered = initial.FlowTypeName is not null && _registrations.TryGetValue(TypeNameIdentity.Normalize(initial.FlowTypeName)!, out var registration)
                ? registration
                : null;
            var serviceProvider = scope.ServiceProvider;

            bool IsSameStart(FlowState existing)
            {
                Func<string?, string, bool> inputEquivalent = registered is not null
                    ? registered.InputEquivalent
                    : (persisted, requested) => ReflectedInputEquivalent(serviceProvider, existing, persisted, requested);
                return FlowStateConcurrency.IsSameStart(existing, initial.FlowTypeName, initial.InputTypeName, initial.InputJson, inputEquivalent);
            }

            // A start job outlives the run it started: a dead-letter replay, or a Kafka consumer
            // group rewound past it, delivers it again long after the run finished and its ledger
            // expired. Insert-if-absent then succeeds, and the "new" run re-executes every step —
            // and every side effect — of work that completed weeks ago. A start older than the
            // ledger lifetime with NO ledger behind it is that replay (or a start that never ran
            // and whose ledger would itself have expired by now, which an ExecuteAsync wake-up
            // would equally find gone): dropped, loudly. Checked only when the ledger is absent —
            // a live run past StateExpiry (a long park keeps its ledger through the retention
            // floor) still owns this job as its wake-up and takes the ordinary path below.
            for (var createAttempt = 1; ; createAttempt++)
            {
                if (StartedBeyondStateExpiry(initial, out var age)
                    && await store.LoadAsync(flowId).ConfigureAwait(false) is null)
                {
                    SafeLog.Try(
                        (Logger: _logger, FlowId: flowId, initial.FlowTypeName, Age: age, StateExpiry: _options.StateExpiry),
                        static s => s.Logger.LogError(
                            "Durable flow {FlowId} ({FlowType}) start job dropped: the start is {Age} old — past {StateExpiryOption} ({StateExpiry}) — and no ledger exists, so the run it started has finished and expired (or never ran within its ledger's lifetime). Re-creating it would re-execute completed work; start the flow again if it is still wanted.",
                            s.FlowId, AsyncResponseTypeResolution.DescribeForDiagnostics(s.FlowTypeName), s.Age, $"{nameof(DurableFlowOptions)}.{nameof(DurableFlowOptions.StateExpiry)}", s.StateExpiry));
                    return;
                }

                if (await FlowStateConcurrency.TryCreateAsync(store, flowId, initial, _options.StateExpiry).ConfigureAwait(false))
                {
                    // The starter died (or has not got there yet) between its publish and its own
                    // create: the job is the durable record of the start, so the ledger comes from it.
                    // Guarded: the ledger is created. A throw here dead-lettered (under a persistent
                    // throw) a start job whose Running, never-executed ledger nothing would wake —
                    // the orphan publish-first exists to prevent.
                    SafeLog.Try(
                        (Logger: _logger, FlowId: flowId, initial.FlowTypeName),
                        static s => s.Logger.LogInformation("Durable flow {FlowId} ({FlowType}) ledger created from its start job.", s.FlowId, AsyncResponseTypeResolution.DescribeForDiagnostics(s.FlowTypeName)));
                    break;
                }

                // A plain load decides the common case — StartAsync publishes before its own
                // create, so this job's create normally loses to the starter's — and executing ends
                // in lease- and revision-fenced writes. It is read again currently only where its
                // answer would otherwise end the job: a store whose loads can lag behind another
                // process's writes (Cosmos session reads) may not show this process the starter's
                // fresh create yet (each such miss cost a create round trip, and three of them
                // handed a perfectly good start back to the transport), or may still show an older
                // run under a reused id, and the drop below writes nothing for a fence to correct.
                var existing = await store.LoadAsync(flowId).ConfigureAwait(false);
                if (existing is null || !IsSameStart(existing))
                    existing = await store.LoadCurrentAsync(flowId).ConfigureAwait(false);

                if (existing is null)
                {
                    // The create lost to a ledger that is gone by the time it is read: a previous
                    // run under a reused id expiring (or being pruned) right between the two calls —
                    // every store has that window, and some widen it. Acknowledging here lost the
                    // new run outright (the starter, for its part, leaves the create to this job),
                    // so the create is simply tried again; a start that has meanwhile aged past the
                    // ledger lifetime is still dropped above. Bounded: a store that keeps answering
                    // "exists" for a row it cannot load is a store fault, and the transport's retry
                    // and dead-letter policy is the alarm for it.
                    if (createAttempt < MaxStartCreateAttempts)
                    {
                        SafeLog.Try(
                            (Logger: _logger, FlowId: flowId, Attempt: createAttempt + 1),
                            static s => s.Logger.LogDebug(
                                "Durable flow {FlowId} start job lost its create to a ledger that is already expired or gone; creating again (attempt {Attempt}).",
                                s.FlowId, s.Attempt));
                        continue;
                    }

                    throw new InvalidOperationException(
                        $"The start job for durable flow '{flowId}' could neither create its ledger nor load the one the store reports as existing, " +
                        $"on each of {MaxStartCreateAttempts} attempts; the start is handed back to the worker transport.");
                }

                if (!IsSameStart(existing))
                {
                    // The id was reused for different work. The starter that published this job
                    // saw the same conflict on its own create and threw DurableFlowIdConflictException
                    // to its caller (a caller-chosen id already bound is refused before anything is
                    // published at all); executing the EXISTING run here would wake a flow nobody
                    // asked to wake, and a second one cannot live under the same id. Drop the job,
                    // loudly.
                    SafeLog.Try(
                        (Logger: _logger, FlowId: flowId, Existing: existing.FlowTypeName, Requested: initial.FlowTypeName),
                        static s => s.Logger.LogError(
                            "Durable flow {FlowId} start job dropped: the id is already bound to flow type {ExistingFlowType} with different input, not {RequestedFlowType}. Idempotent retries must use the same flow type, input type, and semantically identical input.",
                            s.FlowId,
                            AsyncResponseTypeResolution.DescribeForDiagnostics(s.Existing),
                            AsyncResponseTypeResolution.DescribeForDiagnostics(s.Requested)));
                    return;
                }

                // Same start, ledger already there (the starter's own create won, or this is a
                // redelivery / an idempotent re-start of a live run): fall through and execute it.
                break;
            }
        }

        await ExecuteCoreAsync(flowId, deliveryStartedUtc).ConfigureAwait(false);
    }

    /// <summary>
    /// How often a start job tries its create when each attempt loses to a ledger that is gone by
    /// the time it is read (see <see cref="CreateAndExecuteAsync"/>).
    /// </summary>
    internal const int MaxStartCreateAttempts = 3;

    /// <summary>
    /// Whether the start job's carried ledger was stamped longer ago than a ledger lives
    /// (<see cref="DurableFlowOptions.StateExpiry"/> — every save, the terminal one included,
    /// retains it for at least that long past <see cref="FlowState.CreatedAtUtc"/>). A carrier
    /// without the stamp is never judged: there is nothing to measure.
    /// </summary>
    private bool StartedBeyondStateExpiry(FlowState initial, out TimeSpan age)
    {
        age = TimeSpan.Zero;
        if (initial.CreatedAtUtc is not { } createdAt)
            return false;

        var createdAtUtc = createdAt.Kind == DateTimeKind.Local ? createdAt.ToUniversalTime() : createdAt;
        age = _timeProvider.GetUtcNow().UtcDateTime - createdAtUtc;
        return age > _options.StateExpiry;
    }

    /// <inheritdoc />
    public async Task ResumeAsync(string flowId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

        var state = await store.LoadAsync(flowId).ConfigureAwait(false);

        // Ignoring the resume writes nothing, so no fence corrects a stale read behind it: a store
        // whose loads can serve an older copy of a present ledger (Cosmos session reads from
        // another process) may still show the Suspended run an operator has just set back to
        // Running. Look again, authoritatively, before dropping the resume.
        if (state is not null && state.Status != FlowRunStatus.Running)
            state = await store.LoadCurrentAsync(flowId).ConfigureAwait(false);

        if (state is null)
        {
            SafeLog.Try(
                (Logger: _logger, FlowId: flowId),
                static s => s.Logger.LogWarning("Durable flow {FlowId} cannot resume: no state (unknown, expired, or unreadable).", s.FlowId));
            return;
        }

        if (state.Status != FlowRunStatus.Running)
        {
            SafeLog.Try(
                (Logger: _logger, FlowId: flowId, state.Status),
                static s => s.Logger.LogDebug("Durable flow {FlowId} is already {Status}; ignoring resume.", s.FlowId, s.Status));
            return;
        }

        SafeLog.Try(
            (Logger: _logger, FlowId: flowId),
            static s => s.Logger.LogDebug("Durable flow {FlowId} resuming via worker transport.", s.FlowId));
        await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(flowId)).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RecoverAsync(string flowId, object payload, string correlationId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();
        var checkpointed = false;
        var running = false;
        var lastStatus = FlowRunStatus.Running;
        string? recoveredStep = null;
        FlowState? checkpointedState = null;
        long sizeBefore = 0;

        bool CheckpointRecovered(FlowState state)
        {
            checkpointed = false;
            recoveredStep = null;
            checkpointedState = null;
            lastStatus = state.Status;
            running = state.Status == FlowRunStatus.Running;

            // Suspended runs still CHECKPOINT the recovered terminal payload — the response
            // exists nowhere else once this callback returns — but are never woken (see
            // below): suspension means an operator took manual control, and ResumeAsync
            // continues from the checkpoint instead of re-running the remote step.
            var checkpointable = running || state.Status == FlowRunStatus.Suspended;
            if (!checkpointable || state.Steps is null)
                return false;

            // A step that FAULTED on this id is not pending on it (see
            // DurableFlowContext.IsAwaitingResponse): the engine restarts it fresh, and flow code may
            // already have caught the fault and moved on — completing it here rewrote that history,
            // and the next replay took the other branch. Such a response falls through to the
            // no-pending-step path below, like any other stale one.
            var pending = state.Steps.FirstOrDefault(pair => DurableFlowContext.IsAwaitingResponse(pair.Value, correlationId));
            if (pending.Value is null)
                return false;

            sizeBefore = FlowStateSize.Take(state);
            pending.Value.Completed = true;
            pending.Value.ResultJson = SerializeRecoveredResult(payload, pending.Value.PendingPayloadTypeFullName);
            pending.Value.PendingCorrelationId = null;
            pending.Value.PendingPayloadTypeFullName = null;
            pending.Value.Faulted = false;
            pending.Value.Message = "Terminal response recovered after subscriber loss.";
            pending.Value.CompletedAtUtc = _timeProvider.GetUtcNow().UtcDateTime;
            state.LastMessage = $"Step '{pending.Key}' recovered after subscriber loss.";
            checkpointed = true;
            recoveredStep = pending.Key;
            checkpointedState = state;
            return true;
        }

        var found = await FlowStateConcurrency.MutateAsync(store, flowId, _options.StateExpiry, _timeProvider, CheckpointRecovered)
            .ConfigureAwait(false);

        // Nothing was checkpointed. That conclusion writes NOTHING — no revision fence stands
        // behind it to correct a stale read — and the callback is acknowledged with the payload
        // gone. A store whose loads can serve an older copy of a present ledger (Cosmos session
        // reads from a process that never received the holder's session token) may simply not show
        // this process the breadcrumb yet: look again, authoritatively, before concluding.
        //
        // Whatever status the first look reported, terminal ones included. A finished run's id can
        // be reused — its ledger deleted, a new run started under the same id — and a lagging copy
        // then still shows the PREVIOUS run, Succeeded or Failed, while the new one waits on this
        // very response. Trusting that copy consumed the response against the wrong generation of
        // the ledger: the registration was deleted and the new run stayed pending until its
        // timeout.
        if (found && !checkpointed)
        {
            found = await FlowStateConcurrency.MutateAsync(
                    new CurrentReadFlowStateStore(store), flowId, _options.StateExpiry, _timeProvider, CheckpointRecovered)
                .ConfigureAwait(false);
        }

        if (!found)
        {
            SafeLog.Try(
                (Logger: _logger, FlowId: flowId, CorrelationId: correlationId),
                static s => s.Logger.LogWarning("Durable flow {FlowId} cannot recover response {CorrelationId}: no state found.", s.FlowId, s.CorrelationId));
            return;
        }

        if (!checkpointed)
        {
            if (!running)
            {
                SafeLog.Try(
                    (Logger: _logger, FlowId: flowId, Status: lastStatus, CorrelationId: correlationId),
                    static s => s.Logger.LogDebug("Durable flow {FlowId} is {Status}; ignoring recovered correlationId {CorrelationId}.", s.FlowId, s.Status, s.CorrelationId));
                return;
            }

            // Still Running with no matching pending step: a previous delivery may have crashed
            // between checkpointing this response and enqueueing the run, making this redelivery
            // the only remaining wake-up. Re-enqueue instead of dropping — it is idempotent, and
            // worst case the job finds a live holder's lease and acks as a duplicate.
            SafeLog.Try(
                (Logger: _logger, FlowId: flowId, CorrelationId: correlationId),
                static s => s.Logger.LogDebug("Durable flow {FlowId} has no pending step for recovered correlationId {CorrelationId}; re-enqueueing execution to cover a checkpoint/enqueue crash window.", s.FlowId, s.CorrelationId));
            await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(flowId)).ConfigureAwait(false);
            return;
        }

        // A recovered response can be the write that takes the ledger past a warning band, and no
        // context saw it: the next execution seeds its warning above the ledger's new size.
        // Self-guarding: the checkpoint is committed, and the step-completed event and wake-up
        // below must follow it.
        DurableFlowContext.WarnIfWriteCrossedLedgerWarning(_logger, _options, checkpointedState!, sizeBefore);

        // The completion recorded here is the ONLY chance observers get to see this step finish:
        // the replayed execution short-circuits the now-memoized step without notifying. Notified
        // before the wake-up is enqueued so observers (e.g. the Testing probe's step waiters) see
        // the completion before the resumed run races past it. Best-effort by necessity: once the
        // checkpoint settles, the pending correlation id is gone, and a redelivered RecoverAsync
        // can no longer tell which step this response completed — an observer that throws here
        // loses the event (unlike run-finished, which re-derives from the persisted status).
        await NotifyStepCompletedAsync(flowId, recoveredStep!, correlationId).ConfigureAwait(false);

        if (!running)
        {
            SafeLog.Try(
                (Logger: _logger, FlowId: flowId, CorrelationId: correlationId),
                static s => s.Logger.LogInformation(
                    "Durable flow {FlowId} checkpointed recovered correlationId {CorrelationId} while Suspended; not waking the run — ResumeAsync continues from the checkpoint.",
                    s.FlowId, s.CorrelationId));
            return;
        }

        SafeLog.Try(
            (Logger: _logger, FlowId: flowId, CorrelationId: correlationId),
            static s => s.Logger.LogDebug("Durable flow {FlowId} checkpointed recovered correlationId {CorrelationId}; resuming.", s.FlowId, s.CorrelationId));
        await _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(flowId)).ConfigureAwait(false);
    }

    private async Task NotifyStepCompletedAsync(string flowId, string stepName, string correlationId)
    {
        if (_observers.Length == 0)
            return;

        var stepEvent = new DurableFlowStepEvent(flowId, stepName, DurableFlowStepKind.Awaited, correlationId, WakeAtUtc: null);
        foreach (var observer in _observers)
            await observer.OnStepCompletedAsync(stepEvent).ConfigureAwait(false);
    }

    /// <summary>
    /// Serializes a recovered terminal payload for the step checkpoint AS THE STEP'S DECLARED
    /// response type (recorded at await time): replay deserializes the checkpoint as that declared
    /// type, and the recovered payload's runtime type may be a <c>[JsonPolymorphic]</c> derived
    /// whose runtime-type serialization omits the discriminator — breaking every replay against an
    /// abstract declared base, or silently truncating against a concrete one. Falls back to the
    /// runtime type when the recorded name is absent (ledgers written before it existed) or no
    /// longer resolves to a compatible type.
    /// </summary>
    private static string SerializeRecoveredResult(object payload, string? declaredTypeFullName)
    {
        var declaredType = declaredTypeFullName is null
            ? null
            : PayloadRecoveryClassifier.ResolvePayloadType(declaredTypeFullName);

        return declaredType is not null && declaredType.IsInstanceOfType(payload)
            ? AsyncResponseJson.Serialize(payload, declaredType)
            : AsyncResponseJson.Serialize(payload, payload.GetType());
    }

    /// <inheritdoc />
    public Task FailAsync(string flowId, Exception exception)
        => FailCoreAsync(flowId, exception, correlationId: null);

    public Task FailAsync(string flowId, Exception exception, string correlationId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        return FailCoreAsync(flowId, exception, correlationId);
    }

    private async Task FailCoreAsync(string flowId, Exception exception, string? correlationId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentNullException.ThrowIfNull(exception);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

        FlowState? updated = null;
        var failedNow = false;
        var stale = false;
        bool MarkFailed(FlowState state)
        {
            updated = state;
            failedNow = false;
            stale = false;
            if (state.Status != FlowRunStatus.Running)
                return false;

            // A correlation-scoped failure only counts against the step still pending on
            // that id (RecoverAsync parity): a dead worker's registration outlives the
            // replacement's, so a late error for a superseded or already-settled correlation
            // id must not fail a run that is live on another one. Nor one whose step FAULTED on
            // that id (DurableFlowContext.IsAwaitingResponse): the breadcrumb outlives the fault,
            // but the engine restarts the step fresh — and a flow that caught the fault (a
            // best-effort step) has already moved on, so a late remote failure behind it failed a
            // healthy run terminally; a flow that did not catch it is in the transport's retry
            // backoff, about to restart the step.
            if (correlationId is not null
                && (state.Steps is null
                    || !state.Steps.Values.Any(step => DurableFlowContext.IsAwaitingResponse(step, correlationId))))
            {
                stale = true;
                return false;
            }

            state.Status = FlowRunStatus.Failed;
            state.LastMessage = exception.Message;
            failedNow = true;
            return true;
        }

        var found = await FlowStateConcurrency.MutateAsync(store, flowId, _options.StateExpiry, _timeProvider, MarkFailed)
            .ConfigureAwait(false);

        // Ignoring the signal is concluded without a write, so nothing fences it: a store whose
        // loads can serve an older copy of a present ledger may not show this process the step now
        // pending on this id (RecoverAsync parity). Look again, authoritatively, before ignoring
        // the failure. Likewise a run that reads Suspended — an operator may just have set it back
        // to Running, and a lagging copy would drop the failure its resumed run still waits on —
        // and one that reads Succeeded or Failed: under a reused id that copy can be the PREVIOUS
        // run's, and acting on it both dropped the failure the new run waits on and re-notified
        // observers and the parent of an outcome that belongs to a ledger already deleted.
        if (found && !failedNow)
        {
            var confirmed = await FlowStateConcurrency.MutateAsync(
                    new CurrentReadFlowStateStore(store), flowId, _options.StateExpiry, _timeProvider, MarkFailed)
                .ConfigureAwait(false);

            // A ledger that is gone by the second look leaves a terminal first look standing: the
            // run did finish, and the at-least-once re-notify below is all that is left to do for
            // it. It also keeps a test double mocking the store — Moq answers the default
            // interface member with null — re-notifying a finished run instead of reporting it
            // unknown. Not when the second pass marked the run failed and then lost the ledger
            // under its write (failedNow): that status was never persisted, and notifying it told
            // observers and the parent of a failure that exists nowhere.
            found = confirmed
                || (!failedNow && updated is { Status: FlowRunStatus.Succeeded or FlowRunStatus.Failed });
        }

        if (!found || updated is null)
        {
            SafeLog.Try(
                (Logger: _logger, FlowId: flowId),
                static s => s.Logger.LogWarning("Durable flow {FlowId} cannot be failed: no state (unknown, expired, or unreadable).", s.FlowId));
            return;
        }

        if (stale)
        {
            SafeLog.Try(
                (Logger: _logger, FlowId: flowId, CorrelationId: correlationId),
                static s => s.Logger.LogDebug("Durable flow {FlowId} has no step pending on correlationId {CorrelationId}; ignoring stale failure signal.", s.FlowId, s.CorrelationId));
            return;
        }

        if (!failedNow)
        {
            SafeLog.Try(
                (Logger: _logger, FlowId: flowId, updated.Status),
                static s => s.Logger.LogDebug("Durable flow {FlowId} is already {Status}; ignoring failure signal.", s.FlowId, s.Status));
            // At-least-once re-notify, as in ExecuteAsync: the prior delivery of this failure
            // signal may have marked the run and then died in its own run-finished notification.
            if (updated.Status is FlowRunStatus.Succeeded or FlowRunStatus.Failed)
                await NotifyRunFinishedAsync(updated).ConfigureAwait(false);
            await NotifyParentAsync(updated).ConfigureAwait(false);
            return;
        }

        // Run-finished observers BEFORE the parent wake-up, matching ExecuteAsync and the
        // already-terminal branch above: enqueueing the parent first would let it resume and
        // finish before this run's terminal event is recorded, and an observer throw after the
        // parent was already notified would re-enqueue the parent a second time on redelivery.
        await NotifyRunFinishedAsync(updated).ConfigureAwait(false);
        await NotifyParentAsync(updated).ConfigureAwait(false);

        // Guarded: everything the signal had to do is done; a throw here turned it into a
        // RecoveryCallbackFailedException, a redelivered signal, and a second parent wake-up.
        SafeLog.Try(
            (Logger: _logger, Error: exception, FlowId: flowId),
            static s => s.Logger.LogWarning(s.Error, "Durable flow {FlowId} failed via lost-subscriber routing: {Message}", s.FlowId, s.Error.Message));
    }

    private DurableFlowContext CreateContext(
        IFlowStateStore store,
        FlowState state,
        FlowExecutionLease lease,
        DateTime deliveryStartedUtc)
        => new(
            state,
            store,
            _builder,
            _propagation,
            _options,
            _subscriber,
            _recoverableSubscriber,
            _logger,
            lease,
            _timeProvider,
            _observers,
            _workerTransport,
            _channelDefaultWaitTimeout,
            _hostStopping,
            deliveryStartedUtc);

    private async Task<bool> InvokeFlowAsync(
        IServiceProvider serviceProvider,
        FlowState state,
        DurableFlowContext context)
    {
        // Statically-typed path for flows registered via WithDurableFlow<TFlow, TInput>(): no
        // type-name resolution, no MakeGenericType, no MethodInfo.Invoke — the path trimmed and
        // Native AOT apps rely on.
        if (state.FlowTypeName is not null && _registrations.TryGetValue(TypeNameIdentity.Normalize(state.FlowTypeName)!, out var registration))
        {
            // Fail closed exactly like the reflection fallback: a run persisted with a different
            // input type than the registration's TInput would otherwise silently parse the old
            // payload as the new type — renamed or added members become defaults — and execute
            // the remaining steps against wrong input. Null tolerated: ledgers written before the
            // stamp existed carry no input type name.
            if (state.InputTypeName is not null
                && !TypeNameIdentity.Same(state.InputTypeName, registration.InputTypeFullName))
            {
                throw new InvalidOperationException(
                    $"Durable flow type '{state.FlowTypeName}' is registered with input type '{registration.InputTypeFullName}', " +
                    // Store data, rendered like ResolveType's names (below): raw, a ledger's type name
                    // copied megabytes of store-written text or its line breaks into this message.
                    $"but the persisted run carries input type '{AsyncResponseTypeResolution.DescribeForDiagnostics(state.InputTypeName)}'; the flow state was written by an " +
                    "incompatible flow definition.");
            }

            var flow = ResolveFlowFromDi(serviceProvider, registration.FlowType);
            var input = state.InputJson is null ? null : registration.DeserializeInput(state.InputJson);
            try
            {
                await registration.ExecuteAsync(flow, context, input).ConfigureAwait(false);
            }
            catch (Exception ex) when (context.IsSuspended && ex is not DurableFlowSuspendedException)
            {
                throw ParkOutranConversion(state, ex);
            }
            catch (Exception ex) when (context.Interruption is { } interruption && ex is not DurableFlowInterruptedException)
            {
                InterruptionOutranConversion(state, ex);
                interruption.Throw();
                throw;
            }

            await context.FlushProgressAsync().ConfigureAwait(false);
            return context.IsSuspended;
        }

        return await InvokeFlowByReflectionAsync(serviceProvider, state, context).ConfigureAwait(false);
    }

    /// <summary>
    /// A park committed — its wake-up is published and its lease released — and flow code then
    /// turned the park's cancellation into another exception (<c>catch (OperationCanceledException)
    /// { throw new DurableFlowFailedException(...); }</c> around a context call, or a wrapping
    /// rethrow). Taken at face value that failed the run terminally with its child still running,
    /// or retried a delivery whose continuation was already queued — and saved through a lease the
    /// park had released. The park stands: the conversion is logged and the execution ends
    /// suspended.
    /// </summary>
    private DurableFlowSuspendedException ParkOutranConversion(FlowState state, Exception converted)
    {
        // Guarded: this builds the exception that REPLACES the conversion. A throwing logging
        // provider's exception replaced it instead, and the committed park read as a failed
        // delivery — retried, re-parked, publishing another wake-up each time.
        SafeLog.Try(
            (Logger: _logger, Error: converted, state.FlowId),
            static s => s.Logger.LogWarning(
                s.Error,
                "Durable flow {FlowId} parked (its wake-up is already published), but flow code converted the park's cancellation into {ExceptionType}; the run stays parked. Let DurableFlowInterruptedException — an OperationCanceledException — propagate from context calls.",
                s.FlowId,
                s.Error.GetType().FullName));
        return new DurableFlowSuspendedException(state.LastMessage ?? $"Flow {state.FlowId} is suspended.");
    }

    /// <summary>
    /// The host-stop counterpart of <see cref="ParkOutranConversion"/>: an in-process timer was
    /// interrupted and handed back — no wake-up exists, the delivery must be redelivered — and flow
    /// code turned the interruption into another exception. A <see cref="DurableFlowFailedException"/>
    /// taken at face value failed the run terminally on a deploy; any other type read as a handler
    /// failure, costing the wake-up a broker delivery attempt and in the end dead-lettering it. The
    /// caller rethrows the interruption itself, which the transport recognises as a hand-back.
    /// </summary>
    private void InterruptionOutranConversion(FlowState state, Exception converted)
        // Guarded: the caller rethrows the interruption next — its TYPE is the hand-back signal,
        // and a throwing logging provider's exception escaped in its place.
        => SafeLog.Try(
            (Logger: _logger, Error: converted, state.FlowId),
            static s => s.Logger.LogWarning(
                s.Error,
                "Durable flow {FlowId} was interrupted by host stop (its delivery is handed back for redelivery), but flow code converted the interruption into {ExceptionType}; the delivery is still handed back. Let DurableFlowInterruptedException — an OperationCanceledException — propagate from context calls.",
                s.FlowId,
                s.Error.GetType().FullName));

    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Reflection fallback for flows not registered via WithDurableFlow<TFlow, TInput>(). In a trimmed app an " +
                        "unregistered flow fails closed here with an actionable error telling the operator to register it; nothing " +
                        "is silently misexecuted.")]
    [UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "Same fallback contract: the flow type and IDurableFlow<TInput> instantiation exist whenever the app " +
                        "actually defines and starts the flow; otherwise resolution fails closed with guidance.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "MakeGenericType over the flow's input type re-materializes an interface instantiation the user's flow " +
                        "class already implements statically; flows whose types were trimmed fail closed with guidance to use " +
                        "WithDurableFlow<TFlow, TInput>().")]
    private async Task<bool> InvokeFlowByReflectionAsync(
        IServiceProvider serviceProvider,
        FlowState state,
        DurableFlowContext context)
    {
        var flowType = ResolveType(state.FlowTypeName, "flow");
        var inputType = ResolveType(state.InputTypeName, "input");

        var contract = typeof(IDurableFlow<>).MakeGenericType(inputType);

        // The TYPE is checked before anything is resolved: the ledger's FlowTypeName is store data,
        // and resolving first constructed (and later disposed) whatever DI service it named — any
        // registered type's constructor ran on flow-store content before the check rejected it,
        // the same exposure the input type is bounded against below. The instance check stays for
        // registrations whose factory returns something other than the service type.
        if (!contract.IsAssignableFrom(flowType))
            throw FlowContractMismatch(flowType, inputType);

        var flow = ResolveFlowFromDi(serviceProvider, flowType);
        if (!contract.IsInstanceOfType(flow))
            throw FlowContractMismatch(flowType, inputType);

        // Deserialize only AFTER the contract check above has passed: the ledger's InputTypeName is
        // attacker-controlled to anyone who can write the flow store, and STJ construction runs
        // setters/converters/[JsonConstructor] on whatever type it materializes. Requiring a
        // DI-registered flow that implements IDurableFlow<inputType> first bounds the constructible
        // set to input types the application actually declared, instead of any loadable CLR type.
        var input = state.InputJson is null ? null : JsonSafety.SafeDeserialize(state.InputJson, inputType);

        var execute = contract.GetMethod(nameof(IDurableFlow<object>.ExecuteAsync))!;
        try
        {
            try
            {
                await ((Task)execute.Invoke(flow, [context, input])!).ConfigureAwait(false);
            }
            catch (System.Reflection.TargetInvocationException ex) when (ex.InnerException is not null)
            {
                // A synchronously-thrown flow exception arrives wrapped; unwrap so terminal
                // DurableFlowFailedException handling (and user-visible stack traces) see the real one.
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                throw;
            }
        }
        catch (Exception ex) when (context.IsSuspended && ex is not DurableFlowSuspendedException)
        {
            throw ParkOutranConversion(state, ex);
        }
        catch (Exception ex) when (context.Interruption is { } interruption && ex is not DurableFlowInterruptedException)
        {
            InterruptionOutranConversion(state, ex);
            interruption.Throw();
            throw;
        }

        await context.FlushProgressAsync().ConfigureAwait(false);
        return context.IsSuspended;
    }

    /// <summary>
    /// The start job's input comparison for a flow with no <see cref="DurableFlowRegistration"/> (one
    /// started through <c>IDurableFlows.StartAsync</c> but executed by reflection) — the counterpart of
    /// <see cref="DurableFlowRegistration.InputEquivalent"/>. The starter always compares by value (it
    /// knows <c>TInput</c>); comparing the JSON shape here instead made the two disagree once the input
    /// type gained a member: the starter logged "re-enqueues the existing run" and this job, reading
    /// <c>{"TenantId":7}</c> and <c>{"TenantId":7,"Region":null}</c> as different work, was dropped.
    /// <para>
    /// Both inputs are read as the ledger's input type and written back the same way — but only once
    /// the checks <see cref="InvokeFlowByReflectionAsync"/> makes before IT deserializes anything have
    /// passed, without constructing the flow: the type names are store data, so the input type must be
    /// one that a DI-registered flow class declares through <c>IDurableFlow&lt;TInput&gt;</c>. A name
    /// that does not resolve or pass those checks leaves the shape comparison standing; an input
    /// today's type cannot read is a mismatch, never an exception.
    /// </para>
    /// </summary>
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Same reflection fallback as InvokeFlowByReflectionAsync, reached only for flows not registered via " +
                        "WithDurableFlow<TFlow, TInput>(): MakeGenericType re-materializes an interface instantiation the flow " +
                        "class already implements statically, and a failure leaves the JSON-shape comparison standing.")]
    private static bool ReflectedInputEquivalent(IServiceProvider serviceProvider, FlowState existing, string? persisted, string requested)
    {
        if (FlowStateJson.JsonEquivalent(persisted, requested))
            return true;
        if (persisted is null)
            return false;

        try
        {
            if (existing.FlowTypeName is not { } flowTypeName
                || existing.InputTypeName is not { } inputTypeName
                || ReflectionExtensions.ResolveServiceType(flowTypeName) is not { } flowType
                || ReflectionExtensions.ResolveServiceType(inputTypeName) is not { } inputType
                || !typeof(IDurableFlow<>).MakeGenericType(inputType).IsAssignableFrom(flowType)
                || serviceProvider.GetService<IServiceProviderIsService>()?.IsService(flowType) != true)
            {
                return false;
            }

            return FlowStateJson.JsonEquivalent(Normalize(persisted), Normalize(requested));

            string Normalize(string json)
                => JsonSafety.SafeDeserialize(json, inputType) is { } value ? AsyncResponseJson.Serialize(value, value.GetType()) : "null";
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    private static InvalidOperationException FlowContractMismatch(Type flowType, Type inputType)
        => new(
            $"Durable flow type '{flowType.FullName}' does not implement IDurableFlow<{inputType.Name}> " +
            "matching the persisted input type; the flow state was written by an incompatible flow definition.");

    private static object ResolveFlowFromDi(IServiceProvider serviceProvider, Type flowType)
    {
        try
        {
            return serviceProvider.GetRequiredService(flowType);
        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException(
                $"Durable flow type '{flowType.FullName}' is not registered in DI. Register it with " +
                $"WithDurableFlow<{flowType.Name}, TInput>() (or services.AddScoped<{flowType.Name}>()) so the flow can be " +
                "resolved on execute and resume.", ex);
        }
    }

    private static Type ResolveType(string? fullName, string kind)
    {
        if (string.IsNullOrWhiteSpace(fullName))
            throw new InvalidOperationException($"The persisted flow state carries no {kind} type name; it was written by an incompatible producer.");

        return ReflectionExtensions.ResolveServiceType(fullName)
            // The name is store data: rendered through the diagnostics helper so an unresolvable
            // one cannot copy megabytes of store-written text, or its raw line breaks, into this
            // message and from there into a log on every delivery.
            ?? throw new InvalidOperationException(
                $"Cannot resolve {kind} type '{AsyncResponseTypeResolution.DescribeForDiagnostics(fullName)}'. For plugin/collectible-assembly scenarios register a resolver " +
                $"via {nameof(AsyncResponseTypeResolution)}.{nameof(AsyncResponseTypeResolution.RegisterAssembly)}.");
    }

    /// <summary>
    /// Observer hook for terminal transitions. Fires AFTER the terminal save: an observer that
    /// throws here (crash injection) fails a delivery whose run is already terminal, so the
    /// redelivered execution re-notifies and acks — the run's outcome is never at stake, and the
    /// terminal event is delivered at least once.
    /// </summary>
    /// <summary>
    /// Best-effort attempt-failure notification (see
    /// <see cref="IDurableFlowExecutionObserver.OnRunAttemptFailedAsync"/>): the attempt's own
    /// exception is already propagating for redelivery, so observer throws are swallowed instead
    /// of masking it.
    /// </summary>
    private async Task NotifyRunAttemptFailedAsync(FlowState state)
    {
        if (_observers.Length == 0)
            return;

        var runEvent = new DurableFlowRunEvent(state.FlowId!, state.Status, state.LastMessage);
        foreach (var observer in _observers)
        {
            try
            {
                await observer.OnRunAttemptFailedAsync(runEvent).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Guarded: a throwing provider here replaced the attempt's own exception — on a
                // hand-back, the DurableFlowInterruptedException whose TYPE is all a transport
                // tells a hand-back from a failure by.
                SafeLog.Try(
                    (Logger: _logger, Error: ex, state.FlowId),
                    static s => s.Logger.LogWarning(
                        s.Error,
                        "A durable-flow execution observer threw in OnRunAttemptFailedAsync for {FlowId}; ignoring so the attempt's own failure propagates.",
                        s.FlowId));
            }
        }
    }

    private async Task NotifyRunFinishedAsync(FlowState state)
    {
        if (_observers.Length == 0)
            return;

        var runEvent = new DurableFlowRunEvent(state.FlowId!, state.Status, state.LastMessage);
        foreach (var observer in _observers)
            await observer.OnRunFinishedAsync(runEvent).ConfigureAwait(false);
    }

    private Task NotifyParentAsync(FlowState state)
    {
        if (string.IsNullOrWhiteSpace(state.ParentFlowId))
            return Task.CompletedTask;

        // A suspended child cannot unblock its parent — the parent would only re-attach and go
        // back to waiting. The terminal transition after an operator un-suspends notifies then.
        if (state.Status == FlowRunStatus.Suspended)
            return Task.CompletedTask;

        var parentFlowId = state.ParentFlowId;
        // Guarded: logged before the parent's wake-up is enqueued, which a throw here skipped.
        SafeLog.Try(
            (Logger: _logger, state.FlowId, state.Status, ParentFlowId: parentFlowId, state.ParentStepName),
            static s => s.Logger.LogInformation(
                "Durable child flow {FlowId} reached {Status}; resuming parent flow {ParentFlowId} step '{ParentStepName}'.",
                s.FlowId,
                s.Status,
                s.ParentFlowId,
                s.ParentStepName));

        return _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(parentFlowId));
    }
}
