using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.ExceptionServices;

namespace AsyncResponse;

/// <summary>
/// Runtime <see cref="IDurableFlowContext"/> bound to one execution of one flow run. Owns the
/// checkpointed-flow mechanics so flow code doesn't have to: step guards, result memoization, the
/// pending-correlation-id breadcrumb, fresh-start vs re-attach, and the durable resume/failure
/// callbacks that point back at the flow executor.
/// <para>
/// Not thread-safe by design: a flow body runs sequentially, and <c>until</c> predicates run on
/// the channel's dispatch path only while the flow itself is parked awaiting that same step.
/// </para>
/// </summary>
internal sealed class DurableFlowContext : IDurableFlowContext
{
    private readonly FlowState _state;
    private readonly IFlowStateStore _store;
    private readonly IAsyncResponseBuilder _builder;
    private readonly AsyncResponseContextPropagation _propagation;
    private readonly DurableFlowOptions _options;
    private readonly IAsyncResponseSubscriber _subscriber;
    private readonly IRecoverableAsyncResponseSubscriber? _recoverableSubscriber;
    private readonly ILogger _logger;
    private readonly FlowExecutionLease _lease;
    private readonly TimeProvider _timeProvider;
    private readonly IDurableFlowExecutionObserver[] _observers;
    private readonly IWorkerTransport? _workerTransport;
    private readonly TimeSpan? _channelDefaultWaitTimeout;
    private readonly CancellationToken _hostStopping;
    private readonly DateTime? _deliveryStartedUtc;
    private bool _suspended;

    // The failure of a park that did not commit (see ParkAsync). Sticky like _suspended: flow
    // code that swallowed the first throw gets it again from every later context call, and from
    // the executor's flush when the body returns normally.
    private ExceptionDispatchInfo? _parkFailure;

    // The host-stop interruption of an in-process wait that was handed back rather than handed
    // over (see Interrupt). Sticky for the same reason as _parkFailure.
    private ExceptionDispatchInfo? _interrupted;

    // Step names that RETURNED in this execution (memoized or freshly completed); see GetStep.
    private HashSet<string>? _returnedSteps;

    // The innermost step call of this context in flight (null when none); see EnterStep.
    private StepToken? _innermostStep;

    // The step call executing on the current async flow. Tells a step called from INSIDE the
    // innermost running step's body (nested: sequential, supported) from a sibling started next to
    // a running step (Task.WhenAll: concurrent, not supported) — the sibling starts from its
    // parent's execution context, which carries the PARENT's token, not the running call's. Bare
    // tokens rather than the context: execution-context snapshots outlive the execution (the
    // in-memory transport keeps one with every delayed wake-up), and must not pin the ledger.
    private static readonly AsyncLocal<StepToken?> ActiveStepOwner = new();
    private DateTime _lastPersistenceUtc;

    // The next ledger-size estimate (in chars) that logs the growth warning; long.MaxValue when
    // the warning is disabled. Doubles after every warning so a long run logs O(log n) times.
    private long _nextLedgerSizeWarningChars;

    /// <summary>
    /// The deepest child-flow nesting a long park supports (see
    /// <see cref="ExtendAncestorLedgersAsync"/>): every ancestor up to the root is refreshed, and a
    /// chain longer than this fails the run terminally instead of being silently truncated — the
    /// previous 16-level cap stopped walking with the root unrefreshed, so a leaf nested 17 deep
    /// parked "successfully" while its root expired underneath it. Cycles are detected separately
    /// (a visited set), so this bounds only the cost of a legitimately absurd nesting.
    /// </summary>
    internal const int MaxAncestorLedgerDepth = 256;

    /// <summary>Creates the context for one execution of the given run.</summary>
    public DurableFlowContext(
        FlowState state,
        IFlowStateStore store,
        IAsyncResponseBuilder builder,
        AsyncResponseContextPropagation propagation,
        DurableFlowOptions options,
        IAsyncResponseSubscriber subscriber,
        IRecoverableAsyncResponseSubscriber? recoverableSubscriber,
        ILogger logger,
        FlowExecutionLease lease,
        TimeProvider? timeProvider = null,
        IDurableFlowExecutionObserver[]? observers = null,
        IWorkerTransport? workerTransport = null,
        TimeSpan? channelDefaultWaitTimeout = null,
        CancellationToken hostStopping = default,
        DateTime? deliveryStartedUtc = null)
    {
        _state = state;
        _store = store;
        _builder = builder;
        _propagation = propagation;
        _options = options;
        _nextLedgerSizeWarningChars = InitialLedgerWarningChars(options.LedgerSizeWarningBytes, state);
        _subscriber = subscriber;
        _recoverableSubscriber = recoverableSubscriber;
        _logger = logger;
        _lease = lease;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _observers = observers ?? [];
        _workerTransport = workerTransport;
        _channelDefaultWaitTimeout = channelDefaultWaitTimeout;
        _hostStopping = hostStopping;
        _deliveryStartedUtc = deliveryStartedUtc;

        // A ledger its start already made large (a 600 KiB input against the 512 KiB default) is
        // past the threshold before anything is saved: warned here, on the run's first execution,
        // or not at all — later executions start at the next doubling (see InitialLedgerWarningChars).
        if (state.Attempts <= 1)
            WarnIfLedgerLarge();
    }

    private DateTime UtcNow => _timeProvider.GetUtcNow().UtcDateTime;

    /// <summary>
    /// Invokes every registered execution observer. Observers run on the execution path by
    /// contract: an observer exception fails this execution attempt exactly like a step failure
    /// (AsyncResponse.Testing injects deterministic crashes through precisely this).
    /// </summary>
    private async ValueTask NotifyStepAsync(
        Func<IDurableFlowExecutionObserver, DurableFlowStepEvent, ValueTask> invoke,
        string stepName,
        DurableFlowStepKind kind,
        string? correlationId = null,
        DateTime? wakeAtUtc = null)
    {
        if (_observers.Length == 0)
            return;

        var stepEvent = new DurableFlowStepEvent(FlowId, stepName, kind, correlationId, wakeAtUtc);
        foreach (var observer in _observers)
            await invoke(observer, stepEvent).ConfigureAwait(false);
    }

    internal bool IsSuspended => _suspended;

    /// <summary>
    /// The host-stop interruption this execution was handed back with (see <see cref="Interrupt"/>),
    /// or <c>null</c>. The executor rethrows it in place of whatever flow code converted it into.
    /// </summary>
    internal ExceptionDispatchInfo? Interruption => _interrupted;

    /// <inheritdoc />
    public string FlowId => _state.FlowId!;

    /// <inheritdoc />
    public async Task StepAsync(string name, Func<Task> step, CancellationToken cancellationToken = default)
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(step);

        using var active = EnterStep(name);
        var checkpoint = GetStep(name);
        if (checkpoint.Completed)
            return;

        cancellationToken.ThrowIfCancellationRequested();
        await NotifyStepAsync(static (o, e) => o.OnStepStartingAsync(e), name, DurableFlowStepKind.Local).ConfigureAwait(false);
        await step().ConfigureAwait(false);
        _lease.ThrowIfLost();
        // Deliberately NOT the caller's token (awaited-step parity): the step's side effect has
        // already happened, so the checkpoint is its only record. A cancellation here lost the
        // checkpoint — the redelivered execution re-ran the side effect — and the store's
        // OperationCanceledException tripped MarkLost on a lease whose row was intact.
        await CompleteStepAsync(name, checkpoint, resultJson: null, CancellationToken.None).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TResult> StepAsync<TResult>(string name, Func<Task<TResult>> step, CancellationToken cancellationToken = default)
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(step);

        using var active = EnterStep(name);
        var checkpoint = GetStep(name);
        if (checkpoint.Completed)
            return DeserializeResult<TResult>(checkpoint.ResultJson);

        cancellationToken.ThrowIfCancellationRequested();
        await NotifyStepAsync(static (o, e) => o.OnStepStartingAsync(e), name, DurableFlowStepKind.Local).ConfigureAwait(false);
        var result = await step().ConfigureAwait(false);
        _lease.ThrowIfLost();
        // Not the caller's token: see the untyped overload above.
        await CompleteStepAsync(name, checkpoint, AsyncResponseJson.Serialize(result), CancellationToken.None).ConfigureAwait(false);
        return result;
    }

    /// <inheritdoc />
    public async Task DelayAsync(string name, TimeSpan delay, CancellationToken cancellationToken = default)
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        // The guard BEFORE the step lookup, like every other step: GetStep inserts into the
        // ledger's step dictionary, and a concurrent sibling (Task.WhenAll over context calls)
        // mutated it — orphan entry persisted with the other step's save, or a torn enumeration
        // inside that save misreported as a lost lease — before the guard ever threw.
        using var active = EnterStep(name);
        var checkpoint = GetStep(name);
        if (checkpoint.Completed)
            return;

        // The due time anchors at the FIRST execution that reaches this step and is checkpointed;
        // replays (crash, redeploy, chunked wake-up) wait out the remainder, never restart. The
        // checkpointed instant therefore wins outright — the argument is not even looked at on a
        // replay, exactly as in DelayUntilAsync: an edit to the delay while a run is mid-sleep
        // must not change that run, and least of all fail it (validating the new argument here
        // turned a parked, perfectly valid timer into a terminal failure on resume).
        if (checkpoint.WakeAtUtc is { } persisted)
        {
            await DelayCoreAsync(name, checkpoint, persisted, cancellationToken).ConfigureAwait(false);
            return;
        }

        // Fresh arrival: validate the requested span BEFORE the UtcNow.Add below, so
        // TimeSpan.MaxValue (or any absurd span) surfaces as the terminal sleep-ceiling failure
        // rather than the DateTime overflow the addition would throw first.
        var requested = delay > TimeSpan.Zero ? delay : TimeSpan.Zero;
        ThrowIfSleepBeyondLedger(name, requested);
        await DelayCoreAsync(name, checkpoint, UtcNow.Add(requested), cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task DelayUntilAsync(string name, DateTimeOffset wakeAtUtc, CancellationToken cancellationToken = default)
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        // Guard first — see DelayAsync.
        using var active = EnterStep(name);
        var checkpoint = GetStep(name);
        if (checkpoint.Completed)
            return;

        // The checkpointed instant wins over the argument on replay, so a code edit that changes
        // the target while a run is mid-sleep cannot double- or under-sleep that run.
        await DelayCoreAsync(name, checkpoint, checkpoint.WakeAtUtc ?? wakeAtUtc.UtcDateTime, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The timer itself; the caller holds the step guard (<see cref="EnterStep"/>).</summary>
    private async Task DelayCoreAsync(string name, FlowStepState checkpoint, DateTime wakeAtUtc, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await NotifyStepAsync(static (o, e) => o.OnStepStartingAsync(e), name, DurableFlowStepKind.Timer, wakeAtUtc: wakeAtUtc).ConfigureAwait(false);

        var remaining = wakeAtUtc - UtcNow;
        var firstPass = checkpoint.WakeAtUtc is null;

        // The ledger bound is settled at the FIRST arm, against the options in force then; a
        // replay never re-litigates the checkpointed sleep against the CURRENT options — raising
        // StateExpiry mid-sleep shrinks the recomputed bound and would terminally fail a parked
        // timer that was valid when it was armed, exactly the class of failure the
        // argument-ignoring contract above rules out.
        if (firstPass)
            ThrowIfSleepBeyondLedger(name, remaining);

        // The skew proof rides the wake-up that carried it, and that wake-up targets the ONE step
        // whose due time is already persisted — the parked timer this delivery re-executes.
        // Claiming it here (one-shot) scopes the forced-early fallback to that step alone: an
        // unrelated later timer in the same replay suspends normally instead of inheriting an
        // exemption that would pin it in process for its full remainder, or fail it on the
        // 49.7-day ceiling below.
        var forcedEarly = !firstPass && WorkerJobSkewScope.TryConsumeForcedEarlyExecution();

        if (firstPass)
        {
            // Persist the breadcrumb BEFORE any wake-up can exist, with a TTL that covers the whole
            // sleep plus the normal idle margin — a run must never out-sleep its own ledger.
            checkpoint.WakeAtUtc = wakeAtUtc;
            checkpoint.Faulted = false;
            checkpoint.Message = remaining > TimeSpan.Zero ? $"Sleeping until {wakeAtUtc:O}." : null;
            await SaveForSleepAsync(wakeAtUtc, cancellationToken).ConfigureAwait(false);
        }

        if (remaining > TimeSpan.Zero)
        {
            await NotifyStepAsync(static (o, e) => o.OnStepWaitingAsync(e), name, DurableFlowStepKind.Timer, wakeAtUtc: wakeAtUtc).ConfigureAwait(false);

            // MaxPublishDelay <= zero marks a transport whose delayed capability is unavailable in
            // the current configuration (an SQS FIFO worker queue): suspend-then-throw would strand
            // the run as "sleeping" with no wake-up, so treat it as not delayed-capable at all.
            //
            // The skew marker rules out suspension for a different reason: this delivery only
            // happened because the executor proved (over consecutive hops) that the transport's
            // delay gate and the stamping clock disagree. Suspending again would enqueue a FRESH
            // wake-up whose stall counters start at zero, discarding that proof and looping
            // forever — so the remainder is waited out in process instead, which honors the due
            // time. That fallback needs no new envelope and is the same one non-delayed transports
            // always take.
            if (remaining > _options.TimerInProcessThreshold
                && !forcedEarly
                && _workerTransport is IDelayedWorkerTransport delayedTransport
                && delayedTransport.MaxPublishDelay > TimeSpan.Zero)
            {
                // Suspend instead of waiting here: the delayed wake-up job re-executes the flow at
                // (or chunked toward) the due time, and this run holds no worker, lease, or memory
                // while it sleeps. Mirrors the child-flow suspension ordering: persist, enqueue,
                // throw — a crash between the persist and the enqueue leaves this job unacked, so
                // broker redelivery re-runs the step and re-enqueues the wake-up.
                await SuspendForTimerAsync(name, wakeAtUtc, remaining, cancellationToken).ConfigureAwait(false);
                throw new InvalidOperationException("Unreachable.");
            }

            // One delivery is never held past the in-process budget: a longer remainder is waited
            // in hops, each under a fresh delivery (see HandOverTimerAsync). Without a budget the
            // hop is the BCL timer ceiling the wait arms (~49.7 days): a longer sleep on a
            // transport with neither delayed delivery nor an in-flight ceiling (Kafka, NATS, Redis
            // Streams) used to fail the run terminally although a hand-over can wait any remainder.
            // Not for a skew-forced wake-up, whose hand-over would drop the skew proof and loop.
            var hop = InProcessParkBudget() ?? (forcedEarly ? null : AsyncResponseChannelOptions.MaxTimerBackedTimeout);
            var wait = hop is { } budget && budget < remaining ? budget : remaining;

            if (wait > AsyncResponseChannelOptions.MaxTimerBackedTimeout)
            {
                throw new DurableFlowFailedException(
                    $"Timer step '{name}' of flow '{FlowId}' sleeps for {remaining.TotalDays:0.#} days, which exceeds the " +
                    $"{AsyncResponseChannelOptions.MaxTimerBackedTimeout.TotalDays:0.#}-day .NET timer ceiling, and " +
                    "this wake-up was released early because the transport's delay gate and the publishing clock disagree, so " +
                    "re-suspending would loop instead of sleeping. Fix the clock skew between the application and the broker/database.");
            }

            if (!firstPass)
            {
                // Replayed execution about to resume the sleep in process: the executor's
                // unconditional per-attempt save reset the ledger TTL to StateExpiry, and every
                // store recomputes expiry from "now" — a resumed sleep longer than StateExpiry
                // would out-sleep its own ledger and be silently dropped mid-wait. Re-extend to
                // cover the remainder (the suspend path re-extends every pass in SuspendForTimerAsync).
                await SaveForSleepAsync(wakeAtUtc, cancellationToken).ConfigureAwait(false);
            }

            await WaitInProcessAsync(name, wakeAtUtc, wait, cancellationToken).ConfigureAwait(false);
            _lease.ThrowIfLost();

            if (wait < remaining)
            {
                // Measured again rather than computed: a timer that fired late may already have
                // covered the rest, and a due timer completes here like any other.
                var left = wakeAtUtc - UtcNow;
                if (left > TimeSpan.Zero)
                {
                    await HandOverTimerAsync(name, wakeAtUtc, cancellationToken).ConfigureAwait(false);
                    throw new InvalidOperationException("Unreachable.");
                }
            }
        }

        await CompleteStepAsync(name, checkpoint, resultJson: null, CancellationToken.None, kind: DurableFlowStepKind.Timer).ConfigureAwait(false);
    }

    /// <summary>
    /// The longest an in-process timer wait may hold one delivery, or <c>null</c> for no bound:
    /// the transport-derived hop (<see cref="TransportParkBudget"/>), shortened further by
    /// <see cref="DurableFlowOptions.MaxInProcessParkDuration"/>, which also supplies a bound when
    /// the transport advertises none. Capped at the BCL timer ceiling the wait arms.
    /// <para>
    /// The transport's half is also capped at what the DELIVERY has left: the in-flight ceiling
    /// minus the time since this delivery's handler started, minus a tenth of the ceiling as
    /// headroom for the hand-over itself. Half the ceiling assumed the steps before the timer took
    /// at most the other half; a delivery that had already spent 20 minutes of a 30-minute
    /// RabbitMQ <c>consumer_timeout</c> then parked for 15 more, and the broker redelivered it
    /// under its live handler. With nothing left the timer hands over at once — that cannot spin,
    /// since the next delivery starts its in-flight clock from zero.
    /// </para>
    /// </summary>
    private TimeSpan? InProcessParkBudget()
    {
        var budget = TransportParkBudget();
        if (budget is { } half && InFlightCeiling() is { } ceiling && _deliveryStartedUtc is { } started)
        {
            var left = ceiling - (UtcNow - started) - ceiling / 10;
            if (left < half)
                budget = left > TimeSpan.Zero ? left : TimeSpan.Zero;
        }

        if (_options.MaxInProcessParkDuration is { } configured && (budget is null || configured < budget))
            budget = configured;

        return budget;
    }

    /// <summary>
    /// The hop the worker transport's in-flight ceiling allows on its own, or <c>null</c> when it
    /// advertises none: half the ceiling (<see cref="IWorkerTransportInFlightLimit"/>) — the other
    /// half is headroom for the steps that ran before the timer in the same delivery and for the
    /// hand-over itself — capped at the BCL timer ceiling.
    /// </summary>
    private TimeSpan? TransportParkBudget()
    {
        if (InFlightCeiling() is not { } ceiling)
            return null;

        var half = ceiling / 2;
        return half > AsyncResponseChannelOptions.MaxTimerBackedTimeout
            ? AsyncResponseChannelOptions.MaxTimerBackedTimeout
            : half;
    }

    private TimeSpan? InFlightCeiling()
        => _workerTransport is IWorkerTransportInFlightLimit { MaxInFlightDuration: { } ceiling } && ceiling > TimeSpan.Zero
            ? ceiling
            : null;

    /// <summary>
    /// In-process timer wait under the execution lease — the fallback for transports without
    /// delayed delivery and for sub-threshold remainders. Cancellation (caller token, lease loss,
    /// host stop) deliberately leaves the due time untouched: the persisted due time is the
    /// breadcrumb, and the next execution waits out the remainder — the timer itself cannot fault.
    /// <para>
    /// Host stop is wired in explicitly. Nothing else ends this wait at shutdown — the lease keeps
    /// renewing for as long as the process lives — so a deploy used to wait a parked handler out
    /// for the transport's whole drain budget and then kill it. A wait the stop interrupts is
    /// HANDED OVER (<see cref="HandOverAtHostStopAsync"/>): checkpoint, immediate wake-up, suspend,
    /// exactly like a hop, so the delivery is acknowledged. Handing the delivery back unsettled
    /// instead made every deploy during one long sleep cost that job a delivery attempt — brokers
    /// count an unsettled redelivery like a failed one — until the transport's attempt cap
    /// dead-lettered the run's only wake-up without ever running it. A wait that STARTS on a host
    /// already stopping is not handed over but handed back: the wake-up a hand-over publishes can
    /// come straight back to this same host's still-running subscriber, and handing that over
    /// again would loop for the length of the shutdown.
    /// </para>
    /// </summary>
    private async Task WaitInProcessAsync(string name, DateTime wakeAtUtc, TimeSpan wait, CancellationToken cancellationToken)
    {
        if (_hostStopping.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            throw Interrupt(new OperationCanceledException(_hostStopping));

        using var linked = cancellationToken.CanBeCanceled || _hostStopping.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lease.LostToken, _hostStopping)
            : null;

        try
        {
            await Task.Delay(wait, _timeProvider, linked?.Token ?? _lease.LostToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex)
        {
            _lease.ThrowIfLost(ex);
            if (_hostStopping.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                await HandOverAtHostStopAsync(name, wakeAtUtc, ex).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Ends an in-process timer wait at host stop as a hand-over (see
    /// <see cref="WaitInProcessAsync"/>): the wake-up is immediate, and whichever host takes it — a
    /// replica still running, or this one after its restart — waits out the remainder under a
    /// delivery whose attempt count starts from zero. When the hand-over cannot commit (the save
    /// or the publish fails) the delivery is handed back instead, as an interruption.
    /// </summary>
    private async Task HandOverAtHostStopAsync(string name, DateTime wakeAtUtc, OperationCanceledException stop)
    {
        _state.LastMessage = $"Flow {FlowId} sleeping until {wakeAtUtc:O} at step '{name}' (handed over to a fresh delivery at host stop).";
        var id = FlowId;
        try
        {
            await ParkAsync(
                wakeAtUtc,
                () => _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(id)),
                CancellationToken.None,
                recordFailure: false).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not DurableFlowSuspendedException)
        {
            _logger.LogWarning(
                ex,
                "Flow {FlowId} could not hand its in-process timer '{Step}' over to a fresh delivery at host stop; the delivery is handed back to the worker transport instead.",
                FlowId,
                name);
            throw Interrupt(stop);
        }
    }

    /// <summary>
    /// Records and returns the exception an in-process park ends with at host stop when it is not
    /// handed over. A cancellation on purpose (the executor's lease-contention poll throws the same
    /// type): the worker transport treats the job as not executed and redelivers it after the
    /// restart, and neither the step nor the run is faulted. Sticky, like a park (see
    /// <see cref="ThrowIfSuspended"/>): flow code that swallows it must not carry on into the next
    /// step with the timer unfinished, nor return and have the run marked Succeeded.
    /// </summary>
    private DurableFlowInterruptedException Interrupt(Exception cause)
    {
        var interrupted = new DurableFlowInterruptedException(
            $"Host is stopping; durable flow '{FlowId}' left its in-process wait and the delivery is abandoned for redelivery.", cause);
        _interrupted = ExceptionDispatchInfo.Capture(interrupted);
        return interrupted;
    }

    private Task SuspendForTimerAsync(string name, DateTime wakeAtUtc, TimeSpan remaining, CancellationToken cancellationToken)
    {
        _state.LastMessage = $"Flow {FlowId} sleeping until {wakeAtUtc:O} at step '{name}'.";
        var id = FlowId;
        return ParkAsync(
            wakeAtUtc,
            () => _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(id), remaining),
            cancellationToken);
    }

    /// <summary>
    /// Ends an in-process hop with time still to sleep: checkpoint, publish an IMMEDIATE wake-up,
    /// suspend. The wake-up replays to this timer — its due time is checkpointed — and waits the
    /// next hop under a new delivery, whose in-flight clock the broker starts from zero. Only ever
    /// called AFTER a hop was waited: the wake-up is not delayed, so publishing it without having
    /// waited would spin deliveries instead of sleeping.
    /// </summary>
    private Task HandOverTimerAsync(string name, DateTime wakeAtUtc, CancellationToken cancellationToken)
    {
        _state.LastMessage = $"Flow {FlowId} sleeping until {wakeAtUtc:O} at step '{name}' (continuing under a fresh delivery).";
        var id = FlowId;
        return ParkAsync(
            wakeAtUtc,
            () => _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(id)),
            cancellationToken);
    }

    /// <summary>
    /// Commits a park — persist, publish the wake-up — and only then marks this execution
    /// suspended. The executor acknowledges a suspended execution's delivery, so the flag must
    /// never be up before the wake-up exists: raised first (as it used to be), a failed save or
    /// publish left it set, flow code that catches <see cref="Exception"/> around the step carried
    /// on into the next context call, that call threw "suspended", and the delivery was
    /// acknowledged for a run nothing would ever wake — on the child path with a child ledger that
    /// was never enqueued. A failure is kept and surfaced again (see <see cref="ThrowIfSuspended"/>
    /// and <see cref="FlushProgressAsync"/>) so the attempt ends as the retriable failure it is
    /// even when the first throw was swallowed.
    /// <para>
    /// Once committed, this execution stops looking alive: renewal is stopped (and a renewal in
    /// flight joined) BEFORE the wake-up is published, and the lease is released right after it.
    /// The wake-up is the park's own continuation — a child that finishes at once, a timer
    /// hand-over — and it judges the lease in its way by whether it changes; a holder still
    /// renewing while the parked body unwound (user <c>finally</c> blocks, <c>await using</c>, a
    /// catch-and-rethrow of the park's cancellation) had it acknowledged as a duplicate, and the
    /// run stayed Running with nothing queued. See <see cref="FlowExecutionLease.EndForParkAsync"/>.
    /// </para>
    /// </summary>
    private async Task ParkAsync(DateTime waitEndsUtc, Func<Task> publishWakeUp, CancellationToken cancellationToken, bool recordFailure = true)
    {
        try
        {
            await SaveForSleepAsync(waitEndsUtc, cancellationToken).ConfigureAwait(false);
            await _lease.PauseRenewalAsync().ConfigureAwait(false);
            try
            {
                await publishWakeUp().ConfigureAwait(false);
            }
            catch
            {
                // Not parked after all: the attempt carries on as a retriable failure and still
                // owns the run until it ends.
                _lease.ResumeRenewal();
                throw;
            }
        }
        catch (Exception ex) when (recordFailure)
        {
            _parkFailure = ExceptionDispatchInfo.Capture(ex);
            throw;
        }

        _suspended = true;
        await _lease.EndForParkAsync().ConfigureAwait(false);
        throw new DurableFlowSuspendedException(_state.LastMessage ?? $"Flow {FlowId} is suspended.");
    }

    /// <summary>
    /// The longest sleep a run's ledger can survive: the persistence ceiling minus the configured
    /// <see cref="DurableFlowOptions.StateExpiry"/>, so the TTL stamped by
    /// <see cref="SaveForSleepAsync"/> (<c>sleep + StateExpiry</c>) always fits the ceiling with
    /// the full idle margin intact. Allowing sleeps up to the ceiling itself would stamp a TTL
    /// that expires exactly at the due instant — any wake latency or store clock skew then finds
    /// the flow state already gone and strands the run unfinished.
    /// </summary>
    private void ThrowIfSleepBeyondLedger(string name, TimeSpan sleep)
    {
        var maxSleep = AsyncResponseChannelOptions.MaxPersistenceTtl - _options.StateExpiry;
        if (sleep <= maxSleep)
            return;

        throw new DurableFlowFailedException(
            $"Timer step '{name}' of flow '{FlowId}' sleeps for {sleep.TotalDays:0} days; the maximum is " +
            $"{maxSleep.TotalDays:0} days — the {AsyncResponseChannelOptions.MaxPersistenceTtl.TotalDays:0}-day persistence ceiling minus " +
            $"{nameof(DurableFlowOptions)}.{nameof(DurableFlowOptions.StateExpiry)} ({_options.StateExpiry.TotalDays:0.#} days) — because ledger TTL " +
            "stamps are computed as \"now + sleep + StateExpiry\" and the ledger must outlive its own wake-up.");
    }

    /// <summary>
    /// Checkpoint save whose TTL covers a known wait window (a timer's sleep, or an awaited step's
    /// timeout): <c>remaining + StateExpiry</c>, saturated at the persistence ceiling. The ordinary
    /// <see cref="SaveAsync"/> TTL bounds <em>idle</em> time between checkpoints; a run parked on a
    /// timer or an awaited response is idle by design for the whole window — and because
    /// <see cref="ThrowIfSleepBeyondLedger"/> caps every sleep at ceiling − StateExpiry (and step
    /// timeouts are timer-bounded far below it), the sum here always carries the full StateExpiry
    /// margin past the due instant.
    /// <para>
    /// The retention floor is anchored to the instant the wait ENDS (<paramref name="waitEndsUtc"/>
    /// + StateExpiry), never to "now + a remainder measured earlier": that one crept forward with
    /// the clock between the measurement and each save, so a second save for the same wait (a
    /// first-pass park checkpoints twice, a hop, a replay) never found the ancestors covered and
    /// rewrote every one of them — bumping each revision — for a floor they already had.
    /// </para>
    /// </summary>
    private async Task SaveForSleepAsync(DateTime waitEndsUtc, CancellationToken cancellationToken)
    {
        var now = UtcNow;
        var remaining = waitEndsUtc - now;
        var margin = AsyncResponseChannelOptions.MaxPersistenceTtl - _options.StateExpiry;
        var ttl = remaining <= TimeSpan.Zero
            ? _options.StateExpiry
            : remaining >= margin
                ? AsyncResponseChannelOptions.MaxPersistenceTtl
                : remaining + _options.StateExpiry;
        var retainUntil = remaining >= margin
            ? FlowStateRetention.FloorAt(now, AsyncResponseChannelOptions.MaxPersistenceTtl)
            : FlowStateRetention.FloorAt(waitEndsUtc, _options.StateExpiry);

        // The wait outlives this save's own TTL stamp only if every later write of this ledger
        // carries it forward — a spurious early redelivery of the parked run stamps the plain
        // StateExpiry in the executor's per-attempt save before it replays back here. The floor
        // in the ledger is what those writes honor (FlowStateRetention.EffectiveTtl).
        if (ttl > _options.StateExpiry)
            FlowStateRetention.RaiseFloorTo(_state, retainUntil);
        await SaveAsync(cancellationToken, ttl: ttl).ConfigureAwait(false);

        // A parked ancestor's row must survive this run's whole wait, not just its own idle
        // margin: nothing refreshes an ancestor while it waits on this chain (lease renewal only
        // stamps the lease columns), so a descendant parking beyond the ancestor's StateExpiry
        // silently expired the ancestor and the eventual completion wake-up found no state.
        // Part of the park, not insurance around it: a failure here propagates BEFORE any wake-up
        // is published (every caller publishes after this save), so the delivery is retried from
        // the checkpoint above instead of the run parking on an ancestor that will expire under it.
        if (ttl > _options.StateExpiry && _state.ParentFlowId is not null)
            await ExtendAncestorLedgersAsync(retainUntil, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Retention extension of the WHOLE ancestor chain when this run parks for a window its own
    /// plain <see cref="DurableFlowOptions.StateExpiry"/> would not cover. Each
    /// <see cref="FlowRunStatus.Running"/> ancestor gets its <see cref="FlowState.RetainUntilUtc"/>
    /// floor raised to cover the wait and its row re-stamped with the wait's TTL — a terminal or
    /// operator-suspended run is not waiting on this chain, and an absent row is never resurrected
    /// (the walk stops there and the expired-ancestor failure surfaces on wake-up, as before). A
    /// store failure PROPAGATES: the callers all publish their wake-up only after this returns, so
    /// the park fails with nothing published and the transport redelivers the execution, which
    /// replays to the same step and retries the chain. Swallowing it (an earlier behavior) let the
    /// child park "successfully" — wake-up and all — while the parent it would eventually complete
    /// into expired mid-wait, after which every step past the parent's child-await was lost with
    /// the parent's checkpoints. The chain is walked to the root with cycle detection; a chain
    /// that revisits an id or exceeds <see cref="MaxAncestorLedgerDepth"/> fails the run terminally
    /// (deterministic on every replay) rather than being truncated in silence.
    /// <para>
    /// A LOST compare-and-swap is not success. The previous design treated it as one — "a
    /// concurrent writer means the ancestor is alive and re-stamping its own expiry" — but the
    /// competing write was computed without this park in view: the parent replaying its
    /// child-await from a snapshot taken before this run persisted its sleep stamps the plain
    /// StateExpiry, and the executor's per-attempt save always does. Either one left the parent's
    /// row expiring under a wait this run had just parked into, with its wake-up published. So the
    /// ancestor is re-read after a lost race: when the write that won already carries a floor
    /// reaching this park (another extension of the same chain, or an earlier attempt of this
    /// one), the retention is proven and the walk moves on; otherwise the extension is retried
    /// against the new revision, a bounded number of times. Every write here still advances the
    /// ancestor's revision — it has to, the floor lives in the ledger — so a retry can cost an
    /// actively-executing ancestor one checkpoint (its next save loses the compare-and-swap and
    /// its delivery replays from the last one, now carrying the floor). That is the price of the
    /// guarantee; the earlier eight-attempt <see cref="FlowStateConcurrency.MutateAsync"/> fight
    /// was avoided by ceding the race, and ceding it is what lost the parent. The attempt bound
    /// keeps the fight finite: losing every attempt abandons the park (nothing published) so the
    /// delivery retries it later, exactly like a store failure.
    /// </para>
    /// <para>
    /// Every write of the ancestor after this one carries the floor forward (see
    /// <see cref="FlowStateRetention"/>), so the extension has to land once, not win every race
    /// from here to the wake-up.
    /// </para>
    /// </summary>
    private async Task ExtendAncestorLedgersAsync(DateTime retainUntil, CancellationToken cancellationToken)
    {
        // For the messages below: how long the park keeps the chain.
        var ttl = retainUntil - UtcNow;
        var visited = new HashSet<string>(StringComparer.Ordinal) { FlowId };
        var ancestorId = _state.ParentFlowId;
        while (ancestorId is not null)
        {
            if (!visited.Add(ancestorId))
            {
                // Corrupted ledgers (ParentFlowId loops back into the chain). Deterministic on
                // every replay, so terminal: parking would leave the run waiting on ancestors
                // whose retention can never be established.
                throw new DurableFlowFailedException(
                    $"Flow '{FlowId}' cannot park for {ttl}: its ancestor chain revisits flow '{ancestorId}' (a cycle in ParentFlowId), " +
                    "so the ledgers it would wait on cannot be kept alive. The stored ledgers are inconsistent; the run is failed rather than parked.");
            }

            if (visited.Count > MaxAncestorLedgerDepth + 1)
            {
                throw new DurableFlowFailedException(
                    $"Flow '{FlowId}' cannot park for {ttl}: it is nested more than {MaxAncestorLedgerDepth} child flows deep, and every ancestor's ledger " +
                    "must be kept alive for the wait. Flatten the nesting.");
            }

            try
            {
                ancestorId = await ExtendOneAncestorAsync(ancestorId, retainUntil, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Logged with the chain context, then rethrown AS IS: the park is abandoned with
                // nothing published, the delivery retries, and the store's own exception type
                // stays visible to whoever classifies it upstream.
                _logger.LogWarning(
                    ex,
                    "Flow {FlowId} could not extend ancestor flow {AncestorFlowId}'s ledger retention for its {Ttl} park; abandoning the park (no wake-up is published) so the delivery retries it.",
                    FlowId, ancestorId, ttl);
                throw;
            }
        }
    }

    /// <summary>
    /// How many times one ancestor's extension is retried against a revision a concurrent write
    /// took. Each attempt re-reads the ancestor first, and a floor already reaching the park ends
    /// the attempt without a write.
    /// </summary>
    internal const int MaxAncestorExtensionAttempts = 4;

    /// <summary>
    /// Extends one ancestor (see <see cref="ExtendAncestorLedgersAsync"/>) and returns the id of
    /// the next ancestor up, or <c>null</c> when the walk stops here: the row is gone, the run is
    /// not <see cref="FlowRunStatus.Running"/>, or it has no parent.
    /// </summary>
    private async Task<string?> ExtendOneAncestorAsync(string ancestorId, DateTime retainUntil, CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            // The TTL this write stamps reaches the absolute floor from wherever the clock is now
            // (never under the plain idle margin).
            var now = UtcNow;
            var ttl = retainUntil - now > _options.StateExpiry ? retainUntil - now : _options.StateExpiry;
            var ancestor = await _store.LoadAsync(ancestorId, cancellationToken).ConfigureAwait(false);
            if (ancestor is null)
            {
                _logger.LogWarning(
                    "Flow {FlowId} parked for {Ttl} but ancestor flow {AncestorFlowId} has no state (expired or deleted); its chain keeps the current expiry.",
                    FlowId, ttl, ancestorId);
                return null;
            }

            if (ancestor.Status != FlowRunStatus.Running)
                return null;

            if (FlowStateRetention.Covers(ancestor, retainUntil))
            {
                // Proven by the re-read: whoever wrote last carried a floor reaching this park
                // (the write that beat a previous attempt, or a sibling park on the same chain).
                return ancestor.ParentFlowId;
            }

            FlowStateRetention.RaiseFloorTo(ancestor, retainUntil);
            var expectedRevision = ancestor.Revision;
            ancestor.Revision = checked(expectedRevision + 1);
            ancestor.UpdatedAtUtc = now;
            if (await _store.TryUpdateAsync(
                    ancestorId,
                    ancestor,
                    expectedRevision,
                    FlowStateRetention.EffectiveTtl(ancestor, ttl, now),
                    leaseId: null,
                    cancellationToken).ConfigureAwait(false))
                return ancestor.ParentFlowId;

            if (attempt >= MaxAncestorExtensionAttempts)
            {
                // Not terminal: the ancestor is being written continuously right now, and the
                // next replay of this step may find it quiet. The park is abandoned with nothing
                // published, so the delivery retries it — the same route a store failure takes.
                throw new InvalidOperationException(
                    $"Flow '{FlowId}' could not extend ancestor flow '{ancestorId}'s ledger retention for its {ttl} park: " +
                    $"a concurrent write advanced the ancestor's revision on each of {attempt} attempts. The park is abandoned so the delivery retries it.");
            }

            _logger.LogDebug(
                "Flow {FlowId} lost the revision race extending ancestor flow {AncestorFlowId}'s ledger retention (attempt {Attempt}); re-reading it.",
                FlowId, ancestorId, attempt);
        }
    }

    /// <inheritdoc />
    public Task<TResponse> AwaitStepAsync<TResponse>(
        string name,
        Func<string, Task> trigger,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default) where TResponse : IAsyncResponsePayload
        => AwaitStepCoreAsync<TResponse>(name, trigger, until: null, timeout, cancellationToken);

    /// <inheritdoc />
    public Task<TResponse> AwaitStepAsync<TResponse>(
        string name,
        Func<string, Task> trigger,
        Func<TResponse, bool> until,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default) where TResponse : IAsyncResponsePayload
    {
        ArgumentNullException.ThrowIfNull(until);
        return AwaitStepCoreAsync<TResponse>(name, trigger, payload => new ValueTask<bool>(until(payload)), timeout, cancellationToken);
    }

    /// <inheritdoc />
    public Task<TResponse> AwaitStepAsync<TResponse>(
        string name,
        Func<string, Task> trigger,
        Func<TResponse, Task<bool>> until,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default) where TResponse : IAsyncResponsePayload
    {
        ArgumentNullException.ThrowIfNull(until);
        return AwaitStepCoreAsync<TResponse>(name, trigger, payload => new ValueTask<bool>(until(payload)), timeout, cancellationToken);
    }

    /// <inheritdoc />
    public Task ReportProgressAsync(string message, CancellationToken cancellationToken = default)
    {
        ThrowIfSuspended();
        _state.LastMessage = message;
        var now = UtcNow;
        if (_options.ProgressPersistenceInterval <= TimeSpan.Zero
            || now - _lastPersistenceUtc >= _options.ProgressPersistenceInterval)
            return SaveAsync(cancellationToken);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public TValue? GetValue<TValue>(string key)
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return _state.Values is not null && _state.Values.TryGetValue(key, out var json)
            ? JsonSafety.SafeDeserialize<TValue>(json)
            : default;
    }

    /// <inheritdoc />
    public Task SetValueAsync<TValue>(string key, TValue value, CancellationToken cancellationToken = default)
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var values = _state.Values ??= new Dictionary<string, string>(StringComparer.Ordinal);
        values[key] = AsyncResponseJson.Serialize(value);
        return SaveAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<FlowState> AwaitChildFlowAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.Interfaces)] TFlow, TInput>(
        string name,
        TInput input,
        string? flowId = null,
        bool failOnChildFailure = true,
        CancellationToken cancellationToken = default)
        where TFlow : class, IDurableFlow<TInput>
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(input);
        if (flowId is not null)
            ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        using var active = EnterStep(name);
        var checkpoint = GetStep(name);
        var requestedChildFlowId = flowId ?? $"{FlowId}:{name}";
        var breadcrumb = checkpoint.ChildFlowId;
        if (breadcrumb is not null && !string.Equals(breadcrumb, requestedChildFlowId, StringComparison.Ordinal))
        {
            throw new DurableFlowFailedException(
                $"Step '{name}' of flow '{FlowId}' is already bound to child flow id '{breadcrumb}', " +
                $"but this execution requested '{requestedChildFlowId}'. A durable step must keep the same child id on every replay.");
        }

        var childFlowId = breadcrumb ?? requestedChildFlowId;
        if (FlowStateConcurrency.FlowIdNotPortable(childFlowId) is { } rejection)
        {
            // Deterministic on every replay, so terminal rather than retriable: the composed id
            // can never become portable, and the constrained stores would reject the child row
            // anyway after a full budget of wasted redeliveries.
            throw new DurableFlowFailedException(
                $"Step '{name}' of flow '{FlowId}' composed a non-portable child flow id. {rejection}");
        }

        var inputJson = AsyncResponseJson.Serialize(input);
        if (checkpoint.Completed)
        {
            var completedChild = DeserializeResult<FlowState>(checkpoint.ResultJson)
                ?? throw new DurableFlowFailedException(
                    $"Completed child step '{name}' of flow '{FlowId}' has no child-state snapshot.");
            ThrowIfChildMismatched<TFlow, TInput>(completedChild, childFlowId, name, inputJson, completed: true);
            ThrowIfChildFailed(completedChild, failOnChildFailure);
            return completedChild;
        }

        await NotifyStepAsync(static (o, e) => o.OnStepStartingAsync(e), name, DurableFlowStepKind.ChildFlow).ConfigureAwait(false);

        var child = await _store.LoadAsync(childFlowId, cancellationToken).ConfigureAwait(false);
        if (child is null)
        {
            if (breadcrumb is not null)
            {
                // The breadcrumb is persisted only after the child state exists, so a missing child
                // here means its ledger expired (StateExpiry) or was deleted while this parent was
                // suspended. Its outcome is unknowable; re-running it blind would re-execute side
                // effects of a possibly-completed run. Fail deterministically instead.
                throw new DurableFlowFailedException(
                    $"Child flow '{childFlowId}' has no state (expired or deleted) while parent flow '{FlowId}' was waiting on step '{name}'. " +
                    "Its outcome is unknown, so it is not re-run automatically. Size DurableFlowOptions.StateExpiry beyond the longest child idle time, " +
                    "or start a new parent run to re-execute the work.");
            }

            // Create the child BEFORE persisting the breadcrumb: "breadcrumb exists" must always
            // imply "child state existed", which keeps the expired-child check above sound. A crash
            // between the two writes is safe — the child id is deterministic, so the re-delivered
            // parent execution loads this child instead of re-creating it.
            child = CreateChildState<TFlow, TInput>(childFlowId, name, inputJson);
            if (await FlowStateConcurrency.TryCreateAsync(
                    _store,
                    childFlowId,
                    child,
                    _options.StateExpiry,
                    cancellationToken).ConfigureAwait(false))
            {
                _logger.LogDebug("Flow {FlowId} started child flow {ChildFlowId} for step '{Step}'.", FlowId, childFlowId, name);
            }
            else
            {
                child = await _store.LoadAsync(childFlowId, cancellationToken).ConfigureAwait(false)
                    ?? throw new InvalidOperationException($"Child flow '{childFlowId}' was created concurrently but could not be loaded.");
                ThrowIfChildMismatched<TFlow, TInput>(child, childFlowId, name, inputJson);
            }
        }
        else
        {
            ThrowIfChildMismatched<TFlow, TInput>(child, childFlowId, name, inputJson);
        }

        if (breadcrumb is null)
        {
            checkpoint.ChildFlowId = childFlowId;
            checkpoint.Faulted = false;
            checkpoint.Message = $"Waiting for child flow '{childFlowId}'.";
            await SaveAsync(cancellationToken).ConfigureAwait(false);
        }

        switch (child.Status)
        {
            // A terminal child snapshot is a settled outcome: memoize it uninterruptibly (local
            // and awaited-step parity) so a cancellation here cannot trip MarkLost on a healthy lease.
            // The caller gets the SNAPSHOT — the reduced shape the memo holds (no ambient Context,
            // nested child-step results elided) — on the first completion exactly as on every
            // replay, which reads it back from the memo above. Returning the loaded child here
            // handed the first execution a richer object than any re-execution would ever see, so
            // parent logic could branch differently (or fail) after a restart on a step it had
            // already completed; the whole point of the memo is that the two are indistinguishable.
            case FlowRunStatus.Succeeded:
            {
                var snapshotJson = FlowStateJson.SerializeSnapshot(child);
                await CompleteStepAsync(name, checkpoint, snapshotJson, CancellationToken.None, kind: DurableFlowStepKind.ChildFlow).ConfigureAwait(false);
                return MaterializeChildSnapshot(name, snapshotJson);
            }

            case FlowRunStatus.Failed:
            {
                checkpoint.Message = child.LastMessage;
                var snapshotJson = FlowStateJson.SerializeSnapshot(child);
                await CompleteStepAsync(name, checkpoint, snapshotJson, CancellationToken.None, faulted: true, kind: DurableFlowStepKind.ChildFlow).ConfigureAwait(false);
                var snapshot = MaterializeChildSnapshot(name, snapshotJson);
                ThrowIfChildFailed(snapshot, failOnChildFailure);
                return snapshot;
            }

            default:
                await NotifyStepAsync(static (o, e) => o.OnStepWaitingAsync(e), name, DurableFlowStepKind.ChildFlow).ConfigureAwait(false);
                await SuspendForChildAsync(childFlowId, child, cancellationToken).ConfigureAwait(false);
                throw new InvalidOperationException("Unreachable.");
        }
    }

    private async Task<TResponse> AwaitStepCoreAsync<TResponse>(
        string name,
        Func<string, Task> trigger,
        Func<TResponse, ValueTask<bool>>? until,
        TimeSpan? timeout,
        CancellationToken cancellationToken) where TResponse : IAsyncResponsePayload
    {
        ThrowIfSuspended();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(trigger);

        using var active = EnterStep(name);
        var checkpoint = GetStep(name);
        if (checkpoint.Completed)
            return DeserializeResult<TResponse>(checkpoint.ResultJson);

        // Re-attach when a previous execution already triggered this step and died waiting; start
        // fresh when there is no breadcrumb or the last attempt faulted (steps are idempotent).
        var reattach = checkpoint.PendingCorrelationId is not null && !checkpoint.Faulted;
        var correlationId = reattach
            ? checkpoint.PendingCorrelationId!
            : AsyncResponseContext.GenerateCorrelationId();
        var stepTimeout = timeout ?? _options.DefaultStepTimeout;
        // The window the ledger must outlive: the resolved step timeout, or — for a timeout-less
        // wait — the channel's declared default waiter timeout, which the channel arms on the
        // waiter below anyway. Null only when the channel declares nothing; such waits keep the
        // historical plain-TTL stamp and re-arm-in-full replays.
        var waitWindow = stepTimeout ?? _channelDefaultWaitTimeout;

        await NotifyStepAsync(static (o, e) => o.OnStepStartingAsync(e), name, DurableFlowStepKind.Awaited, correlationId).ConfigureAwait(false);

        if (reattach && checkpoint.AwaitDeadlineUtc is { } awaitDeadline)
        {
            // The deadline persisted at the FIRST arm is the step's fault clock across
            // executions: a replay arms the REMAINDER, never a fresh full window. Recomputing the
            // window per attempt let every redelivery inside the timeout restart both the fault
            // clock and the ledger TTL from zero — a remote system that never answers produced a
            // run that neither completed nor alarmed, kept alive indefinitely.
            var remainingWindow = awaitDeadline - UtcNow;
            if (remainingWindow <= TimeSpan.Zero)
            {
                // The window elapsed while no execution was live. Recovery may have consumed the
                // response and completed the step in that gap — prefer its checkpoint over a
                // fault (same authority argument as the post-registration short-circuit below).
                if (await TryShortCircuitRecoveredCheckpointAsync(name, checkpoint).ConfigureAwait(false))
                    return DeserializeResult<TResponse>(checkpoint.ResultJson);

                // Settle exactly like the live timeout the waiter would have produced: the fault
                // is recorded so the next execution restarts the step fresh, and the exception
                // propagates as retriable for the transport's bounded redelivery.
                var timedOut = new TimeoutException(
                    $"Timed out waiting for response for correlationId {correlationId}: the await deadline {awaitDeadline:O} " +
                    "elapsed while no execution was live.");
                checkpoint.Faulted = true;
                checkpoint.Message = timedOut.Message;
                await SaveAsync(CancellationToken.None, cause: timedOut).ConfigureAwait(false);
                throw timedOut;
            }

            stepTimeout = remainingWindow;
            waitWindow = remainingWindow;
        }

        var waiter = await CreateWaiterAsync(correlationId, until, stepTimeout, name).ConfigureAwait(false);
        var triggerCompleted = reattach;
        var notifyCompletion = false;
        try
        {
            if (reattach && await TryShortCircuitRecoveredCheckpointAsync(name, checkpoint).ConfigureAwait(false))
            {
                // Lost-subscriber recovery checkpointed this step between our state load and the
                // waiter registration. Its wake-up delivery will find OUR lease alive and ack as
                // a duplicate, so nothing would ever wake the parked wait — take the checkpointed
                // result now instead of waiting out the full step timeout for a response that was
                // already consumed. (Recovery always checkpoints BEFORE enqueueing its wake-up,
                // so a completed persisted checkpoint here is authoritative.)
                return DeserializeResult<TResponse>(checkpoint.ResultJson);
            }

            if (!reattach)
            {
                // Persist the breadcrumb AFTER the registration exists and BEFORE the send:
                // "breadcrumb persisted" therefore implies "someone is listening", so a crash on
                // either side of the send re-attaches (or times out and restarts the idempotent
                // step) — never a lost run, never a double-send.
                checkpoint.PendingCorrelationId = correlationId;
                checkpoint.PendingPayloadTypeFullName = typeof(TResponse).FullName;
                checkpoint.Faulted = false;
                checkpoint.Message = null;
                // The fault clock survives crashes and redeliveries only through this stamp (see
                // the re-attach deadline branch above); null when the effective window is unknown,
                // and such waits keep the recompute-per-attempt behavior.
                checkpoint.AwaitDeadlineUtc = waitWindow is { } window ? UtcNow.Add(window) : null;
                // The ledger must outlive the wait it records, exactly as SaveForSleepAsync covers
                // a timer's sleep: with a wait window longer than StateExpiry, a plain-TTL stamp
                // expires the row (and with it the lease renewal's anchor) mid-wait — the lease is
                // marked lost against a row that no longer exists and the run is unrecoverable. A
                // wait whose window is unknown keeps the plain stamp: bounding open-ended idleness
                // is what StateExpiry is documented to do.
                if (checkpoint.AwaitDeadlineUtc is { } armedUntil)
                    await SaveForSleepAsync(armedUntil, cancellationToken).ConfigureAwait(false);
                else
                    await SaveAsync(cancellationToken).ConfigureAwait(false);

                await trigger(correlationId).ConfigureAwait(false);
                triggerCompleted = true;
            }
            else
            {
                // Replayed execution re-attaching to an in-flight wait: the executor's
                // unconditional per-attempt save reset the ledger TTL to StateExpiry, so a wait
                // window longer than StateExpiry would out-live its own ledger and strand the
                // run mid-wait — re-extend to cover the wait, exactly as the fresh path above
                // and the timer path's replay branch do. With a persisted deadline the window is
                // the REMAINDER (shrunk above); a legacy ledger without one re-extends (and
                // re-arms) the full window — its fault clock restarts, the pre-deadline behavior.
                // A window-less re-attach keeps the plain stamp the executor already wrote.
                if (waitWindow is { } replayWindow)
                    await SaveForSleepAsync(checkpoint.AwaitDeadlineUtc ?? UtcNow.Add(replayWindow), cancellationToken).ConfigureAwait(false);

                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug(
                        "Flow {FlowId} step '{Step}' re-attaching to in-flight correlationId {CorrelationId}.",
                        FlowId, name, correlationId);
                }
            }

            await NotifyStepAsync(static (o, e) => o.OnStepWaitingAsync(e), name, DurableFlowStepKind.Awaited, correlationId).ConfigureAwait(false);

            WarnIfWaitOutlivesInFlightCeiling(name, waitWindow);
            var response = await WaitForResponseAsync(waiter.ResponseTask, cancellationToken).ConfigureAwait(false);

            checkpoint.PendingCorrelationId = null;
            checkpoint.PendingPayloadTypeFullName = null;
            // Deliberately NOT the caller's token: once the response is claimed from the channel
            // it exists nowhere else, so the completion checkpoint must not be interruptible — a
            // cancellation here used to leave `pending` set with the response already consumed,
            // and the redelivered execution re-attached to a correlation id nothing could answer.
            await CompleteStepAsync(name, checkpoint, AsyncResponseJson.Serialize(response), CancellationToken.None, kind: DurableFlowStepKind.Awaited, correlationId: correlationId, notify: false).ConfigureAwait(false);
            notifyCompletion = true;
            return response;
        }
        catch (OperationCanceledException ex) when (triggerCompleted)
        {
            // SETTLE the handoff before deciding. A point-in-time IsCompletedSuccessfully check
            // raced the channel's dispatch: the response could win the task a moment after the
            // check, leaving a consumed response behind a still-pending ledger. Disposing the
            // waiter cancels its response task unless something already completed it (the channel
            // contract since the dispose-cancels fix), so after this await the task is TERMINAL
            // and the decision below is the race's single authoritative outcome. The finally's
            // second dispose is a no-op behind the subscription's cleanup latch.
            await waiter.DisposeAsync().ConfigureAwait(false);

            if (waiter.ResponseTask.IsCompletedSuccessfully)
            {
                // Delivery won the settlement: the channel claimed and acked that message — it
                // exists nowhere else, and re-attaching to its consumed correlation id would park
                // the run until the step timeout. The checkpoint therefore wins over the
                // cancellation: persist the received payload and return it; the caller's token
                // gets its say again at the next step boundary.
                var received = await SettleWonResponseAsync(name, checkpoint, waiter.ResponseTask.Result, correlationId, ex).ConfigureAwait(false);
                notifyCompletion = true;
                return received;
            }

            // No response was won, so nothing is at risk: a lost lease is now just the takeover
            // signal it always was.
            _lease.ThrowIfLost(ex);

            if (waiter.ResponseTask.IsFaulted)
            {
                // The wait FAULTED — a throwing Until predicate (possibly between the catch
                // filter and the settlement), or the disposal drain abandoning a wedged delivery
                // as AsyncResponseIndeterminateDeliveryException. Either way the message may be
                // consumed: restart the idempotent step fresh, exactly like the general fault
                // path below. The checkpoint records the fault's own message (not the
                // cancellation's) so the ledger says WHY the step restarts.
                var fault = waiter.ResponseTask.Exception?.GetBaseException();
                checkpoint.Faulted = true;
                checkpoint.Message = fault?.Message ?? ex.Message;
                await SaveAsync(CancellationToken.None, cause: fault ?? ex).ConfigureAwait(false);
                throw;
            }

            // Cancellation won the settlement (the task is now canceled; nothing was delivered).
            // WAIT-SIDE cancellation is infrastructure, not a step verdict: the channel cancels
            // in-flight waiters when it is disposed at host shutdown, and the caller's token
            // means "stop this execution", not "the step failed" — the remote operation is still
            // in flight. The persisted breadcrumb must survive untouched so the redelivered
            // execution RE-ATTACHES to the same correlation id; marking the checkpoint faulted
            // here turned every graceful shutdown mid-await into a fresh-correlation restart that
            // re-sent the remote request. (A response that never arrives still faults via the
            // step timeout.)
            //
            // The filter keeps this branch away from TRIGGER-thrown cancellation (an HttpClient
            // timeout surfaces as TaskCanceledException): the request may never have left the
            // process, so that case falls through to the fault path below and restarts fresh.
            throw;
        }
        catch (Exception ex)
        {
            if (triggerCompleted)
            {
                // The remote request is in flight (or already answered). SETTLE the handoff
                // before deciding, exactly as the cancellation branch does: after this await the
                // response task is terminal and the decision below is authoritative.
                await waiter.DisposeAsync().ConfigureAwait(false);

                if (waiter.ResponseTask.IsCompletedSuccessfully)
                {
                    // The response was won. Task.WaitAsync hands back a completed task BEFORE it
                    // consults the token, so a lease lost in the same instant the response landed
                    // returns the payload and lands here (not in the cancellation branch) when the
                    // fenced completion save trips ThrowIfLost — and the clock-based check inside
                    // that save can trip on its own in the same window. Settled exactly as the
                    // cancellation branch settles it: without this the redelivered execution
                    // re-attached to a consumed correlation id, burned the step timeout, and
                    // re-sent the request.
                    var received = await SettleWonResponseAsync(name, checkpoint, waiter.ResponseTask.Result, correlationId, ex).ConfigureAwait(false);
                    notifyCompletion = true;
                    return received;
                }

                if (!waiter.ResponseTask.IsFaulted)
                {
                    // Nothing was delivered and the wait itself did not fault: the throw came from
                    // OUTSIDE the wait (a step observer, the logger, the replay branch's ledger
                    // re-extension) while the remote request was already sent. Marking the step
                    // faulted here made the redelivered execution mint a fresh correlation id and
                    // send the request AGAIN — the double-send the breadcrumb exists to prevent,
                    // and worse than a real crash, which leaves the breadcrumb intact. Keep it:
                    // the next execution re-attaches, or the persisted deadline faults it.
                    checkpoint.Message = ex.Message;
                    await SaveAsync(CancellationToken.None, cause: ex).ConfigureAwait(false);
                    throw;
                }
            }

            // Timeout, trigger failure (including trigger-thrown cancellation), or a faulted
            // wait: record it so the next execution restarts this step fresh instead of
            // re-attaching to a dead correlation id. The original failure rides along as `cause`
            // so a rejected save cannot displace it.
            checkpoint.Faulted = true;
            checkpoint.Message = ex.Message;
            await SaveAsync(CancellationToken.None, cause: ex).ConfigureAwait(false);
            throw;
        }
        finally
        {
            await waiter.DisposeAsync().ConfigureAwait(false);
            // Notification is outside the response-settlement catches: an observer failure
            // must end this attempt after its durable checkpoint, not checkpoint and notify twice.
            if (notifyCompletion)
                await NotifyStepAsync(static (o, e) => o.OnStepCompletedAsync(e), name, DurableFlowStepKind.Awaited, correlationId, checkpoint.WakeAtUtc).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// An awaited step holds its delivery for the whole wait, and — unlike a timer — it is NOT
    /// handed over to a fresh delivery at the in-process budget: disposing the waiter deletes its
    /// lost-subscriber recovery registration, so a response landing between one hop's waiter and
    /// the next hop's re-attach would find neither a subscriber nor a recovery target and be
    /// dropped. On a transport with an in-flight ceiling a wait longer than the budget can
    /// therefore outlive its delivery: the broker redelivers the job while this handler is still
    /// parked, and the copy contends on the execution lease this handler holds. That is
    /// configuration the operator can fix and should hear about — hence one warning per parked
    /// step. Judged against the TRANSPORT's half-ceiling, the budget its message names:
    /// <see cref="DurableFlowOptions.MaxInProcessParkDuration"/> shortens timer hops only, and
    /// comparing against it warned about a broker ceiling no such step could reach.
    /// </summary>
    private void WarnIfWaitOutlivesInFlightCeiling(string name, TimeSpan? waitWindow)
    {
        if (InFlightCeiling() is not { } ceiling
            || TransportParkBudget() is not { } budget
            || waitWindow <= budget)
        {
            return;
        }

        _logger.LogWarning(
            "Flow {FlowId} step '{Step}' waits in process for up to {WaitWindow} for its response, but the worker transport redelivers a job held longer than {InFlightCeiling} even while its handler is alive (in-process budget: {Budget}). Awaited steps are not handed over to a fresh delivery the way timers are, so a response slower than that is awaited under a delivery the broker has already handed to another consumer. Keep the step's timeout within the budget, raise the broker's ceiling, or poll with a durable timer between shorter awaited steps.",
            FlowId,
            name,
            waitWindow?.ToString() ?? "an unbounded time",
            ceiling,
            budget);
    }

    /// <summary>
    /// Checkpoints a response that WON the waiter's settlement while the attempt was already
    /// unwinding (a cancellation, or a throw from outside the wait). The one place this is done,
    /// because both unwinding branches need it and each was once fixed without the other.
    /// <para>
    /// The lease is checked AFTER settling, never before. Running ThrowIfLost first threw while
    /// the waiter still held a claimed, channel-acked response: the payload was dropped with no
    /// checkpoint and no re-publish, PendingCorrelationId stayed set, and the redelivered
    /// execution re-attached to a correlation id that could never be answered — one lost response
    /// plus one duplicate remote request. A lost lease cannot write lease-fenced, so the payload
    /// is persisted through the lease-less compare-and-swap the recovery path already uses; only
    /// then is the takeover signal raised, with <paramref name="cause"/> attached.
    /// </para>
    /// </summary>
    private async Task<TResponse> SettleWonResponseAsync<TResponse>(
        string name,
        FlowStepState checkpoint,
        TResponse received,
        string correlationId,
        Exception cause)
    {
        checkpoint.PendingCorrelationId = null;
        if (_lease.IsLost)
        {
            await CheckpointReceivedWithoutLeaseAsync(name, checkpoint, received, correlationId).ConfigureAwait(false);
            _lease.ThrowIfLost(cause);
        }

        try
        {
            await CompleteStepAsync(name, checkpoint, AsyncResponseJson.Serialize(received), CancellationToken.None, kind: DurableFlowStepKind.Awaited, correlationId: correlationId, notify: false).ConfigureAwait(false);
        }
        catch (Exception saveFailure) when (_lease.IsLost)
        {
            // The lease lapsed (or the store refused the fenced write) between the check above and
            // this save — every failed fenced save marks it lost. Same rescue as above, or the
            // claimed response is dropped with the breadcrumb still pointing at a consumed id.
            _logger.LogDebug(saveFailure, "Flow {FlowId} step '{Step}' could not checkpoint its claimed response under the lease; falling back to the lease-less checkpoint.", FlowId, name);
            await CheckpointReceivedWithoutLeaseAsync(name, checkpoint, received, correlationId).ConfigureAwait(false);
            _lease.ThrowIfLost(cause);
            throw;
        }

        return received;
    }

    /// <summary>
    /// Re-reads the persisted step checkpoint after the re-attach waiter registration exists and,
    /// when recovery already completed the step, syncs the in-memory ledger so the caller can
    /// short-circuit. Best-effort: a store read failure logs and falls through to the normal wait
    /// (the behavior before this check existed) rather than faulting the step.
    /// </summary>
    private async Task<bool> TryShortCircuitRecoveredCheckpointAsync(string name, FlowStepState checkpoint)
    {
        try
        {
            var persisted = await _store.LoadAsync(FlowId).ConfigureAwait(false);
            if (persisted?.Steps is null
                || !persisted.Steps.TryGetValue(name, out var persistedStep)
                || !persistedStep.Completed)
            {
                return false;
            }

            // Adopt ONLY while the persisted run is still Running. The revision sync below makes
            // the next checkpoint's CAS succeed, so adopting the revision of a writer that ALSO
            // transitioned the run (RecoverAsync's escalation into FailAsync marking it Failed, an
            // operator parking it) would let this execution's next save write the stale in-memory
            // Status/LastMessage over that transition — resurrecting a terminally Failed run.
            // Falling through keeps the stale revision, so the next save loses the CAS and the
            // delivery abandons and retries: the documented outcome for losing a concurrent write.
            if (persisted.Status is not FlowRunStatus.Running)
                return false;

            // Sync the ledger revision too: the recovery write that completed this step advanced
            // it, and a stale in-memory revision would fail the NEXT checkpoint's compare-and-swap
            // — aborting every execution that took this short-circuit as a phantom "concurrent
            // write" and forcing a pointless redelivery.
            _state.Revision = persisted.Revision;
            checkpoint.Completed = true;
            checkpoint.ResultJson = persistedStep.ResultJson;
            checkpoint.PendingCorrelationId = null;
            checkpoint.PendingPayloadTypeFullName = null;
            checkpoint.Faulted = false;
            checkpoint.Message = persistedStep.Message;
            checkpoint.CompletedAtUtc = persistedStep.CompletedAtUtc;
            MarkStepReturned(name);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Flow {FlowId} step '{Step}' could not re-read its checkpoint before re-attaching; continuing with the normal wait.",
                FlowId, name);
            return false;
        }
    }

    private async Task<IAsyncResponseWaiter<TResponse>> CreateWaiterAsync<TResponse>(
        string correlationId,
        Func<TResponse, ValueTask<bool>>? until,
        TimeSpan? timeout,
        string stepName) where TResponse : IAsyncResponsePayload
    {
        if (_recoverableSubscriber is not null)
        {
            // The durable safety net: a response landing while no process is executing this flow
            // checkpoints the terminal payload and re-enqueues the run, or terminally fails it —
            // the same at-least-once, idempotency-required contract as hand-registered callbacks.
            var flowId = FlowId;
            Expression<Func<IDurableFlowExecutor, Task>> resume = executor => executor.RecoverAsync(
                flowId,
                Placeholder.Payload<TResponse>()!,
                Placeholder.CorrelationId());
            // Correlation-scoped like the resume target: a dead worker's registration outlives
            // the replacement's, so an unscoped failure let a late error for a superseded
            // correlation id terminally fail a run that was live on another one.
            Expression<Func<IDurableFlowExecutor, Task>> failure = executor => executor.FailAsync(
                flowId,
                Placeholder.Exception(),
                Placeholder.CorrelationId());

            return await _recoverableSubscriber.CreateRecoverableResponseWaiter(
                correlationId,
                CallbackExpressionConverter.ToReflectionCall(resume),
                CallbackExpressionConverter.ToReflectionCall(failure),
                until,
                timeout).ConfigureAwait(false);
        }

        _logger.LogDebug(
            "Flow {FlowId} step '{Step}': the configured channel exposes no recoverable subscriber; lost-subscriber recovery is unavailable for this wait.",
            FlowId, stepName);

        return await _subscriber.CreateResponseWaiter(correlationId, until, timeout).ConfigureAwait(false);
    }

    private FlowState CreateChildState<TFlow, TInput>(string flowId, string parentStepName, string inputJson)
    {
        var now = UtcNow;
        return new FlowState
        {
            FlowId = flowId,
            FlowTypeName = typeof(TFlow).FullName,
            InputTypeName = typeof(TInput).FullName,
            InputJson = inputJson,
            Status = FlowRunStatus.Running,
            LastMessage = $"Child flow started by {FlowId}.",
            CreatedAtUtc = now,
            UpdatedAtUtc = now,
            ParentFlowId = FlowId,
            ParentStepName = parentStepName,
            Context = _propagation.Capture()
        };
    }

    private Task EnqueueChildAsync(string childFlowId)
    {
        var id = childFlowId;
        return _builder.EnqueueWorkerAsync<IDurableFlowExecutor>(executor => executor.ExecuteAsync(id));
    }

    private async Task SuspendForChildAsync(string childFlowId, FlowState? child, CancellationToken cancellationToken)
    {
        // Persist the suspension BEFORE the child becomes runnable: once the child is enqueued it
        // can complete and re-execute this parent on another worker at any moment, and a save after
        // that point would clobber the re-execution's newer checkpoints with this stale snapshot.
        // The executor therefore does NOT save again on the suspension path.
        _state.LastMessage = $"Flow {FlowId} suspended waiting for child flow {childFlowId}.";

        // Cover the child's OWN park window, not just this parent's idle margin. A plain
        // StateExpiry save here (and the executor's per-attempt save above it) SHRANK a ledger the
        // child had already extended through ExtendAncestorLedgersAsync — and nothing re-extends
        // it while the child is parked in-process under a live lease, because that rescue enqueue
        // is acked as redundant and the child never replays. The parent's row then expired
        // mid-park and the child's completion wake-up found no state: the parent run, and every
        // step after this one, silently lost.
        await ParkAsync(ChildParkEndsUtc(child), () => EnqueueChildAsync(childFlowId), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// When the child's own persisted park ends — its pending timer wake or awaited deadline,
    /// whichever is furthest out. "Now" when the child is simply running, which leaves the plain
    /// StateExpiry behavior unchanged.
    /// </summary>
    private DateTime ChildParkEndsUtc(FlowState? child)
    {
        var now = UtcNow;
        if (child?.Steps is not { Count: > 0 } steps)
            return now;

        var furthest = now;
        foreach (var step in steps.Values)
        {
            if (step.Completed)
                continue;

            if (step.WakeAtUtc is { } wakeAt && wakeAt > furthest)
                furthest = wakeAt;

            if (step.AwaitDeadlineUtc is { } deadline && deadline > furthest)
                furthest = deadline;
        }

        return furthest;
    }

    private void MarkStepReturned(string name)
        => (_returnedSteps ??= new HashSet<string>(StringComparer.Ordinal)).Add(name);

    /// <summary>
    /// Marks a step call in flight for its whole duration. A flow body is sequential by contract
    /// and nothing here is thread-safe: two steps running at once (<c>Task.WhenAll</c> over two
    /// context calls) interleave their writes of one ledger and one revision counter, which used to
    /// surface — sometimes — as a rejected checkpoint blamed on a lost execution lease. A second
    /// call that starts while one is in flight fails immediately with the actual reason instead.
    /// A step called from INSIDE the innermost running step's own body is sequential and stays
    /// allowed, at any depth. Being inside SOME running step used to be enough, so two calls run
    /// in parallel inside one step's body (<c>StepAsync("outer", () =&gt; Task.WhenAll(...))</c>)
    /// both passed as "nested" and raced exactly as siblings at the top level do.
    /// </summary>
    private StepScope EnterStep(string name)
    {
        // Scoped to the calling step method: an async method's changes to the execution context
        // never flow back to its caller, so the caller never sees this call's token.
        //
        // Top level: nothing of this run is executing.
        var topLevel = new StepToken(parent: null);
        if (Interlocked.CompareExchange(ref _innermostStep, topLevel, null) is null)
        {
            ActiveStepOwner.Value = topLevel;
            return new StepScope(this, topLevel);
        }

        // Nested: made from inside the body of the call that is innermost right now. A token
        // flowing in from anywhere else — a sibling's parent, another execution's finished step
        // captured in an execution context — is never this run's innermost call.
        if (ActiveStepOwner.Value is { Done: false } current)
        {
            var nested = new StepToken(current);
            if (Interlocked.CompareExchange(ref _innermostStep, nested, current) == current)
            {
                ActiveStepOwner.Value = nested;
                return new StepScope(this, nested);
            }
        }

        throw new InvalidOperationException(
            $"Step '{name}' of flow '{FlowId}' was started while another step of the same run was still executing. A durable flow body runs " +
            "its steps sequentially — await each context call before making the next one (no Task.WhenAll over steps, not even inside a " +
            "step's body). For parallel work, start child flows, or run the parallel part inside one step body without context calls.");
    }

    /// <summary>One step call in flight, and the call it is nested in.</summary>
    private sealed class StepToken(StepToken? parent)
    {
        public StepToken? Parent { get; } = parent;

        public volatile bool Done;
    }

    private readonly struct StepScope(DurableFlowContext owner, StepToken token) : IDisposable
    {
        public void Dispose()
        {
            token.Done = true;

            // Pop every finished call off the top: normally just this one, but a nested call that
            // outlived its parent (never awaited) leaves the finished parent beneath it.
            while (Volatile.Read(ref owner._innermostStep) is { Done: true } top)
                Interlocked.CompareExchange(ref owner._innermostStep, top.Parent, top);
        }
    }

    private void ThrowIfSuspended()
    {
        // A committed park first: it released the lease on purpose (see ParkAsync), so the lease
        // check below would misreport it as a takeover.
        if (_suspended)
            throw new DurableFlowSuspendedException(_state.LastMessage ?? $"Flow {FlowId} is suspended.");
        _lease.ThrowIfLost();
        _parkFailure?.Throw();
        _interrupted?.Throw();
    }

    /// <summary>
    /// Reads a just-memoized child snapshot back through the SAME deserializer the replay branch
    /// uses, so the object handed to the first completion is bit-for-bit what every later
    /// execution receives.
    /// </summary>
    private FlowState MaterializeChildSnapshot(string stepName, string snapshotJson)
        => DeserializeResult<FlowState>(snapshotJson)
            ?? throw new DurableFlowFailedException(
                $"Completed child step '{stepName}' of flow '{FlowId}' has no child-state snapshot.");

    private static void ThrowIfChildFailed(FlowState child, bool failOnChildFailure)
    {
        if (failOnChildFailure && child.Status == FlowRunStatus.Failed)
            throw new DurableFlowFailedException($"Child flow '{child.FlowId}' failed: {child.LastMessage ?? "no message"}");
    }

    private void ThrowIfChildMismatched<TFlow, TInput>(
        FlowState child,
        string childFlowId,
        string stepName,
        string requestedInputJson,
        bool completed = false)
    {
        // A child id is owned by exactly one parent: the notification that resumes a suspended
        // parent follows the child's single ParentFlowId, so a second parent awaiting the same id
        // would suspend and never wake. Reject collisions loudly instead of parking forever.
        if (!string.Equals(child.ParentFlowId, FlowId, StringComparison.Ordinal))
        {
            var owner = child.ParentFlowId is null ? "a run not started by AwaitChildFlowAsync" : $"parent flow '{child.ParentFlowId}'";
            throw new DurableFlowFailedException(
                $"Step '{stepName}' of flow '{FlowId}' awaits child flow id '{childFlowId}', but that id belongs to {owner}. " +
                "Child flow ids are exclusive to the parent that started them — pass a flowId that is unique per parent run " +
                "(the default '{parentFlowId}:{stepName}' id is always safe).");
        }

        if (!string.Equals(child.FlowId, childFlowId, StringComparison.Ordinal)
            || !string.Equals(child.ParentStepName, stepName, StringComparison.Ordinal))
        {
            throw new DurableFlowFailedException(
                $"Child flow id '{childFlowId}' is bound to a different child step than '{stepName}' of parent flow '{FlowId}'. " +
                "A child id is exclusive to one parent step.");
        }

        // Type identity, not the ordinal string (TypeNameIdentity): a generic argument's assembly
        // version is part of FullName, and a deploy that moved it failed every parent whose child
        // was created before it.
        if (!TypeNameIdentity.Same(child.FlowTypeName, typeof(TFlow).FullName))
        {
            if (completed)
            {
                // The same settled-outcome rule as the input check below: ownership is proven by
                // the two checks above (the memo is this parent step's own), so a different type
                // name here is the child class renamed or moved since it finished — not an id
                // collision — and failing the parent would throw that outcome away.
                _logger.LogWarning(
                    "Flow {FlowId} step '{Step}' requested child flow {ChildFlowId} as {RequestedFlowType}, but the completed child ran as {PersistedFlowType}; returning the completed child's memoized outcome.",
                    FlowId, stepName, childFlowId, typeof(TFlow).FullName, AsyncResponseTypeResolution.DescribeForDiagnostics(child.FlowTypeName));
                return;
            }

            throw new DurableFlowFailedException(
                $"Step '{stepName}' of flow '{FlowId}' awaits child flow id '{childFlowId}' as {typeof(TFlow).FullName}, " +
                $"but the persisted run is {AsyncResponseTypeResolution.DescribeForDiagnostics(child.FlowTypeName)}. The flowId collides with a different flow — use a unique child id.");
        }

        // The VALUE is compared, not the JSON shape the serializer happened to give it when the
        // child was created: a member added to TInput since (nulls and defaults are written) made
        // every in-flight parent's replay differ from its own persisted child and fail terminally.
        if (TypeNameIdentity.Same(child.InputTypeName, typeof(TInput).FullName)
            && FlowStateJson.InputEquivalent<TInput>(child.InputJson, requestedInputJson))
        {
            return;
        }

        if (completed)
        {
            // A completed step answers from its memo whatever the current arguments are — a
            // local step never re-reads its lambda, a timer never re-reads its delay. The child
            // finished (possibly weeks ago) and its outcome is settled; failing the PARENT
            // terminally over an input edit made since would throw that outcome away.
            _logger.LogWarning(
                "Flow {FlowId} step '{Step}' requested child flow {ChildFlowId} with a different input type or value than the completed child ran with; returning the completed child's memoized outcome.",
                FlowId, stepName, childFlowId);
            return;
        }

        throw new DurableFlowFailedException(
            $"Step '{stepName}' of flow '{FlowId}' requested child flow id '{childFlowId}' with a different input " +
            "type or value than the persisted child. Replays must use semantically identical child input.");
    }

    private FlowStepState GetStep(string name)
    {
        // A name that already RETURNED in this execution is being used for a second step. The
        // checkpoint is keyed by name alone, so the second use would be answered from the first
        // one's memo: a step inside a loop ran its first iteration and silently skipped the rest,
        // returning iteration one's result every time. A step that THREW is not recorded, so
        // retrying it under its name within one execution keeps working.
        if (_returnedSteps is not null && _returnedSteps.Contains(name))
        {
            throw new InvalidOperationException(
                $"Step name '{name}' was already used in this execution of flow '{FlowId}'. Checkpoints are keyed by step name, so a second " +
                "step with the same name would be skipped and handed the first one's result. Give every step a unique name — inside a loop, " +
                "put the iteration key in it (for example $\"send-{item.Id}\").");
        }

        var steps = _state.Steps ??= new Dictionary<string, FlowStepState>(StringComparer.Ordinal);
        if (!steps.TryGetValue(name, out var step))
        {
            if (_options.MaxRetainedSteps is { } limit && steps.Count >= limit)
                throw new DurableFlowFailedException(
                    $"Flow '{FlowId}' cannot add step '{name}': its {limit}-step MaxRetainedSteps budget is exhausted. " +
                    "No side effects of this step were started. Partition the work into bounded child flows, or explicitly raise the budget after measuring checkpoint costs.");
            step = new FlowStepState();
            steps[name] = step;
        }
        else if (step.Completed)
        {
            // Every caller returns a completed step's memo straight away.
            MarkStepReturned(name);
        }

        return step;
    }

    private async Task CompleteStepAsync(
        string name,
        FlowStepState step,
        string? resultJson,
        CancellationToken cancellationToken,
        bool faulted = false,
        DurableFlowStepKind kind = DurableFlowStepKind.Local,
        string? correlationId = null,
        bool notify = true)
    {
        step.Completed = true;
        step.ResultJson = resultJson;
        step.PendingCorrelationId = null;
        // Cleared together with the breadcrumb on EVERY settlement path (the FlowState contract):
        // a stale declared-type name on a completed step would mislead the next recovery pass.
        step.PendingPayloadTypeFullName = null;
        // A memoized failed child keeps Faulted = true so operators can spot the failure on the
        // step itself instead of digging through ResultJson.
        step.Faulted = faulted;
        step.CompletedAtUtc = UtcNow;
        _state.LastMessage = faulted ? $"Step '{name}' completed (child flow failed)." : $"Step '{name}' completed.";
        MarkStepReturned(name);
        await SaveAsync(cancellationToken).ConfigureAwait(false);

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Flow {FlowId} step '{Step}' completed.", FlowId, name);

        if (notify)
            await NotifyStepAsync(static (o, e) => o.OnStepCompletedAsync(e), name, kind, correlationId, step.WakeAtUtc).ConfigureAwait(false);
    }

    /// <summary>
    /// Persists a response that was already claimed and acked by the channel when this execution's
    /// lease had ALREADY been lost — the one case where a lease-fenced write is impossible but the
    /// payload exists nowhere else. Uses the same lease-less compare-and-swap the recovery
    /// dispatcher uses, and re-reads the ledger so it mutates whatever the new owner wrote rather
    /// than clobbering it. Best-effort by construction: on a conflict or an absent ledger the step
    /// simply restarts, which is the pre-existing behavior — but on the common path the response
    /// survives instead of being dropped.
    /// <para>
    /// Fenced to THIS attempt, exactly as <c>DurableFlowExecutor.RecoverAsync</c> fences a recovered
    /// payload: the reloaded step must still be pending on <paramref name="correlationId"/> and the
    /// run must still be checkpointable. Losing the lease means a takeover may already have run —
    /// timed the breadcrumb out, re-triggered the step under a NEW correlation id, or failed the
    /// run — and a write keyed only on "step name, not completed" would complete the newer
    /// attempt's pending step with this attempt's stale response (revision CAS cannot catch it:
    /// the mutation deliberately targets the freshly loaded revision). A stale response is
    /// discarded with a warning; the newer attempt's own response is the one that counts.
    /// </para>
    /// </summary>
    private async Task CheckpointReceivedWithoutLeaseAsync<T>(
        string name,
        FlowStepState step,
        T received,
        string correlationId)
    {
        var resultJson = AsyncResponseJson.Serialize(received);
        var completedAtUtc = UtcNow;
        var applied = false;
        string? skipReason = null;
        FlowState? written = null;
        long estimateBefore = 0;

        try
        {
            var found = await FlowStateConcurrency.MutateAsync(
                _store,
                FlowId,
                _options.StateExpiry,
                _timeProvider,
                state =>
                {
                    applied = false;
                    skipReason = null;

                    // Same eligibility as RecoverAsync: Suspended runs still take the checkpoint
                    // (an operator parked the run; the payload exists nowhere else and un-parking
                    // replays from it), terminal runs never do.
                    if (state.Status is not (FlowRunStatus.Running or FlowRunStatus.Suspended))
                    {
                        skipReason = $"the run is {state.Status}";
                        return false;
                    }

                    if (state.Steps is not { } steps || !steps.TryGetValue(name, out var current))
                    {
                        skipReason = "the step no longer exists in the ledger";
                        return false;
                    }

                    if (current.Completed)
                    {
                        skipReason = "the step is already completed";
                        return false;
                    }

                    if (!string.Equals(current.PendingCorrelationId, correlationId, StringComparison.Ordinal))
                    {
                        skipReason = current.PendingCorrelationId is null
                            ? "the step is no longer pending on any correlation id"
                            : "the step is pending on a newer correlation id (a takeover re-triggered it)";
                        return false;
                    }

                    estimateBefore = FlowStateJson.EstimateLedgerChars(state);
                    current.Completed = true;
                    current.ResultJson = resultJson;
                    current.PendingCorrelationId = null;
                    current.PendingPayloadTypeFullName = null;
                    current.Faulted = false;
                    current.CompletedAtUtc = completedAtUtc;
                    state.LastMessage = $"Step '{name}' completed (checkpointed after the execution lease was lost).";
                    written = state;
                    applied = true;
                    return true;
                },
                CancellationToken.None).ConfigureAwait(false);

            if (applied)
            {
                WarnIfWriteCrossedLedgerWarning(_logger, _options, written!, estimateBefore);
                _logger.LogWarning(
                    "Flow {FlowId} lost its execution lease while step '{Step}' held a claimed response for correlationId {CorrelationId}; the response was checkpointed without the lease so the takeover resumes from it.",
                    FlowId,
                    name,
                    correlationId);
            }
            else
            {
                _logger.LogWarning(
                    "Flow {FlowId} lost its execution lease while step '{Step}' held a claimed response for correlationId {CorrelationId}; the response was discarded because {Reason}.",
                    FlowId,
                    name,
                    correlationId,
                    found ? skipReason : "the ledger no longer exists");
            }
        }
        catch (Exception ex)
        {
            // The takeover signal is raised by the caller regardless; losing this write only means
            // the step restarts as it did before.
            _logger.LogError(
                ex,
                "Flow {FlowId} could not checkpoint the claimed response for step '{Step}' after losing its execution lease; the step will restart.",
                FlowId,
                name);
        }

        if (!applied)
            return;

        step.Completed = true;
        step.ResultJson = resultJson;
        step.PendingCorrelationId = null;
        step.PendingPayloadTypeFullName = null;
        step.CompletedAtUtc = completedAtUtc;
    }

    /// <summary>
    /// Called by the executor when the flow body returned normally, before it marks the run
    /// Succeeded. Only a swallowed failure is surfaced here; a throttled progress report is NOT
    /// flushed — the executor's terminal save overwrites <see cref="FlowState.LastMessage"/> the
    /// next moment, so writing the whole ledger for it first was a wasted store write per run.
    /// </summary>
    internal Task FlushProgressAsync()
    {
        // The body returned normally although a park failed — flow code swallowed the throw. The
        // run is neither finished nor parked, so the executor must not mark it Succeeded: the
        // failure surfaces here, where the executor awaits the body's outcome.
        if (_parkFailure is { } failure)
            return Task.FromException(failure.SourceException);

        // Likewise a swallowed host-stop interruption: the timer it left is unfinished.
        if (_interrupted is { } interrupted)
            return Task.FromException(interrupted.SourceException);

        return Task.CompletedTask;
    }

    private async Task SaveAsync(CancellationToken cancellationToken, Exception? cause = null, TimeSpan? ttl = null)
    {
        _state.UpdatedAtUtc = UtcNow;
        await _lease.SaveAsync(_state, ttl ?? _options.StateExpiry, cancellationToken, cause).ConfigureAwait(false);

        _lastPersistenceUtc = UtcNow;
        WarnIfLedgerLarge();
    }

    /// <summary>
    /// The first ledger size this execution warns at: the configured threshold on the run's first
    /// execution (the executor counts it before building the context) — or, on a later one, the
    /// next doubling above the ledger's current size. Every wake-up builds a fresh context, and
    /// starting each one at the bare threshold warned again on the first save of every execution of
    /// a run that was already large (a parent awaiting 200 children logged about 200 warnings), where
    /// the option promises one per doubling over the run. The first execution keeps the bare
    /// threshold because no earlier one can have warned: seeding it at a doubling too meant a ledger
    /// that started past the threshold never logged its first crossing. A crossing made between
    /// executions, by a write outside any context's saves, is warned by that write
    /// (<see cref="WarnIfWriteCrossedLedgerWarning"/>).
    /// </summary>
    private static long InitialLedgerWarningChars(long? threshold, FlowState state)
    {
        if (threshold is not { } next)
            return long.MaxValue;

        if (state.Attempts <= 1)
            return next;

        return NextLedgerWarningBand(next, FlowStateJson.EstimateLedgerChars(state));
    }

    /// <summary>The first doubling of <paramref name="threshold"/> above <paramref name="estimate"/>, saturating.</summary>
    private static long NextLedgerWarningBand(long threshold, long estimate)
    {
        var next = threshold;
        while (next <= estimate)
        {
            if (next > long.MaxValue / 2)
                return long.MaxValue - 1;
            next *= 2;
        }

        return next;
    }

    /// <summary>
    /// The growth warning for a ledger write made outside a context's own saves: a lease-less
    /// checkpoint (a recovered response, a response won after the lease was lost). The next
    /// execution seeds its first warning at the
    /// next doubling above the ledger's size (see <see cref="InitialLedgerWarningChars"/>), so a
    /// crossing such a write makes is logged here or never: when the write took the estimate from
    /// below a doubling of the threshold to at or past it.
    /// </summary>
    internal static void WarnIfWriteCrossedLedgerWarning(ILogger logger, DurableFlowOptions options, FlowState written, long estimateBefore)
    {
        if (options.LedgerSizeWarningBytes is not { } threshold)
            return;

        var estimate = FlowStateJson.EstimateLedgerChars(written);
        if (estimate >= NextLedgerWarningBand(threshold, estimateBefore))
            LogLedgerLarge(logger, written, estimate, threshold);
    }

    /// <summary>
    /// Every checkpoint rewrites the whole ledger, so a run whose steps retain sizeable results
    /// pays a persistence cost that grows with each completed step (about N²/2 step-results
    /// serialized over a run of N similar steps) until it hits the store's hard cap. The
    /// <see cref="DurableFlowOptions.LedgerSizeWarningBytes"/> threshold turns that curve into an
    /// early operator signal: one warning when it is first crossed, another at each doubling.
    /// </summary>
    private void WarnIfLedgerLarge()
    {
        if (_nextLedgerSizeWarningChars == long.MaxValue)
            return;

        var estimate = FlowStateJson.EstimateLedgerChars(_state);
        if (estimate < _nextLedgerSizeWarningChars)
            return;

        LogLedgerLarge(_logger, _state, estimate, _options.LedgerSizeWarningBytes);

        // Next warning at the next doubling of the CURRENT size (a single huge result may have
        // skipped several thresholds at once), saturating instead of overflowing.
        _nextLedgerSizeWarningChars = estimate > long.MaxValue / 2 ? long.MaxValue - 1 : estimate * 2;
    }

    private static void LogLedgerLarge(ILogger logger, FlowState state, long estimate, long? threshold)
        => logger.LogWarning(
            "Durable flow {FlowId} ledger is roughly {LedgerBytes} bytes over {StepCount} step(s), past the {Threshold}-byte LedgerSizeWarningBytes threshold. Every checkpoint rewrites the whole ledger, so persistence cost now grows with each completed step and the store's MaxStateBytes cap is the hard limit. Keep step results small (persist large data yourself and pass references) or partition a long history into child flows.",
            state.FlowId,
            estimate,
            state.Steps?.Count ?? 0,
            threshold);

    /// <summary>
    /// The awaited step's wait: ended by the response, the caller's token, or lease loss — but
    /// deliberately NOT by host stop (unlike <see cref="WaitInProcessAsync"/>). Every exit through
    /// the wait-side cancellation branch disposes the waiter, and disposing a waiter deletes its
    /// lost-subscriber recovery registration on every channel. ApplicationStopping fires before
    /// any hosted service stops, so ending the wait there replaced the channel's own shutdown —
    /// which cancels its waiters but KEEPS their registrations — with the path that deletes them:
    /// a response landing during the deploy's downtime then found neither a subscriber nor a
    /// registration and was dropped, and the restarted run re-attached to a consumed correlation
    /// id, waited out its deadline and re-sent the request. The channel's shutdown ends the wait.
    /// </summary>
    private async Task<TResponse> WaitForResponseAsync<TResponse>(Task<TResponse> responseTask, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled)
            return await responseTask.WaitAsync(_lease.LostToken).ConfigureAwait(false);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lease.LostToken);
        return await responseTask.WaitAsync(linked.Token).ConfigureAwait(false);
    }

    private static TResult DeserializeResult<TResult>(string? resultJson)
        => resultJson is null ? default! : JsonSafety.SafeDeserialize<TResult>(resultJson)!;
}
