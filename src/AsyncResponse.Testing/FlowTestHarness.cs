using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace AsyncResponse.Testing;

/// <summary>
/// Durable-flow test harness: runs real flows on the in-memory engine
/// (<see cref="AsyncResponseTestHarness"/>) with step-level observation, scripted replies to
/// awaited steps, deterministic crash injection at step boundaries, and virtual-time control for
/// durable timers — without touching the flow class under test.
/// <para>
/// A flow can be driven two ways. <see cref="FlowRunHandle.ExecuteDirectAsync"/> invokes the
/// executor inline — single-threaded and fully deterministic, ideal for crash-at-checkpoint
/// matrices (an injected crash surfaces as the returned attempt's
/// <see cref="SimulatedCrashException"/>; call it again to "restart"). Alternatively, starting via
/// <see cref="StartFlowAsync{TFlow, TInput}"/> runs the whole production pipeline: the worker
/// queue executes the run, crashes ride the transport's redelivery-with-backoff, and
/// <see cref="AsyncResponseTestHarness.AdvanceAsync"/> drives retries, timers, and schedules.
/// </para>
/// </summary>
public sealed class FlowTestHarness : IAsyncDisposable
{
    private readonly FlowProbe _probe;

    private FlowTestHarness(AsyncResponseTestHarness engine, FlowProbe probe)
    {
        Engine = engine;
        _probe = probe;
    }

    /// <summary>Builds the engine (with the flow probe installed) and starts it.</summary>
    public static async Task<FlowTestHarness> StartAsync(Action<AsyncResponseTestHarnessOptions>? configure = null)
    {
        var probe = new FlowProbe();
        var engine = await AsyncResponseTestHarness.StartAsync(options =>
        {
            configure?.Invoke(options);
            options.FlowObservers.Add(probe);
        }).ConfigureAwait(false);
        return new FlowTestHarness(engine, probe);
    }

    /// <summary>The underlying engine harness (publisher, builder, clock, restart).</summary>
    public AsyncResponseTestHarness Engine { get; }

    /// <summary>The virtual clock (shorthand for <c>Engine.Clock</c>).</summary>
    public VirtualTimeProvider Clock => Engine.Clock;

    /// <summary>Advances virtual time (shorthand for <c>Engine.AdvanceAsync</c>).</summary>
    public Task AdvanceAsync(TimeSpan delta) => Engine.AdvanceAsync(delta);

    /// <summary>
    /// Starts a flow through the production pipeline (ledger create + worker-queue wake-up) and
    /// returns its handle.
    /// </summary>
    public async Task<FlowRunHandle> StartFlowAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.Interfaces)] TFlow, TInput>(
        TInput input,
        string? flowId = null)
        where TFlow : class, IDurableFlow<TInput>
    {
        var id = await Engine.Flows.StartAsync<TFlow, TInput>(input, flowId).ConfigureAwait(false);
        return Attach(id);
    }

    /// <summary>Attaches a handle to an existing run (e.g. one started by a cron schedule).</summary>
    public FlowRunHandle Attach(string flowId)
        => new(this, flowId);

    /// <summary>
    /// Arms a one-shot crash that fires when <paramref name="stepName"/> is next about to execute
    /// (before any of its side effects). The execution attempt fails with
    /// <see cref="SimulatedCrashException"/> and the run resumes from its last checkpoint on the
    /// next delivery — the flow class under test needs no instrumentation. Pass
    /// <paramref name="flowId"/> when more than one run (concurrent flows, or a parent and its
    /// child) can reach the step: an unscoped crash fires on whichever run gets there first, so a
    /// test could pass without the intended run ever exercising its recovery path.
    /// </summary>
    public void CrashBeforeStep(string stepName, string? flowId = null)
        => _probe.ArmCrash(stepName, beforeStep: true, flowId);

    /// <summary>
    /// Arms a one-shot crash that fires right after <paramref name="stepName"/>'s completion
    /// checkpoint persists — the classic "died between the checkpoint and the next step" window.
    /// Pass <paramref name="flowId"/> to pin the crash to one run; see
    /// <see cref="CrashBeforeStep"/>.
    /// </summary>
    public void CrashAfterStep(string stepName, string? flowId = null)
        => _probe.ArmCrash(stepName, beforeStep: false, flowId);

    internal FlowProbe Probe => _probe;

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => Engine.DisposeAsync();
}

/// <summary>A handle on one flow run under test.</summary>
public sealed class FlowRunHandle
{
    private readonly FlowTestHarness _harness;

    internal FlowRunHandle(FlowTestHarness harness, string flowId)
    {
        _harness = harness;
        FlowId = flowId;
    }

    /// <summary>The run id.</summary>
    public string FlowId { get; }

    /// <summary>Loads the run's current persisted state (its ledger), or <c>null</c> when none exists.</summary>
    public Task<FlowState?> GetStateAsync()
        => _harness.Engine.Flows.GetStateAsync(FlowId);

    /// <summary>
    /// Executes the run inline on the calling thread (no worker queue): returns when the executor
    /// attempt finishes — completed, suspended (timer/child parked), or failed. An injected crash
    /// or a step exception propagates to the caller; call again to simulate the next delivery.
    /// </summary>
    public async Task ExecuteDirectAsync()
    {
        // Counted so AdvanceAsync's settle treats this attempt like a worker job: it holds no
        // worker slot, so a step it parks would otherwise tip ParkedCount past OutstandingJobs
        // and let the clock advance mid-attempt.
        var directRun = _harness.Engine.OnDirectRunStarted();
        try
        {
            await _harness.Engine.FlowExecutor.ExecuteAsync(FlowId).ConfigureAwait(false);
        }
        finally
        {
            _harness.Engine.OnDirectRunFinished(directRun);
        }
    }

    /// <summary>Re-enqueues the run on the worker queue (the operator's resume action).</summary>
    public Task ResumeAsync()
        => _harness.Engine.Flows.ResumeAsync(FlowId);

    /// <summary>How many times <paramref name="stepName"/> actually started executing (memoized skips excluded).</summary>
    public int StepExecutions(string stepName)
        => _harness.Probe.CountEvents(FlowId, stepName, FlowProbe.EventKind.Starting);

    /// <summary>Every recorded step event for this run, in order.</summary>
    public IReadOnlyList<FlowProbeEvent> Events
        => _harness.Probe.EventsFor(FlowId);

    /// <summary>
    /// Waits (bounded by the harness real-time guard) until <paramref name="stepName"/> is parked
    /// awaiting its response, and returns the correlation id to answer. The park is held in
    /// process, so after <see cref="AsyncResponseTestHarness.SimulateRestartAsync"/> only the NEW
    /// incarnation's park satisfies it (resume the run first) — never the dead incarnation's.
    /// </summary>
    public async Task<string> WaitForAwaitingStepAsync(string stepName)
    {
        var stepEvent = await _harness.Probe.WaitForAsync(
            FlowId,
            e => e.Kind == FlowProbe.EventKind.Waiting && e.Step.Kind == DurableFlowStepKind.Awaited && e.Step.StepName == stepName,
            _harness.Engine.RealTimeGuard,
            $"step '{stepName}' of flow '{FlowId}' to be awaiting a response").ConfigureAwait(false);
        return stepEvent.Step.CorrelationId!;
    }

    /// <summary>
    /// Waits until a timer step is parked, returning its due time (advance the clock past it to
    /// wake the run). After <see cref="AsyncResponseTestHarness.SimulateRestartAsync"/>, a timer
    /// that waited in process (at or below <see cref="DurableFlowOptions.TimerInProcessThreshold"/>)
    /// counts only once the new incarnation parks it again, like
    /// <see cref="WaitForAwaitingStepAsync"/>; a suspended timer's wake-up is carried over, so its
    /// wait still counts.
    /// </summary>
    public async Task<DateTime> WaitForTimerStepAsync(string stepName)
    {
        var stepEvent = await _harness.Probe.WaitForAsync(
            FlowId,
            e => e.Kind == FlowProbe.EventKind.Waiting && e.Step.Kind == DurableFlowStepKind.Timer && e.Step.StepName == stepName,
            _harness.Engine.RealTimeGuard,
            $"timer step '{stepName}' of flow '{FlowId}' to be sleeping").ConfigureAwait(false);
        return stepEvent.Step.WakeAtUtc!.Value;
    }

    /// <summary>
    /// Waits until the given step's completion checkpoint persists. A checkpoint is durable, so
    /// one persisted before <see cref="AsyncResponseTestHarness.SimulateRestartAsync"/> still
    /// satisfies it.
    /// </summary>
    public Task WaitForStepCompletedAsync(string stepName)
        => _harness.Probe.WaitForAsync(
            FlowId,
            e => e.Kind == FlowProbe.EventKind.Completed && e.Step.StepName == stepName,
            _harness.Engine.RealTimeGuard,
            $"step '{stepName}' of flow '{FlowId}' to complete");

    /// <summary>Waits until the run reaches a terminal status and returns it.</summary>
    public async Task<FlowRunStatus> WaitForFinishedAsync()
    {
        var finished = await _harness.Probe.WaitForRunAsync(
            FlowId,
            _harness.Engine.RealTimeGuard,
            $"flow '{FlowId}' to finish").ConfigureAwait(false);
        return finished.Status;
    }

    /// <summary>
    /// Answers the run's currently awaited step: replies to the correlation id of the most recent
    /// awaited-step wait (optionally the named step's). Progress-aware steps take several replies —
    /// non-terminal payloads keep the wait open exactly as in production.
    /// <para>
    /// After <see cref="AsyncResponseTestHarness.SimulateRestartAsync"/>, the new incarnation's
    /// wait once the run has recorded anything in it; until then — nothing has re-executed the run
    /// — the wait that survived the restart, which lost-subscriber recovery routes into the run.
    /// When the re-executed step may arm a fresh correlation id (its deadline lapsed while the
    /// engine was down), resume the run and await <see cref="WaitForAwaitingStepAsync"/> before
    /// replying.
    /// </para>
    /// </summary>
    public async Task ReplyAsync<T>(T response, string? stepName = null) where T : IAsyncResponsePayload
    {
        var correlationId = await NextReplyTargetAsync(stepName).ConfigureAwait(false);
        await _harness.Engine.PublishAsync(response, correlationId).ConfigureAwait(false);
    }

    /// <summary>Fails the run's currently awaited step with an exception (a remote failure).</summary>
    public async Task ReplyExceptionAsync(Exception exception, string? stepName = null)
    {
        var correlationId = await NextReplyTargetAsync(stepName).ConfigureAwait(false);
        await _harness.Engine.PublishExceptionAsync(exception, correlationId).ConfigureAwait(false);
    }

    // One call resolves the live un-answered wait or parks for the NEXT one — replaying history
    // here (as the old fallback did) handed back an already-answered or abandoned correlation id,
    // and the reply published to it was silently dropped.
    private Task<string> NextReplyTargetAsync(string? stepName)
        => _harness.Probe.WaitForNextAwaitedAsync(
            FlowId,
            stepName,
            _harness.Engine.RealTimeGuard,
            stepName is null
                ? $"flow '{FlowId}' to be awaiting any step"
                : $"step '{stepName}' of flow '{FlowId}' to be awaiting a response");
}

/// <summary>One observation recorded by the flow probe.</summary>
/// <param name="Kind">Which lifecycle point.</param>
/// <param name="Step">The step event as reported by the executor.</param>
public readonly record struct FlowProbeEvent(FlowProbe.EventKind Kind, DurableFlowStepEvent Step);

/// <summary>
/// The harness's <see cref="IDurableFlowExecutionObserver"/>: records step events per run, wakes
/// waiters, and throws armed one-shot crashes. Public only for the event-kind enum and the
/// recorded-event type; tests interact through <see cref="FlowTestHarness"/>.
/// </summary>
public sealed class FlowProbe : IDurableFlowExecutionObserver
{
    /// <summary>Step lifecycle points the probe records.</summary>
    public enum EventKind
    {
        /// <summary>The step is about to execute (<see cref="IDurableFlowExecutionObserver.OnStepStartingAsync"/>).</summary>
        Starting = 0,

        /// <summary>The step is durably parked (<see cref="IDurableFlowExecutionObserver.OnStepWaitingAsync"/>).</summary>
        Waiting = 1,

        /// <summary>The step's checkpoint persisted (<see cref="IDurableFlowExecutionObserver.OnStepCompletedAsync"/>).</summary>
        Completed = 2
    }

    private readonly object _gate = new();
    private readonly ConcurrentDictionary<string, List<Recorded>> _events = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DurableFlowRunEvent> _finished = new(StringComparer.Ordinal);
    private readonly List<Waiter> _waiters = [];
    private readonly List<RunWaiter> _runWaiters = [];
    private (string Step, bool Before, string? FlowId)? _armedCrash;

    /// <summary>
    /// The engine incarnation events are recorded in, bumped by each simulated restart (under
    /// <c>_gate</c>). The history keeps every incarnation — counts and the event timeline span
    /// restarts — but a barrier skips an IN-PROCESS park an earlier incarnation recorded: that
    /// park died with its process, and handing it back let a test answer a correlation id the
    /// re-executed step had already replaced. Everything else stays visible across a restart —
    /// a completion checkpoint, a suspended timer whose wake-up the restart carried over — because
    /// it describes durable state that is still true.
    /// </summary>
    private int _incarnation;

    /// <summary>The engine clock and in-process timer threshold (see <see cref="ParksInProcess"/>); set under <c>_gate</c>.</summary>
    private TimeProvider _clock = TimeProvider.System;
    private TimeSpan _timerInProcessThreshold;

    /// <summary>Starts a new incarnation (<see cref="AsyncResponseTestHarness.SimulateRestartAsync"/>).</summary>
    internal void BeginIncarnation()
    {
        lock (_gate)
            _incarnation++;
    }

    /// <summary>Binds the engine clock and the current incarnation's in-process timer threshold.</summary>
    internal void Arm(TimeProvider clock, TimeSpan timerInProcessThreshold)
    {
        lock (_gate)
        {
            _clock = clock;
            _timerInProcessThreshold = timerInProcessThreshold;
        }
    }

    internal void ArmCrash(string stepName, bool beforeStep, string? flowId = null)
    {
        lock (_gate)
        {
            // One slot, so a second arm would silently discard a still-unfired first one — and
            // the test would pass without ever exercising the crash it asked for. Fail loudly
            // instead; arm the next crash after the current one fires.
            if (_armedCrash is { } armed)
                throw new InvalidOperationException(
                    $"A crash is already armed for step '{armed.Step}' ({(armed.Before ? "before" : "after")} the step) and has not fired yet; " +
                    "arm one crash at a time, after the previous one has fired.");

            _armedCrash = (stepName, beforeStep, flowId);
        }
    }

    ValueTask IDurableFlowExecutionObserver.OnStepStartingAsync(DurableFlowStepEvent step)
    {
        Record(EventKind.Starting, step);
        MaybeCrash(step.StepName, beforeStep: true, step.FlowId);
        return default;
    }

    ValueTask IDurableFlowExecutionObserver.OnStepWaitingAsync(DurableFlowStepEvent step)
    {
        Record(EventKind.Waiting, step);
        return default;
    }

    ValueTask IDurableFlowExecutionObserver.OnStepCompletedAsync(DurableFlowStepEvent step)
    {
        Record(EventKind.Completed, step);
        MaybeCrash(step.StepName, beforeStep: false, step.FlowId);
        return default;
    }

    ValueTask IDurableFlowExecutionObserver.OnRunFinishedAsync(DurableFlowRunEvent run)
    {
        _finished[run.FlowId] = run;
        RunWaiter[] due;
        lock (_gate)
        {
            due = [.. _runWaiters.Where(w => w.FlowId == run.FlowId)];
            _runWaiters.RemoveAll(w => w.FlowId == run.FlowId);
        }

        foreach (var waiter in due)
            waiter.Completion.TrySetResult(run);
        return default;
    }

    private void MaybeCrash(string stepName, bool beforeStep, string flowId)
    {
        lock (_gate)
        {
            // A flow-scoped arm only fires on ITS run: the slot is process-wide and one-shot, so
            // an unscoped crash intended for run B could be consumed by run A (or a child flow
            // reusing the step name) reaching the step first — and B's recovery assertion then
            // passes without the recovery path ever executing.
            if (_armedCrash is not { } armed
                || armed.Step != stepName
                || armed.Before != beforeStep
                || (armed.FlowId is not null && !string.Equals(armed.FlowId, flowId, StringComparison.Ordinal)))
            {
                return;
            }

            _armedCrash = null;
        }

        throw new SimulatedCrashException(stepName, beforeStep);
    }

    private void Record(EventKind kind, DurableFlowStepEvent step)
    {
        var recorded = new FlowProbeEvent(kind, step);
        var list = _events.GetOrAdd(step.FlowId, static _ => []);
        Waiter[] due;
        lock (_gate)
        {
            var nowUtc = _clock.GetUtcNow().UtcDateTime;
            list.Add(new Recorded(recorded, _incarnation, kind == EventKind.Waiting && ParksInProcess(list, step, nowUtc), nowUtc));
            due = [.. _waiters.Where(w => w.FlowId == step.FlowId && w.Predicate(recorded))];
            foreach (var waiter in due)
                _waiters.Remove(waiter);
        }

        foreach (var waiter in due)
            waiter.Completion.TrySetResult(recorded);
    }

    /// <summary>
    /// Whether a step's Waiting event is a park held IN PROCESS, which dies with its incarnation:
    /// an awaited step, or a timer whose remainder is at or below the in-process threshold (the
    /// engine decides the same way, against the same clock). A longer timer — and a child-flow
    /// step — suspends instead: the job ends and the wake-up is a scheduled job a restart carries
    /// over. Caller holds <c>_gate</c>.
    /// <para>
    /// A timer's remainder is measured from the clock at the step's Starting event: the engine
    /// computes the remainder it decides on right after that notification, and a first pass then
    /// saves its breadcrumb before notifying Waiting — a clock advanced during that save made a
    /// suspended timer look in process here, and a restart then hid its carried-over wait.
    /// </para>
    /// </summary>
    private bool ParksInProcess(List<Recorded> list, DurableFlowStepEvent step, DateTime nowUtc)
    {
        switch (step.Kind)
        {
            case DurableFlowStepKind.Awaited:
                return true;
            case DurableFlowStepKind.Timer when step.WakeAtUtc is { } wakeAtUtc:
                var decidedAtUtc = nowUtc;
                for (var index = list.Count - 1; index >= 0; index--)
                {
                    var started = list[index];
                    if (started.Event.Kind == EventKind.Starting
                        && started.Event.Step.Kind == DurableFlowStepKind.Timer
                        && string.Equals(started.Event.Step.StepName, step.StepName, StringComparison.Ordinal))
                    {
                        decidedAtUtc = started.ClockUtc;
                        break;
                    }
                }

                return wakeAtUtc - decidedAtUtc <= _timerInProcessThreshold;
            default:
                return false;
        }
    }

    internal int CountEvents(string flowId, string stepName, EventKind kind)
    {
        if (!_events.TryGetValue(flowId, out var list))
            return 0;

        lock (_gate)
            return list.Count(r => r.Event.Kind == kind && r.Event.Step.StepName == stepName);
    }

    internal IReadOnlyList<FlowProbeEvent> EventsFor(string flowId)
    {
        if (!_events.TryGetValue(flowId, out var list))
            return [];

        lock (_gate)
            return [.. list.Select(r => r.Event)];
    }

    internal string? LatestAwaitedCorrelationId(string flowId, string? stepName)
    {
        lock (_gate)
            return LatestAwaitedCorrelationIdCore(flowId, stepName);
    }

    /// <summary>Same as <see cref="LatestAwaitedCorrelationId"/>; the caller holds <c>_gate</c>.</summary>
    private string? LatestAwaitedCorrelationIdCore(string flowId, string? stepName)
    {
        if (!_events.TryGetValue(flowId, out var list))
            return null;

        // Walking backwards, remember the correlation ids of already-answered awaited steps: a
        // Waiting event whose step has a LATER Completed event is consumed — a reply published
        // to that dead correlation id is silently dropped by the channel, and the caller's
        // null-fallback (wait for the run's NEXT awaited step to park) is the correct path.
        // Progress-aware steps stay un-completed across non-terminal replies, so repeated
        // replies to the same live correlation id still resolve here. Only the NEWEST Waiting
        // of each step is considered live at all: a faulted attempt leaves no Completed event
        // behind, and returning its abandoned correlation id (an older Waiting of a step the
        // run has since restarted with a fresh id) would park the caller's reply forever.
        //
        // Across a simulated restart: once the run has recorded anything in the new incarnation,
        // only that incarnation counts (its wait may carry a fresh id). Before that — nothing has
        // re-executed the run yet — the wait that survived the restart is still the live one: a
        // reply to it is routed into the run by lost-subscriber recovery, as for a response that
        // arrives while a real process is down.
        var incarnation = _incarnation;
        var currentOnly = list.Exists(r => r.Incarnation == incarnation);
        HashSet<string>? answered = null;
        HashSet<string>? seenWaitingSteps = null;
        for (var index = list.Count - 1; index >= 0; index--)
        {
            if (currentOnly && list[index].Incarnation != incarnation)
                break;

            var candidate = list[index].Event;
            if (candidate.Kind == EventKind.Completed
                && candidate.Step.Kind == DurableFlowStepKind.Awaited
                && candidate.Step.CorrelationId is { } answeredCid)
            {
                (answered ??= new HashSet<string>(StringComparer.Ordinal)).Add(answeredCid);
                continue;
            }

            if (candidate.Kind == EventKind.Waiting
                && candidate.Step.Kind == DurableFlowStepKind.Awaited
                && candidate.Step.CorrelationId is { } correlationId)
            {
                var newestForStep = (seenWaitingSteps ??= new HashSet<string>(StringComparer.Ordinal)).Add(candidate.Step.StepName);
                if (!newestForStep || answered?.Contains(correlationId) == true)
                    continue;

                if (stepName is null || candidate.Step.StepName == stepName)
                    return correlationId;
            }
        }

        return null;
    }

    /// <summary>
    /// Returns the newest un-answered awaited-step correlation id, or waits for the run's NEXT
    /// awaited park when none is live. The liveness re-check runs under the same gate as the
    /// waiter registration, so a park recorded between the caller's
    /// <see cref="LatestAwaitedCorrelationId"/> miss and this call cannot be skipped — and,
    /// unlike a history replay, an already-answered Waiting is never returned (a reply published
    /// to a consumed correlation id is silently dropped by the channel).
    /// </summary>
    internal async Task<string> WaitForNextAwaitedAsync(
        string flowId,
        string? stepName,
        TimeSpan realTimeGuard,
        string description)
    {
        Waiter waiter;
        lock (_gate)
        {
            if (LatestAwaitedCorrelationIdCore(flowId, stepName) is { } live)
                return live;

            waiter = new Waiter(
                flowId,
                e => e.Kind == EventKind.Waiting
                    && e.Step.Kind == DurableFlowStepKind.Awaited
                    && e.Step.CorrelationId is not null
                    && (stepName is null || e.Step.StepName == stepName),
                new TaskCompletionSource<FlowProbeEvent>(TaskCreationOptions.RunContinuationsAsynchronously));
            _waiters.Add(waiter);
        }

        try
        {
            var stepEvent = await AwaitBoundedAsync(waiter.Completion.Task, realTimeGuard, description).ConfigureAwait(false);
            return stepEvent.Step.CorrelationId!;
        }
        finally
        {
            // Same cleanup contract as WaitForAsync below: timed-out waiters must not accumulate.
            lock (_gate)
                _waiters.Remove(waiter);
        }
    }

    internal async Task<FlowProbeEvent> WaitForAsync(
        string flowId,
        Func<FlowProbeEvent, bool> predicate,
        TimeSpan realTimeGuard,
        string description)
    {
        Waiter waiter;
        lock (_gate)
        {
            if (_events.TryGetValue(flowId, out var list))
            {
                // Replay NEWEST match first: a faulted attempt leaves an older Waiting event for a
                // step the run has since restarted with a fresh correlation id (nothing ever
                // Completes the abandoned one), so front-to-back replay handed back the abandoned
                // event — a reply to its correlation id was silently dropped and the live wait
                // never resolved. An in-process park an earlier incarnation recorded is skipped:
                // after a simulated restart it came straight back, before the new incarnation had
                // parked at all (and with the old id, when the re-executed step arms a fresh one).
                // Only the park: a checkpoint or a suspended timer recorded before the restart is
                // still true, and the replay records neither again.
                for (var index = list.Count - 1; index >= 0; index--)
                {
                    if (list[index].InProcessPark && list[index].Incarnation != _incarnation)
                        continue;

                    if (predicate(list[index].Event))
                        return list[index].Event;
                }
            }

            waiter = new Waiter(flowId, predicate, new TaskCompletionSource<FlowProbeEvent>(TaskCreationOptions.RunContinuationsAsynchronously));
            _waiters.Add(waiter);
        }

        try
        {
            return await AwaitBoundedAsync(waiter.Completion.Task, realTimeGuard, description).ConfigureAwait(false);
        }
        finally
        {
            // A satisfied waiter was already removed by Record; a timed-out one would otherwise
            // stay registered forever — its predicate re-evaluated on every future event and its
            // completion source retained. The gate makes the removal race-safe against a Record
            // completing it at the same moment (either way it leaves the list exactly once).
            lock (_gate)
                _waiters.Remove(waiter);
        }
    }

    internal async Task<DurableFlowRunEvent> WaitForRunAsync(string flowId, TimeSpan realTimeGuard, string description)
    {
        RunWaiter waiter;
        lock (_gate)
        {
            if (_finished.TryGetValue(flowId, out var finished))
                return finished;

            waiter = new RunWaiter(flowId, new TaskCompletionSource<DurableFlowRunEvent>(TaskCreationOptions.RunContinuationsAsynchronously));
            _runWaiters.Add(waiter);
        }

        try
        {
            return await AwaitBoundedAsync(waiter.Completion.Task, realTimeGuard, description).ConfigureAwait(false);
        }
        finally
        {
            // Same cleanup contract as WaitForAsync: timed-out run waiters must not accumulate.
            lock (_gate)
                _runWaiters.Remove(waiter);
        }
    }

    /// <summary>Registered-but-unsatisfied waiters — exposed so tests can prove timed-out waits don't leak.</summary>
    internal int PendingWaiterCount
    {
        get
        {
            lock (_gate)
                return _waiters.Count + _runWaiters.Count;
        }
    }

    private static async Task<T> AwaitBoundedAsync<T>(Task<T> task, TimeSpan realTimeGuard, string description)
    {
        // The guard runs on REAL time deliberately: it bounds a hung test regardless of what the
        // virtual clock is doing (which may be exactly what is broken).
        try
        {
            return await task.WaitAsync(realTimeGuard, TimeProvider.System).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            throw new TimeoutException(
                $"Timed out after {realTimeGuard} (real time) waiting for {description}. " +
                "If the flow is sleeping on a timer or a retry backoff, advance the virtual clock first. " +
                "A run whose last execution failed and exhausted the transport's retry budget also never " +
                "finishes — it stays Running with its wake-up dropped; check the logs for 'dropping it'.");
        }
    }

    private readonly record struct Recorded(FlowProbeEvent Event, int Incarnation, bool InProcessPark, DateTime ClockUtc);

    private sealed record Waiter(string FlowId, Func<FlowProbeEvent, bool> Predicate, TaskCompletionSource<FlowProbeEvent> Completion);

    private sealed record RunWaiter(string FlowId, TaskCompletionSource<DurableFlowRunEvent> Completion);
}
