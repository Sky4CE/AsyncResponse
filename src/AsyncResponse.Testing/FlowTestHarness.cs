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
    /// next delivery — the flow class under test needs no instrumentation.
    /// </summary>
    public void CrashBeforeStep(string stepName)
        => _probe.ArmCrash(stepName, beforeStep: true);

    /// <summary>
    /// Arms a one-shot crash that fires right after <paramref name="stepName"/>'s completion
    /// checkpoint persists — the classic "died between the checkpoint and the next step" window.
    /// </summary>
    public void CrashAfterStep(string stepName)
        => _probe.ArmCrash(stepName, beforeStep: false);

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
    public Task ExecuteDirectAsync()
        => _harness.Engine.FlowExecutor.ExecuteAsync(FlowId);

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
    /// awaiting its response, and returns the correlation id to answer.
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

    /// <summary>Waits until a timer step is parked, returning its due time (advance the clock past it to wake the run).</summary>
    public async Task<DateTime> WaitForTimerStepAsync(string stepName)
    {
        var stepEvent = await _harness.Probe.WaitForAsync(
            FlowId,
            e => e.Kind == FlowProbe.EventKind.Waiting && e.Step.Kind == DurableFlowStepKind.Timer && e.Step.StepName == stepName,
            _harness.Engine.RealTimeGuard,
            $"timer step '{stepName}' of flow '{FlowId}' to be sleeping").ConfigureAwait(false);
        return stepEvent.Step.WakeAtUtc!.Value;
    }

    /// <summary>Waits until the given step's completion checkpoint persists.</summary>
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
    /// </summary>
    public async Task ReplyAsync<T>(T response, string? stepName = null) where T : IAsyncResponsePayload
    {
        var correlationId = _harness.Probe.LatestAwaitedCorrelationId(FlowId, stepName)
            ?? (stepName is null
                ? await WaitForLatestAwaitedAsync().ConfigureAwait(false)
                : await WaitForAwaitingStepAsync(stepName).ConfigureAwait(false));
        await _harness.Engine.PublishAsync(response, correlationId).ConfigureAwait(false);
    }

    /// <summary>Fails the run's currently awaited step with an exception (a remote failure).</summary>
    public async Task ReplyExceptionAsync(Exception exception, string? stepName = null)
    {
        var correlationId = _harness.Probe.LatestAwaitedCorrelationId(FlowId, stepName)
            ?? (stepName is null
                ? await WaitForLatestAwaitedAsync().ConfigureAwait(false)
                : await WaitForAwaitingStepAsync(stepName).ConfigureAwait(false));
        await _harness.Engine.PublishExceptionAsync(exception, correlationId).ConfigureAwait(false);
    }

    private async Task<string> WaitForLatestAwaitedAsync()
    {
        var stepEvent = await _harness.Probe.WaitForAsync(
            FlowId,
            e => e.Kind == FlowProbe.EventKind.Waiting && e.Step.Kind == DurableFlowStepKind.Awaited,
            _harness.Engine.RealTimeGuard,
            $"flow '{FlowId}' to be awaiting any step").ConfigureAwait(false);
        return stepEvent.Step.CorrelationId!;
    }
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
    private readonly ConcurrentDictionary<string, List<FlowProbeEvent>> _events = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DurableFlowRunEvent> _finished = new(StringComparer.Ordinal);
    private readonly List<Waiter> _waiters = [];
    private readonly List<RunWaiter> _runWaiters = [];
    private (string Step, bool Before)? _armedCrash;

    internal void ArmCrash(string stepName, bool beforeStep)
    {
        lock (_gate)
            _armedCrash = (stepName, beforeStep);
    }

    ValueTask IDurableFlowExecutionObserver.OnStepStartingAsync(DurableFlowStepEvent step)
    {
        Record(EventKind.Starting, step);
        MaybeCrash(step.StepName, beforeStep: true);
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
        MaybeCrash(step.StepName, beforeStep: false);
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

    private void MaybeCrash(string stepName, bool beforeStep)
    {
        lock (_gate)
        {
            if (_armedCrash is not { } armed || armed.Step != stepName || armed.Before != beforeStep)
                return;

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
            list.Add(recorded);
            due = [.. _waiters.Where(w => w.FlowId == step.FlowId && w.Predicate(recorded))];
            foreach (var waiter in due)
                _waiters.Remove(waiter);
        }

        foreach (var waiter in due)
            waiter.Completion.TrySetResult(recorded);
    }

    internal int CountEvents(string flowId, string stepName, EventKind kind)
    {
        if (!_events.TryGetValue(flowId, out var list))
            return 0;

        lock (_gate)
            return list.Count(e => e.Kind == kind && e.Step.StepName == stepName);
    }

    internal IReadOnlyList<FlowProbeEvent> EventsFor(string flowId)
    {
        if (!_events.TryGetValue(flowId, out var list))
            return [];

        lock (_gate)
            return [.. list];
    }

    internal string? LatestAwaitedCorrelationId(string flowId, string? stepName)
    {
        if (!_events.TryGetValue(flowId, out var list))
            return null;

        lock (_gate)
        {
            // Walking backwards, remember the correlation ids of already-answered awaited steps: a
            // Waiting event whose step has a LATER Completed event is consumed — a reply published
            // to that dead correlation id is silently dropped by the channel, and the caller's
            // null-fallback (wait for the run's NEXT awaited step to park) is the correct path.
            // Progress-aware steps stay un-completed across non-terminal replies, so repeated
            // replies to the same live correlation id still resolve here.
            HashSet<string>? answered = null;
            for (var index = list.Count - 1; index >= 0; index--)
            {
                var candidate = list[index];
                if (candidate.Kind == EventKind.Completed
                    && candidate.Step.Kind == DurableFlowStepKind.Awaited
                    && candidate.Step.CorrelationId is { } answeredCid)
                {
                    (answered ??= new HashSet<string>(StringComparer.Ordinal)).Add(answeredCid);
                    continue;
                }

                if (candidate.Kind == EventKind.Waiting
                    && candidate.Step.Kind == DurableFlowStepKind.Awaited
                    && (stepName is null || candidate.Step.StepName == stepName)
                    && candidate.Step.CorrelationId is { } correlationId
                    && answered?.Contains(correlationId) != true)
                {
                    return correlationId;
                }
            }
        }

        return null;
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
                foreach (var recorded in list)
                {
                    if (predicate(recorded))
                        return recorded;
                }
            }

            waiter = new Waiter(flowId, predicate, new TaskCompletionSource<FlowProbeEvent>(TaskCreationOptions.RunContinuationsAsynchronously));
            _waiters.Add(waiter);
        }

        return await AwaitBoundedAsync(waiter.Completion.Task, realTimeGuard, description).ConfigureAwait(false);
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

        return await AwaitBoundedAsync(waiter.Completion.Task, realTimeGuard, description).ConfigureAwait(false);
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

    private sealed record Waiter(string FlowId, Func<FlowProbeEvent, bool> Predicate, TaskCompletionSource<FlowProbeEvent> Completion);

    private sealed record RunWaiter(string FlowId, TaskCompletionSource<DurableFlowRunEvent> Completion);
}
