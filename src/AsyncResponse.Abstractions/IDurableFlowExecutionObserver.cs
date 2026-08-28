namespace AsyncResponse;

/// <summary>What a <see cref="IDurableFlowContext"/> step is.</summary>
public enum DurableFlowStepKind
{
    /// <summary>A local unit of work (<c>StepAsync</c>).</summary>
    Local = 0,

    /// <summary>A remote operation awaited by correlation id (<c>AwaitStepAsync</c>).</summary>
    Awaited = 1,

    /// <summary>A durable timer (<c>DelayAsync</c> / <c>DelayUntilAsync</c>).</summary>
    Timer = 2,

    /// <summary>A child durable flow (<c>AwaitChildFlowAsync</c>).</summary>
    ChildFlow = 3
}

/// <summary>One step-lifecycle observation of a durable-flow execution.</summary>
/// <param name="FlowId">The flow run id.</param>
/// <param name="StepName">The step's stable name.</param>
/// <param name="Kind">What the step is.</param>
/// <param name="CorrelationId">
/// The awaited step's correlation id (the in-flight breadcrumb), when <paramref name="Kind"/> is
/// <see cref="DurableFlowStepKind.Awaited"/>.
/// </param>
/// <param name="WakeAtUtc">
/// The timer step's due time, when <paramref name="Kind"/> is <see cref="DurableFlowStepKind.Timer"/>.
/// </param>
public readonly record struct DurableFlowStepEvent(
    string FlowId,
    string StepName,
    DurableFlowStepKind Kind,
    string? CorrelationId = null,
    DateTime? WakeAtUtc = null);

/// <summary>A terminal run observation of a durable-flow execution.</summary>
/// <param name="FlowId">The flow run id.</param>
/// <param name="Status">The terminal status the run reached.</param>
/// <param name="Message">The run's last operator-facing message.</param>
public readonly record struct DurableFlowRunEvent(
    string FlowId,
    FlowRunStatus Status,
    string? Message);

/// <summary>
/// Observes durable-flow execution from inside the executor: step activations, waits, checkpoint
/// completions, and terminal run transitions. Register implementations in DI (any number, <b>as
/// singletons</b> — the singleton flow executor resolves them once from the root provider and
/// holds them for its lifetime; a scoped or transient registration fails resolution with an error
/// naming this requirement). The flow executor invokes every registered observer synchronously on
/// the execution path.
/// <para>
/// Intended for test instrumentation (AsyncResponse.Testing's harness is built on it) and
/// lightweight production telemetry. Because observers run <em>on</em> the execution path, an
/// observer that throws fails the current execution attempt exactly like a step failure — the
/// delivery retries from the last checkpoint. AsyncResponse.Testing uses precisely that contract
/// to inject deterministic crashes at step boundaries; telemetry observers must not throw.
/// </para>
/// <para>
/// All methods have no-op default implementations, so an observer overrides only what it needs.
/// </para>
/// </summary>
public interface IDurableFlowExecutionObserver
{
    /// <summary>
    /// A not-yet-completed step is about to execute its body (local step), trigger or re-attach
    /// (awaited step), start or continue its wait (timer), or start/await its child (child-flow
    /// step). Memoized (completed) steps are skipped without an event. Fires before the step's
    /// first side effect of this execution.
    /// </summary>
    ValueTask OnStepStartingAsync(DurableFlowStepEvent step) => default;

    /// <summary>
    /// The step is now durably parked: an awaited step's trigger completed (or its re-attach began)
    /// and the run is waiting on <see cref="DurableFlowStepEvent.CorrelationId"/>; a timer step is
    /// waiting for <see cref="DurableFlowStepEvent.WakeAtUtc"/>; a child-flow step is about to
    /// suspend this run for its child.
    /// </summary>
    ValueTask OnStepWaitingAsync(DurableFlowStepEvent step) => default;

    /// <summary>The step's completion checkpoint was persisted.</summary>
    ValueTask OnStepCompletedAsync(DurableFlowStepEvent step) => default;

    /// <summary>
    /// The current execution attempt ended without the run reaching a terminal status: the
    /// attempt failed (or lost its execution lease) and the delivery retries from the last
    /// checkpoint. Any step this attempt had reported waiting is no longer parked in this
    /// process; the retry re-raises whatever still applies. Best-effort, unlike the other
    /// events: the attempt's own exception is already propagating, so an observer throw here is
    /// swallowed rather than allowed to mask it.
    /// </summary>
    ValueTask OnRunAttemptFailedAsync(DurableFlowRunEvent run) => default;

    /// <summary>
    /// The run reached a terminal status (<see cref="FlowRunStatus.Succeeded"/> or
    /// <see cref="FlowRunStatus.Failed"/>). Delivered <b>at least once</b> per terminal run: a
    /// redelivered duplicate of an already-terminal wake-up re-notifies before acking, because the
    /// original notification may be exactly what failed the prior delivery — implementations must
    /// tolerate duplicates.
    /// </summary>
    ValueTask OnRunFinishedAsync(DurableFlowRunEvent run) => default;
}
