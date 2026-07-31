using System.Text.Json.Serialization;

namespace AsyncResponse;

/// <summary>Lifecycle of a durable flow run.</summary>
public enum FlowRunStatus
{
    /// <summary>The run is executing or waiting to be (re-)executed.</summary>
    Running = 0,

    /// <summary>The flow body completed; every step checkpointed as done.</summary>
    Succeeded = 1,

    /// <summary>
    /// The run was terminally failed — by a <see cref="DurableFlowFailedException"/>, or by the
    /// lost-subscriber failure route of an awaited step.
    /// </summary>
    Failed = 2,

    /// <summary>
    /// The run is parked by an operator. Wake-ups, recoveries, resumes, and failure signals are
    /// all ignored while suspended, so a dead-lettered run cannot be resurrected or terminally
    /// failed behind the operator's back by a late response. Not terminal: set the status back to
    /// <see cref="Running"/> and call <c>IDurableFlowExecutor.ResumeAsync</c> to replay the run
    /// from its checkpoints. A parent awaiting a suspended child keeps waiting.
    /// </summary>
    Suspended = 3
}

/// <summary>
/// The persisted state of one durable flow run: which steps completed (with memoized results),
/// which awaited step is in flight, and the run's status. This is the flow's entire durable
/// memory — the "ledger" of the checkpointed-flow pattern, owned by the library.
/// <para>
/// <b>Contract warning:</b> instances are serialized into the flow state store and must remain
/// readable across deployments. Treat property names as a wire contract — additive changes only;
/// <see cref="SchemaVersion"/> lets readers reject entries written with an unrecognized schema
/// (see <see cref="FlowStateSchema"/>).
/// </para>
/// </summary>
public sealed class FlowState
{
    /// <summary>The required wire schema version this state was written with.</summary>
    [JsonRequired]
    public int SchemaVersion { get; set; } = FlowStateSchema.Current;

    /// <summary>
    /// Optimistic-concurrency revision maintained by durable flow stores. It starts at zero and is
    /// incremented on every conditional checkpoint so a stale executor cannot overwrite newer state.
    /// </summary>
    public long Revision { get; set; }

    /// <summary>The flow run id.</summary>
    public string? FlowId { get; set; }

    /// <summary>Full name of the flow class, resolved through DI on every (re-)execution.</summary>
    public string? FlowTypeName { get; set; }

    /// <summary>Full name of the input type.</summary>
    public string? InputTypeName { get; set; }

    /// <summary>The flow input, serialized as JSON.</summary>
    public string? InputJson { get; set; }

    /// <summary>The run's lifecycle status.</summary>
    public FlowRunStatus Status { get; set; }

    /// <summary>The most recent progress or outcome message (operator-facing).</summary>
    public string? LastMessage { get; set; }

    /// <summary>UTC timestamp the run was created.</summary>
    public DateTime? CreatedAtUtc { get; set; }

    /// <summary>UTC timestamp of the last persisted change.</summary>
    public DateTime? UpdatedAtUtc { get; set; }

    /// <summary>How many times the flow body has been entered (start, resumes, redeliveries).</summary>
    public int Attempts { get; set; }

    /// <summary>Per-step checkpoints, keyed by the step's stable name.</summary>
    public Dictionary<string, FlowStepState>? Steps { get; set; }

    /// <summary>The flow's user key/value bag (values serialized as JSON).</summary>
    public Dictionary<string, string>? Values { get; set; }

    /// <summary>Parent flow run id when this run was started by <see cref="IDurableFlowContext.AwaitChildFlowAsync{TFlow, TInput}"/>.</summary>
    public string? ParentFlowId { get; set; }

    /// <summary>Parent step name that is waiting for this child flow, when any.</summary>
    public string? ParentStepName { get; set; }

    /// <summary>
    /// Serialized ambient context captured when the run was started (see
    /// <see cref="IAsyncResponseContextPropagator"/>), restored before every (re-)execution —
    /// which may happen in a different deployment.
    /// </summary>
    public Dictionary<string, string>? Context { get; set; }
}

/// <summary>One step's checkpoint inside <see cref="FlowState"/>.</summary>
public sealed class FlowStepState
{
    /// <summary>Whether the step completed; completed steps are skipped on re-runs.</summary>
    public bool Completed { get; set; }

    /// <summary>The step's memoized result (or terminal response payload), serialized as JSON.</summary>
    public string? ResultJson { get; set; }

    /// <summary>
    /// The correlation id of the in-flight awaited operation — the breadcrumb a re-run uses to
    /// re-attach instead of re-triggering. Cleared when the step completes.
    /// </summary>
    public string? PendingCorrelationId { get; set; }

    /// <summary>
    /// Whether the last attempt of this step faulted (timeout or exception); a faulted awaited
    /// step is restarted fresh instead of re-attached.
    /// </summary>
    public bool Faulted { get; set; }

    /// <summary>The step's most recent message (progress or failure).</summary>
    public string? Message { get; set; }

    /// <summary>Child flow run id when this checkpoint is waiting for a child flow.</summary>
    public string? ChildFlowId { get; set; }

    /// <summary>UTC timestamp the step completed.</summary>
    public DateTime? CompletedAtUtc { get; set; }
}

/// <summary>
/// Wire-schema version stamp for <see cref="FlowState"/>. Readers reject versions not explicitly
/// supported by the build instead of guessing compatibility.
/// </summary>
public static class FlowStateSchema
{
    /// <summary>The current wire schema version written by this build.</summary>
    public const int Current = 1;

    /// <summary>
    /// Returns <c>true</c> only for a schema version explicitly supported by this build. Add older
    /// versions here deliberately if a future release provides a tested migration path.
    /// </summary>
    public static bool IsReadable(int entryVersion) => entryVersion == Current;
}
