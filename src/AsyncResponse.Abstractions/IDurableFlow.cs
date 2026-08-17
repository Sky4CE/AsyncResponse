namespace AsyncResponse;

/// <summary>
/// A durable multi-step flow: plain sequential C# whose steps are checkpointed, so the flow can be
/// killed at any point (crash, redeploy, redelivery) and re-run safely — completed steps are
/// skipped, the in-flight awaited step is re-attached, and everything after continues.
/// <para>
/// Implement the flow as ordinary code: conditionals, loops, and data flow are all allowed. The
/// only rules are that each step has a stable unique name (names are persisted in the flow state)
/// and that step bodies are safe to re-execute when the library cannot prove they completed
/// (at-least-once, like every other delivery guarantee in AsyncResponse).
/// </para>
/// <para>
/// Register the implementation in DI (e.g. <c>services.AddScoped&lt;MyFlow&gt;()</c>) and start it with
/// <see cref="IDurableFlows.StartAsync{TFlow,TInput}"/>. The flow class name is persisted in the
/// flow state and resolved when the flow is executed or resumed — treat the class name as a wire
/// contract (rename with a forwarding type, like recovery-callback names).
/// </para>
/// </summary>
/// <typeparam name="TInput">
/// The flow's input, persisted as JSON with the flow state and handed to every (re-)execution.
/// Use one serializable record; capture nothing through closures.
/// </typeparam>
public interface IDurableFlow<in TInput>
{
    /// <summary>
    /// The flow body. Invoked on start and on every resume/redelivery — always from the top, with
    /// completed steps skipping via their checkpoints. Must therefore be safe to call repeatedly.
    /// </summary>
    /// <param name="flow">The step context: checkpointed steps, awaited steps, progress, values.</param>
    /// <param name="input">The input the flow was started with, rehydrated from the flow state.</param>
    Task ExecuteAsync(IDurableFlowContext flow, TInput input);
}

/// <summary>
/// Terminates a durable flow run as <see cref="FlowRunStatus.Failed"/> without transport
/// redelivery. Any other exception thrown from a flow is treated as retriable: it propagates to
/// the worker transport, which redelivers the flow run with bounded attempts and dead-letters it
/// when they are exhausted.
/// </summary>
public sealed class DurableFlowFailedException : Exception
{
    /// <summary>Creates a terminal flow failure with an operator-facing message.</summary>
    public DurableFlowFailedException(string message) : base(message)
    {
    }

    /// <summary>Creates a terminal flow failure wrapping the causing exception.</summary>
    public DurableFlowFailedException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// Thrown by <c>IDurableFlows.StartAsync</c> when the requested flow id already exists bound to a
/// different flow type or a semantically different input — the idempotent-start contract accepts
/// only exact retries. Derives from <see cref="InvalidOperationException"/> for compatibility with
/// callers that catch that. The distinct type exists so callers running deterministic-id races
/// (the scheduled-flow service, outbox-style starters) can tell <em>this id is taken with other
/// data</em> apart from every other <see cref="InvalidOperationException"/> a start can throw —
/// treating, say, an input-factory failure as a benign duplicate would report an occurrence as
/// having run when nothing ran.
/// </summary>
public sealed class DurableFlowIdConflictException : InvalidOperationException
{
    /// <summary>Creates the conflict with an operator-facing message naming the flow id.</summary>
    public DurableFlowIdConflictException(string message) : base(message)
    {
    }
}

/// <summary>
/// Thrown by <c>IDurableFlows.StartAsync</c> when the flow's ledger was committed but its worker
/// job could not be published — the run exists, in <c>Running</c>, with nothing scheduled to
/// execute it.
/// <para>
/// The distinct type exists to carry <see cref="FlowId"/> out of the failure. A start called
/// without an explicit id generates one, and a plain throw discarded it: the caller was left
/// knowing a flow might exist but not which, and the store interface has no enumeration to go
/// looking. With the id in hand, recovery is a re-call of <c>StartAsync</c> with that same id —
/// idempotent by contract, since an identical start re-enqueues the existing run rather than
/// creating a second one.
/// </para>
/// <para>
/// Publication is retried before this surfaces, so it means the transport stayed unavailable, not
/// that it blinked. The ambiguous case is deliberately included: a publish that may or may not
/// have landed also throws here, because a duplicate delivery of a durable flow is harmless
/// (completed steps skip via their checkpoints) while a dropped one is not.
/// </para>
/// </summary>
public sealed class DurableFlowNotDispatchedException : InvalidOperationException
{
    /// <summary>Creates the failure for <paramref name="flowId"/>.</summary>
    public DurableFlowNotDispatchedException(string flowId, Exception? innerException = null)
        : base(
            $"Durable flow '{flowId}' was persisted but its worker job could not be published, so nothing " +
            $"is scheduled to execute it. Retry the start with this same flow id — an identical start " +
            $"re-enqueues the existing run instead of creating a duplicate.",
            innerException)
        => FlowId = flowId;

    /// <summary>The id of the persisted-but-undispatched run, so a caller can re-drive it.</summary>
    public string FlowId { get; }
}
