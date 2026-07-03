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
