namespace AsyncResponse;

/// <summary>
/// Thrown when a durable-flow ledger row exists but this build cannot interpret it: its JSON is
/// malformed, or it carries a schema version outside <see cref="FlowStateSchema.IsReadable"/>.
/// <para>
/// This is the distinction <see cref="IFlowStateStore.LoadAsync"/> exists to preserve. A
/// <c>null</c> load means the run is genuinely gone — never started, already pruned, TTL expired —
/// and the only correct response is to acknowledge the wake-up and stop. An unreadable ledger
/// means the opposite: the run is still there, still <see cref="FlowRunStatus.Running"/>, and the
/// message being handled is very likely its only remaining wake-up. Collapsing the two let a
/// replica that could not read a ledger report success and acknowledge that wake-up, stranding a
/// live flow with nothing left to resume it.
/// </para>
/// <para>
/// The realistic source is a rolling deployment, which is what the schema gate is for: an older
/// replica draws a job whose ledger a newer replica already rewrote. Throwing routes that job into
/// the transport's normal retry/dead-letter path, where a replica that <em>can</em> read the
/// ledger gets its turn and an operator sees the ones that nothing can.
/// </para>
/// </summary>
public sealed class FlowStateUnreadableException : InvalidOperationException
{
    /// <summary>Creates the exception for <paramref name="flowId"/>.</summary>
    public FlowStateUnreadableException(string flowId, string reason, Exception? innerException = null)
        : base(
            $"Durable flow '{flowId}' has a stored ledger that this build cannot read ({reason}). " +
            "The run still exists and must not be acknowledged as complete; the delivery is retried " +
            "or dead-lettered so a build that can read it, or an operator, resolves it.",
            innerException)
    {
        FlowId = flowId;
        Reason = reason;
    }

    /// <summary>The flow whose ledger could not be read.</summary>
    public string FlowId { get; }

    /// <summary>Why the ledger could not be read, for metrics and operator triage.</summary>
    public string Reason { get; }
}
