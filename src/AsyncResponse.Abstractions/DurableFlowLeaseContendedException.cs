namespace AsyncResponse;

/// <summary>
/// Thrown when a durable-flow wake-up could not acquire the execution lease and found no proof
/// that the run is executing on a live worker.
/// <para>
/// A held lease is not proof of a live holder: the holder may have died inside its unexpired
/// lease window, and the message being handled is then very likely the run's only remaining
/// wake-up. The engine acknowledges a contended wake-up as a duplicate only on evidence — the
/// lease was renewed or taken over while the wake-up waited (see
/// <see cref="IFlowStateStore.ObserveLeaseAsync"/>) — never because its <em>own</em> configured
/// lease window elapsed: the lease in the way may have been issued by another deployment with a
/// longer <c>ExecutionLeaseDuration</c>, and acknowledging on local configuration stranded those
/// runs as <see cref="FlowRunStatus.Running"/> forever.
/// </para>
/// <para>
/// Throwing keeps the wake-up with the worker transport, whose retry policy redelivers it and
/// whose dead-letter queue is the alarm if the contention never resolves.
/// </para>
/// </summary>
public sealed class DurableFlowLeaseContendedException : InvalidOperationException
{
    /// <summary>Creates the exception for <paramref name="flowId"/>.</summary>
    public DurableFlowLeaseContendedException(string flowId, string reason)
        : base(
            $"Durable flow '{flowId}' wake-up could not acquire the execution lease and cannot prove the run is executing elsewhere ({reason}). " +
            "The wake-up is not acknowledged; the worker transport redelivers it.")
    {
        FlowId = flowId;
        Reason = reason;
    }

    /// <summary>The flow whose execution lease stayed contended.</summary>
    public string FlowId { get; }

    /// <summary>Why the contention could not be resolved, for operator triage.</summary>
    public string Reason { get; }
}
