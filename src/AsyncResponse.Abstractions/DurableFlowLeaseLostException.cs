namespace AsyncResponse;

/// <summary>
/// Thrown out of an <see cref="IDurableFlowContext"/> call when <em>this execution attempt</em> of
/// a flow no longer owns the run: its execution lease lapsed or was taken over by another worker,
/// a checkpoint was refused because someone else wrote the ledger first (a lost-subscriber
/// recovery, a failure signal, an operator suspending the run), or the attempt had already
/// released its lease (it parked, or ended). Every checkpoint up to the last committed one is
/// intact, and the work is about to be replayed — by the worker that took the run over, or by
/// the redelivery of this attempt's wake-up.
/// <para>
/// Nothing went wrong with the step, so flow code must not treat it as a step failure: a catch-all
/// that compensates on it runs the compensation on a deposed worker — directly, outside any step,
/// nothing fences it — while the run carries on elsewhere as if the work had succeeded. Exclude it
/// from compensation filters alongside the interruptions:
/// <c>catch (Exception ex) when (ex is not (OperationCanceledException or DurableFlowLeaseLostException))</c>.
/// Context calls made after it keep throwing it, so a swallowed one cannot checkpoint anything.
/// </para>
/// <para>
/// It derives from <see cref="InvalidOperationException"/>, which lease loss surfaced as before
/// this type existed, so existing handlers keep catching it.
/// </para>
/// </summary>
public sealed class DurableFlowLeaseLostException : InvalidOperationException
{
    /// <summary>Creates the exception for <paramref name="flowId"/> with an operator-facing message.</summary>
    public DurableFlowLeaseLostException(string flowId, string message)
        : base(message)
    {
        FlowId = flowId;
    }

    /// <summary>
    /// Creates the exception for <paramref name="flowId"/>, carrying the failure that was in
    /// flight when the loss surfaced (the attempt's own exception is not discarded).
    /// </summary>
    public DurableFlowLeaseLostException(string flowId, string message, Exception? innerException)
        : base(message, innerException)
    {
        FlowId = flowId;
    }

    /// <summary>The flow whose execution this attempt no longer owns.</summary>
    public string FlowId { get; }
}
