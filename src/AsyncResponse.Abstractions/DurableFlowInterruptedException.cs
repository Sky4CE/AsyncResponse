namespace AsyncResponse;

/// <summary>
/// Thrown out of an <see cref="IDurableFlowContext"/> call when <em>this execution attempt</em> of
/// a flow was interrupted — not when the step, or the run, failed:
/// <list type="bullet">
/// <item>the run parked (a durable timer or a child flow suspended it; its wake-up is already
/// published and the next delivery replays from the checkpoints);</item>
/// <item>the host is stopping while the run was parked in process on a timer or an awaited
/// response (the delivery is handed back to the worker transport and redelivered after the
/// restart; the step's checkpoint is left untouched, so the replay waits out the remainder or
/// re-attaches to the same correlation id).</item>
/// </list>
/// <para>
/// Nothing went wrong with the work, so flow code must let it propagate: a catch-all that treats
/// it as a step failure runs compensation — or throws <see cref="DurableFlowFailedException"/> and
/// terminally fails the run — because of a deploy. It derives from
/// <see cref="OperationCanceledException"/>, so the usual
/// <c>catch (Exception ex) when (ex is not OperationCanceledException)</c> filter already excludes
/// it, together with the caller-token cancellations that mean the same thing ("stop this
/// execution", never "the step failed").
/// </para>
/// </summary>
public class DurableFlowInterruptedException : OperationCanceledException
{
    /// <summary>Creates the exception with an operator-facing message.</summary>
    public DurableFlowInterruptedException(string message) : base(message)
    {
    }

    /// <summary>Creates the exception wrapping the cancellation (or other signal) that interrupted the attempt.</summary>
    public DurableFlowInterruptedException(string message, Exception? innerException) : base(message, innerException)
    {
    }
}
