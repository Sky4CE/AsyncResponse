namespace AsyncResponse;

/// <summary>
/// Faults a waiter's <see cref="IAsyncResponseWaiter{T}.ResponseTask"/> when disposal abandoned an
/// in-flight delivery without learning its outcome: the drain of a dispatch that had already
/// claimed a message — typically an <c>Until</c> predicate still running user code — did not finish
/// within the channel's <c>DisposalDrainTimeout</c>. The response may or may not have been consumed
/// from the channel.
/// <para>
/// This is deliberately <em>not</em> a cancellation. A canceled response task tells the caller
/// "nothing was delivered", which invites re-attaching to the correlation id — and if the wedged
/// delivery had consumed the message, that re-attached wait can never be answered. Treat the
/// awaiting step as indeterminate and restart it fresh (steps are idempotent);
/// <c>DurableFlowContext</c> does so automatically through its faulted-wait path.
/// </para>
/// </summary>
public sealed class AsyncResponseIndeterminateDeliveryException : Exception
{
    /// <summary>Runs the AsyncResponseIndeterminateDeliveryException operation.</summary>
    public AsyncResponseIndeterminateDeliveryException(string? correlationId, TimeSpan drainTimeout)
        : base($"Disposal abandoned an in-flight response delivery for correlationId '{correlationId}' " +
               $"after draining for {drainTimeout.TotalSeconds:0.###}s. The response may already have been " +
               "consumed from the channel; treat delivery as indeterminate and restart the awaiting " +
               "(idempotent) step instead of re-attaching to this correlation id.")
    {
        CorrelationId = correlationId;
    }

    /// <summary>The correlation id whose delivery outcome is unknown.</summary>
    public string? CorrelationId { get; }
}
