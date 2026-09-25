namespace AsyncResponse;

/// <summary>
/// Faults a waiter's <see cref="IAsyncResponseWaiter{T}.ResponseTask"/> when the channel can no
/// longer say whether the response was delivered. Three causes: the wait was ENDED — by disposal,
/// or by its own timeout — while a delivery was still in flight, without learning its outcome (the
/// drain of a dispatch that had already claimed a message — typically an <c>Until</c> predicate
/// still running user code — did not finish within the channel's <c>DisposalDrainTimeout</c>,
/// which bounds the timeout's drain as well as disposal's); or a fire-and-forget channel was
/// <em>overloaded</em> (responses for the correlation id arrived faster than the wait could process
/// them and the bounded per-wait buffer filled; the next one could not be admitted). Either way the
/// response may or may not have been consumed from the channel.
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
    /// <summary>
    /// The lapsed-drain form: the wait was ended — disposed, or timed out — while a delivery was in
    /// flight, and that delivery did not finish within <paramref name="drainTimeout"/>.
    /// </summary>
    public AsyncResponseIndeterminateDeliveryException(string? correlationId, TimeSpan drainTimeout)
        : base($"The wait for correlationId '{correlationId}' was ended (disposed or timed out) with a response delivery still " +
               $"in flight, which did not finish within {drainTimeout.TotalSeconds:0.###}s. The response may already have been " +
               "consumed from the channel; treat delivery as indeterminate and restart the awaiting " +
               "(idempotent) step instead of re-attaching to this correlation id.")
    {
        CorrelationId = correlationId;
    }

    /// <summary>
    /// The overload form: <paramref name="bufferedMessages"/> responses were already queued behind
    /// the wait's serial processing when the next one arrived and could not be admitted. Nothing
    /// is discarded silently — the wait is faulted so the caller restarts the (idempotent) step —
    /// but a terminal response may be among the queued or the refused ones.
    /// </summary>
    public AsyncResponseIndeterminateDeliveryException(string? correlationId, int bufferedMessages)
        : base($"Responses for correlationId '{correlationId}' arrived faster than the wait could process them: " +
               $"{bufferedMessages} were already queued behind its serial processing when the next one could not be " +
               "admitted. A terminal response may be among them; treat delivery as indeterminate and restart the " +
               "awaiting (idempotent) step instead of re-attaching to this correlation id. Speed up the completion " +
               "predicate, publish fewer progress messages, or use a retained (database) channel whose backlog stays server-side.")
    {
        CorrelationId = correlationId;
        BufferedMessages = bufferedMessages;
    }

    /// <summary>The correlation id whose delivery outcome is unknown.</summary>
    public string? CorrelationId { get; }

    /// <summary>
    /// For the overload form, how many responses were queued behind the wait's serial processing
    /// when the next one could not be admitted; <c>0</c> for the lapsed-drain form.
    /// </summary>
    public int BufferedMessages { get; }
}
