namespace AsyncResponse;

/// <summary>
/// Thrown by <see cref="IRecoveryStateStore.GetAllAsync"/> when the store cannot confirm that its
/// answer includes every registration saved before the lookup — so the answer must not be settled
/// on. The MongoDB channel's lookup, for one, first gets a majority-acknowledged write through and
/// only then reads the registrations: a primary that a partition has deposed keeps serving reads
/// until it notices, and its answer can miss registrations the new primary took. While no majority
/// acknowledges that write (such a deposed primary, or a primary-secondary-arbiter set with its
/// secondary down) the lookup is refused with this exception, the store's own error inside.
/// <para>
/// Nothing was dispatched and nothing was consumed. Like
/// <see cref="RecoveryStateUnreadableException"/>, the response must not be acknowledged as handled:
/// the broker ingress retries it on its ladder and then propagates it without escalating through
/// <c>SetException</c> — that escalation's own dispatch makes the same lookup, and on a set that
/// had just recovered it would run the FAILURE callback for a response the worker produced
/// successfully — so the transport redelivers the original response, which settles once the store
/// can confirm its view. A direct caller of <c>SetResponse</c>/<c>SetException</c> should retry
/// the same way.
/// </para>
/// </summary>
public sealed class RecoveryStateUnconfirmedException : InvalidOperationException
{
    /// <summary>Creates the exception for <paramref name="correlationId"/>, wrapping the store's own failure.</summary>
    public RecoveryStateUnconfirmedException(string? correlationId, Exception innerException)
        : base(
            $"The recovery registrations for correlation id '{correlationId}' could not be read authoritatively: the store " +
            "could not confirm that its view includes every registration saved before this lookup. No registration was " +
            "dispatched and the response must not be acknowledged as handled; retry the delivery.",
            innerException)
    {
        CorrelationId = correlationId;
    }

    /// <summary>The correlation id whose registrations could not be confirmed.</summary>
    public string? CorrelationId { get; }
}
