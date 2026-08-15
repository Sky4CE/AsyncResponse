namespace AsyncResponse;

/// <summary>
/// Optional capability of a response channel: report how many live waiters are currently
/// subscribed to a correlation id's channel. The async-response watchdog uses it to tell an armed
/// recovery entry that still has a waiting process (healthy) apart from one whose waiter died
/// (the precursor of a stuck flow), without knowing anything channel-specific.
/// <para>
/// Built-in channels implement this on the same instance that implements
/// <see cref="IAsyncResponseSubscriber"/>; it is registered alongside the channel by its
/// <c>With…</c> registration. A custom channel that does not implement it leaves liveness unknown,
/// and the watchdog will not flag entries as stale.
/// </para>
/// </summary>
public interface IActiveSubscriberProbe
{
    /// <summary>
    /// Returns the number of live subscribers awaiting the response channel for
    /// <paramref name="correlationId"/>. <c>0</c> means no waiter is listening (e.g. the owning
    /// process died); a positive value means at least one waiter is still awaiting.
    /// <para>
    /// A <b>negative</b> value means liveness could not be determined — the probe itself failed
    /// (broker unreachable, missing schema, no connected endpoint to ask). Implementations must
    /// return a negative value rather than <c>0</c> in that case: <c>0</c> asserts there is
    /// definitively no waiter, which would make the watchdog flag every over-threshold
    /// registration stale during a transient probe outage. The watchdog treats unknown liveness
    /// as "not stale".
    /// </para>
    /// </summary>
    ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default);
}
