namespace AsyncResponse;

/// <summary>
/// Represents a pending asynchronous response arriving via a correlation channel.
/// Disposal is asynchronous only (<c>await using</c>): it unsubscribes from the channel and clears
/// the persisted recovery state for this correlation id. The waiter deliberately does not expose a
/// synchronous <see cref="IDisposable.Dispose"/> — cleanup is genuinely async (broker unsubscribe +
/// recovery-store delete), and a sync-over-async bridge risked deadlocks under a synchronous
/// <c>using</c> in a sync context.
/// </summary>
/// <typeparam name="T">The response payload type.</typeparam>
public interface IAsyncResponseWaiter<T> : IAsyncDisposable where T : IAsyncResponsePayload
{
    /// <summary>
    /// A task that completes with the response payload when it is published to the channel,
    /// faults if an error envelope arrives or the wait times out, or is canceled when the waiter
    /// is disposed before any terminal signal — so a caller holding this task directly never
    /// waits forever on an abandoned subscription.
    /// </summary>
    Task<T> ResponseTask { get; }
}
