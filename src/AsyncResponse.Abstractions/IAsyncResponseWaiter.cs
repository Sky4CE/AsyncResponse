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
    /// or faults if an error envelope arrives or the wait times out.
    /// </summary>
    Task<T> ResponseTask { get; }
}
