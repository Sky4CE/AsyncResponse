namespace AsyncResponse;

/// <summary>
/// Represents a pending asynchronous response arriving via a correlation channel.
/// Disposing (sync or async) unsubscribes from the channel and clears the persisted
/// recovery state for this correlation id.
/// </summary>
/// <typeparam name="T">The response payload type.</typeparam>
public interface IAsyncResponseWaiter<T> : IDisposable, IAsyncDisposable where T : IAsyncResponsePayload
{
    /// <summary>
    /// A task that completes with the response payload when it is published to the channel,
    /// or faults if an error envelope arrives or the wait times out.
    /// </summary>
    Task<T> ResponseTask { get; }
}
