namespace AsyncResponse.Channels.NATS;

/// <inheritdoc cref="IAsyncResponseWaiter{T}"/>
internal sealed class NatsAsyncResponseWaiter<T>(
    Task<T> _responseTask,
    Func<ValueTask> _cleanupAsync) : IAsyncResponseWaiter<T> where T : IAsyncResponsePayload
{
    /// <inheritdoc/>
    public Task<T> ResponseTask => _responseTask;

    public ValueTask DisposeAsync()
        => _cleanupAsync();
}
