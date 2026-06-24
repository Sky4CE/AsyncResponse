namespace AsyncResponse.Channels.Redis;

/// <inheritdoc cref="IAsyncResponseWaiter{T}"/>
internal sealed class RedisAsyncResponseWaiter<T>(
    Task<T> _responseTask,
    Func<ValueTask> _cleanupAsync) : IAsyncResponseWaiter<T> where T : IAsyncResponsePayload
{
    /// <inheritdoc/>
    public Task<T> ResponseTask => _responseTask;

    public ValueTask DisposeAsync()
        => _cleanupAsync();
}
