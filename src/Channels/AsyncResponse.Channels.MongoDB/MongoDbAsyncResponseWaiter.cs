namespace AsyncResponse.Channels.MongoDB;

/// <inheritdoc cref="IAsyncResponseWaiter{T}"/>
internal sealed class MongoDbAsyncResponseWaiter<T>(
    Task<T> _responseTask,
    Func<ValueTask> _cleanupAsync) : IAsyncResponseWaiter<T> where T : IAsyncResponsePayload
{
    /// <inheritdoc />
    public Task<T> ResponseTask => _responseTask;

    /// <summary>Releases resources held by this instance.</summary>
    public ValueTask DisposeAsync() => _cleanupAsync();
}
