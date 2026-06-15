using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace AsyncResponse.Redis;

/// <inheritdoc cref="IAsyncResponseWaiter{T}"/>
internal sealed class RedisAsyncResponseWaiter<T> : IAsyncResponseWaiter<T> where T : IAsyncResponsePayload
{
    private const string SERVICE_NAME = nameof(RedisAsyncResponseWaiter<T>);

    private readonly ISubscriber _subscriber;
    private readonly RedisChannel _channel;
    private readonly string _recoveryKey;
    private readonly Action<RedisChannel, RedisValue> _redisHandler;
    private readonly IDatabase _database;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts;
    private readonly Func<string, ValueTask> _executorCleanupAsync;
    // Guard to ensure unsubscribe + recovery-state delete only runs once.
    private int _unsubscribed;

    /// <inheritdoc/>
    public Task<T> ResponseTask { get; }

    public RedisAsyncResponseWaiter(
        ISubscriber subscriber,
        RedisChannel channel,
        string recoveryKey,
        Action<RedisChannel, RedisValue> redisHandler,
        Task<T> responseTask,
        IDatabase database,
        ILogger logger,
        CancellationTokenSource cts,
        Func<string, ValueTask> executorCleanupAsync)
    {
        _subscriber = subscriber;
        _channel = channel;
        _recoveryKey = recoveryKey;
        _redisHandler = redisHandler;
        ResponseTask = responseTask;
        _database = database;
        _logger = logger;
        _cts = cts;
        _executorCleanupAsync = executorCleanupAsync;
    }

    /// <summary>
    /// Unsubscribes from the Redis channel and deletes the persisted recovery state.
    /// Safe to call multiple times; actual cleanup runs only once.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _unsubscribed, 1) != 0)
            return;

        try
        {
            _subscriber.Unsubscribe(_channel, _redisHandler);
            var recoveryKeyRemoved = _database.KeyDelete(_recoveryKey);
            if (!recoveryKeyRemoved)
                _logger.LogWarning("{ServiceName}: Failed to delete recovery state {RecoveryKey} for channel {Channel}.",
                    SERVICE_NAME, _recoveryKey, _channel.ToString());

            _ = _executorCleanupAsync(_channel.ToString()!)
                .AsTask()
                .ContinueWith(t =>
                {
                    if (t.IsFaulted)
                        _logger.LogError(t.Exception, "{ServiceName}: Error cleaning up executor for {Channel}.", SERVICE_NAME, _channel.ToString());
                });

            _logger.LogDebug("{ServiceName}: Unsubscribed from channel {Channel} in Dispose.", SERVICE_NAME, _channel.ToString());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{ServiceName}: Error while unsubscribing from channel {Channel} in Dispose.", SERVICE_NAME, _channel.ToString());
        }
        finally
        {
            // Dispose the CTS here so timeout timers never leak.
            _cts.Dispose();
        }
    }

    /// <summary>
    /// Asynchronously unsubscribes from the Redis channel and deletes the persisted recovery state.
    /// Safe to call multiple times; actual cleanup runs only once.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _unsubscribed, 1) != 0)
            return;

        try
        {
            await _subscriber.UnsubscribeAsync(_channel, _redisHandler).ConfigureAwait(false);
            var recoveryKeyRemoved = await _database.KeyDeleteAsync(_recoveryKey).ConfigureAwait(false);
            if (!recoveryKeyRemoved)
                _logger.LogWarning("{ServiceName}: Failed to delete recovery state {RecoveryKey} for channel {Channel}.",
                    SERVICE_NAME, _recoveryKey, _channel.ToString());

            await _executorCleanupAsync(_channel.ToString()!).ConfigureAwait(false);
            _logger.LogDebug("{ServiceName}: Unsubscribed from channel {Channel} in DisposeAsync.", SERVICE_NAME, _channel.ToString());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{ServiceName}: Error while unsubscribing from channel {Channel} in DisposeAsync.", SERVICE_NAME, _channel.ToString());
        }
        finally
        {
            // Dispose the CTS here so timeout timers never leak.
            _cts.Dispose();
        }
    }
}
