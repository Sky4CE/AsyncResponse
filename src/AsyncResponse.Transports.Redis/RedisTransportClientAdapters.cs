using StackExchange.Redis;

namespace AsyncResponse.Transports.Redis;

internal interface IRedisStreamDatabase
{
    Task<RedisValue> StreamAddAsync(
        RedisKey stream,
        NameValueEntry[] values,
        long? maxLength,
        bool useApproximateMaxLength,
        CancellationToken cancellationToken);

    Task<bool> StreamCreateConsumerGroupAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue position,
        bool createStream,
        CancellationToken cancellationToken);

    Task<StreamEntry[]> StreamReadGroupAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue consumerName,
        int count,
        CancellationToken cancellationToken);

    Task<long> StreamAcknowledgeAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue messageId,
        CancellationToken cancellationToken);

    Task<StreamPendingMessageInfo[]> StreamPendingMessagesAsync(
        RedisKey stream,
        RedisValue groupName,
        int count,
        RedisValue consumerName,
        RedisValue? minId,
        RedisValue? maxId,
        long minIdleTimeInMilliseconds,
        CancellationToken cancellationToken);

    Task<StreamEntry[]> StreamClaimAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue consumerName,
        long minIdleTimeInMilliseconds,
        RedisValue[] messageIds,
        CancellationToken cancellationToken);
}

internal sealed class RedisStreamDatabaseAdapter(IDatabase _database, TimeSpan _operationTimeout) : IRedisStreamDatabase
{
    public Task<RedisValue> StreamAddAsync(
        RedisKey stream,
        NameValueEntry[] values,
        long? maxLength,
        bool useApproximateMaxLength,
        CancellationToken cancellationToken)
        => WithCancellation(
            _database.StreamAddAsync(
                stream,
                values,
                messageId: null,
                maxLength: maxLength,
                useApproximateMaxLength: useApproximateMaxLength,
                limit: null,
                trimMode: StreamTrimMode.KeepReferences),
            cancellationToken);

    public Task<bool> StreamCreateConsumerGroupAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue position,
        bool createStream,
        CancellationToken cancellationToken)
        => WithCancellation(
            _database.StreamCreateConsumerGroupAsync(stream, groupName, position, createStream),
            cancellationToken);

    public Task<StreamEntry[]> StreamReadGroupAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue consumerName,
        int count,
        CancellationToken cancellationToken)
        => WithCancellation(
            _database.StreamReadGroupAsync(
                stream,
                groupName,
                consumerName,
                position: StreamPosition.NewMessages,
                count: count,
                noAck: false),
            cancellationToken);

    public Task<long> StreamAcknowledgeAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue messageId,
        CancellationToken cancellationToken)
        => WithCancellation(
            _database.StreamAcknowledgeAsync(stream, groupName, messageId),
            cancellationToken);

    public Task<StreamPendingMessageInfo[]> StreamPendingMessagesAsync(
        RedisKey stream,
        RedisValue groupName,
        int count,
        RedisValue consumerName,
        RedisValue? minId,
        RedisValue? maxId,
        long minIdleTimeInMilliseconds,
        CancellationToken cancellationToken)
        => WithCancellation(
            _database.StreamPendingMessagesAsync(
                stream,
                groupName,
                count,
                consumerName,
                minId,
                maxId,
                minIdleTimeInMilliseconds),
            cancellationToken);

    public Task<StreamEntry[]> StreamClaimAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue consumerName,
        long minIdleTimeInMilliseconds,
        RedisValue[] messageIds,
        CancellationToken cancellationToken)
        => WithCancellation(
            _database.StreamClaimAsync(
                stream,
                groupName,
                consumerName,
                minIdleTimeInMilliseconds,
                messageIds),
            cancellationToken);

    private async Task<T> WithCancellation<T>(Task<T> command, CancellationToken cancellationToken)
    {
        using var timeout = new CancellationTokenSource(_operationTimeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        return await command.WaitAsync(linked.Token).ConfigureAwait(false);
    }
}

internal static class RedisTransportRetry
{
    public static async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> action,
        int maxAttempts,
        TimeSpan baseDelay,
        TimeSpan maxDelay,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxAttempts);

        var attempt = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            attempt++;

            try
            {
                return await action(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < maxAttempts && IsTransient(ex))
            {
                var delay = Backoff(attempt, baseDelay, maxDelay);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public static TimeSpan Backoff(int completedAttempts, TimeSpan baseDelay, TimeSpan maxDelay)
    {
        var multiplier = 1 << Math.Min(completedAttempts - 1, 10);
        var milliseconds = Math.Min(maxDelay.TotalMilliseconds, baseDelay.TotalMilliseconds * multiplier);
        return TimeSpan.FromMilliseconds(Math.Max(1, milliseconds));
    }

    public static bool IsTransient(Exception exception)
        => exception is RedisConnectionException
            or RedisTimeoutException
            or TimeoutException
            or OperationCanceledException;
}
