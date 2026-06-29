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
    /// <summary>Runs the StreamAddAsync operation.</summary>
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
                // KEEPREF (Redis 8+): trim by MAXLEN without rewriting consumer-group PELs. An entry that
                // is trimmed while still pending becomes a tombstone on claim; the subscriber dead-letters
                // those via DiscardUnprocessableAsync rather than wedging. Requires a Redis 8+ server.
                trimMode: StreamTrimMode.KeepReferences),
            cancellationToken);

    /// <summary>Runs the StreamCreateConsumerGroupAsync operation.</summary>
    public Task<bool> StreamCreateConsumerGroupAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue position,
        bool createStream,
        CancellationToken cancellationToken)
        => WithCancellation(
            _database.StreamCreateConsumerGroupAsync(stream, groupName, position, createStream),
            cancellationToken);

    /// <summary>Runs the StreamReadGroupAsync operation.</summary>
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

    /// <summary>Runs the StreamAcknowledgeAsync operation.</summary>
    public Task<long> StreamAcknowledgeAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue messageId,
        CancellationToken cancellationToken)
        => WithCancellation(
            _database.StreamAcknowledgeAsync(stream, groupName, messageId),
            cancellationToken);

    /// <summary>Runs the StreamPendingMessagesAsync operation.</summary>
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

    /// <summary>Runs the StreamClaimAsync operation.</summary>
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
        // StackExchange.Redis enforces its own sync/async command timeouts; this adds an upper bound that
        // also honors the caller's token (e.g. host shutdown). On timeout the in-flight command is
        // abandoned best-effort — the multiplexer keeps running it — and surfaced as a TimeoutException so
        // the retry paths treat it as transient, while a genuine caller cancellation stays an
        // OperationCanceledException and is not retried.
        using var timeout = new CancellationTokenSource(_operationTimeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            return await command.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"The Redis command did not complete within {_operationTimeout}.");
        }
    }
}

internal static class RedisTransportRetry
{
    /// <summary>Runs this background operation until cancellation is requested.</summary>
    public static Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> action,
        int maxAttempts,
        TimeSpan baseDelay,
        TimeSpan maxDelay,
        CancellationToken cancellationToken)
        => AsyncResponseRetry.ExecuteAsync(action, IsTransient, maxAttempts, baseDelay, maxDelay, cancellationToken);

    /// <summary>Runs the IsTransient operation.</summary>
    public static bool IsTransient(Exception exception)
        => exception is RedisConnectionException
            or RedisTimeoutException
            or TimeoutException;
}
