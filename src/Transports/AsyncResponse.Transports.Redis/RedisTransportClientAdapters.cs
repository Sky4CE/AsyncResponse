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

    /// <summary>
    /// XCLAIM JUSTID: transfers ownership of <paramref name="messageIds"/> to
    /// <paramref name="consumerName"/> and resets their idle time WITHOUT bumping the PEL
    /// delivery count, returning the ids actually claimed (already-ACKed ids are absent). Used
    /// as the in-flight batch heartbeat. Carries a claim-nothing default implementation so
    /// out-of-package fakes that never run the subscriber read loop keep compiling.
    /// </summary>
    Task<RedisValue[]> StreamClaimIdsOnlyAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue consumerName,
        long minIdleTimeInMilliseconds,
        RedisValue[] messageIds,
        CancellationToken cancellationToken)
        => Task.FromResult(Array.Empty<RedisValue>());

    /// <summary>
    /// Idempotent XADD: appends <paramref name="values"/> only when <paramref name="dedupKey"/>
    /// is not yet claimed (the marker and the append commit atomically, marker expiring after
    /// <paramref name="dedupTtl"/>), returning <see cref="RedisValue.Null"/> when a previous
    /// attempt's append already committed. Publish retries ride on this: XADD has no natural
    /// identity (the entry id is server-generated), so a retry after an ambiguous timeout — the
    /// adapter abandons the in-flight command best-effort while the multiplexer keeps running
    /// it — appended the same worker job twice. Carries a non-idempotent pass-through default so
    /// out-of-package fakes keep compiling.
    /// </summary>
    Task<RedisValue> StreamAddOnceAsync(
        RedisKey stream,
        RedisKey dedupKey,
        TimeSpan dedupTtl,
        NameValueEntry[] values,
        long? maxLength,
        bool useApproximateMaxLength,
        CancellationToken cancellationToken)
        => StreamAddAsync(stream, values, maxLength, useApproximateMaxLength, cancellationToken);
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
        // Call the classic overload (int? maxLength, no trim-mode parameter) so publishing emits plain
        // `XADD … MAXLEN ~ N` with no Redis 8 KEEPREF/DELREF/ACKED token. That keeps the transport
        // portable across Redis 8+, Valkey, and Dragonfly by construction — independent of whether the
        // StackExchange.Redis version would otherwise fold KEEPREF into the wire form. On Redis 8+ the
        // server default is KEEPREF, so an entry trimmed while still pending becomes a tombstone on claim
        // and the subscriber dead-letters it via DiscardUnprocessableAsync rather than wedging; the older
        // trim behavior on pre-8 servers is equivalent for that path.
        => WithCancellation(
            _database.StreamAddAsync(
                stream,
                values,
                messageId: (RedisValue?)null,
                maxLength: ToInt32MaxLength(maxLength),
                useApproximateMaxLength: useApproximateMaxLength,
                flags: CommandFlags.None),
            cancellationToken);

    // Redis caps a stream's MAXLEN well below int.MaxValue in practice; clamp so the classic overload
    // (int? maxLength) is always reachable without overflow.
    private static int? ToInt32MaxLength(long? maxLength)
        => maxLength is null ? null : (int?)Math.Min(maxLength.Value, int.MaxValue);

    /// <summary>Runs the StreamAddOnceAsync operation.</summary>
    public async Task<RedisValue> StreamAddOnceAsync(
        RedisKey stream,
        RedisKey dedupKey,
        TimeSpan dedupTtl,
        NameValueEntry[] values,
        long? maxLength,
        bool useApproximateMaxLength,
        CancellationToken cancellationToken)
    {
        // MULTI/EXEC guarded by KeyNotExists: the dedup marker and the append commit atomically,
        // so a retried publish whose earlier attempt DID land (an ambiguous timeout) finds the
        // marker and appends nothing. The XADD wire shape matches StreamAddAsync above (classic
        // overload, no Redis 8 trim tokens).
        var transaction = _database.CreateTransaction();
        transaction.AddCondition(Condition.KeyNotExists(dedupKey));
        _ = transaction.StringSetAsync(dedupKey, RedisValue.EmptyString, dedupTtl, flags: CommandFlags.FireAndForget);
        var add = transaction.StreamAddAsync(
            stream,
            values,
            messageId: (RedisValue?)null,
            maxLength: ToInt32MaxLength(maxLength),
            useApproximateMaxLength: useApproximateMaxLength,
            flags: CommandFlags.None);

        var committed = await WithCancellation(transaction.ExecuteAsync(), cancellationToken).ConfigureAwait(false);

        // On a failed condition the queued tasks complete as canceled — do not await them.
        return committed ? await add.ConfigureAwait(false) : RedisValue.Null;
    }

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

    /// <summary>Runs the StreamClaimIdsOnlyAsync operation.</summary>
    public Task<RedisValue[]> StreamClaimIdsOnlyAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue consumerName,
        long minIdleTimeInMilliseconds,
        RedisValue[] messageIds,
        CancellationToken cancellationToken)
        => WithCancellation(
            _database.StreamClaimIdsOnlyAsync(
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
