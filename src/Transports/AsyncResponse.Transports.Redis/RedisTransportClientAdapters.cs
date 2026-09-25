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

    /// <summary>
    /// Deletes <paramref name="consumerName"/> from the group only while it has no pending
    /// entries, atomically (XGROUP DELCONSUMER discards the consumer's pending entries, so a
    /// delete that raced a read or claim into that name would lose them). Returns whether it was
    /// deleted. Carries a delete-nothing default implementation so out-of-package fakes keep
    /// compiling.
    /// </summary>
    Task<bool> TryDeleteIdleConsumerAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue consumerName,
        CancellationToken cancellationToken)
        => Task.FromResult(false);
}

internal sealed class RedisStreamDatabaseAdapter(IDatabase _database, TimeSpan _operationTimeout) : IRedisStreamDatabase
{
    // Redis MULTI/EXEC does not roll back a successful SET when XADD fails. Record the
    // success marker only AFTER XADD succeeds, in the same server-side operation.
    internal const string AppendOnceScript = """
        local previous = redis.call('GET', KEYS[2])
        if previous then
            if previous == '' then
                return redis.error_reply('Invalid worker publish success marker')
            end
            return false
        end
        local command = {KEYS[1]}
        if ARGV[2] ~= '' then
            table.insert(command, 'MAXLEN')
            table.insert(command, ARGV[3])
            table.insert(command, ARGV[2])
        end
        table.insert(command, '*')
        for i = 4, #ARGV do table.insert(command, ARGV[i]) end
        local id = redis.call('XADD', unpack(command))
        redis.call('SET', KEYS[2], id, 'PX', ARGV[1])
        return id
        """;

    // The pending check and the delete in one server-side step: a consumer that still owns
    // pending entries is never deleted (that would drop them), and nothing can read or claim into
    // the name between the two. XPENDING's range-and-consumer form runs on Redis 5+.
    internal const string DeleteIdleConsumerScript = """
        if #redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2]) == 0 then
            return redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], ARGV[2])
        end
        return -1
        """;

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
        // StackExchange.Redis version would otherwise fold KEEPREF into the wire form. (This path
        // writes the dead-letter stream; worker publishes go through the append script below, with
        // the same MAXLEN semantics.) MAXLEN trims by length alone, whatever the consumer group has
        // read: an entry trimmed before any consumer read it vanishes without a trace, and one trimmed
        // while still pending is lost too — Redis 6.2 answers its XCLAIM with a nil tombstone, which
        // the claim loop ACKs by its pending id with a Warning, and 7+ drops it from the pending list
        // silently. Neither path dead-letters it: the cap must sit above the deepest backlog the
        // stream can build up (see RedisAsyncResponseTransportOptions.StreamMaxLength).
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
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(dedupTtl, TimeSpan.Zero);
        var args = new RedisValue[3 + values.Length * 2];
        args[0] = checked((long)Math.Ceiling(dedupTtl.TotalMilliseconds));
        args[1] = ToInt32MaxLength(maxLength) is { } cap ? cap : RedisValue.EmptyString;
        args[2] = useApproximateMaxLength ? "~" : "=";
        for (var i = 0; i < values.Length; i++)
        {
            args[3 + i * 2] = values[i].Name;
            args[4 + i * 2] = values[i].Value;
        }

        return (RedisValue)await WithCancellation(
            _database.ScriptEvaluateAsync(AppendOnceScript, [stream, dedupKey], args),
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Runs the TryDeleteIdleConsumerAsync operation.</summary>
    public async Task<bool> TryDeleteIdleConsumerAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue consumerName,
        CancellationToken cancellationToken)
    {
        var result = await WithCancellation(
            _database.ScriptEvaluateAsync(DeleteIdleConsumerScript, [stream], [groupName, consumerName]),
            cancellationToken).ConfigureAwait(false);
        return (long)result >= 0;
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
        catch (OperationCanceledException)
        {
            // Abandoned, not stopped: a fault the command raises later (the multiplexer disposed
            // under it, a late server error) would otherwise surface only as a
            // TaskScheduler.UnobservedTaskException nobody logs. Observe it here.
            _ = command.ContinueWith(
                static abandoned => _ = abandoned.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);

            if (timeout.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                throw new TimeoutException($"The Redis command did not complete within {_operationTimeout}.");

            throw;
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

    /// <summary>
    /// Whether a failed publish is worth retrying. Besides lost connections and timeouts, that is
    /// the server errors a cluster in transition answers with — TRYAGAIN (the idempotent append is
    /// a two-key script, refused while its slot migrates with only one key moved), CLUSTERDOWN,
    /// LOADING, MASTERDOWN and READONLY (a failover in progress): the server raises them BEFORE
    /// running anything, and the append is keyed by its dedup marker, so a retry cannot apply it
    /// twice. Read as permanent, every publish in a migration or failover window failed at once.
    /// Any other server error (WRONGTYPE, NOSCRIPT, OOM …) stays permanent.
    /// </summary>
    public static bool IsTransient(Exception exception)
        => exception is RedisConnectionException
            or RedisTimeoutException
            or TimeoutException
            or RedisServerException
            {
                Kind: RedisErrorKind.TryAgain
                    or RedisErrorKind.ClusterDown
                    or RedisErrorKind.Loading
                    or RedisErrorKind.MasterDown
                    or RedisErrorKind.ReadOnly
            };
}
