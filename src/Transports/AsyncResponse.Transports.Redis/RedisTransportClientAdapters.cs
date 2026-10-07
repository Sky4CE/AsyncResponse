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
    /// it — appended the same worker job twice.
    /// <para>
    /// <paramref name="maxLength"/> is a capacity, never an eviction: a stream already holding
    /// that many entries first loses only the ones <paramref name="settlingGroup"/> has settled
    /// (delivered and no longer pending), and when it is still full the append is REFUSED with a
    /// server error starting with <see cref="RedisStreamDatabaseAdapter.StreamFullErrorCode"/>
    /// (NATS Discard=New parity) — length-based trimming deleted jobs nobody had run. Carries a
    /// non-idempotent, unbounded pass-through default so out-of-package fakes keep compiling.
    /// </para>
    /// </summary>
    Task<RedisValue> StreamAddOnceAsync(
        RedisKey stream,
        RedisKey dedupKey,
        TimeSpan dedupTtl,
        NameValueEntry[] values,
        long? maxLength,
        RedisValue settlingGroup,
        CancellationToken cancellationToken)
        => StreamAddAsync(stream, values, maxLength: null, useApproximateMaxLength: false, cancellationToken);

    /// <summary>
    /// Settles a worker entry for good: XACK and XDEL in one server-side step, so a settled job
    /// leaves the stream at once and the worker stream holds only unsettled work (NATS
    /// work-queue retention parity) — <see cref="StreamAddOnceAsync"/>'s capacity counts entries,
    /// and an ACKed entry left behind counted against it until a trim reached it. Returns the
    /// XACK count. Carries an ACK-only default so out-of-package fakes keep compiling.
    /// </summary>
    Task<long> StreamAcknowledgeAndDeleteAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue messageId,
        CancellationToken cancellationToken)
        => StreamAcknowledgeAsync(stream, groupName, messageId, cancellationToken);

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
    /// <summary>
    /// Error code (the first word of the server error) the append script refuses a full worker
    /// stream with. <see cref="RedisTransportRetry.IsTransient"/> retries it within the publish
    /// budget — a worker settling one entry frees room — and then the publish fails with it.
    /// </summary>
    internal const string StreamFullErrorCode = "ASYNCRESPONSE_STREAM_FULL";

    // Redis MULTI/EXEC does not roll back a successful SET when XADD fails. Record the
    // success marker only AFTER XADD succeeds, in the same server-side operation.
    //
    // ARGV: [1] marker TTL (ms), [2] capacity or '' (uncapped), [3] the settling group's
    // last-delivered id or '' (not read yet: nothing is trimmed), [4] that group's name,
    // [5..] the entry's field/value pairs.
    //
    // The capacity is enforced by REFUSAL, never by length trimming: `XADD … MAXLEN ~ N` trimmed
    // by length alone, so past the cap it deleted jobs no consumer had read and jobs pending in a
    // handler, with no dead-letter copy — a durable flow's wake-up included. Only a full stream
    // pays anything: it first drops the entries the worker group has SETTLED — every id below
    // both its last-delivered id (passed in, see StreamAddOnceAsync) and its oldest pending id
    // (read here, atomically with the trim) — with `XTRIM … MINID` (Redis 6.2+), and refuses the
    // append when that freed nothing. Settlement deletes its entry (StreamAcknowledgeAndDelete),
    // so the trim only reaches entries settled without one (ACKed by an older version, or by an
    // operator). Ids are compared as decimal strings: sequence numbers exceed a Lua double.
    internal const string AppendOnceScript = $$"""
        local previous = redis.call('GET', KEYS[2])
        if previous then
            if previous == '' then
                return redis.error_reply('Invalid worker publish success marker')
            end
            return false
        end
        if ARGV[2] ~= '' then
            local capacity = tonumber(ARGV[2])
            local length = redis.call('XLEN', KEYS[1])
            if length >= capacity and ARGV[3] ~= '' then
                local settledBelow = ARGV[3]
                local pending = redis.call('XPENDING', KEYS[1], ARGV[4])
                if pending[1] > 0 then
                    local pm, ps = string.match(pending[2], '^(%d+)-(%d+)$')
                    local sm, ss = string.match(settledBelow, '^(%d+)-(%d+)$')
                    local earlier
                    if #pm ~= #sm then earlier = #pm < #sm
                    elseif pm ~= sm then earlier = pm < sm
                    elseif #ps ~= #ss then earlier = #ps < #ss
                    else earlier = ps < ss end
                    if earlier then settledBelow = pending[2] end
                end
                redis.call('XTRIM', KEYS[1], 'MINID', settledBelow)
                length = redis.call('XLEN', KEYS[1])
            end
            if length >= capacity then
                return redis.error_reply('{{StreamFullErrorCode}} the worker stream holds ' .. length ..
                    ' entries its consumer group has not settled (capacity ' .. capacity ..
                    '); the publish was refused rather than evicting unprocessed jobs')
            end
        end
        local command = {KEYS[1], '*'}
        for i = 5, #ARGV do table.insert(command, ARGV[i]) end
        local id = redis.call('XADD', unpack(command))
        redis.call('SET', KEYS[2], id, 'PX', ARGV[1])
        return id
        """;

    // XACK and XDEL as one step: an XDEL that could fail after its XACK would leave a settled
    // entry counting against the capacity, and an XDEL before the XACK would leave a pending id
    // with no entry (a nil XCLAIM tombstone). Unconditional XDEL: a 0 from XACK means the entry
    // was already settled (an early-ACK re-ACK, a reclaim's duplicate), so deleting it is safe.
    internal const string AcknowledgeAndDeleteScript = """
        local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
        redis.call('XDEL', KEYS[1], ARGV[2])
        return acked
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
        // StackExchange.Redis version would otherwise fold KEEPREF into the wire form. This path
        // writes only the dead-letter stream, which nothing consumes: MAXLEN makes it a bounded
        // evict-oldest archive. Worker publishes never go through it — MAXLEN trims by length
        // alone, whatever the consumer group has read, so it deleted unread and pending jobs; the
        // append script refuses a full worker stream instead.
        => WithCancellation(
            static (database, s) => database.StreamAddAsync(
                s.stream,
                s.values,
                messageId: (RedisValue?)null,
                maxLength: ToInt32MaxLength(s.maxLength),
                useApproximateMaxLength: s.useApproximateMaxLength,
                flags: CommandFlags.None),
            (stream, values, maxLength, useApproximateMaxLength),
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
        RedisValue settlingGroup,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(dedupTtl, TimeSpan.Zero);
        var args = new RedisValue[4 + values.Length * 2];
        args[0] = checked((long)Math.Ceiling(dedupTtl.TotalMilliseconds));
        args[1] = maxLength is { } capacity ? capacity : RedisValue.EmptyString;
        args[2] = RedisValue.EmptyString;
        args[3] = settlingGroup.IsNullOrEmpty ? RedisValue.EmptyString : settlingGroup;
        for (var i = 0; i < values.Length; i++)
        {
            args[4 + i * 2] = values[i].Name;
            args[5 + i * 2] = values[i].Value;
        }

        try
        {
            return await AppendOnceAsync(stream, dedupKey, args, cancellationToken).ConfigureAwait(false);
        }
        catch (RedisServerException full) when (IsStreamFull(full) && !settlingGroup.IsNullOrEmpty)
        {
            // Full: find what the worker group has settled and let the script drop exactly that
            // before it decides again. The group's last-delivered id comes from XINFO GROUPS, read
            // here and not in the script (Dragonfly wedges the key on XINFO inside a script); the
            // script still reads the oldest pending id atomically with its trim. Safe across the
            // two steps: an id below the last-delivered id and not pending is ACKed, and stays
            // ACKed — a later read or claim never makes an id at or below it pending again. No
            // group, no stream, or a group that has delivered nothing: nothing is settled, so the
            // stream stays full and the refusal stands.
            var lastDelivered = await ReadLastDeliveredIdAsync(stream, settlingGroup, cancellationToken).ConfigureAwait(false);
            if (lastDelivered.IsNullOrEmpty)
                throw;

            args[2] = lastDelivered;
            return await AppendOnceAsync(stream, dedupKey, args, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<RedisValue> AppendOnceAsync(RedisKey stream, RedisKey dedupKey, RedisValue[] args, CancellationToken cancellationToken)
        => (RedisValue)await WithCancellation(
            static (database, s) => database.ScriptEvaluateAsync(AppendOnceScript, [s.stream, s.dedupKey], s.args),
            (stream, dedupKey, args),
            cancellationToken).ConfigureAwait(false);

    private async Task<RedisValue> ReadLastDeliveredIdAsync(RedisKey stream, RedisValue groupName, CancellationToken cancellationToken)
    {
        StreamGroupInfo[] groups;
        try
        {
            groups = await WithCancellation(
                static (database, s) => database.StreamGroupInfoAsync(s),
                stream,
                cancellationToken).ConfigureAwait(false);
        }
        catch (RedisServerException)
        {
            return RedisValue.Null; // no such key: the full stream vanished meanwhile — retry decides
        }

        foreach (var group in groups)
        {
            if (group.Name == groupName.ToString())
                return group.LastDeliveredId is { } id && id != "0-0" ? id : RedisValue.Null;
        }

        return RedisValue.Null;
    }

    /// <summary>Whether <paramref name="exception"/> is the append script's full-stream refusal.</summary>
    internal static bool IsStreamFull(RedisServerException exception)
        => exception.Message.StartsWith(StreamFullErrorCode, StringComparison.Ordinal);

    /// <summary>Runs the StreamAcknowledgeAndDeleteAsync operation.</summary>
    public async Task<long> StreamAcknowledgeAndDeleteAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue messageId,
        CancellationToken cancellationToken)
        => (long)await WithCancellation(
            static (database, s) => database.ScriptEvaluateAsync(AcknowledgeAndDeleteScript, [s.stream], [s.groupName, s.messageId]),
            (stream, groupName, messageId),
            cancellationToken).ConfigureAwait(false);

    /// <summary>Runs the TryDeleteIdleConsumerAsync operation.</summary>
    public async Task<bool> TryDeleteIdleConsumerAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue consumerName,
        CancellationToken cancellationToken)
    {
        var result = await WithCancellation(
            static (database, s) => database.ScriptEvaluateAsync(DeleteIdleConsumerScript, [s.stream], [s.groupName, s.consumerName]),
            (stream, groupName, consumerName),
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
            static (database, s) => database.StreamCreateConsumerGroupAsync(s.stream, s.groupName, s.position, s.createStream),
            (stream, groupName, position, createStream),
            cancellationToken);

    /// <summary>Runs the StreamReadGroupAsync operation.</summary>
    public Task<StreamEntry[]> StreamReadGroupAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue consumerName,
        int count,
        CancellationToken cancellationToken)
        => WithCancellation(
            static (database, s) => database.StreamReadGroupAsync(
                s.stream,
                s.groupName,
                s.consumerName,
                position: StreamPosition.NewMessages,
                count: s.count,
                noAck: false),
            (stream, groupName, consumerName, count),
            cancellationToken);

    /// <summary>Runs the StreamAcknowledgeAsync operation.</summary>
    public Task<long> StreamAcknowledgeAsync(
        RedisKey stream,
        RedisValue groupName,
        RedisValue messageId,
        CancellationToken cancellationToken)
        => WithCancellation(
            static (database, s) => database.StreamAcknowledgeAsync(s.stream, s.groupName, s.messageId),
            (stream, groupName, messageId),
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
            static (database, s) => database.StreamPendingMessagesAsync(
                s.stream,
                s.groupName,
                s.count,
                s.consumerName,
                s.minId,
                s.maxId,
                s.minIdleTimeInMilliseconds),
            (stream, groupName, count, consumerName, minId, maxId, minIdleTimeInMilliseconds),
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
            static (database, s) => database.StreamClaimAsync(
                s.stream,
                s.groupName,
                s.consumerName,
                s.minIdleTimeInMilliseconds,
                s.messageIds),
            (stream, groupName, consumerName, minIdleTimeInMilliseconds, messageIds),
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
            static (database, s) => database.StreamClaimIdsOnlyAsync(
                s.stream,
                s.groupName,
                s.consumerName,
                s.minIdleTimeInMilliseconds,
                s.messageIds),
            (stream, groupName, consumerName, minIdleTimeInMilliseconds, messageIds),
            cancellationToken);

    /// <summary>
    /// Starts the command only once the caller's token has been checked, then bounds it. The
    /// command is built INSIDE, not by the caller: StackExchange.Redis queues a command the moment
    /// it is called, so a caller already cancelled — a stop landing between two loop steps — still
    /// sent it, and an XREADGROUP or XCLAIM moved entries into the stopping consumer's pending list
    /// while its reply was discarded. Settlements pass <see cref="CancellationToken.None"/> and are
    /// unaffected. <paramref name="start"/> is static and takes its arguments as
    /// <paramref name="state"/>, so a call allocates no closure.
    /// </summary>
    private async Task<T> WithCancellation<TState, T>(
        Func<IDatabase, TState, Task<T>> start,
        TState state,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var command = start(_database, state);

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
    /// The append script's full-stream refusal is retried too (NATS parity: JetStream answers a
    /// full Discard=New work queue with a 503, which its publish retries): it is raised before
    /// anything is appended, and a worker settling one entry is all it takes to clear it. Any
    /// other server error (WRONGTYPE, NOSCRIPT, OOM …) stays permanent.
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
            }
            || exception is RedisServerException server && RedisStreamDatabaseAdapter.IsStreamFull(server);
}
