using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace AsyncResponse.Transports.Redis;

internal abstract class RedisSubscriberService : BackgroundService
{
    /// <summary>
    /// Whether the payload is within the engine's inbound size budget. Overridden by the response
    /// ingress subscriber, which is the only role that parses the BODY to find a correlation id —
    /// the worker role reads a header and never touches payload size. Default true so a role
    /// without a budget behaves exactly as before.
    /// </summary>
    protected virtual bool IsWithinInboundBudget(string payload) => true;

    private static readonly string GeneratedConsumerName = CreateGeneratedConsumerName();

    private readonly IRedisStreamDatabase _database;

    /// <summary>Runs the RedisSubscriberService operation.</summary>
    protected RedisSubscriberService(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IConnectionMultiplexer multiplexer,
        ILogger logger)
        : this(
            options,
            new RedisStreamDatabaseAdapter(multiplexer.GetDatabase(), options.Value.OperationTimeout),
            logger)
    {
    }

    /// <summary>Runs the RedisSubscriberService operation.</summary>
    protected RedisSubscriberService(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IRedisStreamDatabase database,
        ILogger logger)
    {
        Options = options.Value;
        RedisTransportOptionsValidator.ValidateCommon(Options);
        _database = database;
        Logger = logger;
    }

    protected RedisAsyncResponseTransportOptions Options { get; }
    protected ILogger Logger { get; }

    protected abstract RedisKey Stream { get; }
    protected abstract RedisValue ConsumerGroup { get; }
    protected abstract RedisSubscriberOptions SubscriberOptions { get; }
    protected abstract RedisSubscriberRole SubscriberRole { get; }
    /// <summary>Handles the delivered message.</summary>
    protected abstract Task HandleMessageAsync(RedisStreamDelivery delivery, CancellationToken cancellationToken);

    /// <summary>Runs this background operation until cancellation is requested.</summary>
    /// <summary>
    /// Validates subscriber options here rather than at the top of <c>ExecuteAsync</c>: since
    /// Microsoft.Extensions.Hosting.Abstractions 10.0.10, <c>BackgroundService.StartAsync</c> no
    /// longer runs <c>ExecuteAsync</c> inline, so a throw there surfaces only through the host's
    /// background-exception handling — or never, when a fast stop discards the queued work —
    /// instead of failing host startup synchronously.
    /// </summary>
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        RedisMessageDispatcher.ValidateOptions(Options, SubscriberOptions, SubscriberRole);
        return base.StartAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => SubscriberSupervisor.RunAsync(
            RunSubscriberAsync,
            stoppingToken,
            failures => AsyncResponseRetry.Backoff(
                failures,
                Options.SubscriberRetryBaseDelay,
                Options.SubscriberRetryMaxDelay),
            (ex, retryDelay) => Logger.LogWarning(
                ex,
                "Redis subscriber failed for stream {Stream} ({Role}); retrying in {RetryDelay}.",
                Stream.ToString(),
                SubscriberRole,
                retryDelay));

    private async Task RunSubscriberAsync(CancellationToken stoppingToken)
    {
        if (Options.CreateConsumerGroups)
            await EnsureConsumerGroupAsync(stoppingToken).ConfigureAwait(false);

        var consumerName = ResolveConsumerName(Options, SubscriberRole);
        await using var dispatcher = RedisMessageDispatcher.Create(
            HandleMessageAsync,
            _database,
            Options,
            SubscriberOptions,
            Logger,
            Stream,
            ConsumerGroup,
            SubscriberRole);

        Logger.LogInformation(
            "Redis subscriber started. Stream: {Stream}. Group: {ConsumerGroup}. Consumer: {ConsumerName}. Role: {Role}. AckMode: {AckMode}.",
            Stream.ToString(),
            ConsumerGroup.ToString(),
            consumerName.ToString(),
            SubscriberRole,
            SubscriberOptions.AckMode);

        var nextPendingClaimAt = DateTimeOffset.UtcNow;
        while (!stoppingToken.IsCancellationRequested)
        {
            var processed = 0;

            // When the dispatcher is saturated (ACK-after-enqueue queue full) stop pulling new entries:
            // reading them would only move the backlog into the pending-entry list and spin the loop.
            // The unread entries stay as new messages in the stream until capacity frees.
            if (dispatcher.CanAcceptMore)
            {
                var utcNow = DateTimeOffset.UtcNow;
                if (utcNow >= nextPendingClaimAt)
                {
                    processed += await ClaimPendingAsync(dispatcher, consumerName, stoppingToken).ConfigureAwait(false);
                    nextPendingClaimAt = utcNow + SubscriberOptions.PendingClaimInterval;
                }

                // Clamp the read to the dispatcher's FREE slots (ASB/SQS parity), not merely to
                // "is there one": a full BatchSize read into a nearly-full early-ACK queue deferred
                // the surplus into the PEL un-ACKed, every reclaim bumped its delivery count, and
                // the pre-execution cap eventually dead-lettered healthy jobs whose handler never
                // ran. The claim above may have taken the last slot.
                var readCount = Math.Min(SubscriberOptions.BatchSize, dispatcher.FreeCapacity);
                if (readCount > 0)
                {
                    var entries = await _database.StreamReadGroupAsync(
                        Stream,
                        ConsumerGroup,
                        consumerName,
                        readCount,
                        stoppingToken).ConfigureAwait(false);

                    processed += await DispatchBatchAsync(
                        dispatcher,
                        entries,
                        consumerName,
                        static _ => 1,
                        stoppingToken).ConfigureAwait(false);
                }
            }

            // Throttle when nothing advanced — an empty stream, or every entry deferred under backpressure.
            if (processed == 0)
                await Task.Delay(SubscriberOptions.EmptyPollDelay, stoppingToken).ConfigureAwait(false);
        }
    }

    private async Task<RedisDispatchOutcome> DispatchEntryAsync(
        RedisMessageDispatcher dispatcher,
        StreamEntry entry,
        int attempt,
        CancellationToken cancellationToken)
    {
        RedisStreamDelivery delivery;
        try
        {
            delivery = CreateDelivery(entry, attempt);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A foreign/malformed entry (or a trimmed tombstone) can never be handled; dead-letter and
            // ACK it so it drains instead of poisoning the pending-claim loop forever.
            //
            // Deliberately every non-cancellation exception, not just InvalidDataException:
            // CreateDelivery also runs correlation-id extraction, and anything that escapes here
            // leaves the entry in the PEL to be reclaimed and re-thrown every PendingClaimInterval,
            // abandoning the rest of the claimed batch behind it — MaxDeliveryAttempts cannot help,
            // because it is keyed on a delivery this path never constructed.
            await dispatcher.DiscardUnprocessableAsync(Stream, ConsumerGroup, entry, ex, cancellationToken).ConfigureAwait(false);
            return RedisDispatchOutcome.Processed;
        }

        return await dispatcher.HandleAsync(delivery, cancellationToken).ConfigureAwait(false);
    }

    private async Task EnsureConsumerGroupAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _database.StreamCreateConsumerGroupAsync(
                Stream,
                ConsumerGroup,
                StreamPosition.Beginning,
                createStream: true,
                cancellationToken).ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.OrdinalIgnoreCase))
        {
            // The group already exists; this is the expected path after the first app instance.
        }
    }

    private async Task<int> ClaimPendingAsync(
        RedisMessageDispatcher dispatcher,
        RedisValue consumerName,
        CancellationToken cancellationToken)
    {
        // Same clamp as the read: reclaiming more than the dispatcher can take defers the rest
        // straight back into the PEL with a bumped delivery count.
        var claimCount = Math.Min(SubscriberOptions.PendingClaimBatchSize, dispatcher.FreeCapacity);
        if (claimCount <= 0)
            return 0;

        var minIdleMs = ToPositiveMilliseconds(SubscriberOptions.PendingMessageMinIdleTime);
        var pending = await _database.StreamPendingMessagesAsync(
            Stream,
            ConsumerGroup,
            claimCount,
            RedisValue.Null,
            minId: null,
            maxId: null,
            minIdleMs,
            cancellationToken).ConfigureAwait(false);

        if (pending.Length == 0)
            return 0;

        var pendingById = pending.ToDictionary(
            item => item.MessageId.ToString(),
            StringComparer.Ordinal);
        var claimed = await _database.StreamClaimAsync(
            Stream,
            ConsumerGroup,
            consumerName,
            minIdleMs,
            pending.Select(item => item.MessageId).ToArray(),
            cancellationToken).ConfigureAwait(false);

        // Redis 5/6 answer XCLAIM with a nil entry for an id whose message was trimmed while still
        // pending (7.x drops it from the PEL instead). A nil entry has no id, so neither the
        // dispatch path nor the JUSTID heartbeat can name it, and it stayed in the PEL to be
        // re-claimed every cycle. When the reply is complete its order matches the request, so
        // the tombstones are ACKed by their pending ids here; a partial reply leaves them for the
        // next cycle. Either way only real entries are dispatched.
        if (Array.Exists(claimed, static entry => entry.Id.IsNull))
        {
            if (claimed.Length == pending.Length)
            {
                for (var index = 0; index < claimed.Length; index++)
                {
                    if (!claimed[index].Id.IsNull)
                        continue;

                    var tombstoneId = pending[index].MessageId;
                    try
                    {
                        await _database.StreamAcknowledgeAsync(Stream, ConsumerGroup, tombstoneId, CancellationToken.None).ConfigureAwait(false);
                        Logger.LogWarning(
                            "Redis pending entry {MessageId} on {Stream} was trimmed while still pending; ACKed the tombstone so it drains.",
                            tombstoneId.ToString(),
                            Stream.ToString());
                    }
                    catch (Exception ex)
                    {
                        Logger.LogWarning(
                            ex,
                            "Failed to ACK trimmed pending entry {MessageId} on {Stream}; it is retried on the next pending-claim cycle.",
                            tombstoneId.ToString(),
                            Stream.ToString());
                    }
                }
            }

            claimed = Array.FindAll(claimed, static entry => !entry.Id.IsNull);
        }

        return await DispatchBatchAsync(
            dispatcher,
            claimed,
            consumerName,
            entry =>
            {
                var priorDeliveries = pendingById.TryGetValue(entry.Id.ToString(), out var info)
                    ? info.DeliveryCount
                    : 1;
                return Math.Max(1, priorDeliveries + 1);
            },
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<int> DispatchBatchAsync(
        RedisMessageDispatcher dispatcher,
        StreamEntry[] entries,
        RedisValue consumerName,
        Func<StreamEntry, int> attemptFor,
        CancellationToken stoppingToken)
    {
        if (entries.Length == 0)
            return 0;

        // The batch is dispatched serially, and XREADGROUP/XCLAIM stamped every entry's idle
        // clock at read time — so a slow handler lets the idle time of the later (still
        // unprocessed) entries cross PendingMessageMinIdleTime, where a sibling's pending-claim
        // scan steals and re-runs them concurrently, bumping their PEL delivery count toward the
        // dead-letter cap on work that never once failed. While the batch is in flight, a
        // heartbeat claims the unprocessed entries back to this consumer with XCLAIM JUSTID,
        // which resets idle WITHOUT bumping the delivery count.
        var progress = new BatchProgress();
        using var renewalCancellation = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var renewalTask = RenewClaimLoopAsync(entries, consumerName, progress, renewalCancellation.Token);
        var processed = 0;
        try
        {
            foreach (var entry in entries)
            {
                try
                {
                    if (await DispatchEntryAsync(dispatcher, entry, attemptFor(entry), stoppingToken).ConfigureAwait(false)
                        == RedisDispatchOutcome.Processed)
                    {
                        processed++;
                    }
                }
                finally
                {
                    // Also counts Deferred entries: they were left pending ON PURPOSE, so the
                    // heartbeat must stop touching them and let their idle accrue toward reclaim.
                    progress.MarkSettled();
                }
            }
        }
        finally
        {
            renewalCancellation.Cancel();
            await renewalTask.ConfigureAwait(false);
        }

        return processed;
    }

    private async Task RenewClaimLoopAsync(
        StreamEntry[] entries,
        RedisValue consumerName,
        BatchProgress progress,
        CancellationToken cancellationToken)
    {
        // ~PendingMessageMinIdleTime/3: two chances to land an idle reset inside every reclaim
        // window even when one sweep is delayed by a slow round trip.
        var interval = TimeSpan.FromMilliseconds(Math.Max(1, SubscriberOptions.PendingMessageMinIdleTime.TotalMilliseconds / 3));
        try
        {
            while (true)
            {
                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);

                // Claim from the first unsettled entry onward: that covers the entry currently in
                // the handler plus everything still waiting its turn. minIdle 0 resets the idle
                // clock unconditionally, and the races are harmless — an entry ACKed while this
                // sweep is in flight has left the PEL (the claim simply skips it), and a failed
                // entry is settled before MarkSettled runs, so its post-failure idle countdown is
                // never stretched.
                var settled = progress.SettledCount;
                if (settled >= entries.Length)
                    return;

                var remaining = new RedisValue[entries.Length - settled];
                for (var i = settled; i < entries.Length; i++)
                    remaining[i - settled] = entries[i].Id;

                try
                {
                    await _database.StreamClaimIdsOnlyAsync(
                        Stream,
                        ConsumerGroup,
                        consumerName,
                        minIdleTimeInMilliseconds: 0,
                        remaining,
                        cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    Logger.LogWarning(
                        ex,
                        "Failed to refresh the pending idle time of {EntryCount} Redis entries on {Stream}; a sibling may reclaim them while they are still queued here (at-least-once preserved).",
                        remaining.Length,
                        Stream.ToString());
                }
            }
        }
        catch (OperationCanceledException)
        {
            // The batch finished or the subscriber is stopping.
        }
    }

    private sealed class BatchProgress
    {
        // Settled only ever increments, so a monotonic volatile read is enough — no lock, and a
        // stale read only claims an already-settled entry once more (an ACKed id is simply
        // absent from the PEL, and a failed one gets its reclaim delayed by one sweep).
        private int _settledCount;

        public int SettledCount => Volatile.Read(ref _settledCount);

        public void MarkSettled() => Interlocked.Increment(ref _settledCount);
    }

    private RedisStreamDelivery CreateDelivery(StreamEntry entry, int attempt)
    {
        var payload = RedisCorrelationIdExtractor.TryReadField(entry, Options.PayloadField);
        if (string.IsNullOrWhiteSpace(payload))
        {
            throw new InvalidDataException(
                $"Redis stream entry {entry.Id} on {Stream.ToString()} does not contain payload field '{Options.PayloadField}'.");
        }

        // Body-path extraction parses the whole payload, so it is gated on the inbound budget;
        // the field/header path reads metadata only and is unaffected by payload size.
        var correlationId = SubscriberRole is RedisSubscriberRole.ResponseIngress
            ? IsWithinInboundBudget(payload)
                ? RedisCorrelationIdExtractor.Extract(entry, payload, Options)
                : null
            : RedisCorrelationIdExtractor.TryReadField(entry, Options.CorrelationIdField);

        return new RedisStreamDelivery(
            Stream,
            ConsumerGroup,
            entry.Id,
            payload,
            correlationId,
            attempt,
            entry);
    }

    private static long ToPositiveMilliseconds(TimeSpan value)
        => Math.Max(1, (long)Math.Ceiling(value.TotalMilliseconds));

    private static RedisValue ResolveConsumerName(
        RedisAsyncResponseTransportOptions options,
        RedisSubscriberRole role)
    {
        // Append the role even to an explicitly configured name so the worker and response subscribers
        // never share a consumer identity (and therefore a pending-entry list) within their groups.
        var baseName = !string.IsNullOrWhiteSpace(options.ConsumerName)
            ? options.ConsumerName
            : GeneratedConsumerName;

        return $"{baseName}-{role.ToString().ToLowerInvariant()}";
    }

    private static string CreateGeneratedConsumerName()
    {
        var name = $"{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():N}";
        return TrimConsumerName(name);
    }

    internal static string TrimConsumerName(string name)
        => name.Length <= 64 ? name : name[..64];
}

internal sealed class RedisWorkerSubscriber : RedisSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;
    private readonly RedisTransportKeySchema _keys;

    /// <summary>Runs the RedisWorkerSubscriber operation.</summary>
    public RedisWorkerSubscriber(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IConnectionMultiplexer multiplexer,
        IAsyncResponseIngress ingress,
        ILogger<RedisWorkerSubscriber> logger)
        : base(options, multiplexer, logger)
    {
        _ingress = ingress;
        _keys = new RedisTransportKeySchema(options.Value);
    }

    internal RedisWorkerSubscriber(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IRedisStreamDatabase database,
        IAsyncResponseIngress ingress,
        ILogger<RedisWorkerSubscriber> logger)
        : base(options, database, logger)
    {
        _ingress = ingress;
        _keys = new RedisTransportKeySchema(options.Value);
    }

    protected override RedisKey Stream => _keys.WorkerStream;
    protected override RedisValue ConsumerGroup => Options.WorkerConsumerGroup;
    protected override RedisSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override RedisSubscriberRole SubscriberRole => RedisSubscriberRole.Worker;

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(RedisStreamDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(delivery.Payload);
}

internal sealed class RedisResponseIngressSubscriber : RedisSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;
    private readonly RedisTransportKeySchema _keys;

    /// <summary>Runs the RedisResponseIngressSubscriber operation.</summary>
    public RedisResponseIngressSubscriber(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IConnectionMultiplexer multiplexer,
        IAsyncResponseIngress ingress,
        ILogger<RedisResponseIngressSubscriber> logger)
        : base(options, multiplexer, logger)
    {
        _ingress = ingress;
        _keys = new RedisTransportKeySchema(options.Value);
    }

    internal RedisResponseIngressSubscriber(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IRedisStreamDatabase database,
        IAsyncResponseIngress ingress,
        ILogger<RedisResponseIngressSubscriber> logger)
        : base(options, database, logger)
    {
        _ingress = ingress;
        _keys = new RedisTransportKeySchema(options.Value);
    }

    protected override RedisKey Stream => _keys.ResponseStream;
    protected override RedisValue ConsumerGroup => Options.ResponseConsumerGroup;
    protected override RedisSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
    protected override RedisSubscriberRole SubscriberRole => RedisSubscriberRole.ResponseIngress;

    /// <inheritdoc />
    protected override bool IsWithinInboundBudget(string payload) => !_ingress.IsOverInboundBudget(payload);

    /// <summary>Handles the delivered message.</summary>
    protected override Task HandleMessageAsync(RedisStreamDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleResponseMessageAsync(delivery.Payload, delivery.CorrelationId);
}
