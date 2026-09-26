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

    /// <summary>
    /// The most the stop-time consumer retirement may take — one script round trip, a fraction of
    /// this on a healthy server. It runs alongside the early-ACK drain, so an unreachable Redis
    /// holds a stop at most this long beyond the drain.
    /// </summary>
    private static readonly TimeSpan ConsumerRetirementBudget = TimeSpan.FromSeconds(5);

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

    /// <summary>
    /// Clocks the pending-claim schedule, on its monotonic timestamp: scheduled on the wall clock,
    /// a backward step (VM resume, an NTP correction) suspended every reclaim for the size of the
    /// step, stranding failed entries and a crashed peer's entries that long. The system clock in
    /// production; the seam exists for tests (SQS parity).
    /// </summary>
    internal TimeProvider Clock { get; set; } = TimeProvider.System;

    protected abstract RedisKey Stream { get; }
    protected abstract RedisValue ConsumerGroup { get; }
    protected abstract RedisSubscriberOptions SubscriberOptions { get; }
    protected abstract RedisSubscriberRole SubscriberRole { get; }

    /// <summary>
    /// Whether this subscriber takes no new delivery any more because host stop has begun — the
    /// worker role's <see cref="WorkerIntakeGate"/>. A response subscriber never closes: waiters
    /// keep being served through a stop.
    /// </summary>
    protected virtual bool IntakeClosed => false;

    /// <summary>
    /// The flow engine handed a delivery back (<see cref="RedisMessageDispatcher.HandBackSignalled"/>),
    /// or host stop has begun (<see cref="IntakeClosed"/>): either way the host IS stopping, so
    /// read, claim and start nothing more. The hand-back latch stays the fallback when no host
    /// lifetime is registered.
    /// </summary>
    private bool TakesNothingMore(RedisMessageDispatcher dispatcher) => dispatcher.HandBackSignalled || IntakeClosed;

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

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // The dispatcher — and with it the ACK-after-enqueue queue and its workers — belongs to
        // the hosted service, not to one supervised attempt. Disposing it IS the stop-time drain
        // (wait BackgroundDrainTimeout, then cancel and dead-letter whatever is still queued), so
        // owning it per attempt ran that drain on every read-loop failure of a host that was NOT
        // stopping: an XREADGROUP that outlived OperationTimeout paused consumption for the drain
        // budget and then buried queued, already-ACKed work as "drain budget lapsed" — or lost it
        // outright when the dead-letter XADD rode the same stalled Redis. Nothing in it is per
        // attempt (the stream adapter wraps the host's reconnecting multiplexer), so every rebuilt
        // attempt feeds this one instance and only the host stop drains it (NATS parity).
        var dispatcher = RedisMessageDispatcher.Create(
            HandleMessageAsync,
            _database,
            Options,
            SubscriberOptions,
            Logger,
            Stream,
            ConsumerGroup,
            SubscriberRole);

        try
        {
            await SubscriberSupervisor.RunAsync(
                attemptToken => RunSubscriberAsync(dispatcher, attemptToken),
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
                    retryDelay),
                healthyRunThreshold: AsyncResponseRetry.MaxAttainableDelay(Options.SubscriberRetryBaseDelay, Options.SubscriberRetryMaxDelay)).ConfigureAwait(false);
        }
        finally
        {
            // The loop has stopped for good: retire this process's consumer alongside the drain
            // (early-ACK entries were ACKed at enqueue, so the drain never touches its pending list).
            var retirement = RetireGeneratedConsumerAsync();
            await dispatcher.DisposeAsync().ConfigureAwait(false);
            await retirement.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Every process start generates a fresh consumer name, and nothing ever removed the old ones,
    /// so each group's consumer list grew by one entry per start and deploy for as long as the
    /// stream lived. Once the loop has stopped for good this process deletes its own generated
    /// consumer — only while that consumer has no pending entries, checked atomically on the
    /// server, since deleting a consumer discards its pending list; after the loop only this
    /// process ever claimed into the name, so that list can only have shrunk. A configured
    /// <see cref="RedisAsyncResponseTransportOptions.ConsumerName"/> is stable across restarts and
    /// left alone. Best effort, bounded by <see cref="ConsumerRetirementBudget"/> (and the adapter's
    /// OperationTimeout) so an unreachable Redis cannot stretch the host's stop: a failure leaves the
    /// consumer behind exactly as before, and a crash still does (a periodic idle sweep is not
    /// done). Never throws.
    /// </summary>
    private async Task RetireGeneratedConsumerAsync()
    {
        if (!string.IsNullOrWhiteSpace(Options.ConsumerName))
            return;

        var consumerName = ResolveConsumerName(Options, SubscriberRole);
        try
        {
            using var budget = new CancellationTokenSource(ConsumerRetirementBudget);
            var deleted = await _database.TryDeleteIdleConsumerAsync(Stream, ConsumerGroup, consumerName, budget.Token).ConfigureAwait(false);
            Logger.LogDebug(
                deleted
                    ? "Deleted Redis consumer {ConsumerName} from group {ConsumerGroup} on {Stream} at subscriber stop."
                    : "Kept Redis consumer {ConsumerName} in group {ConsumerGroup} on {Stream} at subscriber stop: it still owns pending entries, which a peer reclaims.",
                consumerName.ToString(),
                ConsumerGroup.ToString(),
                Stream.ToString());
        }
        catch (Exception ex)
        {
            Logger.LogDebug(
                ex,
                "Could not delete Redis consumer {ConsumerName} from group {ConsumerGroup} on {Stream} at subscriber stop; it stays in the group.",
                consumerName.ToString(),
                ConsumerGroup.ToString(),
                Stream.ToString());
        }
    }

    private async Task RunSubscriberAsync(RedisMessageDispatcher dispatcher, CancellationToken stoppingToken)
    {
        if (Options.CreateConsumerGroups)
            await EnsureConsumerGroupAsync(stoppingToken).ConfigureAwait(false);

        var consumerName = ResolveConsumerName(Options, SubscriberRole);

        Logger.LogInformation(
            "Redis subscriber started. Stream: {Stream}. Group: {ConsumerGroup}. Consumer: {ConsumerName}. Role: {Role}. AckMode: {AckMode}.",
            Stream.ToString(),
            ConsumerGroup.ToString(),
            consumerName.ToString(),
            SubscriberRole,
            SubscriberOptions.AckMode);

        long? lastPendingClaim = null;
        while (!stoppingToken.IsCancellationRequested)
        {
            // The flow engine handed a delivery back: the host IS stopping — the engine reacts to
            // ApplicationStopping, which fires before this subscriber's token — so read and claim
            // nothing more; wait for the stop. Ending only the batch kept the loop going through
            // the whole stop window: every flow wake-up read there was handed back too, pending on
            // this stopping consumer with an attempt spent, where no live peer could take it
            // before PendingMessageMinIdleTime — and past that, this host re-claimed its own
            // hand-backs every PendingClaimInterval, each claim another attempt. (Early ACK: every
            // one of them was already ACKed, so each became a dead-letter copy.) The worker's
            // intake gate closes the same way at ApplicationStopping itself, before any hand-back:
            // the wake-ups this host's own hand-overs publish are then left to a live replica
            // instead of being taken here and handed back again.
            if (TakesNothingMore(dispatcher))
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken).ConfigureAwait(false);
                break;
            }

            var processed = 0;

            // When the dispatcher is saturated (ACK-after-enqueue queue full) stop pulling new entries:
            // reading them would only move the backlog into the pending-entry list and spin the loop.
            // The unread entries stay as new messages in the stream until capacity frees.
            if (dispatcher.CanAcceptMore)
            {
                if (lastPendingClaim is not { } last || Clock.GetElapsedTime(last) >= SubscriberOptions.PendingClaimInterval)
                {
                    var claimStarted = Clock.GetTimestamp();
                    processed += await ClaimPendingAsync(dispatcher, consumerName, stoppingToken).ConfigureAwait(false);
                    lastPendingClaim = claimStarted;
                }

                // Clamp the read to the dispatcher's FREE slots (ASB/SQS parity), not merely to
                // "is there one": a full BatchSize read into a nearly-full early-ACK queue deferred
                // the surplus into the PEL un-ACKed, every reclaim bumped its delivery count, and
                // the pre-execution cap eventually dead-lettered healthy jobs whose handler never
                // ran. The claim above may have taken the last slot.
                var readCount = TakesNothingMore(dispatcher) ? 0 : Math.Min(ReadBatchSize, dispatcher.FreeCapacity);
                if (readCount > 0)
                {
                    // A stop that landed during the claim above ends the pass here: the adapter
                    // queues XREADGROUP the moment it is called, so reading with the cancelled
                    // token still moved an entry into this stopping consumer's pending list, where
                    // it waited out PendingMessageMinIdleTime before a peer could take it — and the
                    // peer's claim spent another attempt.
                    if (stoppingToken.IsCancellationRequested)
                        break;

                    var entries = await _database.StreamReadGroupAsync(
                        Stream,
                        ConsumerGroup,
                        consumerName,
                        readCount,
                        stoppingToken).ConfigureAwait(false);

                    processed += (await DispatchBatchAsync(
                        dispatcher,
                        entries,
                        consumerName,
                        static _ => 1,
                        stoppingToken).ConfigureAwait(false)).Processed;
                }
            }

            // Throttle when nothing advanced — an empty stream, or every entry deferred under backpressure.
            if (processed == 0 && !TakesNothingMore(dispatcher))
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

    /// <summary>
    /// Whether settlement waits for the handler. Redis counts a delivery when XREADGROUP or XCLAIM
    /// hands an entry over, not when a handler starts, and an ACK-after-handler batch runs
    /// serially: every entry read or claimed behind a handler that kills the process (stack
    /// overflow, OOM, FailFast) came back with its count bumped without ever having run — after
    /// MaxDeliveryAttempts crashes the pre-execution cap dead-lettered up to BatchSize-1 healthy
    /// batch-mates along with the poison one — and a handler that simply ran long (a flow timer
    /// waiting in process) pinned the rest of its batch here, their idle clocks reset by the
    /// heartbeat so no peer could take them. So this mode reads one entry at a time and claims
    /// each reclaim candidate right before it runs, leaving the rest where nothing is counted and
    /// any peer can take them (NATS parity). ACK-after-enqueue settles each entry as it is
    /// accepted, so it keeps the batch.
    /// </summary>
    private bool SettlesAfterHandler => SubscriberOptions.AckMode is not RedisAckMode.AckAfterEnqueue;

    private int ReadBatchSize => SettlesAfterHandler ? 1 : SubscriberOptions.BatchSize;

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

        if (pending.Length == 0 || cancellationToken.IsCancellationRequested || TakesNothingMore(dispatcher))
            return 0;

        if (!SettlesAfterHandler)
            return (await ClaimAndDispatchAsync(dispatcher, consumerName, pending, minIdleMs, cancellationToken).ConfigureAwait(false)).Processed;

        // XPENDING still lists up to PendingClaimBatchSize candidates, so reclaim throughput does
        // not collapse to one entry per PendingClaimInterval — but each is XCLAIMed only right
        // before it runs, so only the entry about to execute has its count bumped. XCLAIM's
        // min-idle re-check skips a candidate a peer took in the meantime.
        var processed = 0;
        foreach (var candidate in pending)
        {
            // Stopping: claim nothing more — an unclaimed candidate keeps its count and stays
            // claimable by a peer.
            if (cancellationToken.IsCancellationRequested || TakesNothingMore(dispatcher))
                break;

            // The listing is as old as every earlier candidate's handler run — minutes, behind a
            // slow one — and a peer may have claimed, failed and released this entry since, so its
            // delivery count there is stale: the attempt number would come out one low per missed
            // claim, running a poison entry past MaxDeliveryAttempts and dead-lettering it with a
            // wrong attempt. Re-read just this id, which also re-checks that it is still idle
            // enough to claim; gone from the listing means a peer holds it (or it was settled).
            var current = await _database.StreamPendingMessagesAsync(
                Stream,
                ConsumerGroup,
                1,
                RedisValue.Null,
                minId: candidate.MessageId,
                maxId: candidate.MessageId,
                minIdleMs,
                cancellationToken).ConfigureAwait(false);
            if (current.Length == 0)
                continue;

            // The re-read took a round trip: a stop landing in it must not still claim (the
            // adapter would queue the XCLAIM with the cancelled token, bumping the count of an
            // entry that then sits in this stopping consumer's pending list).
            if (cancellationToken.IsCancellationRequested || TakesNothingMore(dispatcher))
                break;

            var result = await ClaimAndDispatchAsync(dispatcher, consumerName, current, minIdleMs, cancellationToken).ConfigureAwait(false);
            processed += result.Processed;

            // The flow engine handed a delivery back: the host is stopping, even if this
            // subscriber's token has not been cancelled yet. Treated like the stop itself.
            if (result.HandedBack)
                break;
        }

        return processed;
    }

    private async Task<BatchResult> ClaimAndDispatchAsync(
        RedisMessageDispatcher dispatcher,
        RedisValue consumerName,
        StreamPendingMessageInfo[] pending,
        long minIdleMs,
        CancellationToken cancellationToken)
    {
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

        // Redis 6.2 answers XCLAIM with a nil entry for an id whose message was trimmed while still
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

    /// <summary>What a dispatched batch came to: the entries that counted as progress, and whether the flow engine handed one back because the host is stopping.</summary>
    private readonly record struct BatchResult(int Processed, bool HandedBack);

    private async Task<BatchResult> DispatchBatchAsync(
        RedisMessageDispatcher dispatcher,
        StreamEntry[] entries,
        RedisValue consumerName,
        Func<StreamEntry, int> attemptFor,
        CancellationToken stoppingToken)
    {
        if (entries.Length == 0)
            return default;

        // The batch is dispatched serially, and XREADGROUP/XCLAIM stamped every entry's idle
        // clock at read time — so a slow handler lets the idle time of the later (still
        // unprocessed) entries cross PendingMessageMinIdleTime, where a sibling's pending-claim
        // scan steals and re-runs them concurrently, bumping their PEL delivery count toward the
        // dead-letter cap on work that never once failed. While the batch is in flight, a
        // heartbeat claims the unprocessed entries back to this consumer with XCLAIM JUSTID,
        // which resets idle WITHOUT bumping the delivery count. The heartbeat is NOT tied to the
        // stop token: a handler takes no token, so it outlives the stop signal, and cancelling its
        // idle reset there let PendingMessageMinIdleTime lapse under the live handler on every
        // rolling deploy — a peer's pending claim took the entry and ran it a second time while it
        // was still executing here. It ends only when this loop has let go of the batch.
        var progress = new BatchProgress();
        using var renewalCancellation = new CancellationTokenSource();
        var renewalTask = RenewClaimLoopAsync(entries, consumerName, progress, stoppingToken, renewalCancellation.Token);
        var processed = 0;
        var handedBack = false;
        try
        {
            foreach (var entry in entries)
            {
                // Stopping: do not start what has not started. The rest of the batch used to run
                // on, handler after handler, past the stop signal; left unstarted it stays pending
                // (Redis has no NAK), its idle clock no longer reset, for a peer to reclaim. A
                // hand-back from the flow engine — here, or in an early-ACK worker — is the same
                // signal, arriving before the token, and so is the worker's intake gate closing at
                // ApplicationStopping: an early-ACK entry is never enqueued and ACKed after it.
                if (stoppingToken.IsCancellationRequested || handedBack || TakesNothingMore(dispatcher))
                    break;

                try
                {
                    switch (await DispatchEntryAsync(dispatcher, entry, attemptFor(entry), stoppingToken).ConfigureAwait(false))
                    {
                        case RedisDispatchOutcome.Processed:
                            processed++;
                            break;
                        case RedisDispatchOutcome.HandedBack:
                            handedBack = true;
                            break;
                    }
                }
                finally
                {
                    // Also counts Deferred and handed-back entries: they were left pending ON
                    // PURPOSE, so the heartbeat must stop touching them and let their idle accrue
                    // toward reclaim.
                    progress.MarkSettled();
                }
            }
        }
        finally
        {
            renewalCancellation.Cancel();
            await renewalTask.ConfigureAwait(false);
        }

        return new BatchResult(processed, handedBack);
    }

    private async Task RenewClaimLoopAsync(
        StreamEntry[] entries,
        RedisValue consumerName,
        BatchProgress progress,
        CancellationToken stoppingToken,
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
                // never stretched. Once stopping — the token, or the worker's intake gate — only the
                // entry in the handler is kept: the rest will not start here, so its idle clock is
                // left to run for a peer's reclaim.
                var settled = progress.SettledCount;
                if (settled >= entries.Length)
                    return;

                var end = stoppingToken.IsCancellationRequested || IntakeClosed ? settled + 1 : entries.Length;
                var remaining = new RedisValue[end - settled];
                for (var i = settled; i < end; i++)
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
            // The dispatch loop let go of the batch.
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
        => ComposeGeneratedConsumerName(Environment.MachineName, Environment.ProcessId, Guid.NewGuid());

    /// <summary>Length budget of the generated consumer name, before the role suffix.</summary>
    internal const int MaxGeneratedConsumerNameLength = 64;

    /// <summary>
    /// <c>{machine}-{pid}-{guid}</c> within <see cref="MaxGeneratedConsumerNameLength"/>, with the
    /// MACHINE NAME giving up the characters. The process id and the GUID are the only parts that
    /// make the name unique, and they sit at the end: the old head-keeping cut
    /// (<c>name[..64]</c>) removed them first, so a 63-character host name — a Kubernetes pod
    /// name, or any host at Linux's HOST_NAME_MAX — left every process on that host with the SAME
    /// consumer identity. Consumers that share a name share one pending-entry list inside the
    /// group: each could claim and acknowledge entries the other was still handling. The suffix is
    /// at most 44 characters, so the machine name always keeps at least 20.
    /// </summary>
    internal static string ComposeGeneratedConsumerName(string machineName, int processId, Guid instance)
    {
        var suffix = $"-{processId.ToString(System.Globalization.CultureInfo.InvariantCulture)}-{instance:N}";
        return PortableText.TruncateWellFormed(machineName, MaxGeneratedConsumerNameLength - suffix.Length) + suffix;
    }
}

internal sealed class RedisWorkerSubscriber : RedisSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;
    private readonly RedisTransportKeySchema _keys;
    private readonly WorkerIntakeGate _intake;

    /// <summary>Runs the RedisWorkerSubscriber operation.</summary>
    public RedisWorkerSubscriber(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IConnectionMultiplexer multiplexer,
        IAsyncResponseIngress ingress,
        ILogger<RedisWorkerSubscriber> logger,
        IHostApplicationLifetime? hostLifetime = null)
        : base(options, multiplexer, logger)
    {
        _ingress = ingress;
        _keys = new RedisTransportKeySchema(options.Value);
        _intake = new WorkerIntakeGate(hostLifetime);
    }

    internal RedisWorkerSubscriber(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IRedisStreamDatabase database,
        IAsyncResponseIngress ingress,
        ILogger<RedisWorkerSubscriber> logger,
        IHostApplicationLifetime? hostLifetime = null)
        : base(options, database, logger)
    {
        _ingress = ingress;
        _keys = new RedisTransportKeySchema(options.Value);
        _intake = new WorkerIntakeGate(hostLifetime);
    }

    protected override RedisKey Stream => _keys.WorkerStream;
    protected override RedisValue ConsumerGroup => Options.WorkerConsumerGroup;
    protected override RedisSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override RedisSubscriberRole SubscriberRole => RedisSubscriberRole.Worker;

    /// <inheritdoc />
    protected override bool IntakeClosed => _intake.IsClosed;

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
