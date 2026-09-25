using AsyncResponse.Transports.Redis;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression pins for the round-33 review's Redis worker-transport findings: the early-ACK read
/// and pending-claim clamp, the trimmed-tombstone XCLAIM reply, the awaiting dispatcher's
/// unguarded burials, and the discard path's forwarded stopping token — plus the fixpoint round-1
/// pins that reuse the same stream model (one-at-a-time ack-after-handler reads and claims, the
/// service-owned early-ACK queue, stop and host-stop hand-back behaviour, the monotonic claim
/// schedule, the remaining guarded burials). Every fact here was proven red against the pre-fix
/// code.
/// </summary>
public sealed class RedisTransportRound33RegressionTests
{
    private static readonly TimeSpan WaitBudget = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Round 33, early-ACK read clamp. The read loop gated on the boolean <c>CanAcceptMore</c> and
    /// then asked XREADGROUP for a full <c>BatchSize</c> (16) — into an ACK-after-enqueue queue with
    /// one free slot. The surplus failed <c>TryWrite</c>, came back <c>Deferred</c> and sat in the
    /// PEL un-ACKed; every reclaim bumped its delivery count until the pre-execution cap
    /// dead-lettered healthy jobs whose handler never ran. The read is now clamped to the
    /// dispatcher's free slots (ASB/SQS parity). Pre-fix: the first COUNT is 16 against a queue of
    /// capacity 2.
    /// </summary>
    [Fact]
    public async Task EarlyAckSubscriber_ClampsEveryXreadgroupCountToTheQueuesFreeSlots()
    {
        var database = new ModelRedisStreamDatabase();
        database.Append(Entry("1-0", "p1"), Entry("2-0", "p2"), Entry("3-0", "p3"), Entry("4-0", "p4"), Entry("5-0", "p5"));
        var ingress = new GatedWorkerIngress("p1");
        var subscriber = WorkerSubscriber(
            database,
            ingress,
            options =>
            {
                options.BatchSize = 16;
                options.UseAckAfterEnqueue(1, 2, TimeSpan.FromSeconds(5));
            });

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Started("p1").WaitAsync(WaitBudget); // 1-0 is wedged in the single worker

            var firstRead = database.Reads[0];
            Assert.True(
                firstRead.Count <= 2,
                $"XREADGROUP asked for COUNT {firstRead.Count} against an early-ACK queue of capacity 2.");

            // 1-0 is in the handler and 2-0 holds the queue's only other slot: exactly one slot is
            // free, so the next read asks for exactly one entry.
            await WaitUntilAsync(() => database.Reads.Count >= 2, "a second XREADGROUP");
            Assert.Equal(1, database.Reads[1].Count);

            ingress.Release("p1");
            await WaitUntilAsync(
                () => database.Acks.Count == 5 && ingress.Handled.Count == 5,
                "all five entries to be ACKed and handled");
        }
        finally
        {
            ingress.ReleaseAll();
            await subscriber.StopAsync(CancellationToken.None);
        }

        // No read ever outran the queue, so nothing was deferred back into the PEL: every entry
        // XREADGROUP handed out was ACKed at enqueue and executed exactly once.
        Assert.All(database.Reads, read => Assert.InRange(read.Count, 1, 2));
        Assert.Equal(["1-0", "2-0", "3-0", "4-0", "5-0"], database.Acks.Order(StringComparer.Ordinal));
        Assert.Equal(["p1", "p2", "p3", "p4", "p5"], ingress.Handled.Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// Round 33, early-ACK claim clamp. <c>ClaimPendingAsync</c> asked XPENDING/XCLAIM for
    /// <c>PendingClaimBatchSize</c> entries whatever the queue's free slots — reclaiming more than the
    /// dispatcher could take deferred the rest straight back into the PEL with a bumped delivery
    /// count. The claim is now clamped like the read. Pre-fix: the loop's first XPENDING asks for
    /// 16 against a queue of capacity 2.
    /// </summary>
    [Fact]
    public async Task EarlyAckSubscriber_ClampsThePendingClaimToTheQueuesFreeSlots()
    {
        var database = new ModelRedisStreamDatabase();
        database.Append(Entry("1-0", "p1"), Entry("2-0", "p2"));
        var ingress = new GatedWorkerIngress("p1");
        var subscriber = WorkerSubscriber(
            database,
            ingress,
            options =>
            {
                options.PendingClaimBatchSize = 16;
                options.PendingClaimInterval = TimeSpan.FromMilliseconds(20);
                options.UseAckAfterEnqueue(1, 2, TimeSpan.FromSeconds(5));
            });

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            // The loop's first claim runs against an empty queue: it asks for the capacity, not
            // the batch size.
            await WaitUntilAsync(() => database.PendingCounts.Count >= 1, "the first XPENDING");
            Assert.Equal(2, database.PendingCounts[0]);

            // 1-0 is in the handler and 2-0 holds the other slot: the next scheduled claim asks
            // for the single free slot.
            await ingress.Started("p1").WaitAsync(WaitBudget);
            await WaitUntilAsync(() => database.PendingCounts.Count >= 2, "a second XPENDING");
            Assert.Equal(1, database.PendingCounts[1]);
        }
        finally
        {
            ingress.ReleaseAll();
            await subscriber.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>
    /// Fixpoint r1 (S6a#4). Redis counts a delivery when XREADGROUP hands an entry over, and an
    /// ACK-after-handler batch runs serially — yet the loop still read <c>BatchSize</c> (16)
    /// entries at a time. Every entry read behind a handler that crashed the process came back
    /// with its count bumped without ever having run (MaxDeliveryAttempts crashes later the
    /// pre-execution cap buried the healthy batch-mates with the poison one), and a handler that
    /// ran for hours pinned the rest of its batch here, their idle clocks reset by the heartbeat so
    /// no peer could take them. This mode now reads one entry at a time, leaving the rest unread
    /// where nothing is counted and any peer can take them. Pre-fix: the first COUNT is 16, and
    /// 2-0 and 3-0 sit in this consumer's PEL behind the wedged 1-0.
    /// </summary>
    [Fact]
    public async Task AckAfterHandlerSubscriber_ReadsOneEntryAtATime()
    {
        var database = new ModelRedisStreamDatabase();
        database.Append(Entry("1-0", "p1"), Entry("2-0", "p2"), Entry("3-0", "p3"));
        var ingress = new GatedWorkerIngress("p1");
        var subscriber = WorkerSubscriber(database, ingress, options => options.BatchSize = 16);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Started("p1").WaitAsync(WaitBudget);

            Assert.Equal(1, database.Reads[0].Count);
            Assert.Equal<string>(["1-0"], database.PendingIds);

            ingress.Release("p1");
            await WaitUntilAsync(() => database.Acks.Count == 3, "all three entries to be ACKed");
        }
        finally
        {
            ingress.ReleaseAll();
            await subscriber.StopAsync(CancellationToken.None);
        }

        Assert.All(database.Reads, read => Assert.Equal(1, read.Count));
        Assert.Equal<string>(["p1", "p2", "p3"], ingress.Handled);
    }

    /// <summary>
    /// Fixpoint r1 (S6a#4), the reclaim half. XCLAIM bumps the delivery count too, and the claim
    /// loop claimed every XPENDING candidate at once before running them serially — so each
    /// reclaim of a crash-looping poison entry bumped its whole claimed batch toward the cap, and
    /// a long handler pinned the rest. XPENDING still lists up to <c>PendingClaimBatchSize</c>
    /// candidates (reclaim throughput does not collapse to one per interval), but each is claimed
    /// only right before it runs. Pre-fix: one XCLAIM for all three ids while 1-0 is wedged.
    /// </summary>
    [Fact]
    public async Task AckAfterHandlerSubscriber_ClaimsEachReclaimCandidateRightBeforeItRuns()
    {
        var database = new ModelRedisStreamDatabase();
        database.AddPending("1-0", Entry("1-0", "p1"));
        database.AddPending("2-0", Entry("2-0", "p2"));
        database.AddPending("3-0", Entry("3-0", "p3"));
        var ingress = new GatedWorkerIngress("p1");
        var subscriber = WorkerSubscriber(database, ingress, options => options.PendingClaimBatchSize = 16);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Started("p1").WaitAsync(WaitBudget);

            Assert.Equal<string[]>([["1-0"]], database.Claims);

            ingress.Release("p1");
            await WaitUntilAsync(() => database.Acks.Count == 3, "all three reclaimed entries to be ACKed");
        }
        finally
        {
            ingress.ReleaseAll();
            await subscriber.StopAsync(CancellationToken.None);
        }

        // One listing, then each candidate's own entry re-read right before its claim.
        Assert.Equal<int>([16, 1, 1, 1], database.PendingCounts);
        Assert.Equal<string[]>([["1-0"], ["2-0"], ["3-0"]], database.Claims);
        Assert.Equal<string>(["p1", "p2", "p3"], ingress.Handled);
    }

    /// <summary>
    /// Fixpoint r1 (S6a#3). The early-ACK dispatcher belonged to one supervised attempt, and
    /// disposing it IS the stop-time drain: an XREADGROUP that outlived OperationTimeout ended the
    /// attempt, the drain waited BackgroundDrainTimeout on a host that was not stopping, then
    /// cancelled — and every queued, already-ACKed entry was dead-lettered as
    /// <c>drain_budget_lapsed_after_ack</c> (or lost, when that XADD rode the same stalled Redis).
    /// The dispatcher now belongs to the hosted service and the rebuilt attempt feeds the same
    /// queue. Pre-fix: 2-0, queued behind the wedged 1-0 when the read failed, is buried instead of
    /// handled.
    /// </summary>
    [Fact]
    public async Task EarlyAckSubscriber_AFailedReadDoesNotDrainTheQueueOfAHostThatIsNotStopping()
    {
        var database = new ModelRedisStreamDatabase
        {
            ReadFault = read => read == 2 ? new TimeoutException("The Redis command did not complete within 00:00:10.") : null
        };
        database.Append(Entry("1-0", "p1"), Entry("2-0", "p2"));
        var ingress = new GatedWorkerIngress("p1");
        var subscriber = WorkerSubscriber(
            database,
            ingress,
            options => options.UseAckAfterEnqueue(1, 2, TimeSpan.FromMilliseconds(50)));

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            // 1-0 is wedged in the only worker and 2-0 waits in the queue, both ACKed at enqueue;
            // the next read fails and the supervisor rebuilds the attempt.
            await ingress.Started("p1").WaitAsync(WaitBudget);
            await WaitUntilAsync(() => database.CreateGroupCalls >= 2, "the supervisor to rebuild the attempt after the failed XREADGROUP");

            ingress.Release("p1");
            await WaitUntilAsync(
                () => ingress.Handled.Contains("p2") || database.Adds.Count > 0,
                "2-0 to be handled or dead-lettered");
        }
        finally
        {
            ingress.ReleaseAll();
            await subscriber.StopAsync(CancellationToken.None);
        }

        Assert.Empty(database.Adds);
        Assert.Equal<string>(["p1", "p2"], ingress.Handled);
    }

    /// <summary>
    /// Fixpoint r1 (S6a#6). On host stop the loop kept starting handlers — the next reclaim
    /// candidate, the rest of a batch — while the idle-reset heartbeat, linked to the stop token,
    /// died at once: the live handler's idle clock ran out inside the shutdown window, and a peer's
    /// pending claim took the entry and ran it a second time while it was still executing here.
    /// Now nothing new starts after the stop, and the heartbeat keeps the entry in the handler
    /// until the handler lets go. Pre-fix: no heartbeat after the stop, and 2-0 is claimed and run
    /// once 1-0 finishes.
    /// </summary>
    [Fact]
    public async Task AckAfterHandlerSubscriber_OnStop_StartsNothingNew_AndKeepsTheLiveHandlersEntryClaimed()
    {
        var database = new ModelRedisStreamDatabase();
        database.AddPending("1-0", Entry("1-0", "p1"));
        database.AddPending("2-0", Entry("2-0", "p2"));
        var ingress = new GatedWorkerIngress("p1");
        // A 90 ms reclaim window puts the heartbeat at 30 ms.
        var subscriber = WorkerSubscriber(database, ingress, options => options.PendingMessageMinIdleTime = TimeSpan.FromMilliseconds(90));

        await subscriber.StartAsync(CancellationToken.None);
        Task stopping = Task.CompletedTask;
        try
        {
            await ingress.Started("p1").WaitAsync(WaitBudget);

            // The stop lands while 1-0 is in the handler, which takes no token and runs on.
            stopping = subscriber.StopAsync(CancellationToken.None);
            var beatsAtStop = database.Heartbeats.Count;
            await WaitUntilAsync(() => database.Heartbeats.Count > beatsAtStop, "an idle-reset heartbeat after the stop");
            Assert.All(database.Heartbeats.Skip(beatsAtStop), ids => Assert.Equal<string>(["1-0"], ids));
        }
        finally
        {
            ingress.ReleaseAll();
            await stopping;
            await subscriber.StopAsync(CancellationToken.None);
        }

        Assert.Equal<string>(["p1"], ingress.Handled);
        Assert.Equal<string[]>([["1-0"]], database.Claims);
        Assert.Equal<string>(["2-0"], database.PendingIds); // left for a peer, its count untouched
    }

    /// <summary>
    /// Fixpoint r1 (S6a#6), the batch half: an early-ACK batch kept enqueueing and ACKing after
    /// the stop, feeding a queue that was about to drain under the shutdown budget. The rest of
    /// the batch now stays pending for a peer. Pre-fix: 2-0 and 3-0 are ACKed after the stop.
    /// </summary>
    [Fact]
    public async Task EarlyAckSubscriber_OnStop_LeavesTheRestOfTheBatchPending()
    {
        var database = new ModelRedisStreamDatabase();
        database.Append(Entry("1-0", "p1"), Entry("2-0", "p2"), Entry("3-0", "p3"));
        var ingress = new GatedWorkerIngress();
        RedisWorkerSubscriber? subscriber = null;
        // The stop lands between the first entry's enqueue-and-ACK and the second's: StopAsync
        // cancels the stopping token synchronously, before the loop moves on.
        database.OnAck = id =>
        {
            if (id == "1-0")
                _ = subscriber!.StopAsync(CancellationToken.None);
        };
        subscriber = WorkerSubscriber(
            database,
            ingress,
            options =>
            {
                options.BatchSize = 3;
                options.UseAckAfterEnqueue(1, 3, TimeSpan.FromSeconds(5));
            });

        await subscriber.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => database.Acks.Count >= 1, "the first enqueue-and-ACK");
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Equal(3, database.Reads[0].Count);
        Assert.Equal<string>(["1-0"], database.Acks);
        Assert.Equal<string>(["2-0", "3-0"], database.PendingIds);
        Assert.Equal<string>(["p1"], ingress.Handled);
    }

    /// <summary>
    /// Fixpoint r1 (host-stop hand-back) at the loop: a hand-back from the flow engine means the
    /// host is stopping even though this subscriber's token is still live, so the loop treats it
    /// like its own stop — the handed-back entry stays pending and unACKed, no further reclaim
    /// candidate is claimed and started, and (pre-commit review of fixpoint round 1) nothing more
    /// is read either: ending only the claim cycle went straight on to XREADGROUP, and every flow
    /// wake-up read during the stop window was handed back too, pending on the stopping consumer
    /// with an attempt spent. Deterministic: the stop lands inside the hand-back itself, after the
    /// dispatcher has recognised it with the token still live — the old loop's XREADGROUP in the
    /// same iteration runs regardless. Pre-fix: one XREADGROUP after the hand-back.
    /// </summary>
    [Fact]
    public async Task AckAfterHandlerSubscriber_AHostStopHandBack_LeavesTheEntryPending_AndReadsAndClaimsNothingMore()
    {
        var database = new ModelRedisStreamDatabase();
        database.AddPending("1-0", Entry("1-0", "p1"));
        database.AddPending("2-0", Entry("2-0", "p2"));
        database.Append(Entry("3-0", "p3"));
        var ingress = new GatedWorkerIngress
        {
            Fault = payload => payload == "p1"
                ? new DurableFlowInterruptedException("Host is stopping; durable flow 'flow-1' left its in-process wait and the delivery is abandoned for redelivery.")
                : null
        };
        RedisWorkerSubscriber? subscriber = null;
        Task stopping = Task.CompletedTask;
        var logger = new StopOnLogLogger<RedisWorkerSubscriber>("handed back by the flow engine", () => stopping = subscriber!.StopAsync(CancellationToken.None));
        subscriber = WorkerSubscriber(database, ingress, _ => { }, logger);

        await subscriber.StartAsync(CancellationToken.None);
        await ingress.Started("p1").WaitAsync(WaitBudget);
        await WaitUntilAsync(() => logger.Fired, "the hand-back to be recognised");
        await stopping.WaitAsync(WaitBudget);

        Assert.Equal<string[]>([["1-0"]], database.Claims);
        Assert.Empty(database.Reads);
        Assert.Empty(database.Acks);
        Assert.Empty(database.Adds);
        Assert.Equal<string>(["1-0", "2-0"], database.PendingIds);
        Assert.Empty(ingress.Handled);
        Assert.Equal(1, database.CreateGroupCalls); // no failure restart either
    }

    /// <summary>
    /// Pre-commit review of fixpoint round 1, the early-ACK half: a worker's hand-back latches the
    /// stop for the loop too, so the rest of the batch is not enqueued and ACKed — each of those
    /// wake-ups would be handed back by the next worker in turn, and every one, already ACKed, would
    /// become a dead-letter copy instead of a pending entry a live peer takes. Deterministic: the
    /// first ACK holds the loop until the worker has reported the hand-back. Pre-fix: 2-0 and 3-0
    /// are enqueued and ACKed.
    /// </summary>
    [Fact]
    public async Task EarlyAckSubscriber_AWorkersHostStopHandBack_LeavesTheRestOfTheBatchPending()
    {
        var database = new ModelRedisStreamDatabase();
        database.Append(Entry("1-0", "p1"), Entry("2-0", "p2"), Entry("3-0", "p3"));
        var ingress = new GatedWorkerIngress
        {
            Fault = payload => payload == "p1"
                ? new DurableFlowInterruptedException("Host is stopping; durable flow 'flow-1' wake-up is abandoned.")
                : null
        };
        var handBackReported = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        database.OnAck = id =>
        {
            // Runs on the loop, inside the first entry's enqueue-and-ACK: the worker (another
            // thread) runs p1 and reports its hand-back before the loop moves on to 2-0.
            if (id == "1-0")
                handBackReported.Task.Wait(WaitBudget);
        };
        var subscriber = WorkerSubscriber(
            database,
            ingress,
            options =>
            {
                options.BatchSize = 3;
                options.UseAckAfterEnqueue(1, 3, TimeSpan.FromSeconds(5));
                options.OnBackgroundFailure = _ =>
                {
                    handBackReported.TrySetResult();
                    return ValueTask.CompletedTask;
                };
            });

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await handBackReported.Task.WaitAsync(WaitBudget);
            await WaitUntilAsync(() => database.Adds.Count == 1, "the handed-back entry's dead-letter copy");
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
        }

        Assert.Single(database.Reads);
        Assert.Equal<string>(["1-0", "1-0"], database.Acks); // enqueue-and-ACK, then the burial's own XACK
        Assert.Equal<string>(["2-0", "3-0"], database.PendingIds);
        Assert.Equal("handed_back_after_commit", RedisTransportTests.Field(Assert.Single(database.Adds), "reason"));
        Assert.Empty(ingress.Handled);
    }

    /// <summary>
    /// Pre-commit review of fixpoint round 1: with one XCLAIM per candidate, the attempt number
    /// came from the XPENDING listing taken before the first candidate ran — as old as every
    /// earlier handler, minutes behind a slow one. A peer that claimed, failed and released a later
    /// candidate meanwhile went uncounted, so a poison entry ran past MaxDeliveryAttempts and was
    /// dead-lettered with a wrong attempt. Each candidate is now re-read right before its claim.
    /// Pre-fix: 2-0 runs on attempt 2 instead of being buried on attempt 4.
    /// </summary>
    [Fact]
    public async Task AckAfterHandlerSubscriber_ReclaimCandidate_UsesItsDeliveryCountAtClaimTime()
    {
        var database = new ModelRedisStreamDatabase();
        database.AddPending("1-0", Entry("1-0", "p1"));
        database.AddPending("2-0", Entry("2-0", "p2"));
        var ingress = new GatedWorkerIngress("p1");
        var subscriber = WorkerSubscriber(database, ingress, options => options.MaxDeliveryAttempts = 3);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Started("p1").WaitAsync(WaitBudget);

            // While 1-0 runs, peers claim 2-0 twice and fail: its count is now 3.
            database.SetDeliveryCount("2-0", 3);
            ingress.Release("p1");
            await WaitUntilAsync(() => database.Adds.Count == 1, "2-0 to be dead-lettered on its real attempt");
        }
        finally
        {
            ingress.ReleaseAll();
            await subscriber.StopAsync(CancellationToken.None);
        }

        var dead = Assert.Single(database.Adds);
        Assert.Equal("max_delivery_attempts_exceeded", RedisTransportTests.Field(dead, "reason"));
        Assert.Equal("4", RedisTransportTests.Field(dead, "attempt"));
        Assert.Equal<string>(["p1"], ingress.Handled);
    }

    /// <summary>
    /// Round 33, trimmed tombstone in the XCLAIM reply. Redis 5/6 answer XCLAIM with a nil entry
    /// for an id whose message was trimmed while still pending. The claim loop handed that entry to
    /// the dispatcher, whose discard path built a delivery from the null id and sent it to
    /// XADD/XACK — StackExchange.Redis rejects a null value client-side with
    /// <see cref="ArgumentException"/>, thrown from INSIDE the catch, which replaced the original
    /// error and faulted the subscriber; the tombstone was re-claimed on every restart and the
    /// healthy entry behind it never ran. Nil entries are now filtered out before dispatch and,
    /// when the reply is positional, ACKed by their pending ids so they drain. Pre-fix: the
    /// subscriber restarts in a loop and neither 1-0 nor 2-0 is ever ACKed.
    /// </summary>
    [Fact]
    public async Task Subscriber_ClaimingATrimmedTombstone_AcksItByItsPendingIdAndDispatchesTheRest()
    {
        var database = new ModelRedisStreamDatabase();
        database.AddPending("1-0", StreamEntry.Null); // trimmed while pending: no id, no fields
        database.AddPending("2-0", Entry("2-0", "p2"));
        var ingress = new GatedWorkerIngress();
        var subscriber = WorkerSubscriber(database, ingress, _ => { });

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntilAsync(
                () => database.Acks.Contains("1-0") && database.Acks.Contains("2-0"),
                "the tombstone 1-0 and the live entry 2-0 to be ACKed");
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
        }

        // ACK-after-handler claims each candidate right before it runs (fixpoint r1, S6a#4), so
        // the tombstone is its own positional one-id reply.
        Assert.Equal<string[]>([["1-0"], ["2-0"]], database.Claims);
        Assert.Equal<string>(["p2"], ingress.Handled);
        Assert.Empty(database.Adds); // drained, not dead-lettered
        Assert.Equal(1, database.CreateGroupCalls); // the subscriber never faulted and restarted
    }

    /// <summary>
    /// Round 33, trimmed tombstone — the partial-reply branch. When XCLAIM answers with fewer
    /// elements than were requested the reply is not positional, so the tombstone cannot be
    /// named: it is dropped from the batch (and left for the next cycle) instead of being
    /// dispatched. Pre-fix: the nil entry was dispatched and the <see cref="ArgumentException"/>
    /// from the discard path faulted the subscriber before it ever read the stream. Only a batch
    /// claim can be partial, and since fixpoint r1 only ACK-after-enqueue claims in batches.
    /// </summary>
    [Fact]
    public async Task Subscriber_PartialClaimReplyWithATombstone_SkipsItWithoutFaulting()
    {
        var database = new ModelRedisStreamDatabase { ClaimReply = _ => [StreamEntry.Null] };
        database.AddPending("1-0", StreamEntry.Null);
        database.AddPending("2-0", Entry("2-0", "p2"));
        var ingress = new GatedWorkerIngress();
        var subscriber = WorkerSubscriber(database, ingress, options => options.UseAckAfterEnqueue(1, 2, TimeSpan.FromSeconds(5)));

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            // The loop survives the claim and goes on to read the stream.
            await WaitUntilAsync(() => database.Reads.Count >= 1, "the first XREADGROUP after the claim");
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
        }

        Assert.Single(database.Claims);
        Assert.Empty(database.Acks); // not positional: nothing can be named, so nothing is ACKed
        Assert.Empty(database.Adds);
        Assert.Empty(ingress.Handled);
        Assert.Equal(1, database.CreateGroupCalls);
    }

    /// <summary>
    /// Round 33, the discard path on a tombstone. A <see cref="StreamEntry.Null"/> has no id to
    /// ACK and no payload to record, yet <c>DiscardUnprocessableAsync</c> built a delivery from the
    /// null id and dead-lettered + ACKed it — the client rejects the null value with
    /// <see cref="ArgumentException"/> from inside the caller's catch. It is now a logged no-op;
    /// the claim loop drains a tombstone by its pending id. Pre-fix: the client's
    /// "A null value is not valid in this context" escapes.
    /// </summary>
    [Fact]
    public async Task DiscardUnprocessable_OnATrimmedTombstone_IsALoggedNoOp()
    {
        var database = new ModelRedisStreamDatabase();
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            new RedisSubscriberOptions(),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.DiscardUnprocessableAsync(
            "worker-stream",
            "worker-group",
            StreamEntry.Null,
            new InvalidDataException("no payload field"),
            CancellationToken.None);

        Assert.Empty(database.Adds);
        Assert.Empty(database.Acks);
    }

    /// <summary>
    /// Round 33, unguarded at-cap burial. The awaiting dispatcher's post-handler burial called
    /// <c>DeadLetterAndAckAsync</c> unguarded (the queued sibling guards every burial). A
    /// dead-letter XADD that failed — WRONGTYPE on the dead-letter key, MISCONF/OOM, the adapter's
    /// timeout — escaped <c>HandleAsync</c>, <c>DispatchBatchAsync</c> and the supervisor's restart
    /// loop: the XACK never ran, the same entry was re-claimed every cycle and the whole stream
    /// stopped draining. The burial is now caught and logged; the entry stays pending for the next
    /// claim cycle. Pre-fix: the <see cref="TimeoutException"/> escapes <c>HandleAsync</c>.
    /// </summary>
    [Fact]
    public async Task Awaiting_AtCapBurialWhoseDeadLetterXaddThrows_DoesNotEscapeHandleAsync()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            AddException = new TimeoutException("The Redis command did not complete within 00:00:10.")
        };
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            new RedisSubscriberOptions { MaxDeliveryAttempts = 1 },
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        var outcome = await dispatcher.HandleAsync(Delivery("1-0", attempt: 1), CancellationToken.None);

        Assert.Equal(RedisDispatchOutcome.Processed, outcome);
        Assert.Equal(1, database.AddAttempts);
        Assert.Empty(database.Acks); // the burial failed before the XACK: the entry stays pending
    }

    /// <summary>
    /// Round 33, unguarded pre-execution burial — the same finding's other site: an entry already
    /// past <c>MaxDeliveryAttempts</c> is buried before the handler runs, and that burial was
    /// unguarded too. Pre-fix: the <see cref="TimeoutException"/> escapes <c>HandleAsync</c>.
    /// </summary>
    [Fact]
    public async Task Awaiting_PreExecutionOverCapBurialWhoseDeadLetterXaddThrows_DoesNotEscapeHandleAsync()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            AddException = new TimeoutException("The Redis command did not complete within 00:00:10.")
        };
        var handled = false;
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) =>
            {
                handled = true;
                return Task.CompletedTask;
            },
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            new RedisSubscriberOptions { MaxDeliveryAttempts = 3 },
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        var outcome = await dispatcher.HandleAsync(Delivery("1-0", attempt: 4), CancellationToken.None);

        Assert.Equal(RedisDispatchOutcome.Processed, outcome);
        Assert.False(handled);
        Assert.Equal(1, database.AddAttempts);
        Assert.Empty(database.Acks);
    }

    /// <summary>
    /// Fixpoint r1 (S6a#15). The pending-claim schedule ran on the wall clock
    /// (<c>next = UtcNow + PendingClaimInterval</c>), so a backward clock step — a VM resume, an NTP
    /// correction — suspended every reclaim for the size of the step. It now runs on the monotonic
    /// timestamp. Red on a variant scheduled on the seam's wall clock: after the hour-long backward
    /// step no second XPENDING ever comes.
    /// </summary>
    [Fact]
    public async Task Subscriber_PendingClaimSchedule_SurvivesABackwardWallClockStep()
    {
        var database = new ModelRedisStreamDatabase();
        var clock = new SteppingClock();
        var ingress = new GatedWorkerIngress();
        var subscriber = WorkerSubscriber(database, ingress, options => options.PendingClaimInterval = TimeSpan.FromSeconds(5));
        subscriber.Clock = clock;

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            // The first loop pass claims; later passes, inside the interval, only read.
            await WaitUntilAsync(() => database.Reads.Count >= 3, "a few loop passes");
            Assert.Single(database.PendingCounts);

            // The wall clock jumps an hour back while monotonic time moves past the interval.
            clock.Step(wall: TimeSpan.FromHours(-1), monotonic: TimeSpan.FromSeconds(6));
            await WaitUntilAsync(() => database.PendingCounts.Count >= 2, "the next scheduled XPENDING");
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>A clock whose wall time and monotonic timestamp move independently, only when stepped.</summary>
    private sealed class SteppingClock : TimeProvider
    {
        private long _timestamp;
        private long _utcTicks = new DateTimeOffset(2026, 9, 25, 12, 0, 0, TimeSpan.Zero).UtcTicks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => Interlocked.Read(ref _timestamp);

        public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _utcTicks), TimeSpan.Zero);

        public void Step(TimeSpan wall, TimeSpan monotonic)
        {
            Interlocked.Add(ref _utcTicks, wall.Ticks);
            Interlocked.Add(ref _timestamp, monotonic.Ticks);
        }
    }

    /// <summary>
    /// Fixpoint r1 (S6a#11): the queued dispatcher's pre-execution burial was the one at-cap burial
    /// left unguarded — a failed dead-letter XADD escaped <c>HandleAsync</c> to the supervisor,
    /// which restarted the subscriber; the entry was reclaimed and re-thrown every idle window.
    /// Pre-fix: the <see cref="TimeoutException"/> escapes <c>HandleAsync</c>.
    /// </summary>
    [Fact]
    public async Task Queued_PreExecutionOverCapBurialWhoseDeadLetterXaddThrows_DoesNotEscapeHandleAsync()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            AddException = new TimeoutException("The Redis command did not complete within 00:00:10.")
        };
        var handled = false;
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) =>
            {
                handled = true;
                return Task.CompletedTask;
            },
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            new RedisSubscriberOptions { MaxDeliveryAttempts = 3 }.UseAckAfterEnqueue(1, 4, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        var outcome = await dispatcher.HandleAsync(Delivery("1-0", attempt: 4), CancellationToken.None);

        Assert.Equal(RedisDispatchOutcome.Processed, outcome);
        Assert.False(handled);
        Assert.Equal(1, database.AddAttempts);
        Assert.Empty(database.Acks); // the burial failed before the XACK: the entry stays pending
    }

    /// <summary>
    /// Fixpoint r1 (S6a#11), the unparsable-entry burial: same unguarded write, reached from
    /// inside the subscriber's catch for an entry that could not become a delivery. Pre-fix: the
    /// <see cref="TimeoutException"/> escapes <c>DiscardUnprocessableAsync</c>.
    /// </summary>
    [Fact]
    public async Task DiscardUnprocessable_WhoseDeadLetterXaddThrows_DoesNotEscape()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            AddException = new TimeoutException("The Redis command did not complete within 00:00:10.")
        };
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            new RedisSubscriberOptions(),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.DiscardUnprocessableAsync(
            "worker-stream",
            "worker-group",
            RedisTransportTests.Entry("1-0", ("correlationId", "c1")), // no payload field
            new InvalidDataException("no payload field"),
            CancellationToken.None);

        Assert.Equal(1, database.AddAttempts);
        Assert.Empty(database.Acks);
    }

    /// <summary>
    /// Round 33, discard settlement token. <c>DiscardUnprocessableAsync</c> was the one settlement
    /// in the dispatcher that forwarded the caller's stopping token into
    /// <c>DeadLetterAndAckAsync</c> (every sibling pins <see cref="CancellationToken.None"/>): a
    /// shutdown landing between the dead-letter XADD and the XACK abandoned the ACK and left the
    /// malformed entry in the PEL to be reclaimed and dead-lettered a SECOND time after restart.
    /// Pre-fix: the recorded XADD/XACK tokens are the (cancelled) stopping token.
    /// </summary>
    [Fact]
    public async Task DiscardUnprocessable_SettlesOnCancellationTokenNone()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            new RedisSubscriberOptions(),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);
        var malformed = RedisTransportTests.Entry("1-0", ("correlationId", "c1")); // no payload field

        await dispatcher.DiscardUnprocessableAsync(
            "worker-stream",
            "worker-group",
            malformed,
            new InvalidDataException("no payload field"),
            new CancellationToken(canceled: true));

        Assert.Equal("unparsable_entry", RedisTransportTests.Field(Assert.Single(database.Adds).Values, "reason"));
        Assert.Equal("1-0", Assert.Single(database.Acks).MessageId);
        Assert.False(Assert.Single(database.AddTokens).CanBeCanceled); // CancellationToken.None, not the stopping token
        Assert.False(Assert.Single(database.AckTokens).CanBeCanceled);
    }

    private static RedisStreamDelivery Delivery(string id, int attempt)
        => new(
            "worker-stream",
            "worker-group",
            id,
            "payload-json",
            "corr",
            attempt,
            RedisTransportTests.Entry(id, ("payload", "payload-json"), ("correlationId", "corr")));

    private static StreamEntry Entry(string id, string payload)
        => RedisTransportTests.Entry(id, ("payload", payload), ("correlationId", $"corr-{id}"));

    private static RedisWorkerSubscriber WorkerSubscriber(
        IRedisStreamDatabase database,
        IAsyncResponseIngress ingress,
        Action<RedisSubscriberOptions> configure,
        ILogger<RedisWorkerSubscriber>? logger = null)
    {
        var options = new RedisAsyncResponseTransportOptions
        {
            WorkerStream = "workers",
            WorkerConsumerGroup = "workers-group",
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(1),
            WorkerSubscriber =
            {
                EmptyPollDelay = TimeSpan.FromMilliseconds(1),
                PendingClaimInterval = TimeSpan.FromSeconds(30),
                // Also arms the in-flight idle heartbeat at a third of its value; 30s keeps that
                // heartbeat out of these fast batches.
                PendingMessageMinIdleTime = TimeSpan.FromSeconds(30)
            }
        };
        configure(options.WorkerSubscriber);
        return new RedisWorkerSubscriber(
            Options.Create(options),
            database,
            ingress,
            logger ?? NullLogger<RedisWorkerSubscriber>.Instance);
    }

    /// <summary>
    /// Runs <paramref name="onMatch"/> once, on the logging thread, the first time a message
    /// contains <paramref name="fragment"/>; <see cref="Fired"/> turns true once it has returned.
    /// </summary>
    private sealed class StopOnLogLogger<T>(string fragment, Action onMatch) : ILogger<T>
    {
        private int _matched;
        private int _fired;

        public bool Fired => Volatile.Read(ref _fired) != 0;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!formatter(state, exception).Contains(fragment, StringComparison.Ordinal) || Interlocked.Exchange(ref _matched, 1) != 0)
                return;

            onMatch();
            Volatile.Write(ref _fired, 1);
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + WaitBudget;
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException($"Timed out waiting for {what}.");

            await Task.Delay(10);
        }
    }

    /// <summary>
    /// Stands in for the stream adapter with the server/client behaviours these findings hinge
    /// on, which the shared fake leaves out: XREADGROUP honours COUNT and serves each new entry
    /// once (into the PEL); XPENDING lists the un-ACKed pending entries and XCLAIM answers them
    /// positionally, with <see cref="StreamEntry.Null"/> for an id trimmed while still pending
    /// (Redis 5/6); and XADD/XACK reject a null value with <see cref="ArgumentException"/> exactly
    /// as StackExchange.Redis does client-side (the real adapter forwards the values untouched).
    /// Min-idle is not modelled. The subscriber loop and its workers run on background threads,
    /// so every record is taken under a lock and read back as a snapshot.
    /// </summary>
    private sealed class ModelRedisStreamDatabase : IRedisStreamDatabase
    {
        private readonly object _gate = new();
        private readonly Queue<StreamEntry> _newEntries = new();
        private readonly List<PendingEntry> _pending = [];
        private readonly List<ReadCall> _reads = [];
        private readonly List<int> _pendingCounts = [];
        private readonly List<string[]> _claims = [];
        private readonly List<string> _acks = [];
        private readonly List<NameValueEntry[]> _adds = [];
        private int _createGroupCalls;

        /// <summary>Replaces the positional XCLAIM reply (for example with a partial one).</summary>
        public Func<RedisValue[], StreamEntry[]>? ClaimReply { get; set; }

        /// <summary>Given the 1-based XREADGROUP number, the exception that read fails with (or null).</summary>
        public Func<int, Exception?>? ReadFault { get; set; }

        /// <summary>Runs after an id is ACKed, outside the model's lock.</summary>
        public Action<string>? OnAck { get; set; }

        /// <summary>The ids of every XCLAIM JUSTID heartbeat, in call order.</summary>
        public IReadOnlyList<string[]> Heartbeats => Snapshot(_heartbeats);

        private readonly List<string[]> _heartbeats = [];

        /// <summary>Every XREADGROUP, in call order.</summary>
        public IReadOnlyList<ReadCall> Reads => Snapshot(_reads);

        /// <summary>The COUNT argument of every XPENDING, in call order.</summary>
        public IReadOnlyList<int> PendingCounts => Snapshot(_pendingCounts);

        /// <summary>The ids requested by every XCLAIM, in call order.</summary>
        public IReadOnlyList<string[]> Claims => Snapshot(_claims);

        /// <summary>Every ACKed id, in call order.</summary>
        public IReadOnlyList<string> Acks => Snapshot(_acks);

        /// <summary>The fields of every XADD (dead-letter writes), in call order.</summary>
        public IReadOnlyList<NameValueEntry[]> Adds => Snapshot(_adds);

        /// <summary>One per subscriber (re)start: the loop ensures the group before it reads.</summary>
        public int CreateGroupCalls => Volatile.Read(ref _createGroupCalls);

        /// <summary>The ids in the pending-entry list (handed out, not yet ACKed).</summary>
        public IReadOnlyList<string> PendingIds
        {
            get
            {
                lock (_gate)
                {
                    return _pending.Select(item => item.Id.ToString()).ToArray();
                }
            }
        }

        public void Append(params StreamEntry[] entries)
        {
            lock (_gate)
            {
                foreach (var entry in entries)
                    _newEntries.Enqueue(entry);
            }
        }

        public void AddPending(RedisValue id, StreamEntry entry, int deliveryCount = 1)
        {
            lock (_gate)
            {
                _pending.Add(new PendingEntry(id, entry, deliveryCount));
            }
        }

        /// <summary>What a peer's claim does to a pending entry's delivery count.</summary>
        public void SetDeliveryCount(RedisValue id, int deliveryCount)
        {
            lock (_gate)
            {
                var index = _pending.FindIndex(item => item.Id == id);
                _pending[index] = _pending[index] with { DeliveryCount = deliveryCount };
            }
        }

        public Task<RedisValue> StreamAddAsync(
            RedisKey stream,
            NameValueEntry[] values,
            long? maxLength,
            bool useApproximateMaxLength,
            CancellationToken cancellationToken)
        {
            foreach (var value in values)
                AssertNotNull(value.Value);

            lock (_gate)
            {
                _adds.Add(values);
                return Task.FromResult<RedisValue>($"{_adds.Count}-0");
            }
        }

        public Task<bool> StreamCreateConsumerGroupAsync(
            RedisKey stream,
            RedisValue groupName,
            RedisValue position,
            bool createStream,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _createGroupCalls);
            return Task.FromResult(true);
        }

        public Task<StreamEntry[]> StreamReadGroupAsync(
            RedisKey stream,
            RedisValue groupName,
            RedisValue consumerName,
            int count,
            CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _reads.Add(new ReadCall(consumerName.ToString(), count));
                if (ReadFault?.Invoke(_reads.Count) is { } fault)
                    return Task.FromException<StreamEntry[]>(fault);

                var served = new List<StreamEntry>();
                while (served.Count < count && _newEntries.Count > 0)
                {
                    var entry = _newEntries.Dequeue();
                    _pending.Add(new PendingEntry(entry.Id, entry, DeliveryCount: 1));
                    served.Add(entry);
                }

                return Task.FromResult(served.ToArray());
            }
        }

        public Task<long> StreamAcknowledgeAsync(
            RedisKey stream,
            RedisValue groupName,
            RedisValue messageId,
            CancellationToken cancellationToken)
        {
            AssertNotNull(messageId);
            long removed;
            lock (_gate)
            {
                _acks.Add(messageId.ToString());
                removed = _pending.RemoveAll(item => item.Id == messageId);
            }

            OnAck?.Invoke(messageId.ToString());
            return Task.FromResult(removed);
        }

        public Task<StreamPendingMessageInfo[]> StreamPendingMessagesAsync(
            RedisKey stream,
            RedisValue groupName,
            int count,
            RedisValue consumerName,
            RedisValue? minId,
            RedisValue? maxId,
            long minIdleTimeInMilliseconds,
            CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _pendingCounts.Add(count);
                return Task.FromResult(_pending
                    .Where(item => minId is not { } only || maxId is not { } last || only != last || item.Id == only)
                    .Take(count)
                    .Select(item => PendingInfo(item.Id, item.DeliveryCount))
                    .ToArray());
            }
        }

        public Task<StreamEntry[]> StreamClaimAsync(
            RedisKey stream,
            RedisValue groupName,
            RedisValue consumerName,
            long minIdleTimeInMilliseconds,
            RedisValue[] messageIds,
            CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _claims.Add(messageIds.Select(id => id.ToString()).ToArray());
                if (ClaimReply is not null)
                    return Task.FromResult(ClaimReply(messageIds));

                // Redis 5/6: one element per requested id that is still pending, in request order —
                // the entry itself, or nil when its message was trimmed while pending.
                var reply = new List<StreamEntry>();
                foreach (var id in messageIds)
                {
                    var item = _pending.Find(pending => pending.Id == id);
                    if (item is not null)
                        reply.Add(item.Entry);
                }

                return Task.FromResult(reply.ToArray());
            }
        }

        public Task<RedisValue[]> StreamClaimIdsOnlyAsync(
            RedisKey stream,
            RedisValue groupName,
            RedisValue consumerName,
            long minIdleTimeInMilliseconds,
            RedisValue[] messageIds,
            CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _heartbeats.Add(messageIds.Select(id => id.ToString()).ToArray());
            }

            return Task.FromResult(messageIds);
        }

        /// <summary>StackExchange.Redis's <c>RedisValue.AssertNotNull</c>, which every command argument passes through.</summary>
        private static void AssertNotNull(RedisValue value)
        {
            if (value.IsNull)
                throw new ArgumentException("A null value is not valid in this context");
        }

        private T[] Snapshot<T>(List<T> list)
        {
            lock (_gate)
            {
                return list.ToArray();
            }
        }

        private static StreamPendingMessageInfo PendingInfo(RedisValue messageId, int deliveryCount)
            => (StreamPendingMessageInfo)PendingConstructor.Invoke([messageId, (RedisValue)"old-consumer", 500L, deliveryCount]);

        private static readonly ConstructorInfo PendingConstructor =
            typeof(StreamPendingMessageInfo).GetConstructor(
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                binder: null,
                [typeof(RedisValue), typeof(RedisValue), typeof(long), typeof(int)],
                modifiers: null)
            ?? throw new InvalidOperationException("StreamPendingMessageInfo constructor was not found.");

        private sealed record PendingEntry(RedisValue Id, StreamEntry Entry, int DeliveryCount);

        public sealed record ReadCall(string Consumer, int Count);
    }

    /// <summary>Records every worker payload it handles and wedges the gated ones until released.</summary>
    private sealed class GatedWorkerIngress : IAsyncResponseIngress
    {
        private readonly object _gate = new();
        private readonly HashSet<string> _gated;
        private readonly Dictionary<string, TaskCompletionSource> _started = new(StringComparer.Ordinal);
        private readonly Dictionary<string, TaskCompletionSource> _release = new(StringComparer.Ordinal);
        private readonly List<string> _handled = [];

        public GatedWorkerIngress(params string[] gatedPayloads)
        {
            _gated = new HashSet<string>(gatedPayloads, StringComparer.Ordinal);
        }

        /// <summary>Payloads whose handler ran to completion, in completion order.</summary>
        public IReadOnlyList<string> Handled
        {
            get
            {
                lock (_gate)
                {
                    return _handled.ToArray();
                }
            }
        }

        public Task Started(string payload) => Source(_started, payload).Task;

        public void Release(string payload) => Source(_release, payload).TrySetResult();

        public void ReleaseAll()
        {
            foreach (var payload in _gated)
                Release(payload);
        }

        /// <summary>Given a payload, the exception its handler throws right after starting (or null).</summary>
        public Func<string, Exception?>? Fault { get; set; }

        public async Task HandleWorkerMessageAsync(string messageJson)
        {
            Source(_started, messageJson).TrySetResult();
            if (Fault?.Invoke(messageJson) is { } fault)
                throw fault;

            if (_gated.Contains(messageJson))
                await Source(_release, messageJson).Task;

            lock (_gate)
            {
                _handled.Add(messageJson);
            }
        }

        public Task HandleResponseMessageAsync(string messageJson, string? correlationId)
            => Task.CompletedTask;

        private TaskCompletionSource Source(Dictionary<string, TaskCompletionSource> sources, string payload)
        {
            lock (_gate)
            {
                if (!sources.TryGetValue(payload, out var source))
                {
                    source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    sources[payload] = source;
                }

                return source;
            }
        }
    }
}
