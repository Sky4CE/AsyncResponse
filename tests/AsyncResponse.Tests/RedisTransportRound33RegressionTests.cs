using AsyncResponse.Transports.Redis;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression pins for the round-33 review's Redis worker-transport findings: the early-ACK read
/// and pending-claim clamp, the trimmed-tombstone XCLAIM reply, the awaiting dispatcher's
/// unguarded burials, and the discard path's forwarded stopping token. Every fact here was proven
/// red against the pre-fix code.
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

        Assert.Equal<string>(["1-0", "2-0"], Assert.Single(database.Claims));
        Assert.Equal<string>(["p2"], ingress.Handled);
        Assert.Empty(database.Adds); // drained, not dead-lettered
        Assert.Equal(1, database.CreateGroupCalls); // the subscriber never faulted and restarted
    }

    /// <summary>
    /// Round 33, trimmed tombstone — the partial-reply branch. When XCLAIM answers with fewer
    /// elements than were requested the reply is not positional, so the tombstone cannot be
    /// named: it is dropped from the batch (and left for the next cycle) instead of being
    /// dispatched. Pre-fix: the nil entry was dispatched and the <see cref="ArgumentException"/>
    /// from the discard path faulted the subscriber before it ever read the stream.
    /// </summary>
    [Fact]
    public async Task Subscriber_PartialClaimReplyWithATombstone_SkipsItWithoutFaulting()
    {
        var database = new ModelRedisStreamDatabase { ClaimReply = _ => [StreamEntry.Null] };
        database.AddPending("1-0", StreamEntry.Null);
        database.AddPending("2-0", Entry("2-0", "p2"));
        var ingress = new GatedWorkerIngress();
        var subscriber = WorkerSubscriber(database, ingress, _ => { });

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
        Action<RedisSubscriberOptions> configure)
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
            NullLogger<RedisWorkerSubscriber>.Instance);
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
            lock (_gate)
            {
                _acks.Add(messageId.ToString());
                return Task.FromResult((long)_pending.RemoveAll(item => item.Id == messageId));
            }
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
            => Task.FromResult(messageIds);

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

        public async Task HandleWorkerMessageAsync(string messageJson)
        {
            Source(_started, messageJson).TrySetResult();
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
