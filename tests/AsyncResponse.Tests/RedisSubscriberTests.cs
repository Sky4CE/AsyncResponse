using AsyncResponse.Transports.Redis;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

public class RedisSubscriberTests
{
    [Fact]
    public async Task WorkerSubscriber_ForwardsPayloadAndAcks()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            FirstReadStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
        };
        database.ReadBatches.Enqueue(
        [
            RedisTransportTests.Entry("1-0", ("payload", "worker-json"), ("correlationId", "corr-worker"))
        ]);
        var ingress = new Mock<IAsyncResponseIngress>();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json"))
            .Returns(() =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            });
        var subscriber = new RedisWorkerSubscriber(
            Options.Create(new RedisAsyncResponseTransportOptions
            {
                WorkerStream = "workers",
                WorkerConsumerGroup = "workers-group",
                WorkerSubscriber =
                {
                    EmptyPollDelay = TimeSpan.FromMilliseconds(1),
                    PendingClaimInterval = TimeSpan.FromSeconds(30)
                }
            }),
            database,
            ingress.Object,
            NullLogger<RedisWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        ingress.Verify(i => i.HandleWorkerMessageAsync("worker-json"), Times.Once);
        Assert.Contains(database.Acks, ack => ack.Stream == "workers" && ack.Group == "workers-group" && ack.MessageId == "1-0");
    }

    [Fact]
    public async Task ResponseSubscriber_ExtractsCorrelationAndForwardsPayload()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        database.ReadBatches.Enqueue(
        [
            RedisTransportTests.Entry("1-0", ("payload", """{"CorrelationId":"corr-json","Status":2}"""))
        ]);
        var ingress = new Mock<IAsyncResponseIngress>();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ingress.Setup(i => i.HandleResponseMessageAsync("""{"CorrelationId":"corr-json","Status":2}""", "corr-json"))
            .Returns(() =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            });
        var subscriber = new RedisResponseIngressSubscriber(
            Options.Create(new RedisAsyncResponseTransportOptions
            {
                ResponseStream = "responses",
                ResponseConsumerGroup = "responses-group",
                WorkerSubscriber = { EmptyPollDelay = TimeSpan.FromMilliseconds(1) },
                ResponseSubscriber = { EmptyPollDelay = TimeSpan.FromMilliseconds(1) }
            }),
            database,
            ingress.Object,
            NullLogger<RedisResponseIngressSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        ingress.Verify(i => i.HandleResponseMessageAsync("""{"CorrelationId":"corr-json","Status":2}""", "corr-json"), Times.Once);
        Assert.Contains(database.Acks, ack => ack.Stream == "responses" && ack.Group == "responses-group" && ack.MessageId == "1-0");
    }

    [Fact]
    public async Task WorkerSubscriber_WhenConsumerGroupAlreadyExists_Continues()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            // Deliberately the obsolete overload. The only alternative takes a RedisErrorKind, which
            // StackExchange.Redis ships as experimental (SER007: "subject to change or removal"), and
            // there is no BUSYGROUP kind to pass anyway. What this double must reproduce is the
            // exception type plus the message — RedisSubscriberServices.CreateConsumerGroup catches
            // RedisServerException and filters on "BUSYGROUP", never reading the kind — so taking a
            // dependency on an evaluation-only API here would buy nothing and break on the next bump.
#pragma warning disable CS0618 // Type or member is obsolete
            CreateConsumerGroupException = new RedisServerException("BUSYGROUP Consumer Group name already exists")
#pragma warning restore CS0618
        };
        database.ReadBatches.Enqueue(
        [
            RedisTransportTests.Entry("1-0", ("payload", "worker-json"), ("correlationId", "corr-worker"))
        ]);
        var ingress = new Mock<IAsyncResponseIngress>();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json"))
            .Returns(() =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            });
        var subscriber = WorkerSubscriber(database, ingress.Object);

        await subscriber.StartAsync(CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Single(database.CreateGroupCalls);
        ingress.Verify(i => i.HandleWorkerMessageAsync("worker-json"), Times.Once);
    }

    [Fact]
    public async Task WorkerSubscriber_RetriesAfterReadFailure()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            ReadFailuresBeforeSuccess = 1
        };
        database.ReadBatches.Enqueue(
        [
            RedisTransportTests.Entry("1-0", ("payload", "worker-json"), ("correlationId", "corr-worker"))
        ]);
        var ingress = new Mock<IAsyncResponseIngress>();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json"))
            .Returns(() =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            });
        var subscriber = WorkerSubscriber(database, ingress.Object);

        await subscriber.StartAsync(CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.True(database.ReadGroupCalls.Count >= 2);
        ingress.Verify(i => i.HandleWorkerMessageAsync("worker-json"), Times.Once);
    }

    [Fact]
    public async Task WorkerSubscriber_FreshEntryMissingPayload_DeadLettersAndContinues()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        database.ReadBatches.Enqueue(
        [
            RedisTransportTests.Entry("1-0", ("correlationId", "missing-payload"))
        ]);
        database.ReadBatches.Enqueue(
        [
            RedisTransportTests.Entry("2-0", ("payload", "worker-json"), ("correlationId", "corr-worker"))
        ]);
        var ingress = new Mock<IAsyncResponseIngress>();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json"))
            .Returns(() =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            });
        var subscriber = WorkerSubscriber(database, ingress.Object);

        await subscriber.StartAsync(CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        // The payload-less entry is dead-lettered and ACKed (drained) rather than left to wedge the loop.
        Assert.Contains(database.Acks, ack => ack.MessageId == "1-0");
        Assert.Contains(
            database.Adds,
            add => RedisTransportTests.Field(add.Values, "reason") == "unparsable_entry"
                && RedisTransportTests.Field(add.Values, "messageId") == "1-0");
        // ...and the next valid message is still handled.
        Assert.Contains(database.Acks, ack => ack.MessageId == "2-0");
        ingress.Verify(i => i.HandleWorkerMessageAsync("worker-json"), Times.Once);
    }

    [Fact]
    public async Task WorkerSubscriber_ClaimedEntryMissingPayload_DeadLettersInsteadOfWedging()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            PendingMessages =
            [
                Pending("1-0", "old-consumer", 500, 1)
            ],
            ClaimedMessages =
            [
                RedisTransportTests.Entry("1-0", ("correlationId", "no-payload"))
            ]
        };
        var ingress = new Mock<IAsyncResponseIngress>();
        var subscriber = WorkerSubscriber(
            database,
            ingress.Object,
            options => options.WorkerSubscriber.PendingMessageMinIdleTime = TimeSpan.FromMilliseconds(10));

        await subscriber.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => database.Adds.Count >= 1);
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Equal("unparsable_entry", RedisTransportTests.Field(Assert.Single(database.Adds).Values, "reason"));
        Assert.Contains(database.Acks, ack => ack.MessageId == "1-0");
        ingress.Verify(i => i.HandleWorkerMessageAsync(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task WorkerSubscriber_ClaimsPendingEntriesAndDeadLettersExceededAttempts()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            PendingMessages =
            [
                Pending("1-0", "old-consumer", 500, 5)
            ],
            ClaimedMessages =
            [
                RedisTransportTests.Entry("1-0", ("payload", "worker-json"), ("correlationId", "corr-worker"))
            ]
        };
        var ingress = new Mock<IAsyncResponseIngress>();
        var subscriber = WorkerSubscriber(
            database,
            ingress.Object,
            options =>
            {
                options.WorkerSubscriber.MaxDeliveryAttempts = 5;
                options.WorkerSubscriber.PendingMessageMinIdleTime = TimeSpan.FromMilliseconds(10);
                options.WorkerSubscriber.PendingClaimBatchSize = 3;
                options.WorkerSubscriber.PendingClaimInterval = TimeSpan.FromSeconds(30);
            });

        await subscriber.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => database.Adds.Count == 1);
        await subscriber.StopAsync(CancellationToken.None);

        var pending = Assert.Single(database.PendingCalls);
        Assert.Equal(3, pending.Count);
        Assert.Equal(10, pending.MinIdleTimeInMilliseconds);
        var claim = Assert.Single(database.ClaimCalls);
        Assert.Equal(["1-0"], claim.MessageIds);
        Assert.Equal("max_delivery_attempts_exceeded", RedisTransportTests.Field(Assert.Single(database.Adds).Values, "reason"));
        ingress.Verify(i => i.HandleWorkerMessageAsync(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task WorkerSubscriber_ClaimedEntryMissingFromPendingSummary_UsesFirstRetryAttempt()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase
        {
            PendingMessages =
            [
                Pending("1-0", "old-consumer", 500, 3)
            ],
            ClaimedMessages =
            [
                RedisTransportTests.Entry("2-0", ("payload", "worker-json"), ("correlationId", "corr-worker"))
            ]
        };
        var ingress = new Mock<IAsyncResponseIngress>();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json"))
            .Returns(() =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            });
        var subscriber = WorkerSubscriber(database, ingress.Object);

        await subscriber.StartAsync(CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Contains(database.Acks, ack => ack.MessageId == "2-0");
        ingress.Verify(i => i.HandleWorkerMessageAsync("worker-json"), Times.Once);
    }

    [Fact]
    public async Task WorkerSubscriber_UsesConfiguredConsumerName()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        database.ReadBatches.Enqueue(
        [
            RedisTransportTests.Entry("1-0", ("payload", "worker-json"), ("correlationId", "corr-worker"))
        ]);
        var ingress = new Mock<IAsyncResponseIngress>();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json"))
            .Returns(() =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            });
        var subscriber = WorkerSubscriber(
            database,
            ingress.Object,
            options => options.ConsumerName = "configured-consumer");

        await subscriber.StartAsync(CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Contains(database.ReadGroupCalls, call => call.Consumer == "configured-consumer-worker");
    }

    [Fact]
    public async Task SubscriberBase_WhenLoopExitsAfterCancellation_ReturnsCleanly()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        database.ReadBatches.Enqueue(
        [
            RedisTransportTests.Entry("1-0", ("payload", "worker-json"), ("correlationId", "corr-worker"))
        ]);
        using var cts = new CancellationTokenSource();
        var subscriber = new ProbeSubscriber(
            Options.Create(new RedisAsyncResponseTransportOptions
            {
                WorkerStream = "workers",
                WorkerConsumerGroup = "workers-group",
                CreateConsumerGroups = false,
                WorkerSubscriber =
                {
                    PendingClaimInterval = TimeSpan.FromSeconds(30),
                    EmptyPollDelay = TimeSpan.FromMilliseconds(1)
                }
            }),
            database,
            _ =>
            {
                cts.Cancel();
                return Task.CompletedTask;
            });

        await subscriber.RunAsync(cts.Token);

        Assert.Contains(database.Acks, ack => ack.MessageId == "1-0");
    }

    [Fact]
    public void TrimConsumerName_TruncatesOnlyWhenRedisConsumerNameWouldBeTooLong()
    {
        Assert.Equal("short-consumer", RedisSubscriberService.TrimConsumerName("short-consumer"));
        Assert.Equal(64, RedisSubscriberService.TrimConsumerName(new string('x', 65)).Length);
    }

    [Fact]
    public async Task WorkerSubscriber_SlowBatch_RefreshesPendingIdleOfUnprocessedEntries_ThenStops()
    {
        // Red-on-old: XREADGROUP stamps every batch entry as delivered at read time and nothing
        // refreshed the idle clock while the batch dispatched serially — a handler slower than
        // PendingMessageMinIdleTime / BatchSize let a sibling's pending-claim scan steal the
        // batch tail, re-run it concurrently, and bump its PEL delivery count toward the
        // dead-letter cap on work that never once failed.
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        database.ReadBatches.Enqueue(
        [
            RedisTransportTests.Entry("1-0", ("payload", "p1"), ("correlationId", "c1")),
            RedisTransportTests.Entry("2-0", ("payload", "p2"), ("correlationId", "c2"))
        ]);
        var ingress = new GatedIngress();
        var subscriber = WorkerSubscriber(
            database,
            ingress,
            options =>
            {
                // Cadence is PendingMessageMinIdleTime / 3 = 30ms; the pending-claim scan is
                // parked so only the heartbeat touches the claim APIs.
                options.WorkerSubscriber.PendingMessageMinIdleTime = TimeSpan.FromMilliseconds(90);
                options.WorkerSubscriber.PendingClaimInterval = TimeSpan.FromSeconds(30);
            });

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Started("p1").WaitAsync(TimeSpan.FromSeconds(5)); // 1-0 is wedged in the handler

            // While 1-0 sits in the handler and 2-0 waits its turn, the heartbeat JUSTID-claims
            // BOTH back to this consumer with minIdle 0 (reset idle, never bump delivery count).
            await WaitUntilAsync(() => ClaimedIdSets(database).Any(ids => ids.SequenceEqual(["1-0", "2-0"])));
            lock (database.ClaimIdsOnlyCalls)
            {
                var call = database.ClaimIdsOnlyCalls[0];
                Assert.Equal(0, call.MinIdleTimeInMilliseconds);
                Assert.Equal(database.ReadGroupCalls[0].Consumer, call.Consumer);
            }

            ingress.Release("p1");
            await ingress.Started("p2").WaitAsync(TimeSpan.FromSeconds(5)); // 2-0 is now in the handler

            // 1-0 settled: the sweep shrinks to exactly the unprocessed suffix.
            await WaitUntilAsync(() => ClaimedIdSets(database).Any(ids => ids.SequenceEqual(["2-0"])));

            ingress.Release("p2");
            await WaitUntilAsync(() => database.Acks.Count(ack => ack.MessageId is "1-0" or "2-0") == 2);

            // The heartbeat dies with the batch: no further claims after everything settled.
            int claimsAfterBatch;
            lock (database.ClaimIdsOnlyCalls)
            {
                claimsAfterBatch = database.ClaimIdsOnlyCalls.Count;
            }

            await Task.Delay(150);
            lock (database.ClaimIdsOnlyCalls)
            {
                Assert.Equal(claimsAfterBatch, database.ClaimIdsOnlyCalls.Count);
            }
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task WorkerSubscriber_FastBatch_DoesNotTouchTheClaimHeartbeat()
    {
        // Default PendingMessageMinIdleTime (30s) puts the first sweep at ~10s: a batch settled
        // quickly must never pay a claim round trip.
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        database.ReadBatches.Enqueue(
        [
            RedisTransportTests.Entry("1-0", ("payload", "fast"), ("correlationId", "c1"))
        ]);
        var ingress = new Mock<IAsyncResponseIngress>();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ingress.Setup(i => i.HandleWorkerMessageAsync("fast"))
            .Returns(() =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            });
        var subscriber = WorkerSubscriber(
            database,
            ingress.Object,
            options => options.WorkerSubscriber.PendingMessageMinIdleTime = TimeSpan.FromSeconds(30));

        await subscriber.StartAsync(CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await WaitUntilAsync(() => database.Acks.Any(ack => ack.MessageId == "1-0"));
        await subscriber.StopAsync(CancellationToken.None);

        lock (database.ClaimIdsOnlyCalls)
        {
            Assert.Empty(database.ClaimIdsOnlyCalls);
        }
    }

    private static List<string[]> ClaimedIdSets(RedisTransportTests.FakeRedisStreamDatabase database)
    {
        lock (database.ClaimIdsOnlyCalls)
        {
            return database.ClaimIdsOnlyCalls.Select(call => call.MessageIds).ToList();
        }
    }

    /// <summary>Wedges each worker message in the handler until its payload is released.</summary>
    private sealed class GatedIngress : IAsyncResponseIngress
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, TaskCompletionSource> _started = new(StringComparer.Ordinal);
        private readonly Dictionary<string, TaskCompletionSource> _release = new(StringComparer.Ordinal);

        public Task Started(string payload) => Gate(_started, payload).Task;

        public void Release(string payload) => Gate(_release, payload).TrySetResult();

        public async Task HandleWorkerMessageAsync(string messageJson)
        {
            Gate(_started, messageJson).TrySetResult();
            await Gate(_release, messageJson).Task;
        }

        public Task HandleResponseMessageAsync(string messageJson, string? correlationId = null)
            => Task.CompletedTask;

        private TaskCompletionSource Gate(Dictionary<string, TaskCompletionSource> gates, string payload)
        {
            lock (_gate)
            {
                if (!gates.TryGetValue(payload, out var source))
                {
                    source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    gates[payload] = source;
                }

                return source;
            }
        }
    }

    private static RedisWorkerSubscriber WorkerSubscriber(
        RedisTransportTests.FakeRedisStreamDatabase database,
        IAsyncResponseIngress ingress,
        Action<RedisAsyncResponseTransportOptions>? configure = null)
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
                PendingMessageMinIdleTime = TimeSpan.FromMilliseconds(1)
            }
        };
        configure?.Invoke(options);
        return new RedisWorkerSubscriber(
            Options.Create(options),
            database,
            ingress,
            NullLogger<RedisWorkerSubscriber>.Instance);
    }

    private static StreamPendingMessageInfo Pending(
        RedisValue messageId,
        RedisValue consumerName,
        long idleTimeInMilliseconds,
        int deliveryCount)
        => (StreamPendingMessageInfo)PendingConstructor.Invoke(
            [messageId, consumerName, idleTimeInMilliseconds, deliveryCount]);

    private static readonly ConstructorInfo PendingConstructor =
        typeof(StreamPendingMessageInfo).GetConstructor(
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            [typeof(RedisValue), typeof(RedisValue), typeof(long), typeof(int)],
            modifiers: null)
        ?? throw new InvalidOperationException("StreamPendingMessageInfo constructor was not found.");

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException("Condition was not satisfied before the timeout.");

            await Task.Delay(10);
        }
    }

    private sealed class ProbeSubscriber(
        IOptions<RedisAsyncResponseTransportOptions> options,
        RedisTransportTests.FakeRedisStreamDatabase database,
        Func<RedisStreamDelivery, Task> handler)
        : RedisSubscriberService(options, database, NullLogger.Instance)
    {
        protected override RedisKey Stream => "workers";
        protected override RedisValue ConsumerGroup => "workers-group";
        protected override RedisSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
        protected override RedisSubscriberRole SubscriberRole => RedisSubscriberRole.Worker;

        public Task RunAsync(CancellationToken cancellationToken) => ExecuteAsync(cancellationToken);

        protected override Task HandleMessageAsync(
            RedisStreamDelivery delivery,
            CancellationToken cancellationToken)
            => handler(delivery);
    }

    [Fact]
    public async Task WorkerSubscriber_InvalidOptions_FailHostStartupSynchronously()
    {
        // Red-on-old (Hosting 10.0.10+): validation used to sit at the top of ExecuteAsync, which
        // BackgroundService.StartAsync no longer runs inline — StartAsync returned without
        // throwing and the misconfiguration surfaced late or never. Validation now runs in
        // StartAsync so a misconfigured subscriber fails host startup synchronously.
        var subscriber = new RedisWorkerSubscriber(
            Options.Create(new RedisAsyncResponseTransportOptions
            {
                WorkerStream = "workers",
                WorkerConsumerGroup = "workers-group",
                WorkerSubscriber = { AckMode = RedisAckMode.AckAfterEnqueue }
            }),
            new RedisTransportTests.FakeRedisStreamDatabase(),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<RedisWorkerSubscriber>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.StartAsync(CancellationToken.None));
        Assert.Contains("BackgroundWorkerCount", ex.Message, StringComparison.Ordinal);
    }
}
