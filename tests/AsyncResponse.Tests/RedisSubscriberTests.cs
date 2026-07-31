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
}
