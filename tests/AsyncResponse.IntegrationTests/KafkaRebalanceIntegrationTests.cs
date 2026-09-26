using System.Collections.Concurrent;
using System.Text;
using AsyncResponse.Transports.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Round 1 (G10, S7#6): the real broker's answer to a detached ack-after-handler handler that
/// outlives its partition's assignment. The package leaves librdkafka's default assignor in place
/// ("range,roundrobin", the EAGER protocol: every rebalance revokes every partition), and the range
/// assignor orders members by member id — client id first — so the client ids decide who holds the
/// single partition. Two facts the unit fakes had wrong or could not show: librdkafka keeps an
/// application pause on a partition across a revoke and a re-assignment (the consumer adapter now
/// resumes what a rebalance removes), and an eager rebalance usually hands a partition straight back
/// to the member that was running a message of it.
/// </summary>
[Collection(BrokersCollection.Name)]
[Trait(Batches.Trait, Batches.Brokers)]
public sealed class KafkaRebalanceIntegrationTests(BrokersBatchFixture fixture) : IntegrationTestBase(fixture)
{
    private static readonly ILogger Logger = NullLogger.Instance;

    [Fact]
    public async Task AckAfterHandler_AHandlerThatOutlivesItsPartitionsAssignment_NeitherStallsNorRewindsTheGroup()
    {
        // Member C1 detaches P0@0; member C2 joins, takes P0 (its client id sorts first), re-consumes
        // and commits the whole partition, and leaves; P0 comes back to C1. The fixed dispatcher
        // neither holds the new assignment's messages behind the stale handler nor stores that
        // handler's offset when it finally settles — librdkafka accepts a store for a partition
        // that is assigned AGAIN, and Kafka's offset commit is not monotonic, so the old code
        // rewound the group to 1. And P0 must come back FETCHING: librdkafka kept C1's pause from
        // before the revoke, so nothing of the new assignment ever arrived (the stale handler,
        // an orphan, never resumes it either).
        var prefix = NewId("r1-rebalance");
        var group = $"{prefix}-workers";
        var c1Options = Options(prefix, group, clientId: $"zz-{prefix}");
        var c2Options = Options(prefix, group, clientId: $"aa-{prefix}");
        var topic = new KafkaTransportTopicSchema(c1Options).WorkerTopic;
        using var producer = await CreateTopicWithMessagesAsync(c1Options, topic, count: 6);

        var releaseStale = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var c1Handled = new ConcurrentQueue<long>();
        var c2Handled = new ConcurrentQueue<long>();

        using var c1 = new KafkaConsumerClientFactory(c1Options).Create(KafkaSubscriberRole.Worker);
        c1.Subscribe(topic);
        var c1Dispatcher = CreateDispatcher(
            async (delivery, _) =>
            {
                if (delivery.Offset == 0)
                    await releaseStale.Task.ConfigureAwait(false);
                c1Handled.Enqueue(delivery.Offset);
            },
            c1,
            producer,
            c1Options,
            topic,
            group);

        using var stopC1 = new CancellationTokenSource();
        var c1Loop = Task.Factory.StartNew(() => PollLoop(c1, c1Dispatcher, stopC1.Token), TaskCreationOptions.LongRunning);
        try
        {
            // C1 detaches P0@0 (the stale handler) and pauses the partition behind it.
            await PollUntilAsync(() => c1Dispatcher.HasDetachedWork, "C1 never detached P0@0");

            // C2 joins: the rebalance revokes P0 from C1 and hands it to C2, which re-consumes the
            // partition from the (still empty) committed position and commits past all six.
            using (var c2 = new KafkaConsumerClientFactory(c2Options).Create(KafkaSubscriberRole.Worker))
            {
                c2.Subscribe(topic);
                var c2Dispatcher = CreateDispatcher(
                    (delivery, _) =>
                    {
                        c2Handled.Enqueue(delivery.Offset);
                        return Task.CompletedTask;
                    },
                    c2,
                    producer,
                    c2Options,
                    topic,
                    group);

                using var stopC2 = new CancellationTokenSource();
                var c2Loop = Task.Factory.StartNew(() => PollLoop(c2, c2Dispatcher, stopC2.Token), TaskCreationOptions.LongRunning);
                await PollUntilAsync(() => c2Handled.Contains(5), "C2 never took over and consumed P0");
                Assert.True(c1.GetAssignmentGeneration(topic, 0) > 0, "the revoke never reached C1's rebalance handler");

                stopC2.Cancel();
                await c2Loop;
                await c2Dispatcher.DisposeAsync();
                c2.Close(); // commits 6 and leaves: P0 goes back to C1
            }

            // A message of the NEW assignment must not wait behind the stale handler.
            await producer.PublishAsync(topic, key: null, Encoding.UTF8.GetBytes("m6"), [], CancellationToken.None);
            await PollUntilAsync(() => c1Handled.Contains(6), "P0@6 was held behind the stale P0@0 handler");

            // The stale handler settles only now — its offset must not be stored.
            releaseStale.SetResult();
            await PollUntilAsync(() => c1Handled.Contains(0), "the stale handler never finished");
            await Task.Delay(TimeSpan.FromSeconds(1)); // a few settlement ticks
        }
        finally
        {
            releaseStale.TrySetResult();
            stopC1.Cancel();
            await c1Loop;
            await c1Dispatcher.DisposeAsync();
            c1.Close();
        }

        Assert.Equal(7, CommittedOffset(c1Options, group, topic));
    }

    [Fact]
    public async Task AckAfterHandler_APartitionHandedStraightBackByAnEagerRebalance_NeverRunsTheRunningMessageTwiceAtOnce()
    {
        // r1 critic H1: C1 (its client id sorts first) holds P0 and detaches P0@0. C2 joins; the
        // eager protocol revokes P0 from C1 and the range assignor hands it straight back to C1,
        // which re-fetches from the group's committed offset — P0@0 itself, still running, its
        // offset not stored yet. That copy must wait behind the running handler (and be dropped once
        // the handler has settled the offset), not start alongside it; and P0 must not stall.
        var prefix = NewId("r1-handback");
        var group = $"{prefix}-workers";
        var c1Options = Options(prefix, group, clientId: $"aa-{prefix}");
        var c2Options = Options(prefix, group, clientId: $"zz-{prefix}");
        var topic = new KafkaTransportTopicSchema(c1Options).WorkerTopic;
        using var producer = await CreateTopicWithMessagesAsync(c1Options, topic, count: 6);

        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var c1Consumed = new ConcurrentQueue<long>();
        var c1Handled = new ConcurrentQueue<long>();
        var c2Handled = new ConcurrentQueue<long>();
        var runningFirst = 0;
        var maxConcurrentFirst = 0;

        using var c1 = new KafkaConsumerClientFactory(c1Options).Create(KafkaSubscriberRole.Worker);
        c1.Subscribe(topic);
        var c1Dispatcher = CreateDispatcher(
            async (delivery, _) =>
            {
                if (delivery.Offset == 0)
                {
                    var now = Interlocked.Increment(ref runningFirst);
                    int seen;
                    while (now > (seen = Volatile.Read(ref maxConcurrentFirst)) && Interlocked.CompareExchange(ref maxConcurrentFirst, now, seen) != seen)
                    {
                    }

                    try
                    {
                        await release.Task.ConfigureAwait(false);
                    }
                    finally
                    {
                        Interlocked.Decrement(ref runningFirst);
                    }
                }

                c1Handled.Enqueue(delivery.Offset);
            },
            c1,
            producer,
            c1Options,
            topic,
            group);

        using var stopC1 = new CancellationTokenSource();
        var c1Loop = Task.Factory.StartNew(() => PollLoop(c1, c1Dispatcher, stopC1.Token, c1Consumed), TaskCreationOptions.LongRunning);
        try
        {
            await PollUntilAsync(() => c1Dispatcher.HasDetachedWork, "C1 never detached P0@0");

            using (var c2 = new KafkaConsumerClientFactory(c2Options).Create(KafkaSubscriberRole.Worker))
            {
                c2.Subscribe(topic);
                var c2Dispatcher = CreateDispatcher(
                    (delivery, _) =>
                    {
                        c2Handled.Enqueue(delivery.Offset);
                        return Task.CompletedTask;
                    },
                    c2,
                    producer,
                    c2Options,
                    topic,
                    group);

                using var stopC2 = new CancellationTokenSource();
                var c2Loop = Task.Factory.StartNew(() => PollLoop(c2, c2Dispatcher, stopC2.Token), TaskCreationOptions.LongRunning);
                try
                {
                    // The rebalance reached C1 and P0 came straight back FETCHING: the copy of the
                    // running message was consumed again (a partition still paused from before the
                    // revoke never fetches it — the partition stalls instead).
                    await PollUntilAsync(
                        () => c1.GetAssignmentGeneration(topic, 0) > 0 && c1Consumed.Count(offset => offset == 0) >= 2,
                        "P0 never came back to C1 fetching from the running message");
                    await Task.Delay(TimeSpan.FromSeconds(1)); // time for a copy started by mistake to be running
                    Assert.Equal(1, Volatile.Read(ref maxConcurrentFirst));

                    release.SetResult();
                    await PollUntilAsync(() => Enumerable.Range(1, 5).All(offset => c1Handled.Contains(offset)), "P0 stalled after the running message settled");

                    Assert.Equal(1, c1Handled.Count(offset => offset == 0));
                    Assert.Equal(1, Volatile.Read(ref maxConcurrentFirst));
                    Assert.Empty(c2Handled); // the single partition stayed with C1 throughout
                }
                finally
                {
                    release.TrySetResult();
                    stopC2.Cancel();
                    await c2Loop;
                    await c2Dispatcher.DisposeAsync();
                    c2.Close();
                }
            }
        }
        finally
        {
            release.TrySetResult();
            stopC1.Cancel();
            await c1Loop;
            await c1Dispatcher.DisposeAsync();
            c1.Close();
        }

        Assert.Equal(6, CommittedOffset(c1Options, group, topic));
    }

    [Fact]
    public async Task AckAfterEnqueue_ABackpressurePauseLiftedWhileAnEagerRebalanceHadThePartitionAway_DoesNotStallIt()
    {
        // Round 2 (r2 S7#1): the early-ACK poll loop pauses the whole assignment while its queue is
        // full. C2 joins; the eager protocol revokes P0 from C1 — librdkafka keeps C1's pause on it
        // — and, while the group re-joins, nothing is assigned to C1. A queue slot frees in that
        // window: the loop's resume covers the EMPTY assignment. P0 then comes straight back to C1
        // (its client id sorts first) and must come back FETCHING: before the fix it stayed paused
        // for the life of the consumer, since nothing was fetched to fill the queue and trip
        // another pause-and-resume.
        var prefix = NewId("r2-backpressure");
        var group = $"{prefix}-workers";
        var c1Options = Options(prefix, group, clientId: $"aa-{prefix}");
        var c2Options = Options(prefix, group, clientId: $"zz-{prefix}");
        var topic = new KafkaTransportTopicSchema(c1Options).WorkerTopic;
        using var producer = await CreateTopicWithMessagesAsync(c1Options, topic, count: 3);

        // Built exactly as KafkaConsumerClientFactory builds it, keeping the raw consumer at hand to
        // see its assignment.
        KafkaConsumerClientAdapter? c1 = null;
        using var raw = new ConsumerBuilder<string?, byte[]>(KafkaConsumerClientFactory.BuildConfig(c1Options, KafkaSubscriberRole.Worker))
            .SetPartitionsAssignedHandler((_, assigned) => c1!.OnPartitionsAssigned(assigned))
            .SetPartitionsRevokedHandler((_, revoked) => c1!.OnPartitionsRemoved(revoked))
            .SetPartitionsLostHandler((_, lost) => c1!.OnPartitionsRemoved(lost))
            .Build();
        c1 = new KafkaConsumerClientAdapter(raw);
        c1.Subscribe(topic);

        using var stopC2 = new CancellationTokenSource();
        Task? c2Loop = null;
        IKafkaConsumerClient? c2 = null;
        try
        {
            // C1 owns P0 and is fetching; then its queue fills and the loop pauses the assignment.
            await PollUntilAsync(() => c1.Consume(TimeSpan.FromMilliseconds(100)) is not null, "C1 never consumed P0");
            c1.PauseAssignment();

            c2 = new KafkaConsumerClientFactory(c2Options).Create(KafkaSubscriberRole.Worker);
            c2.Subscribe(topic);
            var c2Consumer = c2;
            c2Loop = Task.Factory.StartNew(
                () =>
                {
                    while (!stopC2.IsCancellationRequested)
                        c2Consumer.Consume(TimeSpan.FromMilliseconds(100));
                },
                TaskCreationOptions.LongRunning);

            // C1 serves the eager revoke. Polled with a zero wait, so the re-assignment — which needs
            // the re-join round trips after the revoke callback returned — cannot be served in the
            // same poll: C1 is left with nothing assigned.
            await PollUntilAsync(
                () =>
                {
                    c1.Consume(TimeSpan.Zero);
                    return c1.GetAssignmentGeneration(topic, 0) > 0;
                },
                "the rebalance never revoked P0 from C1");
            Assert.Empty(raw.Assignment);

            // A queue slot frees while nothing is assigned.
            c1.ResumeAssignment();

            // P0 comes back to C1, and something of it must be fetched again.
            await PollUntilAsync(
                () => c1.Consume(TimeSpan.FromMilliseconds(100)) is not null,
                "P0 came back to C1 still paused by the backpressure pause lifted while it was away");
        }
        finally
        {
            stopC2.Cancel();
            if (c2Loop is not null)
                await c2Loop;

            c2?.Close();
            c2?.Dispose();
            c1.Close();
        }
    }

    private KafkaAsyncResponseTransportOptions Options(string prefix, string group, string clientId)
    {
        var options = new KafkaAsyncResponseTransportOptions
        {
            BootstrapServers = Fixture.KafkaBootstrapServers!,
            TopicPrefix = prefix,
            WorkerConsumerGroup = group,
            ResponseConsumerGroup = $"{prefix}-responses",
            ClientId = clientId,
            DeadLetterEnabled = false,
            OffsetCommitInterval = TimeSpan.FromMilliseconds(200),
            ConfigureConsumer = config =>
            {
                config.SessionTimeoutMs = 6000;
                config.HeartbeatIntervalMs = 2000;
            }
        };
        options.WorkerSubscriber.MaxPollInterval = TimeSpan.FromSeconds(10);
        options.WorkerSubscriber.PollTimeout = TimeSpan.FromMilliseconds(100);
        options.WorkerSubscriber.DetachHandlerAfter = TimeSpan.Zero;
        return options;
    }

    private static async Task<KafkaProducerClientAdapter> CreateTopicWithMessagesAsync(
        KafkaAsyncResponseTransportOptions options,
        string topic,
        int count)
    {
        await new KafkaAdminClientAdapter(options).EnsureTopicsAsync([topic], numPartitions: 1, replicationFactor: -1, CancellationToken.None);
        var producer = new KafkaProducerClientAdapter(options);
        for (var i = 0; i < count; i++)
            await producer.PublishAsync(topic, key: null, Encoding.UTF8.GetBytes($"m{i}"), [], CancellationToken.None);

        return producer;
    }

    private static KafkaMessageDispatcher CreateDispatcher(
        Func<KafkaDelivery, CancellationToken, Task> handler,
        IKafkaConsumerClient consumer,
        IKafkaProducerClient producer,
        KafkaAsyncResponseTransportOptions options,
        string topic,
        string group)
        => KafkaMessageDispatcher.Create(
            handler,
            consumer,
            producer,
            options,
            options.WorkerSubscriber,
            Logger,
            topic,
            group,
            KafkaSubscriberRole.Worker);

    /// <summary>The subscriber's poll loop in miniature: settle finished detached handlers, poll, accept.</summary>
    private static void PollLoop(
        IKafkaConsumerClient consumer,
        KafkaMessageDispatcher dispatcher,
        CancellationToken stop,
        ConcurrentQueue<long>? consumed = null)
    {
        while (!stop.IsCancellationRequested)
        {
            dispatcher.SettleCompleted();
            var message = consumer.Consume(TimeSpan.FromMilliseconds(100));
            if (message is null)
                continue;

            consumed?.Enqueue(message.Offset);
            dispatcher.Accept(
                new KafkaDelivery(message.Topic, message.Partition, message.Offset, Encoding.UTF8.GetString(message.Payload!), null, message.Headers),
                CancellationToken.None);
        }
    }

    private static async Task PollUntilAsync(Func<bool> condition, string failure)
    {
        var reached = await PollAsync(() => Task.FromResult(condition()), done => done, TimeSpan.FromSeconds(60));
        Assert.True(reached, failure);
    }

    private static long CommittedOffset(KafkaAsyncResponseTransportOptions options, string group, string topic)
    {
        using var probe = new ConsumerBuilder<string?, byte[]>(new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = group,
            EnableAutoCommit = false
        }).Build();

        return probe.Committed([new TopicPartition(topic, new Partition(0))], TimeSpan.FromSeconds(10)).Single().Offset.Value;
    }
}
