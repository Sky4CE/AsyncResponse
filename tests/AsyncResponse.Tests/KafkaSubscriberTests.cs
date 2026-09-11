using AsyncResponse.Transports.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

public class KafkaSubscriberTests
{
    [Fact]
    public async Task WorkerSubscriber_ForwardsPayloadAndStoresOffset()
    {
        var consumer = new FakeKafkaConsumerClient();
        consumer.Enqueue(KafkaTestData.Message("workers", offset: 5, payload: "worker-json", ("correlationId", "corr-worker")));

        var ingress = new Mock<IAsyncResponseIngress>();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json"))
            .Returns(() =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            });

        var subscriber = CreateWorkerSubscriber(consumer, ingress.Object, options =>
        {
            options.WorkerTopic = "workers";
            options.WorkerConsumerGroup = "workers-group";
        });

        await subscriber.StartAsync(CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await KafkaTestData.WaitUntilAsync(() => consumer.StoredOffsets.Count == 1);
        await subscriber.StopAsync(CancellationToken.None);

        ingress.Verify(i => i.HandleWorkerMessageAsync("worker-json"), Times.Once);
        Assert.Equal("workers", Assert.Single(consumer.Subscriptions));
        Assert.Equal(new FakeKafkaConsumerClient.StoredOffset("workers", 0, 5), Assert.Single(consumer.StoredOffsets));
        Assert.True(consumer.Closed);
        Assert.True(consumer.Disposed);
    }

    [Fact]
    public async Task ResponseSubscriber_ForwardsPayloadWithHeaderCorrelation()
    {
        var consumer = new FakeKafkaConsumerClient();
        consumer.Enqueue(KafkaTestData.Message("responses", offset: 1, payload: """{"State":"ok"}""", ("correlationId", "corr-header")));

        var ingress = new Mock<IAsyncResponseIngress>();
        var handled = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        ingress.Setup(i => i.HandleResponseMessageAsync(It.IsAny<string>(), It.IsAny<string?>()))
            .Returns((string _, string? correlationId) =>
            {
                handled.TrySetResult(correlationId);
                return Task.CompletedTask;
            });

        var subscriber = CreateResponseSubscriber(consumer, ingress.Object, options => options.ResponseTopic = "responses");

        await subscriber.StartAsync(CancellationToken.None);
        Assert.Equal("corr-header", await handled.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        await subscriber.StopAsync(CancellationToken.None);

        ingress.Verify(i => i.HandleResponseMessageAsync("""{"State":"ok"}""", "corr-header"), Times.Once);
        Assert.Equal(KafkaSubscriberRole.ResponseIngress, Assert.Single(FactoryOf(subscriber).CreatedRoles));
    }

    [Fact]
    public async Task ResponseSubscriber_FallsBackToJsonBodyCorrelation()
    {
        var consumer = new FakeKafkaConsumerClient();
        consumer.Enqueue(KafkaTestData.Message("responses", offset: 1, payload: """{"CorrelationId":"corr-body"}"""));

        var ingress = new Mock<IAsyncResponseIngress>();
        var handled = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        ingress.Setup(i => i.HandleResponseMessageAsync(It.IsAny<string>(), It.IsAny<string?>()))
            .Returns((string _, string? correlationId) =>
            {
                handled.TrySetResult(correlationId);
                return Task.CompletedTask;
            });

        var subscriber = CreateResponseSubscriber(consumer, ingress.Object, options => options.ResponseTopic = "responses");

        await subscriber.StartAsync(CancellationToken.None);
        Assert.Equal("corr-body", await handled.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        await subscriber.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task WorkerSubscriber_EmptyPayload_DeadLettersWithoutInvokingIngress()
    {
        var consumer = new FakeKafkaConsumerClient();
        consumer.Enqueue(KafkaTestData.Message("workers", offset: 2, payload: "", ("correlationId", "corr-x")));

        var ingress = new Mock<IAsyncResponseIngress>();
        var producer = new FakeKafkaProducerClient();
        var subscriber = CreateWorkerSubscriber(
            consumer,
            ingress.Object,
            options => options.WorkerTopic = "workers",
            producer);

        await subscriber.StartAsync(CancellationToken.None);
        await KafkaTestData.WaitUntilAsync(() => producer.Publishes.Count == 1 && consumer.StoredOffsets.Count == 1);
        await subscriber.StopAsync(CancellationToken.None);

        ingress.Verify(i => i.HandleWorkerMessageAsync(It.IsAny<string>()), Times.Never);
        var dead = Assert.Single(producer.Publishes);
        Assert.Equal("workers.deadletter", dead.Topic);
        Assert.Equal("unprocessable_message", FakeKafkaProducerClient.Header(dead.Headers, "reason"));
        Assert.Equal(2, Assert.Single(consumer.StoredOffsets).Offset);
    }

    [Fact]
    public async Task Subscriber_CreateTopics_ProvisionsTopicAndDeadLetterTopic()
    {
        var consumer = new FakeKafkaConsumerClient();
        var adminClient = new FakeKafkaAdminClient();
        var subscriber = CreateWorkerSubscriber(
            consumer,
            Mock.Of<IAsyncResponseIngress>(),
            options =>
            {
                options.WorkerTopic = "workers";
                options.TopicNumPartitions = 4;
            },
            adminClient: adminClient);

        await subscriber.StartAsync(CancellationToken.None);
        await KafkaTestData.WaitUntilAsync(() => adminClient.EnsureTopicsCalls.Count == 1);
        await subscriber.StopAsync(CancellationToken.None);

        var call = Assert.Single(adminClient.EnsureTopicsCalls);
        Assert.Equal(["workers", "workers.deadletter"], call.Topics);
        Assert.Equal(4, call.NumPartitions);
        Assert.Equal(-1, call.ReplicationFactor);
    }

    [Fact]
    public async Task Subscriber_CreateTopicsDisabled_SkipsProvisioning()
    {
        var consumer = new FakeKafkaConsumerClient();
        var adminClient = new FakeKafkaAdminClient();
        var subscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        consumer.Enqueue(KafkaTestData.Message("workers", offset: 1, payload: "{}"));
        ingress.Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(() =>
            {
                subscribed.TrySetResult();
                return Task.CompletedTask;
            });

        var subscriber = CreateWorkerSubscriber(
            consumer,
            ingress.Object,
            options =>
            {
                options.WorkerTopic = "workers";
                options.CreateTopics = false;
            },
            adminClient: adminClient);

        await subscriber.StartAsync(CancellationToken.None);
        await subscribed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Empty(adminClient.EnsureTopicsCalls);
    }

    [Fact]
    public async Task Subscriber_CloseFailure_StillDisposesConsumer()
    {
        var consumer = new FakeKafkaConsumerClient
        {
            CloseException = new InvalidOperationException("close failed")
        };
        var subscriber = CreateWorkerSubscriber(
            consumer,
            Mock.Of<IAsyncResponseIngress>(),
            options => options.WorkerTopic = "workers");

        await subscriber.StartAsync(CancellationToken.None);
        await KafkaTestData.WaitUntilAsync(() => consumer.Subscriptions.Count == 1);
        await subscriber.StopAsync(CancellationToken.None);

        Assert.True(consumer.Closed);
        Assert.True(consumer.Disposed);
    }

    [Fact]
    public async Task Subscriber_RestartsWithFreshConsumerAfterConsumeFailure()
    {
        var failingConsumer = new FakeKafkaConsumerClient
        {
            NextConsumeException = new InvalidOperationException("broker hiccup")
        };
        var recoveredConsumer = new FakeKafkaConsumerClient();
        recoveredConsumer.Enqueue(KafkaTestData.Message("workers", offset: 1, payload: "worker-json"));

        var ingress = new Mock<IAsyncResponseIngress>();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json"))
            .Returns(() =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            });

        var factory = new FakeKafkaConsumerClientFactory(failingConsumer, recoveredConsumer);
        var subscriber = new KafkaWorkerSubscriber(
            Options.Create(NewOptions(options => options.WorkerTopic = "workers")),
            factory,
            new FakeKafkaProducerClient(),
            new FakeKafkaAdminClient(),
            ingress.Object,
            NullLogger<KafkaWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Equal(2, factory.CreatedRoles.Count);
        Assert.True(failingConsumer.Closed); // the failed consumer left the group cleanly
        Assert.Single(recoveredConsumer.StoredOffsets);
    }

    [Fact]
    public async Task Subscriber_PausesAndResumesPartitions_UnderQueueSaturation()
    {
        var consumer = new FakeKafkaConsumerClient();
        consumer.Enqueue(KafkaTestData.Message("workers", offset: 1, payload: "job-1"));
        consumer.Enqueue(KafkaTestData.Message("workers", offset: 2, payload: "job-2"));
        consumer.Enqueue(KafkaTestData.Message("workers", offset: 3, payload: "job-3"));

        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(async (string _) =>
            {
                if (Interlocked.Increment(ref calls) == 1)
                {
                    handlerStarted.TrySetResult();
                    await releaseHandler.Task.ConfigureAwait(false);
                }
            });

        var subscriber = CreateWorkerSubscriber(consumer, ingress.Object, options =>
        {
            options.WorkerTopic = "workers";
            options.WorkerSubscriber.UseAckAfterEnqueue(
                backgroundWorkerCount: 1,
                backgroundQueueCapacity: 1,
                TimeSpan.FromSeconds(5));
        });

        await subscriber.StartAsync(CancellationToken.None);

        // Worker blocks on job-1 while job-2 fills the single-slot queue → the subscriber pauses.
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await KafkaTestData.WaitUntilAsync(() => consumer.PauseCount >= 1);

        // Freeing the worker drains the queue → the subscriber resumes and consumes job-3.
        releaseHandler.TrySetResult();
        await KafkaTestData.WaitUntilAsync(() => consumer.ResumeCount >= 1);
        await KafkaTestData.WaitUntilAsync(() => Volatile.Read(ref calls) == 3 && consumer.StoredOffsets.Count == 3);

        await subscriber.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Subscriber_ReassertsThePauseEverySaturatedTick_SoARebalanceCannotUnpauseIt()
    {
        // Regression (r24): the poll loop paused only on the !paused edge, but Pause() snapshots
        // the CURRENT assignment and a rebalance during backpressure hands this member partitions
        // with their pause state reset — the edge-triggered pause left them fetching into the full
        // queue and parked the poll thread. The loop now re-asserts the pause on every saturated
        // tick, so PauseAssignment keeps being called while the queue stays full.
        var consumer = new FakeKafkaConsumerClient();
        consumer.Enqueue(KafkaTestData.Message("workers", offset: 1, payload: "job-1"));
        consumer.Enqueue(KafkaTestData.Message("workers", offset: 2, payload: "job-2"));

        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(async (string _) =>
            {
                if (Interlocked.Increment(ref calls) == 1)
                {
                    handlerStarted.TrySetResult();
                    await releaseHandler.Task.ConfigureAwait(false);
                }
            });

        var subscriber = CreateWorkerSubscriber(consumer, ingress.Object, options =>
        {
            options.WorkerTopic = "workers";
            options.WorkerSubscriber.UseAckAfterEnqueue(
                backgroundWorkerCount: 1,
                backgroundQueueCapacity: 1,
                TimeSpan.FromSeconds(5));
        });

        await subscriber.StartAsync(CancellationToken.None);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Reach a steady saturated state (paused, handler parked, queue full)...
        await KafkaTestData.WaitUntilAsync(() => consumer.Paused);
        var pausesAtSteadyState = consumer.PauseCount;

        // ...then, with NOTHING changing (the handler stays parked, so no resume can intervene),
        // the count must keep growing: every backpressure tick re-asserts the pause. The old
        // edge-triggered pause froze the count here, because `paused` never flipped back.
        await KafkaTestData.WaitUntilAsync(() => consumer.PauseCount >= pausesAtSteadyState + 2);

        releaseHandler.TrySetResult();
        await subscriber.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Subscriber_InvalidEarlyAckOptions_FailFastOnStart()
    {
        var subscriber = CreateWorkerSubscriber(
            new FakeKafkaConsumerClient(),
            Mock.Of<IAsyncResponseIngress>(),
            options => options.WorkerSubscriber.AckMode = KafkaAckMode.AckAfterEnqueue);

        // Red-on-old (Hosting 10.0.10+): validation used to sit at the top of ExecuteAsync, which
        // BackgroundService.StartAsync no longer runs inline — StartAsync returned without
        // throwing and the fault surfaced only through ExecuteTask (or never, on a fast stop).
        // Validation now runs in StartAsync so the name of this test is true again.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => subscriber.StartAsync(CancellationToken.None));
        Assert.Contains(nameof(KafkaSubscriberOptions.BackgroundWorkerCount), ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Round 35: the reviewer's two-message scenario. Offset 10 exhausts its handling and its
    /// dead-letter publish fails; offset 11 then succeeds. Pre-fix, the burial failure was swallowed
    /// with offset 10 left unstored, offset 11's settlement stored the partition position past it,
    /// and the auto-committer committed that — a restart skipped offset 10 with no dead-letter copy
    /// anywhere. The subscriber must now fault at offset 10 (never storing past it) and be rebuilt by
    /// the supervisor, which re-consumes from the committed position.
    /// </summary>
    [Fact]
    public async Task WorkerSubscriber_WhenAFailedMessageCannotBeDeadLettered_NeverCommitsPastIt_AndRestarts()
    {
        var first = new FakeKafkaConsumerClient();
        first.Enqueue(KafkaTestData.Message("workers", offset: 10, payload: "poison", ("correlationId", "corr-poison")));
        first.Enqueue(KafkaTestData.Message("workers", offset: 11, payload: "fine", ("correlationId", "corr-fine")));
        var second = new FakeKafkaConsumerClient();
        var factory = new FakeKafkaConsumerClientFactory(first, second);

        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("poison")).ThrowsAsync(new InvalidOperationException("handler boom"));
        ingress.Setup(i => i.HandleWorkerMessageAsync("fine")).Returns(Task.CompletedTask);

        var options = NewOptions(o =>
        {
            o.WorkerTopic = "workers";
            o.WorkerConsumerGroup = "workers-group";
            o.WorkerSubscriber.MaxDeliveryAttempts = 1;
            o.WorkerSubscriber.HandlerRetryBaseDelay = TimeSpan.FromMilliseconds(1);
            o.WorkerSubscriber.HandlerRetryMaxDelay = TimeSpan.FromMilliseconds(2);
            o.PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1);
            o.PublishRetryMaxDelay = TimeSpan.FromMilliseconds(2);
        });
        var subscriber = new KafkaWorkerSubscriber(
            Options.Create(options),
            factory,
            new FakeKafkaProducerClient { PublishException = new InvalidOperationException("dead-letter topic gone") },
            new FakeKafkaAdminClient(),
            ingress.Object,
            NullLogger<KafkaWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            // The supervisor rebuilt the consumer: the first one faulted at offset 10.
            await KafkaTestData.WaitUntilAsync(() => factory.CreatedRoles.Count >= 2, TimeSpan.FromSeconds(10));
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
        }

        // Nothing was ever stored on the faulted consumer: offset 11 was never handled behind the
        // unresolved offset 10, so its close committed nothing past the poison message.
        Assert.Empty(first.StoredOffsets);
        Assert.True(first.Closed);
        ingress.Verify(i => i.HandleWorkerMessageAsync("fine"), Times.Never);
    }

    // ---------- Round 37: a long handler no longer stalls the poll loop (F7) ----------

    [Fact]
    public async Task WorkerSubscriber_KeepsPollingWhileAHandlerRunsLong()
    {
        // Regression (round 37, F7): the poll thread awaited the whole handler, so a durable-flow
        // step awaiting a remote response or a timer stopped every Consume() call for its duration
        // — past max.poll.interval.ms the broker evicted the consumer, rebalanced its partitions,
        // and redelivered the message to a peer that started the same work again. Past
        // DetachHandlerAfter (the default second here) the handler is detached and the loop must
        // keep calling Consume, which is what the broker counts as liveness.
        var consumer = new FakeKafkaConsumerClient();
        consumer.Enqueue(KafkaTestData.Message("workers", offset: 1, payload: "slow-job"));

        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("slow-job"))
            .Returns(async () =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
            });

        var subscriber = CreateWorkerSubscriber(consumer, ingress.Object, options => options.WorkerTopic = "workers");
        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // Past the inline budget: with the handler still parked, the poll count must keep
            // climbing. The old loop was blocked inside the handler and froze it here.
            await Task.Delay(TimeSpan.FromMilliseconds(1500));
            var pollsAfterDetach = consumer.ConsumeCalls;
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            Assert.True(
                consumer.ConsumeCalls > pollsAfterDetach + 5,
                $"Consume was called {consumer.ConsumeCalls - pollsAfterDetach} time(s) in 500 ms while the handler ran; the poll loop is stalled.");
            Assert.Empty(consumer.StoredOffsets); // not settled yet

            releaseHandler.SetResult();
            await KafkaTestData.WaitUntilAsync(() => consumer.StoredOffsets.Count == 1);
            Assert.Equal(new FakeKafkaConsumerClient.StoredOffset("workers", 0, 1), Assert.Single(consumer.StoredOffsets));
        }
        finally
        {
            releaseHandler.TrySetResult();
            await subscriber.StopAsync(CancellationToken.None);
        }

        ingress.Verify(i => i.HandleWorkerMessageAsync("slow-job"), Times.Once);
    }

    [Fact]
    public async Task WorkerSubscriber_OtherPartitionsKeepFlowing_WhileOneHandlerIsDetached()
    {
        // Same finding, the other consequence: with the poll thread parked in one partition's
        // handler, every other partition assigned to the consumer stalled behind it.
        var consumer = new FakeKafkaConsumerClient();
        consumer.Enqueue(KafkaTestData.MessageOn("workers", partition: 0, offset: 1, payload: "slow-job"));
        consumer.Enqueue(KafkaTestData.MessageOn("workers", partition: 1, offset: 1, payload: "quick-job"));

        var slowStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSlow = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var quickHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("slow-job"))
            .Returns(async () =>
            {
                slowStarted.TrySetResult();
                await releaseSlow.Task.ConfigureAwait(false);
            });
        ingress.Setup(i => i.HandleWorkerMessageAsync("quick-job"))
            .Returns(() =>
            {
                quickHandled.TrySetResult();
                return Task.CompletedTask;
            });

        var subscriber = CreateWorkerSubscriber(consumer, ingress.Object, options => options.WorkerTopic = "workers");
        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await slowStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // Partition 1's message is handled while partition 0's handler is still running.
            await quickHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await KafkaTestData.WaitUntilAsync(() => consumer.StoredOffsets.Count == 1);
            Assert.Equal(new FakeKafkaConsumerClient.StoredOffset("workers", 1, 1), Assert.Single(consumer.StoredOffsets));
            Assert.False(releaseSlow.Task.IsCompleted);

            releaseSlow.SetResult();
            await KafkaTestData.WaitUntilAsync(() => consumer.StoredOffsets.Count == 2);
        }
        finally
        {
            releaseSlow.TrySetResult();
            await subscriber.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task WorkerSubscriber_DetachedHandler_PausesItsPartition_AndResumesItOnceSettled()
    {
        var consumer = new FakeKafkaConsumerClient();
        consumer.Enqueue(KafkaTestData.MessageOn("workers", partition: 3, offset: 5, payload: "slow-job"));
        consumer.Enqueue(KafkaTestData.MessageOn("workers", partition: 3, offset: 6, payload: "next-job"));

        var slowStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSlow = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new List<string>();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(async (string payload) =>
            {
                lock (handled)
                {
                    handled.Add(payload);
                }

                if (payload == "slow-job")
                {
                    slowStarted.TrySetResult();
                    await releaseSlow.Task.ConfigureAwait(false);
                }
            });

        var subscriber = CreateWorkerSubscriber(consumer, ingress.Object, options =>
        {
            options.WorkerTopic = "workers";
            options.WorkerSubscriber.DetachHandlerAfter = TimeSpan.FromMilliseconds(20);
        });
        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await slowStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await KafkaTestData.WaitUntilAsync(() => consumer.IsPartitionPaused(3));

            // Paused: the next message on the partition is NOT consumed behind the running one.
            await Task.Delay(200);
            lock (handled)
            {
                Assert.Equal(["slow-job"], handled);
            }

            Assert.Empty(consumer.StoredOffsets);

            releaseSlow.SetResult();
            await KafkaTestData.WaitUntilAsync(() => consumer.StoredOffsets.Count == 2);
            Assert.Equal([5L, 6L], consumer.StoredOffsets.Select(stored => stored.Offset));
            Assert.Equal(3, Assert.Single(consumer.PartitionPauses));
            Assert.Equal(3, Assert.Single(consumer.PartitionResumes));
            lock (handled)
            {
                Assert.Equal(["slow-job", "next-job"], handled);
            }
        }
        finally
        {
            releaseSlow.TrySetResult();
            await subscriber.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task WorkerSubscriber_StopWaitsForADetachedHandler_AndStoresItsOffsetBeforeClosing()
    {
        var consumer = new FakeKafkaConsumerClient();
        consumer.Enqueue(KafkaTestData.Message("workers", offset: 8, payload: "slow-job"));

        var slowStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSlow = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("slow-job"))
            .Returns(async () =>
            {
                slowStarted.TrySetResult();
                await releaseSlow.Task.ConfigureAwait(false);
            });

        var subscriber = CreateWorkerSubscriber(consumer, ingress.Object, options =>
        {
            options.WorkerTopic = "workers";
            options.WorkerSubscriber.DetachHandlerAfter = TimeSpan.FromMilliseconds(20);
        });
        await subscriber.StartAsync(CancellationToken.None);
        await slowStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await KafkaTestData.WaitUntilAsync(() => consumer.IsPartitionPaused(0));

        var stopping = subscriber.StopAsync(CancellationToken.None);
        await Task.Delay(200);
        Assert.False(stopping.IsCompleted);
        Assert.False(consumer.Closed);

        releaseSlow.SetResult();
        await stopping.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(new FakeKafkaConsumerClient.StoredOffset("workers", 0, 8), Assert.Single(consumer.StoredOffsets));
        Assert.True(consumer.Closed);
    }

    [Fact]
    public async Task WorkerSubscriber_DetachedHandlerThatCannotBeDeadLettered_RestartsWithoutCommittingPastIt()
    {
        // The round-35 contract through the detached path: the burial failure surfaces from the
        // poll thread's settlement tick, the consumer closes without storing the offset, and the
        // supervisor rebuilds it.
        var first = new FakeKafkaConsumerClient();
        first.Enqueue(KafkaTestData.Message("workers", offset: 10, payload: "poison"));
        var second = new FakeKafkaConsumerClient();
        var factory = new FakeKafkaConsumerClientFactory(first, second);

        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("poison"))
            .Returns(async () =>
            {
                await Task.Delay(100);
                throw new InvalidOperationException("handler boom");
            });

        var options = NewOptions(o =>
        {
            o.WorkerTopic = "workers";
            o.WorkerSubscriber.MaxDeliveryAttempts = 1;
            o.WorkerSubscriber.DetachHandlerAfter = TimeSpan.FromMilliseconds(20);
            o.PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1);
            o.PublishRetryMaxDelay = TimeSpan.FromMilliseconds(2);
        });
        var subscriber = new KafkaWorkerSubscriber(
            Options.Create(options),
            factory,
            new FakeKafkaProducerClient { PublishException = new InvalidOperationException("dead-letter topic gone") },
            new FakeKafkaAdminClient(),
            ingress.Object,
            NullLogger<KafkaWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await KafkaTestData.WaitUntilAsync(() => factory.CreatedRoles.Count >= 2, TimeSpan.FromSeconds(10));
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
        }

        Assert.Empty(first.StoredOffsets);
        Assert.True(first.Closed);
    }

    // ---------- Helpers ----------

    private static readonly Dictionary<KafkaSubscriberService, FakeKafkaConsumerClientFactory> Factories = [];

    private static FakeKafkaConsumerClientFactory FactoryOf(KafkaSubscriberService subscriber)
        => Factories[subscriber];

    private static KafkaAsyncResponseTransportOptions NewOptions(Action<KafkaAsyncResponseTransportOptions>? configure = null)
    {
        var options = KafkaTestData.NewOptions();
        options.SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1);
        options.SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(5);
        options.WorkerSubscriber.PollTimeout = TimeSpan.FromMilliseconds(10);
        options.WorkerSubscriber.BackpressurePollDelay = TimeSpan.FromMilliseconds(5);
        options.ResponseSubscriber.PollTimeout = TimeSpan.FromMilliseconds(10);
        options.ResponseSubscriber.BackpressurePollDelay = TimeSpan.FromMilliseconds(5);
        configure?.Invoke(options);
        return options;
    }

    private static KafkaWorkerSubscriber CreateWorkerSubscriber(
        FakeKafkaConsumerClient consumer,
        IAsyncResponseIngress ingress,
        Action<KafkaAsyncResponseTransportOptions>? configure = null,
        FakeKafkaProducerClient? producer = null,
        FakeKafkaAdminClient? adminClient = null)
    {
        var factory = new FakeKafkaConsumerClientFactory(consumer);
        var subscriber = new KafkaWorkerSubscriber(
            Options.Create(NewOptions(configure)),
            factory,
            producer ?? new FakeKafkaProducerClient(),
            adminClient ?? new FakeKafkaAdminClient(),
            ingress,
            NullLogger<KafkaWorkerSubscriber>.Instance);
        Factories[subscriber] = factory;
        return subscriber;
    }

    private static KafkaResponseIngressSubscriber CreateResponseSubscriber(
        FakeKafkaConsumerClient consumer,
        IAsyncResponseIngress ingress,
        Action<KafkaAsyncResponseTransportOptions>? configure = null)
    {
        var factory = new FakeKafkaConsumerClientFactory(consumer);
        var subscriber = new KafkaResponseIngressSubscriber(
            Options.Create(NewOptions(configure)),
            factory,
            new FakeKafkaProducerClient(),
            new FakeKafkaAdminClient(),
            ingress,
            NullLogger<KafkaResponseIngressSubscriber>.Instance);
        Factories[subscriber] = factory;
        return subscriber;
    }
}
