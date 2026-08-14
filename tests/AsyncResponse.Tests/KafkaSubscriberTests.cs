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
