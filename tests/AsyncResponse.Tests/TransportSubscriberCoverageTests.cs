using AsyncResponse.Transports.GooglePubSub;
using AsyncResponse.Transports.NATS;
using AsyncResponse.Transports.SQS;
using AsyncResponse.Transports.AzureServiceBus;
using AsyncResponse.Transports.Redis;
using AsyncResponse.Transports.Kafka;
using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Reflection;
using Xunit;
using Amazon.SQS.Model;
using StackExchange.Redis;
using Confluent.Kafka;
using Google.Cloud.PubSub.V1;
using MongoDB.Driver;

namespace AsyncResponse.Tests;

public sealed class TransportSubscriberCoverageTests
{
    private readonly ServiceProvider _services = new ServiceCollection().BuildServiceProvider();

    [Fact]
    public async Task GooglePubSubWorkerTransport_PublicConstructor_CompilesAndRuns()
    {
        var options = Options.Create(new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            WorkerTopicId = "topic-a"
        });
        await using var transport = new GooglePubSubWorkerTransport(options);
        Assert.NotNull(transport);
    }

    [Fact]
    public async Task GooglePubSubSubscriberService_ExitsNormallyWhenClientTaskCompletes()
    {
        var client = new FakeSubscriberClient();
        var ingress = new Mock<IAsyncResponseIngress>();
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerSubscriptionId = "workers"
            }),
            ingress.Object,
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            (_, _) => Task.FromResult<IGooglePubSubSubscriberClient>(client));

        var startTask = subscriber.StartAsync(CancellationToken.None);

        // Spontaneously complete the client task
        await client.StopAsync(new SubscriberClient.ShutdownOptions(), CancellationToken.None);

        await startTask; // should exit normally
    }

    [Fact]
    public async Task GooglePubSubSubscriberService_HandlesCancellationException()
    {
        var ingress = new Mock<IAsyncResponseIngress>();
        using var cts = new CancellationTokenSource();
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerSubscriptionId = "workers"
            }),
            ingress.Object,
            NullLogger<GooglePubSubWorkerSubscriber>.Instance,
            (_, _) => {
                cts.Cancel();
                throw new OperationCanceledException(cts.Token);
            });

        await subscriber.StartAsync(cts.Token);
    }

    [Fact]
    public void SqsOptionsValidator_ThrowsOnNonPositiveTimeSpan()
    {
        var options = new SqsAsyncResponseOptions
        {
            WorkerQueue = "worker",
            ResponseQueue = "response",
            CorrelationIdAttribute = "corr",
            DefaultReplyTargetName = "reply",
            PublishRetryBaseDelay = TimeSpan.Zero
        };

        var ex = Assert.Throws<InvalidOperationException>(() => SqsOptionsValidator.ValidateCommon(options));
        Assert.Contains("must be positive", ex.Message);
    }

    [Fact]
    public async Task SqsWorkerTransport_GetQueueUrlAsync_DoubleCheckedLockingCoverage()
    {
        var options = Options.Create(new SqsAsyncResponseOptions
        {
            WorkerQueue = "worker-queue",
            ResponseQueue = "response-queue",
            CorrelationIdAttribute = "corr",
            DefaultReplyTargetName = "reply"
        });

        var client = new SimpleSqsClient();
        await using var transport = new SqsWorkerTransport(options, client);

        var job = new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "Service",
                MethodName = "Method",
                Params = []
            }
        };

        var task1 = transport.PublishAsync(job);
        var task2 = transport.PublishAsync(job);

        await Task.Delay(10);
        client.QueueUrlTcs.SetResult("https://sqs.test.local/url");

        await Task.WhenAll(task1, task2);
        Assert.Equal(1, client.GetQueueUrlCalls);
    }

    [Fact]
    public async Task SqsQueueProvisioningService_QueueNameExistsException_ConvergesAttributes()
    {
        var sqsOptions = new SqsAsyncResponseOptions
        {
            WorkerQueue = "worker",
            ResponseQueue = "response",
            CreateQueues = true,
            DeadLetterQueueSuffix = "-dlq",
            MaxReceiveCount = 5
        };
        var options = Options.Create(sqsOptions);

        var client = new SimpleSqsClient();
        client.QueueUrlTcs.SetResult("https://sqs.test.local/url");
        var service = new SqsQueueProvisioningService(options, client, NullLogger<SqsQueueProvisioningService>.Instance);

        // Call EnsureQueueWithDeadLetterAsync directly to avoid any background service lifecycle
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await service.EnsureQueueWithDeadLetterAsync(sqsOptions, "worker", cts.Token);

        Assert.True(client.SetQueueAttributesCalls > 0);
    }

    [Fact]
    public void RedisCorrelationIdExtractor_HandlesNullValues()
    {
        var entry = new StreamEntry("1-0", null!);
        var result = RedisCorrelationIdExtractor.TryReadField(entry, "corr");
        Assert.Null(result);
    }

    [Fact]
    public async Task RedisMessageDispatcher_NotifyBackgroundFailureAsync_HandlesNullCallback()
    {
        var database = new RedisTransportTests.FakeRedisStreamDatabase();
        var transportOptions = new RedisAsyncResponseTransportOptions();
        var subscriberOptions = new RedisSubscriberOptions
        {
            OnBackgroundFailure = null
        };

        var dispatcher = new AwaitingRedisMessageDispatcher(
            (_, _) => Task.CompletedTask,
            database,
            transportOptions,
            subscriberOptions,
            NullLogger.Instance,
            "stream",
            "group",
            RedisSubscriberRole.Worker);

        var delivery = new RedisStreamDelivery(
            "stream",
            "group",
            "1-0",
            "{}",
            "corr",
            1,
            new StreamEntry("1-0", [new NameValueEntry("field", "value")]));

        var method = typeof(RedisMessageDispatcher).GetMethod("NotifyBackgroundFailureAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var task = (ValueTask)method.Invoke(dispatcher, [delivery, new Exception("boom")])!;
        await task;
    }

    [Fact]
    public async Task NatsMessageDispatcher_InvokeBackgroundFailureAsync_HandlesNullCallback()
    {
        var mockJetStream = new Mock<INatsJetStreamTransport>();
        var transportOptions = new NatsAsyncResponseTransportOptions();
        var subscriberOptions = new NatsSubscriberOptions
        {
            OnBackgroundFailure = null
        };
        var schema = new NatsTransportSubjectSchema(transportOptions);

        var dispatcher = new NatsMessageDispatcher(
            (_, _) => Task.CompletedTask,
            mockJetStream.Object,
            transportOptions,
            subscriberOptions,
            schema,
            NullLogger.Instance,
            NatsSubscriberRole.Worker,
            "consumer");

        var delivery = new NatsJobDelivery(
            "subject",
            "{}",
            new Dictionary<string, string>(),
            1,
            () => ValueTask.CompletedTask,
            _ => ValueTask.CompletedTask,
            () => ValueTask.CompletedTask);

        var method = typeof(NatsMessageDispatcher).GetMethod("InvokeBackgroundFailureAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var task = (Task)method.Invoke(dispatcher, [delivery, new Exception("boom")])!;
        await task;
    }

    [Fact]
    public async Task AzureServiceBusMessageDispatcher_NotifyBackgroundFailureAsync_HandlesNullCallback()
    {
        var transportOptions = new AzureServiceBusAsyncResponseOptions();
        var subscriberOptions = new AzureServiceBusSubscriberOptions
        {
            OnBackgroundFailure = null
        };

        var dispatcher = new AwaitingAzureServiceBusMessageDispatcher(
            (_, _) => Task.CompletedTask,
            transportOptions,
            subscriberOptions,
            NullLogger.Instance,
            "queue",
            AzureServiceBusSubscriberRole.Worker);

        var delivery = new AzureServiceBusTransportDelivery(
            "queue",
            "{}",
            "id",
            "corr",
            1L,
            1,
            new Dictionary<string, object?>(),
            () => ValueTask.CompletedTask,
            () => ValueTask.CompletedTask,
            (_, _) => ValueTask.CompletedTask,
            _ => ValueTask.CompletedTask);

        var method = typeof(AzureServiceBusMessageDispatcher).GetMethod("NotifyBackgroundFailureAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var task = (ValueTask)method.Invoke(dispatcher, [delivery, new Exception("boom"), "queue", AzureServiceBusSubscriberRole.Worker])!;
        await task;
    }

    [Fact]
    public async Task QueuedKafkaMessageDispatcher_HandleAsync_QueueFullAndCancelledToken_Coverage()
    {
        var consumer = new FakeKafkaConsumerClient();
        var producer = new FakeKafkaProducerClient();
        var transportOptions = new KafkaAsyncResponseTransportOptions();
        var subscriberOptions = new KafkaSubscriberOptions
        {
            BackgroundQueueCapacity = 1,
            BackgroundWorkerCount = 1
        };

        var tcsBlock = new TaskCompletionSource();
        var dispatcher = new QueuedKafkaMessageDispatcher(
            async (_, ct) => await tcsBlock.Task,
            consumer,
            producer,
            transportOptions,
            subscriberOptions,
            NullLogger.Instance,
            "topic",
            "group",
            KafkaSubscriberRole.Worker);

        var delivery1 = new KafkaDelivery("topic", 0, 100L, "{}", "corr", Array.Empty<KafkaTransportHeader>());
        var delivery2 = new KafkaDelivery("topic", 0, 101L, "{}", "corr", Array.Empty<KafkaTransportHeader>());
        var delivery3 = new KafkaDelivery("topic", 0, 102L, "{}", "corr", Array.Empty<KafkaTransportHeader>());

        await dispatcher.HandleAsync(delivery1, CancellationToken.None);
        await Task.Delay(10);

        await dispatcher.HandleAsync(delivery2, CancellationToken.None);

        var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => dispatcher.HandleAsync(delivery3, cts.Token));

        tcsBlock.SetResult();
        await dispatcher.DisposeAsync();
    }

    [Fact]
    public async Task KafkaProducerClientAdapter_MocksIProducer_Coverage()
    {
        var options = new KafkaAsyncResponseTransportOptions();
        var adapter = new KafkaProducerClientAdapter(options);

        var mockProducer = new Mock<IProducer<string?, byte[]>>();
        var lazyProducer = new Lazy<IProducer<string?, byte[]>>(() => mockProducer.Object);

        var field = typeof(KafkaProducerClientAdapter).GetField("_producer", BindingFlags.Instance | BindingFlags.NonPublic)!;
        field.SetValue(adapter, lazyProducer);

        var mockResult = new DeliveryResult<string?, byte[]>
        {
            Topic = "topic",
            Partition = 1,
            Offset = 100
        };
        mockProducer.Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string?, byte[]>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(mockResult);

        var pubResult = await adapter.PublishAsync("topic", "key", [], [], CancellationToken.None);
        Assert.Equal("topic", pubResult.Topic);
        Assert.Equal(1, pubResult.Partition);
        Assert.Equal(100, pubResult.Offset);

        mockProducer.Setup(p => p.Flush(It.IsAny<TimeSpan>()))
            .Throws(new KafkaException(ErrorCode.Local_Application));

        adapter.Dispose();
    }

    [Fact]
    public async Task MongoDbTransportStore_WatchQueueAsync_InvokesOnNotification()
    {
        var mockCollection = new Mock<IMongoCollection<MongoTransportMessageDocument>>();
        var mockDatabase = new Mock<IMongoDatabase>().WithTestNamespace();
        mockDatabase.Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(mockCollection.Object);

        var mockCursor = new Mock<IChangeStreamCursor<ChangeStreamDocument<MongoTransportMessageDocument>>>();
        mockCursor.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);

        mockCursor.SetupGet(c => c.Current).Returns(new ChangeStreamDocument<MongoTransportMessageDocument>[] { null! });

        mockCollection.Setup(c => c.WatchAsync(
            It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoTransportMessageDocument>, ChangeStreamDocument<MongoTransportMessageDocument>>>(),
            It.IsAny<ChangeStreamOptions>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(mockCursor.Object);

        var options = Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false });
        using var store = new MongoDbTransportStore(mockDatabase.Object, options);

        var notificationCalled = false;
        await store.WatchQueueAsync("queue", () =>
        {
            notificationCalled = true;
            return Task.CompletedTask;
        }, CancellationToken.None);

        Assert.True(notificationCalled);
    }
}

// Minimal fakes needed for compilation in tests context
internal sealed class FakeSubscriberClient : IGooglePubSubSubscriberClient
{
    private readonly TaskCompletionSource _run = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int StartCalls { get; private set; }
    public int StopCalls { get; private set; }

    public Task StartAsync(Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>> handler)
    {
        StartCalls++;
        return _run.Task;
    }

    public Task StopAsync(SubscriberClient.ShutdownOptions options, CancellationToken cancellationToken)
    {
        StopCalls++;
        _run.TrySetResult();
        return Task.CompletedTask;
    }
}

internal sealed class SimpleSqsClient : ISqsClient
{
    public TaskCompletionSource<string> QueueUrlTcs = new();
    public int GetQueueUrlCalls;
    public int SetQueueAttributesCalls;

    public Task<string> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken = default)
    {
        GetQueueUrlCalls++;
        return QueueUrlTcs.Task;
    }

    public Task<string> CreateQueueAsync(string queueName, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken = default)
    {
        throw new QueueNameExistsException("Queue exists");
    }

    public Task<string> GetQueueArnAsync(string queueUrl, CancellationToken cancellationToken = default)
    {
        return Task.FromResult("arn:aws:sqs:us-east-1:000000000000:queue");
    }

    public Task SetQueueAttributesAsync(string queueUrl, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken = default)
    {
        SetQueueAttributesCalls++;
        return Task.CompletedTask;
    }

    public Task<string> SendMessageAsync(SqsOutboundMessage message, CancellationToken cancellationToken = default)
    {
        return Task.FromResult("message-id");
    }
    public Task<IReadOnlyList<SqsTransportDelivery>> ReceiveMessagesAsync(SqsReceiveRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
