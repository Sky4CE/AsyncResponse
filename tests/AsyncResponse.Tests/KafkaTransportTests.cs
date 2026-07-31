using AsyncResponse.Transports.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Text.Json;
using System.Text;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

public class KafkaTransportTests
{
    // ---------- Publish ----------

    [Fact]
    public async Task PublishAsync_SerializesEnvelope_WithCorrelationHeaderAndPartitionKey()
    {
        var producer = new FakeKafkaProducerClient();
        var transport = CreateTransport(producer, options => options.WorkerTopic = "jobs");

        await transport.PublishAsync(Envelope("corr-1"));

        var publish = Assert.Single(producer.Publishes);
        Assert.Equal("jobs", publish.Topic);
        Assert.Equal("corr-1", publish.Key);
        Assert.Equal("corr-1", FakeKafkaProducerClient.Header(publish.Headers, "correlationId"));

        var roundTripped = JsonSerializer.Deserialize<WorkerJobEnvelope>(publish.Payload)!;
        Assert.Equal("corr-1", roundTripped.CorrelationId);
        Assert.Equal("AsyncResponse.Tests.IKafkaWorkerSpy", roundTripped.Call.ServiceInterfaceFullName);
        Assert.Equal(WorkerJobEnvelopeSchema.Current, roundTripped.SchemaVersion);
    }

    [Fact]
    public async Task PublishAsync_EmitsKafkaActivityTags()
    {
        using var collector = new AsyncResponseActivityCollector();
        var producer = new FakeKafkaProducerClient();
        var transport = CreateTransport(producer, options => options.WorkerTopic = "jobs");

        await transport.PublishAsync(Envelope("corr-1"));

        var activity = collector.Single("asyncresponse.worker.publish", "asyncresponse.transport", "kafka");
        Assert.Equal("corr-1", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.correlation_id"));
        Assert.Equal("kafka", AsyncResponseActivityCollector.Tag(activity, "messaging.system"));
        Assert.Equal("jobs", AsyncResponseActivityCollector.Tag(activity, "messaging.destination.name"));
        Assert.Equal(0, AsyncResponseActivityCollector.Tag(activity, "messaging.kafka.destination.partition"));
        Assert.Equal(1L, AsyncResponseActivityCollector.Tag(activity, "messaging.kafka.message.offset"));
    }

    [Fact]
    public async Task PublishAsync_WithoutCorrelationId_OmitsHeaderAndKey()
    {
        var producer = new FakeKafkaProducerClient();
        var transport = CreateTransport(producer);

        await transport.PublishAsync(Envelope(correlationId: null));

        var publish = Assert.Single(producer.Publishes);
        Assert.Null(publish.Key);
        Assert.Empty(publish.Headers);
    }

    [Fact]
    public async Task PublishAsync_UsesDefaultTopicFromPrefix()
    {
        var producer = new FakeKafkaProducerClient();
        var transport = CreateTransport(producer, options => options.TopicPrefix = "orders");

        await transport.PublishAsync(Envelope("corr-1"));

        Assert.Equal("orders.transport.worker", Assert.Single(producer.Publishes).Topic);
    }

    [Fact]
    public async Task PublishAsync_RetriesTransientBrokerFailures()
    {
        var producer = new FakeKafkaProducerClient { TransientPublishFailuresBeforeSuccess = 2 };
        var transport = CreateTransport(producer);

        await transport.PublishAsync(Envelope("corr-1"));

        Assert.Equal(3, producer.PublishAttempts);
        Assert.Single(producer.Publishes);
    }

    [Fact]
    public async Task PublishAsync_NonTransientFailure_Propagates()
    {
        var producer = new FakeKafkaProducerClient { PublishException = new InvalidOperationException("boom") };
        var transport = CreateTransport(producer);

        await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(Envelope("corr-1")));
        Assert.Equal(1, producer.PublishAttempts);
    }

    [Fact]
    public async Task PublishAsync_ExhaustsTransientRetries_Propagates()
    {
        var producer = new FakeKafkaProducerClient { TransientPublishFailuresBeforeSuccess = 10 };
        var transport = CreateTransport(producer);

        await Assert.ThrowsAsync<KafkaException>(() => transport.PublishAsync(Envelope("corr-1")));
        Assert.Equal(3, producer.PublishAttempts); // PublishMaxAttempts
    }

    [Fact]
    public async Task PublishAsync_NullJob_Throws()
    {
        var transport = CreateTransport(new FakeKafkaProducerClient());
        await Assert.ThrowsAsync<ArgumentNullException>(() => transport.PublishAsync(null!));
    }

    [Fact]
    public void Transport_MissingBootstrapServers_FailsFast()
        => Assert.Throws<InvalidOperationException>(() => new KafkaWorkerTransport(
            Options.Create(new KafkaAsyncResponseTransportOptions()),
            new FakeKafkaProducerClient()));

    // ---------- Transient classification ----------

    [Theory]
    [InlineData(ErrorCode.Local_Transport, true)]
    [InlineData(ErrorCode.Local_TimedOut, true)]
    [InlineData(ErrorCode.UnknownTopicOrPart, true)]
    [InlineData(ErrorCode.Local_Fatal, false)]
    public void IsTransient_ClassifiesKafkaErrors(ErrorCode code, bool expected)
        => Assert.Equal(expected, KafkaTransportRetry.IsTransient(new KafkaException(new Error(code))));

    [Fact]
    public void IsTransient_TimeoutIsTransient_OtherExceptionsAreNot()
    {
        Assert.True(KafkaTransportRetry.IsTransient(new TimeoutException()));
        Assert.False(KafkaTransportRetry.IsTransient(new InvalidOperationException()));
    }

    // ---------- Client adapters ----------

    [Fact]
    public void ConsumerAdapter_MapsMessagesAndDelegatesConsumerOperations()
    {
        var assignment = new List<TopicPartition> { new("jobs", new Partition(2)) };
        var headers = new Headers
        {
            { "correlationId", Encoding.UTF8.GetBytes("corr") }
        };
        var consumer = new Moq.Mock<IConsumer<string?, byte[]>>();
        consumer.SetupGet(c => c.Assignment).Returns(assignment);
        consumer.SetupSequence(c => c.Consume(It.IsAny<TimeSpan>()))
            .Returns((ConsumeResult<string?, byte[]>?)null!)
            .Returns(new ConsumeResult<string?, byte[]> { IsPartitionEOF = true })
            .Returns(new ConsumeResult<string?, byte[]>
            {
                Topic = "jobs",
                Partition = new Partition(2),
                Offset = new Offset(41),
                Message = new Message<string?, byte[]>
                {
                    Value = Encoding.UTF8.GetBytes("payload"),
                    Headers = headers
                }
            })
            .Returns(new ConsumeResult<string?, byte[]>
            {
                Topic = "jobs",
                Partition = new Partition(2),
                Offset = new Offset(42),
                Message = null
            });
        var adapter = new KafkaConsumerClientAdapter(consumer.Object);

        adapter.Subscribe("jobs");
        Assert.Null(adapter.Consume(TimeSpan.Zero));
        Assert.Null(adapter.Consume(TimeSpan.Zero));

        var message = adapter.Consume(TimeSpan.Zero)!;
        Assert.Equal("jobs", message.Topic);
        Assert.Equal(2, message.Partition);
        Assert.Equal(41, message.Offset);
        Assert.Equal("payload", Encoding.UTF8.GetString(message.Payload!));
        Assert.Equal("corr", Assert.Single(message.Headers).ValueUtf8);

        var messageWithoutPayload = adapter.Consume(TimeSpan.Zero)!;
        Assert.Null(messageWithoutPayload.Payload);
        Assert.Empty(messageWithoutPayload.Headers);

        adapter.StoreOffset("jobs", 2, 41);
        adapter.PauseAssignment();
        adapter.ResumeAssignment();
        adapter.Close();
        adapter.Dispose();

        consumer.Verify(c => c.Subscribe("jobs"));
        consumer.Verify(c => c.StoreOffset(It.Is<TopicPartitionOffset>(offset =>
            offset.Topic == "jobs" &&
            offset.Partition.Value == 2 &&
            offset.Offset.Value == 42)));
        consumer.Verify(c => c.Pause(assignment));
        consumer.Verify(c => c.Resume(assignment));
        consumer.Verify(c => c.Close());
        consumer.Verify(c => c.Dispose());
    }

    [Fact]
    public async Task ProducerAdapter_BuildsConfigLazilyWhenPublishing()
    {
        var options = KafkaTestData.NewOptions();
        var marker = new InvalidOperationException("stop before broker build");
        options.ClientId = "client-a";
        options.ConfigureProducer = config =>
        {
            Assert.Equal("localhost:9092", config.BootstrapServers);
            Assert.Equal("client-a", config.ClientId);
            Assert.Equal(Acks.All, config.Acks);
            Assert.True(config.EnableIdempotence);
            throw marker;
        };
        using var adapter = new KafkaProducerClientAdapter(options);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            adapter.PublishAsync(
                "jobs",
                "corr",
                Encoding.UTF8.GetBytes("payload"),
                [KafkaTransportHeader.Utf8("correlationId", "corr")],
                CancellationToken.None));

        Assert.Same(marker, ex);
    }

    [Fact]
    public void ProducerAdapter_BuildsAndDisposesNativeProducerWithoutConnecting()
    {
        var options = KafkaTestData.NewOptions();
        var configured = false;
        options.OperationTimeout = TimeSpan.FromMilliseconds(100);
        options.ConfigureProducer = _ => configured = true;
        var adapter = new KafkaProducerClientAdapter(options);
        var lazy = (Lazy<IProducer<string?, byte[]>>)typeof(KafkaProducerClientAdapter)
            .GetField("_producer", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(adapter)!;

        _ = lazy.Value;
        adapter.Dispose();

        Assert.True(configured);
    }

    [Fact]
    public void ConsumerFactory_ConfiguresRoleSpecificConsumer()
    {
        var options = KafkaTestData.NewOptions();
        options.ClientId = "client-a";
        options.WorkerConsumerGroup = "worker-group";
        options.ResponseConsumerGroup = "response-group";
        options.OffsetCommitInterval = TimeSpan.FromMilliseconds(123);
        var groups = new List<string?>();
        options.ConfigureConsumer = config =>
        {
            groups.Add(config.GroupId);
            Assert.Equal("localhost:9092", config.BootstrapServers);
            Assert.Equal("client-a-" + (groups.Count == 1 ? "worker" : "responseingress"), config.ClientId);
            Assert.True(config.EnableAutoCommit);
            Assert.False(config.EnableAutoOffsetStore);
            Assert.Equal(123, config.AutoCommitIntervalMs);
            Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
            Assert.False(config.EnablePartitionEof);
            throw new InvalidOperationException("stop before broker build");
        };
        var factory = new KafkaConsumerClientFactory(options);

        Assert.Throws<InvalidOperationException>(() => factory.Create(KafkaSubscriberRole.Worker));
        Assert.Throws<InvalidOperationException>(() => factory.Create(KafkaSubscriberRole.ResponseIngress));
        Assert.Equal(["worker-group", "response-group"], groups);
    }

    [Fact]
    public void ConsumerFactory_BuildsAndDisposesNativeConsumerWithoutConnecting()
    {
        var options = KafkaTestData.NewOptions();
        var configured = false;
        options.ConfigureConsumer = _ => configured = true;
        var factory = new KafkaConsumerClientFactory(options);

        using var consumer = factory.Create(KafkaSubscriberRole.Worker);

        Assert.True(configured);
    }

    [Fact]
    public async Task AdminAdapter_ConfiguresAdminClientLazily()
    {
        var options = KafkaTestData.NewOptions();
        options.ClientId = "client-a";
        options.ConfigureAdminClient = config =>
        {
            Assert.Equal("localhost:9092", config.BootstrapServers);
            Assert.Equal("client-a-admin", config.ClientId);
            throw new InvalidOperationException("stop before broker build");
        };

        var adapter = new KafkaAdminClientAdapter(options);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            adapter.EnsureTopicsAsync(["jobs"], 3, 1, CancellationToken.None));
    }

    [Fact]
    public void ClientDefaults_UseConfiguredClientIdOrGeneratedFallback()
    {
        var options = KafkaTestData.NewOptions();
        Assert.StartsWith("asyncresponse-", KafkaTransportClientDefaults.ResolveClientId(options), StringComparison.Ordinal);

        options.ClientId = "client-a";
        Assert.Equal("client-a", KafkaTransportClientDefaults.ResolveClientId(options));
    }

    [Fact]
    public void PublicWorkerTransportConstructor_ValidatesOptionsWithoutConnecting()
    {
        var transport = new KafkaWorkerTransport(Options.Create(KafkaTestData.NewOptions()));

        Assert.NotNull(transport);
    }

    // ---------- Topic schema ----------

    [Fact]
    public void TopicSchema_DefaultsDeriveFromPrefix()
    {
        var schema = new KafkaTransportTopicSchema(KafkaTestData.NewOptions());

        Assert.Equal("asyncresponse.transport.worker", schema.WorkerTopic);
        Assert.Equal("asyncresponse.transport.response", schema.ResponseTopic);
        Assert.Equal("asyncresponse.transport.worker.deadletter", schema.DeadLetterTopicFor(schema.WorkerTopic));
        Assert.Equal("asyncresponse.transport.response.deadletter", schema.DeadLetterTopicFor(schema.ResponseTopic));
    }

    [Fact]
    public void TopicSchema_ExplicitNamesWin()
    {
        var options = KafkaTestData.NewOptions();
        options.WorkerTopic = "jobs";
        options.ResponseTopic = "replies";
        options.DeadLetterTopicSuffix = ".dlq";
        var schema = new KafkaTransportTopicSchema(options);

        Assert.Equal("jobs", schema.WorkerTopic);
        Assert.Equal("replies", schema.ResponseTopic);
        Assert.Equal("jobs.dlq", schema.DeadLetterTopicFor("jobs"));
    }

    [Fact]
    public void TopicSchema_ExplicitDeadLetterTopic_OverridesSuffix()
    {
        var options = KafkaTestData.NewOptions();
        options.DeadLetterTopic = "poison";
        var schema = new KafkaTransportTopicSchema(options);

        Assert.Equal("poison", schema.DeadLetterTopicFor("anything"));
    }

    // ---------- Options validation ----------

    [Theory]
    [MemberData(nameof(InvalidTransportOptions))]
    public void ValidateCommon_RejectsInvalidOptions(
        Action<KafkaAsyncResponseTransportOptions> mutate,
        string expectedMessageFragment)
    {
        var options = KafkaTestData.NewOptions();
        mutate(options);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            KafkaTransportOptionsValidator.ValidateCommon(options));

        Assert.Contains(expectedMessageFragment, ex.Message, StringComparison.Ordinal);
    }

    public static TheoryData<Action<KafkaAsyncResponseTransportOptions>, string> InvalidTransportOptions()
        => new()
        {
            { options => options.BootstrapServers = " ", nameof(KafkaAsyncResponseTransportOptions.BootstrapServers) },
            { options => options.TopicPrefix = "", nameof(KafkaAsyncResponseTransportOptions.TopicPrefix) },
            { options => options.WorkerConsumerGroup = "", nameof(KafkaAsyncResponseTransportOptions.WorkerConsumerGroup) },
            { options => options.ResponseConsumerGroup = "", nameof(KafkaAsyncResponseTransportOptions.ResponseConsumerGroup) },
            { options => options.CorrelationIdHeader = "", nameof(KafkaAsyncResponseTransportOptions.CorrelationIdHeader) },
            { options => options.DefaultReplyTargetName = "", nameof(KafkaAsyncResponseTransportOptions.DefaultReplyTargetName) },
            { options => options.DeadLetterTopicSuffix = "", nameof(KafkaAsyncResponseTransportOptions.DeadLetterTopicSuffix) },
            { options => options.OffsetCommitInterval = TimeSpan.Zero, nameof(KafkaAsyncResponseTransportOptions.OffsetCommitInterval) },
            { options => options.OperationTimeout = TimeSpan.Zero, nameof(KafkaAsyncResponseTransportOptions.OperationTimeout) },
            { options => options.PublishMaxAttempts = 0, nameof(KafkaAsyncResponseTransportOptions.PublishMaxAttempts) },
            { options => options.PublishRetryBaseDelay = TimeSpan.Zero, nameof(KafkaAsyncResponseTransportOptions.PublishRetryBaseDelay) },
            { options => options.PublishRetryBaseDelay = TimeSpan.FromSeconds(5), nameof(KafkaAsyncResponseTransportOptions.PublishRetryBaseDelay) },
            { options => options.SubscriberRetryBaseDelay = TimeSpan.FromSeconds(30), nameof(KafkaAsyncResponseTransportOptions.SubscriberRetryBaseDelay) },
            { options => options.TopicNumPartitions = 0, nameof(KafkaAsyncResponseTransportOptions.TopicNumPartitions) },
            { options => options.TopicNumPartitions = -2, nameof(KafkaAsyncResponseTransportOptions.TopicNumPartitions) },
            { options => options.TopicReplicationFactor = 0, nameof(KafkaAsyncResponseTransportOptions.TopicReplicationFactor) },
            { options => options.HostShutdownTimeout = TimeSpan.Zero, nameof(KafkaAsyncResponseTransportOptions.HostShutdownTimeout) }
        };

    [Fact]
    public void ValidateCommon_DeadLetterDisabled_AllowsEmptySuffix()
    {
        var options = KafkaTestData.NewOptions();
        options.DeadLetterEnabled = false;
        options.DeadLetterTopicSuffix = "";

        KafkaTransportOptionsValidator.ValidateCommon(options);
    }

    [Fact]
    public void AddReplyTarget_RejectsBlankArguments()
    {
        var options = KafkaTestData.NewOptions();
        Assert.Throws<ArgumentException>(() => options.AddReplyTarget("", "topic"));
        Assert.Throws<ArgumentException>(() => options.AddReplyTarget("name", " "));
    }

    // ---------- Correlation extraction ----------

    [Fact]
    public void Extract_PrefersHeaderOverBody()
    {
        var correlationId = KafkaCorrelationIdExtractor.Extract(
            [KafkaTransportHeader.Utf8("correlationId", "corr-header")],
            """{"CorrelationId":"corr-body"}""",
            KafkaTestData.NewOptions());

        Assert.Equal("corr-header", correlationId);
    }

    [Fact]
    public void Extract_FallsBackToJsonPaths_CaseInsensitive()
    {
        var correlationId = KafkaCorrelationIdExtractor.Extract(
            [],
            """{"correlationid":"corr-body"}""",
            KafkaTestData.NewOptions());

        Assert.Equal("corr-body", correlationId);
    }

    [Fact]
    public void Extract_WalksNestedPaths_AndUnwrapsNestedJsonStrings()
    {
        var payload = """{"PubSubParams":"{\"CustomParameters\":{\"CorrelationId\":\"corr-nested\"}}"}""";

        var correlationId = KafkaCorrelationIdExtractor.Extract([], payload, KafkaTestData.NewOptions());

        Assert.Equal("corr-nested", correlationId);
    }

    [Fact]
    public void Extract_CustomHeaderName_IsHonored()
    {
        var options = KafkaTestData.NewOptions();
        options.CorrelationIdHeader = "x-corr";

        var correlationId = KafkaCorrelationIdExtractor.Extract(
            [KafkaTransportHeader.Utf8("x-corr", "corr-custom")],
            "{}",
            options);

        Assert.Equal("corr-custom", correlationId);
    }

    [Fact]
    public void Extract_InvalidJsonAndNoHeader_ReturnsNull()
        => Assert.Null(KafkaCorrelationIdExtractor.Extract([], "not-json", KafkaTestData.NewOptions()));

    [Fact]
    public void Extract_NoPathsConfigured_ReturnsNull()
    {
        var options = KafkaTestData.NewOptions();
        options.CorrelationIdJsonPaths = [];

        Assert.Null(KafkaCorrelationIdExtractor.Extract([], """{"CorrelationId":"corr"}""", options));
    }

    [Fact]
    public void Extract_BlankMessageAndNullRoot_ReturnNull()
    {
        Assert.Null(KafkaCorrelationIdExtractor.Extract([], " ", KafkaTestData.NewOptions()));
        Assert.Null(KafkaCorrelationIdExtractor.Extract([], "null", KafkaTestData.NewOptions()));
    }

    [Fact]
    public void Extract_BlankPathInvalidNestedJsonAndNonStringValues_AreHandled()
    {
        var options = KafkaTestData.NewOptions();
        options.CorrelationIdJsonPaths = [" ", "Broken.CorrelationId", "Items.Id", "Count"];
        var payload = """{"Broken":"{\"CorrelationId\":","Items":[{"Id":"array"}],"Count":42}""";

        Assert.Equal("42", KafkaCorrelationIdExtractor.Extract([], payload, options));
    }

    [Fact]
    public void TryReadHeader_MissingOrNullValue_ReturnsNull()
    {
        Assert.Null(KafkaCorrelationIdExtractor.TryReadHeader([], "correlationId"));
        Assert.Null(KafkaCorrelationIdExtractor.TryReadHeader([KafkaTransportHeader.Utf8("other", "corr")], "correlationId"));
        Assert.Null(KafkaCorrelationIdExtractor.TryReadHeader([new KafkaTransportHeader("correlationId", null)], "correlationId"));
    }

    [Fact]
    public void Extract_WhenNoConfiguredPathMatches_ReturnsNull()
        => Assert.Null(KafkaCorrelationIdExtractor.Extract([], """{"Other":"corr"}""", KafkaTestData.NewOptions()));

    // ---------- Reply targets ----------

    [Fact]
    public void ReplyTarget_DefaultResolvesToResponseTopic()
    {
        var provider = new KafkaReplyTargetProvider(Options.Create(KafkaTestData.NewOptions()));

        var target = provider.GetReplyTarget();

        Assert.Equal("default", target.Name);
        Assert.Equal("kafka", target.Transport);
        Assert.Equal("asyncresponse.transport.response", target.Address);
        Assert.Equal("asyncresponse.transport.response", target.Properties["topic"]);
        Assert.Equal("asyncresponse-responses", target.Properties["consumerGroup"]);
        Assert.Equal("correlationId", target.Properties["correlationIdHeader"]);
        Assert.Equal("localhost:9092", target.Properties["bootstrapServers"]);
    }

    [Fact]
    public void ReplyTarget_NamedTargetIsResolved()
    {
        var options = KafkaTestData.NewOptions();
        options.AddReplyTarget("regional", "responses.eu");

        var target = new KafkaReplyTargetProvider(Options.Create(options)).GetReplyTarget("regional");

        Assert.Equal("regional", target.Name);
        Assert.Equal("responses.eu", target.Address);
    }

    [Fact]
    public void ReplyTarget_UnknownName_Throws()
    {
        var provider = new KafkaReplyTargetProvider(Options.Create(KafkaTestData.NewOptions()));

        var ex = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("missing"));
        Assert.Contains("missing", ex.Message, StringComparison.Ordinal);
    }

    // ---------- DI registration ----------

    [Fact]
    public void WithKafkaTransport_ReplacesWorkerTransportReplyProvider_AndRegistersHostedServices()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithKafkaTransport(options => options.BootstrapServers = "localhost:9092")
            .WithInMemoryDurableFlows());

        Assert.IsType<KafkaWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.IsType<KafkaReplyTargetProvider>(provider.GetRequiredService<IAsyncResponseReplyTargetProvider>());
        Assert.Equal("Kafka", provider.GetRequiredService<AsyncResponseTransportMarker>().Name);

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        Assert.Contains(hostedServices, s => s is KafkaWorkerSubscriber);
        Assert.Contains(hostedServices, s => s is KafkaResponseIngressSubscriber);
    }

    [Fact]
    public void WithKafkaTransport_AppliesConfigureOptions()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithKafkaTransport(options =>
            {
                options.BootstrapServers = "broker:9092";
                options.TopicPrefix = "orders";
                options.WorkerSubscriber.UseAckAfterEnqueue(4, 256);
            }));

        var options = provider.GetRequiredService<IOptions<KafkaAsyncResponseTransportOptions>>().Value;
        Assert.Equal("broker:9092", options.BootstrapServers);
        Assert.Equal("orders", options.TopicPrefix);
        Assert.Equal(KafkaAckMode.AckAfterEnqueue, options.WorkerSubscriber.AckMode);
        Assert.Equal(4, options.WorkerSubscriber.BackgroundWorkerCount);
        Assert.Equal(256, options.WorkerSubscriber.BackgroundQueueCapacity);
    }

    [Fact]
    public void WithKafkaTransport_NullConfigure_Throws()
        => Assert.Throws<ArgumentNullException>(
            () => new ServiceCollection().AddAsyncResponse().WithKafkaTransport(null!));

    [Fact]
    public async Task KafkaTransport_RegistersExactlyOneTransportMarker()
    {
        var provider = Build(builder => builder
            .WithInMemoryChannel()
            .WithKafkaTransport(options => options.BootstrapServers = "localhost:9092")
            .WithInMemoryDurableFlows());

        var validator = new AsyncResponseStartupValidator(
            provider.GetServices<AsyncResponseChannelMarker>(),
            provider.GetServices<AsyncResponseTransportMarker>(),
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>(),
            provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AsyncResponseOptions>>());

        await validator.StartAsync(CancellationToken.None); // one channel + transport + flow store → must not throw
        await validator.StopAsync(CancellationToken.None);
    }

    // ---------- Helpers ----------

    private static WorkerJobEnvelope Envelope(string? correlationId)
        => new()
        {
            CorrelationId = correlationId,
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "AsyncResponse.Tests.IKafkaWorkerSpy",
                MethodName = "OnWorkerJob",
                Params = [CallbackParam.ForValue(42)]
            }
        };

    private static KafkaWorkerTransport CreateTransport(
        FakeKafkaProducerClient producer,
        Action<KafkaAsyncResponseTransportOptions>? configure = null)
    {
        var options = KafkaTestData.NewOptions();
        options.PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1);
        options.PublishRetryMaxDelay = TimeSpan.FromMilliseconds(2);
        configure?.Invoke(options);
        return new KafkaWorkerTransport(Options.Create(options), producer);
    }

    private static ServiceProvider Build(Action<AsyncResponseRegistrationBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        configure(services.AddAsyncResponse());
        return services.BuildServiceProvider();
    }
}
