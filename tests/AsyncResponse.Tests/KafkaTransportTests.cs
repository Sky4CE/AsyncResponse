using AsyncResponse.Transports.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Text.Json;
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
            { options => options.ShutdownTimeout = TimeSpan.Zero, nameof(KafkaAsyncResponseTransportOptions.ShutdownTimeout) },
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
            .WithKafkaTransport(options => options.BootstrapServers = "localhost:9092"));

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
            .WithKafkaTransport(options => options.BootstrapServers = "localhost:9092"));

        var validator = new AsyncResponseStartupValidator(
            provider.GetServices<AsyncResponseChannelMarker>(),
            provider.GetServices<AsyncResponseTransportMarker>());

        await validator.StartAsync(CancellationToken.None); // single channel + single Kafka transport → must not throw
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
