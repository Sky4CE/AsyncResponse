using AsyncResponse.Transports.RabbitMQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public class RabbitMqTransportTests
{
    [Fact]
    public void WithRabbitMqTransport_ReplacesWorkerTransportAndReplyTargetProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        var provider = services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithRabbitMqTransport(options =>
            {
                options.ConnectionString = "amqp://guest:guest@localhost:5672/";
                options.WorkerExchange = "workers";
                options.WorkerQueue = "workers";
                options.WorkerRoutingKey = "workers";
                options.ResponseExchange = "responses";
                options.ResponseQueue = "responses";
                options.ResponseRoutingKey = "responses";
            })
            .Services
            .BuildServiceProvider();

        Assert.IsType<RabbitMqWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.IsType<RabbitMqReplyTargetProvider>(provider.GetRequiredService<IAsyncResponseReplyTargetProvider>());
        Assert.Equal("RabbitMQ", provider.GetRequiredService<AsyncResponseTransportMarker>().Name);
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is RabbitMqWorkerSubscriber);
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is RabbitMqResponseIngressSubscriber);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void WorkerTransport_RequiresWorkerExchange(string value)
    {
        var options = Options.Create(new RabbitMqAsyncResponseOptions
        {
            WorkerExchange = value
        });

        Assert.Throws<InvalidOperationException>(() => new RabbitMqWorkerTransport(options, new FakeConnectionFactory()));
    }

    [Fact]
    public async Task WorkerTransport_PublishesSerializedJobAndDeclaresTopology()
    {
        var channel = new FakeRabbitMqChannel();
        var transport = new RabbitMqWorkerTransport(
            Options.Create(new RabbitMqAsyncResponseOptions
            {
                WorkerExchange = "ar.worker",
                WorkerQueue = "ar.worker.q",
                WorkerRoutingKey = "ar.worker.rk",
                CorrelationIdHeader = "cid"
            }),
            new FakeConnectionFactory(channel));

        await transport.PublishAsync(WorkerJob("corr-rabbit", 42));

        var publish = Assert.Single(channel.Published);
        Assert.Equal("ar.worker", publish.Exchange);
        Assert.Equal("ar.worker.rk", publish.RoutingKey);
        Assert.Equal("corr-rabbit", publish.Properties.CorrelationId);
        Assert.Equal("corr-rabbit", AssertHeader(publish.Properties, "cid"));
        Assert.Contains(channel.ExchangeDeclares, item => item.Exchange == "ar.worker");
        Assert.Contains(channel.QueueDeclares, item => item.Queue == "ar.worker.q");
        Assert.Contains(channel.QueueBinds, item => item.Queue == "ar.worker.q" && item.Exchange == "ar.worker" && item.RoutingKey == "ar.worker.rk");

        var job = JsonSerializer.Deserialize<WorkerJobEnvelope>(Encoding.UTF8.GetString(publish.Body.ToArray()));
        Assert.Equal("corr-rabbit", job!.CorrelationId);
        Assert.Equal(nameof(IRecoverySpy.OnWorkerJob), job.Call.MethodName);
    }

    [Fact]
    public void ReplyTargetProvider_UsesResponseExchangeAsDefaultTarget()
    {
        var provider = new RabbitMqReplyTargetProvider(Options.Create(new RabbitMqAsyncResponseOptions
        {
            ResponseExchange = "ar.response",
            ResponseQueue = "ar.response.q",
            ResponseRoutingKey = "ar.response.rk"
        }));

        var target = provider.GetReplyTarget();

        Assert.Equal("default", target.Name);
        Assert.Equal(RabbitMqAsyncResponseOptions.TransportName, target.Transport);
        Assert.Equal("ar.response:ar.response.rk", target.Address);
        Assert.Equal("ar.response", target.Properties["exchange"]);
        Assert.Equal("ar.response.rk", target.Properties["routingKey"]);
        Assert.Equal("ar.response.q", target.Properties["queue"]);
    }

    [Fact]
    public void ReplyTargetProvider_ResolvesNamedTargets()
    {
        var options = new RabbitMqAsyncResponseOptions();
        options.AddReplyTarget("regional", "regional.exchange", "regional.route");
        options.ReplyTargets["regional"].Queue = "regional.queue";
        options.ReplyTargets["regional"].Properties["tenant"] = "acme";

        var target = new RabbitMqReplyTargetProvider(Options.Create(options)).GetReplyTarget("regional");

        Assert.Equal("regional.exchange:regional.route", target.Address);
        Assert.Equal("regional.queue", target.Properties["queue"]);
        Assert.Equal("acme", target.Properties["tenant"]);
    }

    [Fact]
    public void CorrelationExtractor_ReadsPropertyThenHeaderThenJson()
    {
        var options = new RabbitMqAsyncResponseOptions { CorrelationIdHeader = "cid" };

        Assert.Equal("from-property", RabbitMqCorrelationIdExtractor.Extract(
            Delivery("{}", new BasicProperties { CorrelationId = "from-property" }),
            "{}",
            options));

        Assert.Equal("from-header", RabbitMqCorrelationIdExtractor.Extract(
            Delivery("{}", new BasicProperties
            {
                Headers = new Dictionary<string, object?> { ["cid"] = Encoding.UTF8.GetBytes("from-header") }
            }),
            "{}",
            options));

        Assert.Equal("from-json", RabbitMqCorrelationIdExtractor.Extract(
            Delivery("""{"CustomParameters":{"CorrelationId":"from-json"}}"""),
            """{"CustomParameters":{"CorrelationId":"from-json"}}""",
            options));
    }

    [Fact]
    public async Task WorkerSubscriber_ForwardsMessageBodyAndAcks()
    {
        var channel = new FakeRabbitMqChannel();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress
            .Setup(i => i.HandleWorkerMessageAsync("worker-json"))
            .Returns(Task.CompletedTask);
        var subscriber = new RabbitMqWorkerSubscriber(
            Options.Create(new RabbitMqAsyncResponseOptions
            {
                WorkerExchange = "worker.ex",
                WorkerQueue = "worker.q",
                WorkerRoutingKey = "worker.rk"
            }),
            ingress.Object,
            NullLogger<RabbitMqWorkerSubscriber>.Instance,
            new FakeConnectionFactory(channel));

        await using var host = new HostedServiceRun(subscriber);
        await channel.WaitForConsumerAsync();
        await channel.DeliverAsync(Delivery("worker-json", deliveryTag: 7));

        ingress.Verify(i => i.HandleWorkerMessageAsync("worker-json"), Times.Once);
        Assert.Contains(7UL, channel.Acks);
    }

    [Fact]
    public async Task ResponseSubscriber_ExtractsCorrelationAndForwardsMessageBody()
    {
        var channel = new FakeRabbitMqChannel();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress
            .Setup(i => i.HandleResponseMessageAsync("response-json", "corr-response"))
            .Returns(Task.CompletedTask);
        var subscriber = new RabbitMqResponseIngressSubscriber(
            Options.Create(new RabbitMqAsyncResponseOptions
            {
                ResponseExchange = "response.ex",
                ResponseQueue = "response.q",
                ResponseRoutingKey = "response.rk",
                CorrelationIdHeader = "cid"
            }),
            ingress.Object,
            NullLogger<RabbitMqResponseIngressSubscriber>.Instance,
            new FakeConnectionFactory(channel));

        await using var host = new HostedServiceRun(subscriber);
        await channel.WaitForConsumerAsync();
        await channel.DeliverAsync(Delivery(
            "response-json",
            new BasicProperties { Headers = new Dictionary<string, object?> { ["cid"] = "corr-response" } },
            deliveryTag: 9));

        ingress.Verify(i => i.HandleResponseMessageAsync("response-json", "corr-response"), Times.Once);
        Assert.Contains(9UL, channel.Acks);
    }

    // ---------- Worker transport: validation, null job, publish failure, disposal ----------

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void WorkerTransport_RequiresWorkerQueue(string value)
    {
        var options = Options.Create(new RabbitMqAsyncResponseOptions { WorkerQueue = value });

        var ex = Assert.Throws<InvalidOperationException>(
            () => new RabbitMqWorkerTransport(options, new FakeConnectionFactory()));
        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.WorkerQueue), ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void WorkerTransport_RequiresWorkerRoutingKey(string value)
    {
        var options = Options.Create(new RabbitMqAsyncResponseOptions { WorkerRoutingKey = value });

        var ex = Assert.Throws<InvalidOperationException>(
            () => new RabbitMqWorkerTransport(options, new FakeConnectionFactory()));
        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.WorkerRoutingKey), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WorkerTransport_RequiresPositiveShutdownTimeout()
    {
        var options = Options.Create(new RabbitMqAsyncResponseOptions { ShutdownTimeout = TimeSpan.Zero });

        var ex = Assert.Throws<InvalidOperationException>(
            () => new RabbitMqWorkerTransport(options, new FakeConnectionFactory()));
        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.ShutdownTimeout), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WorkerTransport_PublishNullJob_Throws()
    {
        var transport = new RabbitMqWorkerTransport(
            Options.Create(new RabbitMqAsyncResponseOptions()),
            new FakeConnectionFactory());

        await Assert.ThrowsAsync<ArgumentNullException>(() => transport.PublishAsync(null!));
    }

    [Fact]
    public async Task WorkerTransport_WhenPublishFails_Rethrows()
    {
        var channel = new FakeRabbitMqChannel { ThrowOnPublish = new InvalidOperationException("publish boom") };
        var transport = new RabbitMqWorkerTransport(
            Options.Create(new RabbitMqAsyncResponseOptions()),
            new FakeConnectionFactory(channel));

        await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(WorkerJob("corr", 1)));
    }

    [Fact]
    public async Task WorkerTransport_DisposeAsync_ClosesChannelAndConnection()
    {
        var channel = new FakeRabbitMqChannel();
        var factory = new FakeConnectionFactory(channel);
        var transport = new RabbitMqWorkerTransport(Options.Create(new RabbitMqAsyncResponseOptions()), factory);

        await transport.PublishAsync(WorkerJob("corr", 1));
        await transport.DisposeAsync();

        Assert.Equal(1, channel.CloseCalls);
        Assert.Equal(1, channel.DisposeCalls);
        Assert.Equal(1, factory.Connection.CloseCalls);
        Assert.Equal(1, factory.Connection.DisposeCalls);
    }

    [Fact]
    public async Task WorkerTransport_DisposeAsync_WhenNeverPublished_ClosesNothing()
    {
        var channel = new FakeRabbitMqChannel();
        var factory = new FakeConnectionFactory(channel);
        var transport = new RabbitMqWorkerTransport(Options.Create(new RabbitMqAsyncResponseOptions()), factory);

        await transport.DisposeAsync();

        Assert.Equal(0, channel.CloseCalls);
        Assert.Equal(0, channel.DisposeCalls);
        Assert.Equal(0, factory.Connection.CloseCalls);
        Assert.Equal(0, factory.Connection.DisposeCalls);
    }

    [Fact]
    public async Task WorkerTransport_EnablesPublisherConfirmationsOnPublishChannel()
    {
        var factory = new FakeConnectionFactory(new FakeRabbitMqChannel());
        var transport = new RabbitMqWorkerTransport(Options.Create(new RabbitMqAsyncResponseOptions()), factory);

        await transport.PublishAsync(WorkerJob("corr", 1));

        Assert.True(factory.Connection.PublisherConfirmationsRequested);
    }

    [Fact]
    public async Task WorkerTransport_RecreatesChannelAfterTransientConnectFailure()
    {
        var channel = new FakeRabbitMqChannel();
        var factory = new FlakyConnectionFactory(new FakeRabbitMqConnection(channel), failuresBeforeSuccess: 1);
        var transport = new RabbitMqWorkerTransport(Options.Create(new RabbitMqAsyncResponseOptions()), factory);

        // First publish hits the transient connect failure...
        await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(WorkerJob("corr-1", 1)));
        Assert.Empty(channel.Published);

        // ...and the transport recovers on the next publish instead of caching the fault forever.
        await transport.PublishAsync(WorkerJob("corr-2", 2));

        var publish = Assert.Single(channel.Published);
        Assert.Equal("corr-2", publish.Properties.CorrelationId);
        Assert.Equal(2, factory.Attempts);
    }

    [Fact]
    public async Task WorkerTransport_DisposeAsync_IsIdempotent()
    {
        var channel = new FakeRabbitMqChannel();
        var factory = new FakeConnectionFactory(channel);
        var transport = new RabbitMqWorkerTransport(Options.Create(new RabbitMqAsyncResponseOptions()), factory);

        await transport.PublishAsync(WorkerJob("corr", 1));
        await transport.DisposeAsync();
        await transport.DisposeAsync();

        Assert.Equal(1, channel.CloseCalls);
        Assert.Equal(1, factory.Connection.CloseCalls);
    }

    [Fact]
    public async Task WorkerTransport_ReusesChannelAcrossPublishes()
    {
        var channel = new FakeRabbitMqChannel();
        var factory = new FakeConnectionFactory(channel);
        var transport = new RabbitMqWorkerTransport(Options.Create(new RabbitMqAsyncResponseOptions()), factory);

        await transport.PublishAsync(WorkerJob("c1", 1));
        await transport.PublishAsync(WorkerJob("c2", 2));

        Assert.Equal(2, channel.Published.Count);
        Assert.Equal(1, factory.Connection.CreateChannelCalls); // the channel is created once and reused.
    }

    [Fact]
    public async Task WorkerTransport_RecreatesChannelWhenCachedChannelIsClosed()
    {
        var first = new FakeRabbitMqChannel();
        var second = new FakeRabbitMqChannel();
        var factory = new FakeConnectionFactory(first, second);
        var transport = new RabbitMqWorkerTransport(Options.Create(new RabbitMqAsyncResponseOptions()), factory);

        await transport.PublishAsync(WorkerJob("c1", 1));
        // A protocol error (404/406 after an exchange delete/redeclare) closes the channel server-side;
        // the cached object silently goes dead. The next publish must not use it forever.
        first.IsOpen = false;

        await transport.PublishAsync(WorkerJob("c2", 2));

        Assert.Equal(2, factory.Connection.CreateChannelCalls);
        Assert.Equal(1, first.DisposeCalls); // the dead channel is disposed, not leaked
        Assert.Single(first.Published);
        Assert.Single(second.Published);
        Assert.Equal(0, factory.Connection.DisposeCalls); // the still-open connection is reused
    }

    [Fact]
    public async Task WorkerTransport_RecreatesConnectionWhenCachedConnectionIsClosed()
    {
        var firstChannel = new FakeRabbitMqChannel();
        var secondChannel = new FakeRabbitMqChannel();
        var firstConnection = new FakeRabbitMqConnection(firstChannel);
        var secondConnection = new FakeRabbitMqConnection(secondChannel);
        var factory = new SequencedConnectionFactory(firstConnection, secondConnection);
        var transport = new RabbitMqWorkerTransport(Options.Create(new RabbitMqAsyncResponseOptions()), factory);

        await transport.PublishAsync(WorkerJob("c1", 1));
        // With AutomaticRecoveryEnabled = false a dropped connection stays closed for good and its
        // cached channel dies with it; both must be replaced, not returned from the cache.
        firstChannel.IsOpen = false;
        firstConnection.IsOpen = false;

        await transport.PublishAsync(WorkerJob("c2", 2));

        Assert.Equal(1, firstChannel.DisposeCalls);
        Assert.Equal(1, firstConnection.DisposeCalls);
        Assert.Single(secondChannel.Published);
        Assert.Equal(1, secondConnection.CreateChannelCalls);
    }

    [Fact]
    public async Task WorkerTransport_WhenTopologySetupFails_DisposesCreatedChannelAndRecovers()
    {
        var first = new FakeRabbitMqChannel { ThrowOnExchangeDeclare = new InvalidOperationException("declare boom") };
        var second = new FakeRabbitMqChannel();
        var factory = new FakeConnectionFactory(first, second);
        var transport = new RabbitMqWorkerTransport(Options.Create(new RabbitMqAsyncResponseOptions()), factory);

        await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(WorkerJob("c1", 1)));
        Assert.Equal(1, first.DisposeCalls); // the never-cached channel is released, not leaked

        // The transport retries with a fresh channel on the reused connection.
        await transport.PublishAsync(WorkerJob("c2", 2));
        Assert.Single(second.Published);
        Assert.Equal(2, factory.Connection.CreateChannelCalls);
    }

    [Fact]
    public async Task WorkerTransport_DisposeAsync_SwallowsCloseFailures()
    {
        var channel = new FakeRabbitMqChannel { ThrowOnClose = new InvalidOperationException("channel close boom") };
        var factory = new FakeConnectionFactory(channel);
        factory.Connection.ThrowOnClose = new InvalidOperationException("connection close boom");
        var transport = new RabbitMqWorkerTransport(Options.Create(new RabbitMqAsyncResponseOptions()), factory);

        await transport.PublishAsync(WorkerJob("corr", 1));

        // Close failures during shutdown are best-effort: dispose still completes and disposes resources.
        await transport.DisposeAsync();

        Assert.Equal(1, channel.CloseCalls);
        Assert.Equal(1, channel.DisposeCalls);
        Assert.Equal(1, factory.Connection.CloseCalls);
        Assert.Equal(1, factory.Connection.DisposeCalls);
    }

    // ---------- Reply target provider: unconfigured targets and queue metadata ----------

    [Fact]
    public void ReplyTargetProvider_WhenDefaultTargetNotConfigured_Throws()
    {
        var provider = new RabbitMqReplyTargetProvider(Options.Create(new RabbitMqAsyncResponseOptions
        {
            ResponseExchange = "",
            ResponseRoutingKey = ""
        }));

        Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget());
    }

    [Fact]
    public void ReplyTargetProvider_WhenNamedTargetMissing_Throws()
    {
        var provider = new RabbitMqReplyTargetProvider(Options.Create(new RabbitMqAsyncResponseOptions()));

        var ex = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("does-not-exist"));
        Assert.Contains("does-not-exist", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ReplyTargetProvider_FallsBackToResponseQueueWhenTargetQueueMissing()
    {
        var options = new RabbitMqAsyncResponseOptions { ResponseQueue = "shared.response.q" };
        options.AddReplyTarget("regional", "regional.exchange", "regional.route");

        var target = new RabbitMqReplyTargetProvider(Options.Create(options)).GetReplyTarget("regional");

        Assert.Equal("shared.response.q", target.Properties["queue"]);
    }

    [Fact]
    public void ReplyTargetProvider_OmitsQueueWhenNoneConfigured()
    {
        var options = new RabbitMqAsyncResponseOptions { ResponseQueue = "" };
        options.AddReplyTarget("regional", "regional.exchange", "regional.route");

        var target = new RabbitMqReplyTargetProvider(Options.Create(options)).GetReplyTarget("regional");

        Assert.False(target.Properties.ContainsKey("queue"));
    }

    // ---------- Correlation extractor: null/blank/invalid bodies and header conversions ----------

    [Fact]
    public void CorrelationExtractor_NoJsonPaths_ReturnsNull()
    {
        var options = new RabbitMqAsyncResponseOptions { CorrelationIdJsonPaths = [] };

        Assert.Null(RabbitMqCorrelationIdExtractor.Extract(
            Delivery("""{"CorrelationId":"x"}"""),
            """{"CorrelationId":"x"}""",
            options));
    }

    [Fact]
    public void CorrelationExtractor_WhitespaceBody_ReturnsNull()
        => Assert.Null(RabbitMqCorrelationIdExtractor.Extract(
            Delivery("   "),
            "   ",
            new RabbitMqAsyncResponseOptions()));

    [Fact]
    public void CorrelationExtractor_InvalidJson_ReturnsNull()
        => Assert.Null(RabbitMqCorrelationIdExtractor.Extract(
            Delivery("{not-json"),
            "{not-json",
            new RabbitMqAsyncResponseOptions()));

    [Fact]
    public void CorrelationExtractor_NullJsonRoot_ReturnsNull()
        => Assert.Null(RabbitMqCorrelationIdExtractor.Extract(
            Delivery("null"),
            "null",
            new RabbitMqAsyncResponseOptions()));

    [Fact]
    public void CorrelationExtractor_NoMatchingPath_ReturnsNull()
        => Assert.Null(RabbitMqCorrelationIdExtractor.Extract(
            Delivery("""{"Unrelated":"value"}"""),
            """{"Unrelated":"value"}""",
            new RabbitMqAsyncResponseOptions()));

    [Fact]
    public void CorrelationExtractor_NullHeaderValue_FallsThroughToJson()
    {
        var options = new RabbitMqAsyncResponseOptions { CorrelationIdHeader = "cid" };
        var delivery = Delivery(
            """{"CorrelationId":"from-json"}""",
            new BasicProperties { Headers = new Dictionary<string, object?> { ["cid"] = null } });

        Assert.Equal("from-json", RabbitMqCorrelationIdExtractor.Extract(
            delivery,
            """{"CorrelationId":"from-json"}""",
            options));
    }

    [Fact]
    public void CorrelationExtractor_ReadOnlyMemoryHeader_IsDecoded()
    {
        var options = new RabbitMqAsyncResponseOptions { CorrelationIdHeader = "cid" };
        var delivery = Delivery(
            "{}",
            new BasicProperties
            {
                Headers = new Dictionary<string, object?> { ["cid"] = new ReadOnlyMemory<byte>("from-memory"u8.ToArray()) }
            });

        Assert.Equal("from-memory", RabbitMqCorrelationIdExtractor.Extract(delivery, "{}", options));
    }

    [Fact]
    public void CorrelationExtractor_NonStringHeader_UsesToString()
    {
        var options = new RabbitMqAsyncResponseOptions { CorrelationIdHeader = "cid" };
        var delivery = Delivery(
            "{}",
            new BasicProperties { Headers = new Dictionary<string, object?> { ["cid"] = 12345 } });

        Assert.Equal("12345", RabbitMqCorrelationIdExtractor.Extract(delivery, "{}", options));
    }

    [Fact]
    public void CorrelationExtractor_CaseInsensitivePropertyMatch()
    {
        var options = new RabbitMqAsyncResponseOptions { CorrelationIdJsonPaths = ["CorrelationId"] };

        Assert.Equal("ci", RabbitMqCorrelationIdExtractor.Extract(
            Delivery("""{"correlationid":"ci"}"""),
            """{"correlationid":"ci"}""",
            options));
    }

    [Fact]
    public void CorrelationExtractor_UnwrapsNestedJsonStringValue()
    {
        var options = new RabbitMqAsyncResponseOptions
        {
            CorrelationIdJsonPaths = ["CustomParameters.CorrelationId"]
        };
        const string json = """{"CustomParameters":"{\"CorrelationId\":\"nested\"}"}""";

        Assert.Equal("nested", RabbitMqCorrelationIdExtractor.Extract(Delivery(json), json, options));
    }

    [Fact]
    public void CorrelationExtractor_InvalidNestedJsonStringMidPath_ReturnsNull()
    {
        var options = new RabbitMqAsyncResponseOptions
        {
            CorrelationIdJsonPaths = ["CustomParameters.CorrelationId"]
        };
        const string json = """{"CustomParameters":"{ not json"}""";

        Assert.Null(RabbitMqCorrelationIdExtractor.Extract(Delivery(json), json, options));
    }

    [Fact]
    public void CorrelationExtractor_NonStringJsonValue_ReturnsStringForm()
    {
        var options = new RabbitMqAsyncResponseOptions { CorrelationIdJsonPaths = ["CorrelationId"] };

        Assert.Equal("12345", RabbitMqCorrelationIdExtractor.Extract(
            Delivery("""{"CorrelationId":12345}"""),
            """{"CorrelationId":12345}""",
            options));
    }

    [Fact]
    public void CorrelationExtractor_WhitespacePathSegment_ReturnsNull()
    {
        var options = new RabbitMqAsyncResponseOptions { CorrelationIdJsonPaths = [" "] };

        Assert.Null(RabbitMqCorrelationIdExtractor.Extract(Delivery("{}"), "{}", options));
    }

    // ---------- Topology declaration + message properties ----------

    [Fact]
    public async Task Topology_WhenDeclareTopologyDisabled_SkipsWorkerDeclares()
    {
        var channel = new FakeRabbitMqChannel();

        await RabbitMqTopology.EnsureWorkerAsync(channel, new RabbitMqAsyncResponseOptions { DeclareTopology = false });

        Assert.Empty(channel.ExchangeDeclares);
        Assert.Empty(channel.QueueDeclares);
        Assert.Empty(channel.QueueBinds);
    }

    [Fact]
    public async Task Topology_WhenDeclareTopologyDisabled_SkipsResponseDeclares()
    {
        var channel = new FakeRabbitMqChannel();

        await RabbitMqTopology.EnsureResponseAsync(channel, new RabbitMqAsyncResponseOptions { DeclareTopology = false });

        Assert.Empty(channel.ExchangeDeclares);
        Assert.Empty(channel.QueueDeclares);
        Assert.Empty(channel.QueueBinds);
    }

    [Fact]
    public async Task Topology_EnsureResponse_DeclaresExchangeQueueAndBinding()
    {
        var channel = new FakeRabbitMqChannel();

        await RabbitMqTopology.EnsureResponseAsync(channel, new RabbitMqAsyncResponseOptions
        {
            ResponseExchange = "resp.ex",
            ResponseQueue = "resp.q",
            ResponseRoutingKey = "resp.rk"
        });

        Assert.Contains(channel.ExchangeDeclares, item => item.Exchange == "resp.ex" && item.Durable);
        Assert.Contains(channel.QueueDeclares, item => item.Queue == "resp.q" && item.Durable);
        Assert.Contains(channel.QueueBinds, item => item is { Queue: "resp.q", Exchange: "resp.ex", RoutingKey: "resp.rk" });
    }

    [Fact]
    public void CreatePersistentJsonProperties_NullCorrelation_OmitsCorrelationAndHeaders()
    {
        var properties = RabbitMqTopology.CreatePersistentJsonProperties(correlationId: null, correlationHeader: "cid");

        Assert.Equal("application/json", properties.ContentType);
        Assert.True(properties.Persistent);
        Assert.False(string.IsNullOrWhiteSpace(properties.MessageId));
        Assert.Null(properties.CorrelationId);
        Assert.Null(properties.Headers);
    }

    [Fact]
    public void CreatePersistentJsonProperties_BlankHeaderName_SetsCorrelationWithoutHeaders()
    {
        var properties = RabbitMqTopology.CreatePersistentJsonProperties(correlationId: "corr-1", correlationHeader: "");

        Assert.Equal("corr-1", properties.CorrelationId);
        Assert.Null(properties.Headers);
    }

    [Fact]
    public async Task Topology_WhenDeadLetterConfigured_DeclaresDlxAndTagsQueue()
    {
        var channel = new FakeRabbitMqChannel();
        var options = new RabbitMqAsyncResponseOptions
        {
            WorkerExchange = "w.ex",
            WorkerQueue = "w.q",
            WorkerRoutingKey = "w.rk",
            DeadLetterExchange = "dlx",
            DeadLetterQueue = "dlq",
            DeadLetterRoutingKey = "dead"
        };

        await RabbitMqTopology.EnsureWorkerAsync(channel, options);

        Assert.Contains(channel.ExchangeDeclares, e => e.Exchange == "dlx");
        Assert.Contains(channel.QueueBinds, b => b is { Queue: "dlq", Exchange: "dlx", RoutingKey: "dead" });

        var workerQueue = Assert.Single(channel.QueueDeclares, q => q.Queue == "w.q");
        Assert.NotNull(workerQueue.Arguments);
        Assert.Equal("dlx", workerQueue.Arguments!["x-dead-letter-exchange"]);
        Assert.Equal("dead", workerQueue.Arguments["x-dead-letter-routing-key"]);
    }

    [Fact]
    public async Task Topology_DeadLetterWithoutRoutingKey_BindsWithSourceRoutingKey()
    {
        var channel = new FakeRabbitMqChannel();
        var options = new RabbitMqAsyncResponseOptions
        {
            ResponseExchange = "r.ex",
            ResponseQueue = "r.q",
            ResponseRoutingKey = "r.rk",
            DeadLetterExchange = "dlx",
            DeadLetterQueue = "dlq"
        };

        await RabbitMqTopology.EnsureResponseAsync(channel, options);

        Assert.Contains(channel.QueueBinds, b => b is { Queue: "dlq", Exchange: "dlx", RoutingKey: "r.rk" });
        var responseQueue = Assert.Single(channel.QueueDeclares, q => q.Queue == "r.q");
        Assert.Equal("dlx", responseQueue.Arguments!["x-dead-letter-exchange"]);
        Assert.False(responseQueue.Arguments.ContainsKey("x-dead-letter-routing-key"));
    }

    [Fact]
    public async Task Topology_WithoutDeadLetter_LeavesQueueArgumentsNull()
    {
        var channel = new FakeRabbitMqChannel();

        await RabbitMqTopology.EnsureWorkerAsync(channel, new RabbitMqAsyncResponseOptions
        {
            WorkerExchange = "w.ex",
            WorkerQueue = "w.q",
            WorkerRoutingKey = "w.rk"
        });

        var workerQueue = Assert.Single(channel.QueueDeclares);
        Assert.Null(workerQueue.Arguments);
    }

    // ---------- Subscriber hosted-service: queue validation + startup retry loop ----------

    [Fact]
    public async Task WorkerSubscriber_QueueNameRequiresConfiguredWorkerQueue()
    {
        var subscriber = new RabbitMqWorkerSubscriber(
            Options.Create(new RabbitMqAsyncResponseOptions { WorkerQueue = "" }),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<RabbitMqWorkerSubscriber>.Instance,
            new FakeConnectionFactory());

        await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeExecuteAsync(subscriber));
    }

    [Fact]
    public async Task ResponseSubscriber_QueueNameRequiresConfiguredResponseQueue()
    {
        var subscriber = new RabbitMqResponseIngressSubscriber(
            Options.Create(new RabbitMqAsyncResponseOptions { ResponseQueue = "" }),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<RabbitMqResponseIngressSubscriber>.Instance,
            new FakeConnectionFactory());

        await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeExecuteAsync(subscriber));
    }

    [Fact]
    public async Task Subscriber_WhenStartupFails_RetriesThenStopsOnCancellation()
    {
        var logger = new CapturingLogger<RabbitMqWorkerSubscriber>();
        var factory = new ThrowingConnectionFactory();
        var subscriber = new RabbitMqWorkerSubscriber(
            Options.Create(new RabbitMqAsyncResponseOptions
            {
                NetworkRecoveryInterval = TimeSpan.FromMilliseconds(20)
            }),
            Mock.Of<IAsyncResponseIngress>(),
            logger,
            factory);

        await subscriber.StartAsync(CancellationToken.None);
        // Two attempts proves the retry delay elapsed and the loop re-entered after a failure.
        await WaitUntilAsync(() => factory.Attempts >= 2);
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Contains(logger.Snapshot(), entry => entry.Level == LogLevel.Warning);
    }

    [Fact]
    public void Subscriber_RejectsNonPositiveRecoveryInterval()
    {
        // The RabbitMQ client uses the interval DIRECTLY in its automatic-recovery loop: a
        // negative value faults (and terminates) that loop, zero spins it. The old "fall back to
        // 5 seconds" subscriber behavior is gone — a non-positive interval is a configuration
        // error rejected before any connection is attempted.
        Assert.Throws<InvalidOperationException>(() => RabbitMqMessageDispatcher.ValidateOptions(
            new RabbitMqAsyncResponseOptions { NetworkRecoveryInterval = TimeSpan.Zero },
            new RabbitMqSubscriberOptions(),
            RabbitMqSubscriberRole.Worker));
        Assert.Throws<InvalidOperationException>(() => RabbitMqMessageDispatcher.ValidateOptions(
            new RabbitMqAsyncResponseOptions { NetworkRecoveryInterval = TimeSpan.FromSeconds(-1) },
            new RabbitMqSubscriberOptions(),
            RabbitMqSubscriberRole.Worker));
    }

    [Fact]
    public async Task Subscriber_WhenCancelledDuringConnect_StopsGracefully()
    {
        using var cts = new CancellationTokenSource();
        var factory = new ThrowingConnectionFactory(cts);
        var subscriber = new RabbitMqWorkerSubscriber(
            Options.Create(new RabbitMqAsyncResponseOptions()),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<RabbitMqWorkerSubscriber>.Instance,
            factory);

        // The connect attempt cancels the stopping token and throws; ExecuteAsync must observe the
        // cancellation and return instead of surfacing the OperationCanceledException.
        await InvokeExecuteAsync(subscriber, cts.Token);

        Assert.Equal(1, factory.Attempts);
    }

    [Fact]
    public async Task Subscriber_WhenConsumerTerminatesMidRun_RebuildsSubscription()
    {
        var firstChannel = new FakeRabbitMqChannel();
        var secondChannel = new FakeRabbitMqChannel();
        var factory = new FakeConnectionFactory(firstChannel, secondChannel);
        var logger = new CapturingLogger<RabbitMqWorkerSubscriber>();
        var subscriber = new RabbitMqWorkerSubscriber(
            Options.Create(new RabbitMqAsyncResponseOptions
            {
                WorkerExchange = "worker.ex",
                WorkerQueue = "worker.q",
                WorkerRoutingKey = "worker.rk",
                NetworkRecoveryInterval = TimeSpan.FromMilliseconds(20)
            }),
            Mock.Of<IAsyncResponseIngress>(),
            logger,
            factory);

        await using var host = new HostedServiceRun(subscriber);
        await firstChannel.WaitForConsumerAsync();

        // Broker-side basic.cancel (queue deleted) or channel-level close: deliveries stop while the
        // host keeps running; nothing throws into the subscriber on its own.
        firstChannel.TerminateConsumer("the broker canceled the consumer");

        // The retry loop must dispose the dead channel/connection and consume again on a fresh channel.
        await secondChannel.WaitForConsumerAsync();
        Assert.Equal(1, firstChannel.DisposeCalls);
        Assert.True(factory.Connection.DisposeCalls >= 1);
        Assert.Contains(
            logger.Snapshot(),
            entry => entry.Level == LogLevel.Warning
                && entry.Message.Contains("worker.q", StringComparison.Ordinal)
                && entry.Exception?.Message.Contains("stopped receiving", StringComparison.Ordinal) == true);
    }

    [Fact]
    public async Task Subscriber_NormalHostStop_ExitsCleanlyWithoutRebuild()
    {
        var channel = new FakeRabbitMqChannel();
        var factory = new FakeConnectionFactory(channel);
        var subscriber = new RabbitMqWorkerSubscriber(
            Options.Create(new RabbitMqAsyncResponseOptions
            {
                WorkerExchange = "worker.ex",
                WorkerQueue = "worker.q",
                WorkerRoutingKey = "worker.rk"
            }),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<RabbitMqWorkerSubscriber>.Instance,
            factory);

        await using (new HostedServiceRun(subscriber))
        {
            await channel.WaitForConsumerAsync();
        }

        // Clean shutdown cancels the consumer, which completes the termination task too (cancel-ok
        // raises UnregisteredAsync); the stopping-token guard must keep that from causing a rebuild.
        Assert.Equal("consumer-tag", Assert.Single(channel.Cancels));
        Assert.Equal(1, channel.ConsumeCalls);
        Assert.Equal(1, factory.Connection.CreateChannelCalls);
        Assert.Equal(1, channel.CloseCalls);
        Assert.Equal(1, factory.Connection.CloseCalls);
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
            await Task.Delay(10, deadline.Token);
    }

    private static Task InvokeExecuteAsync(RabbitMqSubscriberService subscriber, CancellationToken cancellationToken = default)
    {
        var method = typeof(RabbitMqSubscriberService).GetMethod(
            "ExecuteAsync",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        return (Task)method.Invoke(subscriber, [cancellationToken])!;
    }

    private static WorkerJobEnvelope WorkerJob(string correlationId, int orderId)
        => new()
        {
            CorrelationId = correlationId,
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
                MethodName = nameof(IRecoverySpy.OnWorkerJob),
                Params = [CallbackParam.ForValue(orderId)]
            }
        };

    private static RabbitMqDelivery Delivery(
        string body,
        BasicProperties? properties = null,
        ulong deliveryTag = 1)
        => new(
            "consumer",
            deliveryTag,
            Redelivered: false,
            "exchange",
            "route",
            properties ?? new BasicProperties(),
            Encoding.UTF8.GetBytes(body),
            CancellationToken.None);

    private static string? AssertHeader(BasicProperties properties, string key)
    {
        Assert.NotNull(properties.Headers);
        Assert.True(properties.Headers!.TryGetValue(key, out var value));
        return value switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            string s => s,
            _ => value?.ToString()
        };
    }

    private sealed class HostedServiceRun : IAsyncDisposable
    {
        private readonly IHostedService _service;
        private readonly CancellationTokenSource _cts = new();

        public HostedServiceRun(IHostedService service)
        {
            _service = service;
            _service.StartAsync(_cts.Token).GetAwaiter().GetResult();
        }

        public async ValueTask DisposeAsync()
        {
            await _service.StopAsync(CancellationToken.None);
            _cts.Dispose();
        }
    }

    private sealed class FakeConnectionFactory(params IRabbitMqChannel[] channels) : IRabbitMqConnectionFactory
    {
        public FakeRabbitMqConnection Connection { get; } =
            new(channels.Length == 0 ? [new FakeRabbitMqChannel()] : channels);

        public Task<IRabbitMqConnection> CreateConnectionAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IRabbitMqConnection>(Connection);
    }

    private sealed class SequencedConnectionFactory(params IRabbitMqConnection[] connections) : IRabbitMqConnectionFactory
    {
        private int _attempts;

        public Task<IRabbitMqConnection> CreateConnectionAsync(CancellationToken cancellationToken = default)
        {
            var index = Math.Min(Interlocked.Increment(ref _attempts) - 1, connections.Length - 1);
            return Task.FromResult(connections[index]);
        }
    }

    private sealed class FakeRabbitMqConnection(params IRabbitMqChannel[] channels) : IRabbitMqConnection
    {
        public bool IsOpen { get; set; } = true;
        public int CloseCalls { get; private set; }
        public int DisposeCalls { get; private set; }
        public int CreateChannelCalls { get; private set; }
        public bool? PublisherConfirmationsRequested { get; private set; }
        public Exception? ThrowOnClose { get; set; }

        // Sequential channels (last repeats) so channel-recreation tests can hand out a fresh one.
        public Task<IRabbitMqChannel> CreateChannelAsync(bool publisherConfirmations = false, CancellationToken cancellationToken = default)
        {
            CreateChannelCalls++;
            PublisherConfirmationsRequested = publisherConfirmations;
            return Task.FromResult(channels[Math.Min(CreateChannelCalls - 1, channels.Length - 1)]);
        }

        public Task CloseAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            CloseCalls++;
            if (ThrowOnClose is not null)
                throw ThrowOnClose;
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FakeRabbitMqChannel : IRabbitMqChannel
    {
        private readonly TaskCompletionSource _consumerReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<string> _terminated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private Func<RabbitMqDelivery, Task>? _handler;

        public bool IsOpen { get; set; } = true;
        public List<(string Exchange, string Type, bool Durable, bool AutoDelete)> ExchangeDeclares { get; } = [];
        public List<(string Queue, bool Durable, bool Exclusive, bool AutoDelete, IDictionary<string, object?>? Arguments)> QueueDeclares { get; } = [];
        public List<(string Queue, string Exchange, string RoutingKey)> QueueBinds { get; } = [];
        public List<(string Exchange, string RoutingKey, BasicProperties Properties, ReadOnlyMemory<byte> Body)> Published { get; } = [];
        public List<ulong> Acks { get; } = [];
        public List<(ulong DeliveryTag, bool Requeue)> Nacks { get; } = [];
        public List<string> Cancels { get; } = [];
        public int ConsumeCalls { get; private set; }
        public ushort PrefetchCount { get; private set; }
        public int CloseCalls { get; private set; }
        public int DisposeCalls { get; private set; }
        public Exception? ThrowOnPublish { get; init; }
        public Exception? ThrowOnClose { get; init; }
        public Exception? ThrowOnExchangeDeclare { get; init; }

        public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, CancellationToken cancellationToken = default)
        {
            if (ThrowOnExchangeDeclare is not null)
                throw ThrowOnExchangeDeclare;

            ExchangeDeclares.Add((exchange, type, durable, autoDelete));
            return Task.CompletedTask;
        }

        public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object?>? arguments = null, CancellationToken cancellationToken = default)
        {
            QueueDeclares.Add((queue, durable, exclusive, autoDelete, arguments));
            return Task.CompletedTask;
        }

        public Task QueueBindAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken = default)
        {
            QueueBinds.Add((queue, exchange, routingKey));
            return Task.CompletedTask;
        }

        public Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken = default)
        {
            PrefetchCount = prefetchCount;
            return Task.CompletedTask;
        }

        public ValueTask BasicPublishAsync(string exchange, string routingKey, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
        {
            if (ThrowOnPublish is not null)
                throw ThrowOnPublish;

            Published.Add((exchange, routingKey, properties, body));
            return ValueTask.CompletedTask;
        }

        public Task<RabbitMqConsumer> BasicConsumeAsync(string queue, Func<RabbitMqDelivery, Task> handler, CancellationToken cancellationToken = default)
        {
            ConsumeCalls++;
            _handler = handler;
            _consumerReady.TrySetResult();
            return Task.FromResult(new RabbitMqConsumer("consumer-tag", _terminated.Task));
        }

        public Task WaitForConsumerAsync() => _consumerReady.Task.WaitAsync(TimeSpan.FromSeconds(5));

        public Task DeliverAsync(RabbitMqDelivery delivery)
            => (_handler ?? throw new InvalidOperationException("Consumer was not started."))(delivery);

        /// <summary>Simulates a broker-side basic.cancel or a channel-level close: deliveries stop, no call fails.</summary>
        public void TerminateConsumer(string reason) => _terminated.TrySetResult(reason);

        public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default)
        {
            Cancels.Add(consumerTag);
            // The real adapter completes the termination task on a client cancel too (cancel-ok raises
            // UnregisteredAsync); mirror that so shutdown tests exercise the stopping-token guard.
            _terminated.TrySetResult("client-initiated cancel");
            return Task.CompletedTask;
        }

        public ValueTask BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken = default)
        {
            Acks.Add(deliveryTag);
            return ValueTask.CompletedTask;
        }

        public ValueTask BasicNackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default)
        {
            Nacks.Add((deliveryTag, requeue));
            return ValueTask.CompletedTask;
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            CloseCalls++;
            if (ThrowOnClose is not null)
                throw ThrowOnClose;
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowingConnectionFactory(CancellationTokenSource? cancelOnConnect = null) : IRabbitMqConnectionFactory
    {
        private int _attempts;

        public int Attempts => Volatile.Read(ref _attempts);

        public Task<IRabbitMqConnection> CreateConnectionAsync(CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _attempts);

            if (cancelOnConnect is not null)
            {
                cancelOnConnect.Cancel();
                throw new OperationCanceledException(cancelOnConnect.Token);
            }

            throw new InvalidOperationException("broker unreachable");
        }
    }

    private sealed class FlakyConnectionFactory(IRabbitMqConnection connection, int failuresBeforeSuccess) : IRabbitMqConnectionFactory
    {
        private int _attempts;

        public int Attempts => Volatile.Read(ref _attempts);

        public Task<IRabbitMqConnection> CreateConnectionAsync(CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _attempts) <= failuresBeforeSuccess)
                throw new InvalidOperationException("transient connect failure");

            return Task.FromResult(connection);
        }
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        private readonly object _gate = new();
        private readonly List<RecordedLog> _entries = [];

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull
            => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_gate)
                _entries.Add(new RecordedLog(logLevel, formatter(state, exception), exception));
        }

        public IReadOnlyList<RecordedLog> Snapshot()
        {
            lock (_gate)
                return _entries.ToArray();
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();

            public void Dispose()
            {
            }
        }
    }

    private sealed record RecordedLog(LogLevel Level, string Message, Exception? Exception);
}
