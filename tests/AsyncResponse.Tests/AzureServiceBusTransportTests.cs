using AsyncResponse.Transports.AzureServiceBus;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class AzureServiceBusTransportTests
{
    [Fact]
    public void WithAzureServiceBusTransport_ReplacesWorkerTransportAndReplyTargetProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IAzureServiceBusClient>(new FakeServiceBusClient());

        var provider = services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithAzureServiceBusTransport(options =>
            {
                options.ConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
                options.WorkerQueue = "workers";
                options.ResponseQueue = "responses";
            })
            .Services
            .BuildServiceProvider();

        Assert.IsType<AzureServiceBusWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.IsType<AzureServiceBusReplyTargetProvider>(provider.GetRequiredService<IAsyncResponseReplyTargetProvider>());
        Assert.Equal(AzureServiceBusAsyncResponseOptions.TransportName, provider.GetRequiredService<AsyncResponseTransportMarker>().Name);
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is AzureServiceBusWorkerSubscriber);
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is AzureServiceBusResponseIngressSubscriber);
    }

    [Fact]
    public async Task SenderAdapter_MapsOutboundMessageAndDelegatesClose()
    {
        var captured = new List<ServiceBusMessage>();
        var sender = new Mock<ServiceBusSender>();
        sender
            .Setup(s => s.SendMessageAsync(It.IsAny<ServiceBusMessage>(), It.IsAny<CancellationToken>()))
            .Callback<ServiceBusMessage, CancellationToken>((message, _) => captured.Add(message))
            .Returns(Task.CompletedTask);
        sender
            .Setup(s => s.CloseAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        sender
            .Setup(s => s.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var adapter = new AzureServiceBusSenderAdapter(sender.Object);

        await adapter.SendMessageAsync(new AzureServiceBusOutboundMessage(
            """{"ok":true}""",
            "message-1",
            "corr-1",
            new Dictionary<string, object?> { ["cid"] = "corr-1", ["tenant"] = "acme" }));
        await adapter.CloseAsync();
        await adapter.DisposeAsync();

        var message = Assert.Single(captured);
        Assert.Equal("""{"ok":true}""", message.Body.ToString());
        Assert.Equal("application/json", message.ContentType);
        Assert.Equal("message-1", message.MessageId);
        Assert.Equal("corr-1", message.CorrelationId);
        Assert.Equal("corr-1", message.ApplicationProperties["cid"]);
        Assert.Equal("acme", message.ApplicationProperties["tenant"]);
        sender.Verify(s => s.CloseAsync(It.IsAny<CancellationToken>()), Times.Once);
        sender.Verify(s => s.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task ReceiverAdapter_WrapsReceivedMessagesAndDelegatesSettlement()
    {
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("""{"Status":2}"""),
            messageId: "message-1",
            correlationId: "corr-1",
            sequenceNumber: 123,
            deliveryCount: 4,
            properties: new Dictionary<string, object> { ["cid"] = "corr-1" });
        var receiver = new Mock<ServiceBusReceiver>();
        receiver
            .Setup(r => r.ReceiveMessagesAsync(16, TimeSpan.FromSeconds(1), It.IsAny<CancellationToken>()))
            .ReturnsAsync([received]);
        receiver
            .Setup(r => r.CompleteMessageAsync(received, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        receiver
            .Setup(r => r.AbandonMessageAsync(received, null, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        receiver
            .Setup(r => r.DeadLetterMessageAsync(received, "reason", "description", It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        receiver
            .Setup(r => r.RenewMessageLockAsync(received, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        receiver
            .Setup(r => r.CloseAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        receiver
            .Setup(r => r.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var adapter = new AzureServiceBusReceiverAdapter(receiver.Object, queueOverride: "responses");
        using var renewCancellation = new CancellationTokenSource();

        var deliveries = await adapter.ReceiveMessagesAsync(16, TimeSpan.FromSeconds(1));
        var delivery = Assert.Single(deliveries);
        await delivery.CompleteAsync();
        await delivery.AbandonAsync();
        await delivery.DeadLetterAsync("reason", "description");
        await delivery.RenewLockAsync(renewCancellation.Token);
        await adapter.CloseAsync();
        await adapter.DisposeAsync();

        Assert.Equal("responses", delivery.Queue);
        Assert.Equal("""{"Status":2}""", delivery.Body);
        Assert.Equal("message-1", delivery.MessageId);
        Assert.Equal("corr-1", delivery.CorrelationId);
        Assert.Equal(123, delivery.SequenceNumber);
        Assert.Equal(4, delivery.DeliveryCount);
        Assert.Equal("corr-1", delivery.ApplicationProperties["cid"]);
        receiver.Verify(r => r.CompleteMessageAsync(received, It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.AbandonMessageAsync(received, null, It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.DeadLetterMessageAsync(received, "reason", "description", It.IsAny<CancellationToken>()), Times.Once);
        // Renewal must forward the caller's token (not CancellationToken.None) so a stuck renew can
        // be interrupted when the batch settles or the subscriber stops.
        receiver.Verify(r => r.RenewMessageLockAsync(received, renewCancellation.Token), Times.Once);
        receiver.Verify(r => r.CloseAsync(It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task ReceiverAdapter_WhenNoMessages_ReturnsEmptyAndDisposesReceiver()
    {
        var receiver = new Mock<ServiceBusReceiver>();
        receiver
            .Setup(r => r.ReceiveMessagesAsync(1, TimeSpan.FromMilliseconds(1), It.IsAny<CancellationToken>()))
            .ReturnsAsync([]);
        receiver
            .Setup(r => r.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var adapter = new AzureServiceBusReceiverAdapter(receiver.Object);

        var deliveries = await adapter.ReceiveMessagesAsync(1, TimeSpan.FromMilliseconds(1));
        await adapter.DisposeAsync();

        Assert.Empty(deliveries);
        receiver.Verify(r => r.DisposeAsync(), Times.Once);
    }

    [Fact]
    public void ClientAdapter_CreatesSenderAndReceiverFromSdkClient()
    {
        var sdkClient = new Mock<ServiceBusClient>();
        var sender = new Mock<ServiceBusSender>().Object;
        var receiver = new Mock<ServiceBusReceiver>().Object;
        sdkClient.Setup(c => c.CreateSender("workers")).Returns(sender);
        sdkClient
            .Setup(c => c.CreateReceiver(
                "responses",
                It.Is<ServiceBusReceiverOptions>(o => o.ReceiveMode == ServiceBusReceiveMode.PeekLock && o.PrefetchCount == 12)))
            .Returns(receiver);
        var adapter = new AzureServiceBusClientAdapter(sdkClient.Object, ownsClient: false);

        var createdSender = adapter.CreateSender("workers");
        var createdReceiver = adapter.CreateReceiver("responses", new AzureServiceBusSubscriberOptions { PrefetchCount = 12 });

        Assert.IsType<AzureServiceBusSenderAdapter>(createdSender);
        Assert.IsType<AzureServiceBusReceiverAdapter>(createdReceiver);
    }

    [Fact]
    public async Task ClientAdapter_DisposesOwnedSdkClientOnly()
    {
        var ownedClient = new Mock<ServiceBusClient>();
        ownedClient.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        await new AzureServiceBusClientAdapter(ownedClient.Object, ownsClient: true).DisposeAsync();
        ownedClient.Verify(c => c.DisposeAsync(), Times.Once);

        var externalClient = new Mock<ServiceBusClient>();
        externalClient.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        await new AzureServiceBusClientAdapter(externalClient.Object, ownsClient: false).DisposeAsync();
        externalClient.Verify(c => c.DisposeAsync(), Times.Never);
    }

    [Fact]
    public void ClientResolver_UsesRegisteredServiceBusClientOrRequiresConnectionString()
    {
        var registeredClient = new Mock<ServiceBusClient>().Object;
        var providerWithClient = new ServiceCollection()
            .AddSingleton(registeredClient)
            .Configure<AzureServiceBusAsyncResponseOptions>(_ => { })
            .BuildServiceProvider();

        Assert.IsType<AzureServiceBusClientAdapter>(AzureServiceBusClientResolver.Create(providerWithClient));

        var providerWithoutConnectionString = new ServiceCollection()
            .Configure<AzureServiceBusAsyncResponseOptions>(_ => { })
            .BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            AzureServiceBusClientResolver.Create(providerWithoutConnectionString));
        Assert.Contains(nameof(AzureServiceBusAsyncResponseOptions.ConnectionString), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ClientResolver_CreatesOwnedClientFromConnectionString()
    {
        var provider = new ServiceCollection()
            .Configure<AzureServiceBusAsyncResponseOptions>(options =>
            {
                options.ConnectionString = DevelopmentConnectionString;
            })
            .BuildServiceProvider();

        await using var client = AzureServiceBusClientResolver.Create(provider);

        Assert.IsType<AzureServiceBusClientAdapter>(client);
    }

    [Fact]
    public void PublicWorkerTransport_RequiresConnectionString()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            new AzureServiceBusWorkerTransport(Options.Create(new AzureServiceBusAsyncResponseOptions { ConnectionString = "" })));

        Assert.Contains(nameof(AzureServiceBusAsyncResponseOptions.ConnectionString), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PublicWorkerTransport_WithConnectionString_CanConstructAndDisposeWithoutOpeningSender()
    {
        var transport = new AzureServiceBusWorkerTransport(Options.Create(new AzureServiceBusAsyncResponseOptions
        {
            ConnectionString = DevelopmentConnectionString
        }));

        await transport.DisposeAsync();
    }

    [Fact]
    public void WorkerTransport_InvalidCommonOptionsThrow()
    {
        AssertInvalidCommon(
            options => options.ResponseQueue = options.WorkerQueue,
            nameof(AzureServiceBusAsyncResponseOptions.ResponseQueue));
        AssertInvalidCommon(
            options => options.MaxMessagesPerReceive = 0,
            nameof(AzureServiceBusAsyncResponseOptions.MaxMessagesPerReceive));
        AssertInvalidCommon(
            options => options.PublishMaxAttempts = 0,
            nameof(AzureServiceBusAsyncResponseOptions.PublishMaxAttempts));
        AssertInvalidCommon(
            options => options.ReceiveWaitTime = TimeSpan.Zero,
            nameof(AzureServiceBusAsyncResponseOptions.ReceiveWaitTime));
        AssertInvalidCommon(
            options => options.PublishRetryBaseDelay = TimeSpan.FromSeconds(2),
            nameof(AzureServiceBusAsyncResponseOptions.PublishRetryBaseDelay));
        AssertInvalidCommon(
            options => options.SubscriberRetryBaseDelay = TimeSpan.FromSeconds(2),
            nameof(AzureServiceBusAsyncResponseOptions.SubscriberRetryBaseDelay));
    }

    [Fact]
    public async Task WorkerTransport_PublishesSerializedJob()
    {
        var sender = new FakeSender();
        var client = new FakeServiceBusClient { Sender = sender };
        var transport = new AzureServiceBusWorkerTransport(
            Options.Create(new AzureServiceBusAsyncResponseOptions
            {
                WorkerQueue = "worker-q",
                ResponseQueue = "response-q",
                CorrelationIdProperty = "cid"
            }),
            client);

        await transport.PublishAsync(WorkerJob("corr-asb"));

        Assert.Equal("worker-q", Assert.Single(client.SenderQueues));
        var message = Assert.Single(sender.Messages);
        // MessageId is the duplicate-detection key and must never reuse the correlation id, or a
        // dedup-enabled queue silently drops the second job of a flow inside the detection window.
        Assert.NotEmpty(message.MessageId);
        Assert.NotEqual("corr-asb", message.MessageId);
        Assert.Equal("corr-asb", message.CorrelationId);
        Assert.Equal("corr-asb", message.ApplicationProperties["cid"]);
        var roundTripped = JsonSerializer.Deserialize<WorkerJobEnvelope>(message.Body);
        Assert.NotNull(roundTripped);
        Assert.Equal("corr-asb", roundTripped.CorrelationId);
        Assert.Equal("DoWork", roundTripped.Call.MethodName);
    }

    [Fact]
    public async Task WorkerTransport_JobsSharingACorrelationId_GetDistinctMessageIds()
    {
        var sender = new FakeSender();
        var transport = new AzureServiceBusWorkerTransport(
            Options.Create(new AzureServiceBusAsyncResponseOptions()),
            new FakeServiceBusClient { Sender = sender });

        await transport.PublishAsync(WorkerJob("corr-flow"));
        await transport.PublishAsync(WorkerJob("corr-flow"));

        Assert.Equal(2, sender.Messages.Count);
        Assert.NotEqual(sender.Messages[0].MessageId, sender.Messages[1].MessageId);
        Assert.All(sender.Messages, message => Assert.Equal("corr-flow", message.CorrelationId));
    }

    [Fact]
    public async Task WorkerTransport_WhenCorrelationIdIsBlank_GeneratesMessageIdWithoutCorrelationProperties()
    {
        var sender = new FakeSender();
        var transport = new AzureServiceBusWorkerTransport(
            Options.Create(new AzureServiceBusAsyncResponseOptions()),
            new FakeServiceBusClient { Sender = sender });

        await transport.PublishAsync(WorkerJob(" "));

        var message = Assert.Single(sender.Messages);
        Assert.NotEmpty(message.MessageId);
        Assert.Null(message.CorrelationId);
        Assert.False(message.ApplicationProperties.ContainsKey("correlationId"));
    }

    [Fact]
    public async Task WorkerTransport_RetriesTransientPublishFailures()
    {
        var sender = new FakeSender { FailuresBeforeSuccess = 2 };
        var transport = new AzureServiceBusWorkerTransport(
            Options.Create(new AzureServiceBusAsyncResponseOptions
            {
                PublishMaxAttempts = 3,
                PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            new FakeServiceBusClient { Sender = sender });

        await transport.PublishAsync(WorkerJob("corr-retry"));

        Assert.Equal(3, sender.SendAttempts);
        Assert.Single(sender.Messages);
    }

    [Fact]
    public async Task WorkerTransport_WhenTransientPublishKeepsFailing_Propagates()
    {
        var sender = new FakeSender { PublishException = new ServiceBusException(isTransient: true, "send failed") };
        var transport = new AzureServiceBusWorkerTransport(
            Options.Create(new AzureServiceBusAsyncResponseOptions
            {
                PublishMaxAttempts = 2,
                PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            new FakeServiceBusClient { Sender = sender });

        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => transport.PublishAsync(WorkerJob("corr-fail")));

        Assert.StartsWith("send failed", ex.Message, StringComparison.Ordinal);
        Assert.Equal(2, sender.SendAttempts);
    }

    [Fact]
    public async Task WorkerTransport_NonTransientServiceBusFailure_ThrowsWithoutRetry()
    {
        var sender = new FakeSender
        {
            PublishException = new ServiceBusException(
                isTransient: false,
                "message too large",
                reason: ServiceBusFailureReason.MessageSizeExceeded)
        };
        var transport = new AzureServiceBusWorkerTransport(
            Options.Create(new AzureServiceBusAsyncResponseOptions
            {
                PublishMaxAttempts = 3,
                PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            new FakeServiceBusClient { Sender = sender });

        await Assert.ThrowsAsync<ServiceBusException>(() => transport.PublishAsync(WorkerJob("corr-too-large")));

        Assert.Equal(1, sender.SendAttempts);
    }

    [Fact]
    public async Task WorkerTransport_NonServiceBusFailure_ThrowsWithoutRetry()
    {
        var sender = new FakeSender { PublishException = new InvalidOperationException("serialization failed") };
        var transport = new AzureServiceBusWorkerTransport(
            Options.Create(new AzureServiceBusAsyncResponseOptions
            {
                PublishMaxAttempts = 3,
                PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            new FakeServiceBusClient { Sender = sender });

        await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(WorkerJob("corr-fatal")));

        Assert.Equal(1, sender.SendAttempts);
    }

    [Fact]
    public async Task WorkerTransport_DisposeClosesSender()
    {
        var sender = new FakeSender();
        var transport = new AzureServiceBusWorkerTransport(
            Options.Create(new AzureServiceBusAsyncResponseOptions()),
            new FakeServiceBusClient { Sender = sender });

        await transport.PublishAsync(WorkerJob("corr-dispose"));
        await transport.DisposeAsync();
        await transport.DisposeAsync();

        Assert.Equal(1, sender.CloseCalls);
        Assert.Equal(1, sender.DisposeCalls);
    }

    [Fact]
    public async Task WorkerTransport_DisposeStillDisposesSenderWhenCloseFails()
    {
        var sender = new FakeSender { CloseException = new InvalidOperationException("close failed") };
        var transport = new AzureServiceBusWorkerTransport(
            Options.Create(new AzureServiceBusAsyncResponseOptions()),
            new FakeServiceBusClient { Sender = sender });

        await transport.PublishAsync(WorkerJob("corr-close-fail"));
        await transport.DisposeAsync();

        Assert.Equal(1, sender.CloseCalls);
        Assert.Equal(1, sender.DisposeCalls);
    }

    [Fact]
    public async Task WorkerTransport_NullJob_Throws()
    {
        var transport = new AzureServiceBusWorkerTransport(
            Options.Create(new AzureServiceBusAsyncResponseOptions()),
            new FakeServiceBusClient());

        await Assert.ThrowsAsync<ArgumentNullException>(() => transport.PublishAsync(null!));
    }

    [Fact]
    public void ReplyTargetProvider_UsesResponseQueueAsDefaultTarget()
    {
        var provider = new AzureServiceBusReplyTargetProvider(Options.Create(new AzureServiceBusAsyncResponseOptions
        {
            ResponseQueue = "responses",
            CorrelationIdProperty = "cid"
        }));

        var target = provider.GetReplyTarget();

        Assert.Equal("default", target.Name);
        Assert.Equal(AzureServiceBusAsyncResponseOptions.TransportName, target.Transport);
        Assert.Equal("responses", target.Address);
        Assert.Equal("responses", target.Properties["queue"]);
        Assert.Equal("cid", target.Properties["correlationIdProperty"]);
    }

    [Fact]
    public void ReplyTargetProvider_ResolvesNamedTargets()
    {
        var options = new AzureServiceBusAsyncResponseOptions();
        options.AddReplyTarget("regional", "regional-responses");
        options.ReplyTargets["regional"].Properties["region"] = "us";

        var target = new AzureServiceBusReplyTargetProvider(Options.Create(options)).GetReplyTarget("regional");

        Assert.Equal("regional", target.Name);
        Assert.Equal("regional-responses", target.Address);
        Assert.Equal("us", target.Properties["region"]);
    }

    [Fact]
    public void ReplyTargetProvider_MissingNamedTarget_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            new AzureServiceBusReplyTargetProvider(Options.Create(new AzureServiceBusAsyncResponseOptions()))
                .GetReplyTarget("missing"));

        Assert.Contains("missing", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CorrelationIdExtractor_ReadsSystemPropertyThenApplicationPropertyThenJson()
    {
        var options = new AzureServiceBusAsyncResponseOptions { CorrelationIdProperty = "cid" };

        Assert.Equal("from-system", AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(correlationId: "from-system"),
            "{}",
            options));

        Assert.Equal("from-property", AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(properties: new Dictionary<string, object?> { ["cid"] = "from-property" }),
            "{}",
            options));

        Assert.Equal("from-json", AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(body: """{"CustomParameters":{"CorrelationId":"from-json"}}"""),
            """{"CustomParameters":{"CorrelationId":"from-json"}}""",
            options));
    }

    [Fact]
    public void CorrelationIdExtractor_HandlesNestedJsonStringsAndInvalidJson()
    {
        var nested = """
            {
              "PubSubParams": {
                "CustomParameters": "{\"CorrelationId\":\"corr-nested\"}"
              }
            }
            """;

        Assert.Equal("corr-nested", AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(body: nested),
            nested,
            new AzureServiceBusAsyncResponseOptions()));

        Assert.Null(AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(body: "{not-json"),
            "{not-json",
            new AzureServiceBusAsyncResponseOptions()));
    }

    [Fact]
    public void CorrelationIdExtractor_ReturnsNull_WhenTouchedObjectHasExactDuplicateKey()
        // An object with a duplicate key cannot resolve a property, so the id is simply not in this
        // body: extraction reports "not found" and the ingress acknowledges the message as
        // unroutable. Throwing made it a handler failure, which on RabbitMQ's default cap of 0
        // requeued forever.
        => Assert.Null(AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(body: """{"CorrelationId":"1","CorrelationId":"2"}"""),
            """{"CorrelationId":"1","CorrelationId":"2"}""",
            new AzureServiceBusAsyncResponseOptions()));

    [Fact]
    public void CorrelationIdExtractor_HandlesApplicationPropertyTypes()
    {
        var options = new AzureServiceBusAsyncResponseOptions { CorrelationIdProperty = "cid" };

        Assert.Equal("from-bytes", AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(properties: new Dictionary<string, object?> { ["cid"] = Encoding.UTF8.GetBytes("from-bytes") }),
            "{}",
            options));
        Assert.Equal("from-memory", AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(properties: new Dictionary<string, object?> { ["cid"] = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("from-memory")) }),
            "{}",
            options));
        Assert.Equal("from-binary-data", AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(properties: new Dictionary<string, object?> { ["cid"] = BinaryData.FromString("from-binary-data") }),
            "{}",
            options));
        Assert.Equal("42", AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(properties: new Dictionary<string, object?> { ["cid"] = 42 }),
            "{}",
            options));
    }

    [Fact]
    public void CorrelationIdExtractor_FormatsNumericAndTimestampPropertiesCultureInvariantly()
    {
        // Red-on-old: the property-conversion catch-all rendered values under CurrentCulture, so a
        // producer stamping the correlation id as an AMQP double read "1.5" on an en-US consumer
        // and "1,5" on de-DE — the waiter registered on one host was never matched by the ingress
        // on the other, and the wait ran to timeout.
        var options = new AzureServiceBusAsyncResponseOptions { CorrelationIdProperty = "cid" };
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");

            Assert.Equal("1.5", AzureServiceBusCorrelationIdExtractor.Extract(
                Delivery(properties: new Dictionary<string, object?> { ["cid"] = 1.5d }),
                "{}",
                options));
            Assert.Equal("2.25", AzureServiceBusCorrelationIdExtractor.Extract(
                Delivery(properties: new Dictionary<string, object?> { ["cid"] = 2.25m }),
                "{}",
                options));
            Assert.Equal("08/15/2026 10:30:00 +00:00", AzureServiceBusCorrelationIdExtractor.Extract(
                Delivery(properties: new Dictionary<string, object?>
                {
                    ["cid"] = new DateTimeOffset(2026, 8, 15, 10, 30, 0, TimeSpan.Zero)
                }),
                "{}",
                options));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Fact]
    public void CorrelationIdExtractor_ReturnsNullForMissingOrUnreadableSources()
    {
        Assert.Null(AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(),
            "{}",
            new AzureServiceBusAsyncResponseOptions { CorrelationIdJsonPaths = [] }));
        Assert.Null(AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(),
            "   ",
            new AzureServiceBusAsyncResponseOptions()));
        Assert.Null(AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(),
            "null",
            new AzureServiceBusAsyncResponseOptions()));
        Assert.Null(AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(),
            """{"CorrelationId":"corr"}""",
            new AzureServiceBusAsyncResponseOptions { CorrelationIdJsonPaths = [" "] }));
        Assert.Null(AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(),
            """{"CorrelationId":"corr"}""",
            new AzureServiceBusAsyncResponseOptions { CorrelationIdJsonPaths = ["CorrelationId.Value"] }));
        Assert.Null(AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(),
            """{"CustomParameters":"{bad"}""",
            new AzureServiceBusAsyncResponseOptions { CorrelationIdJsonPaths = ["CustomParameters.CorrelationId"] }));
    }

    [Fact]
    public void CorrelationIdExtractor_ReadsCaseInsensitiveAndNumericJsonValues()
    {
        Assert.Equal("corr-case", AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(),
            """{"CustomParameters":{"CorrelationId":"corr-case"}}""",
            new AzureServiceBusAsyncResponseOptions { CorrelationIdJsonPaths = ["customparameters.correlationid"] }));
        Assert.Equal("42", AzureServiceBusCorrelationIdExtractor.Extract(
            Delivery(),
            """{"CorrelationId":42}""",
            new AzureServiceBusAsyncResponseOptions { CorrelationIdJsonPaths = ["CorrelationId"] }));
    }

    [Fact]
    public async Task WorkerSubscriber_ForwardsBodyAndCompletesMessage()
    {
        var receiver = new FakeReceiver();
        var client = new FakeServiceBusClient { Receiver = receiver };
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json")).Returns(Task.CompletedTask);
        var calls = new SettlementCalls();
        var subscriber = new AzureServiceBusWorkerSubscriber(
            Options.Create(new AzureServiceBusAsyncResponseOptions
            {
                WorkerQueue = "workers",
                ResponseQueue = "responses",
                ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
            }),
            client,
            ingress.Object,
            NullLogger<AzureServiceBusWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        receiver.Enqueue(Delivery(calls, queue: "workers", body: "worker-json"));

        await calls.Completed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        ingress.Verify(i => i.HandleWorkerMessageAsync("worker-json"), Times.Once);
        Assert.Contains("workers", client.ReceiverQueues);
        Assert.Equal(1, calls.Complete);
    }

    [Fact]
    public async Task ResponseSubscriber_ExtractsCorrelationAndForwardsBody()
    {
        var receiver = new FakeReceiver();
        var client = new FakeServiceBusClient { Receiver = receiver };
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleResponseMessageAsync("response-json", "corr-response")).Returns(Task.CompletedTask);
        var calls = new SettlementCalls();
        var subscriber = new AzureServiceBusResponseIngressSubscriber(
            Options.Create(new AzureServiceBusAsyncResponseOptions
            {
                WorkerQueue = "workers",
                ResponseQueue = "responses",
                ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
            }),
            client,
            ingress.Object,
            NullLogger<AzureServiceBusResponseIngressSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        receiver.Enqueue(Delivery(calls, queue: "responses", body: "response-json", correlationId: "corr-response"));

        await calls.Completed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        ingress.Verify(i => i.HandleResponseMessageAsync("response-json", "corr-response"), Times.Once);
        Assert.Equal(1, calls.Complete);
    }

    [Fact]
    public async Task WorkerSubscriber_RetriesAfterReceiveFailure()
    {
        var receiver = new FakeReceiver { FailuresBeforeReceive = 1 };
        var client = new FakeServiceBusClient { Receiver = receiver };
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json")).Returns(Task.CompletedTask);
        var calls = new SettlementCalls();
        var subscriber = new AzureServiceBusWorkerSubscriber(
            Options.Create(new AzureServiceBusAsyncResponseOptions
            {
                WorkerQueue = "workers",
                ResponseQueue = "responses",
                ReceiveWaitTime = TimeSpan.FromMilliseconds(10),
                SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            client,
            ingress.Object,
            NullLogger<AzureServiceBusWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        receiver.Enqueue(Delivery(calls, queue: "workers", body: "worker-json"));

        await calls.Completed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.True(receiver.ReceiveAttempts >= 2);
        ingress.Verify(i => i.HandleWorkerMessageAsync("worker-json"), Times.Once);
        Assert.Equal(1, calls.Complete);
    }

    [Fact]
    public async Task Subscriber_RetryDelays_RouteThroughSharedJitteredBackoff()
    {
        // Red-on-old: the subscriber supervised its restarts through a private un-jittered
        // base*2^n helper instead of AsyncResponseRetry.Backoff. Backoff floors every delay at
        // 1 ms after half-jitter, so a sub-millisecond base yields exactly 1 ms; the old helper
        // rendered the same base as a 0 ms (or sub-ms) delay — the first logged retry delays pin
        // which computation the loop routes through.
        var receiver = new FakeReceiver { FailuresBeforeReceive = 2 };
        var client = new FakeServiceBusClient { Receiver = receiver };
        var logger = new RetryDelayCapturingLogger<AzureServiceBusWorkerSubscriber>();
        var calls = new SettlementCalls();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>())).Returns(Task.CompletedTask);
        var subscriber = new AzureServiceBusWorkerSubscriber(
            Options.Create(new AzureServiceBusAsyncResponseOptions
            {
                WorkerQueue = "workers",
                ResponseQueue = "responses",
                ReceiveWaitTime = TimeSpan.FromMilliseconds(10),
                SubscriberRetryBaseDelay = TimeSpan.FromTicks(4000), // 0.4 ms
                SubscriberRetryMaxDelay = TimeSpan.FromSeconds(5)
            }),
            client,
            ingress.Object,
            logger);

        await subscriber.StartAsync(CancellationToken.None);
        receiver.Enqueue(Delivery(calls, queue: "workers"));
        await calls.Completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await subscriber.StopAsync(CancellationToken.None);

        var delays = logger.RetryDelays;
        Assert.True(delays.Count >= 2);
        // Half a 0.4 ms (and 0.8 ms) exponential step sits under the 1 ms floor either way.
        Assert.Equal(TimeSpan.FromMilliseconds(1), delays[0]);
        Assert.Equal(TimeSpan.FromMilliseconds(1), delays[1]);
        Assert.All(delays, delay => Assert.InRange(delay, TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task WorkerSubscriber_EarlyAckSaturated_PausesReceivingAndBoundsRequestSize()
    {
        var receiver = new FakeReceiver();
        var client = new FakeServiceBusClient { Receiver = receiver };
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = 0;
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(async () =>
            {
                Interlocked.Increment(ref handled);
                started.TrySetResult();
                await release.Task;
            });
        var options = new AzureServiceBusAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
        };
        options.WorkerSubscriber.UseAckAfterEnqueue(backgroundWorkerCount: 1, backgroundQueueCapacity: 1, backgroundDrainTimeout: TimeSpan.FromSeconds(5));
        var firstCalls = new SettlementCalls();
        var secondCalls = new SettlementCalls();
        var thirdCalls = new SettlementCalls();
        var subscriber = new AzureServiceBusWorkerSubscriber(
            Options.Create(options),
            client,
            ingress.Object,
            NullLogger<AzureServiceBusWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        receiver.Enqueue(Delivery(firstCalls, queue: "workers", messageId: "m1"));
        receiver.Enqueue(Delivery(secondCalls, queue: "workers", messageId: "m2"));
        receiver.Enqueue(Delivery(thirdCalls, queue: "workers", messageId: "m3"));

        // m1 goes to the (blocked) worker; m2 fills the queue of capacity 1; the receive loop must
        // now pause instead of pulling and abandoning m3 (each abandon burns DeliveryCount).
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await secondCalls.Completed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        var receiveAttemptsWhileSaturated = receiver.ReceiveAttempts;
        await Task.Delay(200);
        Assert.Equal(receiveAttemptsWhileSaturated, receiver.ReceiveAttempts);
        Assert.Equal(0, thirdCalls.Complete);
        Assert.Equal(0, thirdCalls.Abandon);
        // Requests are bounded by the dispatcher's free capacity, never the full MaxMessagesPerReceive.
        Assert.Equal(1, receiver.LastMaxMessages);

        release.TrySetResult();
        await thirdCalls.Completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Equal(3, handled);
        Assert.Equal(0, firstCalls.Abandon + secondCalls.Abandon + thirdCalls.Abandon);
    }

    [Fact]
    public async Task WorkerSubscriber_SlowHandler_RenewsLocksOfUnsettledBatchMessages()
    {
        var receiver = new FakeReceiver();
        var client = new FakeServiceBusClient { Receiver = receiver };
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>())).Returns(Task.CompletedTask);
        ingress.Setup(i => i.HandleWorkerMessageAsync("gate"))
            .Returns(async () =>
            {
                secondStarted.TrySetResult();
                await release.Task;
            });
        var options = new AzureServiceBusAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
        };
        options.WorkerSubscriber.LockRenewalInterval = TimeSpan.FromMilliseconds(50);
        var firstCalls = new SettlementCalls();
        var secondCalls = new SettlementCalls();
        var thirdCalls = new SettlementCalls();
        var subscriber = new AzureServiceBusWorkerSubscriber(
            Options.Create(options),
            client,
            ingress.Object,
            NullLogger<AzureServiceBusWorkerSubscriber>.Instance);

        // All three arrive in one batch; m1's handler completes instantly, m2 blocks in the handler
        // and m3 waits its turn. While the batch is in flight the heartbeat must renew both unsettled
        // messages (the in-handler one and the queued one) and skip the already-settled m1.
        receiver.Enqueue(Delivery(firstCalls, queue: "workers", messageId: "m1"));
        receiver.Enqueue(Delivery(secondCalls, queue: "workers", messageId: "m2", body: "gate"));
        receiver.Enqueue(Delivery(thirdCalls, queue: "workers", messageId: "m3"));
        await subscriber.StartAsync(CancellationToken.None);

        // m2's handler running means m1 was already settled, so every sweep starting from here on
        // must skip it. A sweep visits messages in order, so once m2 gains a renewal any pass that
        // raced m1's settlement has already made its (benign, documented) m1 renew; snapshot m1
        // after that flush, let several more beats renew the unsettled two, and require the settled
        // one to stay frozen — pinning the settled-prefix skip while the renewal loop is
        // demonstrably alive (checking after StopAsync would be vacuous).
        await secondStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        var secondRenewalsAtGate = Volatile.Read(ref secondCalls.RenewLock);
        var thirdRenewalsAtGate = Volatile.Read(ref thirdCalls.RenewLock);
        await WaitUntilAsync(() => Volatile.Read(ref secondCalls.RenewLock) >= secondRenewalsAtGate + 1);
        var firstRenewalsAfterSettle = Volatile.Read(ref firstCalls.RenewLock);
        await WaitUntilAsync(() =>
            Volatile.Read(ref secondCalls.RenewLock) >= secondRenewalsAtGate + 3
            && Volatile.Read(ref thirdCalls.RenewLock) >= thirdRenewalsAtGate + 3);
        Assert.Equal(firstRenewalsAfterSettle, Volatile.Read(ref firstCalls.RenewLock));

        release.TrySetResult();
        await thirdCalls.Completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Equal(1, firstCalls.Complete);
        Assert.Equal(1, secondCalls.Complete);
        Assert.Equal(1, thirdCalls.Complete);
    }

    [Fact]
    public async Task WorkerSubscriber_LockRenewalDisabled_NeverRenews()
    {
        var receiver = new FakeReceiver();
        var client = new FakeServiceBusClient { Receiver = receiver };
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(async () =>
            {
                started.TrySetResult();
                await release.Task;
            });
        var options = new AzureServiceBusAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
        };
        options.WorkerSubscriber.LockRenewalInterval = null;
        var calls = new SettlementCalls();
        var subscriber = new AzureServiceBusWorkerSubscriber(
            Options.Create(options),
            client,
            ingress.Object,
            NullLogger<AzureServiceBusWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        receiver.Enqueue(Delivery(calls, queue: "workers"));

        // Hold the batch open long enough to span several beats of any plausible small renewal
        // interval and require that no renewal ever fires: with interval = null no renewal loop may
        // be started at all. (A regression to the 30s default interval is inherently unobservable
        // in-test without a clock abstraction; the blocked-handler window is the strongest signal
        // available — an instantly settled message would assert nothing.)
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(300);
        Assert.Equal(0, Volatile.Read(ref calls.RenewLock));

        release.TrySetResult();
        await calls.Completed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.Equal(0, calls.RenewLock);
    }

    [Fact]
    public async Task WorkerSubscriber_StuckLockRenewal_IsCancelledPromptlyWhenBatchSettles()
    {
        var receiver = new FakeReceiver();
        var client = new FakeServiceBusClient { Receiver = receiver };
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(async () =>
            {
                started.TrySetResult();
                await release.Task;
            });
        var options = new AzureServiceBusAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            ReceiveWaitTime = TimeSpan.FromMilliseconds(10)
        };
        options.WorkerSubscriber.LockRenewalInterval = TimeSpan.FromMilliseconds(50);
        // Pinned well above every wait cap below so a resume can only come from genuine
        // cancellation, never from the shutdown backstop firing.
        options.ShutdownTimeout = TimeSpan.FromSeconds(15);
        var calls = new SettlementCalls { RenewLockBlocksUntilCancelled = true };
        var subscriber = new AzureServiceBusWorkerSubscriber(
            Options.Create(options),
            client,
            ingress.Object,
            NullLogger<AzureServiceBusWorkerSubscriber>.Instance);

        receiver.Enqueue(Delivery(calls, queue: "workers"));
        await subscriber.StartAsync(CancellationToken.None);

        // Get a renewal pass mid-flight: the handler is blocked, so the sweep runs and its renew
        // call sticks the way a degraded namespace holds it inside the SDK retry pipeline.
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await calls.RenewStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        var receivesWhileBatchInFlight = receiver.ReceiveAttempts;

        // Settling the batch must interrupt the stuck renew through its token: the batch finally
        // (and with it the receive loop) resumes promptly instead of waiting out the SDK retry
        // budget. ShutdownTimeout is pinned at 15s above, so resuming within the 5s wait cap
        // proves genuine cancellation of the in-flight call rather than the drain backstop firing.
        release.TrySetResult();
        await calls.Completed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await WaitUntilAsync(() => receiver.ReceiveAttempts > receivesWhileBatchInFlight);

        await subscriber.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(Volatile.Read(ref calls.RenewLock) >= 1);
        Assert.Equal(1, calls.Complete);
    }

    [Fact]
    public void ValidateSubscriber_NonPositiveLockRenewalInterval_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => AzureServiceBusMessageDispatcher.ValidateOptions(
            new AzureServiceBusAsyncResponseOptions(),
            new AzureServiceBusSubscriberOptions { LockRenewalInterval = TimeSpan.Zero },
            AzureServiceBusSubscriberRole.Worker));

        Assert.Contains(nameof(AzureServiceBusSubscriberOptions.LockRenewalInterval), ex.Message, StringComparison.Ordinal);
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
        {
            cts.Token.ThrowIfCancellationRequested();
            await Task.Delay(20, cts.Token);
        }
    }

    private const string DevelopmentConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    private static void AssertInvalidCommon(
        Action<AzureServiceBusAsyncResponseOptions> configure,
        string expectedOptionName)
    {
        var options = new AzureServiceBusAsyncResponseOptions
        {
            WorkerQueue = "workers",
            ResponseQueue = "responses",
            PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            PublishRetryMaxDelay = TimeSpan.FromMilliseconds(10),
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(10)
        };
        configure(options);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            new AzureServiceBusWorkerTransport(Options.Create(options), new FakeServiceBusClient()));

        Assert.Contains(expectedOptionName, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WorkerTransport_PublishAfterDispose_ThrowsTransportNamedDisposedException()
    {
        // Regression (r23): DisposeAsync used to Release then Dispose the sender gate.
        // SemaphoreSlim.Dispose does not complete pending WaitAsync waiters, so publishers parked
        // on the gate during dispose hung forever. The gate must stay usable: every post-dispose
        // publish wakes in turn and gets the transport-named ObjectDisposedException.
        var transport = new AzureServiceBusWorkerTransport(
            Options.Create(new AzureServiceBusAsyncResponseOptions
            {
                WorkerQueue = "worker-q",
                ResponseQueue = "response-q"
            }),
            new FakeServiceBusClient());

        await transport.DisposeAsync();

        var ex = await Assert.ThrowsAsync<ObjectDisposedException>(() => transport.PublishAsync(WorkerJob("c-disposed")));
        Assert.Contains(nameof(AzureServiceBusWorkerTransport), ex.ObjectName, StringComparison.Ordinal);
    }

    private static WorkerJobEnvelope WorkerJob(string correlationId)
        => new()
        {
            CorrelationId = correlationId,
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IRecoverySpy).AssemblyQualifiedName!,
                MethodName = "DoWork",
                Params = [CallbackParam.ForValue(42)]
            }
        };

    private static AzureServiceBusTransportDelivery Delivery(
        SettlementCalls? calls = null,
        string queue = "queue",
        string body = "{}",
        string messageId = "message-id",
        string? correlationId = null,
        int deliveryCount = 1,
        IReadOnlyDictionary<string, object?>? properties = null)
    {
        calls ??= new SettlementCalls();
        return new AzureServiceBusTransportDelivery(
            queue,
            body,
            messageId,
            correlationId,
            SequenceNumber: 42,
            deliveryCount,
            properties ?? new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
            () =>
            {
                calls.Complete++;
                calls.Completed.TrySetResult();
                return ValueTask.CompletedTask;
            },
            () =>
            {
                calls.Abandon++;
                calls.Abandoned.TrySetResult();
                return ValueTask.CompletedTask;
            },
            (reason, description) =>
            {
                calls.DeadLetter++;
                calls.DeadLetterReason = reason;
                calls.DeadLetterDescription = description;
                calls.DeadLettered.TrySetResult();
                return ValueTask.CompletedTask;
            },
            async cancellationToken =>
            {
                calls.RenewLock++;
                calls.RenewStarted.TrySetResult();
                if (calls.RenewLockBlocksUntilCancelled)
                    // Behaves like ServiceBusReceiver.RenewMessageLockAsync stuck in the SDK retry
                    // pipeline on a degraded namespace: only the caller's token gets it back.
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            });
    }

    private sealed class SettlementCalls
    {
        public int Complete;
        public int Abandon;
        public int DeadLetter;
        public int RenewLock;
        public bool RenewLockBlocksUntilCancelled;
        public string? DeadLetterReason;
        public string? DeadLetterDescription;
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Abandoned { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource DeadLettered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource RenewStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class FakeServiceBusClient : IAzureServiceBusClient
    {
        public FakeSender Sender { get; set; } = new();
        public FakeReceiver Receiver { get; set; } = new();
        public List<string> SenderQueues { get; } = [];
        public List<string> ReceiverQueues { get; } = [];

        public IAzureServiceBusSender CreateSender(string queue)
        {
            SenderQueues.Add(queue);
            return Sender;
        }

        public IAzureServiceBusReceiver CreateReceiver(string queue, AzureServiceBusSubscriberOptions subscriberOptions)
        {
            ReceiverQueues.Add(queue);
            Receiver.LastSubscriberOptions = subscriberOptions;
            return Receiver;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FakeSender : IAzureServiceBusSender
    {
        public List<AzureServiceBusOutboundMessage> Messages { get; } = [];
        public int FailuresBeforeSuccess { get; set; }
        public Exception? PublishException { get; set; }
        public Exception? CloseException { get; set; }
        public int SendAttempts { get; private set; }
        public int CloseCalls { get; private set; }
        public int DisposeCalls { get; private set; }

        public Task SendMessageAsync(AzureServiceBusOutboundMessage message, CancellationToken cancellationToken = default)
        {
            SendAttempts++;
            if (FailuresBeforeSuccess > 0)
            {
                FailuresBeforeSuccess--;
                throw new ServiceBusException(isTransient: true, "transient send failed");
            }

            if (PublishException is not null)
                throw PublishException;

            Messages.Add(message);
            return Task.CompletedTask;
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            CloseCalls++;
            if (CloseException is not null)
                throw CloseException;

            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RetryDelayCapturingLogger<T> : ILogger<T>
    {
        private readonly object _gate = new();
        private readonly List<TimeSpan> _retryDelays = [];

        public IReadOnlyList<TimeSpan> RetryDelays
        {
            get
            {
                lock (_gate)
                    return _retryDelays.ToArray();
            }
        }

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull
            => NoopScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (state is not IReadOnlyList<KeyValuePair<string, object?>> values)
                return;

            foreach (var pair in values)
            {
                if (pair.Key == "RetryDelay" && pair.Value is TimeSpan delay)
                {
                    lock (_gate)
                        _retryDelays.Add(delay);
                }
            }
        }

        private sealed class NoopScope : IDisposable
        {
            public static readonly NoopScope Instance = new();

            public void Dispose()
            {
            }
        }
    }

    private sealed class FakeReceiver : IAzureServiceBusReceiver
    {
        private readonly Channel<AzureServiceBusTransportDelivery> _deliveries = Channel.CreateUnbounded<AzureServiceBusTransportDelivery>();

        public AzureServiceBusSubscriberOptions? LastSubscriberOptions { get; set; }
        public int FailuresBeforeReceive { get; set; }
        public int ReceiveAttempts { get; private set; }
        public int LastMaxMessages { get; private set; }

        public void Enqueue(AzureServiceBusTransportDelivery delivery)
            => _deliveries.Writer.TryWrite(delivery);

        public async Task<IReadOnlyList<AzureServiceBusTransportDelivery>> ReceiveMessagesAsync(
            int maxMessages,
            TimeSpan maxWaitTime,
            CancellationToken cancellationToken = default)
        {
            ReceiveAttempts++;
            LastMaxMessages = maxMessages;
            if (FailuresBeforeReceive > 0)
            {
                FailuresBeforeReceive--;
                throw new InvalidOperationException("receive failed");
            }

            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(maxWaitTime);
            try
            {
                if (!await _deliveries.Reader.WaitToReadAsync(timeout.Token).ConfigureAwait(false))
                    return [];
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                return [];
            }

            var messages = new List<AzureServiceBusTransportDelivery>(maxMessages);
            while (messages.Count < maxMessages && _deliveries.Reader.TryRead(out var delivery))
                messages.Add(delivery);
            return messages;
        }

        public Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task WorkerSubscriber_InvalidOptions_FailHostStartupSynchronously()
    {
        // Red-on-old (Hosting 10.0.10+): validation used to sit at the top of ExecuteAsync, which
        // BackgroundService.StartAsync no longer runs inline — StartAsync returned without
        // throwing and the misconfiguration surfaced late or never. Validation now runs in
        // StartAsync so a misconfigured subscriber fails host startup synchronously.
        var subscriber = new AzureServiceBusWorkerSubscriber(
            Options.Create(new AzureServiceBusAsyncResponseOptions
            {
                WorkerQueue = "workers",
                ResponseQueue = "responses",
                WorkerSubscriber = { AckMode = AzureServiceBusAckMode.AckAfterEnqueue }
            }),
            new FakeServiceBusClient(),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<AzureServiceBusWorkerSubscriber>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.StartAsync(CancellationToken.None));
        Assert.Contains("BackgroundWorkerCount", ex.Message, StringComparison.Ordinal);
    }
}
