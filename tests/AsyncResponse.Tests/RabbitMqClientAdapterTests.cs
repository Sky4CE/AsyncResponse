using AsyncResponse.Transports.RabbitMQ;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Reflection;
using System.Text;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Verifies the thin RabbitMQ.Client adapters forward calls to the underlying
/// <see cref="IConnection"/>/<see cref="IChannel"/> with the arguments AsyncResponse depends on, and that
/// the connection factory validates required settings before opening a socket.
/// </summary>
public class RabbitMqClientAdapterTests
{
    // ---------- RabbitMqConnectionFactoryAdapter ----------

    [Fact]
    public async Task ConnectionFactory_RequiresHostName()
    {
        var factory = new RabbitMqConnectionFactoryAdapter(new RabbitMqAsyncResponseOptions { HostName = "" });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => factory.CreateConnectionAsync());
        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.HostName), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ConnectionFactory_RequiresVirtualHost()
    {
        var factory = new RabbitMqConnectionFactoryAdapter(new RabbitMqAsyncResponseOptions { VirtualHost = "" });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => factory.CreateConnectionAsync());
        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.VirtualHost), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ConnectionFactory_RequiresUserName()
    {
        var factory = new RabbitMqConnectionFactoryAdapter(new RabbitMqAsyncResponseOptions { UserName = "" });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => factory.CreateConnectionAsync());
        Assert.Contains(nameof(RabbitMqAsyncResponseOptions.UserName), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectionFactory_MapsUriAndIndividualConnectionSettings()
    {
        var create = typeof(RabbitMqConnectionFactoryAdapter)
            .GetMethod("CreateFactory", BindingFlags.NonPublic | BindingFlags.Static)!;
        var fromUri = Assert.IsType<ConnectionFactory>(create.Invoke(null,
        [
            new RabbitMqAsyncResponseOptions
            {
                ConnectionString = "amqp://user:password@localhost:5673/vhost",
                ClientProvidedName = "uri-client"
            }
        ]));
        var fromSettings = Assert.IsType<ConnectionFactory>(create.Invoke(null,
        [
            new RabbitMqAsyncResponseOptions
            {
                HostName = "rabbit",
                Port = 5673,
                VirtualHost = "/orders",
                UserName = "worker",
                Password = "secret",
                ClientProvidedName = "settings-client"
            }
        ]));

        Assert.Equal("localhost", fromUri.HostName);
        Assert.Equal(5673, fromUri.Port);
        Assert.Equal("uri-client", fromUri.ClientProvidedName);
        Assert.Equal("rabbit", fromSettings.HostName);
        Assert.Equal(5673, fromSettings.Port);
        Assert.Equal("/orders", fromSettings.VirtualHost);
        Assert.Equal("worker", fromSettings.UserName);
        Assert.Equal("secret", fromSettings.Password);
    }

    // ---------- RabbitMqConnectionAdapter ----------

    [Fact]
    public async Task ConnectionAdapter_CreateChannel_WrapsInnerChannelWithDefaultOptions()
    {
        CreateChannelOptions? capturedOptions = null;
        var innerChannel = new Mock<IChannel>();
        var connection = new Mock<IConnection>();
        connection
            .Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
            .Callback((CreateChannelOptions? options, CancellationToken _) => capturedOptions = options)
            .ReturnsAsync(innerChannel.Object);
        var adapter = new RabbitMqConnectionAdapter(connection.Object);

        var channel = await adapter.CreateChannelAsync();

        Assert.IsType<RabbitMqChannelAdapter>(channel);
        Assert.Null(capturedOptions); // consumer channels keep the default (no publisher confirmations).
    }

    [Fact]
    public async Task ConnectionAdapter_CreateChannel_WithPublisherConfirmations_EnablesConfirmTracking()
    {
        CreateChannelOptions? capturedOptions = null;
        var innerChannel = new Mock<IChannel>();
        var connection = new Mock<IConnection>();
        connection
            .Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
            .Callback((CreateChannelOptions? options, CancellationToken _) => capturedOptions = options)
            .ReturnsAsync(innerChannel.Object);
        var adapter = new RabbitMqConnectionAdapter(connection.Object);

        await adapter.CreateChannelAsync(publisherConfirmations: true);

        Assert.NotNull(capturedOptions);
        Assert.True(capturedOptions!.PublisherConfirmationsEnabled);
        Assert.True(capturedOptions.PublisherConfirmationTrackingEnabled);
    }

    [Fact]
    public async Task ConnectionAdapter_Close_ForwardsReasonAndTimeout()
    {
        using var cts = new CancellationTokenSource();
        var connection = new Mock<IConnection>();
        connection
            .Setup(c => c.CloseAsync(200, "AsyncResponse shutdown", It.IsAny<TimeSpan>(), false, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var adapter = new RabbitMqConnectionAdapter(connection.Object);

        await adapter.CloseAsync(TimeSpan.FromSeconds(3), cts.Token);

        connection.Verify(
            c => c.CloseAsync(200, "AsyncResponse shutdown", TimeSpan.FromSeconds(3), false, cts.Token),
            Times.Once);
    }

    [Fact]
    public async Task ConnectionAdapter_Dispose_ForwardsToInner()
    {
        var connection = new Mock<IConnection>();
        connection.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var adapter = new RabbitMqConnectionAdapter(connection.Object);

        await adapter.DisposeAsync();

        connection.Verify(c => c.DisposeAsync(), Times.Once);
    }

    // ---------- RabbitMqChannelAdapter ----------

    [Fact]
    public async Task ChannelAdapter_ExchangeDeclare_ForwardsArguments()
    {
        using var cts = new CancellationTokenSource();
        var channel = new Mock<IChannel>();
        var adapter = new RabbitMqChannelAdapter(channel.Object);

        await adapter.ExchangeDeclareAsync("ex", "direct", durable: true, autoDelete: false, cts.Token);

        channel.Verify(
            c => c.ExchangeDeclareAsync("ex", "direct", true, false, null, false, false, cts.Token),
            Times.Once);
    }

    [Fact]
    public async Task ChannelAdapter_QueueDeclare_ForwardsArguments()
    {
        using var cts = new CancellationTokenSource();
        var channel = new Mock<IChannel>();
        var arguments = new Dictionary<string, object?> { ["x-dead-letter-exchange"] = "dlx" };
        channel
            .Setup(c => c.QueueDeclareAsync("q", true, false, false, arguments, false, false, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueueDeclareOk("q", 0, 0));
        var adapter = new RabbitMqChannelAdapter(channel.Object);

        await adapter.QueueDeclareAsync("q", durable: true, exclusive: false, autoDelete: false, arguments, cts.Token);

        channel.Verify(
            c => c.QueueDeclareAsync("q", true, false, false, arguments, false, false, cts.Token),
            Times.Once);
    }

    [Fact]
    public async Task ChannelAdapter_QueueBind_ForwardsArguments()
    {
        using var cts = new CancellationTokenSource();
        var channel = new Mock<IChannel>();
        var adapter = new RabbitMqChannelAdapter(channel.Object);

        await adapter.QueueBindAsync("q", "ex", "rk", cts.Token);

        channel.Verify(c => c.QueueBindAsync("q", "ex", "rk", null, false, cts.Token), Times.Once);
    }

    [Fact]
    public async Task ChannelAdapter_BasicQos_UsesPerConsumerScope()
    {
        using var cts = new CancellationTokenSource();
        var channel = new Mock<IChannel>();
        var adapter = new RabbitMqChannelAdapter(channel.Object);

        await adapter.BasicQosAsync(prefetchCount: 16, cts.Token);

        channel.Verify(c => c.BasicQosAsync(0, 16, false, cts.Token), Times.Once);
    }

    [Fact]
    public async Task ChannelAdapter_BasicPublish_PublishesMandatory()
    {
        using var cts = new CancellationTokenSource();
        var channel = new Mock<IChannel>();
        var adapter = new RabbitMqChannelAdapter(channel.Object);
        var properties = new BasicProperties { CorrelationId = "cid" };
        var body = new ReadOnlyMemory<byte>("payload"u8.ToArray());

        await adapter.BasicPublishAsync("ex", "rk", properties, body, cts.Token);

        channel.Verify(
            c => c.BasicPublishAsync("ex", "rk", true, properties, body, cts.Token),
            Times.Once);
    }

    [Fact]
    public async Task ChannelAdapter_BasicAck_ForwardsSingleAck()
    {
        using var cts = new CancellationTokenSource();
        var channel = new Mock<IChannel>();
        var adapter = new RabbitMqChannelAdapter(channel.Object);

        await adapter.BasicAckAsync(deliveryTag: 7, cts.Token);

        channel.Verify(c => c.BasicAckAsync(7, false, cts.Token), Times.Once);
    }

    [Fact]
    public async Task ChannelAdapter_BasicNack_ForwardsRequeueFlag()
    {
        using var cts = new CancellationTokenSource();
        var channel = new Mock<IChannel>();
        var adapter = new RabbitMqChannelAdapter(channel.Object);

        await adapter.BasicNackAsync(deliveryTag: 9, requeue: true, cts.Token);

        channel.Verify(c => c.BasicNackAsync(9, false, true, cts.Token), Times.Once);
    }

    [Fact]
    public async Task ChannelAdapter_BasicCancel_ForwardsConsumerTag()
    {
        using var cts = new CancellationTokenSource();
        var channel = new Mock<IChannel>();
        var adapter = new RabbitMqChannelAdapter(channel.Object);

        await adapter.BasicCancelAsync("consumer-tag", cts.Token);

        channel.Verify(c => c.BasicCancelAsync("consumer-tag", false, cts.Token), Times.Once);
    }

    [Fact]
    public async Task ChannelAdapter_Close_ForwardsReason()
    {
        using var cts = new CancellationTokenSource();
        var channel = new Mock<IChannel>();
        var adapter = new RabbitMqChannelAdapter(channel.Object);

        await adapter.CloseAsync(cts.Token);

        channel.Verify(c => c.CloseAsync(200, "AsyncResponse shutdown", false, cts.Token), Times.Once);
    }

    [Fact]
    public async Task ChannelAdapter_Dispose_ForwardsToInner()
    {
        var channel = new Mock<IChannel>();
        channel.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var adapter = new RabbitMqChannelAdapter(channel.Object);

        await adapter.DisposeAsync();

        channel.Verify(c => c.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task ChannelAdapter_BasicConsume_MapsDeliveryToHandler()
    {
        using var cts = new CancellationTokenSource();
        var channel = new Mock<IChannel>();
        IAsyncBasicConsumer? captured = null;
        channel
            .Setup(c => c.BasicConsumeAsync(
                "q",
                false,
                "",
                false,
                false,
                It.IsAny<IDictionary<string, object?>?>(),
                It.IsAny<IAsyncBasicConsumer>(),
                It.IsAny<CancellationToken>()))
            .Callback((
                string _,
                bool _,
                string _,
                bool _,
                bool _,
                IDictionary<string, object?>? _,
                IAsyncBasicConsumer consumer,
                CancellationToken _) => captured = consumer)
            .ReturnsAsync("consumer-tag");
        var adapter = new RabbitMqChannelAdapter(channel.Object);

        RabbitMqDelivery? received = null;
        var consumer = await adapter.BasicConsumeAsync(
            "q",
            delivery =>
            {
                received = delivery;
                return Task.CompletedTask;
            },
            cts.Token);

        Assert.Equal("consumer-tag", consumer.ConsumerTag);
        Assert.False(consumer.Terminated.IsCompleted);
        var eventingConsumer = Assert.IsType<AsyncEventingBasicConsumer>(captured);

        var properties = new BasicProperties { CorrelationId = "cid", MessageId = "mid" };
        var body = new ReadOnlyMemory<byte>("payload"u8.ToArray());
        await eventingConsumer.HandleBasicDeliverAsync(
            "consumer-tag",
            deliveryTag: 42,
            redelivered: true,
            exchange: "ex",
            routingKey: "rk",
            properties,
            body);

        Assert.NotNull(received);
        Assert.Equal("consumer-tag", received!.ConsumerTag);
        Assert.Equal(42UL, received.DeliveryTag);
        Assert.True(received.Redelivered);
        Assert.Equal("ex", received.Exchange);
        Assert.Equal("rk", received.RoutingKey);
        Assert.Equal("cid", received.BasicProperties.CorrelationId);
        Assert.Equal("payload", Encoding.UTF8.GetString(received.Body.Span));
    }

    [Fact]
    public async Task ChannelAdapter_BrokerCancel_CompletesTerminated()
    {
        var (adapter, channel) = ConsumableChannel();
        var consumer = await adapter.BasicConsumeAsync("q", _ => Task.CompletedTask);
        var eventingConsumer = Assert.IsType<AsyncEventingBasicConsumer>(channel.Captured);

        // Broker-side basic.cancel (queue deleted) raises UnregisteredAsync and nothing else.
        await eventingConsumer.HandleBasicCancelAsync(consumer.ConsumerTag, CancellationToken.None);

        var reason = await consumer.Terminated.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Contains("cancel", reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ChannelAdapter_ChannelShutdown_CompletesTerminatedWithReplyDetails()
    {
        var (adapter, channel) = ConsumableChannel();
        var consumer = await adapter.BasicConsumeAsync("q", _ => Task.CompletedTask);

        // A channel-level protocol close (e.g. 406 precondition-failed) raises ChannelShutdownAsync only.
        channel.Mock.Raise(
            c => c.ChannelShutdownAsync += null,
            channel.Mock.Object,
            new ShutdownEventArgs(ShutdownInitiator.Peer, 406, "PRECONDITION_FAILED"));

        var reason = await consumer.Terminated.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Contains("406", reason, StringComparison.Ordinal);
        Assert.Contains("PRECONDITION_FAILED", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void ChannelAdapter_IsOpen_ForwardsToInner()
    {
        var channel = new Mock<IChannel>();
        channel.SetupGet(c => c.IsOpen).Returns(true);

        Assert.True(new RabbitMqChannelAdapter(channel.Object).IsOpen);

        channel.SetupGet(c => c.IsOpen).Returns(false);
        Assert.False(new RabbitMqChannelAdapter(channel.Object).IsOpen);
    }

    [Fact]
    public void ConnectionAdapter_IsOpen_ForwardsToInner()
    {
        var connection = new Mock<IConnection>();
        connection.SetupGet(c => c.IsOpen).Returns(true);

        Assert.True(new RabbitMqConnectionAdapter(connection.Object).IsOpen);

        connection.SetupGet(c => c.IsOpen).Returns(false);
        Assert.False(new RabbitMqConnectionAdapter(connection.Object).IsOpen);
    }

    private static (RabbitMqChannelAdapter Adapter, ConsumableChannelMock Channel) ConsumableChannel()
    {
        var channel = new ConsumableChannelMock();
        return (new RabbitMqChannelAdapter(channel.Mock.Object), channel);
    }

    private sealed class ConsumableChannelMock
    {
        public Mock<IChannel> Mock { get; } = new();
        public IAsyncBasicConsumer? Captured { get; private set; }

        public ConsumableChannelMock()
            => Mock
                .Setup(c => c.BasicConsumeAsync(
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<bool>(),
                    It.IsAny<IDictionary<string, object?>?>(),
                    It.IsAny<IAsyncBasicConsumer>(),
                    It.IsAny<CancellationToken>()))
                .Callback((
                    string _,
                    bool _,
                    string _,
                    bool _,
                    bool _,
                    IDictionary<string, object?>? _,
                    IAsyncBasicConsumer consumer,
                    CancellationToken _) => Captured = consumer)
                .ReturnsAsync("consumer-tag");
    }
}
