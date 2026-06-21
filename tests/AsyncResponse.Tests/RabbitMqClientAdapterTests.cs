using AsyncResponse.Transports.RabbitMQ;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
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

    // ---------- RabbitMqConnectionAdapter ----------

    [Fact]
    public async Task ConnectionAdapter_CreateChannel_WrapsInnerChannel()
    {
        var innerChannel = new Mock<IChannel>();
        var connection = new Mock<IConnection>();
        connection
            .Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(innerChannel.Object);
        var adapter = new RabbitMqConnectionAdapter(connection.Object);

        var channel = await adapter.CreateChannelAsync();

        Assert.IsType<RabbitMqChannelAdapter>(channel);
        connection.Verify(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()), Times.Once);
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
        channel
            .Setup(c => c.QueueDeclareAsync("q", true, false, false, null, false, false, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueueDeclareOk("q", 0, 0));
        var adapter = new RabbitMqChannelAdapter(channel.Object);

        await adapter.QueueDeclareAsync("q", durable: true, exclusive: false, autoDelete: false, cts.Token);

        channel.Verify(
            c => c.QueueDeclareAsync("q", true, false, false, null, false, false, cts.Token),
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
        var consumerTag = await adapter.BasicConsumeAsync(
            "q",
            delivery =>
            {
                received = delivery;
                return Task.CompletedTask;
            },
            cts.Token);

        Assert.Equal("consumer-tag", consumerTag);
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
}
