using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace AsyncResponse.Transports.RabbitMQ;

internal interface IRabbitMqConnectionFactory
{
    Task<IRabbitMqConnection> CreateConnectionAsync(CancellationToken cancellationToken = default);
}

internal sealed class RabbitMqConnectionFactoryAdapter(
    RabbitMqAsyncResponseOptions options) : IRabbitMqConnectionFactory
{
    public async Task<IRabbitMqConnection> CreateConnectionAsync(CancellationToken cancellationToken = default)
    {
        var factory = CreateFactory(options);
        var connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
        return new RabbitMqConnectionAdapter(connection);
    }

    private static ConnectionFactory CreateFactory(RabbitMqAsyncResponseOptions options)
    {
        var factory = new ConnectionFactory
        {
            AutomaticRecoveryEnabled = options.AutomaticRecoveryEnabled,
            TopologyRecoveryEnabled = options.TopologyRecoveryEnabled,
            NetworkRecoveryInterval = options.NetworkRecoveryInterval,
            RequestedHeartbeat = options.RequestedHeartbeat,
            ClientProvidedName = options.ClientProvidedName,
            ConsumerDispatchConcurrency = 1
        };

        if (!string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            factory.Uri = new Uri(options.ConnectionString);
            return factory;
        }

        factory.HostName = RabbitMqOptionsValidator.Required(options.HostName, nameof(options.HostName));
        factory.Port = options.Port;
        factory.VirtualHost = RabbitMqOptionsValidator.Required(options.VirtualHost, nameof(options.VirtualHost));
        factory.UserName = RabbitMqOptionsValidator.Required(options.UserName, nameof(options.UserName));
        factory.Password = options.Password;
        return factory;
    }
}

internal interface IRabbitMqConnection : IAsyncDisposable
{
    Task<IRabbitMqChannel> CreateChannelAsync(CancellationToken cancellationToken = default);
    Task CloseAsync(TimeSpan timeout, CancellationToken cancellationToken = default);
}

internal sealed class RabbitMqConnectionAdapter(IConnection inner) : IRabbitMqConnection
{
    public async Task<IRabbitMqChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        var channel = await inner.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        return new RabbitMqChannelAdapter(channel);
    }

    public Task CloseAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        => inner.CloseAsync(200, "AsyncResponse shutdown", timeout, abort: false, cancellationToken);

    public ValueTask DisposeAsync() => inner.DisposeAsync();
}

internal interface IRabbitMqChannel : IAsyncDisposable
{
    Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, CancellationToken cancellationToken = default);
    Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, CancellationToken cancellationToken = default);
    Task QueueBindAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken = default);
    Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken = default);
    ValueTask BasicPublishAsync(string exchange, string routingKey, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default);
    Task<string> BasicConsumeAsync(string queue, Func<RabbitMqDelivery, Task> handler, CancellationToken cancellationToken = default);
    Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default);
    ValueTask BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken = default);
    ValueTask BasicNackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default);
    Task CloseAsync(CancellationToken cancellationToken = default);
}

internal sealed class RabbitMqChannelAdapter(IChannel inner) : IRabbitMqChannel
{
    public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, CancellationToken cancellationToken = default)
        => inner.ExchangeDeclareAsync(exchange, type, durable, autoDelete, cancellationToken: cancellationToken);

    public async Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, CancellationToken cancellationToken = default)
        => await inner.QueueDeclareAsync(queue, durable, exclusive, autoDelete, cancellationToken: cancellationToken).ConfigureAwait(false);

    public Task QueueBindAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken = default)
        => inner.QueueBindAsync(queue, exchange, routingKey, cancellationToken: cancellationToken);

    public Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken = default)
        => inner.BasicQosAsync(0, prefetchCount, global: false, cancellationToken);

    public ValueTask BasicPublishAsync(string exchange, string routingKey, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
        => inner.BasicPublishAsync(exchange, routingKey, mandatory: true, properties, body, cancellationToken);

    public Task<string> BasicConsumeAsync(string queue, Func<RabbitMqDelivery, Task> handler, CancellationToken cancellationToken = default)
    {
        var consumer = new AsyncEventingBasicConsumer(inner);
        consumer.ReceivedAsync += (_, args) =>
        {
            var delivery = new RabbitMqDelivery(
                args.ConsumerTag,
                args.DeliveryTag,
                args.Redelivered,
                args.Exchange,
                args.RoutingKey,
                args.BasicProperties,
                args.Body,
                args.CancellationToken);
            return handler(delivery);
        };

        return inner.BasicConsumeAsync(
            queue,
            autoAck: false,
            consumerTag: string.Empty,
            noLocal: false,
            exclusive: false,
            arguments: null,
            consumer,
            cancellationToken);
    }

    public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default)
        => inner.BasicCancelAsync(consumerTag, cancellationToken: cancellationToken);

    public ValueTask BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken = default)
        => inner.BasicAckAsync(deliveryTag, multiple: false, cancellationToken);

    public ValueTask BasicNackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default)
        => inner.BasicNackAsync(deliveryTag, multiple: false, requeue, cancellationToken);

    public Task CloseAsync(CancellationToken cancellationToken = default)
        => inner.CloseAsync(200, "AsyncResponse shutdown", abort: false, cancellationToken);

    public ValueTask DisposeAsync() => inner.DisposeAsync();
}

internal sealed record RabbitMqDelivery(
    string ConsumerTag,
    ulong DeliveryTag,
    bool Redelivered,
    string Exchange,
    string RoutingKey,
    IReadOnlyBasicProperties BasicProperties,
    ReadOnlyMemory<byte> Body,
    CancellationToken CancellationToken);
