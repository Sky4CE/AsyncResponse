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
    /// <summary>Creates the requested resource.</summary>
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
    /// <summary>False once the connection is closed for good; with automatic recovery enabled the client object stays open while it reconnects.</summary>
    bool IsOpen { get; }
    Task<IRabbitMqChannel> CreateChannelAsync(bool publisherConfirmations = false, CancellationToken cancellationToken = default);
    Task CloseAsync(TimeSpan timeout, CancellationToken cancellationToken = default);
}

internal sealed class RabbitMqConnectionAdapter(IConnection inner) : IRabbitMqConnection
{
    public bool IsOpen => inner.IsOpen;

    /// <summary>Creates the requested resource.</summary>
    public async Task<IRabbitMqChannel> CreateChannelAsync(bool publisherConfirmations = false, CancellationToken cancellationToken = default)
    {
        // Enabling publisher confirmations with tracking makes BasicPublishAsync await the broker
        // acknowledgement and throw on a nack or an unroutable (mandatory) return, so a worker job is
        // never silently lost. Consumer channels pass false and keep the lighter default behavior.
        var options = publisherConfirmations
            ? new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true)
            : null;
        var channel = await inner.CreateChannelAsync(options, cancellationToken).ConfigureAwait(false);
        return new RabbitMqChannelAdapter(channel);
    }

    /// <summary>Runs the CloseAsync operation.</summary>
    public Task CloseAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        => inner.CloseAsync(200, "AsyncResponse shutdown", timeout, abort: false, cancellationToken);

    /// <summary>Releases resources held by this instance.</summary>
    public ValueTask DisposeAsync() => inner.DisposeAsync();
}

internal interface IRabbitMqChannel : IAsyncDisposable
{
    /// <summary>False once the channel is closed (a 404/406 protocol error closes it without any callback failing).</summary>
    bool IsOpen { get; }
    Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, CancellationToken cancellationToken = default);
    Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object?>? arguments = null, CancellationToken cancellationToken = default);
    Task QueueBindAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken = default);
    Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken = default);
    ValueTask BasicPublishAsync(string exchange, string routingKey, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default);
    Task<RabbitMqConsumer> BasicConsumeAsync(string queue, Func<RabbitMqDelivery, Task> handler, CancellationToken cancellationToken = default);
    Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default);
    ValueTask BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken = default);
    ValueTask BasicNackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default);
    Task CloseAsync(CancellationToken cancellationToken = default);
}

internal sealed class RabbitMqChannelAdapter(IChannel inner) : IRabbitMqChannel
{
    public bool IsOpen => inner.IsOpen;

    /// <summary>Runs the ExchangeDeclareAsync operation.</summary>
    public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, CancellationToken cancellationToken = default)
        => inner.ExchangeDeclareAsync(exchange, type, durable, autoDelete, cancellationToken: cancellationToken);

    /// <summary>Runs the QueueDeclareAsync operation.</summary>
    public async Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object?>? arguments = null, CancellationToken cancellationToken = default)
        => await inner.QueueDeclareAsync(queue, durable, exclusive, autoDelete, arguments, cancellationToken: cancellationToken).ConfigureAwait(false);

    /// <summary>Runs the QueueBindAsync operation.</summary>
    public Task QueueBindAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken = default)
        => inner.QueueBindAsync(queue, exchange, routingKey, cancellationToken: cancellationToken);

    /// <summary>Runs the BasicQosAsync operation.</summary>
    public Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken = default)
        => inner.BasicQosAsync(0, prefetchCount, global: false, cancellationToken);

    /// <summary>Runs the BasicPublishAsync operation.</summary>
    public ValueTask BasicPublishAsync(string exchange, string routingKey, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
        => inner.BasicPublishAsync(exchange, routingKey, mandatory: true, properties, body, cancellationToken);

    /// <summary>Runs the BasicConsumeAsync operation.</summary>
    public async Task<RabbitMqConsumer> BasicConsumeAsync(string queue, Func<RabbitMqDelivery, Task> handler, CancellationToken cancellationToken = default)
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

        // Deliveries can stop without any exception reaching the subscriber: a broker-side
        // basic.cancel (queue deleted) only raises UnregisteredAsync and a channel-level protocol
        // close only raises ChannelShutdownAsync. Fold both into one termination task the subscriber
        // can await. A client-initiated BasicCancelAsync completes it too (cancel-ok also raises
        // UnregisteredAsync); the subscriber filters that out with its stopping token.
        var terminated = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        consumer.UnregisteredAsync += (_, _) =>
        {
            terminated.TrySetResult("the broker canceled the consumer (basic.cancel, typically a deleted queue)");
            return Task.CompletedTask;
        };
        inner.ChannelShutdownAsync += (_, args) =>
        {
            terminated.TrySetResult($"the channel shut down ({args.ReplyCode} {args.ReplyText})");
            return Task.CompletedTask;
        };

        var consumerTag = await inner.BasicConsumeAsync(
            queue,
            autoAck: false,
            consumerTag: string.Empty,
            noLocal: false,
            exclusive: false,
            arguments: null,
            consumer,
            cancellationToken).ConfigureAwait(false);
        return new RabbitMqConsumer(consumerTag, terminated.Task);
    }

    /// <summary>Runs the BasicCancelAsync operation.</summary>
    public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default)
        => inner.BasicCancelAsync(consumerTag, cancellationToken: cancellationToken);

    /// <summary>Runs the BasicAckAsync operation.</summary>
    public ValueTask BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken = default)
        => inner.BasicAckAsync(deliveryTag, multiple: false, cancellationToken);

    /// <summary>Runs the BasicNackAsync operation.</summary>
    public ValueTask BasicNackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default)
        => inner.BasicNackAsync(deliveryTag, multiple: false, requeue, cancellationToken);

    /// <summary>Runs the CloseAsync operation.</summary>
    public Task CloseAsync(CancellationToken cancellationToken = default)
        => inner.CloseAsync(200, "AsyncResponse shutdown", abort: false, cancellationToken);

    /// <summary>Releases resources held by this instance.</summary>
    public ValueTask DisposeAsync() => inner.DisposeAsync();
}

/// <summary>
/// An active consumer subscription. <see cref="Terminated"/> completes (with a reason) when the
/// broker cancels the consumer or the channel shuts down — cases that stop deliveries forever
/// without failing any pending call.
/// </summary>
internal sealed record RabbitMqConsumer(string ConsumerTag, Task<string> Terminated);

internal sealed record RabbitMqDelivery(
    string ConsumerTag,
    ulong DeliveryTag,
    bool Redelivered,
    string Exchange,
    string RoutingKey,
    IReadOnlyBasicProperties BasicProperties,
    ReadOnlyMemory<byte> Body,
    CancellationToken CancellationToken);
