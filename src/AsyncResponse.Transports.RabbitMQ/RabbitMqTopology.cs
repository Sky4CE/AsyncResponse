using RabbitMQ.Client;

namespace AsyncResponse.Transports.RabbitMQ;

internal static class RabbitMqTopology
{
    private const string DirectExchange = "direct";

    public static async Task EnsureWorkerAsync(
        IRabbitMqChannel channel,
        RabbitMqAsyncResponseOptions options,
        CancellationToken cancellationToken = default)
    {
        if (!options.DeclareTopology)
            return;

        var exchange = RabbitMqOptionsValidator.Required(options.WorkerExchange, nameof(options.WorkerExchange));
        var queue = RabbitMqOptionsValidator.Required(options.WorkerQueue, nameof(options.WorkerQueue));
        var routingKey = RabbitMqOptionsValidator.Required(options.WorkerRoutingKey, nameof(options.WorkerRoutingKey));

        await channel.ExchangeDeclareAsync(exchange, DirectExchange, durable: true, autoDelete: false, cancellationToken).ConfigureAwait(false);
        await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false, cancellationToken).ConfigureAwait(false);
        await channel.QueueBindAsync(queue, exchange, routingKey, cancellationToken).ConfigureAwait(false);
    }

    public static async Task EnsureResponseAsync(
        IRabbitMqChannel channel,
        RabbitMqAsyncResponseOptions options,
        CancellationToken cancellationToken = default)
    {
        if (!options.DeclareTopology)
            return;

        var exchange = RabbitMqOptionsValidator.Required(options.ResponseExchange, nameof(options.ResponseExchange));
        var queue = RabbitMqOptionsValidator.Required(options.ResponseQueue, nameof(options.ResponseQueue));
        var routingKey = RabbitMqOptionsValidator.Required(options.ResponseRoutingKey, nameof(options.ResponseRoutingKey));

        await channel.ExchangeDeclareAsync(exchange, DirectExchange, durable: true, autoDelete: false, cancellationToken).ConfigureAwait(false);
        await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false, cancellationToken).ConfigureAwait(false);
        await channel.QueueBindAsync(queue, exchange, routingKey, cancellationToken).ConfigureAwait(false);
    }

    public static BasicProperties CreatePersistentJsonProperties(string? correlationId, string correlationHeader)
    {
        var properties = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
            Persistent = true,
            MessageId = Guid.NewGuid().ToString("N"),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
        };

        if (!string.IsNullOrWhiteSpace(correlationId))
        {
            properties.CorrelationId = correlationId;
            if (!string.IsNullOrWhiteSpace(correlationHeader))
            {
                properties.Headers = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    [correlationHeader] = correlationId
                };
            }
        }

        return properties;
    }
}
