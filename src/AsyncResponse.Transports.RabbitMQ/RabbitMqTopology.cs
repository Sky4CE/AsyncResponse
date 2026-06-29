using RabbitMQ.Client;

namespace AsyncResponse.Transports.RabbitMQ;

internal static class RabbitMqTopology
{
    private const string DirectExchange = "direct";

    /// <summary>Ensures the required resource exists.</summary>
    public static Task EnsureWorkerAsync(
        IRabbitMqChannel channel,
        RabbitMqAsyncResponseOptions options,
        CancellationToken cancellationToken = default)
    {
        if (!options.DeclareTopology)
            return Task.CompletedTask;

        var exchange = RabbitMqOptionsValidator.Required(options.WorkerExchange, nameof(options.WorkerExchange));
        var queue = RabbitMqOptionsValidator.Required(options.WorkerQueue, nameof(options.WorkerQueue));
        var routingKey = RabbitMqOptionsValidator.Required(options.WorkerRoutingKey, nameof(options.WorkerRoutingKey));

        return DeclareQueueTopologyAsync(channel, options, exchange, queue, routingKey, cancellationToken);
    }

    /// <summary>Ensures the required resource exists.</summary>
    public static Task EnsureResponseAsync(
        IRabbitMqChannel channel,
        RabbitMqAsyncResponseOptions options,
        CancellationToken cancellationToken = default)
    {
        if (!options.DeclareTopology)
            return Task.CompletedTask;

        var exchange = RabbitMqOptionsValidator.Required(options.ResponseExchange, nameof(options.ResponseExchange));
        var queue = RabbitMqOptionsValidator.Required(options.ResponseQueue, nameof(options.ResponseQueue));
        var routingKey = RabbitMqOptionsValidator.Required(options.ResponseRoutingKey, nameof(options.ResponseRoutingKey));

        return DeclareQueueTopologyAsync(channel, options, exchange, queue, routingKey, cancellationToken);
    }

    private static async Task DeclareQueueTopologyAsync(
        IRabbitMqChannel channel,
        RabbitMqAsyncResponseOptions options,
        string exchange,
        string queue,
        string routingKey,
        CancellationToken cancellationToken)
    {
        await channel.ExchangeDeclareAsync(exchange, DirectExchange, durable: true, autoDelete: false, cancellationToken).ConfigureAwait(false);

        var queueArguments = await EnsureDeadLetterTopologyAsync(channel, options, routingKey, cancellationToken).ConfigureAwait(false);

        await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false, queueArguments, cancellationToken).ConfigureAwait(false);
        await channel.QueueBindAsync(queue, exchange, routingKey, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Declares the dead-letter exchange/queue/binding when configured and returns the arguments to apply to the
    /// source queue (<c>x-dead-letter-exchange</c> and optionally <c>x-dead-letter-routing-key</c>), or null when
    /// no dead-letter exchange is configured.
    /// </summary>
    private static async Task<IDictionary<string, object?>?> EnsureDeadLetterTopologyAsync(
        IRabbitMqChannel channel,
        RabbitMqAsyncResponseOptions options,
        string sourceRoutingKey,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(options.DeadLetterExchange))
            return null;

        var deadLetterExchange = options.DeadLetterExchange;
        await channel.ExchangeDeclareAsync(deadLetterExchange, DirectExchange, durable: true, autoDelete: false, cancellationToken).ConfigureAwait(false);

        if (!string.IsNullOrWhiteSpace(options.DeadLetterQueue))
        {
            var bindingRoutingKey = string.IsNullOrWhiteSpace(options.DeadLetterRoutingKey)
                ? sourceRoutingKey
                : options.DeadLetterRoutingKey;

            await channel.QueueDeclareAsync(options.DeadLetterQueue, durable: true, exclusive: false, autoDelete: false, arguments: null, cancellationToken).ConfigureAwait(false);
            await channel.QueueBindAsync(options.DeadLetterQueue, deadLetterExchange, bindingRoutingKey, cancellationToken).ConfigureAwait(false);
        }

        var arguments = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["x-dead-letter-exchange"] = deadLetterExchange
        };

        if (!string.IsNullOrWhiteSpace(options.DeadLetterRoutingKey))
            arguments["x-dead-letter-routing-key"] = options.DeadLetterRoutingKey;

        return arguments;
    }

    /// <summary>Creates the requested resource.</summary>
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
