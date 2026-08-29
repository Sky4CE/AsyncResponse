using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.RabbitMQ;

internal sealed class RabbitMqReplyTargetProvider(
    IOptions<RabbitMqAsyncResponseOptions> _options) : IAsyncResponseReplyTargetProvider
{
    /// <summary>Gets the configured reply target.</summary>
    public AsyncResponseReplyTarget GetReplyTarget(string? name = null)
    {
        var options = _options.Value;
        // Options-validator parity with the other transports' providers: a hand-off address must
        // come from a configuration that passes the transport's own checks.
        RabbitMqOptionsValidator.ValidateConnection(options);
        var targetName = string.IsNullOrWhiteSpace(name)
            ? options.DefaultReplyTargetName
            : name;

        var target = ResolveTarget(options, targetName);
        var exchange = RabbitMqOptionsValidator.Required(
            target.Exchange,
            $"{nameof(RabbitMqReplyTargetOptions)}.{nameof(RabbitMqReplyTargetOptions.Exchange)}");
        var routingKey = RabbitMqOptionsValidator.Required(
            target.RoutingKey,
            $"{nameof(RabbitMqReplyTargetOptions)}.{nameof(RabbitMqReplyTargetOptions.RoutingKey)}");
        var queue = target.Queue ?? options.ResponseQueue;

        // A NAMED target must not route into the worker or dead-letter side (DB-transport parity):
        // the worker publish pair delivers its responses as worker jobs, the dead-letter exchange
        // mixes them into buried traffic, and a declared queue equal to the worker/dead-letter
        // queue does the same one hop later — while the waiter times out.
        var routesToWorker = StringComparer.Ordinal.Equals(exchange, options.WorkerExchange)
            && StringComparer.Ordinal.Equals(routingKey, options.WorkerRoutingKey);
        var routesToDeadLetter = !string.IsNullOrWhiteSpace(options.DeadLetterExchange)
            && StringComparer.Ordinal.Equals(exchange, options.DeadLetterExchange);
        var queueCollides = !string.IsNullOrWhiteSpace(queue)
            && (StringComparer.Ordinal.Equals(queue, options.WorkerQueue)
                || (!string.IsNullOrWhiteSpace(options.DeadLetterQueue) && StringComparer.Ordinal.Equals(queue, options.DeadLetterQueue)));
        if (routesToWorker || routesToDeadLetter || queueCollides)
        {
            throw new InvalidOperationException(
                $"RabbitMQ async-response reply target '{targetName}' routes to '{exchange}:{routingKey}'" +
                $"{(string.IsNullOrWhiteSpace(queue) ? "" : $" (queue '{queue}')")}, which collides with the worker or dead-letter destination; " +
                "its responses would be consumed as worker jobs (or mixed into dead letters).");
        }

        var properties = new Dictionary<string, string>(target.Properties, StringComparer.Ordinal)
        {
            ["exchange"] = exchange,
            ["routingKey"] = routingKey
        };

        if (!string.IsNullOrWhiteSpace(queue))
            properties["queue"] = queue;

        return new AsyncResponseReplyTarget
        {
            Name = targetName,
            Transport = RabbitMqAsyncResponseOptions.TransportName,
            Address = $"{exchange}:{routingKey}",
            Properties = properties
        };
    }

    private static RabbitMqReplyTargetOptions ResolveTarget(
        RabbitMqAsyncResponseOptions options,
        string targetName)
    {
        if (options.ReplyTargets.TryGetValue(targetName, out var configured))
            return configured;

        if (StringComparer.Ordinal.Equals(targetName, options.DefaultReplyTargetName)
            && !string.IsNullOrWhiteSpace(options.ResponseExchange)
            && !string.IsNullOrWhiteSpace(options.ResponseRoutingKey))
        {
            return new RabbitMqReplyTargetOptions
            {
                Exchange = options.ResponseExchange,
                RoutingKey = options.ResponseRoutingKey,
                Queue = options.ResponseQueue
            };
        }

        throw new InvalidOperationException(
            $"RabbitMQ async-response reply target '{targetName}' is not configured. " +
            $"Configure {nameof(RabbitMqAsyncResponseOptions.ResponseExchange)} and " +
            $"{nameof(RabbitMqAsyncResponseOptions.ResponseRoutingKey)} for the default target " +
            $"or add a named target with {nameof(RabbitMqAsyncResponseOptions.AddReplyTarget)}.");
    }
}
