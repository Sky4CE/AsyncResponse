using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.RabbitMQ;

internal sealed class RabbitMqReplyTargetProvider(
    IOptions<RabbitMqAsyncResponseOptions> _options) : IAsyncResponseReplyTargetProvider
{
    public AsyncResponseReplyTarget GetReplyTarget(string? name = null)
    {
        var options = _options.Value;
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
