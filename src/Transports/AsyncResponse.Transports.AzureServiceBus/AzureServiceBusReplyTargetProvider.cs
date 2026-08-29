using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.AzureServiceBus;

internal sealed class AzureServiceBusReplyTargetProvider(
    IOptions<AzureServiceBusAsyncResponseOptions> _options) : IAsyncResponseReplyTargetProvider
{
    /// <summary>Gets the configured reply target.</summary>
    public AsyncResponseReplyTarget GetReplyTarget(string? name = null)
    {
        var options = _options.Value;
        AzureServiceBusOptionsValidator.ValidateCommon(options);
        var targetName = string.IsNullOrWhiteSpace(name)
            ? options.DefaultReplyTargetName
            : name;

        var target = ResolveTarget(options, targetName);
        var queue = AzureServiceBusOptionsValidator.Required(
            target.Queue,
            $"{nameof(AzureServiceBusReplyTargetOptions)}.{nameof(AzureServiceBusReplyTargetOptions.Queue)}");

        // A NAMED target must not be the worker queue (DB-transport parity): its responses would
        // be consumed as worker jobs, NAK-cycled to the cap and dead-lettered, while the waiter
        // times out. (The dead-letter queue is a sub-entity and cannot collide by name.)
        if (StringComparer.Ordinal.Equals(queue, options.WorkerQueue))
        {
            throw new InvalidOperationException(
                $"Azure Service Bus async-response reply target '{targetName}' uses queue '{queue}', which collides with " +
                $"{nameof(AzureServiceBusAsyncResponseOptions.WorkerQueue)}; its responses would be consumed as worker jobs.");
        }

        var properties = new Dictionary<string, string>(target.Properties, StringComparer.Ordinal)
        {
            ["queue"] = queue,
            ["correlationIdProperty"] = options.CorrelationIdProperty
        };

        return new AsyncResponseReplyTarget
        {
            Name = targetName,
            Transport = AzureServiceBusAsyncResponseOptions.TransportName,
            Address = queue,
            Properties = properties
        };
    }

    private static AzureServiceBusReplyTargetOptions ResolveTarget(
        AzureServiceBusAsyncResponseOptions options,
        string targetName)
    {
        if (options.ReplyTargets.TryGetValue(targetName, out var configured))
            return configured;

        if (StringComparer.Ordinal.Equals(targetName, options.DefaultReplyTargetName))
            return new AzureServiceBusReplyTargetOptions { Queue = options.ResponseQueue };

        throw new InvalidOperationException(
            $"Azure Service Bus async-response reply target '{targetName}' is not configured. " +
            $"Configure {nameof(AzureServiceBusAsyncResponseOptions.ResponseQueue)} for the default target " +
            $"or add a named target with {nameof(AzureServiceBusAsyncResponseOptions.AddReplyTarget)}.");
    }
}
