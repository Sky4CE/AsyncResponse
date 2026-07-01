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
