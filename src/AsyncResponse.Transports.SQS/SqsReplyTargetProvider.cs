using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.SQS;

internal sealed class SqsReplyTargetProvider(
    IOptions<SqsAsyncResponseOptions> _options) : IAsyncResponseReplyTargetProvider
{
    /// <summary>Gets the configured reply target.</summary>
    public AsyncResponseReplyTarget GetReplyTarget(string? name = null)
    {
        var options = _options.Value;
        SqsOptionsValidator.ValidateCommon(options);
        var targetName = string.IsNullOrWhiteSpace(name)
            ? options.DefaultReplyTargetName
            : name;

        var target = ResolveTarget(options, targetName);
        var queue = SqsOptionsValidator.Required(
            target.Queue,
            $"{nameof(SqsReplyTargetOptions)}.{nameof(SqsReplyTargetOptions.Queue)}");

        var properties = new Dictionary<string, string>(target.Properties, StringComparer.Ordinal)
        {
            ["queue"] = queue,
            ["correlationIdAttribute"] = options.CorrelationIdAttribute
        };

        return new AsyncResponseReplyTarget
        {
            Name = targetName,
            Transport = SqsAsyncResponseOptions.TransportName,
            Address = queue,
            Properties = properties
        };
    }

    private static SqsReplyTargetOptions ResolveTarget(
        SqsAsyncResponseOptions options,
        string targetName)
    {
        if (options.ReplyTargets.TryGetValue(targetName, out var configured))
            return configured;

        if (StringComparer.Ordinal.Equals(targetName, options.DefaultReplyTargetName))
            return new SqsReplyTargetOptions { Queue = options.ResponseQueue };

        throw new InvalidOperationException(
            $"SQS async-response reply target '{targetName}' is not configured. " +
            $"Configure {nameof(SqsAsyncResponseOptions.ResponseQueue)} for the default target " +
            $"or add a named target with {nameof(SqsAsyncResponseOptions.AddReplyTarget)}.");
    }
}
