using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.Redis;

internal sealed class RedisReplyTargetProvider(
    IOptions<RedisAsyncResponseTransportOptions> _options) : IAsyncResponseReplyTargetProvider
{
    /// <summary>Gets the configured reply target.</summary>
    public AsyncResponseReplyTarget GetReplyTarget(string? name = null)
    {
        var options = _options.Value;
        RedisTransportOptionsValidator.ValidateCommon(options);
        var schema = new RedisTransportKeySchema(options);
        var targetName = string.IsNullOrWhiteSpace(name)
            ? options.DefaultReplyTargetName
            : name;

        var target = ResolveTarget(options, schema, targetName);
        var responseStream = RedisTransportOptionsValidator.Required(
            target.ResponseStream,
            $"{nameof(RedisReplyTargetOptions)}.{nameof(RedisReplyTargetOptions.ResponseStream)}");
        var consumerGroup = target.ConsumerGroup ?? options.ResponseConsumerGroup;

        var properties = new Dictionary<string, string>(target.Properties, StringComparer.Ordinal)
        {
            ["stream"] = responseStream,
            ["consumerGroup"] = consumerGroup,
            ["payloadField"] = options.PayloadField,
            ["correlationIdField"] = options.CorrelationIdField
        };

        return new AsyncResponseReplyTarget
        {
            Name = targetName,
            Transport = RedisAsyncResponseTransportOptions.TransportName,
            Address = responseStream,
            Properties = properties
        };
    }

    private static RedisReplyTargetOptions ResolveTarget(
        RedisAsyncResponseTransportOptions options,
        RedisTransportKeySchema schema,
        string targetName)
    {
        if (options.ReplyTargets.TryGetValue(targetName, out var configured))
            return configured;

        if (StringComparer.Ordinal.Equals(targetName, options.DefaultReplyTargetName))
        {
            return new RedisReplyTargetOptions
            {
                ResponseStream = schema.ResponseStream.ToString(),
                ConsumerGroup = options.ResponseConsumerGroup
            };
        }

        throw new InvalidOperationException(
            $"Redis async-response reply target '{targetName}' is not configured. " +
            $"Configure {nameof(RedisAsyncResponseTransportOptions.ResponseStream)} for the default target " +
            $"or add a named target with {nameof(RedisAsyncResponseTransportOptions.AddReplyTarget)}.");
    }
}
