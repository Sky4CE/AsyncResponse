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

        // ValidateCommon enforces distinctness for the transport-wide streams; a NAMED target
        // must honor the same rule (DB-transport parity) — aimed at the worker or dead-letter
        // stream, its responses are consumed as worker jobs (or mixed into dead letters) while
        // the waiter times out.
        if (StringComparer.Ordinal.Equals(responseStream, schema.WorkerStream.ToString())
            || StringComparer.Ordinal.Equals(responseStream, schema.DeadLetterStream.ToString()))
        {
            throw new InvalidOperationException(
                $"Redis async-response reply target '{targetName}' uses stream '{responseStream}', which collides with " +
                $"{nameof(RedisAsyncResponseTransportOptions.WorkerStream)} or {nameof(RedisAsyncResponseTransportOptions.DeadLetterStream)}; " +
                "its responses would be consumed as worker jobs (or mixed into dead letters).");
        }

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
