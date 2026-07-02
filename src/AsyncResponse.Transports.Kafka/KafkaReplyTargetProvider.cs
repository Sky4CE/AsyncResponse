using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.Kafka;

internal sealed class KafkaReplyTargetProvider(
    IOptions<KafkaAsyncResponseTransportOptions> _options) : IAsyncResponseReplyTargetProvider
{
    /// <summary>Gets the configured reply target.</summary>
    public AsyncResponseReplyTarget GetReplyTarget(string? name = null)
    {
        var options = _options.Value;
        KafkaTransportOptionsValidator.ValidateCommon(options);
        var schema = new KafkaTransportTopicSchema(options);
        var targetName = string.IsNullOrWhiteSpace(name)
            ? options.DefaultReplyTargetName
            : name;

        var target = ResolveTarget(options, schema, targetName);
        var responseTopic = KafkaTransportOptionsValidator.Required(
            target.ResponseTopic,
            $"{nameof(KafkaReplyTargetOptions)}.{nameof(KafkaReplyTargetOptions.ResponseTopic)}");
        var consumerGroup = target.ConsumerGroup ?? options.ResponseConsumerGroup;

        var properties = new Dictionary<string, string>(target.Properties, StringComparer.Ordinal)
        {
            ["topic"] = responseTopic,
            ["consumerGroup"] = consumerGroup,
            ["correlationIdHeader"] = options.CorrelationIdHeader
        };

        if (!string.IsNullOrWhiteSpace(options.BootstrapServers))
            properties["bootstrapServers"] = options.BootstrapServers!;

        return new AsyncResponseReplyTarget
        {
            Name = targetName,
            Transport = KafkaAsyncResponseTransportOptions.TransportName,
            Address = responseTopic,
            Properties = properties
        };
    }

    private static KafkaReplyTargetOptions ResolveTarget(
        KafkaAsyncResponseTransportOptions options,
        KafkaTransportTopicSchema schema,
        string targetName)
    {
        if (options.ReplyTargets.TryGetValue(targetName, out var configured))
            return configured;

        if (StringComparer.Ordinal.Equals(targetName, options.DefaultReplyTargetName))
        {
            return new KafkaReplyTargetOptions
            {
                ResponseTopic = schema.ResponseTopic,
                ConsumerGroup = options.ResponseConsumerGroup
            };
        }

        throw new InvalidOperationException(
            $"Kafka async-response reply target '{targetName}' is not configured. " +
            $"Configure {nameof(KafkaAsyncResponseTransportOptions.ResponseTopic)} for the default target " +
            $"or add a named target with {nameof(KafkaAsyncResponseTransportOptions.AddReplyTarget)}.");
    }
}
