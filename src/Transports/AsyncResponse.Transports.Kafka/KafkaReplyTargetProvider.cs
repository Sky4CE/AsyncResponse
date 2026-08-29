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

        // ValidateCommon enforces distinctness for the transport-wide topics; a NAMED target must
        // honor the same rule (DB-transport parity) — aimed at the worker topic its responses are
        // consumed as worker jobs, and aimed at a derived dead-letter topic they are mixed into
        // buried traffic, while the waiter times out.
        if (StringComparer.Ordinal.Equals(responseTopic, schema.WorkerTopic)
            || StringComparer.Ordinal.Equals(responseTopic, schema.DeadLetterTopicFor(schema.WorkerTopic))
            || StringComparer.Ordinal.Equals(responseTopic, schema.DeadLetterTopicFor(schema.ResponseTopic)))
        {
            throw new InvalidOperationException(
                $"Kafka async-response reply target '{targetName}' uses topic '{responseTopic}', which collides with " +
                $"{nameof(KafkaAsyncResponseTransportOptions.WorkerTopic)} or a derived dead-letter topic; " +
                "its responses would be consumed as worker jobs (or mixed into dead letters).");
        }

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
