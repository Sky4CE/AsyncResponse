using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.NATS;

internal sealed class NatsReplyTargetProvider(
    IOptions<NatsAsyncResponseTransportOptions> _options) : IAsyncResponseReplyTargetProvider
{
    /// <summary>Gets the configured reply target.</summary>
    public AsyncResponseReplyTarget GetReplyTarget(string? name = null)
    {
        var options = _options.Value;
        NatsTransportOptionsValidator.ValidateCommon(options);
        var schema = new NatsTransportSubjectSchema(options);
        var targetName = string.IsNullOrWhiteSpace(name)
            ? options.DefaultReplyTargetName
            : name;

        var target = ResolveTarget(options, schema, targetName);
        var responseSubject = NatsTransportOptionsValidator.Required(
            target.ResponseSubject,
            $"{nameof(NatsReplyTargetOptions)}.{nameof(NatsReplyTargetOptions.ResponseSubject)}");
        var consumer = target.Consumer ?? options.ResponseConsumer;

        var properties = new Dictionary<string, string>(target.Properties, StringComparer.Ordinal)
        {
            ["subject"] = responseSubject,
            ["consumer"] = consumer,
            ["correlationIdHeader"] = options.CorrelationIdHeader
        };

        return new AsyncResponseReplyTarget
        {
            Name = targetName,
            Transport = NatsAsyncResponseTransportOptions.TransportName,
            Address = responseSubject,
            Properties = properties
        };
    }

    private static NatsReplyTargetOptions ResolveTarget(
        NatsAsyncResponseTransportOptions options,
        NatsTransportSubjectSchema schema,
        string targetName)
    {
        if (options.ReplyTargets.TryGetValue(targetName, out var configured))
            return configured;

        if (StringComparer.Ordinal.Equals(targetName, options.DefaultReplyTargetName))
        {
            return new NatsReplyTargetOptions
            {
                ResponseSubject = schema.ResponseSubject,
                Consumer = options.ResponseConsumer
            };
        }

        throw new InvalidOperationException(
            $"NATS async-response reply target '{targetName}' is not configured. " +
            $"Configure {nameof(NatsAsyncResponseTransportOptions.ResponseSubject)} for the default target " +
            $"or add a named target with {nameof(NatsAsyncResponseTransportOptions.AddReplyTarget)}.");
    }
}
