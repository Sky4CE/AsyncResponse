using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.MongoDB;

internal sealed class MongoDbReplyTargetProvider(
    IOptions<MongoDbAsyncResponseTransportOptions> _options) : IAsyncResponseReplyTargetProvider
{
    /// <inheritdoc />
    public AsyncResponseReplyTarget GetReplyTarget(string? name = null)
    {
        var options = _options.Value;
        MongoDbTransportOptionsValidator.ValidateCommon(options);
        var targetName = string.IsNullOrWhiteSpace(name)
            ? options.DefaultReplyTargetName
            : name;

        var target = ResolveTarget(options, targetName);
        var responseQueue = MongoDbTransportOptionsValidator.Required(
            target.ResponseQueue,
            $"{nameof(MongoDbReplyTargetOptions)}.{nameof(MongoDbReplyTargetOptions.ResponseQueue)}");

        var properties = new Dictionary<string, string>(target.Properties, StringComparer.Ordinal)
        {
            ["collection"] = options.MessageCollection,
            ["queue"] = responseQueue,
            ["correlationIdHeader"] = options.CorrelationIdHeader
        };
        if (!string.IsNullOrWhiteSpace(options.DatabaseName))
            properties["database"] = options.DatabaseName!;

        return new AsyncResponseReplyTarget
        {
            Name = targetName,
            Transport = MongoDbAsyncResponseTransportOptions.TransportName,
            Address = responseQueue,
            Properties = properties
        };
    }

    private static MongoDbReplyTargetOptions ResolveTarget(
        MongoDbAsyncResponseTransportOptions options,
        string targetName)
    {
        if (options.ReplyTargets.TryGetValue(targetName, out var configured))
            return configured;

        if (StringComparer.Ordinal.Equals(targetName, options.DefaultReplyTargetName))
            return new MongoDbReplyTargetOptions { ResponseQueue = options.ResponseQueue };

        throw new InvalidOperationException(
            $"MongoDB async-response reply target '{targetName}' is not configured. " +
            $"Configure {nameof(MongoDbAsyncResponseTransportOptions.ResponseQueue)} for the default target " +
            $"or add a named target with {nameof(MongoDbAsyncResponseTransportOptions.AddReplyTarget)}.");
    }
}
