using Google.Cloud.PubSub.V1;

namespace AsyncResponse.Transports.GooglePubSub;

/// <summary>
/// Extracts the AsyncResponse correlation id from an inbound response message: first from the
/// configured message attribute, then from the JSON body via the configured paths (walked by the
/// shared <see cref="CorrelationIdJsonPaths"/>).
/// </summary>
internal static class GooglePubSubCorrelationIdExtractor
{
    /// <summary>Extracts the correlation id from the supplied message.</summary>
    public static string? Extract(
        PubsubMessage message,
        string messageJson,
        GooglePubSubAsyncResponseOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.CorrelationIdAttribute)
            && message.Attributes.TryGetValue(options.CorrelationIdAttribute, out var attributeValue)
            && !string.IsNullOrWhiteSpace(attributeValue))
        {
            return attributeValue;
        }

        return CorrelationIdJsonPaths.Extract(messageJson, options.CorrelationIdJsonPaths);
    }
}
