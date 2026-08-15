namespace AsyncResponse.Transports.SQS;

/// <summary>
/// Extracts the AsyncResponse correlation id from an inbound response message: first from the
/// configured message attribute, then from the JSON body via the configured paths (walked by the
/// shared <see cref="CorrelationIdJsonPaths"/>).
/// </summary>
internal static class SqsCorrelationIdExtractor
{
    /// <summary>Extracts the correlation id from the supplied SQS delivery.</summary>
    public static string? Extract(
        SqsTransportDelivery delivery,
        string messageJson,
        SqsAsyncResponseOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.CorrelationIdAttribute)
            && delivery.MessageAttributes.TryGetValue(options.CorrelationIdAttribute, out var attributeValue)
            && !string.IsNullOrWhiteSpace(attributeValue))
        {
            return attributeValue;
        }

        return CorrelationIdJsonPaths.Extract(messageJson, options.CorrelationIdJsonPaths);
    }
}
