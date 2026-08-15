namespace AsyncResponse.Transports.Kafka;

/// <summary>
/// Extracts the AsyncResponse correlation id from an inbound response message: first from the
/// configured Kafka header, then from the JSON body via the configured paths (walked by the shared
/// <see cref="CorrelationIdJsonPaths"/>).
/// </summary>
internal static class KafkaCorrelationIdExtractor
{
    /// <summary>Extracts the correlation id from the supplied message.</summary>
    public static string? Extract(
        IReadOnlyList<KafkaTransportHeader> headers,
        string messageJson,
        KafkaAsyncResponseTransportOptions options)
    {
        var headerName = KafkaTransportOptionsValidator.Required(
            options.CorrelationIdHeader,
            nameof(options.CorrelationIdHeader));

        var headerValue = TryReadHeader(headers, headerName);
        if (!string.IsNullOrWhiteSpace(headerValue))
            return headerValue;

        return CorrelationIdJsonPaths.Extract(messageJson, options.CorrelationIdJsonPaths);
    }

    internal static string? TryReadHeader(IReadOnlyList<KafkaTransportHeader> headers, string headerName)
    {
        // Kafka headers allow duplicate keys; the first match wins, mirroring broker tooling.
        foreach (var header in headers)
        {
            if (StringComparer.Ordinal.Equals(header.Key, headerName))
                return header.ValueUtf8;
        }

        return null;
    }
}
