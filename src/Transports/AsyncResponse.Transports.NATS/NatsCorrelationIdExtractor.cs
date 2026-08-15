namespace AsyncResponse.Transports.NATS;

/// <summary>
/// Extracts the AsyncResponse correlation id from an inbound response message: first from the
/// configured NATS header, then from the JSON body via the configured paths (walked by the shared
/// <see cref="CorrelationIdJsonPaths"/>).
/// </summary>
internal static class NatsCorrelationIdExtractor
{
    /// <summary>Extracts the correlation id from the supplied message.</summary>
    public static string? Extract(
        IReadOnlyDictionary<string, string>? headers,
        string messageJson,
        NatsAsyncResponseTransportOptions options)
    {
        var headerName = NatsTransportOptionsValidator.Required(options.CorrelationIdHeader, nameof(options.CorrelationIdHeader));

        if (headers is not null && headers.TryGetValue(headerName, out var headerValue) && !string.IsNullOrWhiteSpace(headerValue))
            return headerValue;

        return CorrelationIdJsonPaths.Extract(messageJson, options.CorrelationIdJsonPaths);
    }
}
