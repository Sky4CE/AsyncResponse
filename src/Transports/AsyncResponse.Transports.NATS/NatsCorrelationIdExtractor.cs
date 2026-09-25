using System.Text;

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
        {
            // NATS.Net encodes and decodes header values as ASCII by default (NatsOpts.HeaderEncoding),
            // so a non-ASCII correlation id arrives with '?' in place of each non-ASCII character
            // (our own publish) or UTF-8 byte (a producer that wrote UTF-8). Routing on that
            // mangled value found no waiter and no registration, and the response was dropped while
            // the real waiter timed out. When the header is exactly the mangled form of the body's
            // id, the body's id is the real one; any other header still wins.
            if (headerValue.Contains('?')
                && CorrelationIdJsonPaths.Extract(messageJson, options.CorrelationIdJsonPaths) is { } bodyId
                && IsAsciiMangledForm(headerValue, bodyId))
            {
                return bodyId;
            }

            return headerValue;
        }

        return CorrelationIdJsonPaths.Extract(messageJson, options.CorrelationIdJsonPaths);
    }

    private static bool IsAsciiMangledForm(string headerValue, string bodyId)
        => !string.Equals(headerValue, bodyId, StringComparison.Ordinal)
           && (string.Equals(headerValue, Encoding.ASCII.GetString(Encoding.ASCII.GetBytes(bodyId)), StringComparison.Ordinal)
               || string.Equals(headerValue, Encoding.ASCII.GetString(Encoding.UTF8.GetBytes(bodyId)), StringComparison.Ordinal));
}
