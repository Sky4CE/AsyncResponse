using System.Globalization;
using System.Text;

namespace AsyncResponse.Transports.RabbitMQ;

/// <summary>
/// Extracts the AsyncResponse correlation id from an inbound response message: first from the
/// broker's own CorrelationId, then the configured header, then from the JSON body via the
/// configured paths (walked by the shared <see cref="CorrelationIdJsonPaths"/>).
/// </summary>
internal static class RabbitMqCorrelationIdExtractor
{
    /// <summary>Extracts the correlation id from the supplied message.</summary>
    public static string? Extract(
        RabbitMqDelivery delivery,
        string messageJson,
        RabbitMqAsyncResponseOptions options)
    {
        if (!string.IsNullOrWhiteSpace(delivery.BasicProperties.CorrelationId))
            return delivery.BasicProperties.CorrelationId;

        if (!string.IsNullOrWhiteSpace(options.CorrelationIdHeader)
            && delivery.BasicProperties.Headers is not null
            && delivery.BasicProperties.Headers.TryGetValue(options.CorrelationIdHeader, out var header)
            && TryConvertHeader(header) is { } headerValue
            && !string.IsNullOrWhiteSpace(headerValue))
        {
            return headerValue;
        }

        return CorrelationIdJsonPaths.Extract(messageJson, options.CorrelationIdJsonPaths);
    }

    private static string? TryConvertHeader(object? header)
        => header switch
        {
            null => null,
            string s => s,
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.Span),
            // AMQP headers legally carry numeric/timestamp values, and the id must render the same
            // on every consumer: a locale-formatted "1,5" here never matches the "1.5" the waiter
            // registered under, so the wait runs to timeout on hosts with another CurrentCulture.
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => header.ToString()
        };
}
