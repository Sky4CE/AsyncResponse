using System.Globalization;
using System.Text;

namespace AsyncResponse.Transports.AzureServiceBus;

/// <summary>
/// Extracts the AsyncResponse correlation id from an inbound response message: first from the
/// broker's own CorrelationId, then the configured application property, then from the JSON body
/// via the configured paths (walked by the shared <see cref="CorrelationIdJsonPaths"/>).
/// </summary>
internal static class AzureServiceBusCorrelationIdExtractor
{
    /// <summary>Extracts the correlation id from the supplied Service Bus delivery.</summary>
    public static string? Extract(
        AzureServiceBusTransportDelivery delivery,
        string messageJson,
        AzureServiceBusAsyncResponseOptions options)
    {
        if (!string.IsNullOrWhiteSpace(delivery.CorrelationId))
            return delivery.CorrelationId;

        if (!string.IsNullOrWhiteSpace(options.CorrelationIdProperty)
            && delivery.ApplicationProperties.TryGetValue(options.CorrelationIdProperty, out var property)
            && TryConvertProperty(property) is { } propertyValue
            && !string.IsNullOrWhiteSpace(propertyValue))
        {
            return propertyValue;
        }

        return CorrelationIdJsonPaths.Extract(messageJson, options.CorrelationIdJsonPaths);
    }

    internal static string? TryConvertProperty(object? property)
        => property switch
        {
            null => null,
            string s => s,
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.Span),
            BinaryData data => data.ToString(),
            // AMQP application properties legally carry numeric/timestamp values, and the id must
            // render the same on every consumer: a locale-formatted "1,5" here never matches the
            // "1.5" the waiter registered under, so the wait runs to timeout on hosts with another
            // CurrentCulture.
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => property.ToString()
        };
}
