using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace AsyncResponse.Transports.AzureServiceBus;

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

        var jsonPaths = options.CorrelationIdJsonPaths;
        if (jsonPaths is null || jsonPaths.Length == 0 || string.IsNullOrWhiteSpace(messageJson))
            return null;

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(messageJson);
        }
        catch (JsonException)
        {
            return null;
        }

        if (root is null)
            return null;

        foreach (var path in jsonPaths)
        {
            var value = TryReadPath(root, path);
            if (!string.IsNullOrWhiteSpace(value))
                return value;
        }

        return null;
    }

    internal static string? TryConvertProperty(object? property)
        => property switch
        {
            null => null,
            string s => s,
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.Span),
            BinaryData data => data.ToString(),
            _ => property.ToString()
        };

    private static string? TryReadPath(JsonNode root, string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return null;

        var current = root;
        foreach (var segment in path.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            current = UnwrapJsonString(current);
            if (current is not JsonObject obj)
                return null;

            current = TryGetProperty(obj, segment);
            if (current is null)
                return null;
        }

        current = UnwrapJsonString(current);
        return current switch
        {
            JsonValue value when value.TryGetValue<string>(out var s) => s,
            JsonValue value => value.ToString(),
            _ => null
        };
    }

    private static JsonNode? TryGetProperty(JsonObject obj, string name)
    {
        if (obj.TryGetPropertyValue(name, out var exact))
            return exact;

        foreach (var property in obj)
        {
            if (string.Equals(property.Key, name, StringComparison.OrdinalIgnoreCase))
                return property.Value;
        }

        return null;
    }

    private static JsonNode? UnwrapJsonString(JsonNode? node)
    {
        if (node is not JsonValue value || !value.TryGetValue<string>(out var text))
            return node;

        var trimmed = text.AsSpan().TrimStart();
        if (trimmed.Length == 0 || (trimmed[0] != '{' && trimmed[0] != '['))
            return node;

        try
        {
            return JsonNode.Parse(text);
        }
        catch (JsonException)
        {
            return node;
        }
    }
}
