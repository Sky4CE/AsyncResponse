using Google.Cloud.PubSub.V1;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace AsyncResponse.Transports.GooglePubSub;

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
