using StackExchange.Redis;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace AsyncResponse.Transports.Redis;

internal static class RedisCorrelationIdExtractor
{
    public static string? Extract(
        StreamEntry entry,
        string messageJson,
        RedisAsyncResponseTransportOptions options)
    {
        var field = RedisTransportOptionsValidator.Required(
            options.CorrelationIdField,
            nameof(options.CorrelationIdField));

        var fieldValue = TryReadField(entry, field);
        if (!string.IsNullOrWhiteSpace(fieldValue))
            return fieldValue;

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

    internal static string? TryReadField(StreamEntry entry, string fieldName)
    {
        // A tombstone left after trimming (an entry still referenced by a PEL but deleted from the
        // stream) has null Values; treat it as a missing field rather than throwing.
        if (entry.Values is null)
            return null;

        foreach (var value in entry.Values)
        {
            if (StringComparer.Ordinal.Equals(value.Name.ToString(), fieldName))
                return value.Value.ToString();
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
