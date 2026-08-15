using System.Runtime.CompilerServices;
using System.Text.Json;

namespace AsyncResponse.Transports;

// Shared source for every transport's correlation-id extractor (the 7 broker transports plus
// DbCorrelationIdExtractor, which the 3 database transports funnel through): each csproj pulls
// this file in via <Compile Include="..\Shared\CorrelationIdJsonPaths.cs" />, so it compiles INTO
// each provider assembly. The provider-specific fast path (header/property/attribute lookup) stays
// in the per-transport extractor; only the JSON-body path walk lives here.

/// <summary>
/// Locates the AsyncResponse correlation id inside a message body via configured dotted JSON paths.
/// Configured paths are pre-split into segments once per distinct <c>CorrelationIdJsonPaths</c>
/// array instead of on every delivered message — the array is startup configuration read by every
/// provider's options, not per-message data. The body is walked over a <see cref="JsonDocument"/>
/// rather than a mutable <see cref="System.Text.Json.Nodes.JsonNode"/> DOM: a <see
/// cref="JsonDocument"/> rents its buffer from the array pool and returns it on <c>Dispose</c>,
/// and <see cref="JsonElement.EnumerateObject"/> never allocates a wrapper node per property, where
/// the DOM allocates one node per visited property and is never disposed by its caller.
/// </summary>
internal static class CorrelationIdJsonPaths
{
    // Keyed by the options-held array's identity (never its content): the array is validated once
    // at subscriber startup and re-read on every delivered message afterward, so this amortizes
    // path.Split across the process lifetime of a subscriber instead of paying it per message. A
    // ConditionalWeakTable lets a replaced array (or one built by a short-lived options instance in
    // a test) fall out of the cache with the array itself instead of being pinned forever.
    private static readonly ConditionalWeakTable<string[], string[][]> SplitPathCache = new();

    /// <summary>
    /// Returns the first non-blank value found by walking <paramref name="jsonPaths"/> against
    /// <paramref name="messageJson"/> in order, or <see langword="null"/> when none resolve.
    /// </summary>
    public static string? Extract(string messageJson, string[]? jsonPaths)
    {
        if (jsonPaths is null || jsonPaths.Length == 0 || string.IsNullOrWhiteSpace(messageJson))
            return null;

        var splitPaths = GetSplitPaths(jsonPaths);

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(messageJson);
        }
        catch (JsonException)
        {
            return null;
        }

        using (document)
        {
            foreach (var segments in splitPaths)
            {
                var value = TryReadPath(document.RootElement, segments);
                if (!string.IsNullOrWhiteSpace(value))
                    return value;
            }
        }

        return null;
    }

    private static string[][] GetSplitPaths(string[] jsonPaths)
        => SplitPathCache.GetValue(jsonPaths, static paths =>
        {
            // A blank configured path is dropped here rather than cached as a no-op entry,
            // mirroring the pre-shared walker's per-call IsNullOrWhiteSpace(path) short-circuit.
            // A non-blank path of only dots/whitespace (e.g. ".") is NOT blank by that check, so
            // it is kept even though it splits to zero segments — TryReadPath below walks zero
            // steps and reads the message root itself, same as the pre-shared walker did.
            var nonBlankCount = 0;
            foreach (var path in paths)
            {
                if (!string.IsNullOrWhiteSpace(path))
                    nonBlankCount++;
            }

            var split = new string[nonBlankCount][];
            var index = 0;
            foreach (var path in paths)
            {
                if (string.IsNullOrWhiteSpace(path))
                    continue;
                split[index++] = path.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            }

            return split;
        });

    private static string? TryReadPath(JsonElement root, string[] segments)
    {
        var current = root;

        // An embedded JSON string can appear at any segment, so the unwrap below can reparse
        // more than once per path; only the most recent reparse needs to stay alive; a value it
        // produced is copied out (GetString/GetRawText) before it is disposed.
        JsonDocument? scratch = null;
        try
        {
            foreach (var segment in segments)
            {
                current = UnwrapJsonString(current, ref scratch);
                if (current.ValueKind != JsonValueKind.Object)
                    return null;

                if (!TryGetProperty(current, segment, out var next))
                    return null;

                current = next;
            }

            current = UnwrapJsonString(current, ref scratch);
            return current.ValueKind switch
            {
                JsonValueKind.String => current.GetString(),
                JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False => current.GetRawText(),
                _ => null
            };
        }
        finally
        {
            scratch?.Dispose();
        }
    }

    // Mirrors JsonObject.TryGetPropertyValue then a case-insensitive scan: an exact (ordinal) match
    // wins when present, else the first case-insensitive match in document order. JsonObject
    // materializes its whole backing dictionary the instant any property is touched, so on this
    // runtime looking up ANY property of an object that contains an exact-duplicate key anywhere —
    // not only when the duplicate is the key being requested — throws ArgumentException. A
    // JsonElement walk materializes nothing, so the object is scanned once here to detect and
    // reproduce that rather than silently resolving to one of the duplicates.
    private static bool TryGetProperty(JsonElement obj, string name, out JsonElement value)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var exactFound = false;
        JsonElement exactValue = default;
        var caseInsensitiveFound = false;
        JsonElement caseInsensitiveValue = default;

        foreach (var property in obj.EnumerateObject())
        {
            if (!seen.Add(property.Name))
                throw new ArgumentException($"An item with the same key has already been added. Key: {property.Name}");

            if (!exactFound && string.Equals(property.Name, name, StringComparison.Ordinal))
            {
                exactFound = true;
                exactValue = property.Value;
            }
            else if (!caseInsensitiveFound && string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                caseInsensitiveFound = true;
                caseInsensitiveValue = property.Value;
            }
        }

        if (exactFound)
        {
            value = exactValue;
            return true;
        }

        if (caseInsensitiveFound)
        {
            value = caseInsensitiveValue;
            return true;
        }

        value = default;
        return false;
    }

    // A JSON-string-encoded object/array (e.g. a broker envelope carrying its payload as an escaped
    // string) is parsed and descended into transparently, same as the pre-shared walker.
    private static JsonElement UnwrapJsonString(JsonElement element, ref JsonDocument? scratch)
    {
        if (element.ValueKind != JsonValueKind.String)
            return element;

        var text = element.GetString();
        if (text is null)
            return element;

        var trimmed = text.AsSpan().TrimStart();
        if (trimmed.Length == 0 || (trimmed[0] != '{' && trimmed[0] != '['))
            return element;

        JsonDocument parsed;
        try
        {
            parsed = JsonDocument.Parse(text);
        }
        catch (JsonException)
        {
            return element;
        }

        scratch?.Dispose();
        scratch = parsed;
        return parsed.RootElement;
    }
}
