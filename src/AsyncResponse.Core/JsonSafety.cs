using System.Text.Json;

namespace AsyncResponse;

/// <summary>
/// Defensive JSON helpers for broker ingress payloads.
/// </summary>
internal static class JsonSafety
{
    private static readonly JsonSerializerOptions _defaultOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    /// <summary>
    /// Deserializes with guards for the classic broker-ingress garbage: empty bodies and HTML
    /// error pages. Throws <see cref="InvalidDataException"/> with the offending prefix so the
    /// failure is diagnosable from logs.
    /// </summary>
    public static T? SafeDeserialize<T>(string json, JsonSerializerOptions? options = null)
    {
        var trimmed = json.AsSpan().TrimStart();
        if (trimmed.Length == 0)
            throw new InvalidDataException("Empty message body when JSON was expected.");

        // Guard against HTML error pages.
        if (trimmed[0] == '<')
            throw new InvalidDataException($"Received HTML when JSON was expected: {json[..Math.Min(200, json.Length)]}…");

        try
        {
            return JsonSerializer.Deserialize<T>(json, options ?? _defaultOptions);
        }
        catch (JsonException jsonException)
        {
            // Re-throw with the payload prefix in the message so the failure is diagnosable.
            throw new InvalidDataException($"Failed to parse JSON payload: {json[..Math.Min(200, json.Length)]}…", jsonException);
        }
    }
}
