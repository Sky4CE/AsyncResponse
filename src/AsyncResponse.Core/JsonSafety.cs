using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace AsyncResponse;

/// <summary>
/// Defensive JSON helpers for broker ingress payloads. All overloads resolve contract metadata
/// through <see cref="AsyncResponseJson"/> (trim/AOT-safe) instead of the reflection-based
/// serializer entry points; property matching stays case-insensitive as it always was here.
/// </summary>
internal static class JsonSafety
{
    /// <summary>
    /// Deserializes with guards for the classic broker-ingress garbage: empty bodies and HTML
    /// error pages. Throws <see cref="InvalidDataException"/> with the offending prefix so the
    /// failure is diagnosable from logs.
    /// </summary>
    public static T? SafeDeserialize<T>(string json, JsonSerializerOptions? options = null)
        => SafeDeserialize(json, AsyncResponseJson.GetTypeInfo<T>(WithResolver(options)));

    /// <summary>Deserializes with the ingress guards using pre-resolved contract metadata.</summary>
    public static T? SafeDeserialize<T>(string json, JsonTypeInfo<T> typeInfo)
    {
        ThrowIfClearlyNotJson(json);

        try
        {
            return JsonSerializer.Deserialize(json, typeInfo);
        }
        catch (JsonException jsonException)
        {
            // Re-throw with the payload prefix in the message so the failure is diagnosable.
            throw new InvalidDataException($"Failed to parse JSON payload: {json[..Math.Min(200, json.Length)]}…", jsonException);
        }
    }

    /// <summary>
    /// Non-generic counterpart for callers that only know the target type at runtime (e.g.
    /// materializing a persisted flow input).
    /// </summary>
    public static object? SafeDeserialize(string json, Type returnType, JsonSerializerOptions? options = null)
    {
        ThrowIfClearlyNotJson(json);

        try
        {
            return JsonSerializer.Deserialize(json, AsyncResponseJson.GetTypeInfo(returnType, WithResolver(options)));
        }
        catch (JsonException jsonException)
        {
            // Re-throw with the payload prefix in the message so the failure is diagnosable.
            throw new InvalidDataException($"Failed to parse JSON payload: {json[..Math.Min(200, json.Length)]}…", jsonException);
        }
    }

    /// <summary>
    /// Defaults to the library's case-insensitive chain options. Caller-supplied options are
    /// honored exactly as the reflection-based overloads honored them: an instance with no
    /// resolver gets the runtime's default reflection resolver bound (that is what
    /// <c>JsonSerializer.Deserialize(json, options)</c> used to do on first use), which throws at
    /// runtime when the app disabled reflection-based serialization — same as before, but without
    /// carrying IL2026/IL3050.
    /// </summary>
    private static JsonSerializerOptions WithResolver(JsonSerializerOptions? options)
    {
        if (options is null)
            return AsyncResponseJson.CaseInsensitive;

        // When reflection is unavailable (trimmed/AOT) the resolver stays null and GetTypeInfo
        // surfaces the actionable register-a-context error instead.
        if (options.TypeInfoResolver is null && JsonSerializer.IsReflectionEnabledByDefault)
            PopulateReflectionResolver(options);

        return options;

        [UnconditionalSuppressMessage("Trimming", "IL2026",
            Justification = "Reachable only when JsonSerializer.IsReflectionEnabledByDefault is true; trimmed and AOT builds substitute the feature switch to false and skip this call.")]
        [UnconditionalSuppressMessage("AOT", "IL3050",
            Justification = "Same guard: never reached under Native AOT.")]
        static void PopulateReflectionResolver(JsonSerializerOptions options)
            => options.MakeReadOnly(populateMissingResolver: true);
    }

    /// <summary>Runs the ThrowIfClearlyNotJson operation.</summary>
    public static void ThrowIfClearlyNotJson(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            throw new InvalidDataException("Empty message body when JSON was expected.");

        var trimmed = json.AsSpan().TrimStart();

        // Guard against HTML error pages.
        if (trimmed[0] == '<')
            throw new InvalidDataException($"Received HTML when JSON was expected: {json[..Math.Min(200, json.Length)]}…");
    }
}
