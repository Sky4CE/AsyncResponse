using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace AsyncResponse;

/// <summary>
/// Defensive JSON helpers for broker ingress payloads. All overloads resolve contract metadata
/// through <see cref="AsyncResponseJson"/> (trim/AOT-safe) instead of the reflection-based
/// serializer entry points; property matching stays case-insensitive as it always was here.
/// <para>
/// <b>No body in any message these throw.</b> These run at the ingress, where every inbound
/// response and worker envelope passes through, and the exceptions they raise are logged
/// (<c>AsyncResponseIngress</c>) and, on the response path, republished through
/// <c>SetException</c> to the waiter. A payload prefix in the message therefore reached both the
/// application log and a remote consumer, which is exactly what "the library never logs a message
/// body" (docs/security.md) rules out — arguments, tenant/auth baggage, PII and proxy HTML all
/// travel in that first 200 characters. What is left is size and JSON position, which locate the
/// fault without disclosing it.
/// </para>
/// </summary>
internal static class JsonSafety
{
    /// <summary>
    /// Deserializes with guards for the classic broker-ingress garbage: empty bodies and HTML
    /// error pages. Throws <see cref="InvalidDataException"/> describing the failure's size and
    /// position — never its content — so the failure is diagnosable from logs.
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
            throw ParseFailure(json, jsonException);
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
            throw ParseFailure(json, jsonException);
        }
    }

    /// <summary>
    /// Builds the body-free parse failure: size plus the JSON coordinates the reader stopped at.
    /// <para>
    /// The <see cref="JsonException"/> rides along as the inner exception because it carries the
    /// structural detail (expected token, contract path, line/byte position) that makes a parse
    /// failure actionable, and its own message is bounded metadata — at worst the single offending
    /// character, never a span of the body. That is the line this helper draws: enough to find the
    /// fault, never enough to read the payload.
    /// </para>
    /// </summary>
    private static InvalidDataException ParseFailure(string json, JsonException jsonException)
        => new(
            $"Failed to parse JSON payload ({json.Length} UTF-16 code units) at line {Describe(jsonException.LineNumber)}, " +
            $"byte position {Describe(jsonException.BytePositionInLine)}.",
            jsonException);

    private static string Describe(long? position)
        => position?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "unknown";

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

        // Guard against HTML error pages. Size only, never the markup: the classic source of one
        // is a reverse proxy or auth gateway answering in place of the service, and its body is
        // the last thing to copy into a log — it carries session banners, internal hostnames and
        // whatever the gateway decided to echo back. The '<' that triggered this is already the
        // whole diagnosis.
        if (trimmed[0] == '<')
            throw new InvalidDataException($"Received HTML when JSON was expected ({json.Length} UTF-16 code units).");
    }
}
