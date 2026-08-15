using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace AsyncResponse;

/// <summary>
/// The transport envelope wrapping every published response: either a payload
/// (<see cref="Success"/> = true) or a technical failure description.
/// </summary>
/// <typeparam name="T">The payload type.</typeparam>
internal sealed class AsyncResponseEnvelope<T>
{
    /// <summary>
    /// Wire schema version, stamped with <see cref="AsyncResponseEnvelopeSchema.Current"/> when the
    /// envelope is created. The property is required on the wire; a waiter rejects a missing or
    /// unrecognized version rather than risk misreading it.
    /// </summary>
    public int SchemaVersion { get; set; } = AsyncResponseEnvelopeSchema.Current;
    public bool Success { get; set; }
    public T? Payload { get; set; }
    public string? ExceptionMessage { get; set; }
    public string? ExceptionStackTrace { get; set; }
}

/// <summary>
/// Wire-schema version stamp for <see cref="AsyncResponseEnvelope{T}"/>. New envelopes are stamped
/// with <see cref="Current"/>; a waiter rejects any unrecognized version so a publisher cannot feed
/// an incompatible shape to a waiter.
/// </summary>
internal static class AsyncResponseEnvelopeSchema
{
    /// <summary>The current wire schema version written by this build.</summary>
    public const int Current = 1;

    /// <summary>Returns <c>true</c> when an envelope with <paramref name="entryVersion"/> is safe to read on this build.</summary>
    public static bool IsReadable(int entryVersion) => entryVersion == Current;
}

/// <summary>
/// Pre-configured <see cref="JsonSerializerOptions"/> for (de)serializing
/// <see cref="AsyncResponseEnvelope{T}"/> instances with null-payload tolerance.
/// <para>
/// The envelope's metadata is provided converter-backed via
/// <see cref="JsonMetadataServices.CreateValueInfo{T}"/> (statically instantiated per
/// <typeparamref name="T"/>, no reflection), and everything else — including the payload type —
/// resolves through <see cref="AsyncResponseJson.Resolver"/>. Callers needing trim/AOT-clean
/// (de)serialization go through <see cref="AsyncResponseEnvelopeJson"/> rather than the
/// reflection-based <see cref="JsonSerializer"/> overloads.
/// </para>
/// </summary>
internal static class AsyncResponseEnvelopeOptions<T>
{
    public static readonly JsonSerializerOptions Instance = new() { TypeInfoResolver = new EnvelopeResolver() };

    private sealed class EnvelopeResolver : IJsonTypeInfoResolver
    {
        public JsonTypeInfo? GetTypeInfo(Type type, JsonSerializerOptions options)
            => type == typeof(AsyncResponseEnvelope<T>)
                ? JsonMetadataServices.CreateValueInfo<AsyncResponseEnvelope<T>>(options, new AsyncResponseEnvelopeConverter<T>())
                : AsyncResponseJson.Resolver.GetTypeInfo(type, options);
    }
}

/// <summary>
/// Trim/AOT-clean (de)serialization entry points for <see cref="AsyncResponseEnvelope{T}"/> —
/// the typed-metadata counterparts of <c>JsonSerializer.Serialize(envelope,
/// AsyncResponseEnvelopeOptions&lt;T&gt;.Instance)</c> and
/// <c>JsonSafety.SafeDeserialize&lt;AsyncResponseEnvelope&lt;T&gt;&gt;(json, …)</c>.
/// </summary>
internal static class AsyncResponseEnvelopeJson
{
    /// <summary>Typed metadata for <see cref="AsyncResponseEnvelope{T}"/> bound to its pre-configured options.</summary>
    public static JsonTypeInfo<AsyncResponseEnvelope<T>> TypeInfo<T>()
        => AsyncResponseJson.GetTypeInfo<AsyncResponseEnvelope<T>>(AsyncResponseEnvelopeOptions<T>.Instance);

    /// <summary>Serializes an envelope for publishing.</summary>
    public static string Serialize<T>(AsyncResponseEnvelope<T> envelope)
        => JsonSerializer.Serialize(envelope, TypeInfo<T>());

    /// <summary>Deserializes an envelope with the standard broker-ingress guards (see <see cref="JsonSafety"/>).</summary>
    public static AsyncResponseEnvelope<T>? SafeDeserialize<T>(string json)
        => JsonSafety.SafeDeserialize(json, TypeInfo<T>());
}

/// <summary>
/// Custom converter that tolerates a JSON <c>null</c> payload even when <typeparamref name="T"/>
/// is a non-nullable value type, assigning <c>default(T)</c> instead of throwing.
/// </summary>
internal sealed class AsyncResponseEnvelopeConverter<T> : JsonConverter<AsyncResponseEnvelope<T>>
{
    private static readonly JsonEncodedText SchemaVersionName = JsonEncodedText.Encode("SchemaVersion");
    private static readonly JsonEncodedText SuccessName = JsonEncodedText.Encode("Success");
    private static readonly JsonEncodedText PayloadName = JsonEncodedText.Encode("Payload");
    private static readonly JsonEncodedText ExceptionMessageName = JsonEncodedText.Encode("ExceptionMessage");
    private static readonly JsonEncodedText ExceptionStackTraceName = JsonEncodedText.Encode("ExceptionStackTrace");

    /// <summary>Reads the JSON value.</summary>
    public override AsyncResponseEnvelope<T>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException();
        }

        int schemaVersion = default;
        bool hasSchemaVersion = false;
        bool success = false;
        T? payload = default;
        string? exceptionMessage = null;
        string? exceptionStackTrace = null;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                break;

            if (reader.TokenType == JsonTokenType.PropertyName)
            {
                var property = GetProperty(ref reader);
                reader.Read();

                if (property == EnvelopeProperty.SchemaVersion)
                {
                    if (reader.TokenType != JsonTokenType.Number || !reader.TryGetInt32(out schemaVersion))
                        throw new JsonException("SchemaVersion must be an integer.");
                    hasSchemaVersion = true;
                }
                else if (property == EnvelopeProperty.Success)
                {
                    // Guarded token check: GetBoolean on a non-boolean token throws
                    // InvalidOperationException, which the ingress would misread as a TRANSIENT
                    // fault and retry — a malformed envelope must fail fast as a JsonException.
                    if (reader.TokenType is not (JsonTokenType.True or JsonTokenType.False))
                        throw new JsonException("Success must be a boolean.");
                    success = reader.GetBoolean();
                }
                else if (property == EnvelopeProperty.Payload)
                {
                    // Instead of throwing, assign default(T)
                    payload = reader.TokenType == JsonTokenType.Null
                        ? default
                        : JsonSerializer.Deserialize(ref reader, AsyncResponseJson.GetTypeInfo<T>(options));
                }
                else if (property == EnvelopeProperty.ExceptionMessage)
                {
                    if (reader.TokenType is not (JsonTokenType.Null or JsonTokenType.String))
                        throw new JsonException("ExceptionMessage must be a string or null.");
                    exceptionMessage = reader.TokenType == JsonTokenType.Null ? null : reader.GetString();
                }
                else if (property == EnvelopeProperty.ExceptionStackTrace)
                {
                    if (reader.TokenType is not (JsonTokenType.Null or JsonTokenType.String))
                        throw new JsonException("ExceptionStackTrace must be a string or null.");
                    exceptionStackTrace = reader.TokenType == JsonTokenType.Null ? null : reader.GetString();
                }
                else
                {
                    reader.Skip();
                }
            }
        }

        if (!hasSchemaVersion)
            throw new JsonException("SchemaVersion is required.");

        return new AsyncResponseEnvelope<T>
        {
            SchemaVersion = schemaVersion,
            Success = success,
            Payload = payload!,
            ExceptionMessage = exceptionMessage,
            ExceptionStackTrace = exceptionStackTrace
        };
    }

    /// <summary>Writes the JSON value.</summary>
    public override void Write(Utf8JsonWriter writer, AsyncResponseEnvelope<T> value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteNumber(SchemaVersionName, value.SchemaVersion);
        writer.WriteBoolean(SuccessName, value.Success);
        writer.WritePropertyName(PayloadName);
        JsonSerializer.Serialize(writer, (object?)value.Payload, AsyncResponseJson.GetTypeInfo(typeof(T), options));
        writer.WriteString(ExceptionMessageName, value.ExceptionMessage);
        writer.WriteString(ExceptionStackTraceName, value.ExceptionStackTrace);
        writer.WriteEndObject();
    }

    private static EnvelopeProperty GetProperty(ref Utf8JsonReader reader)
    {
        if (!reader.HasValueSequence)
        {
            var name = reader.ValueSpan;
            switch (name.Length)
            {
                case 13 when name[0] == (byte)'S' && reader.ValueTextEquals("SchemaVersion"u8):
                    return EnvelopeProperty.SchemaVersion;
                case 7 when name[0] == (byte)'S' && reader.ValueTextEquals("Success"u8):
                    return EnvelopeProperty.Success;
                case 7 when name[0] == (byte)'P' && reader.ValueTextEquals("Payload"u8):
                    return EnvelopeProperty.Payload;
                case 16 when name[0] == (byte)'E' && reader.ValueTextEquals("ExceptionMessage"u8):
                    return EnvelopeProperty.ExceptionMessage;
                case 19 when name[0] == (byte)'E' && reader.ValueTextEquals("ExceptionStackTrace"u8):
                    return EnvelopeProperty.ExceptionStackTrace;
            }
        }

        if (reader.ValueTextEquals("SchemaVersion"u8))
            return EnvelopeProperty.SchemaVersion;
        if (reader.ValueTextEquals("Success"u8))
            return EnvelopeProperty.Success;
        if (reader.ValueTextEquals("Payload"u8))
            return EnvelopeProperty.Payload;
        if (reader.ValueTextEquals("ExceptionMessage"u8))
            return EnvelopeProperty.ExceptionMessage;
        if (reader.ValueTextEquals("ExceptionStackTrace"u8))
            return EnvelopeProperty.ExceptionStackTrace;

        return EnvelopeProperty.Unknown;
    }

    private enum EnvelopeProperty
    {
        Unknown,
        SchemaVersion,
        Success,
        Payload,
        ExceptionMessage,
        ExceptionStackTrace
    }
}
