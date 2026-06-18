using System.Text.Json;
using System.Text.Json.Serialization;

namespace AsyncResponse;

/// <summary>
/// The transport envelope wrapping every published response: either a payload
/// (<see cref="Success"/> = true) or a technical failure description.
/// </summary>
/// <typeparam name="T">The payload type.</typeparam>
internal sealed class AsyncResponseEnvelope<T>
{
    public bool Success { get; set; }
    public T? Payload { get; set; }
    public string? ExceptionMessage { get; set; }
    public string? ExceptionStackTrace { get; set; }
}

/// <summary>
/// Pre-configured <see cref="JsonSerializerOptions"/> for (de)serializing
/// <see cref="AsyncResponseEnvelope{T}"/> instances with null-payload tolerance.
/// </summary>
internal static class AsyncResponseEnvelopeOptions<T>
{
    public static readonly JsonSerializerOptions Instance = CreateOptions();

    private static JsonSerializerOptions CreateOptions()
    {
        var options = new JsonSerializerOptions();
        options.Converters.Add(new AsyncResponseEnvelopeConverter<T>());
        return options;
    }
}

/// <summary>
/// Custom converter that tolerates a JSON <c>null</c> payload even when <typeparamref name="T"/>
/// is a non-nullable value type, assigning <c>default(T)</c> instead of throwing.
/// </summary>
internal sealed class AsyncResponseEnvelopeConverter<T> : JsonConverter<AsyncResponseEnvelope<T>>
{
    public override AsyncResponseEnvelope<T>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException();
        }

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
                var isSuccess = reader.ValueTextEquals("Success"u8);
                var isPayload = reader.ValueTextEquals("Payload"u8);
                var isExceptionMessage = reader.ValueTextEquals("ExceptionMessage"u8);
                var isExceptionStackTrace = reader.ValueTextEquals("ExceptionStackTrace"u8);
                reader.Read();

                if (isSuccess)
                {
                    success = reader.GetBoolean();
                }
                else if (isPayload)
                {
                    // Instead of throwing, assign default(T)
                    payload = reader.TokenType == JsonTokenType.Null ? default : JsonSerializer.Deserialize<T>(ref reader, options);
                }
                else if (isExceptionMessage)
                {
                    exceptionMessage = reader.TokenType == JsonTokenType.Null ? null : reader.GetString();
                }
                else if (isExceptionStackTrace)
                {
                    exceptionStackTrace = reader.TokenType == JsonTokenType.Null ? null : reader.GetString();
                }
                else
                {
                    reader.Skip();
                }
            }
        }

        return new AsyncResponseEnvelope<T>
        {
            Success = success,
            Payload = payload!,
            ExceptionMessage = exceptionMessage,
            ExceptionStackTrace = exceptionStackTrace
        };
    }

    public override void Write(Utf8JsonWriter writer, AsyncResponseEnvelope<T> value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteBoolean("Success", value.Success);
        writer.WritePropertyName("Payload");
        JsonSerializer.Serialize(writer, value.Payload, options);
        writer.WriteString("ExceptionMessage", value.ExceptionMessage);
        writer.WriteString("ExceptionStackTrace", value.ExceptionStackTrace);
        writer.WriteEndObject();
    }
}
