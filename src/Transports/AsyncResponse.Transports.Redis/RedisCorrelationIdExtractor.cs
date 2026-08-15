using StackExchange.Redis;

namespace AsyncResponse.Transports.Redis;

/// <summary>
/// Extracts the AsyncResponse correlation id from an inbound response message: first from the
/// configured stream field, then from the JSON body via the configured paths (walked by the shared
/// <see cref="CorrelationIdJsonPaths"/>).
/// </summary>
internal static class RedisCorrelationIdExtractor
{
    /// <summary>Extracts the correlation id from the supplied message.</summary>
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

        return CorrelationIdJsonPaths.Extract(messageJson, options.CorrelationIdJsonPaths);
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
}
