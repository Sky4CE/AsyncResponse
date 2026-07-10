using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace AsyncResponse.DurableFlows.Internal;

internal static class DurableFlowStoreShared
{
    private static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public static void ValidateSave(string flowId, FlowState state, TimeSpan ttl)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentNullException.ThrowIfNull(state);
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl), "TTL must be greater than zero.");
    }

    public static string Serialize(FlowState state) => JsonSerializer.Serialize(state, Options);

    public static FlowState? Deserialize(string json)
    {
        try
        {
            var state = JsonSerializer.Deserialize<FlowState>(json);
            return state is not null && FlowStateSchema.IsReadable(state.SchemaVersion) ? state : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// Throttles opportunistic expired-state pruning: returns <c>true</c> at most once per
    /// <paramref name="interval"/> (a non-positive interval prunes on every operation, matching the
    /// channel packages). Loads already filter on expiry, so throttling never affects correctness.
    /// </summary>
    public static bool ShouldPrune(ref long lastTicks, TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero)
            return true;

        var now = DateTime.UtcNow.Ticks;
        var last = Interlocked.Read(ref lastTicks);
        return now - last >= interval.Ticks
            && Interlocked.CompareExchange(ref lastTicks, now, last) == last;
    }

    /// <summary>
    /// Advisory-lock key for schema DDL, derived exactly like the channel/transport packages
    /// (FNV-1a over <c>asyncresponse:ddl:{schemaName}</c>) so flow-store DDL serializes with any
    /// channel/transport DDL running against the same schema.
    /// </summary>
    public static long SchemaLockKey(string schemaName)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;
        var hash = offset;
        foreach (var b in Encoding.UTF8.GetBytes(SchemaLockResource(schemaName)))
        {
            hash ^= b;
            hash *= prime;
        }

        return unchecked((long)hash);
    }

    /// <summary>SQL Server <c>sp_getapplock</c> resource name for schema DDL (shared with the channel/transport packages).</summary>
    public static string SchemaLockResource(string schemaName)
        => $"asyncresponse:ddl:{schemaName}";

    public static void ValidateIdentifier(string? value, string optionName, string providerName)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{optionName} must be configured.");

        if (value.Length == 0 || !(char.IsAsciiLetter(value[0]) || value[0] == '_'))
            throw new InvalidOperationException($"{optionName} '{value}' must be a simple {providerName} identifier (letters, digits, and underscores; not starting with a digit).");

        foreach (var c in value)
        {
            if (!(char.IsAsciiLetterOrDigit(c) || c == '_'))
                throw new InvalidOperationException($"{optionName} '{value}' must be a simple {providerName} identifier (letters, digits, and underscores; not starting with a digit).");
        }
    }
}
