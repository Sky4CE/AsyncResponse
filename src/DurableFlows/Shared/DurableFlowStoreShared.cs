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

    public static void ValidateCreate(string flowId, FlowState state, TimeSpan ttl)
    {
        ValidateWrite(flowId, state, ttl);
        if (state.Revision != 0)
            throw new ArgumentException("A new flow ledger must start at revision zero.", nameof(state));
    }

    public static void ValidateUpdate(string flowId, FlowState state, long expectedRevision, TimeSpan ttl)
    {
        ValidateWrite(flowId, state, ttl);
        if (expectedRevision < 0)
            throw new ArgumentOutOfRangeException(nameof(expectedRevision), "The expected revision cannot be negative.");
        if (state.Revision != checked(expectedRevision + 1))
            throw new ArgumentException("The new flow-state revision must increment the expected revision by one.", nameof(state));
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

    private static void ValidateWrite(string flowId, FlowState state, TimeSpan ttl)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentNullException.ThrowIfNull(state);
        if (!string.Equals(state.FlowId, flowId, StringComparison.Ordinal))
            throw new ArgumentException("The flow state id must match the store key.", nameof(state));
        if (state.SchemaVersion != FlowStateSchema.Current)
            throw new ArgumentException("The flow state must use the current schema version.", nameof(state));
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl), "TTL must be greater than zero.");
    }
}
