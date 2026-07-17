using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace AsyncResponse;

internal static class FlowStateJson
{
    // FlowState is a library wire type: its metadata is source-generated
    // (AsyncResponseJsonContext), and the ledger omits nulls exactly as before.
    private static JsonTypeInfo<FlowState> TypeInfo
        => AsyncResponseJson.GetTypeInfo<FlowState>(AsyncResponseJson.IgnoreNullWrites);

    public static string Serialize(FlowState state) => JsonSerializer.Serialize(state, TypeInfo);

    public static FlowState? Deserialize(string json)
    {
        try
        {
            var state = JsonSerializer.Deserialize(json, TypeInfo);
            return state is not null && FlowStateSchema.IsReadable(state.SchemaVersion) ? state : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    public static bool JsonEquivalent(string? left, string right)
    {
        if (string.Equals(left, right, StringComparison.Ordinal))
            return true;
        if (left is null)
            return false;

        try
        {
            return JsonNode.DeepEquals(JsonNode.Parse(left), JsonNode.Parse(right));
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>
    /// Serializes a child <see cref="FlowState"/> for memoization as a parent step result, without
    /// the captured ambient <see cref="FlowState.Context"/>: it is propagation machinery (it can
    /// carry principal/tenant values) that the parent never needs, and dropping it keeps nested
    /// child snapshots from compounding ledger size.
    /// </summary>
    public static string SerializeSnapshot(FlowState state)
    {
        var context = state.Context;
        state.Context = null;
        try
        {
            return Serialize(state);
        }
        finally
        {
            state.Context = context;
        }
    }
}
