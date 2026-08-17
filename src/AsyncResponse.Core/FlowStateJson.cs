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

    /// <summary>
    /// Materializes a ledger row that the store has already found. Every failure here means the
    /// row EXISTS and cannot be read, which is categorically different from the row being absent —
    /// so none of them returns <c>null</c>. See <see cref="FlowStateUnreadableException"/> for why
    /// that difference decides whether a wake-up may be acknowledged.
    /// </summary>
    /// <exception cref="FlowStateUnreadableException">The row is present but uninterpretable.</exception>
    public static FlowState Deserialize(string json, string flowId)
    {
        FlowState? state;
        try
        {
            state = JsonSerializer.Deserialize(json, TypeInfo);
        }
        catch (JsonException ex)
        {
            throw new FlowStateUnreadableException(flowId, "the stored JSON is malformed", ex);
        }

        if (state is null)
            throw new FlowStateUnreadableException(flowId, "the stored JSON is the literal null");

        if (!FlowStateSchema.IsReadable(state.SchemaVersion))
        {
            throw new FlowStateUnreadableException(
                flowId,
                $"its schema version is {state.SchemaVersion} and this build reads {FlowStateSchema.Current}");
        }

        return state;
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
