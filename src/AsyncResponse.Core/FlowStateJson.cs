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
    /// the captured ambient <see cref="FlowState.Context"/> (propagation machinery — it can carry
    /// principal/tenant values — that the parent never needs) and without the child's OWN
    /// memoized child snapshots: a step whose <see cref="FlowStepState.ChildFlowId"/> is set has
    /// its <see cref="FlowStepState.ResultJson"/> elided (the id, completion, and fault marker
    /// stay). The snapshot is stored as a JSON <em>string</em> inside the parent's ledger, so every
    /// ancestor level re-escapes the level below it; carrying grandchild snapshots along made the
    /// ledger grow exponentially with nesting depth (a 72-byte leaf became ~77 KB at depth 12 and
    /// ~600 KB at depth 15 — past DynamoDB's item cap — with no business payload at all). Eliding
    /// them makes a memoized snapshot depth-independent: a parent holds its direct children's
    /// outcomes and local step results; a grandchild's own snapshot lives in the grandchild's
    /// ledger, reachable by the elided step's <c>ChildFlowId</c> while that ledger lives.
    /// The instance handed in is restored before returning.
    /// </summary>
    public static string SerializeSnapshot(FlowState state)
    {
        var context = state.Context;
        state.Context = null;

        List<(FlowStepState Step, string ResultJson)>? elided = null;
        if (state.Steps is { } steps)
        {
            foreach (var step in steps.Values)
            {
                if (step.ChildFlowId is null || step.ResultJson is null)
                    continue;

                (elided ??= []).Add((step, step.ResultJson));
                step.ResultJson = null;
            }
        }

        try
        {
            return Serialize(state);
        }
        finally
        {
            state.Context = context;
            if (elided is not null)
            {
                foreach (var (step, resultJson) in elided)
                    step.ResultJson = resultJson;
            }
        }
    }
}
