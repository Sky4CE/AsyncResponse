using System.Text.Json;
using System.Text.Json.Serialization;

namespace AsyncResponse;

internal static class FlowStateJson
{
    private static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public static string Serialize(FlowState state) => JsonSerializer.Serialize(state, Options);

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
