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
}
