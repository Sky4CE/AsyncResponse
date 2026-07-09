using System.Text.Json;

namespace AsyncResponse.DurableFlows.Internal;

internal static class DurableFlowStoreShared
{
    public static void ValidateSave(string flowId, FlowState state, TimeSpan ttl)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentNullException.ThrowIfNull(state);
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl), "TTL must be greater than zero.");
    }

    public static string Serialize(FlowState state) => JsonSerializer.Serialize(state);

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
