namespace AsyncResponse;

/// <summary>A deterministic rejection of a ledger exceeding its state store's size budget.</summary>
public sealed class FlowStateTooLargeException : InvalidOperationException
{
    /// <summary>Creates a size rejection with the measured size and provider budget.</summary>
    public FlowStateTooLargeException(string flowId, long serializedSizeBytes, long maxStateBytes, string providerName)
        : base($"Flow '{flowId}' state serialized to {serializedSizeBytes} bytes, exceeding the {providerName} MaxStateBytes limit of {maxStateBytes} bytes — " +
               "flow state exceeded the provider's size limit. Keep large payloads in your own storage and pass references in flow state; " +
               "see docs/durable-flows.md (ledger-size note).")
    {
        FlowId = flowId;
        SerializedSizeBytes = serializedSizeBytes;
        MaxStateBytes = maxStateBytes;
    }

    /// <summary>The flow whose ledger was rejected.</summary>
    public string FlowId { get; }
    /// <summary>The serialized size measured by the provider.</summary>
    public long SerializedSizeBytes { get; }
    /// <summary>The configured size budget.</summary>
    public long MaxStateBytes { get; }
}
