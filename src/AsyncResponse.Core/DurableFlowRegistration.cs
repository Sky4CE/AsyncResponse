namespace AsyncResponse;

/// <summary>
/// A statically-typed durable-flow registration produced by
/// <c>WithDurableFlow&lt;TFlow, TInput&gt;()</c>. The executor prefers it over the reflection path
/// when re-hydrating a run from its persisted <see cref="FlowState.FlowTypeName"/>: the flow
/// resolves, its input deserializes through typed JSON metadata, and <c>ExecuteAsync</c> is a
/// direct delegate call — no type-name scans, no <c>MakeGenericType</c>, no
/// <c>MethodInfo.Invoke</c> — which is what lets registered flows run under trimming/Native AOT.
/// Unregistered flows keep the historical reflection-based resolution.
/// </summary>
internal sealed class DurableFlowRegistration
{
    /// <summary>The flow class full name as persisted in <see cref="FlowState.FlowTypeName"/>.</summary>
    public required string FlowTypeFullName { get; init; }

    /// <summary>
    /// The TInput full name, formatted exactly as <see cref="FlowState.InputTypeName"/> is stamped
    /// at start (<c>typeof(TInput).FullName</c>), so the executor can reject a persisted run whose
    /// input type no longer matches this registration instead of silently deserializing it.
    /// </summary>
    public required string InputTypeFullName { get; init; }

    /// <summary>The flow class, resolved from DI per execution.</summary>
    public required Type FlowType { get; init; }

    /// <summary>Deserializes the persisted input JSON as the flow's TInput.</summary>
    public required Func<string, object?> DeserializeInput { get; init; }

    /// <summary>Invokes <c>((TFlow)flow).ExecuteAsync(context, (TInput)input)</c> statically.</summary>
    public required Func<object, IDurableFlowContext, object?, Task> ExecuteAsync { get; init; }

    /// <summary>
    /// Whether two serialized inputs of this flow hold the same VALUE, whatever shape a serializer
    /// once gave them (see <see cref="FlowStateJson.InputEquivalent{TInput}"/>): both sides are read
    /// through <see cref="DeserializeInput"/> and written back the same way, so a member added to
    /// the input type since one of them was written does not make them differ. A side this build
    /// cannot read is a mismatch, never an exception.
    /// </summary>
    public bool InputEquivalent(string? persisted, string requested)
    {
        if (FlowStateJson.JsonEquivalent(persisted, requested))
            return true;
        if (persisted is null)
            return false;

        try
        {
            return FlowStateJson.JsonEquivalent(Normalize(persisted), Normalize(requested));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    // Written back by the runtime type — the one this registration's typed read produced — on
    // BOTH sides, so the normalization is symmetric even where it differs from a declared-type write.
    private string Normalize(string json)
        => DeserializeInput(json) is { } value ? AsyncResponseJson.Serialize(value, value.GetType()) : "null";
}
