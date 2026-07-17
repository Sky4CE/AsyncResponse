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

    /// <summary>The flow class, resolved from DI per execution.</summary>
    public required Type FlowType { get; init; }

    /// <summary>Deserializes the persisted input JSON as the flow's TInput.</summary>
    public required Func<string, object?> DeserializeInput { get; init; }

    /// <summary>Invokes <c>((TFlow)flow).ExecuteAsync(context, (TInput)input)</c> statically.</summary>
    public required Func<object, IDurableFlowContext, object?, Task> ExecuteAsync { get; init; }
}
