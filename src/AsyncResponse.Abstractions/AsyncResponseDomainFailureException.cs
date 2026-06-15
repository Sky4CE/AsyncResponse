namespace AsyncResponse;

/// <summary>
/// Raised by the lost-subscriber fallback when a response payload published through
/// <see cref="IAsyncResponsePublisher.SetResponse{T}"/> describes a failed (or unrecognizable)
/// domain state — see <see cref="IAsyncResponsePayload.ClassifyOutcome"/> — and no subscriber
/// was listening on the correlation channel (typically after a redeploy/restart).
/// <para>
/// Instances of this exception are passed to the callback registered via
/// <c>IAsyncResponseBuilder{T}.OnLostSubscriberFailure</c>, so a failed domain response takes
/// the same failure path as a technical <c>SetException</c>. Handlers can pattern-match on this
/// type to distinguish a domain failure (with its payload) from a technical error.
/// </para>
/// </summary>
public sealed class AsyncResponseDomainFailureException : Exception
{
    public AsyncResponseDomainFailureException(
        string? correlationId,
        AsyncResponseOutcome outcome,
        string? payloadTypeFullName,
        string? payloadJson)
        : base(BuildMessage(correlationId, outcome, payloadTypeFullName, payloadJson))
    {
        CorrelationId = correlationId;
        Outcome = outcome;
        PayloadTypeFullName = payloadTypeFullName;
        PayloadJson = payloadJson;
    }

    /// <summary>The correlation id of the channel the response was published on.</summary>
    public string? CorrelationId { get; }

    /// <summary>
    /// The classified domain outcome: <see cref="AsyncResponseOutcome.Failed"/> or
    /// <see cref="AsyncResponseOutcome.Unknown"/>.
    /// </summary>
    public AsyncResponseOutcome Outcome { get; }

    /// <summary>Full name of the payload type the original waiter was registered for.</summary>
    public string? PayloadTypeFullName { get; }

    /// <summary>JSON snapshot of the payload that carried the failed domain state.</summary>
    public string? PayloadJson { get; }

    private static string BuildMessage(
        string? correlationId,
        AsyncResponseOutcome outcome,
        string? payloadTypeFullName,
        string? payloadJson)
        => $"Async response for correlationId '{correlationId}' reported domain outcome '{outcome}' " +
           $"(payload type '{payloadTypeFullName}') while no subscriber was listening. " +
           $"Payload: {payloadJson}";
}
