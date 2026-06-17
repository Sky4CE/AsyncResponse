namespace AsyncResponse;

/// <summary>
/// Raised by the lost-subscriber fallback when a response payload published through
/// <see cref="IAsyncResponsePublisher.SetResponse{T}"/> reports — via
/// <see cref="IAsyncResponsePayload.ShouldResumeOnRecovery"/> returning <c>false</c> — that it
/// should <em>not</em> resume the flow, and no subscriber was listening on the correlation channel
/// (typically after a redeploy/restart).
/// <para>
/// Instances of this exception are passed to the callback registered via
/// <c>IAsyncResponseBuilder{T}.OnLostSubscriberFailure</c>, so a failed domain response takes the
/// same failure path as a technical <c>SetException</c>. Handlers can pattern-match on this type to
/// distinguish a domain failure (with its payload) from a technical error.
/// </para>
/// </summary>
public sealed class AsyncResponseDomainFailureException : Exception
{
    public AsyncResponseDomainFailureException(
        string? correlationId,
        string? payloadTypeFullName,
        string? payloadJson)
        : base(BuildMessage(correlationId, payloadTypeFullName, payloadJson))
    {
        CorrelationId = correlationId;
        PayloadTypeFullName = payloadTypeFullName;
        PayloadJson = payloadJson;
    }

    /// <summary>The correlation id of the channel the response was published on.</summary>
    public string? CorrelationId { get; }

    /// <summary>Full name of the payload type the original waiter was registered for.</summary>
    public string? PayloadTypeFullName { get; }

    /// <summary>JSON snapshot of the payload that declined to resume the flow.</summary>
    public string? PayloadJson { get; }

    private static string BuildMessage(
        string? correlationId,
        string? payloadTypeFullName,
        string? payloadJson)
        => $"Async response for correlationId '{correlationId}' (payload type '{payloadTypeFullName}') " +
           $"declined to resume the flow while no subscriber was listening. Payload: {payloadJson}";
}
