namespace AsyncResponse;

/// <summary>
/// Broadcasts asynchronous responses (payloads or failures) on the response channel identified
/// by a correlation id. Active waiters receive them; when nobody is listening, the
/// lost-subscriber fallback routes them through the <see cref="RecoveryState"/> callbacks stored
/// in the configured <see cref="IRecoveryStateStore"/>.
/// </summary>
public interface IAsyncResponsePublisher
{
    /// <summary>
    /// Publishes a response payload on the channel associated with the specified correlation id.
    /// Any active subscriber awaiting this channel receives the payload — including payloads
    /// that describe a failed business state, which the waiter's <c>Until</c> predicate and flow
    /// code interpret. With no subscribers, the payload's
    /// <see cref="IAsyncResponsePayload.ClassifyOutcome"/> decides between the persisted resume
    /// and failure callbacks.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="response">The payload to publish.</param>
    /// <param name="correlationId">
    /// Optional correlation id; when <c>null</c>, the ambient
    /// <see cref="AsyncResponseContext.CorrelationId"/> is used.
    /// </param>
    Task SetResponse<T>(T response, string? correlationId = null);

    /// <summary>
    /// Publishes a technical failure on the channel associated with the specified correlation id.
    /// Active subscribers fault with the exception; with no subscribers, the persisted failure
    /// callback is invoked.
    /// </summary>
    /// <param name="exception">The exception to publish as the error response.</param>
    /// <param name="correlationId">
    /// Optional correlation id; when <c>null</c>, the ambient
    /// <see cref="AsyncResponseContext.CorrelationId"/> is used.
    /// </param>
    Task SetException(Exception exception, string? correlationId = null);
}
