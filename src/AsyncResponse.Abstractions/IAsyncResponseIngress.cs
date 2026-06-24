namespace AsyncResponse;

/// <summary>
/// Transport-neutral ingress: wire your message broker subscriptions (Google Pub/Sub, RabbitMQ,
/// Kafka, an HTTP webhook, …) to these methods to feed inbound messages into AsyncResponse.
/// <para>
/// The ingress deliberately makes only a <em>transport-level</em> decision: a message that
/// parses as JSON is a response payload and is delivered through the channel's raw ingress path
/// untyped and uninterpreted — a payload whose domain state is failed is still a valid response
/// that active waiters consume through their <c>Until</c> predicates. A message that does not
/// parse carries no payload and is reported through <see cref="IAsyncResponsePublisher.SetException"/>.
/// Domain-state classification happens only in the lost-subscriber fallback, because "nobody is
/// listening" is only knowable after publishing.
/// </para>
/// </summary>
public interface IAsyncResponseIngress
{
    /// <summary>
    /// Handles an inbound response message (raw JSON) for the given correlation id.
    /// Never throws: handling failures are escalated through
    /// <see cref="IAsyncResponsePublisher.SetException"/> so the registered failure callback
    /// runs, keeping broker subscription loops alive.
    /// </summary>
    /// <param name="messageJson">The raw message body.</param>
    /// <param name="correlationId">
    /// The correlation id the calling adapter extracted from the message (attribute/header/body).
    /// It is nullable because extraction from an untrusted broker message can legitimately yield
    /// nothing — a <c>null</c> or blank id means the message is unroutable, so it is logged and
    /// skipped. There is no ambient fallback: pass the extracted id explicitly.
    /// </param>
    Task HandleResponseMessageAsync(string messageJson, string? correlationId);

    /// <summary>
    /// Handles an inbound worker-job message (a serialized <see cref="WorkerJobEnvelope"/>):
    /// restores the correlation context and executes the described service method via the DI
    /// container. Never throws: execution failures are logged so broker subscription loops
    /// stay alive.
    /// </summary>
    /// <param name="messageJson">The raw message body.</param>
    Task HandleWorkerMessageAsync(string messageJson);
}
