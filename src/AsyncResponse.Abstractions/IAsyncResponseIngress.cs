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
    /// Transient handling failures are retried in-process, then escalated through
    /// <see cref="IAsyncResponsePublisher.SetException"/> so the registered failure callback
    /// runs, keeping broker subscription loops alive. It throws only when that escalation
    /// itself fails — the response would otherwise be acknowledged while existing nowhere —
    /// so the transport's redelivery/dead-letter policy gets to retry the delivery.
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
    /// container. Execution failures are logged and then propagated — the transport dispatcher
    /// owns the retry/dead-letter decision, so swallowing here would acknowledge failed jobs
    /// as successes and disable redelivery.
    /// </summary>
    /// <param name="messageJson">The raw message body.</param>
    Task HandleWorkerMessageAsync(string messageJson);

    /// <summary>
    /// Whether <paramref name="messageJson"/> exceeds
    /// <c>AsyncResponseOptions.MaxInboundMessageChars</c>. Adapters call this before doing any
    /// work on the body that the budget is meant to bound — above all, extracting a correlation id
    /// from the body, which parses the whole payload into a <c>JsonDocument</c>.
    /// <para>
    /// Without this the budget arrived too late to mean what it says. A headerless response carries
    /// its id in the body, so the adapter parses first and the ingress checks the size second: the
    /// allocation the budget exists to prevent had already happened. An over-budget message is
    /// still handed to <see cref="HandleResponseMessageAsync"/> (with a <c>null</c> id) so the
    /// rejection, its log line and its metric all stay in one place.
    /// </para>
    /// <para>
    /// Phrased as "is it over?" rather than "is it within?" so that <c>false</c> — the value any
    /// implementation unaware of this method yields, the default below included — means "carry on".
    /// </para>
    /// <para>
    /// Defaulted rather than required: this interface is the documented seam for hand-written
    /// adapters (an HTTP webhook, a broker the library does not ship), and making it abstract would
    /// break every external implementation on upgrade for a guard that is an optimization at the
    /// call site — the ingress enforces the budget either way.
    /// </para>
    /// </summary>
    bool IsOverInboundBudget(string messageJson) => false;
}
