namespace AsyncResponse;

/// <summary>
/// A worker job offloaded for background execution: a serializable method-call description plus
/// the correlation context to restore before executing it.
/// </summary>
public sealed class WorkerJobEnvelope
{
    /// <summary>The service method to execute.</summary>
    public required ReflectionCallDto Call { get; set; }

    /// <summary>
    /// The correlation id captured when the job was enqueued; restored into
    /// <see cref="AsyncResponseContext"/> before execution so downstream publishes correlate.
    /// </summary>
    public string? CorrelationId { get; set; }

    /// <summary>
    /// The reply target captured when the job was enqueued; restored into
    /// <see cref="AsyncResponseContext"/> before execution so downstream remote requests can
    /// publish responses to the same generic ingress.
    /// </summary>
    public AsyncResponseReplyTarget? ReplyTarget { get; set; }

    /// <summary>
    /// Serialized application ambient context captured when the job was enqueued (see
    /// <see cref="IAsyncResponseContextPropagator"/>), restored before the job executes when it is
    /// delivered through a broker ingress. <c>null</c> when no context propagators are registered.
    /// </summary>
    public Dictionary<string, string>? Context { get; set; }
}

/// <summary>
/// Publishes worker jobs for background execution. Implement this against your message broker
/// of choice for distributed execution (any consumer then feeds the message into
/// <see cref="IAsyncResponseIngress.HandleWorkerMessageAsync"/>), or register the built-in
/// in-memory queue (<c>AddAsyncResponse().WithInMemoryTransport()</c>) for development and
/// single-node deployments. Application hosts should select a full transport package rather than
/// raw-registering this interface directly.
/// </summary>
public interface IWorkerTransport
{
    /// <summary>Publishes a worker job for asynchronous execution.</summary>
    Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default);
}
