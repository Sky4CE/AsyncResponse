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
}

/// <summary>
/// Publishes worker jobs for background execution. Implement this against your message broker
/// of choice for distributed execution (any consumer then feeds the message into
/// <see cref="IAsyncResponseIngress.HandleWorkerMessageAsync"/>), or register the built-in
/// in-process queue (<c>AddInProcessWorkerQueue()</c>) for development and single-node
/// deployments.
/// </summary>
public interface IWorkerTransport
{
    /// <summary>Publishes a worker job for asynchronous execution.</summary>
    Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default);
}
