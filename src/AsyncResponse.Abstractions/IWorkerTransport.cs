namespace AsyncResponse;

/// <summary>
/// A worker job offloaded for background execution: a serializable method-call description plus
/// the correlation context to restore before executing it.
/// </summary>
public sealed class WorkerJobEnvelope
{
    /// <summary>
    /// The wire schema version this job was written with. New jobs are always stamped with
    /// <see cref="WorkerJobEnvelopeSchema.Current"/>. Jobs written before this field existed carry no
    /// version on the wire and are read as the current version (and therefore executed); a job whose
    /// version is greater than the reader's current is rejected so a newer producer cannot silently
    /// invoke an incompatible method shape on an older worker.
    /// </summary>
    public int SchemaVersion { get; set; } = WorkerJobEnvelopeSchema.Current;

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

/// <summary>
/// Wire-schema version stamp for <see cref="WorkerJobEnvelope"/>. New jobs are stamped with
/// <see cref="Current"/>. The ingress loader rejects (dead-letters) any job whose version is
/// greater than <see cref="Current"/>: a newer producer must never silently invoke an incompatible
/// method shape on an older worker. Jobs whose version is missing or lower are read
/// forward-compatibly — additive schema changes only.
/// </summary>
public static class WorkerJobEnvelopeSchema
{
    /// <summary>The current wire schema version written by this build.</summary>
    public const int Current = 1;

    /// <summary>
    /// Returns <c>true</c> when a job with <paramref name="entryVersion"/> is safe to execute on
    /// this build. See <see cref="RecoveryStateSchema.IsReadable"/> for the policy.
    /// </summary>
    public static bool IsReadable(int entryVersion)
        => entryVersion <= Current;
}
