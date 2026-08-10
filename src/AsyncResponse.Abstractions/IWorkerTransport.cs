using System.Text.Json.Serialization;

namespace AsyncResponse;

/// <summary>
/// A worker job offloaded for background execution: a serializable method-call description plus
/// the correlation context to restore before executing it.
/// </summary>
public sealed class WorkerJobEnvelope
{
    /// <summary>
    /// The wire schema version this job was written with. New jobs are always stamped with
    /// <see cref="WorkerJobEnvelopeSchema.Current"/>. The property is required on the wire; a missing
    /// or unsupported version is rejected so an incompatible producer cannot silently invoke the
    /// wrong method shape.
    /// </summary>
    [JsonRequired]
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

    /// <summary>
    /// UTC instant before which this job must not execute; <c>null</c> for ordinary immediate jobs.
    /// Stamped by delayed publishes (<see cref="IDelayedWorkerTransport"/>). The worker-job
    /// executor enforces it: a job delivered early — broker imprecision, or a chunked hop on a
    /// transport whose per-publish delay is capped — is re-published for the remaining delay
    /// instead of executed, so the due time holds on every transport. Additive wire property:
    /// absent on jobs written before it existed.
    /// </summary>
    public DateTime? NotBeforeUtc { get; set; }

    /// <summary>
    /// The remaining delay observed the last time the worker-job executor re-published this job
    /// for a further hop of the <see cref="NotBeforeUtc"/> chunk chain; <c>null</c> until the
    /// first re-publish. Consecutive hops must shrink this value: when the due time was stamped
    /// by a different clock than the one gating delivery (client-computed schedule vs the broker
    /// clock), a skewed consumer would otherwise re-publish the same remainder forever — and each
    /// hop is a fresh message id, so delivery counters reset and the loop could never dead-letter.
    /// On a no-progress hop the executor runs the job (early by the skew) instead of re-publishing.
    /// Additive wire property: absent on jobs written before it existed.
    /// </summary>
    public TimeSpan? LastRedelayRemaining { get; set; }
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
/// not explicitly supported: an unrecognized producer must never silently invoke an incompatible
/// method shape. The JSON property is required.
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
        => entryVersion == Current;
}
