namespace AsyncResponse;

/// <summary>
/// Thrown by <see cref="IRecoveryStateStore.GetAllAsync"/> when a correlation id has stored
/// recovery registrations but this build cannot interpret <em>any</em> of them: malformed JSON, an
/// incomplete identity, or a schema version outside
/// <see cref="RecoveryStateSchema.IsReadable"/>.
/// <para>
/// The same distinction <see cref="FlowStateUnreadableException"/> draws for durable-flow ledgers,
/// applied to the recovery path. An empty registration list means "nobody ever armed a recovery
/// callback for this response", and the lost-subscriber dispatcher answers that by acknowledging
/// the delivery — correctly, because there is nothing to run. Filtering unreadable rows down to an
/// empty list made a corrupt or newer-schema registration indistinguishable from that, so a
/// terminal response was acknowledged while the callback that should have received it never ran and
/// the response ceased to exist.
/// </para>
/// <para>
/// <b>Only the all-unreadable case throws.</b> When at least one registration is readable, those
/// are dispatched and the unreadable remainder is logged and counted instead: failing a mixed batch
/// would redeliver callbacks that already succeeded, and — since successful registrations are
/// deleted — the redelivery would find only the unreadable rows and fail again, turning a partially
/// corrupt record into a permanent redelivery loop.
/// </para>
/// </summary>
public sealed class RecoveryStateUnreadableException : InvalidOperationException
{
    /// <summary>Creates the exception for <paramref name="correlationId"/>.</summary>
    public RecoveryStateUnreadableException(string? correlationId, int unreadableCount)
        : base(
            $"All {unreadableCount} stored recovery registration(s) for correlation id '{correlationId}' are unreadable by " +
            "this build (malformed, incomplete identity, or an unsupported schema version). The response must not be " +
            "acknowledged as handled; the delivery is retried or dead-lettered so a build that can read them, or an " +
            "operator, resolves it.")
    {
        CorrelationId = correlationId;
        UnreadableCount = unreadableCount;
    }

    /// <summary>The correlation id whose registrations could not be read.</summary>
    public string? CorrelationId { get; }

    /// <summary>How many stored registrations were rejected, for metrics and operator triage.</summary>
    public int UnreadableCount { get; }
}
