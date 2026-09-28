namespace AsyncResponse;

/// <summary>
/// Thrown by <see cref="IRecoveryStateStore.GetAllAsync"/> when a correlation id has stored,
/// unexpired recovery registrations that this build cannot interpret: malformed JSON, an incomplete
/// identity, or a schema version outside <see cref="RecoveryStateSchema.IsReadable"/>. Stores whose
/// registrations share one blob per correlation id (Redis, NATS) also throw it from
/// <see cref="IRecoveryStateStore.SaveAsync"/> when the stored envelope itself is unparseable: a
/// rewrite that read "unreadable" as "missing" would commit just the new registration over
/// registrations it could not enumerate, destroying every armed callback the blob held.
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
/// <b>One unreadable registration refuses the whole lookup</b>, readable siblings included, and
/// the lookup is refused before any callback runs. Until round 49 a lookup that also found readable
/// registrations returned those alone: the dispatcher invoked and consumed them and the transport
/// acknowledged the response, while the unreadable registration stayed armed with no payload left
/// to deliver — deploying a build that could read it, or repairing it, recovered nothing. Now no
/// registration of the correlation id is dispatched; the delivery is retried or dead-lettered, and
/// a build that can read every registration (the newer one a rolling upgrade is bringing up), or an
/// operator who repairs or removes the unreadable one, settles all of them from the retained
/// payload. Nothing is invoked twice: the readable siblings were never called. On the database
/// channels and NATS, a registration rejected because it carries another correlation id (a legacy
/// case-insensitive collation's match) is readable and belongs elsewhere; it counts as absent, not
/// unreadable. Redis keys are exact, so there such an entry is corrupt and counts as unreadable.
/// </para>
/// </summary>
public sealed class RecoveryStateUnreadableException : InvalidOperationException
{
    /// <summary>Creates the exception for <paramref name="correlationId"/>.</summary>
    public RecoveryStateUnreadableException(string? correlationId, int unreadableCount)
        : base(
            $"{unreadableCount} stored recovery registration(s) for correlation id '{correlationId}' are unreadable by " +
            "this build (malformed, incomplete identity, or an unsupported schema version). No registration of this " +
            "correlation id is dispatched — not even a readable one — and the response must not be acknowledged as " +
            "handled; the delivery is retried or dead-lettered so a build that can read them, or an operator, resolves it.")
    {
        CorrelationId = correlationId;
        UnreadableCount = unreadableCount;
    }

    /// <summary>The correlation id whose registrations could not be read.</summary>
    public string? CorrelationId { get; }

    /// <summary>How many stored registrations were rejected, for metrics and operator triage.</summary>
    public int UnreadableCount { get; }
}
