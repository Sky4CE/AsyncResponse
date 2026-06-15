namespace AsyncResponse;

/// <summary>
/// Per-correlation recovery state, stored by the response channel when a waiter registers.
/// With a durable store (for example Redis) it outlives the in-memory waiter, so a response that
/// arrives after the waiter died (e.g. a redeploy dropped the process) can still be routed: the
/// lost-subscriber dispatcher classifies the payload's domain outcome and invokes
/// <see cref="ResumeCallback"/> or <see cref="FailureCallback"/>.
/// <para>
/// <b>Contract warning:</b> instances are serialized into the backing store (e.g. Redis) and
/// must remain readable across deployments. Treat property names as a wire contract — additive
/// changes only.
/// </para>
/// </summary>
public sealed class RecoveryState
{
    /// <summary>
    /// Invoked when a response whose domain outcome is <see cref="AsyncResponseOutcome.Succeeded"/>
    /// or <see cref="AsyncResponseOutcome.InProgress"/> (or that cannot be classified) arrives
    /// with no live subscriber. Typically resumes or re-registers the owning flow.
    /// </summary>
    public ReflectionCallDto? ResumeCallback { get; set; }

    /// <summary>
    /// Invoked when an exception envelope — or a payload whose domain outcome is
    /// <see cref="AsyncResponseOutcome.Failed"/> or <see cref="AsyncResponseOutcome.Unknown"/> —
    /// arrives with no live subscriber. Typically marks the owning flow as failed (retriable).
    /// </summary>
    public ReflectionCallDto? FailureCallback { get; set; }

    /// <summary>The correlation id this state belongs to; passed back into callbacks.</summary>
    public string? CorrelationId { get; set; }

    /// <summary>
    /// Full name of the payload type the waiter subscribed for. The lost-subscriber fallback
    /// uses it to materialize untyped payloads (responses arriving through a broker ingress are
    /// raw JSON) so their domain outcome can be classified before a callback is chosen.
    /// </summary>
    public string? PayloadTypeFullName { get; set; }

    /// <summary>
    /// UTC timestamp of the waiter registration. Used by the watchdog to detect stale recovery
    /// state (old entries with no live subscriber and no response in sight).
    /// </summary>
    public DateTime? RegisteredAtUtc { get; set; }
}
