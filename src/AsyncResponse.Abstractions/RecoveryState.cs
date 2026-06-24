namespace AsyncResponse;

/// <summary>
/// Per-correlation recovery state, stored by the response channel when a waiter registers.
/// With a durable store (for example Redis) it outlives the in-memory waiter, so a response that
/// arrives after the waiter died (e.g. a redeploy dropped the process) can still be routed: the
/// lost-subscriber dispatcher asks the payload's
/// <see cref="IAsyncResponsePayload.ShouldResumeOnRecovery"/> and invokes
/// <see cref="ResumeCallback"/> or <see cref="FailureCallback"/>.
/// <para>
/// <b>Contract warning:</b> instances are serialized into the backing store (e.g. Redis) and
/// must remain readable across deployments. Treat property names as a wire contract — additive
/// changes only. The <see cref="SchemaVersion"/> stamp lets the loader reject (rather than silently
/// misinterpret) entries written by an incompatible schema — see
/// <see cref="RecoveryStateSchema"/>.
/// </para>
/// <para>
/// <b>One recovery registration per correlation id (current limitation).</b> The store keeps a
/// single recovery registration per correlation id. If multiple <em>recoverable</em> waiters share
/// one correlation id and all of them are lost (e.g. a redeploy drops every process), only one
/// flow's callback is invoked when a late response arrives; the others are surfaced by the watchdog
/// as stale rather than recovered. This is specific to the lost-subscriber recovery path — live
/// shared-correlation fan-out (pub/sub delivery to several live waiters) is unaffected. Give each
/// recoverable flow its own correlation id to guarantee independent recovery. Making recovery
/// fan-out coherent end-to-end (a registration list per correlation id) is tracked as future work.
/// </para>
/// </summary>
public sealed class RecoveryState
{
    /// <summary>
    /// The wire schema version this entry was written with. New entries are always stamped with
    /// <see cref="RecoveryStateSchema.Current"/>. Entries written before this field existed carry no
    /// version on the wire and are read as the current version (and therefore accepted); an entry
    /// whose version is greater than the reader's current is rejected so a newer writer cannot
    /// silently misroute an older deployment's recovery path.
    /// </summary>
    public int SchemaVersion { get; set; } = RecoveryStateSchema.Current;
    /// <summary>
    /// Invoked when a response payload whose
    /// <see cref="IAsyncResponsePayload.ShouldResumeOnRecovery"/> returns <c>true</c> arrives with
    /// no live subscriber. Typically resumes or re-registers the owning flow.
    /// </summary>
    public ReflectionCallDto? ResumeCallback { get; set; }

    /// <summary>
    /// Invoked when an exception envelope — or a payload whose
    /// <see cref="IAsyncResponsePayload.ShouldResumeOnRecovery"/> returns <c>false</c> (or that
    /// cannot be classified) — arrives with no live subscriber. Typically marks the owning flow as
    /// failed (retriable).
    /// </summary>
    public ReflectionCallDto? FailureCallback { get; set; }

    /// <summary>The correlation id this state belongs to; passed back into callbacks.</summary>
    public string? CorrelationId { get; set; }

    /// <summary>
    /// Full name of the payload type the waiter subscribed for. The lost-subscriber fallback
    /// uses it to materialize untyped payloads (responses arriving through a broker ingress are
    /// raw JSON) so the payload can be asked whether to resume before a callback is chosen.
    /// </summary>
    public string? PayloadTypeFullName { get; set; }

    /// <summary>
    /// UTC timestamp of the waiter registration. Used by the watchdog to detect stale recovery
    /// state (old entries with no live subscriber and no response in sight).
    /// </summary>
    public DateTime? RegisteredAtUtc { get; set; }

    /// <summary>
    /// Serialized application ambient context captured at waiter registration (see
    /// <see cref="IAsyncResponseContextPropagator"/>), restored before a lost-subscriber recovery
    /// callback runs — which may be in a different deployment. <c>null</c> when no context
    /// propagators are registered.
    /// </summary>
    public Dictionary<string, string>? Context { get; set; }
}

/// <summary>
/// Wire-schema version stamp for <see cref="RecoveryState"/>. New entries are stamped with
/// <see cref="Current"/>. The loader rejects (returns <c>null</c> rather than handing on a
/// half-interpreted entry) any persisted entry whose version is greater than <see cref="Current"/>:
/// a newer writer must never silently misroute an older deployment's recovery path. Entries whose
/// version is missing or lower are read forward-compatibly — additive schema changes only.
/// <para>
/// Bump <see cref="Current"/> on breaking changes; the only valid new-version policy is "reject".
/// </para>
/// </summary>
public static class RecoveryStateSchema
{
    /// <summary>The current wire schema version written by this build.</summary>
    public const int Current = 1;

    /// <summary>
    /// Returns <c>true</c> when an entry with <paramref name="entryVersion"/> is safe to read on
    /// this build: the current version or an older one (an entry written before the version field
    /// existed reads as the current version). Returns <c>false</c> for a newer writer so the loader
    /// can reject it instead of misinterpreting it.
    /// </summary>
    public static bool IsReadable(int entryVersion)
        => entryVersion <= Current;
}
