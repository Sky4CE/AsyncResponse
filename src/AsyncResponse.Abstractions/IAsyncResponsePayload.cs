namespace AsyncResponse;

/// <summary>
/// Marker for payload models awaited through <see cref="IAsyncResponseBuilder.For{T}(string)"/> and
/// <see cref="IAsyncResponseBuilder.For{T}()"/>. Implement this on your DTO/class/record to opt in.
/// Using an interface intentionally excludes primitive and scalar BCL types such as
/// <see cref="string"/>, <see cref="int"/>, <see cref="bool"/>, <see cref="Guid"/>, and
/// <see cref="DateTime"/>.
/// </summary>
public interface IAsyncResponsePayload
{
    /// <summary>
    /// Decides — for the <em>lost-subscriber</em> recovery path only — what a late response of this
    /// type should do to the flow: <see cref="RecoveryAction.Resume"/> it,
    /// <see cref="RecoveryAction.Fail"/> it, or <see cref="RecoveryAction.KeepWaiting"/> because
    /// this message is a non-terminal progress checkpoint and the flow's real outcome is still in
    /// flight.
    /// <para>
    /// This is consulted exclusively when a response arrives with no live waiter (typically after a
    /// redeploy): the recovering process has the persisted recovery state and the payload, but not
    /// the original waiter — so the decision must be reconstructible from the payload alone.
    /// <see cref="RecoveryAction.Resume"/> routes to the resume callback
    /// (<c>OnLostSubscriberResume</c>) and <see cref="RecoveryAction.Fail"/> to the failure
    /// callback (<c>OnLostSubscriberFailure</c>, wrapped in an
    /// <see cref="AsyncResponseDomainFailureException"/>); both consume the recovery registration.
    /// <see cref="RecoveryAction.KeepWaiting"/> invokes nothing and <em>retains</em> the
    /// registration, so the terminal response that follows still routes — the recovery-side mirror
    /// of a live waiter's <c>Until</c> predicate observing and skipping a progress message.
    /// </para>
    /// <para>
    /// It has <strong>nothing to do with live completion</strong>: an active waiter decides when to
    /// stop with its <c>Until(...)</c> predicate and this method is never called on that path. The
    /// two answer different questions — <c>Until</c> asks "is the operation done?", this asks
    /// "what should this late result do to the flow?".
    /// </para>
    /// <para>
    /// The recovering process materializes the payload from the persisted registration's payload
    /// type before asking, and the chosen callback receives that same materialized instance — so
    /// <c>object</c>-, interface-, or base-typed callback parameters get the concrete payload,
    /// never raw broker JSON.
    /// </para>
    /// <para>
    /// The default returns <see cref="RecoveryAction.Fail"/> so that a payload never resumes a flow
    /// by omission — a failed response can never accidentally take the happy path. Override it only
    /// for payloads used with <see cref="IRecoverableAsyncResponseBuilder"/>. Durable channels
    /// (e.g. Redis) require the override when recovery callbacks are registered, and fail fast at
    /// waiter creation if it is missing.
    /// </para>
    /// </summary>
    RecoveryAction OnRecovery() => RecoveryAction.Fail;
}
