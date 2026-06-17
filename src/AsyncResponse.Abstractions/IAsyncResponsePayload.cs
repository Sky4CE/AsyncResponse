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
    /// Decides — for the <em>lost-subscriber</em> recovery path only — whether a late response of
    /// this type should <em>resume</em> the flow or <em>fail</em> it.
    /// <para>
    /// This is consulted exclusively when a response arrives with no live waiter (typically after a
    /// redeploy): the recovering process has the persisted recovery state and the payload, but not
    /// the original waiter — so the resume-vs-fail decision must be reconstructible from the payload
    /// type itself. <c>true</c> routes the payload to the resume callback
    /// (<c>OnLostSubscriberResume</c>); <c>false</c> routes it to the failure callback
    /// (<c>OnLostSubscriberFailure</c>), wrapped in an
    /// <see cref="AsyncResponseDomainFailureException"/>.
    /// </para>
    /// <para>
    /// It has <strong>nothing to do with live completion</strong>: an active waiter decides when to
    /// stop with its <c>Until(...)</c> predicate and this method is never called on that path. The
    /// two answer different questions — <c>Until</c> asks "is the operation done?", this asks "is
    /// this result a failure?".
    /// </para>
    /// <para>
    /// The default returns <c>false</c> (do not resume) so that a payload never resumes a flow by
    /// omission — a failed response can never accidentally take the happy path. Override it only for
    /// payloads that can carry a domain failure, returning <c>true</c> for the states the flow
    /// should resume on. Durable channels (e.g. Redis) require this override when recovery callbacks
    /// are registered, and fail fast at waiter creation if it is missing.
    /// </para>
    /// </summary>
    bool ShouldResumeOnRecovery() => false;
}
