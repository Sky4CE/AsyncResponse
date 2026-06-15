namespace AsyncResponse;

/// <summary>
/// Interface for payload models that may be used with
/// <see cref="IAsyncResponseBuilder.For{T}(string)"/>.
/// Implement this on your DTO/class/record to opt in. Using an interface intentionally
/// excludes primitive and scalar BCL types such as <see cref="string"/>, <see cref="int"/>,
/// <see cref="bool"/>, <see cref="Guid"/>, and <see cref="DateTime"/>.
/// </summary>
public interface IAsyncResponsePayload
{
    /// <summary>
    /// Classifies the domain outcome carried by this payload.
    /// <para>
    /// A payload delivered through <see cref="IAsyncResponsePublisher.SetResponse{T}"/> is only a
    /// <em>transport-level</em> success: the payload itself may describe a failed business state.
    /// The lost-subscriber fallback (used when the original waiter disappeared, e.g. after a
    /// redeploy) calls this method to decide which registered callback to invoke:
    /// </para>
    /// <list type="bullet">
    /// <item><description><see cref="AsyncResponseOutcome.Succeeded"/> and
    /// <see cref="AsyncResponseOutcome.InProgress"/> route to the resume callback
    /// (<c>OnLostSubscriberResume</c>).</description></item>
    /// <item><description><see cref="AsyncResponseOutcome.Failed"/> and
    /// <see cref="AsyncResponseOutcome.Unknown"/> route to the failure callback
    /// (<c>OnLostSubscriberFailure</c>) wrapped in an
    /// <see cref="AsyncResponseDomainFailureException"/>.</description></item>
    /// </list>
    /// <para>
    /// The implementation must mirror the failure semantics your active waiter applies in its
    /// <c>Until(...)</c> predicate. Active-subscriber behavior is not affected by this method.
    /// Payload types that are only ever published on a success path (failures going through
    /// <c>SetException</c>) should return <see cref="AsyncResponseOutcome.Succeeded"/>.
    /// This member is deliberately required: every payload author must make the classification
    /// decision explicitly, so a "failed payload resumed the happy path" bug cannot be
    /// reintroduced by omission.
    /// </para>
    /// </summary>
    AsyncResponseOutcome ClassifyOutcome();
}
