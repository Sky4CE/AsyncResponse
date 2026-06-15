namespace AsyncResponse;

/// <summary>
/// Domain-level outcome of an async-response payload, as reported by
/// <see cref="IAsyncResponsePayload.ClassifyOutcome"/>.
/// <para>
/// The transport distinguishes only between a payload envelope (<c>SetResponse</c>) and an
/// exception envelope (<c>SetException</c>). A payload envelope, however, may still describe a
/// failed business state (e.g. <c>Status = Error</c>, <c>Success = false</c>). This enum
/// expresses that domain state so the lost-subscriber fallback can decide between the resume
/// callback and the failure callback after a redeploy/restart has dropped the original waiter.
/// </para>
/// </summary>
public enum AsyncResponseOutcome
{
    /// <summary>
    /// The payload carries a state the classifier does not recognize (e.g. an unexpected enum
    /// value or a missing status field). The lost-subscriber fallback treats this conservatively
    /// as a failure rather than resuming the flow.
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// A non-terminal, intermediate state. The lost-subscriber fallback routes this to the
    /// resume callback so the flow can re-register a waiter and continue waiting.
    /// </summary>
    InProgress = 1,

    /// <summary>
    /// The operation completed successfully. The lost-subscriber fallback routes this to the
    /// resume callback.
    /// </summary>
    Succeeded = 2,

    /// <summary>
    /// The operation terminally failed. The lost-subscriber fallback routes this to the failure
    /// callback (wrapped in an <see cref="AsyncResponseDomainFailureException"/>) instead of
    /// resuming the flow.
    /// </summary>
    Failed = 3
}
