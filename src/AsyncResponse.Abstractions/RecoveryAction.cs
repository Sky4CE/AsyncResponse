namespace AsyncResponse;

/// <summary>
/// The route a response payload chooses when it arrives with <em>no live waiter</em> (the
/// lost-subscriber recovery path) — returned by
/// <see cref="IAsyncResponsePayload.OnRecovery"/>.
/// <para>
/// The live path is unaffected: an active waiter decides when to stop with its <c>Until(...)</c>
/// predicate, and this classification is never consulted for it.
/// </para>
/// </summary>
public enum RecoveryAction
{
    /// <summary>
    /// Route the payload to the failure callback (wrapped in an
    /// <see cref="AsyncResponseDomainFailureException"/>) and consume the recovery registration.
    /// The conservative default: a payload never resumes a flow by omission.
    /// </summary>
    Fail = 0,

    /// <summary>
    /// Route the payload to the resume callback and consume the recovery registration. Use for
    /// terminal results the flow should continue from.
    /// </summary>
    Resume = 1,

    /// <summary>
    /// The payload is a <em>non-terminal checkpoint</em> (progress/heartbeat): invoke no callback
    /// and <b>retain</b> the recovery registration, so a later response — typically the terminal
    /// one — still routes. Mirrors what a live waiter's <c>Until</c> predicate does with a
    /// non-matching response: observe and keep waiting. Without this lane a checkpoint arriving
    /// after the waiter died would consume the registration and either fail the flow or resume it
    /// prematurely — and the real terminal response would then find nothing to route against and
    /// be dropped (the flow-deadlock shape recovery exists to prevent).
    /// <para>
    /// The registration remains bounded by <c>RecoveryStateExpiry</c> and visible to the recovery
    /// watchdog while it waits.
    /// </para>
    /// </summary>
    KeepWaiting = 2,
}
