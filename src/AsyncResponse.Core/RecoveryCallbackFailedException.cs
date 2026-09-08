namespace AsyncResponse;

/// <summary>
/// Raised by a publish whose response (or exception) found no live subscriber and whose
/// lost-subscriber <b>failure</b> callback could not be invoked: every attempt of the in-process
/// retry ladder faulted transiently. The message was <b>not</b> acknowledged — the recovery
/// registration stays armed, and the broker ingress propagates this exception untouched (no
/// <c>SetException</c> escalation, which would only re-invoke the same failing callback) so the
/// transport redelivers the message under its own bounded policy (<c>MaxDeliveryAttempts</c>,
/// then the dead-letter destination). The terminal signal therefore survives in the broker until
/// the callback's dependency recovers or an operator replays it from the dead-letter queue,
/// instead of being acknowledged into a log line while the flow stays stuck.
/// <para>
/// Deterministic callback faults — an unauthorized or unresolvable target, a method that no longer
/// binds — are never wrapped in this type: redelivery cannot fix them, so they are logged and the
/// message is acknowledged (the registration stays for the watchdog to surface).
/// </para>
/// <para>
/// A direct caller of <c>IAsyncResponsePublisher.SetResponse</c>/<c>SetException</c> (an HTTP
/// callback endpoint, for instance) sees this exception too; answering the remote system with a
/// retriable status is the equivalent of the broker's redelivery.
/// </para>
/// </summary>
public sealed class RecoveryCallbackFailedException : Exception
{
    /// <summary>Creates the exception for <paramref name="correlationId"/> after <paramref name="attempts"/> failed invocations.</summary>
    public RecoveryCallbackFailedException(string correlationId, int attempts, Exception innerException)
        : base(
            $"The lost-subscriber failure callback for correlationId '{correlationId}' failed on all {attempts} attempts; " +
            "the message was not acknowledged so the transport can redeliver it.",
            innerException)
    {
        CorrelationId = correlationId;
        Attempts = attempts;
    }

    /// <summary>The correlation id whose failure callback could not be invoked.</summary>
    public string CorrelationId { get; }

    /// <summary>How many invocations were attempted before giving up on this delivery.</summary>
    public int Attempts { get; }
}
