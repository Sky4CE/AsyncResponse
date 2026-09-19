namespace AsyncResponse;

/// <summary>
/// Raised by a publish whose response (or exception) found no live subscriber and whose
/// lost-subscriber recovery callbacks could not all be invoked <em>transiently</em>. The message
/// was <b>not</b> acknowledged — every registration whose callback did not succeed stays armed,
/// and the broker ingress propagates this exception untouched (no <c>SetException</c> escalation,
/// which would only re-invoke the same failing callback, or fail flows whose resume merely
/// blipped) so the transport redelivers the message under its own bounded policy
/// (<c>MaxDeliveryAttempts</c>, then the dead-letter destination). The terminal signal therefore
/// survives in the broker until the callback's dependency recovers or an operator replays it from
/// the dead-letter queue, instead of being acknowledged into a log line while the flow stays stuck.
/// <para>Two paths raise it:</para>
/// <list type="bullet">
/// <item><description>the <b>failure</b> callback failed on every attempt of its in-process retry
/// ladder (<see cref="Attempts"/> is that ladder's length);</description></item>
/// <item><description>any resume or exception callback failed transiently, including a single
/// registration or a fan-out in which none succeeded (<see cref="Attempts"/> is 1 — transport
/// redelivery owns the retry). Successful siblings are deleted; failed registrations stay armed.</description></item>
/// </list>
/// <para>
/// Deterministic callback faults — an unauthorized or unresolvable target, a method that no longer
/// binds — are never wrapped in this type. In a partially successful fan-out they are logged
/// and retained for watchdog visibility; when every callback fails deterministically, the
/// original failure propagates to the ingress's exception-routing policy.
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
            $"A lost-subscriber recovery callback for correlationId '{correlationId}' failed transiently ({attempts} attempt(s)); " +
            "the message was not acknowledged so the transport can redeliver it to the registration(s) still armed.",
            innerException)
    {
        CorrelationId = correlationId;
        Attempts = attempts;
    }

    /// <summary>The correlation id whose recovery callback could not be invoked.</summary>
    public string CorrelationId { get; }

    /// <summary>How many in-process invocations were attempted before handing the delivery back to the transport (1 when transport redelivery owns the retry).</summary>
    public int Attempts { get; }
}
