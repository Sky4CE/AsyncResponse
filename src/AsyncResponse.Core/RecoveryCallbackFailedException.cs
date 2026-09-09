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
/// <item><description>a <b>fan-out</b> over several registrations sharing the correlation id
/// partially failed: at least one registration's callback succeeded and was consumed, another's
/// failed transiently (<see cref="Attempts"/> is 1 — the redelivery is the retry). Because the
/// successful registrations are deleted, the redelivery reaches only the one that failed, and a
/// caller that retries the publish once the dependency recovers completes exactly that
/// registration.</description></item>
/// </list>
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
            $"A lost-subscriber recovery callback for correlationId '{correlationId}' failed transiently ({attempts} attempt(s)); " +
            "the message was not acknowledged so the transport can redeliver it to the registration(s) still armed.",
            innerException)
    {
        CorrelationId = correlationId;
        Attempts = attempts;
    }

    /// <summary>The correlation id whose recovery callback could not be invoked.</summary>
    public string CorrelationId { get; }

    /// <summary>How many in-process invocations were attempted before handing the delivery back to the transport (1 for a partially failed fan-out).</summary>
    public int Attempts { get; }
}
