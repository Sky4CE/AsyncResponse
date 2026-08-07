namespace AsyncResponse;

/// <summary>
/// Broadcasts asynchronous responses (payloads or failures) on the response channel identified
/// by a correlation id. Active waiters receive them (subject on the database channels to the
/// same-tick registration/claim tie documented in <c>docs/recovery.md</c>, which is resolved as
/// at-most-once); when nobody is listening, the lost-subscriber fallback routes them through the
/// <see cref="RecoveryState"/> callbacks stored in the configured
/// <see cref="IRecoveryStateStore"/>.
/// </summary>
public interface IAsyncResponsePublisher
{
    /// <summary>
    /// Publishes a response payload on the channel associated with the specified correlation id.
    /// Active subscribers awaiting this channel receive the payload — including payloads
    /// that describe a failed business state, which the waiter's <c>Until</c> predicate and flow
    /// code interpret. One deliberate exception on the database channels: a subscriber whose
    /// registration lands in the same server-clock tick as another process's delivery claim is
    /// excluded from that delivery and times out instead (at-most-once for the tie — see the
    /// shared-correlation section of <c>docs/recovery.md</c>). With no subscribers, the payload's
    /// <see cref="IAsyncResponsePayload.OnRecovery"/> decides between the persisted resume
    /// and failure callbacks — or retains the registration for a non-terminal checkpoint
    /// (<see cref="RecoveryAction.KeepWaiting"/>).
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="response">The payload to publish.</param>
    /// <param name="correlationId">
    /// The correlation id identifying the response channel to publish on. Required — there is no
    /// ambient fallback. Inside a wait trigger use <c>context.CorrelationId</c>; elsewhere pass
    /// <see cref="AsyncResponseContext.CorrelationId"/> explicitly if you want the ambient value.
    /// </param>
    /// <param name="cancellationToken">
    /// Propagates cancellation of the publish. The waiter side is unaffected — only the publisher's
    /// own I/O (broker send, recovery-state lookup) observes the token.
    /// </param>
    Task SetResponse<T>(T response, string correlationId, CancellationToken cancellationToken = default) where T : IAsyncResponsePayload;

    /// <summary>
    /// Publishes a <em>technical</em> failure on the channel associated with the specified
    /// correlation id. Active subscribers' waits fault with <paramref name="exception"/> — the
    /// database channels' exceptions travel through the same delivery watermark as responses, so
    /// the same-tick registration/claim tie (see <c>docs/recovery.md</c>) applies here too; with
    /// no subscribers, the persisted failure callback is invoked.
    /// <para>
    /// <b>Reserve this for genuinely exceptional, unexpected conditions</b> (a dependency is down, a
    /// message could not be deserialized, an invariant was violated) — not for expected business
    /// outcomes. For an <em>expected</em> negative result — a validation failure, a declined or
    /// rejected request, a "not found", a partial success — publish a payload via
    /// <see cref="SetResponse{T}"/> that <em>encodes</em> the outcome (a result-pattern payload such
    /// as <c>Result&lt;T, Error&gt;</c> or a domain-specific discriminated payload) and let the
    /// waiter's <c>Until</c> predicate and flow code branch on it. See the remarks for why this
    /// matters.
    /// </para>
    /// </summary>
    /// <param name="exception">The exception to publish as the error response.</param>
    /// <param name="correlationId">
    /// The correlation id identifying the response channel to publish on. Required — there is no
    /// ambient fallback. Inside a wait trigger use <c>context.CorrelationId</c>; elsewhere pass
    /// <see cref="AsyncResponseContext.CorrelationId"/> explicitly if you want the ambient value.
    /// </param>
    /// <param name="cancellationToken">
    /// Propagates cancellation of the publish. The waiter side is unaffected — only the publisher's
    /// own I/O (broker send, recovery-state lookup) observes the token.
    /// </param>
    /// <remarks>
    /// <para>
    /// <b>Cost.</b> Faulting a wait is expensive, and the cost scales with the number of waiters on
    /// the correlation id. When this exception fans out to <c>N</c> active subscribers, each of the
    /// <c>N</c> awaiting flows resumes by <em>re-throwing</em> the exception, and the .NET runtime
    /// pays the full price of a real <see langword="throw"/>/<see langword="catch"/> every time:
    /// restoring the captured stack trace (via <see cref="System.Runtime.ExceptionServices.ExceptionDispatchInfo"/>),
    /// walking the stack, running first-chance/handler search, and unwinding. That work happens
    /// independently per awaiter and cannot be batched, shared, or amortized — it is inherent to how
    /// the CLR propagates exceptions through faulted <see cref="System.Threading.Tasks.Task"/>s, so
    /// the channel cannot optimize it away. A shared-correlation exception fan-out can therefore cost
    /// one to two orders of magnitude more than the equivalent successful payload fan-out, and it
    /// allocates per awaiter (an exception-dispatch holder for each faulted task). High-frequency or
    /// wide-fan-out failure paths driven by <c>SetException</c> will dominate your latency and GC
    /// profile.
    /// </para>
    /// <para>
    /// <b>Recommended pattern.</b> Model expected failures as <em>data</em>, not exceptions. Define a
    /// payload that carries the outcome — for example a result type that is either a success value or
    /// an error, or a status enum plus an error detail — publish it with <see cref="SetResponse{T}"/>,
    /// and have the waiter inspect it. This keeps the fan-out on the cheap value path (no
    /// <see langword="throw"/>, no stack capture, minimal allocation), makes the failure a
    /// first-class part of your contract that survives serialization across brokers and process
    /// restarts, and lets <see cref="IAsyncResponsePayload.OnRecovery"/> drive
    /// recovery routing for it. Keep <c>SetException</c> for the failures you would genuinely want a
    /// stack trace and an <see langword="catch"/> block for.
    /// </para>
    /// <para>
    /// When a result-pattern payload that declines to resume reaches the lost-subscriber fallback, it
    /// is surfaced to the failure callback as an <see cref="AsyncResponseDomainFailureException"/>,
    /// so a domain failure and a technical <c>SetException</c> can share one failure-handling path
    /// while remaining distinguishable by type.
    /// </para>
    /// </remarks>
    Task SetException(Exception exception, string correlationId, CancellationToken cancellationToken = default);
}
