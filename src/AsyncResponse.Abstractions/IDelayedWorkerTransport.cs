namespace AsyncResponse;

/// <summary>
/// Optional capability of an <see cref="IWorkerTransport"/>: publishing a worker job that becomes
/// deliverable only after a delay, using the broker's native delayed/scheduled delivery.
/// <para>
/// Durable-flow timers (<c>IDurableFlowContext.DelayAsync</c>) prefer this capability — a sleeping
/// run suspends and is woken by a delayed wake-up job, holding no worker, no lease, and no memory
/// while it sleeps. On transports without it, timers fall back to waiting in process (the same
/// footprint as an awaited step). Application code can also schedule its own delayed jobs through
/// the <c>IAsyncResponseBuilder.EnqueueWorkerAsync(..., delay)</c> overloads, which require this
/// capability.
/// </para>
/// <para>
/// <b>Chunking:</b> brokers cap a single hop's delay (SQS: 15 minutes). A transport advertises its
/// cap via <see cref="MaxPublishDelay"/>; callers pass at most that per publish. Delays beyond the
/// cap are handled centrally: the job carries its absolute due time
/// (<see cref="WorkerJobEnvelope.NotBeforeUtc"/>), and the worker-job executor re-publishes an
/// early-delivered job for the remaining delay instead of executing it — so any capped transport
/// still delivers arbitrarily long delays, one maximal hop at a time.
/// </para>
/// </summary>
public interface IDelayedWorkerTransport : IWorkerTransport
{
    /// <summary>
    /// The largest delay this transport can apply to a single publish. Callers must clamp to this;
    /// longer waits ride the <see cref="WorkerJobEnvelope.NotBeforeUtc"/> re-publish chain.
    /// <para>
    /// <see cref="TimeSpan.Zero"/> (or negative) means the capability is unavailable in the
    /// <em>current configuration</em> even though the type implements the interface (e.g. an SQS
    /// FIFO worker queue). The engine treats such a transport exactly like one that does not
    /// implement <see cref="IDelayedWorkerTransport"/> at all: flow timers wait in process and
    /// delayed <c>EnqueueWorkerAsync</c> overloads fail fast at the publish call site — never
    /// after a flow has already persisted itself as sleeping.
    /// </para>
    /// </summary>
    TimeSpan MaxPublishDelay { get; }

    /// <summary>
    /// Publishes a worker job that becomes deliverable no earlier than <paramref name="delay"/>
    /// from now. <paramref name="delay"/> must be positive and at most <see cref="MaxPublishDelay"/>;
    /// callers stamp <see cref="WorkerJobEnvelope.NotBeforeUtc"/> with the absolute due time so
    /// early delivery (broker imprecision, chunked hops) is corrected at execution.
    /// </summary>
    Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default);
}
