namespace AsyncResponse;

/// <summary>
/// Optional capability of an <see cref="IWorkerTransport"/>: advertising the broker's
/// <em>in-flight ceiling</em> — the longest a single delivery can stay unsettled before the broker
/// redelivers it no matter how alive its handler is.
/// <para>
/// Most transports keep a long-running handler's delivery alive for as long as the handler lives
/// (a lock/lease heartbeat, an in-progress signal) and have no such ceiling; they do not implement
/// this interface. Some brokers cap the total: Google Pub/Sub stops extending the ack deadline
/// after <c>MaxTotalAckExtension</c>, RabbitMQ closes a channel whose delivery stays
/// unacknowledged past <c>consumer_timeout</c>, and SQS never lets a message stay invisible for
/// more than 12 hours. Past the ceiling the broker hands the <em>same</em> job to another consumer
/// while the first handler is still running.
/// </para>
/// <para>
/// The durable-flow engine plans in-process waits against this value: a timer that has to wait in
/// process (no <see cref="IDelayedWorkerTransport"/>) parks for a bounded hop well inside the
/// ceiling, then publishes a fresh wake-up and suspends, so no single delivery is ever held past
/// what the broker allows. See <see cref="WorkerJobEnvelope.JobId"/> for how a delivery that does
/// outlive the ceiling is recognised.
/// </para>
/// </summary>
public interface IWorkerTransportInFlightLimit : IWorkerTransport
{
    /// <summary>
    /// The longest one delivery may stay in flight before the broker redelivers it regardless of
    /// handler liveness, or <c>null</c> when the current configuration has no such ceiling.
    /// Values must be positive.
    /// </summary>
    TimeSpan? MaxInFlightDuration { get; }
}
