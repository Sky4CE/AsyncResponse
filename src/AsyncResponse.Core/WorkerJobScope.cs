namespace AsyncResponse;

/// <summary>
/// The worker job executing on the current async flow, published by <c>WorkerJobExecutor</c> for
/// the duration of the invocation.
/// <para>
/// The durable-flow executor needs the job's identity (<see cref="WorkerJobEnvelope.JobId"/>), and
/// the envelope itself, to handle one case the invoked method cannot see from its arguments: the
/// broker redelivering the very job whose handler is still running, because an in-flight ceiling
/// lapsed under it. That delivery finds the execution lease held by its own first delivery; it is
/// the only copy of the run's wake-up the broker still has, so it must be recognised (the lease
/// records the job) and, where the transport can delay, re-published as the SAME job.
/// </para>
/// <para>
/// Entered by the frame that awaits the invocation, and entered for EVERY job — including one
/// that carries no id. An <see cref="AsyncLocal{T}"/> value written inside an async callee never
/// flows back to its caller, so the executor cannot delegate this to a helper; and the in-memory
/// transport runs a job under its ENQUEUER's captured execution context, so a follow-up job would
/// otherwise inherit the ambient job of whichever handler published it.
/// </para>
/// </summary>
internal static class WorkerJobScope
{
    private static readonly AsyncLocal<WorkerJobEnvelope?> _current = new();

    /// <summary>The job executing on this async flow, or <c>null</c> outside a worker job.</summary>
    public static WorkerJobEnvelope? Current => _current.Value;

    /// <summary>Publishes <paramref name="job"/> as the executing job until the returned scope is disposed.</summary>
    public static IDisposable Enter(WorkerJobEnvelope job)
    {
        var previous = _current.Value;
        _current.Value = job;
        return new Scope(previous);
    }

    private sealed class Scope(WorkerJobEnvelope? _previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _current.Value = _previous;
        }
    }
}
