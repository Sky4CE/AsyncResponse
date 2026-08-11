namespace AsyncResponse;

/// <summary>
/// Marks the currently-executing worker job as one the shared executor released EARLY because
/// consecutive re-delay hops proved the publishing and delivery-gating clocks disagree
/// (see <c>WorkerJobExecutor</c>'s redelay stall guard).
/// <para>
/// Durable timers need to know. A timer step that suspends mints a NEW wake-up envelope, and a
/// fresh envelope carries no stall evidence — so under persistent skew the run would suspend, be
/// handed back early, rebuild the proof from scratch, execute, suspend again, and never finish:
/// the guard's own state is erased on every lap. While this marker is set, the timer waits in
/// process for the remainder instead, which honors the due time without minting an envelope that
/// forgets what the previous hops established.
/// </para>
/// </summary>
internal static class WorkerJobSkewScope
{
    private static readonly AsyncLocal<bool> _forcedEarly = new();

    /// <summary>Whether the job executing on this async flow was released early by the stall guard.</summary>
    public static bool IsForcedEarlyExecution => _forcedEarly.Value;

    /// <summary>Marks the current job as force-executed until the returned scope is disposed.</summary>
    public static IDisposable Enter()
    {
        var previous = _forcedEarly.Value;
        _forcedEarly.Value = true;
        return new Scope(previous);
    }

    private sealed class Scope(bool _previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _forcedEarly.Value = _previous;
        }
    }
}
