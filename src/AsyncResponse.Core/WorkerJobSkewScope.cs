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
/// <para>
/// The proof is ONE-SHOT: the wake-up that carried it targets a single parked timer step, and
/// only that step may spend it (<see cref="TryConsumeForcedEarlyExecution"/>). An unrelated later
/// timer reached in the same replay suspends normally — its own wake-up rebuilds the stall
/// evidence if the skew persists — instead of inheriting an exemption that would pin it in
/// process for its full remainder or fail it on the timer ceiling.
/// </para>
/// </summary>
internal static class WorkerJobSkewScope
{
    private static readonly AsyncLocal<Marker?> _forcedEarly = new();

    /// <summary>Whether the job executing on this async flow was released early by the stall guard.</summary>
    public static bool IsForcedEarlyExecution => _forcedEarly.Value is { Consumed: false };

    /// <summary>
    /// Claims the marker for the calling step: returns <c>true</c> exactly once per
    /// <see cref="Enter"/>, then <c>false</c> for every later caller on the same job.
    /// </summary>
    public static bool TryConsumeForcedEarlyExecution()
    {
        if (_forcedEarly.Value is not { Consumed: false } marker)
            return false;

        marker.Consumed = true;
        return true;
    }

    /// <summary>Marks the current job as force-executed until the returned scope is disposed.</summary>
    public static IDisposable Enter()
    {
        var previous = _forcedEarly.Value;
        _forcedEarly.Value = new Marker();
        return new Scope(previous);
    }

    /// <summary>
    /// Heap cell shared by every continuation of the job: an <see cref="AsyncLocal{T}"/> VALUE
    /// written inside an async callee never flows back to its caller, so consumption mutates the
    /// referenced marker instead of the ambient slot.
    /// </summary>
    private sealed class Marker
    {
        public bool Consumed;
    }

    private sealed class Scope(Marker? _previous) : IDisposable
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
