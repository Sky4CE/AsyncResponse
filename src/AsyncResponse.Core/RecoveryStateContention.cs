namespace AsyncResponse;

/// <summary>
/// Pacing for the optimistic read-modify-write loops of the recovery stores that keep every
/// registration of a correlation id in ONE stored value (the Redis and NATS channels). The waiters
/// of one id race on that value whenever they register or finish together — the natural shape of a
/// fan-out. A loser used to retry at once, so the losers of one round collided again in the next:
/// after four immediate attempts only about four writers of a burst had committed. Against Redis 7
/// and NATS 2.15, 25 to 28 of 32 concurrent registrations on one id failed, and concurrent
/// completions left their registrations behind for expiry the same way. A loser now pauses for a
/// randomized, exponentially growing interval (full jitter) before its next attempt, so a burst
/// spreads out and commits in turn.
/// <para>
/// Sized for the server's latency as well as the burst: once pauses reach their ceiling, a window
/// of that length fits about ceiling ÷ (read-to-commit time) commits, and a managed server over TLS
/// or a replicated NATS bucket takes 5–20 ms per read-to-commit. In an event simulation, 16 attempts
/// under a 100 ms ceiling lost a writer of a 64-writer burst (the default fan-out bound) in 5% of
/// bursts at 5 ms and in every burst at 10 ms; 30 attempts under a 250 ms ceiling lose none up to
/// 20 ms (0.2% at 40 ms), finishing such a burst within about 2 s. A single operation that loses
/// every attempt pauses for at most about 5.75 s in total (about 2.9 s on average) before a save
/// gives up or a delete leaves its registration for expiry; a lone or lightly contended writer never
/// waits long, since the ceiling only grows with consecutive losses.
/// </para>
/// <para>
/// The pause runs on the real clock, not the host's <see cref="TimeProvider"/>: it paces requests to
/// a real server, and a virtual clock that no test advances would otherwise stall the save.
/// </para>
/// </summary>
internal static class RecoveryStateContention
{
    /// <summary>Attempts before a save gives up with an exception, or a delete leaves its registration for expiry.</summary>
    public const int MaxAttempts = 30;

    /// <summary>The ceiling of the first pause; each lost attempt doubles it, up to <see cref="MaxDelay"/>.</summary>
    public static readonly TimeSpan BaseDelay = TimeSpan.FromMilliseconds(2);

    /// <summary>The largest ceiling a pause is drawn under.</summary>
    public static readonly TimeSpan MaxDelay = TimeSpan.FromMilliseconds(250);

    private static readonly AsyncLocal<bool> _pausesSuppressed = new();

    /// <summary>Pauses before attempt <paramref name="attempt"/> (one-based count of attempts already lost).</summary>
    public static Task PauseAsync(int attempt, CancellationToken cancellationToken)
        => _pausesSuppressed.Value ? Task.CompletedTask : Task.Delay(Pause(attempt), cancellationToken);

    /// <summary>
    /// Test seam: suppresses the pauses in the calling async flow until disposed — the attempt budget
    /// still applies. A test that forces every attempt to conflict otherwise sleeps through all the
    /// pauses (about 3 s on average) to prove what the attempt count alone decides.
    /// </summary>
    internal static IDisposable SuppressPauses()
    {
        var previous = _pausesSuppressed.Value;
        _pausesSuppressed.Value = true;
        return new PauseScope(previous);
    }

    private sealed class PauseScope(bool previous) : IDisposable
    {
        public void Dispose() => _pausesSuppressed.Value = previous;
    }

    /// <summary>A pause drawn uniformly from zero to <c>min(MaxDelay, BaseDelay × 2^(attempt − 1))</c>.</summary>
    internal static TimeSpan Pause(int attempt)
    {
        var ceiling = Math.Min(MaxDelay.Ticks, BaseDelay.Ticks << Math.Clamp(attempt - 1, 0, 20));
        return TimeSpan.FromTicks(Random.Shared.NextInt64(ceiling + 1));
    }
}
