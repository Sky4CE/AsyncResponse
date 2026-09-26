namespace AsyncResponse.Testing;

/// <summary>
/// A deterministic, manually-advanced <see cref="TimeProvider"/>. Registered as the engine clock
/// (which <see cref="AsyncResponseTestHarness"/> does for you), it makes every time-driven part of
/// AsyncResponse — waiter timeouts, execution leases, retry backoff, durable timers, cron
/// schedules — run on virtual time: a three-day sleep completes the instant the test calls
/// <see cref="Advance"/>.
/// <para>
/// <see cref="Advance"/> moves time <b>stepwise</b>: it walks to each armed timer's due instant in
/// order (due time, then creation order), updates "now" to that instant, and fires the callback
/// inline on the calling thread before moving on. Timers armed by a firing callback (a lease renew
/// loop re-arming itself, a chunked wake-up re-publishing) are honored within the same advance, so
/// interleavings match real time — a renew loop beats a lease expiry that sits later on the
/// timeline, never the other way around.
/// </para>
/// <para>
/// Time never moves on its own; <see cref="GetUtcNow"/> is exact and starts at
/// <see cref="DefaultStartTime"/> (2030-01-01T00:00:00Z) unless a start is supplied. Thread-safe:
/// advances are serialized, and an advance made from inside a timer callback nests (see
/// <see cref="AdvanceTo"/>).
/// </para>
/// <para>
/// <see cref="CreateTimer"/> and <see cref="ITimer.Change"/> accept and reject exactly the
/// arguments the system timer does — whole milliseconds, <c>-1</c> for "never", and the
/// 4294967294 ms (~49.7-day) ceiling — so a timer the virtual clock arms is one production arms too.
/// </para>
/// </summary>
public sealed class VirtualTimeProvider : TimeProvider
{
    /// <summary>The default virtual epoch: a fixed instant, so tests never depend on the wall clock.</summary>
    public static readonly DateTimeOffset DefaultStartTime = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    // _gate guards the clock state and is never held while a callback runs. _advanceGate
    // serializes whole advances and IS held across their callbacks; it is always taken first.
    private readonly object _gate = new();
    private readonly object _advanceGate = new();
    private readonly SortedSet<VirtualTimer> _armed = new(VirtualTimerOrder.Instance);
    private DateTimeOffset _utcNow;
    private long _sequence;

    /// <summary>Creates a provider starting at <see cref="DefaultStartTime"/>.</summary>
    public VirtualTimeProvider()
        : this(DefaultStartTime)
    {
    }

    /// <summary>Creates a provider starting at <paramref name="startTime"/>.</summary>
    public VirtualTimeProvider(DateTimeOffset startTime)
        => _utcNow = startTime.ToUniversalTime();

    /// <inheritdoc/>
    public override DateTimeOffset GetUtcNow()
    {
        lock (_gate)
            return _utcNow;
    }

    /// <inheritdoc/>
    public override TimeZoneInfo LocalTimeZone => TimeZoneInfo.Utc;

    /// <inheritdoc/>
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    /// <inheritdoc/>
    public override long GetTimestamp()
    {
        lock (_gate)
            return _utcNow.UtcTicks;
    }

    /// <summary>The earliest armed timer's due instant, or <c>null</c> when nothing is armed. Diagnostic.</summary>
    public DateTimeOffset? NextTimerDueAt
    {
        get
        {
            lock (_gate)
                return _armed.Count == 0 ? null : _armed.Min!.DueAt;
        }
    }

    /// <summary>
    /// Advances virtual time by <paramref name="delta"/>, firing every timer that falls due, in order.
    /// See <see cref="AdvanceTo"/> for how concurrent and re-entrant advances behave.
    /// </summary>
    public void Advance(TimeSpan delta)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delta, TimeSpan.Zero);

        // The target is relative to "now", so it is computed INSIDE the advance gate: read before
        // it, two threads advancing at once both started from the same instant and landed as one
        // step — or one of them found the clock already past its target and threw "backwards".
        lock (_advanceGate)
            AdvanceTo(GetUtcNow() + delta);
    }

    /// <summary>
    /// Advances virtual time to <paramref name="target"/>, firing every timer that falls due, in order.
    /// <para>
    /// One advance runs at a time. A call from another thread waits for the running advance to
    /// finish and then performs its own, so callbacks never run side by side and a callback never
    /// observes a clock later than its own fire instant. A call from <b>inside a timer callback</b>
    /// (re-entrant, same thread) nests: it runs to completion right there, firing everything due up
    /// to its own target in order, and the outer advance then carries on from wherever time stands —
    /// an outer target the nested advance already passed is simply complete, because time never
    /// moves backwards. Callbacks run on the advancing thread with no clock state locked, so they
    /// may read the clock and create, change, or dispose timers freely; a callback that blocks on
    /// <em>another</em> thread's advance deadlocks, as that advance is waiting for this one.
    /// </para>
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="target"/> is earlier than the current virtual time.</exception>
    public void AdvanceTo(DateTimeOffset target)
    {
        target = target.ToUniversalTime();

        // Monitor re-entrancy is the nesting rule above: the advancing thread's own callbacks
        // re-enter, every other thread queues behind the whole advance.
        lock (_advanceGate)
        {
            var validated = false;
            while (true)
            {
                VirtualTimer due;
                lock (_gate)
                {
                    if (target < _utcNow)
                    {
                        // Only the caller's own request can be "backwards". Re-checked on every
                        // iteration, this also threw out of a perfectly valid advance whose
                        // callback had nested one past its target.
                        if (!validated)
                            throw new ArgumentOutOfRangeException(nameof(target), target, "Cannot advance virtual time backwards.");
                        return;
                    }

                    validated = true;
                    var next = _armed.Count == 0 ? null : _armed.Min;
                    if (next is null || next.DueAt > target)
                    {
                        _utcNow = target;
                        return;
                    }

                    due = next;
                    _armed.Remove(due);
                    if (due.DueAt > _utcNow)
                        _utcNow = due.DueAt;

                    due.PrepareFire(_utcNow, out var rearmed);
                    if (rearmed)
                        _armed.Add(due);
                }

                // Outside the state lock: the callback may read the clock, create timers, or re-arm this one.
                due.Invoke();
            }
        }
    }

    /// <summary>
    /// Advances every time a timer is ARMED — created with a due time, re-armed by
    /// <see cref="ITimer.Change"/>, or re-armed by a periodic fire — and never when one is disposed
    /// or fires for the last time. The harness's settle reads it beside <see cref="NextTimerDueAt"/>
    /// to tell "a job just began a virtual-time wait" apart from a timer going away, which moves
    /// the earliest due time too.
    /// </summary>
    internal long ArmSequence
    {
        get
        {
            lock (_gate)
                return _sequence;
        }
    }

    /// <summary>
    /// Advances to <paramref name="target"/>, or — when another driver already moved the clock past
    /// it — only fires what is due at the current instant: never backwards. Decided under the
    /// advance gate, so two drivers (a pending harness publish and the test's own advance) cannot
    /// both read "now", and the later one throw "Cannot advance virtual time backwards" after the
    /// other passed its target.
    /// </summary>
    internal void AdvanceToAtLeast(DateTimeOffset target)
    {
        target = target.ToUniversalTime();
        lock (_advanceGate)
        {
            var now = GetUtcNow();
            AdvanceTo(target > now ? target : now);
        }
    }

    /// <summary>
    /// Starts <paramref name="operation"/> with every timer created on its async flow — its own
    /// retry backoffs, for instance — attributed to <paramref name="owner"/>
    /// (<see cref="NextTimerDueAtFor"/>). Continuations the operation schedules carry the
    /// attribution; code it merely unblocks (a waiter it completes) runs on its own flow and does
    /// not, and neither does work it enqueues on the in-memory transport — a job, or a delayed
    /// job's due time (<see cref="InMemoryWorkerTransport.TimerAttribution"/>). The caller's own
    /// flow is left unattributed.
    /// </summary>
    internal static Task StartAttributed(object owner, Func<Task> operation)
    {
        var previous = InMemoryWorkerTransport.TimerAttribution.Current;
        InMemoryWorkerTransport.TimerAttribution.Current = owner;
        try
        {
            return operation();
        }
        finally
        {
            InMemoryWorkerTransport.TimerAttribution.Current = previous;
        }
    }

    /// <summary>The earliest armed timer attributed to <paramref name="owner"/>, or <c>null</c> when none is armed.</summary>
    internal DateTimeOffset? NextTimerDueAtFor(object owner)
    {
        lock (_gate)
        {
            // Ordered by due time: the first match is the earliest.
            foreach (var timer in _armed)
            {
                if (ReferenceEquals(timer.Owner, owner))
                    return timer.DueAt;
            }

            return null;
        }
    }

    /// <inheritdoc/>
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);
        var timer = new VirtualTimer(this, callback, state) { Owner = InMemoryWorkerTransport.TimerAttribution.Current };
        timer.Change(dueTime, period);
        return timer;
    }

    /// <summary>A manually-driven timer; visible only through <see cref="ITimer"/>.</summary>
    internal sealed class VirtualTimer(VirtualTimeProvider _owner, TimerCallback _callback, object? _state) : ITimer
    {
        /// <summary><c>System.Threading.Timer</c>'s largest due time and period, in milliseconds (~49.7 days).</summary>
        private const long MaxSupportedTimeoutMilliseconds = 0xFFFFFFFE;

        internal DateTimeOffset DueAt { get; private set; }
        internal long Sequence { get; private set; }

        /// <summary>The attributed operation that created this timer (<see cref="StartAttributed"/>), if any.</summary>
        internal object? Owner { get; init; }
        private TimeSpan _period = Timeout.InfiniteTimeSpan;
        private bool _armed;
        private bool _disposed;

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            // The system timer's own check, to the letter (TimeProvider.System.CreateTimer and its
            // ITimer.Change): whole milliseconds, truncated; -1 means "never"; 0xFFFFFFFE is the
            // ceiling; dueTime is judged first. The laxer check this replaces armed timers the
            // BCL rejects — a 50-day due time, any period past the ceiling — so code passed here
            // and threw ArgumentOutOfRangeException in production.
            var dueMilliseconds = (long)dueTime.TotalMilliseconds;
            ArgumentOutOfRangeException.ThrowIfLessThan(dueMilliseconds, -1, nameof(dueTime));
            ArgumentOutOfRangeException.ThrowIfGreaterThan(dueMilliseconds, MaxSupportedTimeoutMilliseconds, nameof(dueTime));
            var periodMilliseconds = (long)period.TotalMilliseconds;
            ArgumentOutOfRangeException.ThrowIfLessThan(periodMilliseconds, -1, nameof(period));
            ArgumentOutOfRangeException.ThrowIfGreaterThan(periodMilliseconds, MaxSupportedTimeoutMilliseconds, nameof(period));

            lock (_owner._gate)
            {
                if (_disposed)
                    return false;

                if (_armed)
                {
                    _owner._armed.Remove(this);
                    _armed = false;
                }

                // Judged on the same truncated values as the check: a period of 0 or -1 whole
                // milliseconds is one-shot, a due time of -1 is "never", and a negative
                // sub-millisecond due time is 0 (due now) — never an instant before "now".
                _period = periodMilliseconds > 0 ? period : Timeout.InfiniteTimeSpan;
                if (dueMilliseconds == -1)
                    return true;

                DueAt = _owner._utcNow + (dueTime > TimeSpan.Zero ? dueTime : TimeSpan.Zero);
                Sequence = _owner._sequence++;
                _owner._armed.Add(this);
                _armed = true;
                return true;
            }
        }

        /// <summary>Called under the owner's gate just before firing: re-arms periodic timers.</summary>
        internal void PrepareFire(DateTimeOffset now, out bool rearmed)
        {
            if (_period > TimeSpan.Zero && _period != Timeout.InfiniteTimeSpan)
            {
                DueAt = now + _period;
                Sequence = _owner._sequence++;
                rearmed = true;
                return;
            }

            _armed = false;
            rearmed = false;
        }

        internal void Invoke() => _callback(_state);

        public void Dispose()
        {
            lock (_owner._gate)
            {
                if (_disposed)
                    return;

                _disposed = true;
                if (_armed)
                {
                    _owner._armed.Remove(this);
                    _armed = false;
                }
            }
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class VirtualTimerOrder : IComparer<VirtualTimer>
    {
        public static readonly VirtualTimerOrder Instance = new();

        public int Compare(VirtualTimer? x, VirtualTimer? y)
        {
            if (ReferenceEquals(x, y))
                return 0;

            var byDue = x!.DueAt.CompareTo(y!.DueAt);
            return byDue != 0 ? byDue : x.Sequence.CompareTo(y.Sequence);
        }
    }
}
