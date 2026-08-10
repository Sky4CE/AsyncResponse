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
/// <see cref="DefaultStartTime"/> (2030-01-01T00:00:00Z) unless a start is supplied. Thread-safe.
/// </para>
/// </summary>
public sealed class VirtualTimeProvider : TimeProvider
{
    /// <summary>The default virtual epoch: a fixed instant, so tests never depend on the wall clock.</summary>
    public static readonly DateTimeOffset DefaultStartTime = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private readonly object _gate = new();
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

    /// <summary>Advances virtual time by <paramref name="delta"/>, firing every timer that falls due, in order.</summary>
    public void Advance(TimeSpan delta)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delta, TimeSpan.Zero);
        AdvanceTo(GetUtcNow() + delta);
    }

    /// <summary>Advances virtual time to <paramref name="target"/>, firing every timer that falls due, in order.</summary>
    public void AdvanceTo(DateTimeOffset target)
    {
        target = target.ToUniversalTime();

        while (true)
        {
            VirtualTimer due;
            lock (_gate)
            {
                if (target < _utcNow)
                    throw new ArgumentOutOfRangeException(nameof(target), target, "Cannot advance virtual time backwards.");

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

            // Outside the lock: the callback may read the clock, create timers, or re-arm this one.
            due.Invoke();
        }
    }

    internal void Schedule(VirtualTimer timer)
    {
        lock (_gate)
            _armed.Add(timer);
    }

    internal void Unschedule(VirtualTimer timer)
    {
        lock (_gate)
            _armed.Remove(timer);
    }

    internal long NextSequence()
    {
        lock (_gate)
            return _sequence++;
    }

    /// <inheritdoc/>
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);
        var timer = new VirtualTimer(this, callback, state);
        timer.Change(dueTime, period);
        return timer;
    }

    /// <summary>A manually-driven timer; visible only through <see cref="ITimer"/>.</summary>
    internal sealed class VirtualTimer(VirtualTimeProvider _owner, TimerCallback _callback, object? _state) : ITimer
    {
        internal DateTimeOffset DueAt { get; private set; }
        internal long Sequence { get; private set; }
        private TimeSpan _period = Timeout.InfiniteTimeSpan;
        private bool _armed;
        private bool _disposed;

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            // Match System.Threading.Timer's contract: -1ms means "never"; other negatives are invalid.
            if (dueTime < TimeSpan.Zero && dueTime != Timeout.InfiniteTimeSpan)
                throw new ArgumentOutOfRangeException(nameof(dueTime));
            if (period < TimeSpan.Zero && period != Timeout.InfiniteTimeSpan)
                throw new ArgumentOutOfRangeException(nameof(period));

            lock (_owner._gate)
            {
                if (_disposed)
                    return false;

                if (_armed)
                {
                    _owner._armed.Remove(this);
                    _armed = false;
                }

                _period = period;
                if (dueTime == Timeout.InfiniteTimeSpan)
                    return true;

                DueAt = _owner._utcNow + dueTime;
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
