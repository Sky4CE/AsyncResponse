using Microsoft.Extensions.Logging;

namespace AsyncResponse;

/// <summary>
/// Owns the per-channel <see cref="ChannelSerialExecutor"/> instances for a response channel and
/// coordinates their lifecycle so that, for any one channel key, work is never enqueued onto an
/// executor that is concurrently being retired — which would silently drop the message — and at
/// most one <em>live</em> executor exists per channel at a time.
/// <para>
/// The coordination is a single lock guarding the map. <see cref="EnqueueAsync"/> reserves the live
/// executor under that lock, then waits for bounded queue capacity outside it. Retirement closes
/// admission, waits for those reservations, drains the executor, and only then lets a new executor
/// be created. Disposal runs outside the lock.
/// </para>
/// <para>
/// This replaces an earlier <c>ConcurrentDictionary</c> + fire-and-forget <c>Task.Run(remove)</c>
/// scheme in which a new waiter reusing a correlation id mid-drain could observe a second executor
/// for the same channel, briefly violating the per-channel ordering guarantee.
/// </para>
/// </summary>
internal sealed class SerialExecutorRegistry(ILogger _logger)
{
    // How long a retired channel's tombstone blocks executor re-creation. Long enough to outlive
    // any enqueue that was already in flight when cleanup retired the executor, short enough that
    // an unpruned tombstone only ever delays a reused correlation id briefly.
    internal static readonly TimeSpan TombstoneLifetime = TimeSpan.FromSeconds(30);

    private readonly Dictionary<string, ExecutorEntry> _executors = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DateTime> _tombstones = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _registrations = new(StringComparer.Ordinal);
    private readonly object _gate = new();

    /// <summary>
    /// Records a live subscription for <paramref name="channel"/>. While any subscription is
    /// registered, retirement tombstones do not drop work — a retired executor is legitimately
    /// recreated, and the remaining subscription's own cleanup retires it again (no leak).
    /// </summary>
    public void OnSubscriptionRegistered(string channel)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);

        lock (_gate)
        {
            _registrations[channel] = _registrations.TryGetValue(channel, out var count) ? count + 1 : 1;
            _tombstones.Remove(channel);
        }
    }

    /// <summary>Records that a subscription for <paramref name="channel"/> is gone.</summary>
    public void OnSubscriptionRetired(string channel)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);

        lock (_gate)
        {
            if (!_registrations.TryGetValue(channel, out var count))
                return;

            if (count <= 1)
                _registrations.Remove(channel);
            else
                _registrations[channel] = count - 1;
        }
    }

    /// <summary>
    /// Asynchronously enqueues work, applying bounded per-channel backpressure. Returns <c>true</c>
    /// once the work is accepted by a live executor; <c>false</c> when it was suppressed by a
    /// tombstone (the executor was retired with no registration left — every dispatch it ever
    /// admitted has fully completed, so a caller draining before disposal knows nothing is in
    /// flight).
    /// </summary>
    public async ValueTask<bool> EnqueueAsync(string channel, Func<Task> work, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);
        ArgumentNullException.ThrowIfNull(work);

        while (true)
        {
            ExecutorEntry? entry = null;
            Task? retirement = null;
            lock (_gate)
            {
                if (!_executors.TryGetValue(channel, out var current))
                {
                    // A tombstoned channel was retired and no subscription is registered anymore:
                    // recreating an executor here (typically for an enqueue that was already in
                    // flight when cleanup ran) would leak it — nothing retires it again — and the
                    // work item would no-op anyway because the subscriptions are gone. Drop it.
                    // With a subscription still registered the recreate is legitimate (its own
                    // cleanup retires the new executor), so the tombstone does not apply.
                    if (!_registrations.ContainsKey(channel) && IsTombstonedUnderLock(channel))
                    {
                        // Deliberate, but never silent: if this fires for a live waiter, its channel
                        // registered the subscription only after the transport began delivering.
                        _logger.LogWarning(
                            "Suppressed a delivery for channel {Channel}: the channel is tombstoned and has no registered subscription.",
                            channel);
                        return false;
                    }

                    current = new ExecutorEntry(new ChannelSerialExecutor(_logger, channel));
                    _executors[channel] = current;
                }

                if (current.Retiring)
                    retirement = current.Retired.Task;
                else
                {
                    current.InFlightEnqueues++;
                    entry = current;
                }
            }

            if (entry is null)
            {
                await retirement!.WaitAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            bool accepted;
            try
            {
                accepted = await entry.Executor.Enqueue(work, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                TaskCompletionSource? enqueuesDrained = null;
                lock (_gate)
                {
                    entry.InFlightEnqueues--;
                    if (entry.Retiring && entry.InFlightEnqueues == 0)
                        enqueuesDrained = entry.EnqueuesDrained;
                }

                enqueuesDrained?.TrySetResult();
            }

            if (accepted)
                return true;
        }
    }

    /// <summary>
    /// Retires the channel's serial executor (if present), draining its queued work. Safe to call
    /// concurrently with <see cref="EnqueueAsync"/>: admitted enqueues finish against the retiring
    /// executor, while later enqueues wait until it is fully drained before creating a replacement.
    /// </summary>
    public async ValueTask RemoveAsync(string channel)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channel);

        ExecutorEntry? entry;
        Task waitForEnqueues;
        var ownsRetirement = false;
        lock (_gate)
        {
            if (!_executors.TryGetValue(channel, out entry))
                return;

            if (entry.Retiring)
            {
                waitForEnqueues = entry.Retired.Task;
            }
            else
            {
                entry.Retiring = true;
                ownsRetirement = true;
                if (entry.InFlightEnqueues == 0)
                {
                    waitForEnqueues = Task.CompletedTask;
                }
                else
                {
                    entry.EnqueuesDrained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    waitForEnqueues = entry.EnqueuesDrained.Task;
                }
            }
        }

        if (!ownsRetirement)
        {
            await waitForEnqueues.ConfigureAwait(false);
            return;
        }

        try
        {
            await waitForEnqueues.ConfigureAwait(false);
            await entry.Executor.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            lock (_gate)
            {
                if (_executors.TryGetValue(channel, out var current) && ReferenceEquals(current, entry))
                    _executors.Remove(channel);

                // Tombstone the retired channel so an enqueue that raced this retirement cannot
                // recreate a leaked executor; ClearTombstone lifts it the moment a new
                // subscription legitimately reuses the channel.
                _tombstones[channel] = DateTime.UtcNow + TombstoneLifetime;
                PruneTombstonesUnderLock();
            }

            entry.Retired.TrySetResult();
        }
    }

    private bool IsTombstonedUnderLock(string channel)
    {
        if (!_tombstones.TryGetValue(channel, out var expiresAtUtc))
            return false;

        if (expiresAtUtc > DateTime.UtcNow)
            return true;

        _tombstones.Remove(channel);
        return false;
    }

    private void PruneTombstonesUnderLock()
    {
        if (_tombstones.Count == 0)
            return;

        var now = DateTime.UtcNow;
        List<string>? expired = null;
        foreach (var (channel, expiresAtUtc) in _tombstones)
        {
            if (expiresAtUtc <= now)
                (expired ??= []).Add(channel);
        }

        if (expired is null)
            return;

        foreach (var channel in expired)
            _tombstones.Remove(channel);
    }

    private sealed class ExecutorEntry(ChannelSerialExecutor executor)
    {
        public ChannelSerialExecutor Executor { get; } = executor;
        public TaskCompletionSource Retired { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource? EnqueuesDrained { get; set; }
        public int InFlightEnqueues { get; set; }
        public bool Retiring { get; set; }
    }
}
