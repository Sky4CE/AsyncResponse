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
internal sealed class SerialExecutorRegistry(
    ILogger _logger,
    TimeSpan? disposeDrainLimit = null,
    TimeSpan? enqueueDrainLimit = null,
    TimeProvider? timeProvider = null)
{
    // How long a retired channel's tombstone blocks executor re-creation. Long enough to outlive
    // any enqueue that was already in flight when cleanup retired the executor, short enough that
    // an unpruned tombstone only ever delays a reused correlation id briefly.
    internal static readonly TimeSpan TombstoneLifetime = TimeSpan.FromSeconds(30);

    // Upper bound on how long retirement waits for in-flight enqueues to drain. A producer can be
    // parked indefinitely awaiting queue capacity with a token that never fires, and teardown
    // paths await RemoveAsync directly — they must not inherit that hang.
    internal static readonly TimeSpan EnqueueDrainLimit = TimeSpan.FromSeconds(30);

    // Upper bound on how long retirement waits for the executor's dispatched work to finish. A
    // dispatched item runs arbitrary user code (a completion predicate that never finishes), and
    // an unbounded wait here would wedge the channel key permanently — every later enqueue for
    // the correlation id parks on the never-completed retirement, including a NEW waiter that
    // legitimately re-registered the id.
    internal static readonly TimeSpan DisposeDrainLimit = TimeSpan.FromSeconds(30);

    private readonly TimeSpan _disposeDrainLimit = disposeDrainLimit ?? DisposeDrainLimit;

    // Overridable alongside _disposeDrainLimit: RemoveAsync waits on BOTH budgets in sequence, so
    // a caller that could shorten only one still paid the other's full 30 seconds — and the
    // worst-case retirement became "the value I passed, plus 30s" rather than the value passed.
    private readonly TimeSpan _enqueueDrainLimit = enqueueDrainLimit ?? EnqueueDrainLimit;

    // Tombstone expiry runs on the injected clock so a virtual-clock harness can advance past
    // TombstoneLifetime instead of sleeping 30 real seconds. Without it, the drop-a-delivery
    // branch in EnqueueAsync (and tombstone pruning) could not be covered deterministically.
    private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;

    private DateTimeOffset UtcNow => _timeProvider.GetUtcNow();

    private readonly Dictionary<string, ExecutorEntry> _executors = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DateTimeOffset> _tombstones = new(StringComparer.Ordinal);
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
            try
            {
                // Bounded wait: an admitted enqueue can be parked indefinitely on a full queue
                // with a token that never fires. Proceeding after the limit is safe — disposal
                // completes the executor's writer, which unparks the wedged producer, and its
                // retry then lands on the tombstone/recreate machinery built for exactly the
                // enqueue-races-retirement case.
                await waitForEnqueues.WaitAsync(_enqueueDrainLimit, _timeProvider).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                _logger.LogWarning(
                    "Timed out after {DrainLimit} waiting for in-flight enqueues on channel {Channel} to drain; disposing the executor anyway.",
                    _enqueueDrainLimit,
                    channel);
            }

            try
            {
                // Bounded for the same reason: disposal waits for the reader loop, which can be
                // parked in a dispatched item's arbitrary user code. The writer is completed
                // before disposal first awaits, so the abandoned loop drains and exits on its own
                // if the wedged item ever finishes; retiring the entry regardless (the finally
                // below) keeps the channel key usable for future waiters.
                await entry.Executor.DisposeAsync().AsTask().WaitAsync(_disposeDrainLimit, _timeProvider).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                _logger.LogWarning(
                    "Timed out after {DrainLimit} waiting for in-flight work on channel {Channel} to finish; abandoning the hung work item and retiring the executor.",
                    _disposeDrainLimit,
                    channel);
            }
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
                _tombstones[channel] = UtcNow + TombstoneLifetime;
                PruneTombstonesUnderLock();
            }

            entry.Retired.TrySetResult();
        }
    }

    private bool IsTombstonedUnderLock(string channel)
    {
        if (!_tombstones.TryGetValue(channel, out var expiresAtUtc))
            return false;

        if (expiresAtUtc > UtcNow)
            return true;

        _tombstones.Remove(channel);
        return false;
    }

    private void PruneTombstonesUnderLock()
    {
        if (_tombstones.Count == 0)
            return;

        var now = UtcNow;
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
