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
    private readonly Dictionary<string, ExecutorEntry> _executors = new(StringComparer.Ordinal);
    private readonly object _gate = new();

    /// <summary>
    /// Enqueues <paramref name="work"/> onto the channel's serial executor, creating the executor on
    /// first use. Admission is reserved under the registry lock against a guaranteed-live executor.
    /// </summary>
    public void Enqueue(string channel, Func<Task> work)
        => EnqueueAsync(channel, work).AsTask().GetAwaiter().GetResult();

    /// <summary>Asynchronously enqueues work, applying bounded per-channel backpressure.</summary>
    public async ValueTask EnqueueAsync(string channel, Func<Task> work, CancellationToken cancellationToken = default)
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
                return;
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
            }

            entry.Retired.TrySetResult();
        }
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
