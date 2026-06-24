using Microsoft.Extensions.Logging;

namespace AsyncResponse;

/// <summary>
/// Owns the per-channel <see cref="ChannelSerialExecutor"/> instances for a response channel and
/// coordinates their lifecycle so that, for any one channel key, work is never enqueued onto an
/// executor that is concurrently being retired — which would silently drop the message — and at
/// most one <em>live</em> executor exists per channel at a time.
/// <para>
/// The coordination is a single lock guarding the map. <see cref="Enqueue"/> gets-or-creates and
/// writes the work under the lock; because <see cref="RemoveAsync"/> removes an executor from the
/// map under the same lock <em>before</em> disposing it, a write under the lock always targets a
/// live executor. Disposal (which drains the executor) runs outside the lock, so it never blocks
/// producers and never deadlocks a caller that is itself running on the executor's drain loop (the
/// terminal-message path retires its executor via a fire-and-forget continuation for exactly that
/// reason).
/// </para>
/// <para>
/// This replaces an earlier <c>ConcurrentDictionary</c> + fire-and-forget <c>Task.Run(remove)</c>
/// scheme in which a new waiter reusing a correlation id mid-drain could observe a second executor
/// for the same channel, briefly violating the per-channel ordering guarantee.
/// </para>
/// </summary>
internal sealed class SerialExecutorRegistry(ILogger _logger)
{
    private readonly Dictionary<string, ChannelSerialExecutor> _executors = new(StringComparer.Ordinal);
    private readonly object _gate = new();

    /// <summary>
    /// Enqueues <paramref name="work"/> onto the channel's serial executor, creating the executor on
    /// first use. The enqueue is performed under the registry lock against a guaranteed-live executor.
    /// </summary>
    public void Enqueue(string channel, Func<Task> work)
    {
        bool accepted;
        lock (_gate)
        {
            if (!_executors.TryGetValue(channel, out var executor))
            {
                executor = new ChannelSerialExecutor(_logger, channel);
                _executors[channel] = executor;
            }

            // TryEnqueue is non-blocking (unbounded write), so holding the lock across it is cheap;
            // and because a retiring executor leaves the map under this same lock before it is
            // disposed, the executor resolved here is always live — the write cannot be lost.
            accepted = executor.TryEnqueue(work);
        }

        // Defense in depth: under the invariant above this never happens, but if it ever did we want
        // a signal rather than a silently dropped message.
        if (!accepted)
            _logger.LogWarning("Executor rejected message for channel {Channel}.", channel);
    }

    /// <summary>
    /// Retires the channel's serial executor (if present), draining its queued work. Safe to call
    /// concurrently with <see cref="Enqueue"/>: a concurrent enqueue either lands on this executor
    /// before it leaves the map (and is drained by the dispose below) or creates a fresh executor.
    /// </summary>
    public async ValueTask RemoveAsync(string channel)
    {
        ChannelSerialExecutor? executor;
        lock (_gate)
        {
            if (!_executors.TryGetValue(channel, out executor))
                return;

            _executors.Remove(channel);
        }

        await executor.DisposeAsync().ConfigureAwait(false);
    }
}
