using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace AsyncResponse;

/// <summary>
/// Executes asynchronous work items for a specific channel serially: an unbounded
/// <see cref="Channel{T}"/> drained by a single reader loop guarantees per-channel ordering, so
/// progress messages for one correlation id are never processed concurrently or out of order.
/// <para>
/// This is a deliberately lean replacement for an earlier <c>ActionBlock&lt;Func&lt;Task&gt;&gt;</c>:
/// the rest of the hot path is hand-tuned to avoid allocations, and a single reader over a
/// <see cref="System.Threading.Channels"/> queue gives the same strict serial, in-order, one-at-a-time
/// semantics without TPL Dataflow's per-item task/scheduler overhead or the dependency it pulls in.
/// </para>
/// </summary>
internal sealed class ChannelSerialExecutor : IAsyncDisposable
{
    private readonly Channel<Func<Task>> _queue;
    private readonly Task _readerLoop;
    private readonly ILogger _logger;
    private readonly string _channel;

    // Items accepted into the queue but not yet pulled out for execution. Mirrors the old
    // ActionBlock.InputCount: the item currently running is no longer "pending".
    private int _pending;

    /// <summary>How many work items are currently buffered (waiting to run) in this executor.</summary>
    private int PendingCount => Volatile.Read(ref _pending);

    /// <summary>Runs the ChannelSerialExecutor operation.</summary>
    public ChannelSerialExecutor(ILogger logger, string channel)
    {
        _logger = logger;
        _channel = channel;

        // Unbounded so enqueue never blocks (matches the old BoundedCapacity.Unbounded). A single
        // reader loop is the sole consumer; producers (e.g. the broker subscriber callback) may be
        // multiple, so SingleWriter stays false. Synchronous continuations are disabled so a
        // completing work item never runs the next producer's continuation inline.
        _queue = Channel.CreateUnbounded<Func<Task>>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });

        _readerLoop = Task.Run(DrainAsync);
    }

    /// <summary>
    /// The single consumer: pulls work items in FIFO order and runs them one at a time. A work item
    /// that throws is logged and swallowed so the loop stays alive for the rest of the queue —
    /// exactly the resilience the old ActionBlock body provided.
    /// </summary>
    private async Task DrainAsync()
    {
        var reader = _queue.Reader;
        while (await reader.WaitToReadAsync().ConfigureAwait(false))
        {
            while (reader.TryRead(out var work))
            {
                Interlocked.Decrement(ref _pending);

                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug("Channel executor starting work for {Channel} (pending {PendingCount}).", _channel, PendingCount);

                try
                {
                    await work().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Channel executor error for {Channel} (pending {PendingCount}).", _channel, PendingCount);
                    // swallow, so the loop stays alive
                }
                finally
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug("Channel executor completed work for {Channel} (pending {PendingCount}).", _channel, PendingCount);
                }
            }
        }

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Channel {Channel} executor completed (pending {PendingCount}).", _channel, PendingCount);
    }

    /// <summary>
    /// Queues a work delegate for execution. The returned task completes when the item has been
    /// accepted into the queue (not when the work is finished). Returns <c>false</c> when the
    /// executor is already shutting down (the queue was completed by <see cref="DisposeAsync"/>).
    /// </summary>
    public Task<bool> Enqueue(Func<Task> work, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
            return Task.FromCanceled<bool>(cancellationToken);

        return Task.FromResult(TryEnqueue(work));
    }

    /// <summary>
    /// Synchronously queues a work delegate, returning <c>false</c> when the executor is already
    /// shutting down. The queue is unbounded so this never blocks, which makes it safe to call while
    /// holding a lock (see <see cref="SerialExecutorRegistry"/>, which relies on that to enqueue and
    /// retire executors atomically).
    /// </summary>
    public bool TryEnqueue(Func<Task> work)
    {
        // The queue is unbounded, so a write that is going to succeed succeeds synchronously.
        Interlocked.Increment(ref _pending);
        if (_queue.Writer.TryWrite(work))
        {
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Channel executor enqueued work for {Channel} (pending {PendingCount}).", _channel, PendingCount);
            return true;
        }

        Interlocked.Decrement(ref _pending);
        _logger.LogWarning("Channel executor failed to enqueue work for {Channel}; channel already completed (pending {PendingCount}).", _channel, PendingCount);
        return false;
    }

    /// <summary>
    /// Signals that no more work items will be posted and waits for queued work to complete.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Disposing channel executor for {Channel} (pending {PendingCount}).", _channel, PendingCount);

        // Complete the writer so the reader drains the remaining items and the loop exits; then wait
        // for the loop to finish so callers can rely on all queued work having run.
        _queue.Writer.TryComplete();
        await _readerLoop.ConfigureAwait(false);

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Disposed channel executor for {Channel} (pending {PendingCount}).", _channel, PendingCount);
    }
}
