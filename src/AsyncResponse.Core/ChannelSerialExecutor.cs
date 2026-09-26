using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace AsyncResponse;

/// <summary>
/// Executes asynchronous work items for a specific channel serially: a bounded
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
    internal const int DefaultCapacity = 1024;
    private readonly Channel<Func<Task>> _queue;
    private readonly Task _readerLoop;
    private readonly ILogger _logger;
    private readonly string _channel;

    // Items waiting for capacity or accepted into the queue but not yet pulled out for execution.
    // The item currently running is no longer "pending".
    private int _pending;

    /// <summary>How many work items are currently waiting for capacity or waiting to run.</summary>
    private int PendingCount => Volatile.Read(ref _pending);

    /// <summary>Runs the ChannelSerialExecutor operation.</summary>
    public ChannelSerialExecutor(ILogger logger, string channel, int capacity = DefaultCapacity)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity));

        _logger = logger;
        _channel = channel;

        // Waiting writers apply backpressure instead of allowing an overloaded correlation id to
        // retain an unbounded delegate backlog. One reader preserves strict per-key ordering.
        _queue = Channel.CreateBounded<Func<Task>>(new BoundedChannelOptions(capacity)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.Wait
        });

        _readerLoop = Task.Run(DrainAsync);
    }

    /// <summary>
    /// The single consumer: pulls work items in FIFO order and runs them one at a time. A work item
    /// that throws is logged and swallowed so the loop stays alive for the rest of the queue —
    /// exactly the resilience the old ActionBlock body provided. The loop's own log lines go
    /// through <see cref="SafeLog"/>: a throwing logging provider escaping here ended the loop,
    /// and every item queued behind it — the correlation id's later deliveries and its waiter's
    /// drain marker — sat accepted in a queue nothing read again.
    /// </summary>
    private async Task DrainAsync()
    {
        var reader = _queue.Reader;
        while (await reader.WaitToReadAsync().ConfigureAwait(false))
        {
            while (reader.TryRead(out var work))
            {
                Interlocked.Decrement(ref _pending);

                SafeLog.Try(this, static self =>
                {
                    if (self._logger.IsEnabled(LogLevel.Debug))
                        self._logger.LogDebug("Channel executor starting work for {Channel} (pending {PendingCount}).", self._channel, self.PendingCount);
                });

                try
                {
                    await work().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // swallow, so the loop stays alive
                    SafeLog.Try((Self: this, Error: ex), static state => state.Self._logger.LogError(
                        state.Error, "Channel executor error for {Channel} (pending {PendingCount}).", state.Self._channel, state.Self.PendingCount));
                }
                finally
                {
                    SafeLog.Try(this, static self =>
                    {
                        if (self._logger.IsEnabled(LogLevel.Debug))
                            self._logger.LogDebug("Channel executor completed work for {Channel} (pending {PendingCount}).", self._channel, self.PendingCount);
                    });
                }
            }
        }

        SafeLog.Try(this, static self =>
        {
            if (self._logger.IsEnabled(LogLevel.Debug))
                self._logger.LogDebug("Channel {Channel} executor completed (pending {PendingCount}).", self._channel, self.PendingCount);
        });
    }

    /// <summary>
    /// Queues a work delegate for execution. The returned task completes when the item has been
    /// accepted into the queue (not when the work is finished). Returns <c>false</c> when the
    /// executor is already shutting down (the queue was completed by <see cref="DisposeAsync"/>).
    /// </summary>
    public Task<bool> Enqueue(Func<Task> work, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(work);
        if (cancellationToken.IsCancellationRequested)
            return Task.FromCanceled<bool>(cancellationToken);

        return EnqueueCoreAsync(work, cancellationToken);
    }

    private async Task<bool> EnqueueCoreAsync(Func<Task> work, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _pending);
        try
        {
            await _queue.Writer.WriteAsync(work, cancellationToken).ConfigureAwait(false);
            LogEnqueued();
            return true;
        }
        catch (ChannelClosedException)
        {
            Interlocked.Decrement(ref _pending);
            return false;
        }
        catch
        {
            Interlocked.Decrement(ref _pending);
            throw;
        }
    }

    /// <summary>
    /// Synchronously queues a work delegate, returning <c>false</c> when the executor is already
    /// shutting down or full. Use <see cref="Enqueue"/> when the producer can wait for capacity.
    /// <paramref name="logIfFull"/> is <c>false</c> for producers that treat a full queue as
    /// expected backpressure and come back later (the DB channels' dispatch sweep), so a busy
    /// correlation id does not log a warning per sweep tick.
    /// </summary>
    public bool TryEnqueue(Func<Task> work, bool logIfFull = true)
    {
        ArgumentNullException.ThrowIfNull(work);
        Interlocked.Increment(ref _pending);
        if (_queue.Writer.TryWrite(work))
        {
            LogEnqueued();
            return true;
        }

        Interlocked.Decrement(ref _pending);
        SafeLog.Try((Self: this, LogIfFull: logIfFull), static state =>
        {
            if (state.LogIfFull)
                state.Self._logger.LogWarning("Channel executor could not enqueue work for {Channel}; queue is full or completed (pending {PendingCount}).", state.Self._channel, state.Self.PendingCount);
            else if (state.Self._logger.IsEnabled(LogLevel.Debug))
                state.Self._logger.LogDebug("Channel executor for {Channel} is at capacity (pending {PendingCount}); the producer will retry later.", state.Self._channel, state.Self.PendingCount);
        });
        return false;
    }

    // The item is already queued when this runs: a throwing logging provider must not report the
    // accepted item as a failed enqueue (the caller would retry or fault a delivery that will run).
    private void LogEnqueued() => SafeLog.Try(this, static self =>
    {
        if (self._logger.IsEnabled(LogLevel.Debug))
            self._logger.LogDebug("Channel executor enqueued work for {Channel} (pending {PendingCount}).", self._channel, self.PendingCount);
    });

    /// <summary>
    /// Signals that no more work items will be posted and waits for queued work to complete.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // Complete the writer so the reader drains the remaining items and the loop exits; then wait
        // for the loop to finish so callers can rely on all queued work having run. The writer is
        // completed before anything is logged: a throwing provider must not skip it and leave the
        // reader loop parked forever.
        _queue.Writer.TryComplete();
        SafeLog.Try(this, static self =>
        {
            if (self._logger.IsEnabled(LogLevel.Debug))
                self._logger.LogDebug("Disposing channel executor for {Channel} (pending {PendingCount}).", self._channel, self.PendingCount);
        });

        await _readerLoop.ConfigureAwait(false);

        SafeLog.Try(this, static self =>
        {
            if (self._logger.IsEnabled(LogLevel.Debug))
                self._logger.LogDebug("Disposed channel executor for {Channel} (pending {PendingCount}).", self._channel, self.PendingCount);
        });
    }
}
