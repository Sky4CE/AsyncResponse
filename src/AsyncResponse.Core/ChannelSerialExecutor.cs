using Microsoft.Extensions.Logging;
using System.Threading.Tasks.Dataflow;

namespace AsyncResponse;

/// <summary>
/// Executes asynchronous work items for a specific channel serially: a single-parallelism
/// <see cref="ActionBlock{TInput}"/> guarantees per-channel ordering, so progress messages for
/// one correlation id are never processed concurrently or out of order.
/// </summary>
internal sealed class ChannelSerialExecutor : IAsyncDisposable
{
    private const string SERVICE_NAME = nameof(ChannelSerialExecutor);

    private readonly ActionBlock<Func<Task>> _block;
    private readonly ILogger _logger;
    private readonly string _channel;

    /// <summary>How many work items are currently buffered (waiting to run) in this executor.</summary>
    private int PendingCount => _block?.InputCount ?? 0;

    public ChannelSerialExecutor(ILogger logger, string channel)
    {
        _logger = logger;
        _channel = channel;

        _block = new ActionBlock<Func<Task>>(
            async work =>
            {
                logger.LogDebug("{ServiceName}: starting work for {Channel}; PendingCount={PendingCount}",
                    SERVICE_NAME, channel, PendingCount);
                try
                {
                    await work().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "{ServiceName}: Error in channel executor for {Channel}; PendingCount={PendingCount}",
                        SERVICE_NAME, channel, PendingCount);
                    // swallow, so the block stays alive
                }
                finally
                {
                    logger.LogDebug("{ServiceName}: completed work for {Channel}; PendingCount={PendingCount}",
                        SERVICE_NAME, channel, PendingCount);
                }
            },
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1,
                EnsureOrdered = true,
                BoundedCapacity = DataflowBlockOptions.Unbounded,
                CancellationToken = default
            });

        _block.Completion.ContinueWith(t =>
            logger.LogDebug("{ServiceName}: Channel {Channel} executor completed (IsFaulted={Faulted}); PendingCount={PendingCount}",
                SERVICE_NAME, channel, t.IsFaulted, PendingCount));
    }

    /// <summary>
    /// Queues a work delegate for execution. The returned task completes when the item has been
    /// accepted into the queue (not when the work is finished).
    /// </summary>
    public async Task<bool> Enqueue(Func<Task> work, CancellationToken cancellationToken = default)
    {
        var accepted = await _block.SendAsync(work, cancellationToken).ConfigureAwait(false);
        if (accepted)
        {
            _logger.LogDebug("{ServiceName}: enqueued work for {Channel}; now PendingCount={PendingCount}",
                SERVICE_NAME, _channel, PendingCount);
        }
        else
        {
            _logger.LogWarning("{ServiceName}: failed to enqueue work for {Channel} (block completed); PendingCount={PendingCount}",
                SERVICE_NAME, _channel, PendingCount);
        }
        return accepted;
    }

    /// <summary>
    /// Signals that no more work items will be posted and waits for queued work to complete.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        _logger.LogDebug("{ServiceName}: disposing channel executor for {Channel}; PendingCount={PendingCount}",
            SERVICE_NAME, _channel, PendingCount);

        _block.Complete();
        await _block.Completion.ConfigureAwait(false);

        _logger.LogDebug("{ServiceName}: disposed channel executor for {Channel}; PendingCount={PendingCount}",
            SERVICE_NAME, _channel, PendingCount);
    }
}
