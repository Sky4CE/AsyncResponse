using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The per-channel serial executor guarantees that progress/terminal messages for one correlation
/// id are processed one at a time and in FIFO order, and that a throwing work item never tears the
/// channel down.
/// </summary>
public class ChannelSerialExecutorTests
{
    [Fact]
    public async Task ExecutesWorkItemsSeriallyAndInOrder()
    {
        await using var executor = new ChannelSerialExecutor(NullLogger.Instance, "cid");
        var completionOrder = new ConcurrentQueue<int>();
        var concurrentlyActive = 0;
        var observedOverlap = 0;

        for (var i = 0; i < 50; i++)
        {
            var index = i;
            await executor.Enqueue(async () =>
            {
                if (Interlocked.Increment(ref concurrentlyActive) > 1)
                    Interlocked.Exchange(ref observedOverlap, 1);
                await Task.Delay(1);
                completionOrder.Enqueue(index);
                Interlocked.Decrement(ref concurrentlyActive);
            });
        }

        // DisposeAsync completes the block and awaits every queued work item.
        await executor.DisposeAsync();

        Assert.Equal(0, observedOverlap);                                       // never ran two at once
        Assert.Equal(Enumerable.Range(0, 50).ToArray(), completionOrder.ToArray()); // strict FIFO
    }

    [Fact]
    public async Task SwallowsWorkExceptions_AndKeepsProcessing()
    {
        await using var executor = new ChannelSerialExecutor(NullLogger.Instance, "cid");
        var ranAfterThrow = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await executor.Enqueue(() => throw new InvalidOperationException("boom"));
        await executor.Enqueue(() =>
        {
            ranAfterThrow.TrySetResult();
            return Task.CompletedTask;
        });

        await executor.DisposeAsync();

        Assert.True(ranAfterThrow.Task.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task EnqueueAfterDispose_ReturnsFalse()
    {
        var executor = new ChannelSerialExecutor(NullLogger.Instance, "cid");
        await executor.DisposeAsync();

        var accepted = await executor.Enqueue(() => Task.CompletedTask);

        Assert.False(accepted);
    }
}
