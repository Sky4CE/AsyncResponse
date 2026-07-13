using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The registry coordinates per-channel serial-executor lifecycle. Its core guarantee is that a
/// message handed to <see cref="SerialExecutorRegistry.Enqueue"/> is never dropped because the
/// executor it would have used is concurrently being retired — the lifecycle race the old
/// ConcurrentDictionary + fire-and-forget removal scheme was vulnerable to when a correlation id was
/// reused mid-drain.
/// </summary>
public class SerialExecutorRegistryTests
{
    [Fact]
    public async Task Enqueue_RunsWork()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        var ran = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        registry.Enqueue("cid", () => { ran.TrySetResult(); return Task.CompletedTask; });

        await ran.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await registry.RemoveAsync("cid");
    }

    [Fact]
    public async Task SingleChannel_NoRemoval_RunsInOrder()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        var order = new ConcurrentQueue<int>();

        for (var i = 0; i < 100; i++)
        {
            var index = i;
            registry.Enqueue("cid", async () => { await Task.Yield(); order.Enqueue(index); });
        }

        await registry.RemoveAsync("cid"); // drains

        Assert.Equal(Enumerable.Range(0, 100).ToArray(), order.ToArray());
    }

    [Fact]
    public async Task RemoveAsync_DrainsQueuedWork()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        var completed = 0;

        for (var i = 0; i < 20; i++)
            registry.Enqueue("cid", async () => { await Task.Delay(2); Interlocked.Increment(ref completed); });

        await registry.RemoveAsync("cid");

        Assert.Equal(20, completed);
    }

    [Fact]
    public async Task EnqueueAfterRemove_CreatesFreshExecutorAndRuns()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);

        registry.Enqueue("cid", () => Task.CompletedTask);
        await registry.RemoveAsync("cid");

        var ranAgain = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        registry.Enqueue("cid", () => { ranAgain.TrySetResult(); return Task.CompletedTask; });

        await ranAgain.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await registry.RemoveAsync("cid");
    }

    [Fact]
    public async Task ConcurrentEnqueueAndRemove_NeverLosesWork()
    {
        // Reproduces the correlation-id-reuse race: a producer keeps enqueuing while a remover keeps
        // retiring the channel's executor. Every enqueued item must still run exactly once — under
        // the lock each enqueue targets a live executor, and retiring drains what was queued.
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        var executed = 0;
        const int total = 500;

        var producer = Task.Run(() =>
        {
            for (var i = 0; i < total; i++)
                registry.Enqueue("cid", () => { Interlocked.Increment(ref executed); return Task.CompletedTask; });
        });

        var remover = Task.Run(async () =>
        {
            for (var i = 0; i < 60; i++)
            {
                await registry.RemoveAsync("cid");
                await Task.Yield();
            }
        });

        await Task.WhenAll(producer, remover);

        // Drain whatever executor is current after the interleaving settles.
        await registry.RemoveAsync("cid");

        Assert.Equal(total, executed);
    }

    [Fact]
    public async Task RemoveDuringBackpressure_DrainsBeforeReplacementWithoutOverlap()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var order = new ConcurrentQueue<int>();
        var active = 0;
        var overlap = 0;

        async Task RecordAsync(int value, Task? gate = null)
        {
            if (Interlocked.Increment(ref active) > 1)
                Interlocked.Exchange(ref overlap, 1);
            if (value == 1)
                firstStarted.TrySetResult();
            if (gate is not null)
                await gate;
            order.Enqueue(value);
            Interlocked.Decrement(ref active);
        }

        await registry.EnqueueAsync("cid", () => RecordAsync(1, releaseFirst.Task));
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Fill the default bounded queue while the first item is running, then leave one enqueue
        // waiting for capacity so retirement races the exact admission boundary.
        for (var index = 0; index < ChannelSerialExecutor.DefaultCapacity; index++)
            await registry.EnqueueAsync("cid", () => Task.CompletedTask);
        var beforeRetirement = registry.EnqueueAsync("cid", () => RecordAsync(2)).AsTask();
        Assert.False(beforeRetirement.IsCompleted);

        var retirement = registry.RemoveAsync("cid").AsTask();
        var afterRetirement = registry.EnqueueAsync("cid", () => RecordAsync(3)).AsTask();
        await Task.Delay(30);
        Assert.False(afterRetirement.IsCompleted);

        releaseFirst.TrySetResult();
        await Task.WhenAll(beforeRetirement, retirement, afterRetirement).WaitAsync(TimeSpan.FromSeconds(10));
        await registry.RemoveAsync("cid");

        Assert.Equal(0, overlap);
        Assert.Equal([1, 2, 3], order.ToArray());
    }

    [Fact]
    public async Task DifferentChannels_ExecuteInParallel_WhileEachChannelStaysSequential()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        const int channelCount = 4;
        const int perChannel = 6;

        // An async gate sized to the channel count: each channel's FIRST work item awaits it, and it
        // releases only once all channelCount first-items are running at the same time — which proves
        // distinct correlation ids execute in parallel. If the registry serialized across channels,
        // the gate would never reach its count and the await would time out (failing the test).
        var startedCount = 0;
        var allChannelsStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var perChannelConcurrent = new int[channelCount];
        var perChannelOverlap = new int[channelCount];
        var completionOrder = new ConcurrentQueue<int>[channelCount];
        for (var c = 0; c < channelCount; c++)
            completionOrder[c] = new ConcurrentQueue<int>();

        for (var c = 0; c < channelCount; c++)
        {
            var channel = c;
            for (var i = 0; i < perChannel; i++)
            {
                var index = i;
                registry.Enqueue($"cid-{channel}", async () =>
                {
                    // If two items for the SAME channel are ever active at once, record the violation.
                    if (Interlocked.Increment(ref perChannelConcurrent[channel]) > 1)
                        Interlocked.Exchange(ref perChannelOverlap[channel], 1);

                    if (index == 0)
                    {
                        if (Interlocked.Increment(ref startedCount) == channelCount)
                            allChannelsStarted.TrySetResult();
                        await allChannelsStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
                    }

                    await Task.Yield();
                    completionOrder[channel].Enqueue(index);
                    Interlocked.Decrement(ref perChannelConcurrent[channel]);
                });
            }
        }

        for (var c = 0; c < channelCount; c++)
            await registry.RemoveAsync($"cid-{c}");

        for (var c = 0; c < channelCount; c++)
        {
            Assert.Equal(0, perChannelOverlap[c]);  // never two at once within one correlation id
            Assert.Equal(Enumerable.Range(0, perChannel).ToArray(), completionOrder[c].ToArray()); // FIFO per cid
        }
    }
}
