using Xunit;

namespace AsyncResponse.Tests;

public class ChannelSerialExecutorTests
{
    [Fact]
    public void Constructor_RejectsNonPositiveCapacity()
        => Assert.Throws<ArgumentOutOfRangeException>(
            () => new ChannelSerialExecutor(new TestLogger(), "responses", capacity: 0));

    [Fact]
    public async Task Executor_RunsQueuedWorkSeriallySwallowsFailuresAndRejectsAfterDispose()
    {
        var executor = new ChannelSerialExecutor(new TestLogger(), "responses");
        var calls = new List<string>();

        Assert.True(await executor.Enqueue(() =>
        {
            calls.Add("first");
            return Task.CompletedTask;
        }));
        Assert.True(executor.TryEnqueue(() => throw new InvalidOperationException("work failed")));
        Assert.True(await executor.Enqueue(() =>
        {
            calls.Add("second");
            return Task.CompletedTask;
        }));

        await Eventually(() => calls.Count == 2);
        await executor.DisposeAsync();

        Assert.False(executor.TryEnqueue(() => Task.CompletedTask));
        Assert.Equal(["first", "second"], calls);
    }

    [Fact]
    public async Task Enqueue_WithCanceledToken_ReturnsCanceledTask()
    {
        await using var executor = new ChannelSerialExecutor(new TestLogger(), "responses");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var task = executor.Enqueue(() => Task.CompletedTask, cts.Token);

        Assert.True(task.IsCanceled);
        await Assert.ThrowsAsync<TaskCanceledException>(() => task);
    }

    [Fact]
    public async Task EnqueueAfterDispose_ReturnsFalse()
    {
        var executor = new ChannelSerialExecutor(new TestLogger(), "responses");
        await executor.DisposeAsync();

        Assert.False(await executor.Enqueue(() => Task.CompletedTask));
    }

    [Fact]
    public async Task CancellationWhileWaitingForCapacity_PropagatesAndLeavesExecutorUsable()
    {
        var executor = new ChannelSerialExecutor(new TestLogger(), "responses", capacity: 1);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Assert.True(await executor.Enqueue(async () =>
        {
            started.TrySetResult();
            await release.Task;
        }));
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.True(await executor.Enqueue(() => Task.CompletedTask));
        using var cancellation = new CancellationTokenSource();
        var blocked = executor.Enqueue(() => Task.CompletedTask, cancellation.Token);
        await Task.Delay(20);
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blocked);
        release.TrySetResult();
        await executor.DisposeAsync();
    }

    [Fact]
    public async Task BoundedQueue_AppliesBackpressureAndStillRunsEveryAcceptedItem()
    {
        var executor = new ChannelSerialExecutor(new TestLogger(), "responses", capacity: 1);
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = new List<int>();

        Assert.True(await executor.Enqueue(async () =>
        {
            firstStarted.TrySetResult();
            await releaseFirst.Task;
            calls.Add(1);
        }));
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.True(await executor.Enqueue(() =>
        {
            calls.Add(2);
            return Task.CompletedTask;
        }));

        var third = executor.Enqueue(() =>
        {
            calls.Add(3);
            return Task.CompletedTask;
        });
        await Task.Delay(30);
        Assert.False(third.IsCompleted);
        Assert.False(executor.TryEnqueue(() => Task.CompletedTask));

        releaseFirst.TrySetResult();
        Assert.True(await third.WaitAsync(TimeSpan.FromSeconds(2)));
        await executor.DisposeAsync();

        Assert.Equal([1, 2, 3], calls);
    }

    [Fact]
    public async Task ThrowingLoggingProvider_DoesNotEndTheReaderLoop()
    {
        // Microsoft.Extensions.Logging rethrows a provider's failure. Pre-fix the loop's error log
        // (and its Debug lines) sat outside any guard: one failing item plus a provider that throws
        // on Error ended the reader loop, and every item queued behind it — the correlation id's
        // later deliveries, its waiter's drain marker — was accepted into a queue nothing read.
        var logger = new CollectingLogger { ThrowOnMessageContaining = "Channel executor error" };
        var executor = new ChannelSerialExecutor(logger, "responses");
        var secondRan = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Assert.True(executor.TryEnqueue(() => throw new InvalidOperationException("work failed")));
        Assert.True(executor.TryEnqueue(() =>
        {
            secondRan.TrySetResult();
            return Task.CompletedTask;
        }));

        await secondRan.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await executor.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task ThrowingLoggingProvider_AtDebug_NeitherFailsAnAcceptedEnqueueNorSkipsTheDrain()
    {
        // Every Debug line throws: an accepted item must still read as accepted (not a failed
        // enqueue the caller retries or faults), and disposal must still complete the writer —
        // pre-fix its opening Debug line ran first, and throwing there left the loop parked.
        var logger = new CollectingLogger { ThrowOnMessageContaining = "channel executor" };
        var executor = new ChannelSerialExecutor(logger, "responses");
        var ran = 0;

        Assert.True(await executor.Enqueue(() =>
        {
            Interlocked.Increment(ref ran);
            return Task.CompletedTask;
        }));
        Assert.True(executor.TryEnqueue(() =>
        {
            Interlocked.Increment(ref ran);
            return Task.CompletedTask;
        }));

        await executor.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(2, Volatile.Read(ref ran));
        Assert.False(executor.TryEnqueue(() => Task.CompletedTask));
    }

    private static async Task Eventually(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;

            await Task.Delay(10);
        }

        Assert.True(condition());
    }
}
