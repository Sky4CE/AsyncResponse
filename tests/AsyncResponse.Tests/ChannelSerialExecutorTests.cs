using Xunit;

namespace AsyncResponse.Tests;

public class ChannelSerialExecutorTests
{
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
