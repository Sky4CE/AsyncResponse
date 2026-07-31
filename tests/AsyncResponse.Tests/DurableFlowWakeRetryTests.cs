using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Linq.Expressions;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression tests for the at-most-once wake-up hole: a worker-job delivery that found the
/// execution lease held used to be acked and dropped immediately, so a wake delivered during a
/// DEAD holder's unexpired lease window was lost forever and the Running flow stranded. The
/// executor now retries the acquire for a full lease window (a dead holder's lease must expire
/// within it), and only proves-alive holders let the delivery ack as a duplicate.
/// </summary>
public sealed class DurableFlowWakeRetryTests
{
    [Fact]
    public async Task ExecuteAsync_LeaseHeldByDeadHolder_RetriesAcquireAndRunsFlowToCompletion()
    {
        var store = new InMemoryFlowStateStore();
        var state = RunnableState("dead-holder-flow");
        await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));

        // Simulate a holder that died mid-window: the lease is held but never renewed.
        Assert.True(await store.TryAcquireLeaseAsync(state.FlowId!, "dead-holder", TimeSpan.FromMilliseconds(300)));

        await using var harness = CreateHarness(store, new DurableFlowOptions
        {
            ExecutionLeaseDuration = TimeSpan.FromMilliseconds(300),
            ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(100)
        });

        // The redelivered wake must wait out the dead holder's lease and then run the flow itself
        // instead of acking as a duplicate.
        await harness.Executor.ExecuteAsync(state.FlowId!).WaitAsync(TimeSpan.FromSeconds(10));

        var final = await store.LoadAsync(state.FlowId!);
        Assert.Equal(FlowRunStatus.Succeeded, final!.Status);
    }

    [Fact]
    public async Task ExecuteAsync_LeaseHeldByLiveRenewingHolder_GivesUpAfterCeilingWithoutExecuting()
    {
        var store = new InMemoryFlowStateStore();
        var state = RunnableState("live-holder-flow");
        await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));

        // A live holder: keeps renewing its lease the whole time the waiter polls.
        Assert.True(await store.TryAcquireLeaseAsync(state.FlowId!, "live-holder", TimeSpan.FromMilliseconds(300)));
        using var renewals = new CancellationTokenSource();
        var renewLoop = Task.Run(async () =>
        {
            while (!renewals.IsCancellationRequested)
            {
                await store.TryRenewLeaseAsync(state.FlowId!, "live-holder", TimeSpan.FromMilliseconds(300));
                await Task.Delay(50);
            }
        });

        await using var harness = CreateHarness(store, new DurableFlowOptions
        {
            ExecutionLeaseDuration = TimeSpan.FromMilliseconds(300),
            ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(100)
        });

        var waited = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            // A lease that stays held for duration + renew interval proves the holder alive; the
            // delivery is then a true duplicate and must ack without executing the flow.
            await harness.Executor.ExecuteAsync(state.FlowId!).WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            renewals.Cancel();
            await renewLoop;
        }
        waited.Stop();

        // The give-up must come AFTER the full proving window (duration 300ms + renew 100ms), not
        // instantly — an instant return is the old at-most-once hole this file guards against.
        Assert.True(
            waited.Elapsed >= TimeSpan.FromMilliseconds(300),
            $"Gave up after {waited.ElapsedMilliseconds}ms; expected to poll through the ~400ms lease-proving window.");

        var final = await store.LoadAsync(state.FlowId!);
        Assert.Equal(FlowRunStatus.Running, final!.Status);
        Assert.Equal(0, final.Attempts);
    }

    [Fact]
    public async Task RecoverAsync_DuplicateDeliveryWithNoPendingStep_StillReenqueuesRunningFlow()
    {
        // Crash window: a previous delivery checkpointed the response but died before enqueueing
        // the run. The redelivered response finds no matching pending correlation id — it must
        // still re-enqueue execution or the Running flow strands.
        var store = new InMemoryFlowStateStore();
        var state = RunnableState("recover-crash-window");
        state.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
        {
            ["step"] = new FlowStepState { Completed = true, ResultJson = "{}" }
        };
        await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));
        await using var harness = CreateHarness(store, new DurableFlowOptions());

        await harness.Executor.RecoverAsync(state.FlowId!, new OperationResult(), "consumed-cid");

        harness.Builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task RecoverAsync_DuplicateDeliveryForTerminalFlow_DoesNotReenqueue()
    {
        var store = new InMemoryFlowStateStore();
        var state = RunnableState("recover-terminal");
        state.Status = FlowRunStatus.Succeeded;
        await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));
        await using var harness = CreateHarness(store, new DurableFlowOptions());

        await harness.Executor.RecoverAsync(state.FlowId!, new OperationResult(), "consumed-cid");

        harness.Builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    private static FlowState RunnableState(string flowId)
        => new()
        {
            FlowId = flowId,
            FlowTypeName = typeof(NoopFlow).FullName,
            InputTypeName = typeof(TestFlowInput).FullName,
            InputJson = JsonSerializer.Serialize(new TestFlowInput(1)),
            Status = FlowRunStatus.Running,
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow
        };

    private static Harness CreateHarness(IFlowStateStore store, DurableFlowOptions options)
    {
        var services = new ServiceCollection();
        services.AddSingleton(store);
        services.AddSingleton<NoopFlow>();
        var provider = services.BuildServiceProvider();
        var builder = new Mock<IAsyncResponseBuilder>();
        builder.Setup(instance => instance.EnqueueWorkerAsync(
                It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var executor = new DurableFlowExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            builder.Object,
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            new AsyncResponseContextPropagation([]),
            options,
            NullLogger<DurableFlowExecutor>.Instance);
        return new Harness(provider, executor, builder);
    }

    private sealed class Harness(
        ServiceProvider provider,
        DurableFlowExecutor executor,
        Mock<IAsyncResponseBuilder> builder) : IAsyncDisposable
    {
        public DurableFlowExecutor Executor => executor;
        public Mock<IAsyncResponseBuilder> Builder => builder;

        public ValueTask DisposeAsync() => provider.DisposeAsync();
    }

    public sealed class NoopFlow : IDurableFlow<TestFlowInput>
    {
        public Task ExecuteAsync(IDurableFlowContext context, TestFlowInput input) => Task.CompletedTask;
    }
}
