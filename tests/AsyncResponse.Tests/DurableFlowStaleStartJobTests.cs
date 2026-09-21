using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using System.Linq.Expressions;
using Xunit;
using static AsyncResponse.Tests.Round40FlowLeaseTestSupport;

namespace AsyncResponse.Tests;

/// <summary>
/// The start job carries the whole initial ledger and creates the run when none exists
/// (insert-if-absent) — which also holds long after the run finished and its ledger expired. A
/// dead-letter replay, or a Kafka consumer group rewound past the message, then "starts" the run
/// again and re-executes every completed step. A start older than the ledger lifetime with no
/// ledger behind it is dropped instead; a run that is still alive keeps the job as its wake-up.
/// </summary>
public sealed class DurableFlowStaleStartJobTests
{
    private static readonly DurableFlowOptions Options = new() { StateExpiry = TimeSpan.FromDays(14) };

    [Fact]
    public async Task ReplayedStartJob_OlderThanStateExpiry_WithNoLedger_IsDroppedLoudly_NotReExecuted()
    {
        var store = new InMemoryFlowStateStore();
        var log = new CapturingLogger<DurableFlowExecutor>();
        await using var harness = CreateHarness(store, log);
        var initial = RunnableState("stale-start-replayed");
        initial.CreatedAtUtc = initial.UpdatedAtUtc = DateTime.UtcNow.AddDays(-30);

        // Returns normally: the transport acknowledges the replayed job.
        await harness.Executor.CreateAndExecuteAsync(initial.FlowId!, FlowStateJson.Serialize(initial));

        Assert.Equal(0, harness.Flow.Executions);
        Assert.Null(await store.LoadAsync(initial.FlowId!));
        var dropped = Assert.Single(log.Entries, entry => entry.Level == LogLevel.Error);
        Assert.Contains("stale-start-replayed", dropped.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(DurableFlowOptions.StateExpiry), dropped.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StartJob_OlderThanStateExpiry_WhoseRunIsStillAlive_StillExecutesIt()
    {
        // A run parked past StateExpiry keeps its ledger through the retention floor, and the
        // start job — unacknowledged while its handler ran — is the wake-up the broker redelivers
        // when that handler dies. Age alone must never drop it.
        var store = new InMemoryFlowStateStore();
        await using var harness = CreateHarness(store, new CapturingLogger<DurableFlowExecutor>());
        var initial = RunnableState("stale-start-live-run");
        initial.CreatedAtUtc = initial.UpdatedAtUtc = DateTime.UtcNow.AddDays(-30);
        await store.TryCreateAsync(initial.FlowId!, initial, TimeSpan.FromDays(14));

        await harness.Executor.CreateAndExecuteAsync(initial.FlowId!, FlowStateJson.Serialize(initial));

        Assert.Equal(1, harness.Flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(initial.FlowId!))!.Status);
    }

    [Theory]
    [InlineData(true)]    // a start within the ledger lifetime: the starter died before its own create
    [InlineData(false)]   // a carrier without the stamp: nothing to measure, never judged
    public async Task StartJob_ThatCannotBeProvenStale_StillCreatesTheRunFromItsCarrier(bool stamped)
    {
        var store = new InMemoryFlowStateStore();
        await using var harness = CreateHarness(store, new CapturingLogger<DurableFlowExecutor>());
        var initial = RunnableState($"fresh-start-{stamped}");
        initial.CreatedAtUtc = stamped ? DateTime.UtcNow.AddDays(-13) : null;

        await harness.Executor.CreateAndExecuteAsync(initial.FlowId!, FlowStateJson.Serialize(initial));

        Assert.Equal(1, harness.Flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(initial.FlowId!))!.Status);
    }

    private static ExecutorHarness CreateHarness(IFlowStateStore store, ILogger<DurableFlowExecutor> log)
    {
        var services = new ServiceCollection();
        services.AddSingleton(store);
        services.AddSingleton<CountingFlow>();
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
            Options,
            log);
        return new ExecutorHarness(provider, executor);
    }
}
