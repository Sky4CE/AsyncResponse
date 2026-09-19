using System.Text;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class LedgerBudgetTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void BudgetMustBePositive(int limit)
        => Assert.Throws<InvalidOperationException>(() => FlowStateConcurrency.ValidateOptions(new DurableFlowOptions { MaxRetainedSteps = limit }));

    [Fact]
    public async Task BudgetRejectsNewStepsBeforeSideEffects_AndReplaysExistingSteps()
    {
        var calls = new Calls();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.DurableFlows = o => o.MaxRetainedSteps = 2;
            options.ConfigureServices = s => s.AddSingleton(calls);
            options.ConfigureAsyncResponse = b => b.WithDurableFlow<BudgetFlow, int>();
        });
        var accepted = await harness.StartFlowAsync<BudgetFlow, int>(2, "at-budget");
        Assert.Equal(FlowRunStatus.Succeeded, await accepted.WaitForFinishedAsync());
        Assert.Equal(2, calls.Count);
        var state = (await accepted.GetStateAsync())!;
        state.Status = FlowRunStatus.Running;
        var revision = state.Revision++;
        Assert.True(await harness.Engine.Services.GetRequiredService<IFlowStateStore>()
            .TryUpdateAsync(state.FlowId!, state, revision, TimeSpan.FromDays(1)));
        await accepted.ExecuteDirectAsync();
        Assert.Equal(2, calls.Count); // Both existing checkpoints still replay at the cap.

        var rejected = await harness.StartFlowAsync<BudgetFlow, int>(3, "over-budget");
        Assert.Equal(FlowRunStatus.Failed, await rejected.WaitForFinishedAsync());
        Assert.Equal(4, calls.Count); // Third step never invoked.
        Assert.Equal(2, (await rejected.GetStateAsync())!.Steps!.Count);
        Assert.Contains("MaxRetainedSteps", (await rejected.GetStateAsync())!.LastMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DefaultBudget_StopsAt256StepsBeforeTheNextSideEffect()
    {
        var calls = new Calls();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(calls);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<BudgetFlow, int>();
        });
        var run = await harness.StartFlowAsync<BudgetFlow, int>(257);
        Assert.Equal(FlowRunStatus.Failed, await run.WaitForFinishedAsync());
        Assert.Equal(256, calls.Count);
        Assert.Equal(256, (await run.GetStateAsync())!.Steps!.Count);
    }

    [Fact]
    public async Task ExplicitOptOutAllowsLargerHistories()
    {
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.DurableFlows = o => o.MaxRetainedSteps = null;
            options.ConfigureServices = s => s.AddSingleton(new Calls());
            options.ConfigureAsyncResponse = b => b.WithDurableFlow<BudgetFlow, int>();
        });
        var run = await harness.StartFlowAsync<BudgetFlow, int>(257);
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
        Assert.Equal(257, (await run.GetStateAsync())!.Steps!.Count);
    }

    [Fact]
    public async Task BoundedChildTree_HasApproximatelyLinearCheckpointBytes()
    {
        var small = await MeasureAsync(64);
        var large = await MeasureAsync(128);
        Assert.InRange((double)large / small, 1.7, 2.5); // A monolithic history approaches 4x.
    }

    private static async Task<long> MeasureAsync(int steps)
    {
        var store = new CountingStore();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.DurableFlows = o => o.MaxRetainedSteps = 8;
            options.ConfigureAsyncResponse = b =>
            {
                b.Services.Replace(ServiceDescriptor.Singleton<IFlowStateStore>(store));
                b.WithDurableFlow<PartitionedFlow, int>();
            };
        });
        var run = await harness.StartFlowAsync<PartitionedFlow, int>(steps, "partitioned");
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
        Assert.InRange(store.MaxSteps, 1, 8);
        return store.Bytes;
    }

    public sealed class Calls { public int Count; }
    public sealed class BudgetFlow(Calls calls) : IDurableFlow<int>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, int steps)
        {
            for (var i = 0; i < steps; i++)
                await flow.StepAsync("step-" + i, () => { Interlocked.Increment(ref calls.Count); return Task.CompletedTask; });
        }
    }

    public sealed class PartitionedFlow : IDurableFlow<int>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, int steps)
        {
            if (steps > 8)
            {
                await flow.AwaitChildFlowAsync<PartitionedFlow, int>("left", steps / 2);
                await flow.AwaitChildFlowAsync<PartitionedFlow, int>("right", steps - steps / 2);
                return;
            }
            for (var i = 0; i < steps; i++)
                await flow.StepAsync("step-" + i, () => Task.FromResult(new string('x', 1024)));
        }
    }

    public sealed class CountingStore : IFlowStateStore
    {
        private readonly InMemoryFlowStateStore _inner = new();
        public long Bytes;
        public int MaxSteps;
        private readonly object _measureGate = new();
        private void Record(FlowState state)
        {
            Interlocked.Add(ref Bytes, Encoding.UTF8.GetByteCount(FlowStateJson.Serialize(state)));
            lock (_measureGate) MaxSteps = Math.Max(MaxSteps, state.Steps?.Count ?? 0);
        }
        public async Task<bool> TryCreateAsync(string id, FlowState state, TimeSpan ttl, CancellationToken ct = default)
        { var saved = await _inner.TryCreateAsync(id, state, ttl, ct); if (saved) Record(state); return saved; }
        public async Task<bool> TryUpdateAsync(string id, FlowState state, long revision, TimeSpan ttl, string? leaseId = null, CancellationToken ct = default)
        { var saved = await _inner.TryUpdateAsync(id, state, revision, ttl, leaseId, ct); if (saved) Record(state); return saved; }
        public Task<FlowState?> LoadAsync(string id, CancellationToken ct = default) => _inner.LoadAsync(id, ct);
        public Task<bool> TryAcquireLeaseAsync(string id, string leaseId, TimeSpan duration, CancellationToken ct = default) => _inner.TryAcquireLeaseAsync(id, leaseId, duration, ct);
        public Task<bool> TryRenewLeaseAsync(string id, string leaseId, TimeSpan duration, CancellationToken ct = default) => _inner.TryRenewLeaseAsync(id, leaseId, duration, ct);
        public Task ReleaseLeaseAsync(string id, string leaseId, CancellationToken ct = default) => _inner.ReleaseLeaseAsync(id, leaseId, ct);
        public Task<bool> TryDeleteAsync(string id, CancellationToken ct = default) => _inner.TryDeleteAsync(id, ct);
    }
}
