using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Runs a real multi-step flow through the explicit in-memory durable-flow store. Provider-backed
/// stores are covered by their package and integration contract suites.
/// </summary>
public class DurableFlowStoreMatrixTests
{
    [Fact]
    public Task InMemoryDurableStore_RunsFlowEndToEnd()
        => RunFlowAgainstStoreAsync();

    [Fact]
    public async Task InMemoryDurableStore_EnforcesAtomicRevisionAndLeaseContract()
    {
        var store = new InMemoryFlowStateStore();
        var state = NewState("atomic");

        Assert.True(await store.TryCreateAsync("atomic", state, TimeSpan.FromMinutes(1)));
        Assert.False(await store.TryCreateAsync("atomic", NewState("atomic"), TimeSpan.FromMinutes(1)));
        Assert.True(await store.TryAcquireLeaseAsync("atomic", "owner-a", TimeSpan.FromMinutes(1)));
        Assert.False(await store.TryAcquireLeaseAsync("atomic", "owner-b", TimeSpan.FromMinutes(1)));

        state.LastMessage = "checkpoint";
        state.Revision = 1;
        Assert.False(await store.TryUpdateAsync("atomic", state, 0, TimeSpan.FromMinutes(1), "owner-b"));
        Assert.True(await store.TryUpdateAsync("atomic", state, 0, TimeSpan.FromMinutes(1), "owner-a"));
        Assert.False(await store.TryUpdateAsync("atomic", state, 0, TimeSpan.FromMinutes(1), "owner-a"));
        Assert.Equal(1, (await store.LoadAsync("atomic"))!.Revision);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store.TryUpdateAsync("atomic", state, -1, TimeSpan.FromMinutes(1)));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.TryCreateAsync("atomic", NewState("other"), TimeSpan.FromMinutes(1)));
        var unrecognizedSchema = NewState("schema");
        unrecognizedSchema.SchemaVersion = FlowStateSchema.Current + 1;
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.TryCreateAsync("schema", unrecognizedSchema, TimeSpan.FromMinutes(1)));
    }

    private static async Task RunFlowAgainstStoreAsync()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<FlowProbe>();
        services.AddScoped<TestOnboardingFlow>();
        services.AddAsyncResponse()
            .WithInMemoryChannel(options =>
            {
                options.DefaultTimeout = TimeSpan.FromSeconds(10);
                options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
            })
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows();
        await using var provider = services.BuildServiceProvider();

        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<FlowProbe>();

        var flowId = await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new TestFlowInput(7));

        var run = executor.ExecuteAsync(flowId);
        var correlationId = await probe.TriggerFired.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Running, Message = "halfway" }, correlationId);
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, correlationId);
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        // Reload through the store (GetStateAsync is a raw store read — no in-process cache), so
        // these assertions prove the full serialize→envelope→deserialize round trip.
        var state = await flows.GetStateAsync(flowId);
        Assert.NotNull(state);
        Assert.Equal(FlowRunStatus.Succeeded, state!.Status);
        Assert.True(state.Steps!["compute-stamp"].Completed);
        Assert.Contains("107", state.Steps["compute-stamp"].ResultJson);
        Assert.True(state.Steps["remote-op"].Completed);
        Assert.Null(state.Steps["remote-op"].PendingCorrelationId);
        Assert.True(state.Steps["notify"].Completed);
        Assert.Contains("final-status", state.Values!.Keys);
        Assert.Equal(1, state.Attempts);

    }

    private static FlowState NewState(string flowId) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(TestOnboardingFlow).FullName,
        InputTypeName = typeof(TestFlowInput).FullName,
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };
}
