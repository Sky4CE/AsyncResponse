using AsyncResponse.DurableFlows;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The durable-flow store contract, shared by every store's test class. Each store package gets the
/// same assertions run against a real server, so the contract lives here rather than being restated
/// per store — and so store tests can be split across batches without duplicating it.
/// </summary>
internal static class FlowStoreContract
{
    internal static async Task EventuallyAsync(Func<Task> action, TimeSpan? budget = null)
    {
        var timeout = budget ?? TimeSpan.FromSeconds(30);
        var deadline = DateTime.UtcNow + timeout;
        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await action();
                return;
            }
            catch (Exception ex)
            {
                last = ex;
                await Task.Delay(500);
            }
        }

        throw new TimeoutException($"The backing store did not become ready within {timeout}.", last);
    }

    internal static async Task AssertStoreContractAsync(
        IFlowStateStore store,
        TimeSpan? expiryTtl = null,
        TimeSpan? expiryDelay = null)
    {
        var state = CreateState("flow-itest");

        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        var loaded = await store.LoadAsync(state.FlowId!);
        Assert.NotNull(loaded);
        Assert.Equal(FlowRunStatus.Running, loaded!.Status);
        Assert.True(loaded.Steps!["step-a"].Completed);

        state.Status = FlowRunStatus.Succeeded;
        state.LastMessage = "done";
        state.Revision = 1;
        Assert.True(await store.TryUpdateAsync(state.FlowId!, state, 0, TimeSpan.FromMinutes(5)));
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(state.FlowId!))!.Status);

        Assert.True(await store.TryCreateAsync("expired-flow", CreateState("expired-flow"), expiryTtl ?? TimeSpan.FromMilliseconds(1)));
        await Task.Delay(expiryDelay ?? TimeSpan.FromMilliseconds(30));
        Assert.Null(await store.LoadAsync("expired-flow"));

        var concurrent = store;
        var replacement = CreateState("expired-flow");
        Assert.True(await concurrent.TryCreateAsync("expired-flow", replacement, TimeSpan.FromMinutes(5)));
        Assert.NotNull(await store.LoadAsync("expired-flow"));
        Assert.True(await store.TryDeleteAsync("expired-flow"));

        var concurrentFlowId = $"flow-concurrency-{Guid.NewGuid():N}";
        var createResults = await Task.WhenAll(
            Enumerable.Range(0, 16)
                .Select(_ => concurrent.TryCreateAsync(
                    concurrentFlowId,
                    CreateState(concurrentFlowId),
                    TimeSpan.FromMinutes(5))));
        Assert.Single(createResults, static created => created);

        var concurrentState = await store.LoadAsync(concurrentFlowId);
        Assert.NotNull(concurrentState);
        Assert.Equal(0, concurrentState!.Revision);

        Assert.True(await concurrent.TryAcquireLeaseAsync(
            concurrentFlowId,
            "owner-a",
            TimeSpan.FromMinutes(1)));
        Assert.False(await concurrent.TryAcquireLeaseAsync(
            concurrentFlowId,
            "owner-b",
            TimeSpan.FromMinutes(1)));

        concurrentState.Status = FlowRunStatus.Succeeded;
        concurrentState.Revision = 1;
        Assert.False(await concurrent.TryUpdateAsync(
            concurrentFlowId,
            concurrentState,
            expectedRevision: 0,
            TimeSpan.FromMinutes(5),
            leaseId: "owner-b"));
        Assert.True(await concurrent.TryUpdateAsync(
            concurrentFlowId,
            concurrentState,
            expectedRevision: 0,
            TimeSpan.FromMinutes(5),
            leaseId: "owner-a"));
        Assert.False(await concurrent.TryUpdateAsync(
            concurrentFlowId,
            concurrentState,
            expectedRevision: 0,
            TimeSpan.FromMinutes(5),
            leaseId: "owner-a"));
        Assert.Equal(1, (await store.LoadAsync(concurrentFlowId))!.Revision);

        Assert.False(await concurrent.TryRenewLeaseAsync(
            concurrentFlowId,
            "owner-b",
            TimeSpan.FromMinutes(1)));
        Assert.True(await concurrent.TryRenewLeaseAsync(
            concurrentFlowId,
            "owner-a",
            TimeSpan.FromMinutes(1)));
        await concurrent.ReleaseLeaseAsync(concurrentFlowId, "owner-b");
        Assert.False(await concurrent.TryAcquireLeaseAsync(
            concurrentFlowId,
            "owner-b",
            TimeSpan.FromMinutes(1)));
        await concurrent.ReleaseLeaseAsync(concurrentFlowId, "owner-a");
        Assert.True(await concurrent.TryAcquireLeaseAsync(
            concurrentFlowId,
            "owner-b",
            TimeSpan.FromMinutes(1)));
        await concurrent.ReleaseLeaseAsync(concurrentFlowId, "owner-b");
        Assert.True(await store.TryDeleteAsync(concurrentFlowId));

        Assert.True(await store.TryDeleteAsync(state.FlowId!));
        Assert.Null(await store.LoadAsync(state.FlowId!));
        Assert.False(await store.TryDeleteAsync(state.FlowId!));
    }

    internal static FlowState CreateState(string flowId)
        => new()
        {
            FlowId = flowId,
            FlowTypeName = typeof(FlowStoreContract).FullName,
            InputTypeName = typeof(int).FullName,
            InputJson = JsonSerializer.Serialize(7),
            Status = FlowRunStatus.Running,
            LastMessage = "started",
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow,
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
            {
                ["step-a"] = new() { Completed = true, ResultJson = "123", CompletedAtUtc = DateTime.UtcNow }
            }
        };

    internal static string NewIdentifier(string prefix, int maxLength)
    {
        var identifier = $"{prefix}_{Guid.NewGuid():N}";
        return identifier.Length <= maxLength ? identifier : identifier[..maxLength];
    }
}
