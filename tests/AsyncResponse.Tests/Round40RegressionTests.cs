using Xunit;
using static AsyncResponse.Tests.Round40FlowLeaseTestSupport;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 40 (2026-09-19) regressions: a wake-up acknowledged on the waiter's OWN lease window
/// while a crashed owner's longer lease was still unexpired. Written against the pre-existing
/// API only, so this file compiles — and fails — on ca58de0; the database-channel and Redis pins
/// of the same round sit next to their harnesses (<see cref="DbChannelSharedCoverageTests"/>,
/// <see cref="RedisRecoveryStateStoreTests"/>).
/// </summary>
public sealed class Round40RegressionTests
{
    // ---------------------------------------------------------------------------------------
    // F1 (HIGH): the executor polled a held lease for ITS OWN ExecutionLeaseDuration +
    // ExecutionLeaseRenewInterval and then returned "executing on another live worker", which
    // acknowledges the delivery. A lease issued by a previous deployment with a longer duration
    // outlives that window without anyone renewing it, so a successor configured with a shorter
    // lease acknowledged the run's only redelivery and the run stayed Running, Attempts = 0,
    // forever — even after the old lease expired.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task ShorterLeaseSuccessor_WaitsOutACrashedOwnersLongerLease_InsteadOfAckingTheOnlyWakeUp()
    {
        var store = new InMemoryFlowStateStore();
        var state = RunnableState("r40-crashed-owner-longer-lease");
        await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));

        // The previous deployment's worker acquired a 3 s lease and died: nobody renews it.
        Assert.True(await store.TryAcquireLeaseAsync(state.FlowId!, "crashed-owner", TimeSpan.FromSeconds(3)));

        // The successor deployment shortened the lease: its whole window is 250 ms.
        await using var harness = CreateHarness(store, new DurableFlowOptions
        {
            ExecutionLeaseDuration = TimeSpan.FromMilliseconds(200),
            ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(50)
        });

        // The redelivered wake-up is the only one the run will ever get. It must outlast the
        // crashed owner's PERSISTED lease and then execute the flow — not return early.
        await harness.Executor.ExecuteAsync(state.FlowId!).WaitAsync(TimeSpan.FromSeconds(30));

        var final = await store.LoadAsync(state.FlowId!);
        Assert.Equal(FlowRunStatus.Succeeded, final!.Status);
        Assert.Equal(1, final.Attempts);
        Assert.Equal(1, harness.Flow.Executions);
    }
}
