using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using System.Collections;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// Round-29 review regressions in the durable-flow engine: the claimed-but-unrecorded response a
// lost lease used to discard, and the parent ledger TTL that shrank below an already-parked
// child's own window.
// ---------------------------------------------------------------------------------------------

public sealed record R29Input(string Name);

/// <summary>Awaits one step behind a gated predicate, so a test can hold a delivered response in flight.</summary>
public sealed class R29GatedAwaitFlow : IDurableFlow<R29Input>
{
    public static TaskCompletionSource EnteredExecution = NewSource();
    public static TaskCompletionSource InsidePredicate = NewSource();
    public static TaskCompletionSource ReleasePredicate = NewSource();
    public static int Triggers;
    public static string? Received;

    public async Task ExecuteAsync(IDurableFlowContext flow, R29Input input)
    {
        EnteredExecution.TrySetResult();
        var result = await flow.AwaitStepAsync<OperationResult>(
            "remote",
            trigger: _ =>
            {
                Interlocked.Increment(ref Triggers);
                return Task.CompletedTask;
            },
            until: async response =>
            {
                InsidePredicate.TrySetResult();
                await ReleasePredicate.Task;
                return response.Status != OperationStatus.Running;
            },
            timeout: TimeSpan.FromDays(1));

        Received = result.Message;
    }

    public static void Reset()
    {
        EnteredExecution = NewSource();
        InsidePredicate = NewSource();
        ReleasePredicate = NewSource();
        Triggers = 0;
        Received = null;
    }

    public static TaskCompletionSource NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

/// <summary>Parks a two-day timer; the parent below suspends for it.</summary>
public sealed class R29ChildFlow : IDurableFlow<R29Input>
{
    public static readonly TimeSpan Nap = TimeSpan.FromDays(2);

    public Task ExecuteAsync(IDurableFlowContext flow, R29Input input)
        => flow.DelayAsync("child-nap", Nap);
}

public sealed class R29ParentFlow : IDurableFlow<R29Input>
{
    public Task ExecuteAsync(IDurableFlowContext flow, R29Input input)
        => flow.AwaitChildFlowAsync<R29ChildFlow, R29Input>("the-child", input);
}

public sealed class FlowRegressionRound29Tests
{
    [Fact]
    public async Task LostLeaseWhileHoldingAClaimedResponse_CheckpointsItInsteadOfDroppingIt()
    {
        // Regression (round 29): AwaitStepCoreAsync ran ThrowIfLost at the TOP of its cancellation
        // catch, while the waiter still held a response the channel had already claimed and acked.
        // The payload was dropped with no checkpoint and no re-publish, PendingCorrelationId stayed
        // set, and the redelivered execution re-attached to a correlation id that could never be
        // answered again — one lost response plus one duplicate remote request. A lost lease cannot
        // write lease-fenced, so the payload is now persisted through the lease-less
        // compare-and-swap the recovery path already uses; only then is the takeover raised.
        R29GatedAwaitFlow.Reset();
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<R29GatedAwaitFlow, R29Input>();
            // The disposal drain must outlast the gate this test holds: the harness's virtual clock
            // is free to run while the predicate is parked, and a lapsed drain budget takes the
            // (equally deliberate) indeterminate-delivery path instead of the one under test.
            options.ConfigureServices = services => services.Configure<InMemoryAsyncResponseOptions>(
                channel => channel.DisposalDrainTimeout = TimeSpan.FromDays(1));
            options.DurableFlows = flows =>
            {
                flows.ExecutionLeaseDuration = TimeSpan.FromSeconds(30);
                flows.ExecutionLeaseRenewInterval = TimeSpan.FromSeconds(5);
            };
        });
        await using var _ = harness;

        var run = await harness.StartFlowAsync<R29GatedAwaitFlow, R29Input>(new R29Input("acme"));
        await R29GatedAwaitFlow.EnteredExecution.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var awaiting = await run.WaitForAwaitingStepAsync("remote");
        Assert.Equal(1, Volatile.Read(ref R29GatedAwaitFlow.Triggers));

        // The remote answers. The channel claims and ACKs the response, then parks inside the Until
        // predicate: from here on the payload exists nowhere but this waiter. (Not awaited — the
        // in-memory channel's publish runs the predicate inline and would not return until it does.)
        var publish = harness.Engine.PublishAsync(
            new OperationResult { Status = OperationStatus.Completed, Message = "answered-once" },
            awaiting);
        await R29GatedAwaitFlow.InsidePredicate.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // A peer takes the flow over while the response is in flight: the store stops honoring this
        // lease, and the clock moves past the renewal so the execution actually notices.
        Store(harness).ExpireAllLeases();
        harness.Clock.Advance(TimeSpan.FromSeconds(45));
        await Task.Delay(500);

        // ...and only now does the predicate return, handing over a response whose lease is gone.
        R29GatedAwaitFlow.ReleasePredicate.TrySetResult();
        await publish.WaitAsync(TimeSpan.FromSeconds(20));

        // Poll the ledger on REAL time: the payload must be there, written by the lease-less
        // compare-and-swap rather than discarded with the takeover.
        var state = await Eventually(async () =>
        {
            var loaded = await run.GetStateAsync();
            return loaded?.Steps is { } steps && steps.TryGetValue("remote", out var found) && found.Completed
                ? loaded
                : null;
        });

        Assert.NotNull(state);
        // The marker only this path writes: proof the lease really was lost while the response was
        // held, not that the wait simply completed normally.
        Assert.Contains("after the execution lease was lost", state!.LastMessage!, StringComparison.Ordinal);

        var step = state.Steps!["remote"];
        Assert.Null(step.PendingCorrelationId);
        Assert.Contains("answered-once", step.ResultJson!, StringComparison.Ordinal);

        // ...and a replay resumes from it: the remote is never asked twice.
        await run.ExecuteDirectAsync();
        Assert.Equal(FlowRunStatus.Succeeded, (await run.GetStateAsync())!.Status);
        Assert.Equal(1, Volatile.Read(ref R29GatedAwaitFlow.Triggers));
        Assert.Equal("answered-once", R29GatedAwaitFlow.Received);
    }

    [Fact]
    public async Task ParentReSuspendingForAnAlreadyParkedChild_KeepsItsLedgerPastTheChildsWindow()
    {
        // Regression (round 29): SuspendForChildAsync saved with the plain StateExpiry, which
        // SHRANK a ledger the child had already extended through ExtendAncestorLedgersAsync — and
        // nothing re-extends it while the child stays parked in-process under a live lease, because
        // that rescue enqueue is acked as redundant and the child never replays. The parent's row
        // then expired mid-park and the child's completion wake-up found no state: the parent run,
        // and every step after it, silently lost.
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureAsyncResponse = builder => builder
                .WithDurableFlow<R29ParentFlow, R29Input>()
                .WithDurableFlow<R29ChildFlow, R29Input>();
            options.DurableFlows = flows => flows.StateExpiry = TimeSpan.FromHours(1);
        });
        await using var _ = harness;

        var run = await harness.StartFlowAsync<R29ParentFlow, R29Input>(new R29Input("acme"));
        await harness.Engine.WaitForWorkerIdleAsync();

        // A redelivery of the parent while the child is already parked on its two-day timer: the
        // re-suspension must cover the CHILD's remaining window, not the parent's idle margin.
        await harness.Engine.FlowExecutor.ExecuteAsync(run.FlowId);

        var now = harness.Clock.GetUtcNow().UtcDateTime;
        Assert.True(
            LedgerExpiry(harness, run.FlowId) > now + R29ChildFlow.Nap,
            $"the re-suspended parent's ledger expires in {LedgerExpiry(harness, run.FlowId) - now}; "
                + $"it must outlive the child's remaining {R29ChildFlow.Nap} park.");

        // And end to end: the child's wake-up still finds the parent alive.
        await harness.AdvanceAsync(R29ChildFlow.Nap + TimeSpan.FromMinutes(1));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    private static async Task<T?> Eventually<T>(Func<Task<T?>> probe) where T : class
    {
        var deadline = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(20);
        while (TimeProvider.System.GetUtcNow() < deadline)
        {
            if (await probe() is { } value)
                return value;

            await Task.Delay(25);
        }

        return null;
    }

    private static InMemoryFlowStateStore Store(FlowTestHarness harness)
        => harness.Engine.Services.GetRequiredService<InMemoryFlowStateStore>();

    private static DateTime LedgerExpiry(FlowTestHarness harness, string flowId)
    {
        var store = Store(harness);
        var entries = (IDictionary)store.GetType()
            .GetField("_entries", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(store)!;
        var entry = entries[flowId];
        Assert.NotNull(entry);
        return (DateTime)entry!.GetType().GetProperty("ExpiresAtUtc")!.GetValue(entry)!;
    }
}
