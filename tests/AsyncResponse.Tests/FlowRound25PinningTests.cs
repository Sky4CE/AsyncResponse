using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// Round-25 behavior pins that exercise the NEW surface directly (the persisted await deadline,
// the one-shot skew marker) — kept apart from FlowRegressionRound25Tests so that file still
// compiles against pre-fix code for red-on-old verification.
// ---------------------------------------------------------------------------------------------

public sealed record R25PinInput(string Name);

/// <summary>Awaits without a timeout, so the channel's default resolves the window.</summary>
public sealed class R25TimeoutlessWaitFlow : IDurableFlow<R25PinInput>
{
    public Task ExecuteAsync(IDurableFlowContext flow, R25PinInput input)
        => flow.AwaitStepAsync<OperationResult>("slow", trigger: _ => Task.CompletedTask);
}

/// <summary>Awaits with an explicit 30-day timeout.</summary>
public sealed class R25ExplicitWaitFlow : IDurableFlow<R25PinInput>
{
    public Task ExecuteAsync(IDurableFlowContext flow, R25PinInput input)
        => flow.AwaitStepAsync<OperationResult>("slow", trigger: _ => Task.CompletedTask, timeout: TimeSpan.FromDays(30));
}

public sealed class FlowRound25PinningTests
{
    [Fact]
    public async Task ExplicitTimeout_PersistsTheAwaitDeadlineWithTheBreadcrumb()
    {
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<R25ExplicitWaitFlow, R25PinInput>());
        await using var _ = harness;

        var armedAt = harness.Clock.GetUtcNow().UtcDateTime;
        var run = await harness.StartFlowAsync<R25ExplicitWaitFlow, R25PinInput>(new R25PinInput("acme"));
        await run.WaitForAwaitingStepAsync("slow");

        var state = await run.GetStateAsync();
        Assert.Equal(armedAt + TimeSpan.FromDays(30), state!.Steps!["slow"].AwaitDeadlineUtc);
    }

    [Fact]
    public async Task TimeoutlessWait_PersistsTheChannelDefaultAsItsDeadline()
    {
        // The channel arms DefaultTimeout ?? RecoveryStateExpiry on the waiter; the ledger now
        // records the same window, so a redelivered execution arms the remainder instead of a
        // fresh full default.
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<R25TimeoutlessWaitFlow, R25PinInput>());
        await using var _ = harness;

        var armedAt = harness.Clock.GetUtcNow().UtcDateTime;
        var run = await harness.StartFlowAsync<R25TimeoutlessWaitFlow, R25PinInput>(new R25PinInput("acme"));
        await run.WaitForAwaitingStepAsync("slow");

        var channelOptions = harness.Engine.Services.GetRequiredService<IOptions<InMemoryAsyncResponseOptions>>().Value;
        var effectiveDefault = channelOptions.DefaultTimeout ?? channelOptions.RecoveryStateExpiry;

        var state = await run.GetStateAsync();
        Assert.Equal(armedAt + effectiveDefault, state!.Steps!["slow"].AwaitDeadlineUtc);
    }

    [Fact]
    public void SkewMarker_IsClaimedExactlyOnce_AndRestoredByTheScope()
    {
        Assert.False(WorkerJobSkewScope.IsForcedEarlyExecution);
        Assert.False(WorkerJobSkewScope.TryConsumeForcedEarlyExecution());

        using (WorkerJobSkewScope.Enter())
        {
            Assert.True(WorkerJobSkewScope.IsForcedEarlyExecution);

            // The first timer step to claim the marker wins it; every later reader on the same
            // job sees it spent.
            Assert.True(WorkerJobSkewScope.TryConsumeForcedEarlyExecution());
            Assert.False(WorkerJobSkewScope.IsForcedEarlyExecution);
            Assert.False(WorkerJobSkewScope.TryConsumeForcedEarlyExecution());
        }

        Assert.False(WorkerJobSkewScope.IsForcedEarlyExecution);
    }
}
