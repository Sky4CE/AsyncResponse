using System.Diagnostics;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AsyncResponse.Tests;

public sealed record ParkedRestartInput(string Name);

/// <summary>Parks on an awaited step: holds its worker slot until the test replies.</summary>
public sealed class ParkedOnReplyFlow : IDurableFlow<ParkedRestartInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ParkedRestartInput input)
        => await flow.AwaitStepAsync<OperationResult>("remote", trigger: _ => Task.CompletedTask);
}

/// <summary>Parks on an in-process timer (at or below the threshold): holds its worker slot until the clock moves.</summary>
public sealed class ParkedOnTimerFlow : IDurableFlow<ParkedRestartInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ParkedRestartInput input)
        => await flow.DelayAsync("nap", TimeSpan.FromSeconds(5));
}

/// <summary>
/// An engine-owned park — an awaited step, an in-process timer — can only end when the test
/// replies or advances the virtual clock, and neither happens while the test is awaiting the
/// restart (or the disposal). The old incarnation's graceful stop used to wait on it anyway and
/// burned the whole real-time guard (10 s by default) per restart and again per disposal, in a
/// kit whose point is that nothing sleeps for real. A parked execution is now abandoned for
/// takeover the moment it is all that is left, exactly as the lapsed stop always ended.
/// </summary>
public sealed class TestingHarnessParkedRestartTests
{
    // Half the default guard: the old code needed all of it (≈10 s), the fix needs milliseconds,
    // and the gap between them absorbs any amount of CI scheduling noise.
    private static readonly TimeSpan _bound = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task SimulateRestart_WithAFlowParkedOnAnAwaitedStep_DoesNotWaitOutTheRealTimeGuard()
    {
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ParkedOnReplyFlow, ParkedRestartInput>());
        await using var _ = harness;

        var run = await harness.StartFlowAsync<ParkedOnReplyFlow, ParkedRestartInput>(new ParkedRestartInput("reply"));
        await run.WaitForAwaitingStepAsync("remote");

        var restart = Stopwatch.StartNew();
        await harness.Engine.SimulateRestartAsync();
        restart.Stop();

        Assert.True(
            restart.Elapsed < _bound,
            $"restarting with a flow parked on an awaited step took {restart.Elapsed} of real time; the parked execution must be abandoned, not waited on.");

        // Abandoned for takeover, not lost: the new incarnation re-attaches to the same wait and
        // the reply completes the run.
        await run.ResumeAsync();
        await WaitForWaitingEventsAsync(run, "remote", 2);
        await run.ReplyAsync(new OperationResult { Status = OperationStatus.Completed });
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    [Fact]
    public async Task SimulateRestart_WithAFlowParkedOnAnInProcessTimer_DoesNotWaitOutTheRealTimeGuard()
    {
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ParkedOnTimerFlow, ParkedRestartInput>());
        await using var _ = harness;

        var run = await harness.StartFlowAsync<ParkedOnTimerFlow, ParkedRestartInput>(new ParkedRestartInput("timer"));
        await run.WaitForTimerStepAsync("nap");

        var restart = Stopwatch.StartNew();
        await harness.Engine.SimulateRestartAsync();
        restart.Stop();

        Assert.True(
            restart.Elapsed < _bound,
            $"restarting with a flow parked on an in-process timer took {restart.Elapsed} of real time; the parked execution must be abandoned, not waited on.");

        // The checkpointed due time survived: the new incarnation waits out the remainder.
        await run.ResumeAsync();
        await WaitForWaitingEventsAsync(run, "nap", 2);
        await harness.AdvanceAsync(TimeSpan.FromSeconds(6));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    [Fact]
    public async Task Dispose_WithAFlowStillParked_DoesNotWaitOutTheRealTimeGuard()
    {
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ParkedOnReplyFlow, ParkedRestartInput>());

        var dispose = Stopwatch.StartNew();
        try
        {
            var run = await harness.StartFlowAsync<ParkedOnReplyFlow, ParkedRestartInput>(new ParkedRestartInput("dispose"));
            await run.WaitForAwaitingStepAsync("remote");
            dispose.Restart();
        }
        finally
        {
            await harness.DisposeAsync();
            dispose.Stop();
        }

        Assert.True(
            dispose.Elapsed < _bound,
            $"disposing the harness with a flow still parked took {dispose.Elapsed} of real time — every test that ends mid-wait paid it.");
    }

    /// <summary>Real-time bounded: the redelivered execution has parked again once the step's Waiting event repeats.</summary>
    private static async Task WaitForWaitingEventsAsync(FlowRunHandle run, string stepName, int count)
    {
        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
        while (run.Events.Count(e => e.Kind == Testing.FlowProbe.EventKind.Waiting && e.Step.StepName == stepName) < count)
        {
            Assert.True(
                TimeProvider.System.GetUtcNow() < guard,
                $"step '{stepName}' of flow '{run.FlowId}' never parked again after the restart.");
            await Task.Delay(TimeSpan.FromMilliseconds(5));
        }
    }
}
