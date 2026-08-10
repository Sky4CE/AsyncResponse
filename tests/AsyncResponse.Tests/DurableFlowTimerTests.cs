using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// Durable timer steps (flow.DelayAsync / DelayUntilAsync) exercised end to end on the Testing
// harness: the suspend path (delayed wake-up job, no lease held while sleeping), the in-process
// path, remainder semantics across re-executions, and crash injection around a timer. The flow
// classes carry NO test instrumentation — side effects are observed through an injected recorder
// and the harness probe.
// ---------------------------------------------------------------------------------------------

public sealed record ReminderInput(string Customer);

public sealed class StepRecorder
{
    private readonly List<string> _entries = [];

    public IReadOnlyList<string> Entries
    {
        get { lock (_entries) return [.. _entries]; }
    }

    public void Record(string entry)
    {
        lock (_entries)
            _entries.Add(entry);
    }

    public int Count(string entry)
    {
        lock (_entries)
            return _entries.Count(e => e == entry);
    }
}

public sealed class ReminderFlow(StepRecorder _recorder) : IDurableFlow<ReminderInput>
{
    public static readonly TimeSpan CoolDown = TimeSpan.FromDays(3);

    public async Task ExecuteAsync(IDurableFlowContext flow, ReminderInput input)
    {
        await flow.StepAsync("prepare", () =>
        {
            _recorder.Record("prepare");
            return Task.CompletedTask;
        });

        await flow.DelayAsync("cool-down", CoolDown);

        await flow.StepAsync("send-reminder", () =>
        {
            _recorder.Record($"send-reminder:{input.Customer}");
            return Task.CompletedTask;
        });
    }
}

public sealed class AbsoluteDeadlineFlow(StepRecorder _recorder) : IDurableFlow<ReminderInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ReminderInput input)
    {
        // An instant that is already in the past completes immediately, like a memoized step.
        await flow.DelayUntilAsync("already-due", DateTimeOffset.UnixEpoch);
        await flow.StepAsync("done", () =>
        {
            _recorder.Record("done");
            return Task.CompletedTask;
        });
    }
}

public class DurableFlowTimerTests
{
    [Fact]
    public async Task ThreeDayTimer_SuspendsAndWakesViaDelayedJob_NoWorkerHeldWhileSleeping()
    {
        var recorder = new StepRecorder();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(recorder);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ReminderFlow, ReminderInput>();
        });

        var run = await harness.StartFlowAsync<ReminderFlow, ReminderInput>(new ReminderInput("acme"));
        var wakeAt = await run.WaitForTimerStepAsync("cool-down");

        // The run is parked: ledger Running, due time checkpointed, and — the suspend path's whole
        // point — the worker queue is idle while three days pass.
        await harness.Engine.WaitForWorkerIdleAsync();
        var sleeping = await run.GetStateAsync();
        Assert.Equal(FlowRunStatus.Running, sleeping!.Status);
        Assert.Equal(wakeAt, sleeping.Steps!["cool-down"].WakeAtUtc);
        Assert.False(sleeping.Steps["cool-down"].Completed);
        Assert.Equal(1, recorder.Count("prepare"));

        // Not yet: one day in, still sleeping.
        await harness.AdvanceAsync(TimeSpan.FromDays(1));
        Assert.Equal(FlowRunStatus.Running, (await run.GetStateAsync())!.Status);
        Assert.Equal(0, recorder.Count("send-reminder:acme"));

        // Cross the due time: the delayed wake-up job re-executes the flow, the timer completes,
        // and the remaining steps run exactly once.
        await harness.AdvanceAsync(TimeSpan.FromDays(2));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
        Assert.Equal(1, recorder.Count("prepare"));
        Assert.Equal(1, recorder.Count("send-reminder:acme"));

        var finished = await run.GetStateAsync();
        Assert.True(finished!.Steps!["cool-down"].Completed);
        Assert.Equal(wakeAt, finished.Steps["cool-down"].WakeAtUtc);

        // The timer step ran twice (the execution that parked it, the wake that completed it);
        // the local steps stayed memoized.
        Assert.Equal(2, run.StepExecutions("cool-down"));
        Assert.Equal(1, run.StepExecutions("prepare"));
    }

    [Fact]
    public async Task ShortTimer_WaitsInProcessUnderTheLease_SingleExecution()
    {
        var recorder = new StepRecorder();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(recorder);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ReminderFlow, ReminderInput>();
            options.DurableFlows = flows =>
            {
                // Force the in-process path for the three-day sleep: threshold above the delay.
                flows.TimerInProcessThreshold = TimeSpan.FromDays(30);
                // Test-sized lease cadence: the stepwise advance walks EVERY armed timer, and the
                // default 20-second renew beat would mean ~13k steps across three virtual days.
                flows.ExecutionLeaseDuration = TimeSpan.FromDays(10);
                flows.ExecutionLeaseRenewInterval = TimeSpan.FromDays(1);
            };
        });

        var run = await harness.StartFlowAsync<ReminderFlow, ReminderInput>(new ReminderInput("acme"));
        await run.WaitForTimerStepAsync("cool-down");

        await harness.AdvanceAsync(ReminderFlow.CoolDown);
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());

        // No suspend round-trip: the timer step executed exactly once, in one worker delivery.
        Assert.Equal(1, run.StepExecutions("cool-down"));
        Assert.Equal(1, recorder.Count("send-reminder:acme"));
    }

    [Fact]
    public async Task DelayUntil_PastInstant_CompletesWithoutWaiting()
    {
        var recorder = new StepRecorder();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(recorder);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<AbsoluteDeadlineFlow, ReminderInput>();
        });

        var run = await harness.StartFlowAsync<AbsoluteDeadlineFlow, ReminderInput>(new ReminderInput("acme"));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
        Assert.Equal(1, recorder.Count("done"));
        Assert.Equal(1, run.StepExecutions("already-due"));
    }

    [Fact]
    public async Task WakeDeliveredEarly_ReSuspendsForTheRemainder_DueTimeAnchoredAtFirstExecution()
    {
        var recorder = new StepRecorder();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(recorder);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ReminderFlow, ReminderInput>();
        });

        var run = await harness.StartFlowAsync<ReminderFlow, ReminderInput>(new ReminderInput("acme"));
        var wakeAt = await run.WaitForTimerStepAsync("cool-down");
        await harness.Engine.WaitForWorkerIdleAsync();

        // Simulate an early wake-up (a chunked hop arriving before the due time): resume the run
        // one day in. The executor re-parks it for the REMAINDER — the checkpointed due time,
        // anchored at the first execution, does not move.
        await harness.AdvanceAsync(TimeSpan.FromDays(1));
        await run.ResumeAsync();
        await harness.Engine.WaitForWorkerIdleAsync();

        var reParked = await run.GetStateAsync();
        Assert.Equal(FlowRunStatus.Running, reParked!.Status);
        Assert.Equal(wakeAt, reParked.Steps!["cool-down"].WakeAtUtc);
        Assert.Equal(0, recorder.Count("send-reminder:acme"));
        Assert.True(run.StepExecutions("cool-down") >= 2);

        await harness.AdvanceAsync(TimeSpan.FromDays(2));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
        Assert.Equal(1, recorder.Count("send-reminder:acme"));
    }

    [Fact]
    public async Task CrashInjectedAtTheTimerStep_RedeliveryResumesTheSleep_ExactlyOnceSideEffects()
    {
        var recorder = new StepRecorder();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(recorder);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ReminderFlow, ReminderInput>();
        });

        harness.CrashBeforeStep("cool-down");
        var run = await harness.StartFlowAsync<ReminderFlow, ReminderInput>(new ReminderInput("acme"));

        // The first delivery crashed at the timer; the in-memory transport's redelivery (retry
        // backoff runs on the virtual clock) re-executes from the checkpoint.
        await harness.AdvanceAsync(TimeSpan.FromSeconds(2));
        await run.WaitForTimerStepAsync("cool-down");

        await harness.AdvanceAsync(ReminderFlow.CoolDown);
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());

        // The crash cost a delivery, never a side effect: "prepare" was checkpointed before the
        // crash and skipped on redelivery.
        Assert.Equal(1, recorder.Count("prepare"));
        Assert.Equal(1, recorder.Count("send-reminder:acme"));
    }

    [Fact]
    public async Task CompletedTimer_IsMemoized_LikeAnyStep()
    {
        var recorder = new StepRecorder();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(recorder);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ReminderFlow, ReminderInput>();
        });

        var run = await harness.StartFlowAsync<ReminderFlow, ReminderInput>(new ReminderInput("acme"));
        await run.WaitForTimerStepAsync("cool-down");
        await harness.AdvanceAsync(ReminderFlow.CoolDown + TimeSpan.FromMinutes(1));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());

        var executionsAfterSuccess = run.StepExecutions("cool-down");

        // Duplicate wake-up of a finished run: acks as a no-op, no step re-runs.
        await harness.Engine.FlowExecutor.ExecuteAsync(run.FlowId);
        Assert.Equal(executionsAfterSuccess, run.StepExecutions("cool-down"));
        Assert.Equal(1, recorder.Count("send-reminder:acme"));
    }
}
