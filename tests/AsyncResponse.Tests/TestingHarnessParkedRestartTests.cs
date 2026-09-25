using System.Diagnostics;
using System.Reflection;
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

/// <summary>Suspends on a timer past the in-process threshold: the job ends and the wake-up is a scheduled job.</summary>
public sealed class SuspendedTimerFlow : IDurableFlow<ParkedRestartInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ParkedRestartInput input)
        => await flow.DelayAsync("long-nap", TimeSpan.FromHours(1));
}

/// <summary>Holds <see cref="GatedStepFlow"/>'s step until the test releases it.</summary>
public sealed class StepGate
{
    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

/// <summary>One checkpointed step that runs until the test opens its gate.</summary>
public sealed class GatedStepFlow(StepGate _gate) : IDurableFlow<ParkedRestartInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ParkedRestartInput input)
        => await flow.StepAsync("work", async () =>
        {
            _gate.Entered.TrySetResult();
            await _gate.Release.Task;
        });
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

    [Fact]
    public async Task SimulateRestart_WithAJobQueuedBehindAParkedFlow_CarriesItOverWithoutWaitingOutTheGuard()
    {
        // Regression (fixpoint r1): the stop's "only parks are left" check and the restart's
        // lingering check both counted QUEUED jobs as running user code. With the only worker
        // held by a park, a job queued behind it can never start: the check (2 outstanding vs 1
        // parked) never passed, the stop burned the whole real-time guard, and the restart then
        // refused with "1 execution still running user code" — for a job that never started.
        // And (fixpoint r1 pre-commit review): that job then died with the old incarnation in
        // silence. It never started, so nothing can run it twice: it is carried over like a
        // message a broker still holds — a queued flow start has no ledger yet, so nothing else
        // could ever recover it.
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.Transport = transport => transport.WorkerCount = 1;
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ParkedOnReplyFlow, ParkedRestartInput>();
        });
        await using var _ = harness;

        var parked = await harness.StartFlowAsync<ParkedOnReplyFlow, ParkedRestartInput>(new ParkedRestartInput("parked"));
        await parked.WaitForAwaitingStepAsync("remote");

        // Queued behind the park: the single worker is held until the test replies.
        var queued = await harness.StartFlowAsync<ParkedOnReplyFlow, ParkedRestartInput>(new ParkedRestartInput("queued"));

        var restart = Stopwatch.StartNew();
        await harness.Engine.SimulateRestartAsync();
        restart.Stop();

        Assert.True(
            restart.Elapsed < _bound,
            $"restarting with a job queued behind a park took {restart.Elapsed} of real time; a job that never started is not user code to wait for.");

        // The queued start runs in the new incarnation (holding its only worker until answered).
        await queued.WaitForAwaitingStepAsync("remote");
        await queued.ReplyAsync(new OperationResult { Status = OperationStatus.Completed });
        Assert.Equal(FlowRunStatus.Succeeded, await queued.WaitForFinishedAsync());

        // The parked run is taken over as before.
        await parked.ResumeAsync();
        await parked.WaitForAwaitingStepAsync("remote");
        await parked.ReplyAsync(new OperationResult { Status = OperationStatus.Completed });
        Assert.Equal(FlowRunStatus.Succeeded, await parked.WaitForFinishedAsync());
    }

    public interface IAmbientWork
    {
        Task RecordAsync();
    }

    /// <summary>Shared across incarnations: the ambient value the carried-over job ran under.</summary>
    public sealed class AmbientRecorder
    {
        public static readonly AsyncLocal<string?> Ambient = new();

        public TaskCompletionSource<string?> Seen { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public sealed class AmbientWork(AmbientRecorder _recorder) : IAmbientWork
    {
        public Task RecordAsync()
        {
            _recorder.Seen.TrySetResult(AmbientRecorder.Ambient.Value);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task SimulateRestart_CarriesAQueuedJobOver_UnderItsEnqueuersExecutionContext()
    {
        // Pass-2 precommit review: the carry-over dropped the job's captured ExecutionContext and
        // readmitted it under the restart caller's, so a plain worker job queued behind a park ran
        // in the new incarnation with the TEST's ambient state instead of its enqueuer's — unlike
        // every other job the in-memory transport runs.
        var recorder = new AmbientRecorder();
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.Transport = transport => transport.WorkerCount = 1;
            options.ConfigureServices = services => services
                .AddSingleton(recorder)
                .AddSingleton<IAmbientWork, AmbientWork>();
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ParkedOnReplyFlow, ParkedRestartInput>();
        });
        await using var _ = harness;

        var parked = await harness.StartFlowAsync<ParkedOnReplyFlow, ParkedRestartInput>(new ParkedRestartInput("holds-the-worker"));
        await parked.WaitForAwaitingStepAsync("remote");

        AmbientRecorder.Ambient.Value = "enqueuer";
        await harness.Engine.Builder.EnqueueWorkerAsync<IAmbientWork>(work => work.RecordAsync());
        AmbientRecorder.Ambient.Value = "restart-caller";

        await harness.Engine.SimulateRestartAsync();

        // Hang guard only: the carried-over job runs as soon as the new incarnation starts.
        Assert.Equal("enqueuer", await recorder.Seen.Task.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task AfterARestart_TheStepBarrierWaitsForTheNewIncarnationsPark()
    {
        // Regression (fixpoint r1, backlog since r22): the probe kept one history across restarts
        // and the barriers replayed its newest match, so after SimulateRestartAsync
        // WaitForAwaitingStepAsync returned the DEAD incarnation's park at once — before the new
        // incarnation had parked at all, and with the old correlation id when the re-executed
        // step arms a fresh one (a reply to it is fenced off and the test hangs). The library's
        // own restart tests had to count Waiting events to work around it.
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ParkedOnReplyFlow, ParkedRestartInput>());
        await using var _ = harness;

        var run = await harness.StartFlowAsync<ParkedOnReplyFlow, ParkedRestartInput>(new ParkedRestartInput("barrier"));
        var firstPark = await run.WaitForAwaitingStepAsync("remote");

        await harness.Engine.SimulateRestartAsync();

        // Nothing re-executes the run until it is resumed, so nothing may satisfy the barrier yet.
        var barrier = run.WaitForAwaitingStepAsync("remote");
        Assert.False(barrier.IsCompleted, "the barrier returned the dead incarnation's park");

        await run.ResumeAsync();
        Assert.Equal(firstPark, await barrier); // the re-attached wait keeps its persisted id
        Assert.Equal(2, run.Events.Count(e => e.Kind == Testing.FlowProbe.EventKind.Waiting)); // history spans the restart

        await run.ReplyAsync(new OperationResult { Status = OperationStatus.Completed });
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    [Fact]
    public async Task AfterARestart_ReplyingBeforeAnythingReExecutesTheRun_RoutesThroughRecovery()
    {
        // The other half of the incarnation contract: a reply made before anything has
        // re-executed the run answers the wait that survived the restart — a response arriving
        // while the process is down — and lost-subscriber recovery routes it into the run.
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ParkedOnReplyFlow, ParkedRestartInput>());
        await using var _ = harness;

        var run = await harness.StartFlowAsync<ParkedOnReplyFlow, ParkedRestartInput>(new ParkedRestartInput("while-down"));
        await run.WaitForAwaitingStepAsync("remote");

        await harness.Engine.SimulateRestartAsync();
        await run.ReplyAsync(new OperationResult { Status = OperationStatus.Completed });

        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    [Fact]
    public async Task AfterARestart_AStepCompletedDuringTheOldIncarnationsDrain_StillSatisfiesItsBarrier()
    {
        // Regression (fixpoint r1 pre-commit review): the incarnation filter covered EVERY
        // barrier, so a completion checkpoint recorded before the restart — here, by the old
        // incarnation's graceful drain — was hidden from WaitForStepCompletedAsync. The replay
        // skips a checkpointed step and records nothing new, so the barrier timed out on the
        // real-time guard. Only a park is process-bound; a checkpoint is durable.
        var gate = new StepGate();
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(gate);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<GatedStepFlow, ParkedRestartInput>();
        });
        await using var _ = harness;

        var run = await harness.StartFlowAsync<GatedStepFlow, ParkedRestartInput>(new ParkedRestartInput("drain"));
        await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // The stop drains the running step: it completes in the OLD incarnation.
        var restart = harness.Engine.SimulateRestartAsync();
        gate.Release.TrySetResult();
        await restart;

        await run.WaitForStepCompletedAsync("work");
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    [Fact]
    public async Task AfterARestart_ASuspendedTimersBarrier_ReturnsTheCarriedOverDueTime()
    {
        // Regression (fixpoint r1 pre-commit review): a timer past the in-process threshold
        // suspends — its wake-up is a scheduled job the restart carries over — so its Waiting
        // event describes durable state. Filtered by incarnation, WaitForTimerStepAsync blocked
        // until the new incarnation re-parked, which only happens once the clock passes the very
        // due time the call exists to return.
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<SuspendedTimerFlow, ParkedRestartInput>());
        await using var _ = harness;

        var run = await harness.StartFlowAsync<SuspendedTimerFlow, ParkedRestartInput>(new ParkedRestartInput("suspended"));
        var dueBefore = await run.WaitForTimerStepAsync("long-nap");

        await harness.Engine.SimulateRestartAsync();

        Assert.Equal(dueBefore, await run.WaitForTimerStepAsync("long-nap"));
        await harness.AdvanceAsync(TimeSpan.FromHours(1));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    /// <summary>Suspends on a timer just past the default 10 s in-process threshold.</summary>
    public sealed class BarelySuspendedTimerFlow : IDurableFlow<ParkedRestartInput>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, ParkedRestartInput input)
            => await flow.DelayAsync("short-nap", TimeSpan.FromSeconds(15));
    }

    /// <summary>
    /// Advances the clock once, when <c>stepName</c> reports Waiting — notified before the flow
    /// probe, so the probe records that event at the later instant.
    /// </summary>
    private sealed class AdvanceOnWaiting(string stepName, TimeSpan by) : IDurableFlowExecutionObserver
    {
        private int _fired;

        public VirtualTimeProvider? Clock { get; set; }

        public ValueTask OnStepWaitingAsync(DurableFlowStepEvent step)
        {
            if (step.StepName == stepName && Interlocked.Exchange(ref _fired, 1) == 0)
                Clock!.Advance(by);
            return default;
        }
    }

    [Fact]
    public async Task AfterARestart_ASuspendedTimersBarrier_IsClassifiedAtTheEnginesDecision_NotWhenTheWaitIsRecorded()
    {
        // Pass-2 precommit review: the probe classified a timer's Waiting event with the clock read
        // when it recorded the event. The engine decides on the remainder it computes right after
        // the step's Starting event, and a first pass saves its breadcrumb before notifying Waiting:
        // a clock advanced in between (the settle grace lapsing on a loaded runner) made this
        // suspended timer — 15 s against the 10 s threshold — look like an in-process park (9 s
        // left), and after the restart the barrier hid the carried-over wait until the guard.
        var advancer = new AdvanceOnWaiting("short-nap", TimeSpan.FromSeconds(6));
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.FlowObservers.Add(advancer);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<BarelySuspendedTimerFlow, ParkedRestartInput>();
        });
        await using var _ = harness;
        advancer.Clock = harness.Clock;

        var run = await harness.StartFlowAsync<BarelySuspendedTimerFlow, ParkedRestartInput>(new ParkedRestartInput("decided-early"));
        var dueBefore = await run.WaitForTimerStepAsync("short-nap");
        Assert.Equal(TimeSpan.FromSeconds(9), dueBefore - harness.Clock.GetUtcNow().UtcDateTime);

        await harness.Engine.SimulateRestartAsync();

        Assert.Equal(dueBefore, await run.WaitForTimerStepAsync("short-nap"));

        // The wake-up was published with the 15 s remainder the engine decided on, from the
        // advanced clock: it is due 6 s after the timer.
        await harness.AdvanceAsync(TimeSpan.FromSeconds(15));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    [Fact]
    public async Task AfterARestart_AnInProcessTimersBarrier_WaitsForTheNewIncarnationsPark()
    {
        // The other half of the classification: a timer at or below the threshold parks IN
        // PROCESS, holding its worker slot, and that park died with the old incarnation — so, like
        // an awaited step's, its Waiting event must not satisfy a barrier after the restart.
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ParkedOnTimerFlow, ParkedRestartInput>());
        await using var _ = harness;

        var run = await harness.StartFlowAsync<ParkedOnTimerFlow, ParkedRestartInput>(new ParkedRestartInput("in-process"));
        var dueBefore = await run.WaitForTimerStepAsync("nap");

        await harness.Engine.SimulateRestartAsync();

        var barrier = run.WaitForTimerStepAsync("nap");
        Assert.False(barrier.IsCompleted, "the barrier returned the dead incarnation's in-process park");

        await run.ResumeAsync();
        Assert.Equal(dueBefore, await barrier); // the checkpointed due time survived
        await harness.AdvanceAsync(TimeSpan.FromSeconds(6));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    public interface IBackoffWork
    {
        Task FailAsync();
        Task QueuedAsync();
    }

    /// <summary>Identifies the incarnation (service provider) a job ran on: one per provider.</summary>
    public sealed class IncarnationMarker;

    /// <summary>Shared across incarnations: what <see cref="BackoffWork"/> observed.</summary>
    public sealed class BackoffWorkState
    {
        private int _queuedRuns;

        public int QueuedRuns => Volatile.Read(ref _queuedRuns);
        public TaskCompletionSource<IncarnationMarker> QueuedRanOn { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ReleaseQueued { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void RecordQueuedRun(IncarnationMarker incarnation)
        {
            Interlocked.Increment(ref _queuedRuns);
            QueuedRanOn.TrySetResult(incarnation);
        }
    }

    public sealed class BackoffWork(BackoffWorkState _state, IncarnationMarker _incarnation) : IBackoffWork
    {
        public Task FailAsync() => throw new InvalidOperationException("the job fails and sleeps its ordinary retry backoff");

        public async Task QueuedAsync()
        {
            _state.RecordQueuedRun(_incarnation);
            await _state.ReleaseQueued.Task;
        }
    }

    [Fact]
    public async Task SimulateRestart_WithAJobAsleepInItsOrdinaryRetryBackoff_StillDrainsTheJobQueuedBehindIt()
    {
        // Regression (fixpoint r1 pre-commit review): the stop watcher counted EVERY retry backoff
        // as a wait on the virtual clock. An ordinary backoff — taken before the stop — is bound
        // to the worker host's stopping token and ends the moment that host's stop begins, so it
        // is no reason to cut the stop short. With the only worker in such a backoff and a job
        // queued behind it, the watcher cut the graceful stop at t=0, before any hosted service
        // had stopped: the queued job then ran beside the restart (which refused with "could not
        // establish quiescence" after no time at all), or never ran in the old incarnation. The
        // stop now ends the backoff, drops the failing job, and drains the queue.
        var state = new BackoffWorkState();
        var harness = await AsyncResponseTestHarness.StartAsync(options =>
        {
            options.Transport = transport => transport.WorkerCount = 1;
            options.ConfigureServices = services => services
                .AddSingleton(state)
                .AddSingleton<IncarnationMarker>()
                .AddSingleton<IBackoffWork, BackoffWork>();
        });
        await using var _ = harness;
        var transport = harness.Services.GetRequiredService<InMemoryWorkerTransport>();
        var oldIncarnation = harness.Services.GetRequiredService<IncarnationMarker>();

        await harness.Builder.EnqueueWorkerAsync<IBackoffWork>(target => target.FailAsync());
        await WaitUntilAsync(() => transport.BackingOffJobs == 1, "the failing job never reached its retry backoff");
        await harness.Builder.EnqueueWorkerAsync<IBackoffWork>(target => target.QueuedAsync());

        var restart = harness.SimulateRestartAsync();
        // Hang guard only: the old incarnation's drain runs the queued job, which holds the stop
        // here until released.
        var ranOn = await state.QueuedRanOn.Task.WaitAsync(TimeSpan.FromSeconds(10));
        state.ReleaseQueued.TrySetResult();
        await restart;

        Assert.Same(oldIncarnation, ranOn);
        Assert.Equal(1, state.QueuedRuns);
    }

    public interface IDrainFailingWork
    {
        Task RunAsync();
    }

    /// <summary>Fails once the old incarnation has begun draining, so its retry ladder sleeps on the (frozen) virtual clock.</summary>
    public sealed class DrainFailingWork : IDrainFailingWork
    {
        public InMemoryWorkerTransport? Transport { get; set; }
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task RunAsync()
        {
            Started.TrySetResult();
            var draining = typeof(InMemoryWorkerTransport).GetField("_draining", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
            while (!(bool)draining.GetValue(Transport)! && TimeProvider.System.GetUtcNow() < guard)
                await Task.Delay(TimeSpan.FromMilliseconds(1));

            throw new InvalidOperationException("the job fails during the shutdown drain");
        }
    }

    [Fact]
    public async Task SimulateRestart_WithACrashedJobAsleepInTheDrainBackoff_NeitherWaitsOutTheGuardNorRefuses()
    {
        // Regression (fixpoint r1): a job that fails during the shutdown drain sleeps its retry
        // backoff on the harness's virtual clock, which cannot move while the test awaits the
        // restart. Counted as running user code (and with nothing parked, the stop never ended
        // early), it cost the whole real-time guard and then a false "still running user code"
        // refusal. The backoff is a wait on the clock, like a park.
        var work = new DrainFailingWork();
        var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IDrainFailingWork>(work));
        await using var _ = harness;
        work.Transport = harness.Services.GetRequiredService<InMemoryWorkerTransport>();

        await harness.Builder.EnqueueWorkerAsync<IDrainFailingWork>(target => target.RunAsync());
        await work.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var restart = Stopwatch.StartNew();
        await harness.SimulateRestartAsync();
        restart.Stop();

        Assert.True(
            restart.Elapsed < _bound,
            $"restarting with a crashed job asleep in its drain backoff took {restart.Elapsed} of real time; the backoff is a wait on the virtual clock, not user code.");
    }

    /// <summary>Real-time bounded (a hang guard, not an assertion window).</summary>
    private static async Task WaitUntilAsync(Func<bool> condition, string failure)
    {
        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
        while (!condition())
        {
            Assert.True(TimeProvider.System.GetUtcNow() < guard, failure);
            await Task.Delay(TimeSpan.FromMilliseconds(1));
        }
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
