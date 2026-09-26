using System.Diagnostics;
using System.Reflection;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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
        private int _failRuns;

        public int QueuedRuns => Volatile.Read(ref _queuedRuns);
        public int FailRuns => Volatile.Read(ref _failRuns);
        public void RecordFailRun() => Interlocked.Increment(ref _failRuns);
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
        public Task FailAsync()
        {
            _state.RecordFailRun();
            throw new InvalidOperationException("the job fails and sleeps its ordinary retry backoff");
        }

        public async Task QueuedAsync()
        {
            _state.RecordQueuedRun(_incarnation);
            await _state.ReleaseQueued.Task;
        }
    }

    [Theory]
    [InlineData(5)] // the default: bounded attempts
    [InlineData(0)] // unlimited
    public async Task SimulateRestart_WithAJobAsleepInItsOrdinaryRetryBackoff_StillDrainsTheJobQueuedBehindIt(int maxDeliveryAttempts)
    {
        // Regression (fixpoint r1 pre-commit review): the stop watcher counted EVERY retry backoff
        // as a wait on the virtual clock. An ordinary backoff — taken before the stop — is bound
        // to the worker host's stopping token and ends the moment that host's stop begins, so it
        // is no reason to cut the stop short. With the only worker in such a backoff and a job
        // queued behind it, the watcher cut the graceful stop at t=0, before any hosted service
        // had stopped: the queued job then ran beside the restart (which refused with "could not
        // establish quiescence" after no time at all), or never ran in the old incarnation. The
        // stop now ends the backoff, drops the failing job, and drains the queue.
        //
        // Pre-commit review (fixpoint r2, C1): with bounded attempts a production host stop now
        // retries the interrupted job during the drain instead (S4#2). A simulated restart keeps
        // crash semantics: the crashed attempt dies with the old incarnation — not re-run there,
        // with the flow's next steps inside the restart — whatever the attempt budget.
        var state = new BackoffWorkState();
        var harness = await AsyncResponseTestHarness.StartAsync(options =>
        {
            options.Transport = transport =>
            {
                transport.WorkerCount = 1;
                transport.MaxDeliveryAttempts = maxDeliveryAttempts;
            };
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
        Assert.Equal(1, state.FailRuns); // the crashed attempt was not retried inside the restart
    }

    public interface IDrainFailingWork
    {
        Task RunAsync();
    }

    /// <summary>Fails once the old incarnation has begun draining, so its retry ladder sleeps on the (frozen) virtual clock.</summary>
    public sealed class DrainFailingWork : IDrainFailingWork
    {
        public InMemoryWorkerTransport? Transport { get; set; }
        public FieldInfo? Draining { get; set; }
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Whether the job's failure really happened during the drain — what the test is about.</summary>
        public bool ThrewWhileDraining { get; private set; }

        public async Task RunAsync()
        {
            Started.TrySetResult();
            var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
            while (!(bool)Draining!.GetValue(Transport)! && TimeProvider.System.GetUtcNow() < guard)
                await Task.Delay(TimeSpan.FromMilliseconds(1));

            ThrewWhileDraining = (bool)Draining.GetValue(Transport)!;
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
        // Resolved here, not inside the job: a renamed field threw NullReferenceException there,
        // before the drain began, and the job's ordinary backoff still passed the timing check.
        var draining = typeof(InMemoryWorkerTransport).GetField("_draining", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(draining);
        var work = new DrainFailingWork { Draining = draining };
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
        Assert.True(work.ThrewWhileDraining, "the job failed before the drain began, so its backoff was not the drain's");
    }

    [Fact]
    public async Task SimulateRestart_WithADeliveryPollingForAParkedFlowsLease_NeitherWaitsOutTheGuardNorRefuses()
    {
        // S4#4 (fixpoint r2): with two workers, a duplicate wake-up of a flow parked on an awaited
        // step polls for the parked execution's lease on the virtual clock — which cannot move
        // while the test awaits the restart. Counted as running user code, it burned the whole
        // real-time guard and then refused with "could not establish quiescence"; in production
        // host stop ends that poll with the engine's hand-back.
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.Transport = transport => transport.WorkerCount = 2;
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ParkedOnReplyFlow, ParkedRestartInput>();
        });
        await using var _ = harness;

        var run = await harness.StartFlowAsync<ParkedOnReplyFlow, ParkedRestartInput>(new ParkedRestartInput("contended"));
        await run.WaitForAwaitingStepAsync("remote");

        // A duplicate wake-up: the free worker takes it into the lease-contention poll.
        await run.ResumeAsync();
        var executor = Assert.IsType<DurableFlowExecutor>(harness.Engine.Services.GetRequiredService<IDurableFlowExecutor>());
        await WaitUntilAsync(() => executor.LeaseContentionWaits == 1, "the duplicate delivery never reached the lease-contention poll");

        var restart = Stopwatch.StartNew();
        await harness.Engine.SimulateRestartAsync();
        restart.Stop();

        Assert.True(
            restart.Elapsed < _bound,
            $"restarting with a delivery polling for a lease took {restart.Elapsed} of real time; the poll is a wait on the virtual clock, not user code.");

        // Taken over as after any restart.
        await run.ResumeAsync();
        await run.WaitForAwaitingStepAsync("remote");
        await run.ReplyAsync(new OperationResult { Status = OperationStatus.Completed });
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    [Fact]
    public async Task SimulateRestart_CarriesAScheduledJobOver_UnderItsPublishersExecutionContext()
    {
        // S4#5 (fixpoint r2): the drain retained bare envelopes, so the restart re-scheduled each
        // delayed job under the ambient context of the test that called SimulateRestartAsync — a
        // job relying on its publisher's AsyncLocal state (a tenant, a principal) saw the test's
        // instead, unlike an unstarted immediate job, which keeps its enqueuer's.
        var recorder = new AmbientRecorder();
        var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services
                .AddSingleton(recorder)
                .AddSingleton<IAmbientWork, AmbientWork>());
        await using var _ = harness;

        AmbientRecorder.Ambient.Value = "publisher";
        await harness.Builder.EnqueueWorkerAsync<IAmbientWork>(work => work.RecordAsync(), TimeSpan.FromMinutes(5));
        AmbientRecorder.Ambient.Value = "restart-caller";

        await harness.SimulateRestartAsync();
        await harness.AdvanceAsync(TimeSpan.FromMinutes(5));

        // Hang guard only: the job is due once the clock has moved.
        Assert.Equal("publisher", await recorder.Seen.Task.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Advance_WithAJobAsleepInItsRetryBackoff_DoesNotSpinTheSettleBudget()
    {
        // S4#9 (fixpoint r2): the settle counted a job asleep in a retry backoff armed before it
        // began as busy user code, so every settle over it spun its whole 500 ms real-time budget —
        // twice per advance (after CrashAfterStep, the documented pattern). The backoff is a wait on
        // the virtual clock; advancing is exactly what it needs. Pinned by the settle's own lapse
        // count, not a wall-clock bound (pre-commit review r2, C6): the old code lapses twice here.
        var state = new BackoffWorkState();
        var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services
                .AddSingleton(state)
                .AddSingleton<IncarnationMarker>()
                .AddSingleton<IBackoffWork, BackoffWork>());
        await using var _ = harness;
        var transport = harness.Services.GetRequiredService<InMemoryWorkerTransport>();

        await harness.Builder.EnqueueWorkerAsync<IBackoffWork>(target => target.FailAsync());
        await WaitUntilAsync(() => transport.BackingOffJobs == 1, "the failing job never reached its retry backoff");

        await harness.AdvanceAsync(TimeSpan.FromMilliseconds(10)); // short of the 100 ms backoff

        Assert.Equal(0, harness.SettleBudgetLapses);
        Assert.Equal(1, state.FailRuns);
    }

    [Fact]
    public async Task AZombiesFailedAttempt_DoesNotEraseTheNewIncarnationsParkOfTheSameFlow()
    {
        // S4#7 (fixpoint r2): every incarnation registered the SAME quiesce probe, so an execution
        // the restart abandoned — which keeps its executor and the observers it resolved — could
        // fail its attempt after the restart and erase the new incarnation's park of the same flow
        // (RemoveRun by flow id). Settles then spun their budget and a later restart refused.
        await using var harness = await AsyncResponseTestHarness.StartAsync();
        var zombie = QuiesceObserver(harness);

        await harness.SimulateRestartAsync();
        var live = QuiesceObserver(harness);

        await live.OnStepWaitingAsync(new DurableFlowStepEvent("flow-z", "remote", DurableFlowStepKind.Awaited, CorrelationId: "cid-z"));
        Assert.Equal(1, ParkedCount(harness));

        await zombie.OnRunAttemptFailedAsync(new DurableFlowRunEvent("flow-z", FlowRunStatus.Running, "the abandoned attempt ended"));
        Assert.Equal(1, ParkedCount(harness));

        // The live incarnation's own failure still clears it.
        await live.OnRunAttemptFailedAsync(new DurableFlowRunEvent("flow-z", FlowRunStatus.Running, "timed out"));
        Assert.Equal(0, ParkedCount(harness));
    }

    [Fact]
    public async Task QuiesceProbe_DecidesATimersParkAtItsStartingEvent_NotWhenTheWaitIsRecorded()
    {
        // S4#8 (fixpoint r2), the quiesce probe's twin of the flow probe's round-43 fix: the engine
        // decides whether a timer parks in process on the remainder it measures right after the
        // step's Starting event, and a first pass saves its breadcrumb before notifying Waiting. A
        // clock advanced during that save made this suspended timer — 15 s against the 10 s
        // threshold — look like a 9 s in-process park: a phantom park until its due time, which let
        // the settle advance the clock under another busy job.
        await using var harness = await AsyncResponseTestHarness.StartAsync();
        var observer = QuiesceObserver(harness);

        var suspended = new DurableFlowStepEvent("flow-t", "short-nap", DurableFlowStepKind.Timer, WakeAtUtc: harness.Clock.GetUtcNow().UtcDateTime + TimeSpan.FromSeconds(15));
        await observer.OnStepStartingAsync(suspended);
        harness.Clock.Advance(TimeSpan.FromSeconds(6));
        await observer.OnStepWaitingAsync(suspended);
        Assert.Equal(0, ParkedCount(harness));

        // A timer the engine does park in process still counts.
        var parked = new DurableFlowStepEvent("flow-p", "nap", DurableFlowStepKind.Timer, WakeAtUtc: harness.Clock.GetUtcNow().UtcDateTime + TimeSpan.FromSeconds(5));
        await observer.OnStepStartingAsync(parked);
        await observer.OnStepWaitingAsync(parked);
        Assert.Equal(1, ParkedCount(harness));
    }

    public sealed class StartStopRecorder
    {
        private int _stops;

        public int Stops => Volatile.Read(ref _stops);
        public void RecordStop() => Interlocked.Increment(ref _stops);
    }

    /// <summary>A user hosted service that is neither a BackgroundService nor disposable: only StopAsync ends it.</summary>
    public sealed class RecordingHostedService(StartStopRecorder _recorder) : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _recorder.RecordStop();
            return Task.CompletedTask;
        }
    }

    public sealed class FailingHostedService : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken) => throw new InvalidOperationException("this service cannot start");

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    [Fact]
    public async Task StartAsync_AServiceFailingToStart_StopsTheServicesThatAlreadyStarted()
    {
        // S4#15 (fixpoint r2): the harness recorded its started services only after ALL of them
        // started, so a failing start left the earlier ones running — the disposal that followed
        // stopped nothing.
        var recorder = new StartStopRecorder();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => AsyncResponseTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services
                .AddSingleton(recorder)
                .AddHostedService<RecordingHostedService>();
            options.ConfigureAsyncResponse = builder => builder.Services.AddHostedService<FailingHostedService>();
        }));

        Assert.Contains("cannot start", ex.Message, StringComparison.Ordinal);
        Assert.Equal(1, recorder.Stops);
    }

    /// <summary>Starts cleanly; its stop throws.</summary>
    public sealed class ThrowingStopHostedService : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => throw new NotSupportedException("this service cannot stop");
    }

    [Fact]
    public async Task StartAsync_AServiceFailingToStart_SurfacesItsError_EvenWhenAStartedServiceFailsToStop()
    {
        // Pre-commit review (fixpoint r2, C2): once a partial start stopped the services that had
        // started (S4#15), a StopAsync that threw anything but a cancellation escaped the
        // teardown — the services stopped after it never were, and StartAsync reported that stop
        // failure instead of the start failure the test needs to see.
        var recorder = new StartStopRecorder();
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => AsyncResponseTestHarness.StartAsync(options =>
        {
            // Stopped in reverse: the throwing stop comes before the recording one.
            options.ConfigureServices = services => services
                .AddSingleton(recorder)
                .AddHostedService<RecordingHostedService>()
                .AddHostedService<ThrowingStopHostedService>();
            options.ConfigureAsyncResponse = builder => builder.Services.AddHostedService<FailingHostedService>();
        }));

        Assert.IsType<InvalidOperationException>(ex);
        Assert.Contains("cannot start", ex.Message, StringComparison.Ordinal);
        Assert.Equal(1, recorder.Stops);
    }

    [Fact]
    public async Task Dispose_AServiceFailingToStop_StillStopsTheOthers_AndReportsTheFailure()
    {
        // C2's other half: the failure still surfaces from an ordinary disposal — after every
        // other service has stopped.
        var recorder = new StartStopRecorder();
        var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services
                .AddSingleton(recorder)
                .AddHostedService<RecordingHostedService>()
                .AddHostedService<ThrowingStopHostedService>());

        var ex = await Assert.ThrowsAsync<NotSupportedException>(async () => await harness.DisposeAsync());

        Assert.Contains("cannot stop", ex.Message, StringComparison.Ordinal);
        Assert.Equal(1, recorder.Stops);
    }

    [Fact]
    public async Task SimulateRestart_ARebuildThatThrows_SurfacesItsError_AndDisposeStaysQuiet()
    {
        // S4#15 (fixpoint r2): a restart whose rebuild threw left the harness on the disposed
        // provider of the incarnation before, and DisposeAsync — the test's `await using` — then
        // resolved the transport from it: ObjectDisposedException, replacing the real failure.
        var builds = 0;
        var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = _ =>
            {
                if (++builds == 2)
                    throw new InvalidOperationException("the second build fails");
            });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => harness.SimulateRestartAsync());
        Assert.Equal("the second build fails", ex.Message);

        await harness.DisposeAsync();
    }

    /// <summary>The quiesce probe's observer of the harness's CURRENT incarnation: registered first in every incarnation.</summary>
    private static IDurableFlowExecutionObserver QuiesceObserver(AsyncResponseTestHarness harness)
        => harness.Services.GetServices<IDurableFlowExecutionObserver>().First();

    private static int ParkedCount(AsyncResponseTestHarness harness)
    {
        var quiesce = typeof(AsyncResponseTestHarness)
            .GetField("_quiesce", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(harness)!;
        return (int)quiesce.GetType().GetProperty("ParkedCount")!.GetValue(quiesce)!;
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
