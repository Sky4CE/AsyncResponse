using System.Collections.Concurrent;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

public sealed record Round65Input(string Payload);

/// <summary>The virtual instant each step body of a <see cref="Round65DelayFlow"/> run ran at.</summary>
public sealed class Round65StepClock
{
    public ConcurrentDictionary<string, DateTimeOffset> At { get; } = new(StringComparer.Ordinal);
}

/// <summary>step → 10-minute durable timer (suspends: above the in-process threshold) → step.</summary>
public sealed class Round65DelayFlow(Round65StepClock _steps, TimeProvider _time) : IDurableFlow<Round65Input>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, Round65Input input)
    {
        await flow.StepAsync("work", () =>
        {
            _steps.At["work"] = _time.GetUtcNow();
            return Task.CompletedTask;
        });
        await flow.DelayAsync("cool-down", TimeSpan.FromMinutes(10));
        await flow.StepAsync("after", () =>
        {
            _steps.At["after"] = _time.GetUtcNow();
            return Task.CompletedTask;
        });
    }
}

/// <summary>The scheduled twin of <see cref="Round65DelayFlow"/>, keyed by occurrence.</summary>
public sealed class Round65ScheduledDelayFlow(Round65StepClock _steps, TimeProvider _time) : IDurableFlow<ReportInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ReportInput input)
    {
        await flow.StepAsync("gather", () =>
        {
            _steps.At[$"gather:{input.Occurrence:HHmm}"] = _time.GetUtcNow();
            return Task.CompletedTask;
        });
        await flow.DelayAsync("settle", TimeSpan.FromMinutes(10));
        await flow.StepAsync("send", () =>
        {
            _steps.At[$"send:{input.Occurrence:HHmm}"] = _time.GetUtcNow();
            return Task.CompletedTask;
        });
    }
}

/// <summary>One step whose checkpointed result is <see cref="Round65Input.Payload"/> repeated to the requested size.</summary>
public sealed class Round65BigResultFlow : IDurableFlow<Round65Input>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, Round65Input input)
        => await flow.StepAsync("big", () => Task.FromResult(new string('x', int.Parse(input.Payload, System.Globalization.CultureInfo.InvariantCulture))));
}

/// <summary>
/// The harness's in-memory store behind a lease acquire that takes a few real milliseconds, like
/// any networked store's round trip. It pins WHEN the execution lease arms its renew timer: after
/// the advance has begun settling, while the job is still running engine code — the moment the
/// pre-fix settle mistook for "a virtual wait began". (Without it the job usually reaches its
/// park inside the settle's first yields on a warm JIT, and the defect showed only on a process's
/// first, cold harness.) Every other member forwards unchanged, default interface members included.
/// </summary>
internal sealed class Round65SlowAcquireStore(InMemoryFlowStateStore inner) : IFlowStateStore
{
    /// <summary>Real time, well inside the settle's 500 ms grace.</summary>
    private static readonly TimeSpan AcquireLatency = TimeSpan.FromMilliseconds(30);

    public void ValidateCreate(string flowId, FlowState state, TimeSpan ttl)
        => ((IFlowStateStore)inner).ValidateCreate(flowId, state, ttl);

    public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
        => inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

    public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
        => inner.LoadAsync(flowId, cancellationToken);

    public Task<FlowState?> LoadCurrentAsync(string flowId, CancellationToken cancellationToken = default)
        => ((IFlowStateStore)inner).LoadCurrentAsync(flowId, cancellationToken);

    public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
        => inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

    public async Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
    {
        await Task.Delay(AcquireLatency, TimeProvider.System, cancellationToken);
        return await inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);
    }

    public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

    public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
        => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

    public Task<FlowLeaseObservation?> ObserveLeaseAsync(string flowId, CancellationToken cancellationToken = default)
        => inner.ObserveLeaseAsync(flowId, cancellationToken);

    public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
        => inner.TryDeleteAsync(flowId, cancellationToken);

    /// <summary>Puts this store in front of the harness's own for the whole engine.</summary>
    public static void Register(IServiceCollection services)
        => services.AddSingleton<IFlowStateStore>(provider => new Round65SlowAcquireStore(provider.GetRequiredService<InMemoryFlowStateStore>()));
}

/// <summary>
/// Round 65, group "flowstate": the harness's settle, the execution lease's clock, the
/// scheduler's logging, and the harness store's size budget.
/// </summary>
public sealed class Round65FlowStateTests
{
    // ---------------------------------------------------------------------------------------------
    // F-10 — the harness moved the virtual clock forward under a running job: the job's own
    //        lease-renew arm (20 s) read as "a virtual wait began" on an idle harness, so the first
    //        pass ran at +20 s, its timer woke late, and a park's 30-second renewal join was fired
    //        by the advance, refusing the park as a hung store.

    /// <summary>
    /// StartFlowAsync followed at once by AdvanceAsync — the docs' crash-injection shape. Pre-fix:
    /// the first pass ran at start+20 s in every run (so "after" ran at start+10m20s), and about a
    /// third of runs failed their park ("renewal did not stop within 00:00:30") and took a third
    /// attempt. Looped because the park refusal was a race.
    /// </summary>
    [Fact]
    public async Task AdvanceRightAfterStart_RunsTheFirstPassAtStart_AndTheTimerWakesOnTime()
    {
        var judged = 0;
        for (var iteration = 0; iteration < 20; iteration++)
        {
            var steps = new Round65StepClock();
            var harness = await FlowTestHarness.StartAsync(options =>
            {
                options.ConfigureServices = services => services.AddSingleton(steps);
                options.ConfigureAsyncResponse = builder =>
                {
                    builder.WithDurableFlow<Round65DelayFlow, Round65Input>();
                    Round65SlowAcquireStore.Register(builder.Services);
                };
            });
            await using var _ = harness;
            var start = harness.Clock.GetUtcNow();

            var run = await harness.StartFlowAsync<Round65DelayFlow, Round65Input>(new Round65Input("go"));
            await harness.AdvanceAsync(TimeSpan.FromMinutes(10) + TimeSpan.FromSeconds(1));

            // The settle's real-time grace (500 ms) running out is the harness's documented limit on
            // a saturated machine — a JIT-cold first iteration under full CPU load — and then the
            // clock does move under the job; that iteration's timeline is not this test's to judge.
            // The defect pinned here never lapsed: the settle ended EARLY, on the guard timer's arm.
            if (harness.Engine.SettleBudgetLapses > 0)
                continue;

            judged++;
            var state = await run.GetStateAsync();
            Assert.True(
                state is { Status: FlowRunStatus.Succeeded, Attempts: 2 },
                $"iteration {iteration}: status={state?.Status}, attempts={state?.Attempts}, message={state?.LastMessage}; " +
                $"work ran at {At(steps, "work")}, after at {At(steps, "after")} (start {start:O}).");
            Assert.Equal(start, steps.At["work"]);
            Assert.Equal(start + TimeSpan.FromMinutes(10), steps.At["after"]);
            var waiting = Assert.Single(run.Events, e => e.Kind == AsyncResponse.Testing.FlowProbe.EventKind.Waiting && e.Step.StepName == "cool-down");
            Assert.Equal((start + TimeSpan.FromMinutes(10)).UtcDateTime, waiting.Step.WakeAtUtc);
        }

        Assert.True(judged >= 10, $"only {judged} of 20 iterations settled within the real-time grace");
    }

    /// <summary>
    /// The documented scheduling case: an hourly flow that sleeps 10 minutes, advanced once across
    /// the occurrence, the sleep, and 5 s of slack. Pre-fix the occurrence's first pass ran 20 s
    /// late (its timer anchored at 01:10:20), and the run could still be Running at 01:10:05.
    /// </summary>
    [Fact]
    public async Task HourlyScheduledFlow_WithATimer_IsAnchoredOnTheOccurrence()
    {
        var judged = 0;
        for (var iteration = 0; iteration < 10; iteration++)
        {
            var steps = new Round65StepClock();
            var harness = await FlowTestHarness.StartAsync(options =>
            {
                options.StartTime = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
                options.ConfigureServices = services => services.AddSingleton(steps);
                options.ConfigureAsyncResponse = builder =>
                {
                    builder.WithScheduledFlow<Round65ScheduledDelayFlow, ReportInput>("hourly", "0 * * * *", occurrence => new ReportInput(occurrence));
                    Round65SlowAcquireStore.Register(builder.Services);
                };
            });
            await using var _ = harness;
            var occurrence = new DateTimeOffset(2030, 1, 1, 1, 0, 0, TimeSpan.Zero);

            await harness.AdvanceAsync(TimeSpan.FromHours(1) + TimeSpan.FromMinutes(10) + TimeSpan.FromSeconds(5));

            // The settle's real-time grace (500 ms) running out is the harness's documented limit on
            // a saturated machine — a JIT-cold first iteration under full CPU load — and then the
            // clock does move under the job; that iteration's timeline is not this test's to judge.
            // The defect pinned here never lapsed: the settle ended EARLY, on the guard timer's arm.
            if (harness.Engine.SettleBudgetLapses > 0)
                continue;

            judged++;
            var run = harness.Attach("sched:hourly:20300101T010000Z");
            var state = await run.GetStateAsync();
            Assert.True(
                state is { Status: FlowRunStatus.Succeeded, Attempts: 2 },
                $"iteration {iteration}: status={state?.Status}, attempts={state?.Attempts}, message={state?.LastMessage}; " +
                $"gather ran at {At(steps, "gather:0100")}, send at {At(steps, "send:0100")}.");
            Assert.Equal(occurrence, steps.At["gather:0100"]);
            Assert.Equal(occurrence + TimeSpan.FromMinutes(10), steps.At["send:0100"]);
            var waiting = Assert.Single(run.Events, e => e.Kind == AsyncResponse.Testing.FlowProbe.EventKind.Waiting && e.Step.StepName == "settle");
            Assert.Equal((occurrence + TimeSpan.FromMinutes(10)).UtcDateTime, waiting.Step.WakeAtUtc);
        }

        Assert.True(judged >= 5, $"only {judged} of 10 iterations settled within the real-time grace");
    }

    private static string At(Round65StepClock steps, string key)
        => steps.At.TryGetValue(key, out var at) ? at.ToString("O", System.Globalization.CultureInfo.InvariantCulture) : "never";

    // ---------------------------------------------------------------------------------------------
    // L-16 — the execution-lease deadline was on the wall clock: a backward wall-clock step during
    //        a renewal outage kept the side-effect guard open past the store's lease.

    /// <summary>
    /// The wall clock steps back five minutes while renewals fail; the monotonic clock keeps going.
    /// The lease must be lost once ExecutionLeaseDuration has ELAPSED. Pre-fix: the deadline was
    /// "wall now + duration" at the last renewal, so 61 s later the wall clock still read five
    /// minutes short of it — LostToken stayed live and ThrowIfLost let step bodies run.
    /// </summary>
    [Fact]
    public async Task ExecutionLease_IsLostOnElapsedTime_WhenTheWallClockStepsBack()
    {
        var clock = new SteppedWallClock();
        var store = new Mock<IFlowStateStore>();
        store.Setup(s => s.TryRenewLeaseAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TimeoutException("store partitioned from this host"));
        store.Setup(s => s.ReleaseLeaseAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var options = new DurableFlowOptions
        {
            ExecutionLeaseDuration = TimeSpan.FromSeconds(60),
            ExecutionLeaseRenewInterval = TimeSpan.FromSeconds(20)
        };
        await using var lease = new FlowExecutionLease(store.Object, "flow-stepped", "lease-1", options, NullLogger.Instance, clock);

        clock.WallOffset = -TimeSpan.FromMinutes(5);
        for (var second = 0; second < 59; second++)
            clock.Virtual.Advance(TimeSpan.FromSeconds(1));
        Assert.False(lease.IsLost, "the lease was lost before its duration elapsed");

        clock.Virtual.Advance(TimeSpan.FromSeconds(2));

        Assert.True(lease.IsLost, "61 s elapsed with no successful renewal, yet the lease still reads as held");
        Assert.True(lease.LostToken.IsCancellationRequested, "the deadline watcher did not fire on elapsed time");
        Assert.Throws<DurableFlowLeaseLostException>(() => lease.ThrowIfLost());
    }

    /// <summary>Virtual time, with a wall clock that can be stepped independently of the monotonic one.</summary>
    private sealed class SteppedWallClock : TimeProvider
    {
        public VirtualTimeProvider Virtual { get; } = new();

        public TimeSpan WallOffset { get; set; }

        public override DateTimeOffset GetUtcNow() => Virtual.GetUtcNow() + WallOffset;

        public override long GetTimestamp() => Virtual.GetTimestamp();

        public override long TimestampFrequency => Virtual.TimestampFrequency;

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
            => Virtual.CreateTimer(callback, state, dueTime, period);
    }

    // ---------------------------------------------------------------------------------------------
    // L-01 — the scheduler logged without SafeLog: a throwing logging provider lost an
    //        undispatched occurrence and faulted the scheduler.

    /// <summary>
    /// A failed publish, logged through a provider that throws on that line. Pre-fix the throw
    /// escaped the catch block: the occurrence was never queued for re-drive (lost — nothing was
    /// persisted for it) and the scheduler's loop faulted, which stops the host.
    /// </summary>
    [Fact]
    public async Task Scheduler_ThrowingLoggerOnAFailedPublish_StillRedrivesTheOccurrence_AndKeepsScheduling()
    {
        var time = new VirtualTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 30, TimeSpan.Zero));
        var flows = new ScriptedFlows();
        var failuresLeft = 1;
        flows.OnStart = flowId => Interlocked.Decrement(ref failuresLeft) >= 0
            ? new DurableFlowNotDispatchedException(flowId, new TimeoutException("broker down"))
            : null;
        var logger = new RecordingThrowingLogger<ScheduledFlowService> { ThrowOnMessageContaining = "could not publish the start job" };

        using var scheduler = new ScheduledFlowService(flows, [Hourly()], logger, time);
        await scheduler.StartAsync(CancellationToken.None);
        try
        {
            await WaitForArmedTimerAsync(time);
            time.Advance(TimeSpan.FromMinutes(59) + TimeSpan.FromSeconds(35));
            await flows.WaitForStartsAsync(1);

            // The re-drive, RedriveInterval (30 s) later.
            await WaitForArmedTimerAsync(time);
            time.Advance(TimeSpan.FromSeconds(30));
            await flows.WaitForStartsAsync(2, scheduler.ExecuteTask);
            Assert.Equal(["sched:hourly:20300101T010000Z", "sched:hourly:20300101T010000Z"], flows.Starts.ToArray());

            // And the loop lives on to the next occurrence.
            await WaitForArmedTimerAsync(time);
            time.Advance(TimeSpan.FromHours(1));
            await flows.WaitForStartsAsync(3, scheduler.ExecuteTask);
            Assert.Equal("sched:hourly:20300101T020000Z", flows.Starts.Last());
            Assert.False(scheduler.ExecuteTask!.IsFaulted, "the scheduler faulted on a throwing logging provider");
        }
        finally
        {
            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>
    /// A start that succeeded, logged through a provider that throws on the success line. Pre-fix
    /// that throw was caught as a failed start and logged "failed to start occurrence" at Error.
    /// </summary>
    [Fact]
    public async Task Scheduler_ThrowingLoggerOnASuccessfulStart_DoesNotReportTheStartAsFailed()
    {
        var time = new VirtualTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 30, TimeSpan.Zero));
        var flows = new ScriptedFlows();
        var logger = new RecordingThrowingLogger<ScheduledFlowService> { ThrowOnMessageContaining = "started occurrence" };

        using var scheduler = new ScheduledFlowService(flows, [Hourly()], logger, time);
        await scheduler.StartAsync(CancellationToken.None);
        try
        {
            await WaitForArmedTimerAsync(time);
            time.Advance(TimeSpan.FromMinutes(59) + TimeSpan.FromSeconds(35));
            await flows.WaitForStartsAsync(1);

            // The loop finished the occurrence (logged, or not, and decided) once it re-arms.
            await WaitForArmedTimerAsync(time);
            Assert.False(logger.HasEntry(LogLevel.Error, "failed to start occurrence"), "a successful start was reported as failed");
            Assert.False(scheduler.ExecuteTask!.IsFaulted);
        }
        finally
        {
            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    private static ScheduledFlowRegistration Hourly() => new()
    {
        Name = "hourly",
        CronExpression = "0 * * * *",
        Options = new ScheduledFlowOptions(),
        StartOccurrenceAsync = static (flows, flowId, occurrence, cancellationToken) =>
            flows.StartAsync<NightlyReportFlow, ReportInput>(new ReportInput(occurrence), flowId, cancellationToken)
    };

    private static async Task WaitForArmedTimerAsync(VirtualTimeProvider time)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (time.NextTimerDueAt is null)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("the scheduler loop armed no sleep on the virtual clock");
            await Task.Delay(10);
        }
    }

    /// <summary>Publish-first starts: a failed publish persists nothing, a successful one creates the ledger.</summary>
    private sealed class ScriptedFlows : IDurableFlows
    {
        private readonly ConcurrentDictionary<string, FlowState> _states = new(StringComparer.Ordinal);

        public ConcurrentQueue<string> Starts { get; } = new();

        /// <summary>Returns the exception a start should throw, or null to succeed.</summary>
        public Func<string, Exception?> OnStart { get; set; } = _ => null;

        public Task<string> StartAsync<TFlow, TInput>(TInput input, string? flowId = null, CancellationToken cancellationToken = default)
            where TFlow : class, IDurableFlow<TInput>
        {
            Starts.Enqueue(flowId!);
            if (OnStart(flowId!) is { } failure)
                throw failure;
            _states[flowId!] = new FlowState { FlowId = flowId!, Status = FlowRunStatus.Running, Attempts = 0 };
            return Task.FromResult(flowId!);
        }

        public Task ResumeAsync(string flowId, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<FlowState?> GetStateAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult(_states.TryGetValue(flowId, out var state) ? state : null);

        public async Task WaitForStartsAsync(int count, Task? loop = null)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (Starts.Count < count)
            {
                if (loop is { IsFaulted: true })
                    throw new InvalidOperationException($"The scheduler faulted after [{string.Join(", ", Starts)}].", loop.Exception);
                if (DateTime.UtcNow > deadline)
                    throw new TimeoutException($"Expected {count} start(s); saw [{string.Join(", ", Starts)}].");
                await Task.Delay(10);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // L-17 — the harness's in-memory store had no state-size budget, so a ledger DynamoDB, Cosmos
    //        DB or MongoDB would refuse passed every harness test.

    /// <summary>A start whose initial state is over the budget is refused before anything is published.</summary>
    [Fact]
    public async Task HarnessMaxStateBytes_RefusesAnOversizedInitialState_AtStart()
    {
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.MaxStateBytes = 350_000;
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<Round65DelayFlow, Round65Input>();
        });
        await using var _ = harness;

        var refused = await Assert.ThrowsAsync<FlowStateTooLargeException>(
            () => harness.StartFlowAsync<Round65DelayFlow, Round65Input>(new Round65Input(new string('y', 400_000))));
        Assert.Equal(350_000, refused.MaxStateBytes);
    }

    /// <summary>
    /// A step result that grows the ledger past the budget fails the attempt with the store's
    /// exception, as the checkpoint would on DynamoDB — and passed untouched before the budget.
    /// </summary>
    [Fact]
    public async Task HarnessMaxStateBytes_FailsTheCheckpointThatGrowsTheLedgerPastIt()
    {
        var failures = new AttemptFailures();
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.MaxStateBytes = 350_000;
            options.FlowObservers.Add(failures);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<Round65BigResultFlow, Round65Input>();
        });
        await using var _ = harness;

        var run = await harness.StartFlowAsync<Round65BigResultFlow, Round65Input>(new Round65Input("400000"));

        // A one-step flow fails an attempt only because its checkpoint was refused; without the
        // budget it succeeds on the first one.
        await failures.First.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var state = await run.GetStateAsync();
        Assert.NotNull(state);
        Assert.NotEqual(FlowRunStatus.Succeeded, state.Status);
        Assert.False(state.Steps?.ContainsKey("big") == true, "the oversized step result was checkpointed");
    }

    /// <summary>A default ledger-growth warning is fitted under the budget; one the test set at or above it is refused.</summary>
    [Fact]
    public async Task HarnessMaxStateBytes_FitsTheDefaultLedgerWarning_AndRefusesAnExplicitOneAtOrAboveIt()
    {
        var harness = await AsyncResponseTestHarness.StartAsync(options => options.MaxStateBytes = 350_000);
        await using (harness)
        {
            var flows = harness.Services.GetRequiredService<DurableFlowOptions>();
            Assert.Equal(350_000 - 350_000 / 4, flows.LedgerSizeWarningBytes);
        }

        await Assert.ThrowsAsync<InvalidOperationException>(() => AsyncResponseTestHarness.StartAsync(options =>
        {
            options.MaxStateBytes = 350_000;
            options.DurableFlows = flows => flows.LedgerSizeWarningBytes = 400_000;
        }));
        await Assert.ThrowsAsync<InvalidOperationException>(() => AsyncResponseTestHarness.StartAsync(options => options.MaxStateBytes = 0));
    }

    /// <summary>The store itself, below the harness: unlimited by default, and every write judged once a budget is set.</summary>
    [Fact]
    public async Task InMemoryStore_MaxStateBytes_GuardsCreateUpdateAndPreflight()
    {
        var time = new VirtualTimeProvider();
        var big = new string('z', 2_000);

        var unlimited = new InMemoryFlowStateStore(time);
        ((IFlowStateStore)unlimited).ValidateCreate("free", NewState("free", big), TimeSpan.FromHours(1));
        Assert.True(await unlimited.TryCreateAsync("free", NewState("free", big), TimeSpan.FromHours(1)));

        var store = new InMemoryFlowStateStore(time) { MaxStateBytes = 1_000 };
        Assert.Throws<FlowStateTooLargeException>(() => ((IFlowStateStore)store).ValidateCreate("capped", NewState("capped", big), TimeSpan.FromHours(1)));
        await Assert.ThrowsAsync<FlowStateTooLargeException>(() => store.TryCreateAsync("capped", NewState("capped", big), TimeSpan.FromHours(1)));
        Assert.Null(await store.LoadAsync("capped"));

        Assert.True(await store.TryCreateAsync("capped", NewState("capped", "small"), TimeSpan.FromHours(1)));
        var grown = NewState("capped", big);
        grown.Revision = 1;
        await Assert.ThrowsAsync<FlowStateTooLargeException>(() => store.TryUpdateAsync("capped", grown, 0, TimeSpan.FromHours(1)));
        Assert.Equal(0, (await store.LoadAsync("capped"))!.Revision);
    }

    private static FlowState NewState(string flowId, string message) => new()
    {
        FlowId = flowId,
        FlowTypeName = "Round65",
        Status = FlowRunStatus.Running,
        LastMessage = message,
        Steps = []
    };

    private sealed class AttemptFailures : IDurableFlowExecutionObserver
    {
        public TaskCompletionSource<string> First { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask OnRunAttemptFailedAsync(DurableFlowRunEvent run)
        {
            First.TrySetResult(run.Message ?? "");
            return default;
        }
    }
}
