using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Text.Json;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression pins for the round-65 review of the durable-flow engine (flows-engine slice):
/// <list type="bullet">
/// <item>F-02 — a step that FAULTED on a correlation id is no longer pending on it: a late failure
/// for it (reaching a registration a crashed worker left behind) failed a live run terminally, and
/// a late response completed it behind the flow's back.</item>
/// <item>L-01 — log lines after a durable write or publish in the executor (and the few context
/// lines round 64 missed) were unguarded, so a throwing logging provider rewrote a Succeeded
/// ledger, failed a committed park, doubled a republished wake-up, or orphaned a created start.</item>
/// <item>L-15 — a lost execution lease reached flow code as a plain InvalidOperationException, which
/// the documented compensation filter treated as a step failure.</item>
/// </list>
/// </summary>
public sealed class Round65FlowExecTests
{
    // ---------------------------------------------------------------------------------------------
    // F-02 — late signals for a step that faulted on their correlation id.

    /// <summary>
    /// The review's probe p2, restart shape: the step re-attaches after a redeploy (its first
    /// registration survives), times out, the flow's documented best-effort catch carries on into
    /// a long timer — and the remote job's late failure, routed to the surviving registration,
    /// failed the run. Pre-fix failure: the status reads Failed ("DAG failed late") right after the
    /// publish; post-fix the run stays Running and finishes Succeeded once its timer is due.
    /// </summary>
    [Fact]
    public async Task ALateFailure_ForAnAwaitedStepThatTimedOutAndWasCaught_DoesNotFailTheRun_AfterARestart()
    {
        await using var harness = await StartHarnessAsync<BestEffortFlow>();
        var run = await harness.StartFlowAsync<BestEffortFlow, LateInput>(new LateInput(1), "r65-best-effort-restart");
        var cid = await run.WaitForAwaitingStepAsync("lineage");

        // A redeploy while the step waits: the first incarnation's recovery registration survives.
        await harness.Engine.SimulateRestartAsync();
        await run.ResumeAsync();
        Assert.Equal(cid, await run.WaitForAwaitingStepAsync("lineage"));

        // The step times out and the flow's catch carries on into a 6-hour timer.
        await harness.AdvanceAsync(TimeSpan.FromMinutes(31));
        await run.WaitForTimerStepAsync("settle");
        var mid = await run.GetStateAsync();
        Assert.True(mid!.Steps!["lineage"].Faulted);
        Assert.Equal(cid, mid.Steps["lineage"].PendingCorrelationId);
        Assert.NotEmpty(await harness.Engine.Services.GetRequiredService<IRecoveryStateStore>().GetAllAsync(cid));

        // The remote job fails late, 45 minutes in.
        await harness.Engine.PublishExceptionAsync(new InvalidOperationException("DAG failed late"), cid);

        var after = await run.GetStateAsync();
        Assert.Equal(FlowRunStatus.Running, after!.Status);
        Assert.NotEqual("DAG failed late", after.LastMessage);

        await FinishBestEffortRunAsync(harness, run, cid);
    }

    /// <summary>
    /// The control shape of probe p2 (no restart): the step's only registration is deleted with its
    /// waiter, so the late failure finds nothing to route to. Passes before and after the fix; it
    /// pins that the restart shape above is the only difference.
    /// </summary>
    [Fact]
    public async Task ALateFailure_ForAnAwaitedStepThatTimedOutAndWasCaught_DoesNotFailTheRun_WithoutARestart()
    {
        await using var harness = await StartHarnessAsync<BestEffortFlow>();
        var run = await harness.StartFlowAsync<BestEffortFlow, LateInput>(new LateInput(1), "r65-best-effort-control");
        var cid = await run.WaitForAwaitingStepAsync("lineage");

        await harness.AdvanceAsync(TimeSpan.FromMinutes(31));
        await run.WaitForTimerStepAsync("settle");
        await harness.Engine.PublishExceptionAsync(new InvalidOperationException("DAG failed late"), cid);

        Assert.Equal(FlowRunStatus.Running, (await run.GetStateAsync())!.Status);
        await FinishBestEffortRunAsync(harness, run, cid);
    }

    /// <summary>
    /// Wakes the best-effort run from its 6-hour timer. The replay restarts the faulted step fresh
    /// (the documented best-effort behavior), under a new correlation id; that wait times out too,
    /// is caught again, and the run finishes.
    /// </summary>
    private static async Task FinishBestEffortRunAsync(FlowTestHarness harness, FlowRunHandle run, string faultedCid)
    {
        await harness.AdvanceAsync(TimeSpan.FromHours(6));
        var replayed = await run.GetStateAsync();
        Assert.Equal(FlowRunStatus.Running, replayed!.Status);
        Assert.NotEqual(faultedCid, replayed.Steps!["lineage"].PendingCorrelationId);

        await harness.AdvanceAsync(TimeSpan.FromMinutes(31));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
        Assert.Equal(1, run.StepExecutions("notify"));
    }

    /// <summary>
    /// The smaller variant: a flow that does NOT catch the timeout. The fault is saved, the attempt
    /// fails, and the transport backs off before the redelivery restarts the idempotent step under a
    /// new correlation id. A late failure for the old id landing in that backoff failed the run
    /// terminally instead. Pre-fix failure: Failed right after the publish; post-fix the step
    /// restarts fresh and the run succeeds on the new id's response.
    /// </summary>
    [Fact]
    public async Task ALateFailure_InTheRetryBackoffAfterAnUncaughtTimeout_LetsTheStepRestartFresh()
    {
        await using var harness = await StartHarnessAsync<StrictFlow>(transport: options =>
        {
            options.RetryBaseDelay = TimeSpan.FromMinutes(5);
            options.RetryMaxDelay = TimeSpan.FromMinutes(10);
        });
        var run = await harness.StartFlowAsync<StrictFlow, LateInput>(new LateInput(2), "r65-strict-backoff");
        var cid = await run.WaitForAwaitingStepAsync("lineage");
        await harness.Engine.SimulateRestartAsync();
        await run.ResumeAsync();
        Assert.Equal(cid, await run.WaitForAwaitingStepAsync("lineage"));

        // Timed out at 30 minutes; the redelivery is 5 minutes of backoff away. The fault's save
        // runs on the job's own continuation, so on a loaded runner it can land just after the
        // advance returns: wait for it (bounded) rather than reading once.
        await harness.AdvanceAsync(TimeSpan.FromMinutes(31));
        await WaitForAsync(
            async () => (await run.GetStateAsync())?.Steps?["lineage"] is { Faulted: true } ? "faulted" : null,
            "the timed-out step's fault to be saved");
        var faulted = await run.GetStateAsync();
        Assert.Equal(FlowRunStatus.Running, faulted!.Status);
        Assert.Equal(cid, faulted.Steps!["lineage"].PendingCorrelationId);

        await harness.Engine.PublishExceptionAsync(new InvalidOperationException("DAG failed late"), cid);
        Assert.Equal(FlowRunStatus.Running, (await run.GetStateAsync())!.Status);

        // The redelivery restarts the step fresh, under a new correlation id.
        await harness.AdvanceAsync(TimeSpan.FromMinutes(5));
        var fresh = await WaitForAsync(
            async () => (await run.GetStateAsync())?.Steps?["lineage"] is { Faulted: false, PendingCorrelationId: { } pending } && pending != cid
                ? pending
                : null,
            "the step to restart under a new correlation id");
        await harness.Engine.PublishAsync(new LineageResult { Ok = true }, fresh);

        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
        Assert.Equal(2, StrictFlow.Triggers(run.FlowId));
    }

    /// <summary>
    /// The executor's two lost-subscriber targets, directly. A late RESPONSE for a step that
    /// faulted on its id was checkpointed into it — completing a step whose fault flow code may
    /// already have handled, so the next replay took the other branch. Pre-fix failure: the step
    /// reads Completed with the late payload; the late failure marked the run Failed.
    /// </summary>
    [Fact]
    public async Task RecoverAndFail_ForAStepThatFaultedOnTheirCorrelationId_LeaveTheStepAndTheRunAlone()
    {
        var store = new InMemoryFlowStateStore();
        var state = RunningState("r65-faulted-targets", typeof(StrictFlow));
        state.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
        {
            ["lineage"] = new() { PendingCorrelationId = "cid-faulted", Faulted = true, Message = "Timed out." }
        };
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        var builder = EnqueueRecordingBuilder();
        await using var provider = new ServiceCollection().AddSingleton<IFlowStateStore>(store).BuildServiceProvider();
        var executor = Executor(provider, builder.Object, NullLogger<DurableFlowExecutor>.Instance);

        await executor.RecoverAsync(state.FlowId!, new LineageResult { Ok = true }, "cid-faulted");
        await executor.FailAsync(state.FlowId!, new InvalidOperationException("late failure"), "cid-faulted");

        var persisted = (await store.LoadAsync(state.FlowId!))!;
        Assert.Equal(FlowRunStatus.Running, persisted.Status);
        var step = persisted.Steps!["lineage"];
        Assert.False(step.Completed);
        Assert.Null(step.ResultJson);
        Assert.True(step.Faulted);

        // A step still pending on its id (not faulted) is failed by the same signal, as before.
        state = RunningState("r65-pending-target", typeof(StrictFlow));
        state.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
        {
            ["lineage"] = new() { PendingCorrelationId = "cid-pending" }
        };
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        await executor.FailAsync(state.FlowId!, new InvalidOperationException("real failure"), "cid-pending");
        Assert.Equal(FlowRunStatus.Failed, (await store.LoadAsync(state.FlowId!))!.Status);
    }

    /// <summary>
    /// The deposed holder's lease-less checkpoint of a won response applies the same test: a
    /// takeover that timed the step out (and whose flow may have caught that) must not find the
    /// step completed behind it. Pre-fix failure: the persisted step is completed with the deposed
    /// holder's response although the takeover had faulted it.
    /// </summary>
    [Fact]
    public async Task ALeaseLessCheckpoint_OfAWonResponse_DoesNotCompleteAStepATakeoverFaulted()
    {
        var store = new TakeoverStore("remote", takeover: state =>
        {
            var step = state.Steps!["remote"];
            step.Faulted = true;
            step.Message = "Timed out by the takeover.";
            state.LastMessage = "the takeover caught the timeout and moved on";
        });
        var state = new FlowState { FlowId = "r65-lease-less-vs-faulted", Status = FlowRunStatus.Running };
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));

        var won = new OperationResult { Status = OperationStatus.Completed, Message = "won by the deposed holder" };
        await using (var lease = await AcquireLeaseAsync(store, state.FlowId!))
        {
            var context = Context(state, store, lease, subscriber: SubscriberReturning(Task.FromResult(won)));
            await Assert.ThrowsAnyAsync<InvalidOperationException>(() => context.AwaitStepAsync<OperationResult>("remote", _ => Task.CompletedTask));
        }

        Assert.Equal(1, store.Takeovers);
        var persisted = (await store.LoadAsync(state.FlowId!))!;
        var remote = persisted.Steps!["remote"];
        Assert.False(remote.Completed);
        Assert.Null(remote.ResultJson);
        Assert.True(remote.Faulted);
        Assert.Equal("the takeover caught the timeout and moved on", persisted.LastMessage);
    }

    // ---------------------------------------------------------------------------------------------
    // L-01 — logging after a durable write or publish.

    /// <summary>
    /// The review's probe p1: the success line ran inside the try, after the terminal save. A throw
    /// rewrote the Succeeded ledger's message, fired OnRunAttemptFailedAsync with Status=Succeeded,
    /// and failed the delivery. Pre-fix failure: ExecuteAsync throws the logger's exception.
    /// </summary>
    [Fact]
    public async Task ASucceededRun_StaysCleanlySucceeded_WhenTheSuccessLogThrows()
    {
        var store = new InMemoryFlowStateStore();
        var state = RunningState("r65-success-log", typeof(OneStepFlow));
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        var observer = new RunRecorder();
        await using var provider = new ServiceCollection()
            .AddSingleton<IFlowStateStore>(store)
            .AddSingleton<OneStepFlow>()
            .BuildServiceProvider();
        var logger = new RecordingThrowingLogger<DurableFlowExecutor> { ThrowOnMessageContaining = "completed successfully" };
        var executor = Executor(provider, EnqueueRecordingBuilder().Object, logger, observers: [observer]);

        Assert.Null(await Record.ExceptionAsync(() => executor.ExecuteAsync(state.FlowId!)));

        var persisted = (await store.LoadAsync(state.FlowId!))!;
        Assert.Equal(FlowRunStatus.Succeeded, persisted.Status);
        Assert.Equal("Flow completed.", persisted.LastMessage);
        Assert.Empty(observer.AttemptFailed);
        var finished = Assert.Single(observer.Finished);
        Assert.Equal(FlowRunStatus.Succeeded, finished.Status);
        Assert.Equal("Flow completed.", finished.Message);
    }

    /// <summary>
    /// A committed park (wake-up published, lease released) whose debug line throws used to reach
    /// the general catch, which saved through the released lease and failed the delivery — every
    /// retry re-parked and published another wake-up. Pre-fix failure: ExecuteAsync throws.
    /// </summary>
    [Fact]
    public async Task ACommittedPark_IsAcknowledged_WhenTheSuspendedDebugLineThrows()
    {
        var store = new InMemoryFlowStateStore();
        var state = RunningState("r65-park-log", typeof(SleepingFlow));
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        await using var provider = new ServiceCollection()
            .AddSingleton<IFlowStateStore>(store)
            .AddSingleton<SleepingFlow>()
            .BuildServiceProvider();
        var builder = EnqueueRecordingBuilder();
        var logger = new RecordingThrowingLogger<DurableFlowExecutor> { ThrowOnMessageContaining = "suspended:" };
        var executor = Executor(provider, builder.Object, logger, workerTransport: DelayedTransport());

        Assert.Null(await Record.ExceptionAsync(() => executor.ExecuteAsync(state.FlowId!)));

        Assert.Equal(1, DelayedEnqueues(builder));
        var persisted = (await store.LoadAsync(state.FlowId!))!;
        Assert.Equal(FlowRunStatus.Running, persisted.Status);
        Assert.NotNull(persisted.Steps!["nap"].WakeAtUtc);
    }

    /// <summary>
    /// The start job's create-from-carrier log ran after the create: a throw dead-lettered (under a
    /// persistent throw) a start whose Running, never-executed ledger nothing would wake. Pre-fix
    /// failure: CreateAndExecuteAsync throws and the flow never runs.
    /// </summary>
    [Fact]
    public async Task AStartJobThatCreatesTheLedger_ExecutesTheRun_WhenTheCreatedLogThrows()
    {
        var store = new InMemoryFlowStateStore();
        await using var provider = new ServiceCollection()
            .AddSingleton<IFlowStateStore>(store)
            .AddSingleton<OneStepFlow>()
            .BuildServiceProvider();
        var logger = new RecordingThrowingLogger<DurableFlowExecutor> { ThrowOnMessageContaining = "ledger created from its start job" };
        var executor = Executor(provider, EnqueueRecordingBuilder().Object, logger);
        var carrier = RunningState("r65-start-log", typeof(OneStepFlow));

        Assert.Null(await Record.ExceptionAsync(() => executor.CreateAndExecuteAsync(carrier.FlowId!, FlowStateJson.Serialize(carrier))));

        var persisted = (await store.LoadAsync(carrier.FlowId!))!;
        Assert.Equal(FlowRunStatus.Succeeded, persisted.Status);
        Assert.Equal(1, persisted.Attempts);
    }

    /// <summary>
    /// The holder's own job, redelivered under its live handler and re-published delayed past the
    /// lease: the log line ran after the publish, so a throw NAKed this delivery while its copy
    /// already existed — two copies carrying the holder's JobId, neither ever acknowledged as a
    /// duplicate. Pre-fix failure: the delivery throws the logger's exception after one publish.
    /// </summary>
    [Fact]
    public async Task AnOwnJobRedelivery_RepublishedPastTheLease_IsAcknowledged_WhenTheRepublishLogThrows()
    {
        var time = new VirtualTimeProvider();
        var options = new DurableFlowOptions();
        var job = new WorkerJobEnvelope
        {
            Call = CallbackExpressionConverter.ToReflectionCall<IDurableFlowExecutor>(executor => executor.ExecuteAsync("r65-own-job")),
            JobId = "job-r65-own"
        };
        var holderLease = FlowLeaseContention.NewLeaseId(FlowLeaseContention.JobTag(job.JobId));
        var inner = new InMemoryFlowStateStore();
        Assert.True(await inner.TryCreateAsync("r65-own-job", RunningState("r65-own-job", typeof(OneStepFlow)), TimeSpan.FromMinutes(5)));
        // The holder renews forever: every observation shows the same lease, its expiry a full
        // lease ahead of the (virtual) now — which moves, so the second look proves it alive.
        var store = new LiveHolderStore(inner, () => new FlowLeaseObservation(holderLease, time.GetUtcNow().UtcDateTime + options.ExecutionLeaseDuration));
        var transport = new RecordingDelayedTransport();
        await using var provider = new ServiceCollection().AddSingleton<IFlowStateStore>(store).BuildServiceProvider();
        var logger = new RecordingThrowingLogger<DurableFlowExecutor> { ThrowOnMessageContaining = "past the live holder's lease); acknowledging this delivery" };
        var executor = Executor(provider, EnqueueRecordingBuilder().Object, logger, workerTransport: transport, timeProvider: time, options: options);

        Task delivery;
        using (WorkerJobScope.Enter(job))
            delivery = executor.ExecuteAsync("r65-own-job");
        for (var step = 0; step < 120 && !delivery.IsCompleted; step++)
        {
            await WaitForArmedTimerOrCompletionAsync(time, delivery);
            if (!delivery.IsCompleted)
                time.Advance(TimeSpan.FromSeconds(1));
        }

        Assert.True(delivery.IsCompleted, "The own-job delivery never resolved.");
        Assert.Null(await Record.ExceptionAsync(() => delivery));
        var (hop, _) = Assert.Single(transport.Delayed);
        Assert.Equal(job.JobId, hop.JobId);
    }

    /// <summary>
    /// A context site round 64 missed: the ancestor-extension walk's "ancestor gone" warning runs
    /// after this run's own sleep checkpoint and inside the walk whose failure abandons the park.
    /// Pre-fix failure: the timer throws the logger's exception instead of parking, and no wake-up
    /// is published.
    /// </summary>
    [Fact]
    public async Task ALongTimerPark_UnderAGoneAncestor_StillParks_WhenTheAncestorGoneWarningThrows()
    {
        var clock = new VirtualTimeProvider();
        var store = new InMemoryFlowStateStore();
        var state = new FlowState
        {
            FlowId = "r65-orphan:child",
            Status = FlowRunStatus.Running,
            ParentFlowId = "r65-orphan",
            ParentStepName = "child"
        };
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        var options = new DurableFlowOptions();
        await using var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, state.FlowId!, options, NullLogger.Instance, clock))!;
        var builder = EnqueueRecordingBuilder();
        var logger = new RecordingThrowingLogger<DurableFlowContext> { ThrowOnMessageContaining = "has no state (expired or deleted)" };
        var context = new DurableFlowContext(
            state, store, builder.Object, new AsyncResponseContextPropagation([]), options,
            Mock.Of<IAsyncResponseSubscriber>(), null, logger, lease, clock, workerTransport: DelayedTransport());

        await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => context.DelayAsync("nap", TimeSpan.FromHours(1)));
        Assert.Equal(1, DelayedEnqueues(builder));
    }

    // ---------------------------------------------------------------------------------------------
    // L-15 — a lost lease is not a step failure.

    /// <summary>
    /// A write that is not this attempt's lands while a step body runs (a lost-subscriber recovery,
    /// a failure signal, an operator suspending the run): the step's checkpoint is refused, and the
    /// documented compensation filter must not take that for the step failing. Pre-fix failure: a
    /// plain InvalidOperationException, which the filter caught — the compensation ran.
    /// </summary>
    [Fact]
    public async Task ARefusedCheckpoint_SurfacesAsLeaseLost_AndTheDocumentedCompensationFilterSkipsIt()
    {
        var clock = new VirtualTimeProvider();
        var store = new InMemoryFlowStateStore();
        var state = new FlowState { FlowId = "r65-refused-checkpoint", Status = FlowRunStatus.Running };
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        await using var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, state.FlowId!, new DurableFlowOptions(), NullLogger.Instance, clock))!;
        var context = Context(state, store, lease, clock: clock);

        var (compensated, surfaced) = await ChargeWithDocumentedCompensationAsync(context, async () =>
        {
            var current = (await store.LoadAsync(state.FlowId!))!;
            var revision = current.Revision;
            current.Revision = revision + 1;
            current.LastMessage = "written by someone else";
            Assert.True(await store.TryUpdateAsync(state.FlowId!, current, revision, TimeSpan.FromMinutes(5), leaseId: null));
        });

        Assert.False(compensated, "The compensation filter took a refused checkpoint for a failed charge.");
        var lost = Assert.IsType<DurableFlowLeaseLostException>(surfaced);
        Assert.Equal(state.FlowId, lost.FlowId);
        Assert.IsAssignableFrom<InvalidOperationException>(lost);
    }

    /// <summary>
    /// The lease itself lapsing while the step body runs (a store outage or a pause longer than the
    /// lease): ThrowIfLost after the body. Pre-fix failure: as above.
    /// </summary>
    [Fact]
    public async Task ALeaseLapsingDuringAStepBody_SurfacesAsLeaseLost_AndTheDocumentedCompensationFilterSkipsIt()
    {
        var clock = new VirtualTimeProvider();
        var inner = new InMemoryFlowStateStore();
        var state = new FlowState { FlowId = "r65-lapsed-lease", Status = FlowRunStatus.Running };
        Assert.True(await inner.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        var store = new NoRenewStore(inner);
        var options = new DurableFlowOptions();
        await using var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, state.FlowId!, options, NullLogger.Instance, clock))!;
        var context = Context(state, store, lease, clock: clock);

        var (compensated, surfaced) = await ChargeWithDocumentedCompensationAsync(context, () =>
        {
            clock.Advance(options.ExecutionLeaseDuration + TimeSpan.FromSeconds(1));
            return Task.CompletedTask;
        });

        Assert.False(compensated, "The compensation filter took a lapsed lease for a failed charge.");
        var lost = Assert.IsType<DurableFlowLeaseLostException>(surfaced);
        Assert.Contains("lost its execution lease", lost.Message, StringComparison.Ordinal);
    }

    /// <summary>The docs' compensation example, with the filter docs/durable-flows.md now prescribes.</summary>
    private static async Task<(bool Compensated, Exception? Surfaced)> ChargeWithDocumentedCompensationAsync(IDurableFlowContext flow, Func<Task> charge)
    {
        var compensated = false;
        try
        {
            try
            {
                await flow.StepAsync("charge", charge);
            }
            catch (Exception ex) when (ex is not (OperationCanceledException or DurableFlowLeaseLostException))
            {
                compensated = true;
                throw;
            }
        }
        catch (Exception ex)
        {
            return (compensated, ex);
        }

        return (compensated, null);
    }

    // ---------------------------------------------------------------------------------------------
    // Harness.

    public sealed record LateInput(int N);

    public sealed class LineageResult : IAsyncResponsePayload
    {
        public bool Ok { get; set; }

        public RecoveryAction OnRecovery() => Ok ? RecoveryAction.Resume : RecoveryAction.Fail;
    }

    /// <summary>The docs' best-effort cookbook step, followed by a long timer and a last step.</summary>
    public sealed class BestEffortFlow : IDurableFlow<LateInput>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, LateInput input)
        {
            try
            {
                await flow.AwaitStepAsync<LineageResult>("lineage", _ => Task.CompletedTask, timeout: TimeSpan.FromMinutes(30));
            }
            catch (Exception ex) when (ex is not (OperationCanceledException or DurableFlowLeaseLostException))
            {
                await flow.ReportProgressAsync($"lineage failed ({ex.GetType().Name}); continuing");
            }

            await flow.DelayAsync("settle", TimeSpan.FromHours(6));
            await flow.StepAsync("notify", () => Task.CompletedTask);
        }
    }

    /// <summary>The same awaited step without a catch: a timeout fails the attempt.</summary>
    public sealed class StrictFlow : IDurableFlow<LateInput>
    {
        private static readonly ConcurrentDictionary<string, int> TriggerCounts = new(StringComparer.Ordinal);

        public static int Triggers(string flowId) => TriggerCounts.TryGetValue(flowId, out var count) ? count : 0;

        public async Task ExecuteAsync(IDurableFlowContext flow, LateInput input)
        {
            var flowId = flow.FlowId;
            await flow.AwaitStepAsync<LineageResult>(
                "lineage",
                _ =>
                {
                    TriggerCounts.AddOrUpdate(flowId, 1, static (_, count) => count + 1);
                    return Task.CompletedTask;
                },
                timeout: TimeSpan.FromMinutes(30));
            await flow.StepAsync("notify", () => Task.CompletedTask);
        }
    }

    public sealed class OneStepFlow : IDurableFlow<TestFlowInput>
    {
        public Task ExecuteAsync(IDurableFlowContext flow, TestFlowInput input)
            => flow.StepAsync("only", () => Task.FromResult(input.TenantId));
    }

    public sealed class SleepingFlow : IDurableFlow<TestFlowInput>
    {
        public Task ExecuteAsync(IDurableFlowContext flow, TestFlowInput input)
            => flow.DelayAsync("nap", TimeSpan.FromHours(1));
    }

    private static Task<FlowTestHarness> StartHarnessAsync<TFlow>(Action<InMemoryWorkerTransportOptions>? transport = null)
        where TFlow : class, IDurableFlow<LateInput>
        => FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddScoped<TFlow>();
            // Durable channels keep a registration for 7 days by default; the in-memory channel's
            // 30-minute default would expire the surviving one before the 30-minute step timeout.
            options.Channel = channel => channel.RecoveryStateExpiry = TimeSpan.FromDays(2);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<TFlow, LateInput>();
            if (transport is not null)
                options.Transport = transport;
        });

    private static async Task<string> WaitForAsync(Func<Task<string?>> probe, string what)
    {
        var guard = Stopwatch.StartNew();
        while (true)
        {
            if (await probe() is { } value)
                return value;
            if (guard.Elapsed > TimeSpan.FromSeconds(10))
                throw new TimeoutException($"Timed out waiting for {what}.");
            await Task.Delay(5);
        }
    }

    private static FlowState RunningState(string flowId, Type flowType) => new()
    {
        FlowId = flowId,
        FlowTypeName = flowType.FullName,
        InputTypeName = flowType == typeof(StrictFlow) ? typeof(LateInput).FullName : typeof(TestFlowInput).FullName,
        InputJson = flowType == typeof(StrictFlow) ? JsonSerializer.Serialize(new LateInput(1)) : JsonSerializer.Serialize(new TestFlowInput(1)),
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    private static DurableFlowExecutor Executor(
        ServiceProvider provider,
        IAsyncResponseBuilder builder,
        Microsoft.Extensions.Logging.ILogger<DurableFlowExecutor> logger,
        IEnumerable<IDurableFlowExecutionObserver>? observers = null,
        IWorkerTransport? workerTransport = null,
        TimeProvider? timeProvider = null,
        DurableFlowOptions? options = null)
        => new(
            provider.GetRequiredService<IServiceScopeFactory>(),
            builder,
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            new AsyncResponseContextPropagation([]),
            options ?? new DurableFlowOptions(),
            logger,
            timeProvider: timeProvider,
            observers: observers,
            workerTransport: workerTransport);

    private static DurableFlowContext Context(
        FlowState state,
        IFlowStateStore store,
        FlowExecutionLease lease,
        IAsyncResponseSubscriber? subscriber = null,
        TimeProvider? clock = null)
        => new(
            state,
            store,
            Mock.Of<IAsyncResponseBuilder>(),
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            subscriber ?? Mock.Of<IAsyncResponseSubscriber>(),
            null,
            NullLogger.Instance,
            lease,
            clock);

    private static Mock<IAsyncResponseBuilder> EnqueueRecordingBuilder()
    {
        var builder = new Mock<IAsyncResponseBuilder>();
        builder.Setup(instance => instance.EnqueueWorkerAsync(
                It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        builder.Setup(instance => instance.EnqueueWorkerAsync(
                It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        return builder;
    }

    private static int DelayedEnqueues(Mock<IAsyncResponseBuilder> builder)
        => builder.Invocations.Count(invocation =>
            invocation.Method.Name == nameof(IAsyncResponseBuilder.EnqueueWorkerAsync)
            && invocation.Arguments.Any(argument => argument is TimeSpan));

    private static IDelayedWorkerTransport DelayedTransport()
    {
        var transport = new Mock<IDelayedWorkerTransport>();
        transport.SetupGet(instance => instance.MaxPublishDelay).Returns(TimeSpan.FromDays(7));
        return transport.Object;
    }

    private static async Task<FlowExecutionLease> AcquireLeaseAsync(IFlowStateStore store, string flowId)
        => Assert.IsType<FlowExecutionLease>(await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, flowId, new DurableFlowOptions(), NullLogger.Instance));

    private static IAsyncResponseSubscriber SubscriberReturning(Task<OperationResult> responseTask)
    {
        var waiter = new Mock<IAsyncResponseWaiter<OperationResult>>();
        waiter.SetupGet(instance => instance.ResponseTask).Returns(responseTask);
        waiter.Setup(instance => instance.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var subscriber = new Mock<IAsyncResponseSubscriber>();
        subscriber.Setup(instance => instance.CreateResponseWaiter<OperationResult>(
                It.IsAny<string>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(),
                It.IsAny<TimeSpan?>()))
            .ReturnsAsync(waiter.Object);
        return subscriber.Object;
    }

    private static async Task WaitForArmedTimerOrCompletionAsync(VirtualTimeProvider time, Task delivery)
    {
        var guard = Stopwatch.StartNew();
        while (time.NextTimerDueAt is null && !delivery.IsCompleted)
        {
            if (guard.Elapsed > TimeSpan.FromSeconds(10))
                throw new TimeoutException("The contention poll armed no delay on the virtual clock.");
            await Task.Delay(1);
        }
    }

    private sealed class RunRecorder : IDurableFlowExecutionObserver
    {
        public ConcurrentQueue<DurableFlowRunEvent> AttemptFailed { get; } = new();

        public ConcurrentQueue<DurableFlowRunEvent> Finished { get; } = new();

        public ValueTask OnRunAttemptFailedAsync(DurableFlowRunEvent run)
        {
            AttemptFailed.Enqueue(run);
            return default;
        }

        public ValueTask OnRunFinishedAsync(DurableFlowRunEvent run)
        {
            Finished.Enqueue(run);
            return default;
        }
    }

    private sealed class RecordingDelayedTransport : IDelayedWorkerTransport
    {
        public ConcurrentQueue<(WorkerJobEnvelope Job, TimeSpan Delay)> Delayed { get; } = new();

        public TimeSpan MaxPublishDelay => TimeSpan.FromMinutes(15);

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default)
        {
            Delayed.Enqueue((job, delay));
            return Task.CompletedTask;
        }
    }

    /// <summary>A store whose lease is held by a live holder: never acquirable, observed as <paramref name="observe"/> says.</summary>
    private sealed class LiveHolderStore(InMemoryFlowStateStore inner, Func<FlowLeaseObservation> observe) : IFlowStateStore
    {
        public Task<FlowLeaseObservation?> ObserveLeaseAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult<FlowLeaseObservation?>(observe());

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => Task.FromResult(false);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);
    }

    /// <summary>The in-memory store, refusing every lease renewal (the store side of a lapsed lease).</summary>
    private sealed class NoRenewStore(InMemoryFlowStateStore inner) : IFlowStateStore
    {
        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => Task.FromResult(false);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);
    }

    /// <summary>
    /// Rejects the FIRST lease-fenced completion save for <c>stepName</c> as a store answers a lost
    /// lease, after applying <paramref name="takeover"/> to the persisted ledger through a lease-less
    /// write — so the rescue's reload sees what a real takeover wrote (Round34RegressionTests' shape).
    /// </summary>
    private sealed class TakeoverStore(string stepName, Action<FlowState> takeover) : IFlowStateStore
    {
        private readonly InMemoryFlowStateStore _inner = new();
        private int _takeovers;

        public int Takeovers => Volatile.Read(ref _takeovers);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => _inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => _inner.LoadAsync(flowId, cancellationToken);

        public async Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
        {
            if (leaseId is not null
                && Volatile.Read(ref _takeovers) == 0
                && state.Steps is { } steps
                && steps.TryGetValue(stepName, out var step)
                && step.Completed)
            {
                Interlocked.Increment(ref _takeovers);
                var current = await _inner.LoadAsync(flowId, cancellationToken)
                    ?? throw new InvalidOperationException("The ledger under test vanished.");
                takeover(current);
                var revision = current.Revision;
                current.Revision = revision + 1;
                Assert.True(await _inner.TryUpdateAsync(flowId, current, revision, ttl, leaseId: null, cancellationToken));
                return false;
            }

            return await _inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);
        }

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => _inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => _inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => _inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => _inner.TryDeleteAsync(flowId, cancellationToken);
    }
}
