using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Linq.Expressions;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class DurableFlowExecutorCoverageTests
{
    [Fact]
    public async Task ExecuteAsync_WhenLeaseIsAcquiredButStateDisappears_ReturnsAndReleasesLease()
    {
        var store = new Mock<IFlowStateStore>();
        store.Setup(instance => instance.TryAcquireLeaseAsync(
                "missing",
                It.IsAny<string>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        store.Setup(instance => instance.LoadAsync("missing", It.IsAny<CancellationToken>()))
            .ReturnsAsync((FlowState?)null);
        store.Setup(instance => instance.ReleaseLeaseAsync(
                "missing",
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        await using var harness = CreateHarness(store.Object);

        await harness.Executor.ExecuteAsync("missing");

        store.Verify(instance => instance.ReleaseLeaseAsync(
            "missing",
            It.IsAny<string>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_WhenFlowSwallowsSuspension_StillLeavesParentSuspended()
    {
        var store = new InMemoryFlowStateStore();
        var state = State("swallowed-suspension");
        state.FlowTypeName = typeof(SwallowingSuspensionFlow).FullName;
        state.InputTypeName = typeof(TestFlowInput).FullName;
        state.InputJson = JsonSerializer.Serialize(new TestFlowInput(42));
        await CreateAsync(store, state);
        await using var harness = CreateHarness(
            store,
            services => services.AddSingleton<SwallowingSuspensionFlow>());

        await harness.Executor.ExecuteAsync(state.FlowId!);

        var parent = await store.LoadAsync(state.FlowId!);
        Assert.Equal(FlowRunStatus.Running, parent!.Status);
        Assert.Contains("suspended", parent.LastMessage, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(await store.LoadAsync($"{state.FlowId}:child"));
        harness.Builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ResumeRecoverAndFail_CoverMissingTerminalDuplicateAndParentPaths()
    {
        var store = new InMemoryFlowStateStore();
        await CreateAsync(store, State("running"));
        await CreateAsync(store, State("terminal", FlowRunStatus.Succeeded));
        await CreateAsync(store, State("recover-no-steps"), withSteps: false);
        await CreateAsync(store, State("recover-unmatched"));
        await CreateAsync(store, new FlowState
        {
            FlowId = "terminal-child",
            Status = FlowRunStatus.Failed,
            ParentFlowId = "parent",
            ParentStepName = "child"
        });
        await using var harness = CreateHarness(store);

        await Assert.ThrowsAsync<ArgumentException>(() => harness.Executor.ResumeAsync(" "));
        await harness.Executor.ResumeAsync("missing");
        await harness.Executor.ResumeAsync("terminal");
        await harness.Executor.ResumeAsync("running");

        await Assert.ThrowsAsync<ArgumentException>(() =>
            harness.Executor.RecoverAsync(" ", new object(), "correlation"));
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            harness.Executor.RecoverAsync("running", null!, "correlation"));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            harness.Executor.RecoverAsync("running", new object(), " "));
        await harness.Executor.RecoverAsync("missing", new object(), "correlation");
        await harness.Executor.RecoverAsync("terminal", new object(), "correlation");
        await harness.Executor.RecoverAsync("recover-no-steps", new object(), "correlation");
        await harness.Executor.RecoverAsync("recover-unmatched", new object(), "different");

        await Assert.ThrowsAsync<ArgumentException>(() =>
            harness.Executor.FailAsync(" ", new InvalidOperationException()));
        await Assert.ThrowsAsync<ArgumentNullException>(() => harness.Executor.FailAsync("running", null!));
        await harness.Executor.FailAsync("missing", new InvalidOperationException("missing"));
        await harness.Executor.FailAsync("terminal-child", new InvalidOperationException("duplicate"));

        // Four enqueues: ResumeAsync("running"), FailAsync("terminal-child")'s parent notify, and
        // the two Running-flow recoveries with no matching pending step ("recover-no-steps",
        // "recover-unmatched") — those re-enqueue to cover the checkpoint/enqueue crash window.
        // The terminal-flow recovery deliberately does not enqueue.
        harness.Builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Exactly(4));
    }

    [Fact]
    public async Task RecoverAsync_SettlingAPendingStep_NotifiesStepCompleted()
    {
        // The recovered response settles the awaited step INSIDE RecoverAsync; the replayed
        // execution then short-circuits the memoized step, so this notification is the only
        // completion observers (and the Testing probe's step waiters) ever get for it. Pre-fix
        // it was never raised: the step went Waiting → (silently) done.
        var store = new InMemoryFlowStateStore();
        await CreateAsync(store, State("recover-notify"));
        await CreateAsync(store, State("recover-suspended", FlowRunStatus.Suspended));
        var observer = new RecordingObserver();
        await using var harness = CreateHarness(store, observers: [observer]);

        await harness.Executor.RecoverAsync("recover-notify", new object(), "expected");

        var completed = Assert.Single(observer.Completed);
        Assert.Equal("recover-notify", completed.FlowId);
        Assert.Equal("step", completed.StepName);
        Assert.Equal(DurableFlowStepKind.Awaited, completed.Kind);
        Assert.Equal("expected", completed.CorrelationId);

        // A Suspended run checkpoints the recovered payload too (it exists nowhere else) — the
        // completion is observable even though the run is deliberately not woken.
        await harness.Executor.RecoverAsync("recover-suspended", new object(), "expected");
        Assert.Equal(2, observer.Completed.Count);
        Assert.Equal("recover-suspended", observer.Completed[1].FlowId);
    }

    [Fact]
    public async Task RecoverFailAndResume_OnALaggingPresentCopy_LookAgainAuthoritativelyBeforeAcknowledging()
    {
        // Regression: three executor decisions acknowledge a delivery WITHOUT writing — a recovered
        // response that matches no pending step, a correlation-scoped failure for an id no step is
        // pending on, a resume of a run that does not read Running — so no revision fence corrects
        // a stale read behind them. On a store whose loads can serve an older copy of a present
        // ledger (Cosmos session reads from a process that never got the holder's session token),
        // the holder's breadcrumb checkpoint was not visible yet: the recovered payload and the
        // failure were dropped for good, and an operator's resume of a run set back to Running was
        // ignored. Each now looks again through IFlowStateStore.LoadCurrentAsync before concluding.
        var current = new InMemoryFlowStateStore();
        foreach (var flowId in new[] { "recover-lag", "fail-lag" })
        {
            await CreateAsync(current, PendingOn(flowId, "pre-breadcrumb", revision: 0));
            Assert.True(await current.TryUpdateAsync(flowId, PendingOn(flowId, "breadcrumb", revision: 1), 0, TimeSpan.FromMinutes(5)));
        }

        await CreateAsync(current, State("resume-lag"));
        var store = new LaggingReadStore(current)
        {
            Stale =
            {
                ["recover-lag"] = () => PendingOn("recover-lag", "pre-breadcrumb", revision: 0),
                ["fail-lag"] = () => PendingOn("fail-lag", "pre-breadcrumb", revision: 0),
                ["resume-lag"] = () => State("resume-lag", FlowRunStatus.Suspended)
            }
        };
        await using var harness = CreateHarness(store);

        await harness.Executor.RecoverAsync("recover-lag", new object(), "breadcrumb");
        var recovered = (await current.LoadAsync("recover-lag"))!.Steps!["step"];
        Assert.True(recovered.Completed);
        Assert.NotNull(recovered.ResultJson);
        Assert.Null(recovered.PendingCorrelationId);

        await harness.Executor.FailAsync("fail-lag", new InvalidOperationException("remote failure"), "breadcrumb");
        Assert.Equal(FlowRunStatus.Failed, (await current.LoadAsync("fail-lag"))!.Status);

        await harness.Executor.ResumeAsync("resume-lag");

        // The recovered run is woken, and so is the resumed one.
        harness.Builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Exactly(2));
        Assert.Equal(3, store.CurrentLoads);

        static FlowState PendingOn(string flowId, string correlationId, long revision)
        {
            var state = State(flowId);
            state.Revision = revision;
            state.Steps = new Dictionary<string, FlowStepState> { ["step"] = new() { PendingCorrelationId = correlationId } };
            return state;
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AFailureSignal_ReadingALaggingSuspendedCopy_LooksAgainAuthoritatively_BeforeIgnoringIt(bool correlationScoped)
    {
        // Precommit review (F3): a failure signal for a run that reads Suspended is ignored — by
        // design, while the run really is suspended. But Suspended is not terminal: right after an
        // operator set the run back to Running, a replica behind that write still shows Suspended,
        // and the failure its resumed run waits on was acknowledged and dropped. Nothing is written
        // on that path, so no fence corrects the read; it looks again through LoadCurrentAsync.
        var flowId = $"fail-suspended-lag-{correlationScoped}";
        var current = new InMemoryFlowStateStore();
        await CreateAsync(current, PendingOn(flowId, FlowRunStatus.Running));
        var store = new LaggingReadStore(current)
        {
            Stale = { [flowId] = () => PendingOn(flowId, FlowRunStatus.Suspended) }
        };
        await using var harness = CreateHarness(store);

        if (correlationScoped)
            await harness.Executor.FailAsync(flowId, new InvalidOperationException("remote failure"), "breadcrumb");
        else
            await harness.Executor.FailAsync(flowId, new InvalidOperationException("remote failure"));

        var failed = (await current.LoadAsync(flowId))!;
        Assert.Equal(FlowRunStatus.Failed, failed.Status);
        Assert.Equal("remote failure", failed.LastMessage);
        Assert.Equal(1, store.CurrentLoads);

        // A run that really is Suspended still ignores the signal — after the one current look.
        var suspendedId = $"fail-suspended-really-{correlationScoped}";
        await CreateAsync(current, PendingOn(suspendedId, FlowRunStatus.Suspended));
        if (correlationScoped)
            await harness.Executor.FailAsync(suspendedId, new InvalidOperationException("remote failure"), "breadcrumb");
        else
            await harness.Executor.FailAsync(suspendedId, new InvalidOperationException("remote failure"));

        Assert.Equal(FlowRunStatus.Suspended, (await current.LoadAsync(suspendedId))!.Status);
        Assert.Equal(2, store.CurrentLoads);

        static FlowState PendingOn(string flowId, FlowRunStatus status)
        {
            var state = State(flowId, status);
            state.Steps = new Dictionary<string, FlowStepState> { ["step"] = new() { PendingCorrelationId = "breadcrumb" } };
            return state;
        }
    }

    [Fact]
    public async Task TheOperatorsResume_ReadingALaggingSuspendedCopy_LooksAgainAuthoritatively_BeforeIgnoringIt()
    {
        // Fixpoint r2 (S1#2): round 43 gave the executor's ResumeAsync a current re-read, but not
        // IDurableFlows.ResumeAsync — the documented un-park ("set it back to Running and call
        // ResumeAsync"). An operator script that set the run back to Running through one process
        // and resumed it through another, whose replica still showed Suspended, had the resume
        // ignored: nothing written, so no fence corrected the read, and the run stayed Running with
        // nothing queued while the operator believed it resumed.
        var current = new InMemoryFlowStateStore();
        await CreateAsync(current, State("operator-resume-lag"));
        await CreateAsync(current, State("operator-resume-suspended", FlowRunStatus.Suspended));
        var store = new LaggingReadStore(current)
        {
            Stale = { ["operator-resume-lag"] = () => State("operator-resume-lag", FlowRunStatus.Suspended) }
        };
        var services = new ServiceCollection();
        services.AddSingleton<IFlowStateStore>(store);
        await using var provider = services.BuildServiceProvider();
        var builder = new Mock<IAsyncResponseBuilder>();
        builder.Setup(instance => instance.EnqueueWorkerAsync(
                It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var flows = new DurableFlowService(
            provider.GetRequiredService<IServiceScopeFactory>(),
            builder.Object,
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            NullLogger<DurableFlowService>.Instance);

        await flows.ResumeAsync("operator-resume-lag");

        builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Once);
        Assert.Equal(1, store.CurrentLoads);

        // A run that really is Suspended is still left alone — after the one current look.
        await flows.ResumeAsync("operator-resume-suspended");

        builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Once);
        Assert.Equal(2, store.CurrentLoads);
    }

    // ---------------------------------------------------------------------------------------
    // Round 48, F2: the authoritative second look was taken only when the first one read Running
    // or Suspended. A finished run's id can be reused — its ledger deleted, a new run started
    // under the same id — and a lagging copy then still shows the PREVIOUS run, Succeeded or
    // Failed, while the new run is live. Every decision below writes nothing, so no fence
    // corrected it: the response, the failure or the wake-up was acknowledged against the wrong
    // generation of the ledger.
    // ---------------------------------------------------------------------------------------

    [Theory]
    [InlineData(FlowRunStatus.Succeeded)]
    [InlineData(FlowRunStatus.Failed)]
    public async Task ARecoveredResponse_ReadingThePreviousRunsTerminalLedger_UnderAReusedId_IsCheckpointedOnTheNewRun(FlowRunStatus previousRun)
    {
        var flowId = $"recover-reused-{previousRun}";
        var current = new InMemoryFlowStateStore();
        await CreateAsync(current, PendingOn(flowId, "new-correlation"));
        var store = new LaggingReadStore(current)
        {
            Stale = { [flowId] = () => PreviousRun(flowId, previousRun) }
        };
        var observer = new RecordingObserver();
        await using var harness = CreateHarness(store, observers: [observer]);

        await harness.Executor.RecoverAsync(flowId, new object(), "new-correlation");

        var recovered = (await current.LoadAsync(flowId))!.Steps!["step"];
        Assert.True(recovered.Completed);
        Assert.NotNull(recovered.ResultJson);
        Assert.Null(recovered.PendingCorrelationId);
        Assert.Equal(1, store.CurrentLoads);
        Assert.Equal("new-correlation", Assert.Single(observer.Completed).CorrelationId);
        harness.Builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Once);

        // A run that really is finished still ignores the response — after the one current look.
        var finishedId = $"recover-finished-{previousRun}";
        await CreateAsync(current, FinishedRun(finishedId, previousRun));
        await harness.Executor.RecoverAsync(finishedId, new object(), "new-correlation");

        Assert.Equal(2, store.CurrentLoads);
        Assert.Equal(previousRun, (await current.LoadAsync(finishedId))!.Status);
        harness.Builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [InlineData(FlowRunStatus.Succeeded, true)]
    [InlineData(FlowRunStatus.Succeeded, false)]
    [InlineData(FlowRunStatus.Failed, true)]
    [InlineData(FlowRunStatus.Failed, false)]
    public async Task AFailureSignal_ReadingThePreviousRunsTerminalLedger_UnderAReusedId_FailsTheNewRun(FlowRunStatus previousRun, bool correlationScoped)
    {
        var flowId = $"fail-reused-{previousRun}-{correlationScoped}";
        var current = new InMemoryFlowStateStore();
        await CreateAsync(current, PendingOn(flowId, "new-correlation"));
        var store = new LaggingReadStore(current)
        {
            Stale = { [flowId] = () => PreviousRun(flowId, previousRun) }
        };
        var observer = new AttemptFailureObserver();
        await using var harness = CreateHarness(store, observers: [observer]);

        if (correlationScoped)
            await harness.Executor.FailAsync(flowId, new InvalidOperationException("remote failure"), "new-correlation");
        else
            await harness.Executor.FailAsync(flowId, new InvalidOperationException("remote failure"));

        var failed = (await current.LoadAsync(flowId))!;
        Assert.Equal(FlowRunStatus.Failed, failed.Status);
        Assert.Equal("remote failure", failed.LastMessage);
        Assert.Equal(1, store.CurrentLoads);

        // Observers hear the NEW run fail — not the previous run's outcome a second time.
        var finished = Assert.Single(observer.Finished);
        Assert.Equal(FlowRunStatus.Failed, finished.Status);
        Assert.Equal("remote failure", finished.Message);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AFailureSignal_ForAFinishedRun_StillReNotifies_AfterTheCurrentLook(bool ledgerGoneByTheSecondLook)
    {
        // The at-least-once re-notify of a finished run survives the second look — also when the
        // ledger is gone by then (deleted or expired in between; and a test double mocking the
        // store answers the default interface member with null).
        const string flowId = "fail-finished";
        var current = new InMemoryFlowStateStore();
        if (!ledgerGoneByTheSecondLook)
            await CreateAsync(current, FinishedRun(flowId, FlowRunStatus.Succeeded));
        var store = new LaggingReadStore(current)
        {
            Stale = { [flowId] = () => PreviousRun(flowId, FlowRunStatus.Succeeded) }
        };
        var observer = new AttemptFailureObserver();
        await using var harness = CreateHarness(store, observers: [observer]);

        await harness.Executor.FailAsync(flowId, new InvalidOperationException("late failure"));

        Assert.Equal(1, store.CurrentLoads);
        Assert.Equal(FlowRunStatus.Succeeded, Assert.Single(observer.Finished).Status);
    }

    [Fact]
    public async Task AFailureSignal_WhoseSecondLookLosesTheLedgerUnderItsWrite_NotifiesNothing()
    {
        // Pre-commit review of round 48. The second look finds the run live and marks it failed —
        // in memory; the write is refused and the ledger is gone by the next read (deleted, or
        // expired, under it). The failure was never persisted, so nobody is told of it: the
        // terminal status on the in-memory copy must not pass for a finished first look.
        const string flowId = "fail-lost-write";
        var looks = 0;
        var store = new Mock<IFlowStateStore>();
        store.Setup(instance => instance.LoadAsync(flowId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => PreviousRun(flowId, FlowRunStatus.Succeeded));
        store.Setup(instance => instance.LoadCurrentAsync(flowId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => Interlocked.Increment(ref looks) == 1 ? PendingOn(flowId, "new-correlation") : null);
        store.Setup(instance => instance.TryUpdateAsync(
                flowId, It.IsAny<FlowState>(), It.IsAny<long>(), It.IsAny<TimeSpan>(), It.IsAny<string?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        var observer = new AttemptFailureObserver();
        await using var harness = CreateHarness(store.Object, observers: [observer]);

        await harness.Executor.FailAsync(flowId, new InvalidOperationException("remote failure"), "new-correlation");

        Assert.Equal(2, looks);
        Assert.Empty(observer.Finished);
        harness.Builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Theory]
    [InlineData(FlowRunStatus.Succeeded)]
    [InlineData(FlowRunStatus.Failed)]
    [InlineData(FlowRunStatus.Suspended)]
    public async Task AWakeUp_ReadingThePreviousRunsLedger_UnderAReusedId_ExecutesTheNewRun(FlowRunStatus previousRun)
    {
        var flowId = $"execute-reused-{previousRun}";
        var current = new InMemoryFlowStateStore();
        await CreateAsync(current, Round40FlowLeaseTestSupport.RunnableState(flowId), withSteps: false);
        var store = new LaggingReadStore(current)
        {
            Stale = { [flowId] = () => PreviousRun(flowId, previousRun) }
        };
        var observer = new AttemptFailureObserver();
        await using var harness = CreateHarness(
            store,
            services => services.AddSingleton<Round40FlowLeaseTestSupport.CountingFlow>(),
            observers: [observer]);

        // Only the first plain load lags: once the second look has found the new run, the run's
        // own reads are its own writes.
        store.StaleReadsLeft = 1;
        await harness.Executor.ExecuteAsync(flowId);

        Assert.Equal(1, harness.Provider.GetRequiredService<Round40FlowLeaseTestSupport.CountingFlow>().Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await current.LoadAsync(flowId))!.Status);
        Assert.Equal(1, store.CurrentLoads);
        Assert.Equal("Flow completed.", Assert.Single(observer.Finished).Message);
    }

    [Fact]
    public async Task AWakeUp_ForARunThatReallyIsFinished_IsStillSkipped_AndReNotified()
    {
        const string flowId = "execute-finished";
        var current = new InMemoryFlowStateStore();
        await CreateAsync(current, FinishedRun(flowId, FlowRunStatus.Succeeded), withSteps: false);
        var store = new LaggingReadStore(current);
        var observer = new AttemptFailureObserver();
        await using var harness = CreateHarness(
            store,
            services => services.AddSingleton<Round40FlowLeaseTestSupport.CountingFlow>(),
            observers: [observer]);

        await harness.Executor.ExecuteAsync(flowId);

        Assert.Equal(0, harness.Provider.GetRequiredService<Round40FlowLeaseTestSupport.CountingFlow>().Executions);
        Assert.Equal(1, store.CurrentLoads);
        Assert.Equal(FlowRunStatus.Succeeded, Assert.Single(observer.Finished).Status);
    }

    [Fact]
    public async Task AContendedWakeUp_ReadingThePreviousRunsTerminalLedger_UnderAReusedId_IsNotAcknowledgedAsFinished()
    {
        // The wake-up cannot take the lease (a holder that died inside its window) and, without
        // it, reads the previous run's ledger. Acknowledging it as "already Succeeded" dropped
        // what may be the new run's only wake-up and re-notified the old run's outcome. It keeps
        // waiting on the lease instead, and hands the delivery back when the wait runs out.
        const string flowId = "contended-reused";
        var current = new InMemoryFlowStateStore();
        await CreateAsync(current, Round40FlowLeaseTestSupport.RunnableState(flowId), withSteps: false);
        var store = new LaggingReadStore(current)
        {
            Stale = { [flowId] = () => PreviousRun(flowId, FlowRunStatus.Succeeded) },
            HeldBy = new FlowLeaseObservation("dead-holder", DateTime.UtcNow)
        };
        var observer = new AttemptFailureObserver();
        await using var harness = CreateHarness(
            store,
            services => services.AddSingleton<Round40FlowLeaseTestSupport.CountingFlow>(),
            observers: [observer],
            options: new DurableFlowOptions
            {
                ExecutionLeaseDuration = TimeSpan.FromMilliseconds(200),
                ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(50)
            });

        await Assert.ThrowsAsync<DurableFlowLeaseContendedException>(
            () => harness.Executor.ExecuteAsync(flowId).WaitAsync(TimeSpan.FromSeconds(30)));

        Assert.Empty(observer.Finished);
        Assert.True(store.CurrentLoads >= 1);
        Assert.Equal(FlowRunStatus.Running, (await current.LoadAsync(flowId))!.Status);
    }

    private static FlowState PendingOn(string flowId, string correlationId)
    {
        var state = State(flowId);
        state.Steps = new Dictionary<string, FlowStepState> { ["step"] = new() { PendingCorrelationId = correlationId } };
        return state;
    }

    /// <summary>
    /// The ledger the id's PREVIOUS run left behind, as a lagging copy still shows it: finished,
    /// its step long settled, ten checkpoints in.
    /// </summary>
    private static FlowState PreviousRun(string flowId, FlowRunStatus status)
    {
        var state = FinishedRun(flowId, status);
        state.Revision = 10;
        return state;
    }

    /// <summary>A finished run as the store itself holds it (a ledger is created at revision zero).</summary>
    private static FlowState FinishedRun(string flowId, FlowRunStatus status)
    {
        var state = State(flowId, status);
        state.LastMessage = "previous run";
        state.Steps = new Dictionary<string, FlowStepState> { ["step"] = new() { Completed = true, ResultJson = "{}" } };
        return state;
    }

    /// <summary>
    /// Plain loads served by a replica that has not applied the holder's latest checkpoint for the
    /// ids in <see cref="Stale"/>; <see cref="LoadCurrentAsync"/> and every write go to the
    /// authoritative store.
    /// </summary>
    private sealed class LaggingReadStore(InMemoryFlowStateStore current) : IFlowStateStore
    {
        public Dictionary<string, Func<FlowState>> Stale { get; } = new(StringComparer.Ordinal);

        public int CurrentLoads;

        /// <summary>Plain loads that still lag; afterwards they are current. Unbounded by default.</summary>
        public int StaleReadsLeft = int.MaxValue;

        /// <summary>When set, the lease is held by this holder and cannot be acquired.</summary>
        public FlowLeaseObservation? HeldBy { get; init; }

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => Stale.TryGetValue(flowId, out var stale) && Interlocked.Decrement(ref StaleReadsLeft) >= 0
                ? Task.FromResult<FlowState?>(stale())
                : current.LoadAsync(flowId, cancellationToken);

        public Task<FlowLeaseObservation?> ObserveLeaseAsync(string flowId, CancellationToken cancellationToken = default)
            => HeldBy is not null ? Task.FromResult<FlowLeaseObservation?>(HeldBy) : current.ObserveLeaseAsync(flowId, cancellationToken);

        public Task<FlowState?> LoadCurrentAsync(string flowId, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref CurrentLoads);
            return current.LoadAsync(flowId, cancellationToken);
        }

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => current.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => current.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => HeldBy is not null ? Task.FromResult(false) : current.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => current.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => current.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => current.TryDeleteAsync(flowId, cancellationToken);
    }

    private sealed class RecordingObserver : IDurableFlowExecutionObserver
    {
        public List<DurableFlowStepEvent> Completed { get; } = [];

        public ValueTask OnStepStartingAsync(DurableFlowStepEvent step) => default;

        public ValueTask OnStepWaitingAsync(DurableFlowStepEvent step) => default;

        public ValueTask OnStepCompletedAsync(DurableFlowStepEvent step)
        {
            Completed.Add(step);
            return default;
        }

        public ValueTask OnRunFinishedAsync(DurableFlowRunEvent run) => default;
    }

    [Fact]
    public async Task FailAsync_NotifiesRunFinishedObservers_BeforeTheParentWakeUp()
    {
        // Regression (r24): FailAsync's success branch enqueued the parent's wake-up BEFORE
        // raising run-finished to observers — the opposite order from ExecuteAsync and from its
        // own already-terminal branch. An observer throw after the parent was already notified
        // (the documented crash-injection contract) made the redelivered FailAsync take the
        // already-terminal branch and enqueue the parent a SECOND time; and even without a throw
        // the parent could resume, memoize the failed child step and finish before the child's
        // terminal event was recorded.
        var store = new InMemoryFlowStateStore();
        var state = State("fail-order");
        state.ParentFlowId = "parent-1";
        state.ParentStepName = "child-step";
        await CreateAsync(store, state);
        await using var harness = CreateHarness(store, observers: [new ThrowingRunObserver()]);

        // The observer throws out of the run-finished notification, so FailAsync faults...
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            harness.Executor.FailAsync("fail-order", new InvalidOperationException("child failed")));

        // ...and because run-finished now precedes the parent notification, the parent wake-up
        // was NOT yet enqueued — the redelivered FailAsync notifies it via the terminal branch,
        // exactly once, instead of twice.
        harness.Builder.Verify(
            builder => builder.EnqueueWorkerAsync(
                It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    private sealed class ThrowingRunObserver : IDurableFlowExecutionObserver
    {
        public ValueTask OnStepStartingAsync(DurableFlowStepEvent step) => default;

        public ValueTask OnStepWaitingAsync(DurableFlowStepEvent step) => default;

        public ValueTask OnStepCompletedAsync(DurableFlowStepEvent step) => default;

        public ValueTask OnRunFinishedAsync(DurableFlowRunEvent run)
            => throw new InvalidOperationException("observer boom");
    }

    [Fact]
    public async Task ExecuteAsync_RetryableFailure_RaisesRunAttemptFailed_AndStillPropagates()
    {
        // Regression: an attempt that failed retryably ended without ANY observer event — no
        // step-completed for its parked steps and no run-finished (the run is not terminal) — so
        // an observer tracking in-process state (the Testing harness's quiesce probe) leaked an
        // entry per failed attempt for the rest of the incarnation.
        var store = new InMemoryFlowStateStore();
        var state = State("attempt-fails");
        state.FlowTypeName = typeof(ThrowingBodyFlow).FullName;
        state.InputTypeName = typeof(TestFlowInput).FullName;
        await CreateAsync(store, state);
        var observer = new AttemptFailureObserver();
        await using var harness = CreateHarness(
            store,
            services => services.AddSingleton<ThrowingBodyFlow>(),
            observers: [observer]);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.Executor.ExecuteAsync("attempt-fails"));

        Assert.Equal("flow body exploded", thrown.Message);
        var failed = Assert.Single(observer.AttemptFailures);
        Assert.Equal("attempt-fails", failed.FlowId);
        Assert.Equal(FlowRunStatus.Running, failed.Status);
        Assert.Empty(observer.Finished);
    }

    [Fact]
    public async Task ExecuteAsync_ObserverThrowInRunAttemptFailed_IsSwallowed_KeepingTheOriginalFailure()
    {
        // The attempt's own exception is what the transport retries on; an observer throw in the
        // best-effort attempt-failed notification must never replace it.
        var store = new InMemoryFlowStateStore();
        var state = State("attempt-fails-observer");
        state.FlowTypeName = typeof(ThrowingBodyFlow).FullName;
        state.InputTypeName = typeof(TestFlowInput).FullName;
        await CreateAsync(store, state);
        var observer = new AttemptFailureObserver { ThrowOnAttemptFailed = true };
        await using var harness = CreateHarness(
            store,
            services => services.AddSingleton<ThrowingBodyFlow>(),
            observers: [observer]);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.Executor.ExecuteAsync("attempt-fails-observer"));

        Assert.Equal("flow body exploded", thrown.Message);
        Assert.Single(observer.AttemptFailures);
    }

    public sealed class ThrowingBodyFlow : IDurableFlow<TestFlowInput>
    {
        public Task ExecuteAsync(IDurableFlowContext context, TestFlowInput input)
            => throw new InvalidOperationException("flow body exploded");
    }

    private sealed class AttemptFailureObserver : IDurableFlowExecutionObserver
    {
        public List<DurableFlowRunEvent> AttemptFailures { get; } = [];
        public List<DurableFlowRunEvent> Finished { get; } = [];
        public bool ThrowOnAttemptFailed { get; init; }

        public ValueTask OnRunAttemptFailedAsync(DurableFlowRunEvent run)
        {
            AttemptFailures.Add(run);
            return ThrowOnAttemptFailed
                ? throw new InvalidOperationException("attempt observer boom")
                : default;
        }

        public ValueTask OnRunFinishedAsync(DurableFlowRunEvent run)
        {
            Finished.Add(run);
            return default;
        }
    }

    [Theory]
    [InlineData(InvalidFlowDefinition.MissingFlowType, "no flow type name")]
    [InlineData(InvalidFlowDefinition.UnresolvableFlowType, "Cannot resolve flow type")]
    [InlineData(InvalidFlowDefinition.MissingInputType, "no input type name")]
    [InlineData(InvalidFlowDefinition.UnregisteredFlow, "is not registered in DI")]
    [InlineData(InvalidFlowDefinition.IncompatibleFlow, "does not implement")]
    public async Task ExecuteAsync_InvalidPersistedDefinitions_ReportActionableFailure(
        InvalidFlowDefinition definition,
        string expectedMessage)
    {
        var store = new InMemoryFlowStateStore();
        var state = State("invalid-definition");
        state.FlowTypeName = definition switch
        {
            InvalidFlowDefinition.MissingFlowType => null,
            InvalidFlowDefinition.UnresolvableFlowType => "Missing.Flow.Type, Missing.Assembly",
            InvalidFlowDefinition.IncompatibleFlow => typeof(IncompatibleFlow).FullName,
            _ => typeof(TestOnboardingFlow).FullName
        };
        state.InputTypeName = definition == InvalidFlowDefinition.MissingInputType
            ? null
            : typeof(TestFlowInput).FullName;
        state.InputJson = "{\"TenantId\":1}";
        await CreateAsync(store, state);
        await using var harness = CreateHarness(
            store,
            services => services.AddSingleton<IncompatibleFlow>());

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            harness.Executor.ExecuteAsync(state.FlowId!));

        Assert.Contains(expectedMessage, exception.Message, StringComparison.OrdinalIgnoreCase);
        var persisted = await store.LoadAsync(state.FlowId!);
        Assert.Equal(FlowRunStatus.Running, persisted!.Status);
        Assert.Contains(expectedMessage, persisted.LastMessage, StringComparison.OrdinalIgnoreCase);
    }

    private static async Task CreateAsync(InMemoryFlowStateStore store, FlowState state, bool withSteps = true)
    {
        if (withSteps && state.Steps is null)
        {
            state.Steps = new Dictionary<string, FlowStepState>
            {
                ["step"] = new() { PendingCorrelationId = "expected" }
            };
        }

        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
    }

    private static FlowState State(string flowId, FlowRunStatus status = FlowRunStatus.Running)
        => new()
        {
            FlowId = flowId,
            Status = status
        };

    public sealed class IncompatibleFlow { }

    public sealed class SwallowingSuspensionFlow : IDurableFlow<TestFlowInput>
    {
        public async Task ExecuteAsync(IDurableFlowContext context, TestFlowInput input)
        {
            try
            {
                await context.AwaitChildFlowAsync<TestOnboardingFlow, TestFlowInput>("child", input);
            }
            catch (Exception)
            {
                // User flow code can swallow the internal suspension signal; the executor must
                // still honor the context's persisted IsSuspended flag.
            }
        }
    }

    public enum InvalidFlowDefinition
    {
        MissingFlowType,
        UnresolvableFlowType,
        MissingInputType,
        UnregisteredFlow,
        IncompatibleFlow
    }

    private sealed class Harness : IAsyncDisposable
    {
        private readonly ServiceProvider _provider;

        public Harness(ServiceProvider provider, DurableFlowExecutor executor, Mock<IAsyncResponseBuilder> builder)
        {
            _provider = provider;
            Executor = executor;
            Builder = builder;
        }

        public DurableFlowExecutor Executor { get; }
        public Mock<IAsyncResponseBuilder> Builder { get; }
        public IServiceProvider Provider => _provider;

        public ValueTask DisposeAsync() => _provider.DisposeAsync();
    }

    private static Harness CreateHarness(
        IFlowStateStore store,
        Action<IServiceCollection>? configure = null,
        IEnumerable<IDurableFlowExecutionObserver>? observers = null,
        DurableFlowOptions? options = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(store);
        configure?.Invoke(services);
        var provider = services.BuildServiceProvider();
        var builder = new Mock<IAsyncResponseBuilder>();
        builder.Setup(instance => instance.EnqueueWorkerAsync(
                It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var executor = new DurableFlowExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            builder.Object,
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            new AsyncResponseContextPropagation([]),
            options ?? new DurableFlowOptions(),
            NullLogger<DurableFlowExecutor>.Instance,
            observers: observers);

        return new Harness(provider, executor, builder);
    }
}
