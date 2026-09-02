using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

public sealed record Round33FlowInput(int TenantId);

/// <summary>
/// Cross-execution observation point for <see cref="Round33RemoteStepFlow"/> (singleton in DI):
/// every trigger invocation with the correlation id it was handed, plus a resettable signal for
/// the next one.
/// </summary>
public sealed class Round33FlowProbe
{
    private readonly List<string> _triggered = [];

    public TaskCompletionSource<string> TriggerFired { get; private set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public IReadOnlyList<string> TriggeredCorrelationIds
    {
        get { lock (_triggered) return _triggered.ToArray(); }
    }

    public Task RecordTrigger(string correlationId)
    {
        lock (_triggered)
            _triggered.Add(correlationId);
        TriggerFired.TrySetResult(correlationId);
        return Task.CompletedTask;
    }

    public void ResetTriggerSignal()
        => TriggerFired = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
}

/// <summary>One awaited remote step, then a value-bag write so completion is observable in the ledger.</summary>
public sealed class Round33RemoteStepFlow(Round33FlowProbe _probe) : IDurableFlow<Round33FlowInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, Round33FlowInput input)
    {
        var result = await flow.AwaitStepAsync<OperationResult>(
            "remote-op",
            trigger: _probe.RecordTrigger,
            timeout: TimeSpan.FromSeconds(10));

        await flow.SetValueAsync("final-status", result.Status);
    }
}

/// <summary>Regression pins for the round-33 review's durable-flow and in-memory worker findings.</summary>
public sealed class Round33RegressionTests
{
    // ---------------------------------------------------------------------------------------------
    // F1 — a won response whose fenced completion save is rejected (lease lost) must still be
    //      checkpointed through the lease-less compare-and-swap.

    /// <summary>
    /// Round 33, finding 1: an awaited step WON its response, but the lease-fenced completion save
    /// was rejected (the lease expired or was taken over in the same instant), which lands in the
    /// general catch of <c>AwaitStepCoreAsync</c> rather than the cancellation branch. Pre-fix that
    /// catch only marked the step faulted and re-threw — and the fault save itself was refused by
    /// the now-lost lease — so the claimed, channel-acked payload was persisted NOWHERE: the
    /// takeover execution re-attached to the consumed correlation id, burned the step timeout, and
    /// re-sent the remote request. The fix routes a won response through the same lease-less
    /// checkpoint the cancellation branch already used before raising the takeover signal.
    /// Pre-fix failure: the persisted step is still pending on the breadcrumb (not completed).
    /// </summary>
    [Fact]
    public async Task AwaitStep_LeaseLostAtTheFencedCompletionSave_CheckpointsTheWonResponseWithoutTheLease()
    {
        var store = new RejectFencedCompletionStore("remote");
        var state = new FlowState { FlowId = "r33-lease-lost-at-completion" };
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));

        var won = new OperationResult { Status = OperationStatus.Completed, Message = "won-before-the-lease-was-lost" };
        var triggered = new List<string>();
        InvalidOperationException surfaced;

        await using (var lease = await AcquireLeaseAsync(store, state.FlowId!))
        {
            var context = CreateContext(state, store, SubscriberReturning(Task.FromResult(won)), lease);

            // The takeover signal still propagates: this execution may not continue past the step.
            surfaced = await Assert.ThrowsAsync<InvalidOperationException>(() => context.AwaitStepAsync<OperationResult>(
                "remote",
                correlationId =>
                {
                    triggered.Add(correlationId);
                    return Task.CompletedTask;
                }));

            Assert.True(lease.LostToken.IsCancellationRequested);
        }

        Assert.Equal(1, store.RejectedFencedCompletionSaves);
        Assert.Contains("lost its execution lease", surfaced.Message, StringComparison.Ordinal);
        Assert.Single(triggered);

        // The PERSISTED ledger is what the takeover reloads: the won response must be in it, written
        // by the lease-less compare-and-swap (the only write the store let through after the fence).
        var persisted = await store.LoadAsync(state.FlowId!);
        Assert.NotNull(persisted);
        var step = persisted!.Steps!["remote"];
        Assert.True(step.Completed);
        Assert.Null(step.PendingCorrelationId);
        Assert.Null(step.PendingPayloadTypeFullName);
        Assert.False(step.Faulted);
        Assert.Contains("won-before-the-lease-was-lost", step.ResultJson, StringComparison.Ordinal);

        // The takeover execution resumes from that checkpoint: no waiter, no re-attach to a consumed
        // id, and — above all — no second send of the remote request.
        var consumedIdNeverAnswers = new TaskCompletionSource<OperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        consumedIdNeverAnswers.TrySetCanceled();
        await using (var lease = await AcquireLeaseAsync(store, state.FlowId!))
        {
            var takeover = CreateContext(persisted, store, SubscriberReturning(consumedIdNeverAnswers.Task), lease);

            var replayed = await takeover.AwaitStepAsync<OperationResult>(
                "remote",
                correlationId =>
                {
                    triggered.Add(correlationId);
                    return Task.CompletedTask;
                });

            Assert.Equal(OperationStatus.Completed, replayed.Status);
            Assert.Equal("won-before-the-lease-was-lost", replayed.Message);
        }

        Assert.Single(triggered);
    }

    /// <summary>
    /// Rejects the FIRST lease-fenced write that carries <c>stepName</c> as completed — the awaited
    /// step's completion save — exactly as a store answers a lease that expired or was taken over
    /// (<c>false</c> from the compare-and-swap, no exception). Every lease-less write (the
    /// recovery/rescue compare-and-swap passes <c>leaseId: null</c>) and every other write passes
    /// through to a real in-memory store, so the test observes what the takeover would reload.
    /// </summary>
    private sealed class RejectFencedCompletionStore(string _stepName) : IFlowStateStore
    {
        private readonly InMemoryFlowStateStore _inner = new();
        private int _rejected;

        public int RejectedFencedCompletionSaves => Volatile.Read(ref _rejected);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => _inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => _inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryUpdateAsync(
            string flowId,
            FlowState state,
            long expectedRevision,
            TimeSpan ttl,
            string? leaseId = null,
            CancellationToken cancellationToken = default)
        {
            if (leaseId is not null
                && Volatile.Read(ref _rejected) == 0
                && state.Steps is { } steps
                && steps.TryGetValue(_stepName, out var step)
                && step.Completed)
            {
                Interlocked.Increment(ref _rejected);
                return Task.FromResult(false);
            }

            return _inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);
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

    // ---------------------------------------------------------------------------------------------
    // F2 — a non-cancellation throw AFTER the trigger sent (an observer crash while the step is
    //      reported waiting) must keep the breadcrumb so the redelivery re-attaches.

    /// <summary>
    /// Round 33, finding 2: a throw from OUTSIDE the wait — here an
    /// <see cref="IDurableFlowExecutionObserver.OnStepWaitingAsync"/> that fails — after the trigger
    /// had already sent the remote request landed in the general catch, which marked the step
    /// <c>Faulted</c>. The redelivered execution therefore minted a NEW correlation id and called
    /// the trigger AGAIN: the double remote send the breadcrumb contract exists to prevent (and
    /// worse than a real crash, which leaves the breadcrumb intact). The fix keeps the breadcrumb
    /// when the wait itself did not fault, so the next execution re-attaches to the same id.
    /// Pre-fix failure: the step is persisted faulted, the redelivery reports a fresh correlation
    /// id while waiting, and the trigger has run twice.
    /// </summary>
    [Fact]
    public async Task AwaitStep_ObserverThrowsAfterTheTriggerSent_KeepsTheBreadcrumbSoRedeliveryReattaches()
    {
        var observer = new ThrowOnceWhileWaitingObserver();
        await using var provider = CreateProvider(services => services.AddSingleton<IDurableFlowExecutionObserver>(observer));
        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<Round33FlowProbe>();

        var flowId = await flows.StartAsync<Round33RemoteStepFlow, Round33FlowInput>(new Round33FlowInput(7));

        // Execution 1: the trigger sends; the observer then throws while the step is reported
        // waiting. The attempt fails retriably (that is what makes the transport redeliver).
        await Assert.ThrowsAsync<ObserverCrashException>(() => executor.ExecuteAsync(flowId));
        var sent = Assert.Single(probe.TriggeredCorrelationIds);

        var parked = await flows.GetStateAsync(flowId);
        Assert.NotNull(parked);
        Assert.Equal(FlowRunStatus.Running, parked!.Status);
        var step = parked.Steps!["remote-op"];
        Assert.False(step.Faulted);
        Assert.Equal(sent, step.PendingCorrelationId);

        // Execution 2 (the transport's redelivery): re-attaches to the SAME correlation id and does
        // not run the trigger again.
        var redelivered = executor.ExecuteAsync(flowId);
        var reattached = await observer.SecondWaiting.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(sent, reattached);
        Assert.Single(probe.TriggeredCorrelationIds);

        // Answering the ORIGINAL id completes the run.
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, sent);
        await redelivered.WaitAsync(TimeSpan.FromSeconds(5));

        var final = await flows.GetStateAsync(flowId);
        Assert.NotNull(final);
        Assert.Equal(FlowRunStatus.Succeeded, final!.Status);
        Assert.True(final.Steps!["remote-op"].Completed);
        Assert.Single(probe.TriggeredCorrelationIds);
    }

    private sealed class ObserverCrashException() : Exception("observer crashed while the step was reported waiting");

    /// <summary>
    /// Throws from the FIRST <see cref="OnStepWaitingAsync"/> — after the awaited step's trigger has
    /// sent — and reports the correlation id of the second one (the redelivery's wait). Written here
    /// because <c>FlowTestHarness</c> injects crashes only at step start/completion, never at the
    /// waiting event.
    /// </summary>
    private sealed class ThrowOnceWhileWaitingObserver : IDurableFlowExecutionObserver
    {
        private int _calls;

        public TaskCompletionSource<string?> SecondWaiting { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask OnStepWaitingAsync(DurableFlowStepEvent step)
        {
            if (Interlocked.Increment(ref _calls) == 1)
                throw new ObserverCrashException();

            SecondWaiting.TrySetResult(step.CorrelationId);
            return default;
        }
    }

    // ---------------------------------------------------------------------------------------------
    // F3 — the lost-subscriber FAILURE callback an awaited step registers must be scoped to its
    //      correlation id, so a stale registration cannot terminally fail a run live on another id.

    /// <summary>
    /// Round 33, finding 3 (executor level): the new correlation-scoped
    /// <see cref="IDurableFlowExecutor.FailAsync(string, Exception, string)"/> fails the run ONLY
    /// while a step is still pending on that correlation id; a failure for a superseded or settled
    /// id is stale and ignored — the same scoping <c>RecoverAsync</c> applies to the success target.
    /// Pre-fix: the overload did not exist (this test does not compile on the old code; the two
    /// tests below are the old-compiling proofs of the same finding).
    /// </summary>
    [Fact]
    public async Task FailAsync_WithACorrelationId_IgnoresAStaleIdAndFailsOnlyTheLiveOne()
    {
        await using var provider = CreateProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var store = provider.GetRequiredService<IFlowStateStore>();

        var flowId = await flows.StartAsync<Round33RemoteStepFlow, Round33FlowInput>(new Round33FlowInput(7));
        Assert.True(await FlowStateConcurrency.MutateAsync(
            store,
            flowId,
            TimeSpan.FromMinutes(5),
            timeProvider: null,
            state =>
            {
                state.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
                {
                    ["remote-op"] = new() { PendingCorrelationId = "live-cid" }
                };
                return true;
            }));

        await executor.FailAsync(flowId, new ApplicationException("late error for a superseded id"), "stale-cid");

        var stillRunning = await flows.GetStateAsync(flowId);
        Assert.NotNull(stillRunning);
        Assert.Equal(FlowRunStatus.Running, stillRunning!.Status);
        Assert.Equal("live-cid", stillRunning.Steps!["remote-op"].PendingCorrelationId);

        await executor.FailAsync(flowId, new ApplicationException("the live id's error"), "live-cid");

        var failed = await flows.GetStateAsync(flowId);
        Assert.NotNull(failed);
        Assert.Equal(FlowRunStatus.Failed, failed!.Status);
        Assert.Contains("the live id's error", failed.LastMessage, StringComparison.Ordinal);
    }

    /// <summary>
    /// Round 33, finding 3 (wiring): the failure callback an awaited step registers with the
    /// recoverable subscriber must carry the correlation-id placeholder next to the exception
    /// placeholder, so the lost-subscriber dispatcher binds it to the scoped overload. Pre-fix the
    /// registered call was <c>FailAsync(flowId, Placeholder.Exception())</c> — two parameters, no
    /// correlation id — so the callback failed the run whatever id the late error was for.
    /// Pre-fix failure: the registered callback has two parameters.
    /// </summary>
    [Fact]
    public async Task AwaitStep_RegistersACorrelationScopedFailureCallback()
    {
        ReflectionCallDto? failure = null;
        var waiter = new Mock<IAsyncResponseWaiter<OperationResult>>();
        waiter.SetupGet(instance => instance.ResponseTask).Returns(Task.FromResult(new OperationResult { Status = OperationStatus.Completed }));
        waiter.Setup(instance => instance.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var recoverable = new Mock<IRecoverableAsyncResponseSubscriber>();
        recoverable
            .Setup(subscriber => subscriber.CreateRecoverableResponseWaiter<OperationResult>(
                It.IsAny<string>(),
                It.IsAny<ReflectionCallDto?>(),
                It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(),
                It.IsAny<TimeSpan?>()))
            .Callback<string, ReflectionCallDto?, ReflectionCallDto?, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (_, _, failureCallback, _, _) => failure = failureCallback)
            .ReturnsAsync(waiter.Object);

        var state = new FlowState { FlowId = "r33-scoped-failure-callback" };
        var store = new InMemoryFlowStateStore();
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        await using var lease = await AcquireLeaseAsync(store, state.FlowId!);
        var context = CreateContext(state, store, Mock.Of<IAsyncResponseSubscriber>(), lease, recoverable.Object);

        await context.AwaitStepAsync<OperationResult>("remote", _ => Task.CompletedTask);

        Assert.NotNull(failure);
        Assert.Equal(typeof(IDurableFlowExecutor).FullName, failure!.ServiceInterfaceFullName);
        Assert.Equal(nameof(IDurableFlowExecutor.FailAsync), failure.MethodName);
        Assert.Collection(
            failure.Params,
            parameter => Assert.Equal("r33-scoped-failure-callback", parameter.Value?.ToString()),
            parameter => Assert.Equal(PlaceholderType.Exception, parameter.Placeholder),
            parameter => Assert.Equal(PlaceholderType.CorrelationId, parameter.Placeholder));
    }

    /// <summary>
    /// Round 33, finding 3 (behavioral, through the real in-memory channel): worker A arms the step
    /// on C1 — registering C1's lost-subscriber callbacks — and dies mid-await, leaving that
    /// registration behind. The step's deadline elapses while no execution is live, so the
    /// replacement faults the step and the next delivery restarts it FRESH on C2. A late error for
    /// C1 then arrives with no live subscriber and takes the lost-subscriber route into the flow's
    /// failure callback. Pre-fix that callback was the unscoped <c>FailAsync(flowId, exception)</c>,
    /// which terminally failed a run that was live — and later answered — on C2.
    /// Pre-fix failure: the run is <c>Failed</c> after the stale error instead of <c>Running</c>.
    /// </summary>
    [Fact]
    public async Task StaleLostSubscriberFailure_ForASupersededCorrelationId_LeavesTheLiveRunRunning()
    {
        var clock = new VirtualTimeProvider();
        await using var provider = CreateProvider(services => services.AddSingleton<TimeProvider>(clock));
        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<Round33FlowProbe>();
        var channel = Assert.IsType<InMemoryAsyncResponseChannel>(provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>());

        var flowId = await flows.StartAsync<Round33RemoteStepFlow, Round33FlowInput>(new Round33FlowInput(7));

        // Worker A: arms the step on C1 (the recovery registration for C1 now exists), then dies
        // mid-await — its waiter is abandoned, exactly like a crashed process's: the ResponseTask
        // is canceled and the registration is deliberately left in place.
        var workerA = executor.ExecuteAsync(flowId);
        var c1 = await probe.TriggerFired.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await channel.AbandonAllAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => workerA);

        var abandoned = await flows.GetStateAsync(flowId);
        Assert.NotNull(abandoned);
        Assert.Equal(c1, abandoned!.Steps!["remote-op"].PendingCorrelationId);
        Assert.NotNull(abandoned.Steps["remote-op"].AwaitDeadlineUtc);

        // The step's deadline elapses while no execution is live: the next delivery faults the
        // step for a fresh restart (the persisted-deadline branch), and the one after that — the
        // replacement, worker B — restarts it on C2. C1's registration is still armed.
        clock.Advance(TimeSpan.FromSeconds(11));
        await Assert.ThrowsAsync<TimeoutException>(() => executor.ExecuteAsync(flowId));
        Assert.True((await flows.GetStateAsync(flowId))!.Steps!["remote-op"].Faulted);

        probe.ResetTriggerSignal();
        var workerB = executor.ExecuteAsync(flowId);
        var c2 = await probe.TriggerFired.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.NotEqual(c1, c2);

        // A late error for the SUPERSEDED id: no live subscriber for C1, so the lost-subscriber
        // route invokes the failure callback worker A registered.
        await publisher.SetException(new ApplicationException("late error for the superseded id"), c1);

        var live = await flows.GetStateAsync(flowId);
        Assert.NotNull(live);
        Assert.Equal(FlowRunStatus.Running, live!.Status);
        Assert.Equal(c2, live.Steps!["remote-op"].PendingCorrelationId);

        // The run is live on C2 and completes when C2 is answered.
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, c2);
        await workerB.WaitAsync(TimeSpan.FromSeconds(5));

        var final = await flows.GetStateAsync(flowId);
        Assert.NotNull(final);
        Assert.Equal(FlowRunStatus.Succeeded, final!.Status);
        Assert.Equal([c1, c2], probe.TriggeredCorrelationIds);
    }

    // ---------------------------------------------------------------------------------------------
    // F4 — a job that fails DURING the in-memory worker's shutdown drain keeps its remaining
    //      delivery attempts.

    public interface IDrainRetryProbe
    {
        Task RunAsync(string jobId);
    }

    /// <summary>
    /// Attempt 1 parks on a gate (so the stop lands while the job is RUNNING) and then fails;
    /// attempt 2 succeeds. A 3-attempt budget therefore needs exactly two attempts.
    /// </summary>
    private sealed class DrainRetryProbe : IDrainRetryProbe
    {
        private int _attempts;

        public int Attempts => Volatile.Read(ref _attempts);
        public TaskCompletionSource FirstAttemptStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ReleaseFirstAttempt { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<string> Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task RunAsync(string jobId)
        {
            if (Interlocked.Increment(ref _attempts) == 1)
            {
                FirstAttemptStarted.TrySetResult();
                await ReleaseFirstAttempt.Task;
                throw new InvalidOperationException($"{jobId} transient failure during the drain");
            }

            Completed.TrySetResult(jobId);
        }
    }

    /// <summary>
    /// Round 33, finding 4: the in-memory worker's retry backoff was bound to the stopping token,
    /// but the shutdown drain IS that token's cancellation (<c>BeginShutdownDrain</c> is its
    /// registration) — so a job that failed once during the drain saw its backoff throw before it
    /// began and was dropped after attempt 1 with its remaining attempts unspent:
    /// <c>MaxDeliveryAttempts</c> was effectively 1 for the whole drain, stranding the durable flow
    /// behind the job exactly when lease contention peaks. The fix runs the bounded ladder during
    /// the drain with each backoff capped at <c>RetryBaseDelay</c> (a stop arriving DURING a sleep
    /// still drops, unchanged; unlimited attempts still drop, unchanged).
    /// Pre-fix failure: one attempt, the job dropped ("host shutdown interrupted its retry backoff").
    /// </summary>
    [Fact]
    public async Task InMemoryWorker_JobFailingDuringTheShutdownDrain_StillGetsItsRemainingAttempts()
    {
        var probe = new DrainRetryProbe();
        var logger = new CollectingLogger();
        var provider = new ServiceCollection()
            .AddSingleton<IDrainRetryProbe>(probe)
            .BuildServiceProvider();
        var transport = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions
        {
            MaxDeliveryAttempts = 3,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
            RetryMaxDelay = TimeSpan.FromMilliseconds(2)
        }));
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);
        var host = new InMemoryWorkerHost(transport, executor, logger.For<InMemoryWorkerHost>());
        await host.StartAsync(CancellationToken.None);

        await transport.PublishAsync(new WorkerJobEnvelope
        {
            CorrelationId = "wake-up",
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IDrainRetryProbe).FullName!,
                MethodName = nameof(IDrainRetryProbe.RunAsync),
                Params = [CallbackParam.ForValue("wake-up")]
            }
        });

        // The stop lands while attempt 1 is running: StopAsync cancels the stopping token
        // synchronously (the drain begins before it first awaits), so the attempt's failure is a
        // DRAIN-TIME failure — the token is already set when the retry ladder decides.
        await probe.FirstAttemptStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        using var cutoff = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var stop = host.StopAsync(cutoff.Token);
        probe.ReleaseFirstAttempt.TrySetResult();
        await stop;

        Assert.False(cutoff.IsCancellationRequested, "the stop should have drained on its own, not been cut off");
        Assert.True(probe.Completed.Task.IsCompletedSuccessfully, "the job must be retried and succeed during the drain");
        Assert.Equal(2, probe.Attempts);
        Assert.DoesNotContain(logger.Messages, message => message.Contains("dropping it", StringComparison.Ordinal));
        Assert.Contains(logger.Messages, message => message.Contains("during the shutdown drain", StringComparison.Ordinal));

        host.Dispose();
        await provider.DisposeAsync();
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers (private to this file by design — shared fixtures are left untouched).

    private static ServiceProvider CreateProvider(Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<Round33FlowProbe>();
        services.AddAsyncResponse()
            .WithInMemoryChannel(options =>
            {
                options.DefaultTimeout = TimeSpan.FromSeconds(10);
                options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
            })
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<Round33RemoteStepFlow, Round33FlowInput>();
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    private static DurableFlowContext CreateContext(
        FlowState state,
        IFlowStateStore store,
        IAsyncResponseSubscriber subscriber,
        FlowExecutionLease lease,
        IRecoverableAsyncResponseSubscriber? recoverableSubscriber = null)
        => new(
            state,
            store,
            Mock.Of<IAsyncResponseBuilder>(),
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            subscriber,
            recoverableSubscriber,
            NullLogger.Instance,
            lease);

    /// <summary>Acquires an execution lease on an existing ledger.</summary>
    private static async Task<FlowExecutionLease> AcquireLeaseAsync(IFlowStateStore store, string flowId)
    {
        var lease = await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(
            store,
            flowId,
            new DurableFlowOptions(),
            NullLogger.Instance);
        return Assert.IsType<FlowExecutionLease>(lease);
    }

    /// <summary>A plain (non-recoverable) subscriber whose every waiter hands back <paramref name="responseTask"/>.</summary>
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
}
