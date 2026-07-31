using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Linq.Expressions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>Cross-execution observation point for the test flows (singleton in DI).</summary>
public sealed class FlowProbe
{
    private int _computeRuns, _notifyRuns, _triggerRuns;

    public int ComputeRuns => Volatile.Read(ref _computeRuns);
    public int NotifyRuns => Volatile.Read(ref _notifyRuns);
    public int TriggerRuns => Volatile.Read(ref _triggerRuns);
    public volatile bool ThrowAfterCompute;
    public TaskCompletionSource<string> TriggerFired { get; private set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int BumpCompute() => Interlocked.Increment(ref _computeRuns);
    public void BumpNotify() => Interlocked.Increment(ref _notifyRuns);

    public Task RecordTrigger(string correlationId)
    {
        Interlocked.Increment(ref _triggerRuns);
        TriggerFired.TrySetResult(correlationId);
        return Task.CompletedTask;
    }

    public void ResetTriggerSignal()
        => TriggerFired = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
}

public sealed record TestFlowInput(int TenantId);

/// <summary>Local step (memoized) → awaited remote step (progress-aware) → value bag → local step.</summary>
public sealed class TestOnboardingFlow(FlowProbe _probe) : IDurableFlow<TestFlowInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, TestFlowInput input)
    {
        var stamp = await flow.StepAsync("compute-stamp", () => Task.FromResult(_probe.BumpCompute() * 100 + input.TenantId));

        if (_probe.ThrowAfterCompute)
            throw new InvalidOperationException("transient failure after compute");

        var result = await flow.AwaitStepAsync<OperationResult>(
            "remote-op",
            trigger: _probe.RecordTrigger,
            until: r => r.Status != OperationStatus.Running,
            timeout: TimeSpan.FromSeconds(10));

        await flow.SetValueAsync("stamp", stamp);
        await flow.SetValueAsync("final-status", result.Status);
        await flow.StepAsync("notify", () =>
        {
            _probe.BumpNotify();
            return Task.CompletedTask;
        });
    }
}

/// <summary>Declares itself terminally failed — must not be retried by the transport.</summary>
public sealed class TestTerminallyFailingFlow : IDurableFlow<TestFlowInput>
{
    public Task ExecuteAsync(IDurableFlowContext flow, TestFlowInput input)
        => throw new DurableFlowFailedException("business rule says no");
}

public class DurableFlowTests
{
    private static ServiceProvider CreateProvider(Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<FlowProbe>();
        services.AddScoped<TestOnboardingFlow>();
        services.AddScoped<TestTerminallyFailingFlow>();
        services.AddAsyncResponse()
            .WithInMemoryChannel(options =>
            {
                options.DefaultTimeout = TimeSpan.FromSeconds(10);
                options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
            })
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows();
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    [Theory]
    [InlineData(nameof(DurableFlowOptions.StateExpiry))]
    [InlineData(nameof(DurableFlowOptions.DefaultStepTimeout))]
    [InlineData(nameof(DurableFlowOptions.ExecutionLeaseDuration))]
    [InlineData(nameof(DurableFlowOptions.ExecutionLeaseRenewInterval))]
    [InlineData(nameof(DurableFlowOptions.ProgressPersistenceInterval))]
    public void DurableFlowOptions_InvalidValuesAreRejected(string propertyName)
    {
        var options = new DurableFlowOptions();
        switch (propertyName)
        {
            case nameof(DurableFlowOptions.StateExpiry):
                options.StateExpiry = TimeSpan.Zero;
                break;
            case nameof(DurableFlowOptions.DefaultStepTimeout):
                options.DefaultStepTimeout = TimeSpan.Zero;
                break;
            case nameof(DurableFlowOptions.ExecutionLeaseDuration):
                options.ExecutionLeaseDuration = TimeSpan.Zero;
                break;
            case nameof(DurableFlowOptions.ExecutionLeaseRenewInterval):
                options.ExecutionLeaseRenewInterval = options.ExecutionLeaseDuration;
                break;
            case nameof(DurableFlowOptions.ProgressPersistenceInterval):
                options.ProgressPersistenceInterval = TimeSpan.FromTicks(-1);
                break;
        }

        var exception = Assert.Throws<InvalidOperationException>(() => FlowStateConcurrency.ValidateOptions(options));

        Assert.Contains(propertyName, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WithInMemoryDurableFlows_ConfiguresCommonOptionsInItsOwnCallback()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows(options =>
            {
                options.StateExpiry = TimeSpan.FromDays(14);
                options.ExecutionLeaseDuration = TimeSpan.FromMinutes(2);
                options.ExecutionLeaseRenewInterval = TimeSpan.FromSeconds(30);
                options.ProgressPersistenceInterval = TimeSpan.FromSeconds(2);
            });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<DurableFlowOptions>();

        Assert.Same(
            provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<DurableFlowOptions>>().Value,
            options);
        Assert.Equal(TimeSpan.FromDays(14), options.StateExpiry);
        Assert.Equal(TimeSpan.FromMinutes(2), options.ExecutionLeaseDuration);
        Assert.Equal(TimeSpan.FromSeconds(30), options.ExecutionLeaseRenewInterval);
        Assert.Equal(TimeSpan.FromSeconds(2), options.ProgressPersistenceInterval);
    }

    [Fact]
    public async Task Flow_RunsSteps_AwaitsResponse_MemoizesAndSucceeds()
    {
        await using var provider = CreateProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<FlowProbe>();

        var flowId = await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new TestFlowInput(7));

        // Drive the executor directly (the in-memory worker host is not running in unit tests).
        var run = executor.ExecuteAsync(flowId);
        var correlationId = await probe.TriggerFired.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Running, Message = "halfway" }, correlationId);
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, correlationId);
        await run.WaitAsync(TimeSpan.FromSeconds(5));

        var state = await flows.GetStateAsync(flowId);
        Assert.NotNull(state);
        Assert.Equal(FlowRunStatus.Succeeded, state!.Status);
        Assert.True(state.Steps!["compute-stamp"].Completed);
        Assert.True(state.Steps["remote-op"].Completed);
        Assert.Null(state.Steps["remote-op"].PendingCorrelationId);
        Assert.True(state.Steps["notify"].Completed);
        Assert.Equal(1, probe.ComputeRuns);
        Assert.Equal(1, probe.TriggerRuns);
        Assert.Equal(1, probe.NotifyRuns);

        // The memoized step result and the value bag survived in the persisted state.
        Assert.Contains("107", state.Steps["compute-stamp"].ResultJson);
        Assert.Contains("stamp", state.Values!.Keys);
    }

    [Fact]
    public async Task WithDurableFlows_UsesScopedCustomStore()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddScoped<ScopedStoreDependency>();
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithDurableFlows<ScopedNullFlowStateStore>();

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();

        Assert.Null(await flows.GetStateAsync("missing-flow"));
        await executor.ResumeAsync("missing-flow");
    }

    [Fact]
    public async Task StartAsync_SameIdRequiresSameFlowAndSemanticallyIdenticalInput()
    {
        await using var provider = CreateProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();
        const string flowId = "idempotent-flow";

        Assert.Equal(flowId, await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new(7), flowId));
        Assert.Equal(flowId, await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new(7), flowId));

        var differentInput = await Assert.ThrowsAsync<InvalidOperationException>(
            () => flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new(8), flowId));
        Assert.Contains("different flow type or input", differentInput.Message, StringComparison.Ordinal);

        var differentFlow = await Assert.ThrowsAsync<InvalidOperationException>(
            () => flows.StartAsync<TestTerminallyFailingFlow, TestFlowInput>(new(7), flowId));
        Assert.Contains("different flow type or input", differentFlow.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task StartAsync_ExplicitEmptyFlowIdIsRejected(string flowId)
    {
        await using var provider = CreateProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();

        await Assert.ThrowsAsync<ArgumentException>(
            () => flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new(7), flowId));
    }

    [Fact]
    public async Task Flow_RetriableFailure_KeepsRunning_AndSecondRunSkipsCompletedSteps()
    {
        await using var provider = CreateProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<FlowProbe>();

        probe.ThrowAfterCompute = true;
        var flowId = await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new TestFlowInput(7));

        // Retriable exceptions propagate (that's what makes the transport redeliver the run).
        await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync(flowId));

        var state = await flows.GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Running, state!.Status);
        Assert.True(state.Steps!["compute-stamp"].Completed);
        Assert.Contains("transient", state.LastMessage);

        // "Redelivery": run again — the completed step must be skipped (memoized), not re-executed.
        probe.ThrowAfterCompute = false;
        var run = executor.ExecuteAsync(flowId);
        var correlationId = await probe.TriggerFired.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, correlationId);
        await run.WaitAsync(TimeSpan.FromSeconds(5));

        state = await flows.GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Succeeded, state!.Status);
        Assert.Equal(1, probe.ComputeRuns);
        Assert.Equal(2, state.Attempts);
    }

    [Fact]
    public async Task Flow_WithPendingBreadcrumb_ReattachesInsteadOfRetriggering()
    {
        await using var provider = CreateProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var store = provider.GetRequiredService<IFlowStateStore>();
        var probe = provider.GetRequiredService<FlowProbe>();

        var flowId = await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new TestFlowInput(7));

        // Simulate a previous process that completed the first step, triggered the remote
        // operation, persisted the breadcrumb, and died while awaiting.
        Assert.True(await FlowStateConcurrency.MutateAsync(
            store,
            flowId,
            TimeSpan.FromMinutes(5),
            state =>
            {
                state.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
                {
                    ["compute-stamp"] = new() { Completed = true, ResultJson = "707" },
                    ["remote-op"] = new() { PendingCorrelationId = "pre-baked-cid" }
                };
                return true;
            }));

        var run = executor.ExecuteAsync(flowId);

        // Give the re-attached waiter a moment to register, then answer the ORIGINAL correlation id.
        await Task.Delay(50);
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "pre-baked-cid");
        await run.WaitAsync(TimeSpan.FromSeconds(5));

        var final = await flows.GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Succeeded, final!.Status);
        Assert.Equal(0, probe.TriggerRuns);      // re-attach must NOT re-send the request
        Assert.Equal(0, probe.ComputeRuns);      // completed step skipped
        Assert.Equal(1, probe.NotifyRuns);       // flow continued past the awaited step
    }

    [Fact]
    public async Task Flow_TerminalFailure_MarksFailed_WithoutRethrow_AndBecomesNoOp()
    {
        await using var provider = CreateProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();

        var flowId = await flows.StartAsync<TestTerminallyFailingFlow, TestFlowInput>(new TestFlowInput(1));

        // DurableFlowFailedException is terminal: no rethrow, so the transport acks the job.
        await executor.ExecuteAsync(flowId);

        var state = await flows.GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Failed, state!.Status);
        Assert.Equal("business rule says no", state.LastMessage);

        // Terminal runs are idempotent no-ops for execute and resume alike.
        await executor.ExecuteAsync(flowId);
        await executor.ResumeAsync(flowId);
        Assert.Equal(FlowRunStatus.Failed, (await flows.GetStateAsync(flowId))!.Status);
    }

    [Fact]
    public async Task FailAsync_MarksRunFailed_AndExecuteBecomesNoOp()
    {
        await using var provider = CreateProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var probe = provider.GetRequiredService<FlowProbe>();

        var flowId = await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new TestFlowInput(7));

        await executor.FailAsync(flowId, new ApplicationException("remote step failed while nobody was listening"));

        var state = await flows.GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Failed, state!.Status);
        Assert.Contains("nobody was listening", state.LastMessage);

        await executor.ExecuteAsync(flowId);
        Assert.Equal(0, probe.ComputeRuns);
    }

    [Fact]
    public async Task RecoverAsync_CheckpointsTerminalPayloadBeforeResuming()
    {
        await using var provider = CreateProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var store = provider.GetRequiredService<IFlowStateStore>();
        var probe = provider.GetRequiredService<FlowProbe>();

        var flowId = await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new TestFlowInput(7));
        Assert.True(await FlowStateConcurrency.MutateAsync(
            store,
            flowId,
            TimeSpan.FromMinutes(5),
            state =>
            {
                state.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
                {
                    ["compute-stamp"] = new() { Completed = true, ResultJson = "707" },
                    ["remote-op"] = new() { PendingCorrelationId = "lost-correlation" }
                };
                return true;
            }));

        await executor.RecoverAsync(
            flowId,
            new OperationResult { Status = OperationStatus.Completed, Message = "recovered-result" },
            "lost-correlation");

        var recovered = await store.LoadAsync(flowId);
        var checkpoint = recovered!.Steps!["remote-op"];
        Assert.True(checkpoint.Completed);
        Assert.Null(checkpoint.PendingCorrelationId);
        Assert.Contains("recovered-result", checkpoint.ResultJson);

        await executor.ExecuteAsync(flowId).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(FlowRunStatus.Succeeded, (await flows.GetStateAsync(flowId))!.Status);
        Assert.Equal(0, probe.TriggerRuns);
        Assert.Equal(1, probe.NotifyRuns);
    }

    [Fact]
    public async Task AwaitStep_CancellationCancelsTheActualResponseWait()
    {
        var response = new TaskCompletionSource<OperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var waiter = new Mock<IAsyncResponseWaiter<OperationResult>>();
        waiter.SetupGet(instance => instance.ResponseTask).Returns(response.Task);
        waiter.Setup(instance => instance.DisposeAsync()).Returns(ValueTask.CompletedTask);

        var subscriber = new Mock<IAsyncResponseSubscriber>();
        subscriber.Setup(instance => instance.CreateResponseWaiter<OperationResult>(
                It.IsAny<string>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(),
                It.IsAny<TimeSpan?>()))
            .ReturnsAsync(waiter.Object);

        var state = new FlowState { FlowId = "cancel-flow" };
        var store = new InMemoryFlowStateStore();
        await using var lease = await CreateLeaseAsync(store, state);
        var context = new DurableFlowContext(
            state,
            store,
            Mock.Of<IAsyncResponseBuilder>(),
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            subscriber.Object,
            recoverableSubscriber: null,
            NullLogger.Instance,
            lease);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => context.AwaitStepAsync<OperationResult>(
            "remote",
            _ => Task.CompletedTask,
            timeout: TimeSpan.FromMinutes(1),
            cancellationToken: cancellation.Token));

        Assert.True(state.Steps!["remote"].Faulted);
        waiter.Verify(instance => instance.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task ReportProgress_CoalescesPersistenceAndFlushesLatestValue()
    {
        var state = new FlowState { FlowId = "progress-flow" };
        var store = new RecordingFlowStateStore();
        await using var lease = await CreateLeaseAsync(store, state);
        var context = new DurableFlowContext(
            state,
            store,
            Mock.Of<IAsyncResponseBuilder>(),
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions { ProgressPersistenceInterval = TimeSpan.FromHours(1) },
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            NullLogger.Instance,
            lease);

        await context.ReportProgressAsync("one");
        await context.ReportProgressAsync("two");
        await context.ReportProgressAsync("three");
        await context.FlushProgressAsync();

        Assert.Equal(["one", "three"], store.PersistedMessages);
    }

    [Fact]
    public async Task StartAsync_WithExistingFlowId_IsIdempotent()
    {
        await using var provider = CreateProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();

        var first = await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new TestFlowInput(7), flowId: "run-42");
        var created = (await flows.GetStateAsync("run-42"))!.CreatedAtUtc;

        var second = await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new TestFlowInput(7), flowId: "run-42");

        Assert.Equal("run-42", first);
        Assert.Equal("run-42", second);
        var state = await flows.GetStateAsync("run-42");
        Assert.Equal(created, state!.CreatedAtUtc);
        Assert.Contains("7", state.InputJson);   // an idempotent retry never overwrites the ledger
    }

    [Fact]
    public async Task AwaitStep_WithRecoverableChannel_RegistersExecutorCallbacks()
    {
        ReflectionCallDto? resume = null, failure = null;
        var waiter = new Mock<IAsyncResponseWaiter<OperationResult>>();
        waiter.SetupGet(w => w.ResponseTask).Returns(Task.FromResult(new OperationResult { Status = OperationStatus.Completed }));
        waiter.Setup(w => w.DisposeAsync()).Returns(ValueTask.CompletedTask);

        var recoverable = new Mock<IRecoverableAsyncResponseSubscriber>();
        recoverable
            .Setup(s => s.CreateRecoverableResponseWaiter<OperationResult>(
                It.IsAny<string>(),
                It.IsAny<ReflectionCallDto?>(),
                It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(),
                It.IsAny<TimeSpan?>()))
            .Callback<string, ReflectionCallDto?, ReflectionCallDto?, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (_, r, f, _, _) => { resume = r; failure = f; })
            .ReturnsAsync(waiter.Object);

        var state = new FlowState { FlowId = "flow-x" };
        var store = new InMemoryFlowStateStore();
        await using var lease = await CreateLeaseAsync(store, state);

        var context = new DurableFlowContext(
            state,
            store,
            Mock.Of<IAsyncResponseBuilder>(),
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverable.Object,
            NullLogger.Instance,
            lease);

        var result = await context.AwaitStepAsync<OperationResult>("remote", _ => Task.CompletedTask);

        Assert.Equal(OperationStatus.Completed, result.Status);
        Assert.NotNull(resume);
        Assert.Equal(typeof(IDurableFlowExecutor).FullName, resume!.ServiceInterfaceFullName);
        Assert.Equal(nameof(IDurableFlowExecutor.RecoverAsync), resume.MethodName);
        Assert.Collection(
            resume.Params,
            parameter => Assert.Equal("flow-x", parameter.Value?.ToString()),
            parameter => Assert.Equal(PlaceholderType.Payload, parameter.Placeholder),
            parameter => Assert.Equal(PlaceholderType.CorrelationId, parameter.Placeholder));
        Assert.NotNull(failure);
        Assert.Equal(typeof(IDurableFlowExecutor).FullName, failure!.ServiceInterfaceFullName);
        Assert.Equal(nameof(IDurableFlowExecutor.FailAsync), failure.MethodName);
    }

    [Fact]
    public async Task FlowExecutorCallbacks_AreAuthorizedByDefault_WithAnOtherwiseEmptyAllowlist()
    {
        // The allowlist admits the executor by default (AllowDurableFlowExecutor = true), so users
        // never have to allowlist a library-internal type to keep flow recovery working. A CUSTOM
        // authorizer gets no such implicit entry — that contract is pinned in
        // CallbackAuthorizationTests.CustomAuthorizer_GatesDurableFlowExecutorTargets.
        await using var provider = CreateProvider(services => services.AddSingleton(
            new AsyncResponseCallbackAllowList().Build()));

        Expression<Func<IDurableFlowExecutor, Task>> resume = executor => executor.ResumeAsync("missing-flow");
        var dto = CallbackExpressionConverter.ToReflectionCall(resume);
        var invocation = ReflectionExtensions.ResolveCallback(dto, payload: null, exception: null, correlationId: "missing-flow");
        await provider.InvokeAsync(invocation); // no-op for an unknown flow, but must not be blocked

        // Control: any other target is still subject to the allowlist.
        Expression<Func<FlowProbe, Task>> other = probe => probe.RecordTrigger("x");
        var otherDto = CallbackExpressionConverter.ToReflectionCall(other);
        var otherInvocation = ReflectionExtensions.ResolveCallback(otherDto, payload: null, exception: null, correlationId: "x");
        await Assert.ThrowsAsync<InvalidOperationException>(() => provider.InvokeAsync(otherInvocation));
    }

    private sealed class ScopedStoreDependency;

    private sealed class ScopedNullFlowStateStore : IFlowStateStore
    {
        public ScopedNullFlowStateStore(ScopedStoreDependency dependency)
            => ArgumentNullException.ThrowIfNull(dependency);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => Task.FromResult(false);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult<FlowState?>(null);

        public Task<bool> TryUpdateAsync(
            string flowId,
            FlowState state,
            long expectedRevision,
            TimeSpan ttl,
            string? leaseId = null,
            CancellationToken cancellationToken = default)
            => Task.FromResult(false);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => Task.FromResult(false);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => Task.FromResult(false);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult(false);
    }

    private static async Task<FlowExecutionLease> CreateLeaseAsync(IFlowStateStore store, FlowState state)
    {
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        var lease = await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(
            store,
            state.FlowId!,
            new DurableFlowOptions(),
            NullLogger.Instance);
        return Assert.IsType<FlowExecutionLease>(lease);
    }

    [Fact]
    public async Task LeaseSave_RejectedByConcurrentWrite_ReportsRevisionConflictWithCause()
    {
        // A rejected checkpoint used to be reported unconditionally as "lost its execution lease";
        // a revision conflict from a lease-bypassing writer (RecoverAsync, FailAsync, operator
        // parking) must be diagnosed as such, with the failure that triggered the save attached.
        var state = new FlowState { FlowId = "conflict-flow" };
        var store = new InMemoryFlowStateStore();
        await using var lease = await CreateLeaseAsync(store, state);

        var concurrent = await store.LoadAsync("conflict-flow");
        Assert.NotNull(concurrent);
        var concurrentRevision = concurrent.Revision;
        concurrent.Revision = concurrentRevision + 1;
        Assert.True(await store.TryUpdateAsync("conflict-flow", concurrent, concurrentRevision, TimeSpan.FromMinutes(5)));

        var cause = new TimeoutException("step wait failed");
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => lease.SaveAsync(state, TimeSpan.FromMinutes(5), cause: cause));

        Assert.Contains("concurrent write advanced the ledger", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("lost its execution lease", ex.Message, StringComparison.Ordinal);
        Assert.Same(cause, ex.InnerException);
        Assert.True(lease.LostToken.IsCancellationRequested);
    }

    private sealed class RecordingFlowStateStore : IFlowStateStore
    {
        private readonly InMemoryFlowStateStore _inner = new();

        public List<string?> PersistedMessages { get; } = [];

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => _inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => _inner.LoadAsync(flowId, cancellationToken);

        public async Task<bool> TryUpdateAsync(
            string flowId,
            FlowState state,
            long expectedRevision,
            TimeSpan ttl,
            string? leaseId = null,
            CancellationToken cancellationToken = default)
        {
            var message = state.LastMessage;
            var updated = await _inner.TryUpdateAsync(
                flowId,
                state,
                expectedRevision,
                ttl,
                leaseId,
                cancellationToken);
            if (updated)
                PersistedMessages.Add(message);
            return updated;
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
