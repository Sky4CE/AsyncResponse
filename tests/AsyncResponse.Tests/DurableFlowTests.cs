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
            .WithInMemoryTransport();
        configure?.Invoke(services);
        return services.BuildServiceProvider();
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
        var state = await store.LoadAsync(flowId);
        state!.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
        {
            ["compute-stamp"] = new() { Completed = true, ResultJson = "707" },
            ["remote-op"] = new() { PendingCorrelationId = "pre-baked-cid" }
        };
        await store.SaveAsync(flowId, state, TimeSpan.FromMinutes(5));

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
    public async Task StartAsync_WithExistingFlowId_IsIdempotent()
    {
        await using var provider = CreateProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();

        var first = await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new TestFlowInput(7), flowId: "run-42");
        var created = (await flows.GetStateAsync("run-42"))!.CreatedAtUtc;

        var second = await flows.StartAsync<TestOnboardingFlow, TestFlowInput>(new TestFlowInput(999), flowId: "run-42");

        Assert.Equal("run-42", first);
        Assert.Equal("run-42", second);
        var state = await flows.GetStateAsync("run-42");
        Assert.Equal(created, state!.CreatedAtUtc);
        Assert.Contains("7", state.InputJson);   // the original input wins; no overwrite
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

        var store = new Mock<IFlowStateStore>();
        store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<FlowState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var context = new DurableFlowContext(
            new FlowState { FlowId = "flow-x" },
            store.Object,
            new DurableFlowOptions(),
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverable.Object,
            NullLogger.Instance);

        var result = await context.AwaitStepAsync<OperationResult>("remote", _ => Task.CompletedTask);

        Assert.Equal(OperationStatus.Completed, result.Status);
        Assert.NotNull(resume);
        Assert.Equal(typeof(IDurableFlowExecutor).FullName, resume!.ServiceInterfaceFullName);
        Assert.Equal(nameof(IDurableFlowExecutor.ResumeAsync), resume.MethodName);
        Assert.NotNull(failure);
        Assert.Equal(typeof(IDurableFlowExecutor).FullName, failure!.ServiceInterfaceFullName);
        Assert.Equal(nameof(IDurableFlowExecutor.FailAsync), failure.MethodName);
    }

    [Fact]
    public async Task RecoveryBackedStore_RoundTrips_RejectsNewerSchema_AndDeletes()
    {
        await using var provider = CreateProvider();
        var recoveryStore = provider.GetRequiredService<IRecoveryStateStore>();
        var store = new RecoveryBackedFlowStateStore(recoveryStore);

        var state = new FlowState
        {
            FlowId = "flow-rt",
            FlowTypeName = "X",
            Status = FlowRunStatus.Running,
            CreatedAtUtc = DateTime.UtcNow,
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
            {
                ["a"] = new() { Completed = true, ResultJson = "1" }
            }
        };

        await store.SaveAsync("flow-rt", state, TimeSpan.FromMinutes(5));
        var loaded = await store.LoadAsync("flow-rt");
        Assert.NotNull(loaded);
        Assert.Equal(FlowRunStatus.Running, loaded!.Status);
        Assert.True(loaded.Steps!["a"].Completed);

        // The ledger entry is marked so the watchdog can tell it apart from waiter registrations.
        var entries = await recoveryStore.GetAllAsync("flow-rt");
        var entry = Assert.Single(entries);
        Assert.True(RecoveryBackedFlowStateStore.IsFlowLedger(entry));

        // A newer writer's state is rejected rather than misinterpreted.
        state.SchemaVersion = FlowStateSchema.Current + 1;
        await store.SaveAsync("flow-rt", state, TimeSpan.FromMinutes(5));
        Assert.Null(await store.LoadAsync("flow-rt"));

        Assert.True(await store.TryDeleteAsync("flow-rt"));
        Assert.Null(await store.LoadAsync("flow-rt"));
    }

    [Fact]
    public async Task RecoveryBackedStore_LogsWarningOnce_WhenUsed()
    {
        var logger = new CapturingLogger<RecoveryBackedFlowStateStore>();
        var store = new RecoveryBackedFlowStateStore(new InMemoryRecoveryStateStore(), logger);
        var state = new FlowState
        {
            FlowId = "flow-warn",
            FlowTypeName = "X",
            Status = FlowRunStatus.Running,
            CreatedAtUtc = DateTime.UtcNow
        };

        await store.SaveAsync("flow-warn", state, TimeSpan.FromDays(7));
        await store.SaveAsync("flow-warn", state, TimeSpan.FromDays(7));

        Assert.Single(logger.Entries, entry =>
            entry.Level == LogLevel.Warning &&
            entry.Message.Contains("WithDurableFlows", StringComparison.Ordinal));
    }

    [Fact]
    public async Task FlowExecutorCallbacks_AreImplicitlyAuthorized_EvenWithDenyAllAllowlist()
    {
        await using var provider = CreateProvider(services => services.AddSingleton<IAsyncResponseCallbackAuthorizer>(
            new DenyAllAuthorizer()));

        // The executor's methods are the durable resume/failure targets behind every flow — they
        // must work without users having to allowlist a library-internal type.
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

    private sealed class DenyAllAuthorizer : IAsyncResponseCallbackAuthorizer
    {
        public bool IsAllowed(string serviceInterfaceFullName, string methodName) => false;
    }

    private sealed class ScopedStoreDependency;

    private sealed class ScopedNullFlowStateStore : IFlowStateStore
    {
        public ScopedNullFlowStateStore(ScopedStoreDependency dependency)
            => ArgumentNullException.ThrowIfNull(dependency);

        public Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult<FlowState?>(null);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult(false);
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Entries.Add((logLevel, formatter(state, exception)));
    }
}
