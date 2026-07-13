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

        harness.Builder.Verify(instance => instance.EnqueueWorkerAsync(
            It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
            It.IsAny<CancellationToken>()), Times.Exactly(2));
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

        public ValueTask DisposeAsync() => _provider.DisposeAsync();
    }

    private static Harness CreateHarness(
        IFlowStateStore store,
        Action<IServiceCollection>? configure = null)
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
            new DurableFlowOptions(),
            NullLogger<DurableFlowExecutor>.Instance);

        return new Harness(provider, executor, builder);
    }
}
