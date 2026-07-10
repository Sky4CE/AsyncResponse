using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression tests for the child-flow abandonment modes and write-ordering contracts:
/// duplicate delivery of a suspended parent, resume after a process restart, child-id collisions,
/// expired child state, and suspension persisted before the child becomes runnable.
/// </summary>
public class DurableChildFlowRegressionTests
{
    public sealed record GatedParentInput(int Value);

    /// <summary>Per-provider gate: when blocked, the child flow parks inside its step.</summary>
    public sealed class ChildGate
    {
        public bool Block { get; init; }
        public TaskCompletionSource Released { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public sealed class GatedChildFlow(ChildFlowProbe _probe, ChildGate _gate) : IDurableFlow<TestFlowInput>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, TestFlowInput input)
        {
            await flow.StepAsync("work", async () =>
            {
                _probe.Bump("child-work");
                if (_gate.Block)
                    await _gate.Released.Task;
            });
        }
    }

    public sealed class GatedParentFlow(ChildFlowProbe _probe) : IDurableFlow<GatedParentInput>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, GatedParentInput input)
        {
            await flow.AwaitChildFlowAsync<GatedChildFlow, TestFlowInput>("child-gated", new TestFlowInput(input.Value));
            await flow.StepAsync("parent-finish", () =>
            {
                _probe.Bump("parent-finish");
                return Task.CompletedTask;
            });
        }
    }

    public sealed record CollisionInput(string ChildId);

    public sealed class CollidingParentFlow : IDurableFlow<CollisionInput>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, CollisionInput input)
            => await flow.AwaitChildFlowAsync<GatedChildFlow, TestFlowInput>("collide", new TestFlowInput(1), flowId: input.ChildId);
    }

    /// <summary>
    /// Serializing map-backed store shared across providers, so a second provider models a process
    /// restart that carries over only persisted state.
    /// </summary>
    public sealed class SharedMapFlowStateStore(ConcurrentDictionary<string, string> _map) : IFlowStateStore
    {
        public Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
        {
            _map[flowId] = FlowStateJson.Serialize(state);
            return Task.CompletedTask;
        }

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult(_map.TryGetValue(flowId, out var json) ? JsonSerializer.Deserialize<FlowState>(json) : null);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult(_map.TryRemove(flowId, out _));
    }

    [Fact]
    public async Task SuspendedParent_DuplicateDelivery_CompletesStepsExactlyOnce()
    {
        await using var provider = CreateProvider(blockChild: true);
        var hosted = await StartHostedServicesAsync(provider);
        var gate = provider.GetRequiredService<ChildGate>();
        try
        {
            var flows = provider.GetRequiredService<IDurableFlows>();
            var executor = provider.GetRequiredService<IDurableFlowExecutor>();
            var probe = provider.GetRequiredService<ChildFlowProbe>();

            var parentId = await flows.StartAsync<GatedParentFlow, GatedParentInput>(new GatedParentInput(7), flowId: "dup-root");
            await WaitUntilAsync(() => probe.Count("child-work") == 1); // child started and parked

            // Simulate the transport redelivering the parent job while it is suspended.
            await executor.ExecuteAsync(parentId);

            gate.Released.TrySetResult();
            var root = await WaitForStateAsync(flows, parentId, FlowRunStatus.Succeeded);

            Assert.Equal(1, probe.Count("parent-finish"));
            Assert.True(root.Steps!["child-gated"].Completed);
            Assert.False(root.Steps["child-gated"].Faulted);
            Assert.Equal(FlowRunStatus.Succeeded, (await flows.GetStateAsync("dup-root:child-gated"))!.Status);

            // The memoized child snapshot must not carry the child's captured ambient context.
            Assert.DoesNotContain("\"Context\"", root.Steps["child-gated"].ResultJson, StringComparison.Ordinal);
        }
        finally
        {
            gate.Released.TrySetResult();
            await StopHostedServicesAsync(hosted);
        }
    }

    [Fact]
    public async Task SuspendedParent_ResumesAfterRestart_FromPersistedStateOnly()
    {
        var map = new ConcurrentDictionary<string, string>(StringComparer.Ordinal);

        // First "process": the child parks mid-step, then the process dies (no graceful stop).
        var crashed = CreateProvider(blockChild: true, sharedStore: map);
        await StartHostedServicesAsync(crashed);
        var crashedProbe = crashed.GetRequiredService<ChildFlowProbe>();
        var flowsA = crashed.GetRequiredService<IDurableFlows>();

        var parentId = await flowsA.StartAsync<GatedParentFlow, GatedParentInput>(new GatedParentInput(7), flowId: "restart-root");
        await WaitUntilAsync(() => crashedProbe.Count("child-work") == 1);

        var suspended = await flowsA.GetStateAsync(parentId);
        Assert.Equal(FlowRunStatus.Running, suspended!.Status);
        Assert.Contains("suspended waiting for child flow", suspended.LastMessage);

        await crashed.DisposeAsync(); // crash: in-memory queue and in-flight child are gone

        // Second "process": only the shared store carries over. Re-delivering the parent job (the
        // documented recovery lever — transport redelivery or ResumeAsync) must re-enqueue the
        // child from the breadcrumb and run the whole tree to completion.
        await using var restarted = CreateProvider(blockChild: false, sharedStore: map);
        var hosted = await StartHostedServicesAsync(restarted);
        try
        {
            var flows = restarted.GetRequiredService<IDurableFlows>();
            var executor = restarted.GetRequiredService<IDurableFlowExecutor>();
            var probe = restarted.GetRequiredService<ChildFlowProbe>();

            await executor.ExecuteAsync(parentId);

            var root = await WaitForStateAsync(flows, parentId, FlowRunStatus.Succeeded);
            Assert.Equal(1, probe.Count("child-work"));
            Assert.Equal(1, probe.Count("parent-finish"));
            Assert.True(root.Steps!["child-gated"].Completed);
            Assert.Equal(FlowRunStatus.Succeeded, (await flows.GetStateAsync("restart-root:child-gated"))!.Status);
        }
        finally
        {
            await StopHostedServicesAsync(hosted);
        }
    }

    [Fact]
    public async Task AwaitChildFlow_ChildIdOwnedByAnotherRun_FailsParentInsteadOfParkingForever()
    {
        await using var provider = CreateProvider(blockChild: false);
        var hosted = await StartHostedServicesAsync(provider);
        try
        {
            var flows = provider.GetRequiredService<IDurableFlows>();
            using var scope = provider.CreateScope();
            var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

            // A top-level run (no ParentFlowId) already owns the id the parent wants to await.
            await store.SaveAsync("shared-id", new FlowState
            {
                FlowId = "shared-id",
                FlowTypeName = typeof(GatedChildFlow).FullName,
                InputTypeName = typeof(TestFlowInput).FullName,
                InputJson = JsonSerializer.Serialize(new TestFlowInput(1)),
                Status = FlowRunStatus.Running,
                CreatedAtUtc = DateTime.UtcNow,
                UpdatedAtUtc = DateTime.UtcNow
            }, TimeSpan.FromMinutes(5));

            var parentId = await flows.StartAsync<CollidingParentFlow, CollisionInput>(new CollisionInput("shared-id"), flowId: "collision-owner-root");

            var root = await WaitForStateAsync(flows, parentId, FlowRunStatus.Failed);
            Assert.Contains("belongs to", root.LastMessage);
        }
        finally
        {
            await StopHostedServicesAsync(hosted);
        }
    }

    [Fact]
    public async Task AwaitChildFlow_ChildIdWithDifferentFlowType_FailsParent()
    {
        await using var provider = CreateProvider(blockChild: false);
        var hosted = await StartHostedServicesAsync(provider);
        try
        {
            var flows = provider.GetRequiredService<IDurableFlows>();
            using var scope = provider.CreateScope();
            var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

            // Right parent, wrong flow type under the awaited id.
            await store.SaveAsync("typed-id", new FlowState
            {
                FlowId = "typed-id",
                FlowTypeName = typeof(RecursiveChildFlow).FullName,
                InputTypeName = typeof(RecursiveChildInput).FullName,
                InputJson = JsonSerializer.Serialize(new RecursiveChildInput(0)),
                Status = FlowRunStatus.Running,
                ParentFlowId = "collision-type-root",
                ParentStepName = "collide",
                CreatedAtUtc = DateTime.UtcNow,
                UpdatedAtUtc = DateTime.UtcNow
            }, TimeSpan.FromMinutes(5));

            var parentId = await flows.StartAsync<CollidingParentFlow, CollisionInput>(new CollisionInput("typed-id"), flowId: "collision-type-root");

            var root = await WaitForStateAsync(flows, parentId, FlowRunStatus.Failed);
            Assert.Contains("collides with a different flow", root.LastMessage);
        }
        finally
        {
            await StopHostedServicesAsync(hosted);
        }
    }

    [Fact]
    public async Task AwaitChildFlow_ChildStateGone_FailsParentTerminally_WithoutRerunningChild()
    {
        await using var provider = CreateProvider(blockChild: true);
        var hosted = await StartHostedServicesAsync(provider);
        var gate = provider.GetRequiredService<ChildGate>();
        try
        {
            var flows = provider.GetRequiredService<IDurableFlows>();
            var executor = provider.GetRequiredService<IDurableFlowExecutor>();
            var probe = provider.GetRequiredService<ChildFlowProbe>();
            using var scope = provider.CreateScope();
            var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

            var parentId = await flows.StartAsync<GatedParentFlow, GatedParentInput>(new GatedParentInput(7), flowId: "expiry-root");
            await WaitUntilAsync(() => probe.Count("child-work") == 1);

            // Simulate the child ledger expiring while the parent is suspended.
            Assert.True(await store.TryDeleteAsync("expiry-root:child-gated"));

            await executor.ExecuteAsync(parentId);

            var root = await flows.GetStateAsync(parentId);
            Assert.Equal(FlowRunStatus.Failed, root!.Status);
            Assert.Contains("has no state (expired or deleted)", root.LastMessage);
            // The child must NOT be silently re-created and re-run: its outcome is unknown.
            Assert.Equal(1, probe.Count("child-work"));
        }
        finally
        {
            gate.Released.TrySetResult();
            await StopHostedServicesAsync(hosted);
        }
    }

    [Fact]
    public async Task AwaitChildFlow_PersistsSuspendedParentBeforeChildIsRunnable()
    {
        // Write-ordering contract: once the child job is enqueued it can complete and re-execute
        // the parent on another worker at any moment, so the suspended parent state (breadcrumb +
        // suspension message) must already be durable at enqueue time. A save after enqueue could
        // clobber the re-execution's newer checkpoints with a stale snapshot.
        var map = new ConcurrentDictionary<string, string>(StringComparer.Ordinal);
        var store = new SharedMapFlowStateStore(map);
        var state = new FlowState
        {
            FlowId = "ordering-root",
            FlowTypeName = typeof(GatedParentFlow).FullName,
            Status = FlowRunStatus.Running,
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow
        };

        string? parentJsonAtEnqueue = null;
        var builder = new Mock<IAsyncResponseBuilder>();
        builder
            .Setup(b => b.EnqueueWorkerAsync(It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>()))
            .Callback(() => map.TryGetValue("ordering-root", out parentJsonAtEnqueue))
            .Returns(Task.CompletedTask);

        var context = new DurableFlowContext(
            state,
            store,
            builder.Object,
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            NullLogger.Instance);

        await Assert.ThrowsAsync<DurableFlowSuspendedException>(
            () => context.AwaitChildFlowAsync<GatedChildFlow, TestFlowInput>("child-step", new TestFlowInput(1)));

        Assert.NotNull(parentJsonAtEnqueue);
        var persisted = JsonSerializer.Deserialize<FlowState>(parentJsonAtEnqueue!)!;
        Assert.Contains("suspended waiting for child flow", persisted.LastMessage);
        Assert.Equal("ordering-root:child-step", persisted.Steps!["child-step"].ChildFlowId);

        // The child state itself must exist before the breadcrumb ("breadcrumb implies child").
        var child = JsonSerializer.Deserialize<FlowState>(map["ordering-root:child-step"])!;
        Assert.Equal("ordering-root", child.ParentFlowId);
        Assert.Equal("child-step", child.ParentStepName);
    }

    [Fact]
    public void SerializeSnapshot_StripsAmbientContext_AndRestoresItOnTheInstance()
    {
        var state = new FlowState
        {
            FlowId = "snap",
            Status = FlowRunStatus.Succeeded,
            Context = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "42" }
        };

        var json = FlowStateJson.SerializeSnapshot(state);

        Assert.DoesNotContain("\"Context\"", json, StringComparison.Ordinal);
        Assert.NotNull(state.Context); // the live instance keeps its context
        Assert.Equal("42", state.Context!["tenant"]);
    }

    private static ServiceProvider CreateProvider(bool blockChild, ConcurrentDictionary<string, string>? sharedStore = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ChildFlowProbe>();
        services.AddSingleton(new ChildGate { Block = blockChild });
        services.AddScoped<GatedChildFlow>();
        services.AddScoped<GatedParentFlow>();
        services.AddScoped<CollidingParentFlow>();
        services.AddScoped<RecursiveChildFlow>();
        var builder = services.AddAsyncResponse(options => options.Watchdog.Enabled = false)
            .WithInMemoryChannel()
            .WithInMemoryTransport();

        if (sharedStore is not null)
        {
            services.AddSingleton(sharedStore);
            builder.WithCustomDurableFlows<SharedMapFlowStateStore>();
        }

        return services.BuildServiceProvider();
    }

    private static async Task<FlowState> WaitForStateAsync(IDurableFlows flows, string flowId, FlowRunStatus status)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        FlowState? state;
        do
        {
            state = await flows.GetStateAsync(flowId);
            if (state?.Status == status)
                return state;

            await Task.Delay(25);
        } while (DateTime.UtcNow < deadline);

        throw new TimeoutException($"Flow {flowId} did not reach {status}; last status was {state?.Status} ({state?.LastMessage}).");
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition())
        {
            cts.Token.ThrowIfCancellationRequested();
            await Task.Delay(20, cts.Token);
        }
    }

    private static async Task<IReadOnlyList<IHostedService>> StartHostedServicesAsync(IServiceProvider provider)
    {
        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);
        return hosted;
    }

    private static async Task StopHostedServicesAsync(IEnumerable<IHostedService> hosted)
    {
        foreach (var service in hosted)
            await service.StopAsync(CancellationToken.None);
    }
}
