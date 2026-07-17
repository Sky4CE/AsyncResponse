using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
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
    private static readonly TimeSpan CrashTestLeaseDuration = TimeSpan.FromMilliseconds(500);

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
        var sharedStore = new InMemoryFlowStateStore();
        var crashedStore = new CrashableFlowStateStore(sharedStore);

        // First "process": the child parks mid-step, then the process dies (no graceful stop).
        var crashed = CreateProvider(blockChild: true, crashedStore);
        await StartHostedServicesAsync(crashed);
        var crashedProbe = crashed.GetRequiredService<ChildFlowProbe>();
        var flowsA = crashed.GetRequiredService<IDurableFlows>();

        var parentId = await flowsA.StartAsync<GatedParentFlow, GatedParentInput>(new GatedParentInput(7), flowId: "restart-root");
        await WaitUntilAsync(() => crashedProbe.Count("child-work") == 1);

        var suspended = await flowsA.GetStateAsync(parentId);
        Assert.Equal(FlowRunStatus.Running, suspended!.Status);
        Assert.Contains("suspended waiting for child flow", suspended.LastMessage);

        crashedStore.Crash();
        await crashed.DisposeAsync(); // crash: in-memory queue and in-flight child are gone
        await Task.Delay(CrashTestLeaseDuration + TimeSpan.FromMilliseconds(250));

        // Second "process": only the shared store carries over. Re-delivering the parent job (the
        // documented recovery lever — transport redelivery or ResumeAsync) must re-enqueue the
        // child from the breadcrumb and run the whole tree to completion.
        await using var restarted = CreateProvider(
            blockChild: false,
            new CrashableFlowStateStore(sharedStore));
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
            Assert.True(await store.TryCreateAsync("shared-id", new FlowState
            {
                FlowId = "shared-id",
                FlowTypeName = typeof(GatedChildFlow).FullName,
                InputTypeName = typeof(TestFlowInput).FullName,
                InputJson = JsonSerializer.Serialize(new TestFlowInput(1)),
                Status = FlowRunStatus.Running,
                CreatedAtUtc = DateTime.UtcNow,
                UpdatedAtUtc = DateTime.UtcNow
            }, TimeSpan.FromMinutes(5)));

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
            Assert.True(await store.TryCreateAsync("typed-id", new FlowState
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
            }, TimeSpan.FromMinutes(5)));

            var parentId = await flows.StartAsync<CollidingParentFlow, CollisionInput>(new CollisionInput("typed-id"), flowId: "collision-type-root");

            var root = await WaitForStateAsync(flows, parentId, FlowRunStatus.Failed);
            Assert.Contains("collides with a different flow", root.LastMessage);
        }
        finally
        {
            await StopHostedServicesAsync(hosted);
        }
    }

    [Theory]
    [InlineData("parent-step")]
    [InlineData("input-type")]
    [InlineData("input-value")]
    public async Task AwaitChildFlow_ChildIdWithDifferentPersistedContract_FailsParent(string mismatch)
    {
        await using var provider = CreateProvider(blockChild: false);
        var hosted = await StartHostedServicesAsync(provider);
        try
        {
            var flows = provider.GetRequiredService<IDurableFlows>();
            using var scope = provider.CreateScope();
            var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();

            Assert.True(await store.TryCreateAsync("contract-id", new FlowState
            {
                FlowId = "contract-id",
                FlowTypeName = typeof(GatedChildFlow).FullName,
                InputTypeName = mismatch == "input-type" ? typeof(RecursiveChildInput).FullName : typeof(TestFlowInput).FullName,
                InputJson = JsonSerializer.Serialize(mismatch == "input-value" ? new TestFlowInput(2) : new TestFlowInput(1)),
                Status = FlowRunStatus.Running,
                ParentFlowId = "collision-contract-root",
                ParentStepName = mismatch == "parent-step" ? "another-step" : "collide",
                CreatedAtUtc = DateTime.UtcNow,
                UpdatedAtUtc = DateTime.UtcNow
            }, TimeSpan.FromMinutes(5)));

            var parentId = await flows.StartAsync<CollidingParentFlow, CollisionInput>(
                new CollisionInput("contract-id"),
                flowId: "collision-contract-root");

            var root = await WaitForStateAsync(flows, parentId, FlowRunStatus.Failed);
            Assert.Contains("different", root.LastMessage, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            await StopHostedServicesAsync(hosted);
        }
    }

    [Fact]
    public async Task AwaitChildFlow_CompletedCheckpoint_WithChangedInput_FailsInsteadOfReturningStaleChild()
    {
        var store = new InMemoryFlowStateStore();
        var child = new FlowState
        {
            FlowId = "completed-root:child",
            FlowTypeName = typeof(GatedChildFlow).FullName,
            InputTypeName = typeof(TestFlowInput).FullName,
            InputJson = JsonSerializer.Serialize(new TestFlowInput(1)),
            Status = FlowRunStatus.Succeeded,
            ParentFlowId = "completed-root",
            ParentStepName = "child",
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow
        };
        var parent = new FlowState
        {
            FlowId = "completed-root",
            FlowTypeName = typeof(GatedParentFlow).FullName,
            Status = FlowRunStatus.Running,
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
            {
                ["child"] = new()
                {
                    Completed = true,
                    ChildFlowId = child.FlowId,
                    ResultJson = FlowStateJson.SerializeSnapshot(child)
                }
            },
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow
        };

        Assert.True(await store.TryCreateAsync(parent.FlowId!, parent, TimeSpan.FromMinutes(5)));
        await using var lease = await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(
            store,
            parent.FlowId!,
            new DurableFlowOptions(),
            NullLogger.Instance);
        Assert.NotNull(lease);

        var context = CreateContext(parent, store, lease!);
        var error = await Assert.ThrowsAsync<DurableFlowFailedException>(() =>
            context.AwaitChildFlowAsync<GatedChildFlow, TestFlowInput>("child", new TestFlowInput(2)));

        Assert.Contains("semantically identical child input", error.Message);
    }

    [Fact]
    public async Task AwaitChildFlow_ReplayWithChangedExplicitChildId_FailsFast()
    {
        var store = new InMemoryFlowStateStore();
        var parent = new FlowState
        {
            FlowId = "replay-root",
            FlowTypeName = typeof(GatedParentFlow).FullName,
            Status = FlowRunStatus.Running,
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
            {
                ["child"] = new() { ChildFlowId = "original-child-id" }
            },
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow
        };

        Assert.True(await store.TryCreateAsync(parent.FlowId!, parent, TimeSpan.FromMinutes(5)));
        await using var lease = await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(
            store,
            parent.FlowId!,
            new DurableFlowOptions(),
            NullLogger.Instance);
        Assert.NotNull(lease);

        var context = CreateContext(parent, store, lease!);
        var error = await Assert.ThrowsAsync<DurableFlowFailedException>(() =>
            context.AwaitChildFlowAsync<GatedChildFlow, TestFlowInput>(
                "child",
                new TestFlowInput(1),
                flowId: "changed-child-id"));

        Assert.Contains("already bound", error.Message);
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
        var store = new InMemoryFlowStateStore();
        var state = new FlowState
        {
            FlowId = "ordering-root",
            FlowTypeName = typeof(GatedParentFlow).FullName,
            Status = FlowRunStatus.Running,
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow
        };

        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5)));
        await using var lease = await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(
            store,
            state.FlowId!,
            new DurableFlowOptions(),
            NullLogger.Instance);
        Assert.NotNull(lease);

        FlowState? parentStateAtEnqueue = null;
        var builder = new Mock<IAsyncResponseBuilder>();
        builder
            .Setup(b => b.EnqueueWorkerAsync(It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>()))
            .Callback(() => parentStateAtEnqueue = store.LoadAsync("ordering-root").GetAwaiter().GetResult())
            .Returns(Task.CompletedTask);

        var context = new DurableFlowContext(
            state,
            store,
            builder.Object,
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            NullLogger.Instance,
            lease!);

        await Assert.ThrowsAsync<DurableFlowSuspendedException>(
            () => context.AwaitChildFlowAsync<GatedChildFlow, TestFlowInput>("child-step", new TestFlowInput(1)));

        var persisted = Assert.IsType<FlowState>(parentStateAtEnqueue);
        Assert.Contains("suspended waiting for child flow", persisted.LastMessage);
        Assert.Equal("ordering-root:child-step", persisted.Steps!["child-step"].ChildFlowId);

        // The child state itself must exist before the breadcrumb ("breadcrumb implies child").
        var child = await store.LoadAsync("ordering-root:child-step");
        Assert.NotNull(child);
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

    private static ServiceProvider CreateProvider(bool blockChild, CrashableFlowStateStore? flowStore = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ChildFlowProbe>();
        services.AddSingleton(new ChildGate { Block = blockChild });
        services.AddScoped<GatedChildFlow>();
        services.AddScoped<GatedParentFlow>();
        services.AddScoped<CollidingParentFlow>();
        services.AddScoped<RecursiveChildFlow>();
        var builder = services.AddAsyncResponse(options =>
            {
                options.Watchdog.Enabled = false;
            })
            .WithInMemoryChannel()
            .WithInMemoryTransport();

        if (flowStore is not null)
        {
            services.AddSingleton(flowStore);
            builder.WithDurableFlows<CrashableFlowStateStore>(options =>
            {
                options.ExecutionLeaseDuration = CrashTestLeaseDuration;
                options.ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(100);
            });
        }
        else
        {
            builder.WithInMemoryDurableFlows();
        }

        return services.BuildServiceProvider();
    }

    private static DurableFlowContext CreateContext(
        FlowState state,
        IFlowStateStore store,
        FlowExecutionLease lease)
        => new(
            state,
            store,
            Mock.Of<IAsyncResponseBuilder>(),
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            NullLogger.Instance,
            lease);

    /// <summary>
    /// Gives each simulated process its own store client while sharing persisted state. Once a
    /// client crashes it can no longer renew or release leases, exactly like a dead replica; the
    /// next client takes over after the persisted lease expires.
    /// </summary>
    private sealed class CrashableFlowStateStore(InMemoryFlowStateStore inner) : IFlowStateStore
    {
        private bool _available = true;

        public void Crash() => Volatile.Write(ref _available, false);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => Available().TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => Available().LoadAsync(flowId, cancellationToken);

        public Task<bool> TryUpdateAsync(
            string flowId,
            FlowState state,
            long expectedRevision,
            TimeSpan ttl,
            string? leaseId = null,
            CancellationToken cancellationToken = default)
            => Available().TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public Task<bool> TryAcquireLeaseAsync(
            string flowId,
            string leaseId,
            TimeSpan leaseDuration,
            CancellationToken cancellationToken = default)
            => Available().TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(
            string flowId,
            string leaseId,
            TimeSpan leaseDuration,
            CancellationToken cancellationToken = default)
            => Volatile.Read(ref _available)
                ? inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken)
                : Task.FromResult(false);

        public Task ReleaseLeaseAsync(
            string flowId,
            string leaseId,
            CancellationToken cancellationToken = default)
            => Volatile.Read(ref _available)
                ? inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken)
                : Task.CompletedTask;

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => Available().TryDeleteAsync(flowId, cancellationToken);

        private InMemoryFlowStateStore Available()
            => Volatile.Read(ref _available)
                ? inner
                : throw new InvalidOperationException("The simulated process has crashed.");
    }

    private static async Task<FlowState> WaitForStateAsync(IDurableFlows flows, string flowId, FlowRunStatus status)
    {
        // Generous on purpose: these scenarios drive multi-hop chains (parent execute → child
        // enqueue → child execute → parent notify → parent re-execute) over per-provider
        // in-memory queues, and a fully parallel dual-TFM suite run can starve the thread pool
        // for seconds at a time. Healthy runs return in milliseconds — the polling loop exits
        // as soon as the state lands.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
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
