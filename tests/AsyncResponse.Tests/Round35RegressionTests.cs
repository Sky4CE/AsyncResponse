using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regressions for round 35 (external architect review of 684a3fb): behavior pins that compile
/// against the pre-fix tree and fail there. Pins over API this round introduced
/// (<c>CreateAndExecuteAsync</c>, <c>LedgerSizeWarningBytes</c>, <c>SerialExecutorRegistry.
/// TryEnqueue</c>) live in <see cref="Round35NewApiTests"/>; the DB-channel sweep pin lives in
/// <c>DbChannelSharedCoverageTests</c> and the Kafka pins in the Kafka dispatcher/subscriber tests.
/// </summary>
public sealed class Round35RegressionTests
{
    // ---------------------------------------------------------------------------------------------
    // A1 — StartAsync committed the ledger and THEN published. A process dying between the two left
    //      a Running ledger with Attempts = 0 that nothing would ever execute, and IFlowStateStore has
    //      no enumeration for a reconciler to find it. The publish is now the commit point: the job
    //      carries the initial ledger and its execution creates the run.

    public sealed record R35Input(string Name);

    /// <summary>A flow that records having run, so a test can prove whether it did.</summary>
    public sealed class R35MarkerFlow : IDurableFlow<R35Input>
    {
        public static int Executions;

        public Task ExecuteAsync(IDurableFlowContext flow, R35Input input)
        {
            Interlocked.Increment(ref Executions);
            return Task.CompletedTask;
        }
    }

    /// <summary>A worker transport that records every published job and can be told to refuse.</summary>
    private sealed class CapturingWorkerTransport : IWorkerTransport
    {
        public int PublishAttempts;
        public volatile bool Fail;
        public readonly List<WorkerJobEnvelope> Published = [];

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref PublishAttempts);
            if (Fail)
                throw new InvalidOperationException("broker unavailable");

            lock (Published)
                Published.Add(job);
            return Task.CompletedTask;
        }

        public WorkerJobEnvelope[] Snapshot()
        {
            lock (Published)
                return Published.ToArray();
        }
    }

    private static ServiceProvider BuildFlowProvider(CapturingWorkerTransport transport, VirtualTimeProvider? clock = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        if (clock is not null)
            services.AddSingleton<TimeProvider>(clock);
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<R35MarkerFlow, R35Input>();
        services.AddSingleton<IWorkerTransport>(transport);
        return services.BuildServiceProvider();
    }

    /// <summary>Walks the start's retry ladder on the virtual clock until the task settles.</summary>
    private static async Task DriveRetryLadderAsync(Task start, VirtualTimeProvider clock)
    {
        for (var i = 0; i < 200 && !start.IsCompleted; i++)
        {
            if (clock.NextTimerDueAt is { } due)
                clock.Advance(due - clock.GetUtcNow() + TimeSpan.FromMilliseconds(1));
            else
                await Task.Delay(5);
        }
    }

    /// <summary>
    /// Pre-fix failure: <c>LoadAsync(ex.FlowId)</c> returned a Running ledger — the orphan the
    /// exception told the caller to re-drive, which a caller that never saw the exception (the
    /// process died) could not.
    /// </summary>
    [Fact]
    public async Task Start_WhosePublishFails_PersistsNothing_SoNoRunIsStranded()
    {
        var clock = new VirtualTimeProvider();
        var transport = new CapturingWorkerTransport { Fail = true };
        await using var provider = BuildFlowProvider(transport, clock);
        var flows = provider.GetRequiredService<IDurableFlows>();
        var store = provider.GetRequiredService<InMemoryFlowStateStore>();

        var start = flows.StartAsync<R35MarkerFlow, R35Input>(new R35Input("acme"), "flow-r35-undispatched");
        await DriveRetryLadderAsync(start, clock);

        var ex = await Assert.ThrowsAsync<DurableFlowNotDispatchedException>(() => start);
        Assert.Equal("flow-r35-undispatched", ex.FlowId);
        Assert.True(transport.PublishAttempts > 1, $"expected the publish to be retried; saw {transport.PublishAttempts} attempt(s)");

        // The publish is the commit point: nothing exists for a reconciler to have to find.
        Assert.Null(await store.LoadAsync(ex.FlowId));

        // And the documented retry works: the same id creates the run and publishes exactly one job.
        transport.Fail = false;
        var retried = await flows.StartAsync<R35MarkerFlow, R35Input>(new R35Input("acme"), ex.FlowId);
        Assert.Equal(ex.FlowId, retried);
        Assert.NotNull(await store.LoadAsync(ex.FlowId));
        Assert.Single(transport.Snapshot());
    }

    /// <summary>
    /// The crash window itself: the job is published, the starter's own ledger write never
    /// happens. Pre-fix failure: the job carried only the id (<c>ExecuteAsync</c>), so replaying it
    /// against an absent ledger logged "no state" and acknowledged — the run never existed and
    /// never ran.
    /// </summary>
    [Fact]
    public async Task Start_PublishesAJobThatCreatesTheLedgerItself_SoACrashBeforeTheStartersWriteCannotStrandTheRun()
    {
        var transport = new CapturingWorkerTransport();
        await using var provider = BuildFlowProvider(transport);
        var flows = provider.GetRequiredService<IDurableFlows>();
        var store = provider.GetRequiredService<InMemoryFlowStateStore>();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        var id = await flows.StartAsync<R35MarkerFlow, R35Input>(new R35Input("acme"), "flow-r35-self-creating");
        var job = Assert.Single(transport.Snapshot());
        Assert.Equal("CreateAndExecuteAsync", job.Call.MethodName);

        // Simulate the crash: the starter's ledger is gone, the published job is all that survives.
        Assert.True(await store.TryDeleteAsync(id));
        Assert.Null(await store.LoadAsync(id));

        var before = Volatile.Read(ref R35MarkerFlow.Executions);
        await ingress.HandleWorkerMessageAsync(AsyncResponseJson.Serialize(job));

        var state = await store.LoadAsync(id);
        Assert.NotNull(state);
        Assert.Equal(FlowRunStatus.Succeeded, state!.Status);
        Assert.Equal(typeof(R35MarkerFlow).FullName, state.FlowTypeName);
        Assert.Equal(before + 1, Volatile.Read(ref R35MarkerFlow.Executions));
    }

    /// <summary>Pin (unchanged behavior): an identical explicit-id start re-enqueues, a conflicting one is rejected.</summary>
    [Fact]
    public async Task Start_WithAnExistingIdenticalRun_ReEnqueuesIt_AndRejectsDifferentInput()
    {
        var transport = new CapturingWorkerTransport();
        await using var provider = BuildFlowProvider(transport);
        var flows = provider.GetRequiredService<IDurableFlows>();
        var store = provider.GetRequiredService<InMemoryFlowStateStore>();

        var id = await flows.StartAsync<R35MarkerFlow, R35Input>(new R35Input("acme"), "flow-r35-idempotent");
        var again = await flows.StartAsync<R35MarkerFlow, R35Input>(new R35Input("acme"), id);

        Assert.Equal(id, again);
        Assert.Equal(2, transport.Snapshot().Length);
        Assert.NotNull(await store.LoadAsync(id));

        await Assert.ThrowsAsync<DurableFlowIdConflictException>(
            () => flows.StartAsync<R35MarkerFlow, R35Input>(new R35Input("other"), id));
    }

    // ---------------------------------------------------------------------------------------------
    // R3 — a void-returning target was awaited as "already complete", so an `async void`
    //      implementation was acknowledged (and its DI scope disposed) while its body was still
    //      running at the first await.

    public interface IVoidWorker
    {
        void Run();
    }

    public sealed class AsyncVoidWorker : IVoidWorker
    {
        public readonly TaskCompletionSource Gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Started;

        public async void Run()
        {
            Interlocked.Increment(ref Started);
            await Gate.Task;
        }
    }

    public sealed class SyncVoidWorker : IVoidWorker
    {
        public int Runs;

        public void Run() => Runs++;
    }

    private static ReflectionInvocationDto VoidRunDto(Type serviceType) => new()
    {
        ServiceInterfaceFullName = serviceType.FullName!,
        MethodName = nameof(IVoidWorker.Run),
        Params = []
    };

    /// <summary>Pre-fix failure: the invocation completed successfully and <c>Started</c> was 1 — the body was mid-flight.</summary>
    [Fact]
    public async Task AsyncVoidImplementation_BehindAnInterface_IsRejectedBeforeItStarts()
    {
        var worker = new AsyncVoidWorker();
        var services = new ServiceCollection();
        services.AddSingleton<IVoidWorker>(worker);
        await using var provider = services.BuildServiceProvider();

        var ex = await Assert.ThrowsAsync<CallbackTargetUnresolvableException>(() => provider.InvokeAsync(VoidRunDto(typeof(IVoidWorker))));

        Assert.Contains("async void", ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(AsyncVoidWorker), ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, Volatile.Read(ref worker.Started));
        worker.Gate.SetResult();
    }

    /// <summary>Pre-fix failure: same as the interface case, for a class-typed service (rejected at plan time).</summary>
    [Fact]
    public async Task AsyncVoidImplementation_OnAClassTypedService_IsRejectedBeforeItStarts()
    {
        var worker = new AsyncVoidWorker();
        var services = new ServiceCollection();
        services.AddSingleton(worker);
        await using var provider = services.BuildServiceProvider();

        var ex = await Assert.ThrowsAsync<CallbackTargetUnresolvableException>(() => provider.InvokeAsync(VoidRunDto(typeof(AsyncVoidWorker))));

        Assert.Contains("async void", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, Volatile.Read(ref worker.Started));
        worker.Gate.SetResult();
    }

    /// <summary>Synchronous void targets are still supported: the guard is about the async marker, not the return type.</summary>
    [Fact]
    public async Task SyncVoidImplementation_StillDispatches()
    {
        var worker = new SyncVoidWorker();
        var services = new ServiceCollection();
        services.AddSingleton<IVoidWorker>(worker);
        await using var provider = services.BuildServiceProvider();

        await provider.InvokeAsync(VoidRunDto(typeof(IVoidWorker)));
        await provider.InvokeAsync(VoidRunDto(typeof(IVoidWorker)));

        Assert.Equal(2, worker.Runs);
    }

    // ---------------------------------------------------------------------------------------------
    // M1 — the expression converter knew the exact MethodInfo but persisted only name + arity, so an
    //      interface with `Run(int)` / `Run(string)` accepted `svc => svc.Run(1)` and every dispatch
    //      of the job then failed as ambiguous — after publication.

    public interface IOverloaded
    {
        Task Run(int value);
        Task Run(string value);
        Task Unique(int value);
    }

    /// <summary>Pre-fix failure: the converter returned a descriptor; the throw came at dispatch.</summary>
    [Fact]
    public void AmbiguousOverload_FailsAtConversion_NotAtDispatch()
    {
        var ex = Assert.Throws<CallbackTargetUnresolvableException>(
            () => CallbackExpressionConverter.ToReflectionCall<IOverloaded>(svc => svc.Run(1)));

        Assert.Contains("overloads", ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(IOverloaded.Run), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UniqueMethod_StillConverts()
    {
        var dto = CallbackExpressionConverter.ToReflectionCall<IOverloaded>(svc => svc.Unique(7));

        Assert.Equal(nameof(IOverloaded.Unique), dto.MethodName);
        Assert.Equal(7, Assert.Single(dto.Params).Value);
    }

    /// <summary>Pre-fix failure: the job was published and failed on the worker; here nothing reaches the transport.</summary>
    [Fact]
    public async Task EnqueueWorker_WithAnAmbiguousTarget_ThrowsInTheCallersStack_AndPublishesNothing()
    {
        var transport = new CapturingWorkerTransport();
        await using var provider = BuildFlowProvider(transport);
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();

        await Assert.ThrowsAsync<CallbackTargetUnresolvableException>(() => builder.EnqueueWorkerAsync<IOverloaded>(svc => svc.Run(1)));

        Assert.Empty(transport.Snapshot());
    }

    /// <summary>Pre-fix failure: the registration succeeded and the recovery callback was unresolvable when it fired.</summary>
    [Fact]
    public async Task RecoveryCallbackRegistration_WithAnAmbiguousTarget_ThrowsAtRegistration()
    {
        var transport = new CapturingWorkerTransport();
        await using var provider = BuildFlowProvider(transport);
        var builder = provider.GetRequiredService<IRecoverableAsyncResponseBuilder>();

        Assert.Throws<CallbackTargetUnresolvableException>(
            () => builder.For<OperationResult>().OnLostSubscriberResume<IOverloaded>(svc => svc.Run(1)));
    }
}
