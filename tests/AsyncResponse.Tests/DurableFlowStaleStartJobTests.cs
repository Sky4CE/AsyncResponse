using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using System.Linq.Expressions;
using Xunit;
using static AsyncResponse.Tests.Round40FlowLeaseTestSupport;

namespace AsyncResponse.Tests;

/// <summary>
/// The start job carries the whole initial ledger and creates the run when none exists
/// (insert-if-absent) — which also holds long after the run finished and its ledger expired. A
/// dead-letter replay, or a Kafka consumer group rewound past the message, then "starts" the run
/// again and re-executes every completed step. A start older than the ledger lifetime with no
/// ledger behind it is dropped instead; a run that is still alive keeps the job as its wake-up.
/// </summary>
public sealed class DurableFlowStaleStartJobTests
{
    private static readonly DurableFlowOptions Options = new() { StateExpiry = TimeSpan.FromDays(14) };

    [Fact]
    public async Task ReplayedStartJob_OlderThanStateExpiry_WithNoLedger_IsDroppedLoudly_NotReExecuted()
    {
        var store = new InMemoryFlowStateStore();
        var log = new CapturingLogger<DurableFlowExecutor>();
        await using var harness = CreateHarness(store, log);
        var initial = RunnableState("stale-start-replayed");
        initial.CreatedAtUtc = initial.UpdatedAtUtc = DateTime.UtcNow.AddDays(-30);

        // Returns normally: the transport acknowledges the replayed job.
        await harness.Executor.CreateAndExecuteAsync(initial.FlowId!, FlowStateJson.Serialize(initial));

        Assert.Equal(0, harness.Flow.Executions);
        Assert.Null(await store.LoadAsync(initial.FlowId!));
        var dropped = Assert.Single(log.Entries, entry => entry.Level == LogLevel.Error);
        Assert.Contains("stale-start-replayed", dropped.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(DurableFlowOptions.StateExpiry), dropped.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StartJob_OlderThanStateExpiry_WhoseRunIsStillAlive_StillExecutesIt()
    {
        // A run parked past StateExpiry keeps its ledger through the retention floor, and the
        // start job — unacknowledged while its handler ran — is the wake-up the broker redelivers
        // when that handler dies. Age alone must never drop it.
        var store = new InMemoryFlowStateStore();
        await using var harness = CreateHarness(store, new CapturingLogger<DurableFlowExecutor>());
        var initial = RunnableState("stale-start-live-run");
        initial.CreatedAtUtc = initial.UpdatedAtUtc = DateTime.UtcNow.AddDays(-30);
        await store.TryCreateAsync(initial.FlowId!, initial, TimeSpan.FromDays(14));

        await harness.Executor.CreateAndExecuteAsync(initial.FlowId!, FlowStateJson.Serialize(initial));

        Assert.Equal(1, harness.Flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(initial.FlowId!))!.Status);
    }

    [Theory]
    [InlineData(true)]    // a start within the ledger lifetime: the starter died before its own create
    [InlineData(false)]   // a carrier without the stamp: nothing to measure, never judged
    public async Task StartJob_ThatCannotBeProvenStale_StillCreatesTheRunFromItsCarrier(bool stamped)
    {
        var store = new InMemoryFlowStateStore();
        await using var harness = CreateHarness(store, new CapturingLogger<DurableFlowExecutor>());
        var initial = RunnableState($"fresh-start-{stamped}");
        initial.CreatedAtUtc = stamped ? DateTime.UtcNow.AddDays(-13) : null;

        await harness.Executor.CreateAndExecuteAsync(initial.FlowId!, FlowStateJson.Serialize(initial));

        Assert.Equal(1, harness.Flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(initial.FlowId!))!.Status);
    }

    // ---------------------------------------------------------------------------------------
    // A create lost to a ledger that is gone by the time it is read.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task StartJob_WhoseCreateLosesToALedgerThatThenExpires_CreatesAgain_AndExecutesTheRun()
    {
        // A reused id whose previous ledger expires between the job's create ("exists") and its
        // load (gone). Acknowledging there lost the new run: the starter leaves the create to the
        // job, and the job gave up. The create is tried again.
        var store = new LosingCreateStore(new InMemoryFlowStateStore(), lostCreates: 1);
        await using var harness = CreateHarness(store, new CapturingLogger<DurableFlowExecutor>());
        var initial = RunnableState("reused-id-at-expiry");

        await harness.Executor.CreateAndExecuteAsync(initial.FlowId!, FlowStateJson.Serialize(initial));

        Assert.Equal(2, store.CreateAttempts);
        Assert.Equal(1, harness.Flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(initial.FlowId!))!.Status);
    }

    [Fact]
    public async Task StartJob_WhoseStoreKeepsReportingAnUnloadableLedger_IsHandedBack_NotAcknowledged()
    {
        var store = new LosingCreateStore(new InMemoryFlowStateStore(), lostCreates: int.MaxValue);
        await using var harness = CreateHarness(store, new CapturingLogger<DurableFlowExecutor>());
        var initial = RunnableState("reused-id-store-fault");

        var handedBack = await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.Executor.CreateAndExecuteAsync(initial.FlowId!, FlowStateJson.Serialize(initial)));

        Assert.Contains("could neither create its ledger nor load", handedBack.Message, StringComparison.Ordinal);
        Assert.Equal(DurableFlowExecutor.MaxStartCreateAttempts, store.CreateAttempts);
        Assert.Equal(0, harness.Flow.Executions);
    }

    [Theory]
    [InlineData(false)]   // the plain load misses the starter's fresh create
    [InlineData(true)]    // the plain load still shows an older run under the reused id
    public async Task StartJob_WhoseCreateLosesToALedgerALaggingReadCannotSeeYet_ReadsItCurrently_AndExecutesTheRun(bool showsAnOlderRun)
    {
        // Precommit review (S10#4 residual): after a lost create the ledger was re-read with a plain
        // load. On a store whose loads can lag behind another process's writes (Cosmos session reads)
        // that load still missed the starter's fresh create, and after three such rounds a perfectly
        // good start was handed back to the transport — or it showed the previous run under a reused
        // id, and the start was dropped as different work. Either answer is re-read currently now.
        var inner = new InMemoryFlowStateStore();
        var initial = RunnableState("lagging-read-start");
        Assert.True(await inner.TryCreateAsync(initial.FlowId!, RunnableState("lagging-read-start"), TimeSpan.FromDays(1)));
        FlowState? olderRun = null;
        if (showsAnOlderRun)
        {
            olderRun = RunnableState("lagging-read-start");
            olderRun.InputJson = System.Text.Json.JsonSerializer.Serialize(new TestFlowInput(2));
        }

        var store = new SessionLaggingStore(inner, lagging: true, olderRun);
        var log = new CapturingLogger<DurableFlowExecutor>();
        await using var harness = CreateHarness(store, log);

        await harness.Executor.CreateAndExecuteAsync(initial.FlowId!, FlowStateJson.Serialize(initial));

        Assert.Equal(1, harness.Flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await inner.LoadAsync(initial.FlowId!))!.Status);
        Assert.DoesNotContain(log.Entries, entry => entry.Level == LogLevel.Error);
        Assert.Equal(1, store.CreateAttempts);
        Assert.Equal(1, store.CurrentLoads);
    }

    [Fact]
    public async Task StartJob_WhoseCreateLosesToTheStartersLedger_DecidesOnAPlainLoad_WithoutACurrentRead()
    {
        // Pass-2 precommit review: StartAsync publishes before its own create, so a start job's create
        // normally loses and every start reaches the re-read. A current read there is an extra
        // write-path request per start on Cosmos; the plain load decides, and executing ends in
        // fenced writes. Only an answer that would end the job is read again currently.
        var inner = new InMemoryFlowStateStore();
        var initial = RunnableState("normal-start");
        Assert.True(await inner.TryCreateAsync(initial.FlowId!, RunnableState("normal-start"), TimeSpan.FromDays(1)));
        var store = new SessionLaggingStore(inner, lagging: false);
        await using var harness = CreateHarness(store, new CapturingLogger<DurableFlowExecutor>());

        await harness.Executor.CreateAndExecuteAsync(initial.FlowId!, FlowStateJson.Serialize(initial));

        Assert.Equal(1, store.CreateAttempts);
        Assert.Equal(0, store.CurrentLoads);
        Assert.Equal(1, harness.Flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await inner.LoadAsync(initial.FlowId!))!.Status);
    }

    /// <summary>
    /// When <c>lagging</c>, plain loads serve a copy behind another process's writes — nothing, or
    /// <c>staleCopy</c> (a session-consistent replica that has not applied the starter's create) —
    /// until this process takes the lease; <see cref="LoadCurrentAsync"/> is authoritative and counted.
    /// </summary>
    private sealed class SessionLaggingStore(InMemoryFlowStateStore inner, bool lagging, FlowState? staleCopy = null) : IFlowStateStore
    {
        private readonly string? _staleCopyJson = staleCopy is null ? null : FlowStateJson.Serialize(staleCopy);
        private int _createAttempts;
        private int _currentLoads;
        private volatile bool _wroteThrough;

        public int CreateAttempts => Volatile.Read(ref _createAttempts);

        public int CurrentLoads => Volatile.Read(ref _currentLoads);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _createAttempts);
            return inner.TryCreateAsync(flowId, state, ttl, cancellationToken);
        }

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => !lagging || _wroteThrough
                ? inner.LoadAsync(flowId, cancellationToken)
                : Task.FromResult(_staleCopyJson is null ? null : FlowStateJson.Deserialize(_staleCopyJson, flowId));

        public Task<FlowState?> LoadCurrentAsync(string flowId, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _currentLoads);
            return inner.LoadAsync(flowId, cancellationToken);
        }

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public async Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        {
            var acquired = await inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);
            _wroteThrough |= acquired;
            return acquired;
        }

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<FlowLeaseObservation?> ObserveLeaseAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.ObserveLeaseAsync(flowId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);
    }

    /// <summary>Answers the first <c>lostCreates</c> creates "exists" without a row behind them — a ledger that expired right after.</summary>
    private sealed class LosingCreateStore(InMemoryFlowStateStore inner, int lostCreates) : IFlowStateStore
    {
        private int _createAttempts;

        public int CreateAttempts => Volatile.Read(ref _createAttempts);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => Interlocked.Increment(ref _createAttempts) <= lostCreates
                ? Task.FromResult(false)
                : inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);
    }

    // ---------------------------------------------------------------------------------------
    // An idempotent re-start across a deploy that added a member to the input type.
    // ---------------------------------------------------------------------------------------

    /// <summary>A scheduled flow's input, one member newer than the build that wrote the ledger.</summary>
    public sealed record ShapeInput(int TenantId, string? Region = null);

    public sealed class ShapeFlow : IDurableFlow<ShapeInput>
    {
        private int _executions;

        public int Executions => Volatile.Read(ref _executions);

        public Task ExecuteAsync(IDurableFlowContext context, ShapeInput input)
        {
            Interlocked.Increment(ref _executions);
            return Task.CompletedTask;
        }
    }

    /// <summary>The ledger the previous build wrote and never executed: the input without the new member.</summary>
    private static FlowState OldShapeLedger(string flowId) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(ShapeFlow).FullName,
        InputTypeName = typeof(ShapeInput).FullName,
        InputJson = "{\"TenantId\":7}",
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    [Fact]
    public async Task StartJob_ForARunWrittenBeforeTheInputGainedAMember_IsTheSameStart_AndExecutesIt()
    {
        // The scheduler's startup probe re-drives a never-executed occurrence the previous build
        // wrote: the new build serializes the same value with the new member ("Region":null). A
        // shape comparison called that different work, dropped the published start job, and left
        // the run Running with zero attempts.
        var store = new InMemoryFlowStateStore();
        await store.TryCreateAsync("shape-start-job", OldShapeLedger("shape-start-job"), TimeSpan.FromDays(1));
        var flow = new ShapeFlow();
        var services = new ServiceCollection();
        services.AddSingleton<IFlowStateStore>(store);
        services.AddSingleton(flow);
        await using var provider = services.BuildServiceProvider();
        var log = new CapturingLogger<DurableFlowExecutor>();
        var executor = new DurableFlowExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Mock.Of<IAsyncResponseBuilder>(),
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            new AsyncResponseContextPropagation([]),
            Options,
            log,
            registrations: [ShapeFlowRegistration()]);

        var carrier = OldShapeLedger("shape-start-job");
        carrier.InputJson = AsyncResponseJson.Serialize(new ShapeInput(7));
        Assert.Contains("Region", carrier.InputJson, StringComparison.Ordinal);

        await executor.CreateAndExecuteAsync(carrier.FlowId!, FlowStateJson.Serialize(carrier));

        Assert.Equal(1, flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(carrier.FlowId!))!.Status);
        Assert.DoesNotContain(log.Entries, entry => entry.Level == LogLevel.Error);
    }

    [Fact]
    public async Task StartJob_OfAFlowExecutedByReflection_ForARunWrittenBeforeTheInputGainedAMember_IsTheSameStart_AndExecutesIt()
    {
        // Precommit review (A2): a flow in DI but not registered with WithDurableFlow has no
        // registration, and the start job fell back to comparing the JSON shape while the starter
        // compares the value — the starter logged "re-enqueues the existing run" for a caller's
        // retry across the deploy, and this job dropped it as different work (S1#9 again).
        var store = new InMemoryFlowStateStore();
        await store.TryCreateAsync("shape-start-job-reflective", OldShapeLedger("shape-start-job-reflective"), TimeSpan.FromDays(1));
        var flow = new ShapeFlow();
        var services = new ServiceCollection();
        services.AddSingleton<IFlowStateStore>(store);
        services.AddSingleton(flow);
        await using var provider = services.BuildServiceProvider();
        var log = new CapturingLogger<DurableFlowExecutor>();
        var executor = new DurableFlowExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Mock.Of<IAsyncResponseBuilder>(),
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            new AsyncResponseContextPropagation([]),
            Options,
            log);

        var carrier = OldShapeLedger("shape-start-job-reflective");
        carrier.InputJson = AsyncResponseJson.Serialize(new ShapeInput(7));
        Assert.Contains("Region", carrier.InputJson, StringComparison.Ordinal);

        await executor.CreateAndExecuteAsync(carrier.FlowId!, FlowStateJson.Serialize(carrier));

        Assert.Equal(1, flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(carrier.FlowId!))!.Status);
        Assert.DoesNotContain(log.Entries, entry => entry.Level == LogLevel.Error);

        // A genuinely different value is still different work: dropped, loudly, the run untouched.
        await store.TryCreateAsync("shape-start-job-reflective-other", OldShapeLedger("shape-start-job-reflective-other"), TimeSpan.FromDays(1));
        var other = OldShapeLedger("shape-start-job-reflective-other");
        other.InputJson = AsyncResponseJson.Serialize(new ShapeInput(8));
        await executor.CreateAndExecuteAsync(other.FlowId!, FlowStateJson.Serialize(other));
        Assert.Equal(1, flow.Executions);
        Assert.Single(log.Entries, entry => entry.Level == LogLevel.Error);
    }

    /// <summary>An input type that counts its constructions: store data must not reach it before the flow-contract check.</summary>
    public sealed class CanaryInput
    {
        private static int _constructed;

        [System.Text.Json.Serialization.JsonConstructor]
        public CanaryInput(int tenantId)
        {
            Interlocked.Increment(ref _constructed);
            TenantId = tenantId;
        }

        public static int Constructed => Volatile.Read(ref _constructed);

        public int TenantId { get; }

        public string? Region { get; init; }
    }

    /// <summary>Declares <see cref="CanaryInput"/> as its input.</summary>
    public sealed class CanaryFlow : IDurableFlow<CanaryInput>
    {
        public Task ExecuteAsync(IDurableFlowContext context, CanaryInput input) => Task.CompletedTask;
    }

    [Theory]
    [InlineData(false)]  // the ledger names a DI service that does not implement IDurableFlow<CanaryInput>
    [InlineData(true)]   // the ledger names a flow that implements it but is not registered in DI
    public async Task StartJob_ComparedByReflection_NeverDeserializesTheInput_OfAFlowThatFailsTheContractChecks(bool implementsTheContract)
    {
        // The reflection path's value comparison reads both inputs as the ledger's input type — a
        // name anyone who can write the store controls. Like the execution itself, it does so only
        // for an input type a DI-registered flow declares; anything else keeps the shape comparison.
        var flowId = $"canary-start-{implementsTheContract}";
        var ledger = new FlowState
        {
            FlowId = flowId,
            FlowTypeName = implementsTheContract ? typeof(CanaryFlow).FullName : typeof(ShapeFlow).FullName,
            InputTypeName = typeof(CanaryInput).FullName,
            InputJson = "{\"TenantId\":7}",
            Status = FlowRunStatus.Running,
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow
        };
        var store = new InMemoryFlowStateStore();
        await store.TryCreateAsync(flowId, ledger, TimeSpan.FromDays(1));
        var services = new ServiceCollection();
        services.AddSingleton<IFlowStateStore>(store);
        services.AddSingleton(new ShapeFlow());
        await using var provider = services.BuildServiceProvider();
        var log = new CapturingLogger<DurableFlowExecutor>();
        var executor = new DurableFlowExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Mock.Of<IAsyncResponseBuilder>(),
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            new AsyncResponseContextPropagation([]),
            Options,
            log);
        var carrier = FlowStateJson.Deserialize(FlowStateJson.Serialize(ledger), flowId);
        carrier.InputJson = "{\"TenantId\":7,\"Region\":null}";
        var constructedBefore = CanaryInput.Constructed;

        await executor.CreateAndExecuteAsync(flowId, FlowStateJson.Serialize(carrier));

        Assert.Equal(constructedBefore, CanaryInput.Constructed);
        Assert.Single(log.Entries, entry => entry.Level == LogLevel.Error);
        Assert.Equal(0, (await store.LoadAsync(flowId))!.Attempts);
    }

    [Fact]
    public async Task StartAsync_RetriedAcrossADeployThatAddedAnInputMember_IsIdempotent_NotAConflict()
    {
        var store = new InMemoryFlowStateStore();
        await store.TryCreateAsync("shape-start-retry", OldShapeLedger("shape-start-retry"), TimeSpan.FromDays(1));
        var services = new ServiceCollection();
        services.AddSingleton<IFlowStateStore>(store);
        await using var provider = services.BuildServiceProvider();
        var starter = new DurableFlowService(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Mock.Of<IAsyncResponseBuilder>(),
            new AsyncResponseContextPropagation([]),
            Options,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DurableFlowService>.Instance);

        Assert.Equal("shape-start-retry", await starter.StartAsync<ShapeFlow, ShapeInput>(new ShapeInput(7), "shape-start-retry"));

        // A genuinely different value is still a conflict.
        await Assert.ThrowsAsync<DurableFlowIdConflictException>(
            () => starter.StartAsync<ShapeFlow, ShapeInput>(new ShapeInput(8), "shape-start-retry"));
    }

    // ---------------------------------------------------------------------------------------
    // The starter around its commit point.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task StartAsync_WithACallerIdAlreadyBoundToDifferentWork_PublishesNothing()
    {
        // Fixpoint r1 (GS1#9): the conflict was found only after the start job was published — the
        // caller was told "refused" while the job sat in the queue, and if the old ledger expired
        // before it ran, the job created the refused run anyway.
        var store = new InMemoryFlowStateStore();
        await store.TryCreateAsync("bound-id", OldShapeLedger("bound-id"), TimeSpan.FromDays(1));
        var (starter, published, _) = CreateStarter(store);

        await Assert.ThrowsAsync<DurableFlowIdConflictException>(
            () => starter.StartAsync<ShapeFlow, ShapeInput>(new ShapeInput(8), "bound-id"));

        Assert.Equal(0, published());
    }

    [Fact]
    public async Task StartAsync_WhoseLedgerReadFailsAfterThePublish_StillReturnsTheId()
    {
        // Fixpoint r1 (GS1#4): the start job is published — the run WILL execute — but the read
        // after a lost create threw straight out of StartAsync with no id, and a caller retrying
        // with a generated id started a second, independent run.
        var store = new FailingAfterPublishStore(new InMemoryFlowStateStore()) { LoseCreate = true, FailLoads = true };
        var (starter, published, _) = CreateStarter(store);

        var id = await starter.StartAsync<ShapeFlow, ShapeInput>(new ShapeInput(7));

        Assert.StartsWith("flow-", id, StringComparison.Ordinal);
        Assert.Equal(1, published());
    }

    [Fact]
    public async Task StartAsync_WhoseCallerCancelsRightAfterThePublish_StillCreatesTheLedger_AndReturnsTheId()
    {
        // ...and the caller's token cancelled between the publish and the create threw a plain
        // cancellation for a start that runs anyway. Nothing past the commit point is interruptible.
        var store = new InMemoryFlowStateStore();
        using var cancellation = new CancellationTokenSource();
        var (starter, published, _) = CreateStarter(store, onPublished: cancellation.Cancel);

        var id = await starter.StartAsync<ShapeFlow, ShapeInput>(new ShapeInput(7), "cancelled-after-publish", cancellation.Token);

        Assert.Equal("cancelled-after-publish", id);
        Assert.Equal(1, published());
        Assert.NotNull(await store.LoadAsync(id));
    }

    private static (DurableFlowService Starter, Func<int> Published, ServiceProvider Provider) CreateStarter(IFlowStateStore store, Action? onPublished = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(store);
        var provider = services.BuildServiceProvider();
        var publishes = 0;
        var builder = new Mock<IAsyncResponseBuilder>();
        builder.Setup(instance => instance.EnqueueWorkerAsync(
                It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<CancellationToken>()))
            .Callback(() =>
            {
                Interlocked.Increment(ref publishes);
                onPublished?.Invoke();
            })
            .Returns(Task.CompletedTask);
        var starter = new DurableFlowService(
            provider.GetRequiredService<IServiceScopeFactory>(),
            builder.Object,
            new AsyncResponseContextPropagation([]),
            Options,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DurableFlowService>.Instance);
        return (starter, () => Volatile.Read(ref publishes), provider);
    }

    /// <summary>A store whose create can be lost and whose reads can fail — the shapes a starter meets after its publish.</summary>
    private sealed class FailingAfterPublishStore(InMemoryFlowStateStore inner) : IFlowStateStore
    {
        public bool LoseCreate { get; init; }

        public bool FailLoads { get; init; }

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => LoseCreate ? Task.FromResult(false) : inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => FailLoads ? Task.FromException<FlowState?>(new TimeoutException("store unreachable")) : inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);
    }

    private static DurableFlowRegistration ShapeFlowRegistration() => new()
    {
        FlowTypeFullName = typeof(ShapeFlow).FullName!,
        InputTypeFullName = typeof(ShapeInput).FullName!,
        FlowType = typeof(ShapeFlow),
        DeserializeInput = static json => JsonSafety.SafeDeserialize<ShapeInput>(json),
        ExecuteAsync = static (flow, context, input) => ((ShapeFlow)flow).ExecuteAsync(context, (ShapeInput)input!)
    };

    private static ExecutorHarness CreateHarness(IFlowStateStore store, ILogger<DurableFlowExecutor> log)
    {
        var services = new ServiceCollection();
        services.AddSingleton(store);
        services.AddSingleton<CountingFlow>();
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
            Options,
            log);
        return new ExecutorHarness(provider, executor);
    }
}
