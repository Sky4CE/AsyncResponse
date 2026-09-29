using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Channels.NATS;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.Redis;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regressions for round 49 (external 79/100 review of b483b654): behavior pins that compile
/// against the pre-fix tree and fail there. Pins over the batch-deletion seam this round introduced
/// (<c>IRecoveryStateBatchDeletion</c>) live in <see cref="Round49NewApiTests"/>.
/// </summary>
public sealed class Round49RegressionTests
{
    public sealed record R49Input(int Value);

    public sealed class R49ChildFlow : IDurableFlow<R49Input>
    {
        public Task ExecuteAsync(IDurableFlowContext flow, R49Input input) => Task.CompletedTask;
    }

    // ---------------------------------------------------------------------------------------------
    // F1 — a parent memoized a stale child outcome. AwaitChildFlowAsync read the child through a
    //      plain LoadAsync, which may return an older copy of a present ledger; under a reused
    //      child id that copy is the PREVIOUS run. Its terminal status was memoized into the
    //      parent's ledger — a write whose fences cover the parent, not the child — so a transient
    //      stale read became the parent's permanent answer on every replay.

    private static FlowState State(string id, string? parent = null, FlowRunStatus status = FlowRunStatus.Running, int input = 1) => new()
    {
        FlowId = id,
        ParentFlowId = parent,
        ParentStepName = parent is null ? null : "child",
        Status = status,
        FlowTypeName = typeof(R49ChildFlow).FullName,
        InputTypeName = typeof(R49Input).FullName,
        InputJson = JsonSerializer.Serialize(new R49Input(input)),
        LastMessage = status == FlowRunStatus.Running ? null : $"previous run {status}",
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    private static ServiceProvider BuildContextProvider(IWorkerTransport transport, TimeProvider clock)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(clock);
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<R49ChildFlow, R49Input>();
        services.AddSingleton(transport);
        return services.BuildServiceProvider();
    }

    private static DurableFlowContext CreateContext(
        ServiceProvider provider,
        FlowState state,
        IFlowStateStore store,
        FlowExecutionLease lease,
        DurableFlowOptions options,
        TimeProvider clock,
        IWorkerTransport transport)
        => new(
            state,
            store,
            provider.GetRequiredService<IAsyncResponseBuilder>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>(),
            options,
            provider.GetRequiredService<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            NullLogger.Instance,
            lease,
            clock,
            workerTransport: transport);

    /// <summary>
    /// Pre-fix failure: the parent read the child's previous run through the lagging plain load
    /// and memoized it — a Failed copy failed the parent terminally (and every replay with it)
    /// while the current child was still running; a Succeeded copy handed the parent the previous
    /// run's result. Now the current child is read, the parent suspends on it, and nothing is
    /// memoized.
    /// </summary>
    [Theory]
    [InlineData(FlowRunStatus.Failed)]
    [InlineData(FlowRunStatus.Succeeded)]
    public async Task AwaitChildFlow_ALaggingCopyOfTheChildsPreviousRun_IsNotMemoized_TheParentWaitsForTheCurrentRun(FlowRunStatus previousRun)
    {
        var clock = new VirtualTimeProvider();
        var transport = new CapturingDelayedTransport();
        await using var provider = BuildContextProvider(transport, clock);
        var inner = provider.GetRequiredService<IFlowStateStore>();
        var parentId = $"r49-reused-{previousRun}";
        var childId = $"{parentId}:child";

        // The parent already created the (current) child and is re-executed — a duplicate wake-up,
        // a redelivery — while the child runs.
        var parent = State(parentId);
        parent.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal) { ["child"] = new() { ChildFlowId = childId } };
        Assert.True(await inner.TryCreateAsync(parentId, parent, TimeSpan.FromDays(1)));
        Assert.True(await inner.TryCreateAsync(childId, State(childId, parentId), TimeSpan.FromDays(1)));

        var store = new LaggingFlowStateStore(inner);
        store.ServeStaleCopy(childId, State(childId, parentId, previousRun));

        var options = new DurableFlowOptions();
        await using var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, parentId, options, NullLogger.Instance, clock))!;
        var context = CreateContext(provider, (await inner.LoadAsync(parentId))!, store, lease, options, clock, transport);

        await Assert.ThrowsAsync<DurableFlowSuspendedException>(
            () => context.AwaitChildFlowAsync<R49ChildFlow, R49Input>("child", new R49Input(1)));

        var step = (await inner.LoadAsync(parentId))!.Steps!["child"];
        Assert.False(step.Completed);
        Assert.False(step.Faulted);
        Assert.Null(step.ResultJson);
        // The suspension re-enqueued the current child, which re-notifies the parent when it ends.
        Assert.True(transport.Count >= 1);
    }

    /// <summary>
    /// Pre-fix failure: a lagging copy of a previous run that another parent owned failed the
    /// ownership check — terminally, as an id collision — although the current child is this
    /// parent's own.
    /// </summary>
    [Fact]
    public async Task AwaitChildFlow_ALaggingCopyBoundToAnotherParent_DoesNotFailTheParentAsAnIdCollision()
    {
        var clock = new VirtualTimeProvider();
        var transport = new CapturingDelayedTransport();
        await using var provider = BuildContextProvider(transport, clock);
        var inner = provider.GetRequiredService<IFlowStateStore>();
        const string parentId = "r49-owner";
        const string childId = "r49-shared-child";

        var parent = State(parentId);
        parent.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal) { ["child"] = new() { ChildFlowId = childId } };
        Assert.True(await inner.TryCreateAsync(parentId, parent, TimeSpan.FromDays(1)));
        Assert.True(await inner.TryCreateAsync(childId, State(childId, parentId), TimeSpan.FromDays(1)));

        var store = new LaggingFlowStateStore(inner);
        store.ServeStaleCopy(childId, State(childId, "r49-previous-owner", FlowRunStatus.Succeeded, input: 7));

        var options = new DurableFlowOptions();
        await using var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, parentId, options, NullLogger.Instance, clock))!;
        var context = CreateContext(provider, (await inner.LoadAsync(parentId))!, store, lease, options, clock, transport);

        await Assert.ThrowsAsync<DurableFlowSuspendedException>(
            () => context.AwaitChildFlowAsync<R49ChildFlow, R49Input>("child", new R49Input(1), flowId: childId));

        Assert.False((await inner.LoadAsync(parentId))!.Steps!["child"].Completed);
    }

    /// <summary>
    /// The second child read, after the create lost to a concurrent creator. Pre-fix failure: the
    /// plain re-read returned the previous run under the reused id and the parent memoized its
    /// failure; now the winner's current ledger is read and the parent suspends on it.
    /// </summary>
    [Fact]
    public async Task AwaitChildFlow_CreateLostToAConcurrentCreator_ReadsTheWinnerCurrently()
    {
        var clock = new VirtualTimeProvider();
        var transport = new CapturingDelayedTransport();
        await using var provider = BuildContextProvider(transport, clock);
        var inner = provider.GetRequiredService<IFlowStateStore>();
        const string parentId = "r49-lost-create";
        const string childId = $"{parentId}:child";
        Assert.True(await inner.TryCreateAsync(parentId, State(parentId), TimeSpan.FromDays(1)));

        var store = new LaggingFlowStateStore(inner);
        // Before the create the id has no ledger at all; once another execution has created the
        // current child, this process's plain reads still show the id's previous run.
        store.ServeStaleCopy(childId, State(childId, parentId, FlowRunStatus.Failed), onlyAfterCreateAttempt: true);
        store.BeforeCreate = (id, _) => inner.TryCreateAsync(id, State(id, parentId), TimeSpan.FromDays(1));

        var options = new DurableFlowOptions();
        await using var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, parentId, options, NullLogger.Instance, clock))!;
        var context = CreateContext(provider, (await inner.LoadAsync(parentId))!, store, lease, options, clock, transport);

        await Assert.ThrowsAsync<DurableFlowSuspendedException>(
            () => context.AwaitChildFlowAsync<R49ChildFlow, R49Input>("child", new R49Input(1)));

        var step = (await inner.LoadAsync(parentId))!.Steps!["child"];
        Assert.False(step.Completed);
        Assert.Equal(childId, step.ChildFlowId);
        Assert.Equal(FlowRunStatus.Running, (await inner.LoadAsync(childId))!.Status);
    }

    private static readonly DurableFlowOptions ShortLedgerOptions = new()
    {
        StateExpiry = TimeSpan.FromMinutes(1),
        TimerInProcessThreshold = TimeSpan.Zero
    };

    /// <summary>
    /// The same invariant in the parked child's ancestor walk. Pre-fix failure: the walk stopped
    /// on a lagging copy of a reused ancestor id — the previous run finished, or carrying its own
    /// retention floor — without extending the CURRENT run, which then expired two virtual minutes
    /// into the child's hour-long park (and the child's completion would find no parent).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AncestorExtension_ALaggingCopyOfAReusedAncestorsPreviousRun_StillExtendsTheCurrentRun(bool previousRunCoversThePark)
    {
        var clock = new VirtualTimeProvider();
        var transport = new CapturingDelayedTransport();
        await using var provider = BuildContextProvider(transport, clock);
        var inner = provider.GetRequiredService<IFlowStateStore>();
        const string rootId = "r49-root";
        const string childId = "r49-root:child";
        var root = State(rootId);
        root.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal) { ["child"] = new() { ChildFlowId = childId } };
        Assert.True(await inner.TryCreateAsync(rootId, root, TimeSpan.FromMinutes(1)));
        Assert.True(await inner.TryCreateAsync(childId, State(childId, rootId), TimeSpan.FromMinutes(1)));

        var store = new LaggingFlowStateStore(inner);
        var previous = previousRunCoversThePark
            ? State(rootId)
            : State(rootId, status: FlowRunStatus.Succeeded);
        if (previousRunCoversThePark)
            previous.RetainUntilUtc = clock.GetUtcNow().UtcDateTime.AddDays(30);
        store.ServeStaleCopy(rootId, previous);

        await using (var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, childId, ShortLedgerOptions, NullLogger.Instance, clock))!)
        {
            var context = CreateContext(provider, (await inner.LoadAsync(childId))!, store, lease, ShortLedgerOptions, clock, transport);
            await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => context.DelayAsync("long-wait", TimeSpan.FromHours(1)));
        }

        // The park's window is an hour; the current root outlives its own one-minute expiry.
        clock.Advance(TimeSpan.FromMinutes(2));
        Assert.NotNull(await inner.LoadAsync(rootId));
        Assert.NotNull(await inner.LoadAsync(childId));
    }

    /// <summary>
    /// The confirmation's null is the answer. Pre-fix (and in this round's first cut, which fell
    /// back to the lagging copy): the walk followed the previous run's ParentFlowId from a covered
    /// copy of an ancestor whose ledger is gone, and raised the floor of a ledger in a chain that is
    /// not this park's.
    /// </summary>
    [Fact]
    public async Task AncestorExtension_AnAncestorGoneBehindALaggingCoveredCopy_EndsTheWalk_WithoutTouchingThePreviousRunsChain()
    {
        var clock = new VirtualTimeProvider();
        var transport = new CapturingDelayedTransport();
        await using var provider = BuildContextProvider(transport, clock);
        var inner = provider.GetRequiredService<IFlowStateStore>();
        const string goneId = "r49-gone";
        const string childId = "r49-gone:child";
        const string previousParentId = "r49-previous-parent";
        Assert.True(await inner.TryCreateAsync(previousParentId, State(previousParentId), TimeSpan.FromMinutes(1)));
        Assert.True(await inner.TryCreateAsync(childId, State(childId, goneId), TimeSpan.FromMinutes(1)));

        var store = new LaggingFlowStateStore(inner);
        var previous = State(goneId, previousParentId);
        previous.RetainUntilUtc = clock.GetUtcNow().UtcDateTime.AddDays(30);
        store.ServeStaleCopy(goneId, previous);

        await using (var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, childId, ShortLedgerOptions, NullLogger.Instance, clock))!)
        {
            var context = CreateContext(provider, (await inner.LoadAsync(childId))!, store, lease, ShortLedgerOptions, clock, transport);
            await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => context.DelayAsync("long-wait", TimeSpan.FromHours(1)));
        }

        Assert.Null((await inner.LoadAsync(previousParentId))!.RetainUntilUtc);
    }

    private sealed class CapturingDelayedTransport : IDelayedWorkerTransport
    {
        private readonly List<WorkerJobEnvelope> _jobs = [];

        public TimeSpan MaxPublishDelay => TimeSpan.FromDays(30);

        public int Count
        {
            get { lock (_jobs) return _jobs.Count; }
        }

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            lock (_jobs)
                _jobs.Add(job);
            return Task.CompletedTask;
        }

        public Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default)
            => PublishAsync(job, cancellationToken);
    }

    /// <summary>
    /// A store read from a process whose PLAIN loads of some ids still return an older copy — the
    /// previous run under a reused id — while <see cref="IFlowStateStore.LoadCurrentAsync"/> and
    /// every write see the current ledger (a Cosmos session read, a MongoDB read off a deposed
    /// primary).
    /// </summary>
    private sealed class LaggingFlowStateStore(IFlowStateStore inner) : IFlowStateStore
    {
        private readonly ConcurrentDictionary<string, (FlowState Copy, bool AfterCreate)> _stale = new(StringComparer.Ordinal);
        private volatile bool _createAttempted;

        /// <summary>Runs inside the first create instead of it, and answers it (a concurrent creator won).</summary>
        public Func<string, FlowState, Task<bool>>? BeforeCreate { get; set; }

        public void ServeStaleCopy(string flowId, FlowState copy, bool onlyAfterCreateAttempt = false)
            => _stale[flowId] = (copy, onlyAfterCreateAttempt);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => _stale.TryGetValue(flowId, out var stale) && (!stale.AfterCreate || _createAttempted)
                ? Task.FromResult<FlowState?>(Copy(stale.Copy))
                : inner.LoadAsync(flowId, cancellationToken);

        public Task<FlowState?> LoadCurrentAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.LoadCurrentAsync(flowId, cancellationToken);

        public async Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
        {
            if (BeforeCreate is { } hook)
            {
                BeforeCreate = null;
                _createAttempted = true;
                await hook(flowId, state);
                return false;
            }

            return await inner.TryCreateAsync(flowId, state, ttl, cancellationToken);
        }

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<FlowLeaseObservation?> ObserveLeaseAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.ObserveLeaseAsync(flowId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);

        private static FlowState Copy(FlowState state) => FlowStateJson.Deserialize(FlowStateJson.Serialize(state), state.FlowId!);
    }

    // ---------------------------------------------------------------------------------------------
    // F3 — a lookup that found readable AND unreadable registrations returned the readable ones.
    //      The dispatcher invoked and consumed them, the transport acknowledged the response, and
    //      the unreadable registration (a newer schema mid-rolling-upgrade, a corrupt row) stayed
    //      armed with no payload left to deliver: deploying a build that can read it recovered
    //      nothing. One unreadable registration now refuses the whole lookup, before any callback.

    private static RecoveryState Readable(string correlationId, string method = nameof(IR49Spy.Resume)) => new()
    {
        RegistrationId = Guid.NewGuid(),
        CorrelationId = correlationId,
        PayloadTypeFullName = typeof(OperationResult).FullName,
        RegisteredAtUtc = DateTime.UtcNow,
        ResumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IR49Spy).FullName!,
            MethodName = method,
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
        },
        FailureCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IR49Spy).FullName!,
            MethodName = nameof(IR49Spy.Fail),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
        }
    };

    private static RecoveryState NewerSchema(string correlationId)
    {
        var state = Readable(correlationId);
        state.SchemaVersion = RecoveryStateSchema.Current + 1;
        return state;
    }

    [Fact]
    public async Task RedisLookup_AReadableAndANewerSchemaRegistration_IsRefusedAsAWhole()
    {
        var redis = new StatefulRedis();
        var store = redis.CreateStore(new TestTimeProvider());
        var expires = new TestTimeProvider().Now + TimeSpan.FromMinutes(5);
        redis.Value = "{\"Registrations\":["
                      + $"{{\"State\":{JsonSerializer.Serialize(Readable("r49-mixed"))},\"ExpiresAtUtc\":\"{expires:O}\"}},"
                      + $"{{\"State\":{JsonSerializer.Serialize(NewerSchema("r49-mixed"))},\"ExpiresAtUtc\":\"{expires:O}\"}}"
                      + "]}";

        var refused = await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => store.GetAllAsync("r49-mixed"));

        Assert.Equal("r49-mixed", refused.CorrelationId);
        Assert.Equal(1, refused.UnreadableCount);
    }

    [Fact]
    public async Task NatsLookup_AReadableAndANewerSchemaRegistration_IsRefusedAsAWhole()
    {
        var kv = new FakeNatsKvStore();
        var store = new NatsRecoveryStateStore(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance, new TestTimeProvider());
        await SeedNatsAsync(kv, store, "r49-mixed", Readable("r49-mixed"), NewerSchema("r49-mixed"));

        var refused = await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => store.GetAllAsync("r49-mixed"));

        Assert.Equal(1, refused.UnreadableCount);
    }

    public static TheoryData<string> DatabaseRecoveryStores => ["PostgreSql", "SqlServer", "MongoDb"];

    /// <summary>The shared database reader (PostgreSQL, SQL Server, MongoDB): a malformed row, and a newer-schema row.</summary>
    [Theory]
    [MemberData(nameof(DatabaseRecoveryStores))]
    public void DatabaseLookup_AReadableRowBesideAnUnreadableOne_IsRefusedAsAWhole(string provider)
    {
        object store = provider switch
        {
            "PostgreSql" => new PostgreSqlRecoveryStateStore(null!, NullLogger<PostgreSqlRecoveryStateStore>.Instance),
            "SqlServer" => new SqlServerRecoveryStateStore(null!, NullLogger<SqlServerRecoveryStateStore>.Instance),
            _ => new MongoDbRecoveryStateStore(null!, NullLogger<MongoDbRecoveryStateStore>.Instance)
        };
        var deserializeStates = store.GetType().BaseType!.GetMethod("DeserializeStates", BindingFlags.Instance | BindingFlags.NonPublic)!;

        foreach (var unreadable in new[] { JsonSerializer.Serialize(NewerSchema("r49-db")), "{not-json" })
        {
            IReadOnlyList<string> rows = [JsonSerializer.Serialize(Readable("r49-db")), unreadable];
            var thrown = Assert.Throws<TargetInvocationException>(() => deserializeStates.Invoke(store, [rows, "r49-db"]));
            var refused = Assert.IsType<RecoveryStateUnreadableException>(thrown.InnerException);
            Assert.Equal(1, refused.UnreadableCount);
        }

        // A readable row carrying ANOTHER correlation id (a legacy case-insensitive collation's
        // match) is absence for this id, not corruption: it does not refuse the lookup.
        var other = Readable("R49-DB");
        IReadOnlyList<string> collated = [JsonSerializer.Serialize(Readable("r49-db")), JsonSerializer.Serialize(other)];
        var states = (IReadOnlyList<RecoveryState>)deserializeStates.Invoke(store, [collated, "r49-db"])!;
        Assert.Equal("r49-db", Assert.Single(states).CorrelationId);
    }

    public interface IR49Spy
    {
        Task Resume(OperationResult payload);
        Task Fail(Exception exception);
    }

    private sealed class R49Spy : IR49Spy
    {
        private int _resumed;
        private int _failed;

        public int Resumed => Volatile.Read(ref _resumed);
        public int Failed => Volatile.Read(ref _failed);

        public Task Resume(OperationResult payload)
        {
            Interlocked.Increment(ref _resumed);
            return Task.CompletedTask;
        }

        public Task Fail(Exception exception)
        {
            Interlocked.Increment(ref _failed);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// End to end through the channel's publisher — what a transport ingress or an HTTP callback
    /// endpoint calls. Pre-fix failure: the publish returned normally (the broker would have
    /// acknowledged it) after invoking the readable registration's callback, and the newer-schema
    /// registration was left armed with the payload gone. Now the publish fails with nothing
    /// invoked and both registrations kept; once the unreadable registration is readable (a
    /// compatible build, an operator's repair), the redelivered response settles BOTH, each once.
    /// </summary>
    [Fact]
    public async Task LostResponse_WithAnUnreadableSibling_IsNotSettled_AndARedeliveryAfterRepairSettlesEveryRegistration()
    {
        var spy = new R49Spy();
        var kv = new FakeNatsKvStore();
        var recoveryStore = new NatsRecoveryStateStore(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance, new TestTimeProvider());
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IR49Spy>(spy);
        services.AddSingleton<IRecoveryStateStore>(recoveryStore);
        services.AddAsyncResponse().WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        const string correlationId = "r49-rolling-upgrade";
        var current = Readable(correlationId);
        var newer = NewerSchema(correlationId);
        await SeedNatsAsync(kv, recoveryStore, correlationId, current, newer);
        var response = new OperationResult { Status = OperationStatus.Completed, Message = "terminal" };

        await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => publisher.SetResponse(response, correlationId));

        Assert.Equal(0, spy.Resumed);
        Assert.Equal(2, StoredNatsRegistrations(kv, correlationId).Count);

        // The build that wrote the newer registration is the one a redelivery now reaches; here,
        // the registration is rewritten in the schema this build reads.
        RewriteNatsSchemaVersions(kv, correlationId, RecoveryStateSchema.Current);
        await publisher.SetResponse(response, correlationId);

        Assert.Equal(2, spy.Resumed);
        Assert.False(kv.Entries.ContainsKey(NatsSubjectSchema.RecoveryKey(correlationId)));
    }

    /// <summary>
    /// The store's refusal must not pre-empt the snapshot-race re-check: a waiter that subscribed
    /// in time takes the response live. Pre-fix (for an all-unreadable lookup since round 28, and
    /// for a mixed one in this round's first cut) the dispatch failed with the store's exception
    /// instead — a direct <c>SetResponse</c> caller saw it. With no live subscriber it propagates,
    /// so the delivery is not acknowledged.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ARefusedLookup_StillYieldsToALiveSubscriber_AndPropagatesWithoutOne(bool exceptionRoute)
    {
        var store = new RefusingRecoveryStateStore();
        var dispatcher = new LostSubscriberCallbackDispatcher(
            new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            new AsyncResponseContextPropagation([]),
            NullLogger.Instance);

        Task<LostSubscriberDispatchResult> Dispatch(Func<ValueTask<bool>>? hasLiveSubscriber) => exceptionRoute
            ? dispatcher.DispatchLostExceptions(store, "r49-refused", new TimeoutException("gave up"), "r49", CancellationToken.None, hasLiveSubscriber)
            : dispatcher.DispatchLostResponses(store, "r49-refused", new OperationResult { Status = OperationStatus.Completed }, "r49", CancellationToken.None, hasLiveSubscriber);

        var live = await Dispatch(() => new ValueTask<bool>(true));
        Assert.True(live.RetryLive);
        Assert.False(live.CallbackInvoked);

        await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => Dispatch(() => new ValueTask<bool>(false)));
        await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => Dispatch(null));
    }

    private sealed class RefusingRecoveryStateStore : IRecoveryStateStore
    {
        public Task SaveAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default)
            => throw new RecoveryStateUnreadableException(correlationId, 1);

        public Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
    }

    // ---------------------------------------------------------------------------------------------
    // F2 — every consumed registration of a fan-out was deleted on its own. Redis and NATS keep a
    //      correlation id's registrations together in one value, so each delete read, parsed,
    //      filtered and rewrote everything that remained: N registrations cost N + (N−1) + … + 1
    //      entries rewritten and N round trips while the delivery stayed open.

    /// <summary>
    /// A payload type whose registrations always wait past a response: the one registration of the
    /// fan-out that is NOT consumed, so the batch removal has a survivor to rewrite around.
    /// </summary>
    public sealed class R49CheckpointPayload : IAsyncResponsePayload
    {
        public string? Message { get; set; }

        public RecoveryAction OnRecovery() => RecoveryAction.KeepWaiting;
    }

    private static RecoveryState Survivor(string correlationId)
    {
        var state = Readable(correlationId);
        state.PayloadTypeFullName = typeof(R49CheckpointPayload).FullName;
        return state;
    }

    private static (ServiceProvider Provider, LostSubscriberCallbackDispatcher Dispatcher) CreateDispatcher(R49Spy spy, Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IR49Spy>(spy);
        configure?.Invoke(services);
        services.AddAsyncResponse().WithInMemoryChannel();
        var provider = services.BuildServiceProvider();
        return (provider, new LostSubscriberCallbackDispatcher(
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>(),
            NullLogger.Instance));
    }

    /// <summary>
    /// Pre-fix failure: N consumed registrations cost N conditional rewrites (the last one a key
    /// delete) and N + 1 reads, rewriting ~N²/2 registrations in total. Now the fan-out costs two
    /// reads and ONE rewrite — of the survivor alone, with its own expiry — whatever N is.
    /// </summary>
    // 63 consumed + the survivor = 64, the default MaxRecoveryRegistrationsPerCorrelationId (round 51).
    [Theory]
    [InlineData(16)]
    [InlineData(63)]
    public async Task RedisFanOut_ConsumedRegistrations_AreRemovedInOneRewrite_WhateverTheirNumber(int consumedCount)
    {
        var spy = new R49Spy();
        var (provider, dispatcher) = CreateDispatcher(spy);
        await using var _ = provider;
        var time = new TestTimeProvider();
        var redis = new StatefulRedis();
        var store = redis.CreateStore(time);
        const string correlationId = "r49-redis-fan-out";

        var survivor = Survivor(correlationId);
        await store.SaveAsync(correlationId, survivor, TimeSpan.FromMinutes(30));
        var survivorExpiry = time.Now + TimeSpan.FromMinutes(30);
        for (var i = 0; i < consumedCount; i++)
            await store.SaveAsync(correlationId, Readable(correlationId), TimeSpan.FromMinutes(5));
        redis.ResetCounters();

        await dispatcher.DispatchLostResponses(store, correlationId, new OperationResult { Status = OperationStatus.Completed }, "r49", CancellationToken.None);

        Assert.Equal(consumedCount, spy.Resumed);
        Assert.Equal(1, redis.Transactions);
        Assert.Equal(1, redis.Sets);
        Assert.Equal(0, redis.KeyDeletes);
        Assert.True(redis.Gets <= 2, $"{redis.Gets} reads for one fan-out");
        Assert.Equal(redis.Value.ToString().Length, redis.WrittenChars);

        using var written = JsonDocument.Parse(redis.Value.ToString());
        var entry = Assert.Single(written.RootElement.GetProperty("Registrations").EnumerateArray());
        Assert.Equal(survivor.RegistrationId, entry.GetProperty("State").GetProperty("RegistrationId").GetGuid());
        Assert.Equal(survivorExpiry, entry.GetProperty("ExpiresAtUtc").GetDateTimeOffset());
    }

    /// <summary>The NATS KV envelope, for the lost-RESPONSE and the lost-EXCEPTION fan-out alike.</summary>
    [Theory]
    [InlineData(16, false)]
    [InlineData(63, false)]
    [InlineData(63, true)]
    public async Task NatsFanOut_ConsumedRegistrations_AreRemovedInOneRewrite_WhateverTheirNumber(int consumedCount, bool exceptionRoute)
    {
        var spy = new R49Spy();
        var (provider, dispatcher) = CreateDispatcher(spy);
        await using var _ = provider;
        var kv = new FakeNatsKvStore();
        var store = new NatsRecoveryStateStore(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance, new TestTimeProvider());
        const string correlationId = "r49-nats-fan-out";

        // On the exception route the survivor is a registration with no failure callback.
        var survivor = Survivor(correlationId);
        if (exceptionRoute)
            survivor.FailureCallback = null;
        await store.SaveAsync(correlationId, survivor, TimeSpan.FromMinutes(30));
        for (var i = 0; i < consumedCount; i++)
            await store.SaveAsync(correlationId, Readable(correlationId), TimeSpan.FromMinutes(5));
        kv.PutCount = 0;
        kv.DeleteCount = 0;
        kv.WrittenChars = 0;
        kv.ResetGetStatistics();

        if (exceptionRoute)
            await dispatcher.DispatchLostExceptions(store, correlationId, new TimeoutException("remote gave up"), "r49", CancellationToken.None);
        else
            await dispatcher.DispatchLostResponses(store, correlationId, new OperationResult { Status = OperationStatus.Completed }, "r49", CancellationToken.None);

        Assert.Equal(consumedCount, exceptionRoute ? spy.Failed : spy.Resumed);
        Assert.Equal(1, kv.PutCount);
        Assert.Equal(0, kv.DeleteCount);
        Assert.True(kv.GetCount <= 2, $"{kv.GetCount} reads for one fan-out");
        var key = NatsSubjectSchema.RecoveryKey(correlationId);
        Assert.Equal(kv.Entries[key].Length, kv.WrittenChars);
        Assert.Equal(survivor.RegistrationId, Assert.Single(StoredNatsRegistrations(kv, correlationId)).RegistrationId);
    }

    /// <summary>
    /// Pin: batching the deletes does not change settlement. A transient sibling failure still
    /// propagates for redelivery with the consumed registrations ALREADY removed, so the
    /// redelivery reaches the failed registration alone.
    /// </summary>
    [Fact]
    public async Task NatsFanOut_TransientSiblingFailure_RemovesTheConsumedBeforeItPropagates()
    {
        var spy = new R49Spy();
        var (provider, dispatcher) = CreateDispatcher(spy, services => services.AddSingleton<IR49Boom, R49Boom>());
        await using var _ = provider;
        var kv = new FakeNatsKvStore();
        var store = new NatsRecoveryStateStore(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance, new TestTimeProvider());
        const string correlationId = "r49-nats-partial";
        var boom = Readable(correlationId);
        boom.ResumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IR49Boom).FullName!,
            MethodName = nameof(IR49Boom.Resume),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
        };
        boom.FailureCallback = null;
        await store.SaveAsync(correlationId, Readable(correlationId), TimeSpan.FromMinutes(5));
        await store.SaveAsync(correlationId, boom, TimeSpan.FromMinutes(5));
        await store.SaveAsync(correlationId, Readable(correlationId), TimeSpan.FromMinutes(5));

        await Assert.ThrowsAsync<RecoveryCallbackFailedException>(() => dispatcher.DispatchLostResponses(
            store, correlationId, new OperationResult { Status = OperationStatus.Completed }, "r49", CancellationToken.None));

        Assert.Equal(2, spy.Resumed);
        Assert.Equal(boom.RegistrationId, Assert.Single(await store.GetAllAsync(correlationId)).RegistrationId);
    }

    /// <summary>
    /// Pin: a fan-out cancelled part-way still removes what it consumed. The removal runs as the
    /// dispatch unwinds, so a registration whose callback already ran is not invoked again by the
    /// redelivery; the one that was interrupted stays armed.
    /// </summary>
    [Fact]
    public async Task NatsFanOut_CancelledPartWay_StillRemovesTheRegistrationsItConsumed()
    {
        var spy = new R49Spy();
        using var cancellation = new CancellationTokenSource();
        var (provider, dispatcher) = CreateDispatcher(spy, services => services.AddSingleton<IR49Boom>(new R49Canceller(cancellation)));
        await using var _ = provider;
        var kv = new FakeNatsKvStore();
        var store = new NatsRecoveryStateStore(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance, new TestTimeProvider());
        const string correlationId = "r49-nats-cancelled";
        var consumed = Readable(correlationId);
        var interrupted = Readable(correlationId);
        interrupted.ResumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IR49Boom).FullName!,
            MethodName = nameof(IR49Boom.Resume),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
        };
        interrupted.FailureCallback = null;
        await store.SaveAsync(correlationId, consumed, TimeSpan.FromMinutes(5));
        await store.SaveAsync(correlationId, interrupted, TimeSpan.FromMinutes(5));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => dispatcher.DispatchLostResponses(
            store, correlationId, new OperationResult { Status = OperationStatus.Completed }, "r49", cancellation.Token));

        Assert.Equal(1, spy.Resumed);
        Assert.Equal(interrupted.RegistrationId, Assert.Single(await store.GetAllAsync(correlationId)).RegistrationId);
    }

    private sealed class R49Canceller(CancellationTokenSource cancellation) : IR49Boom
    {
        public Task Resume(OperationResult payload)
        {
            cancellation.Cancel();
            throw new OperationCanceledException(cancellation.Token);
        }
    }

    public interface IR49Boom
    {
        Task Resume(OperationResult payload);
    }

    private sealed class R49Boom : IR49Boom
    {
        public Task Resume(OperationResult payload) => throw new TimeoutException("dependency briefly down");
    }

    // ---------------------------------------------------------------------------------------------
    // Shared helpers.

    private static async Task SeedNatsAsync(FakeNatsKvStore kv, NatsRecoveryStateStore store, string correlationId, params RecoveryState[] states)
    {
        // Saved at the current schema (SaveAsync admits nothing else), then rewritten in place with
        // the versions the test wants — what a newer build's save would have left behind.
        var versions = states.Select(state => state.SchemaVersion).ToArray();
        foreach (var state in states)
        {
            state.SchemaVersion = RecoveryStateSchema.Current;
            await store.SaveAsync(correlationId, state, TimeSpan.FromMinutes(5));
        }

        var key = NatsSubjectSchema.RecoveryKey(correlationId);
        var envelope = JsonSerializer.Deserialize<NatsRecoveryStateStore.StoredRecoveryState>(kv.Entries[key])!;
        for (var i = 0; i < envelope.States!.Count; i++)
            envelope.States[i].SchemaVersion = versions[i];
        kv.Entries[key] = JsonSerializer.Serialize(envelope);
        for (var i = 0; i < states.Length; i++)
            states[i].SchemaVersion = versions[i];
    }

    private static void RewriteNatsSchemaVersions(FakeNatsKvStore kv, string correlationId, int schemaVersion)
    {
        var key = NatsSubjectSchema.RecoveryKey(correlationId);
        var envelope = JsonSerializer.Deserialize<NatsRecoveryStateStore.StoredRecoveryState>(kv.Entries[key])!;
        foreach (var state in envelope.States!)
            state.SchemaVersion = schemaVersion;
        kv.Entries[key] = JsonSerializer.Serialize(envelope);
    }

    private static List<RecoveryState> StoredNatsRegistrations(FakeNatsKvStore kv, string correlationId)
        => JsonSerializer.Deserialize<NatsRecoveryStateStore.StoredRecoveryState>(kv.Entries[NatsSubjectSchema.RecoveryKey(correlationId)])!.States!;
}

/// <summary>
/// A Redis database with state behind the recovery store's reads and transactions: GET returns the
/// current value, and a transaction commits only while the value is still the one it was created
/// over (the store creates it straight after its read, so that IS the read the condition names) —
/// then applies its queued SET or DEL. <see cref="BeforeExecute"/> lets a test slip a concurrent
/// write in between. Counts every command for the scaling pins.
/// </summary>
internal sealed class StatefulRedis
{
    private readonly Mock<IConnectionMultiplexer> _multiplexer = new();
    private readonly Mock<IDatabase> _database = new();

    public RedisValue Value = RedisValue.Null;
    public int Gets, Transactions, Sets, KeyDeletes, Conflicts;
    public long WrittenChars;

    /// <summary>Runs once, inside the next EXEC and before its condition is checked.</summary>
    public Action? BeforeExecute;

    /// <summary>Every EXEC finds its condition broken, as under a writer that never pauses.</summary>
    public bool ConflictEveryExecute;

    public StatefulRedis()
    {
        _multiplexer
            .Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>()))
            .Returns(_database.Object);
        _database
            .Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(() =>
            {
                Gets++;
                return Value;
            });
        _database
            .Setup(d => d.CreateTransaction(It.IsAny<object?>()))
            .Returns(CreateTransaction);
    }

    public RedisRecoveryStateStore CreateStore(TimeProvider time, Action<RedisAsyncResponseOptions>? configure = null)
    {
        var options = new RedisAsyncResponseOptions { KeyPrefix = "ar" };
        configure?.Invoke(options);
        return new(_multiplexer.Object, Options.Create(options), NullLogger<RedisRecoveryStateStore>.Instance, time);
    }

    public void ResetCounters()
    {
        Gets = Transactions = Sets = KeyDeletes = Conflicts = 0;
        WrittenChars = 0;
    }

    private ITransaction CreateTransaction()
    {
        var readAtCreation = Value;
        var transaction = new Mock<ITransaction>();
        transaction
            .Setup(t => t.ExecuteAsync(It.IsAny<CommandFlags>()))
            .ReturnsAsync(() =>
            {
                if (BeforeExecute is { } concurrentWrite)
                {
                    BeforeExecute = null;
                    concurrentWrite();
                }

                if (ConflictEveryExecute || Value != readAtCreation)
                {
                    Conflicts++;
                    return false;
                }

                Transactions++;
                foreach (var invocation in transaction.Invocations)
                {
                    if (invocation.Method.Name == nameof(IDatabase.StringSetAsync))
                    {
                        Value = (RedisValue)invocation.Arguments[1]!;
                        Sets++;
                        WrittenChars += Value.ToString().Length;
                    }
                    else if (invocation.Method.Name == nameof(IDatabase.KeyDeleteAsync))
                    {
                        Value = RedisValue.Null;
                        KeyDeletes++;
                    }
                }

                return true;
            });
        return transaction.Object;
    }
}
