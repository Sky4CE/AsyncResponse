using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Testing;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// Round-26 review regressions outside the DB channel base (whose facts live beside their existing
// harnesses in DbChannelSharedCoverageTests / MongoDbChannelCoverageTests): the portable-id
// contracts, watchdog option validation, the retry predicate's exception filter, the executor
// registry's clock and drain budgets, derived relational names, and the ancestor ledger refresh.
// ---------------------------------------------------------------------------------------------
public sealed class Round26RegressionTests
{
    // -----------------------------------------------------------------------------------------
    // Finding 2 — the correlation-id contract dropped the control-character rule the flow-id
    // contract enforces, though PortableText documents both as one shared contract.
    // -----------------------------------------------------------------------------------------

    [Theory]
    [InlineData('\0')] // PostgreSQL rejects NUL in a text column (22021); SQL Server stores it.
    [InlineData('\n')]
    [InlineData('\u001f')]
    public void CorrelationIdWithAControlCharacter_IsRejectedByThePortableContract(char control)
    {
        var correlationId = $"order{control}42";

        var rejection = AsyncResponseChannelOptions.CorrelationIdNotPortable(correlationId);

        Assert.NotNull(rejection);
        Assert.Contains("control character", rejection, StringComparison.Ordinal);
        // The guard is the door every public wait/publish walks through, so the contract has to
        // bite there too and not only in the pure predicate.
        var thrown = Assert.Throws<ArgumentException>(() => CorrelationIdGuard.ThrowIfUnusable(correlationId));
        Assert.Contains("control character", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FlowIdWithAControlCharacter_StaysRejected_SoBothContractsAgree()
    {
        // The parity half of the same finding: whatever the correlation-id guard now rejects, the
        // flow-id guard must still reject, or "checked in one place" drifts again.
        Assert.NotNull(FlowStateConcurrency.FlowIdNotPortable("flow\0id"));
        Assert.NotNull(AsyncResponseChannelOptions.CorrelationIdNotPortable("flow\0id"));
    }

    [Fact]
    public void CosmosOnlyFlowIdRules_AreDeliberatelyNotAppliedToCorrelationIds()
    {
        // Pins the SCOPE of the fix as much as the fix: the UTF-8 byte cap and the '/ \ ? #' set
        // exist for Cosmos DB item ids, and no bundled channel persists a correlation id to
        // Cosmos. Mirroring them here would reject ids every shipped channel handles correctly.
        Assert.Null(AsyncResponseChannelOptions.CorrelationIdNotPortable("tenant/acme#42?x"));
        Assert.NotNull(FlowStateConcurrency.FlowIdNotPortable("tenant/acme#42?x"));
    }

    // -----------------------------------------------------------------------------------------
    // Finding 10 — both guards indexed [0]/[^1] with no length check, so an empty id threw
    // IndexOutOfRangeException out of the method whose contract is to EXPLAIN bad ids.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public void EmptyCorrelationId_ReturnsARejectionInsteadOfThrowingIndexOutOfRange()
    {
        var rejection = AsyncResponseChannelOptions.CorrelationIdNotPortable(string.Empty);

        Assert.NotNull(rejection);
        Assert.Contains("empty", rejection, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void EmptyFlowId_ReturnsARejectionInsteadOfThrowingIndexOutOfRange()
    {
        var rejection = FlowStateConcurrency.FlowIdNotPortable(string.Empty);

        Assert.NotNull(rejection);
        Assert.Contains("empty", rejection, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task TryCreateAsync_WithAnEmptyFlowId_ThrowsArgumentException_NotIndexOutOfRange()
    {
        // TryCreateAsync is documented as the single door every create walks through and does no
        // whitespace pre-check of its own, so it is the reachable face of the guard's hole.
        var store = new InMemoryFlowStateStore();

        var thrown = await Assert.ThrowsAsync<ArgumentException>(() => FlowStateConcurrency.TryCreateAsync(
            store,
            string.Empty,
            new FlowState { FlowId = string.Empty },
            TimeSpan.FromMinutes(1)));

        Assert.Equal("flowId", thrown.ParamName);
    }

    // -----------------------------------------------------------------------------------------
    // Finding 4 — StaleAfter and MaxScanEntries were unvalidated, so a bad value silently
    // disabled the stuck-flow alarm instead of failing startup.
    // -----------------------------------------------------------------------------------------

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void WatchdogMaxScanEntriesBelowOne_FailsAtStartup(int maxScanEntries)
    {
        // Pre-fix this started cleanly and then truncated on the FIRST entry of every scan:
        // Evaluate ran over an empty set, so no stale registration was ever reported while the
        // logs still showed a scan completing each interval.
        var options = new AsyncResponseWatchdogOptions { MaxScanEntries = maxScanEntries };

        var thrown = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(AsyncResponseWatchdogOptions.MaxScanEntries), thrown.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void WatchdogStaleAfterAtOrBelowZero_FailsAtStartup(int staleAfterHours)
    {
        // The mirror failure: "utcNow - registeredAtUtc >= StaleAfter" holds for every entry, so
        // every healthy waiter is logged stale at Warning on every scan.
        var options = new AsyncResponseWatchdogOptions { StaleAfter = TimeSpan.FromHours(staleAfterHours) };

        var thrown = Assert.Throws<InvalidOperationException>(options.Validate);
        Assert.Contains(nameof(AsyncResponseWatchdogOptions.StaleAfter), thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WatchdogDefaults_StillValidate()
    {
        // Guards the guard: the new bounds must not reject the shipped defaults.
        new AsyncResponseWatchdogOptions().Validate();
    }

    // -----------------------------------------------------------------------------------------
    // Finding 5 — isTransient ran inside a `when` filter, where the CLR swallows a throwing
    // predicate and reads the filter as false.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public async Task AThrowingIsTransientPredicate_SurfacesInsteadOfSilentlyDisablingRetries()
    {
        // Pre-fix the NullReferenceException below vanished (filters that throw evaluate as
        // false), the original fault was rethrown on attempt 1, and the predicate bug appeared in
        // no log and no telemetry — the retry budget was silently gone.
        var attempts = 0;
        var original = new TimeoutException("store blipped");

        var thrown = await Assert.ThrowsAsync<AggregateException>(() => AsyncResponseRetry.ExecuteAsync<bool>(
            _ =>
            {
                attempts++;
                throw original;
            },
            isTransient: _ => throw new NullReferenceException("predicate bug"),
            maxAttempts: 4,
            baseDelay: TimeSpan.FromMilliseconds(1),
            maxDelay: TimeSpan.FromMilliseconds(2),
            CancellationToken.None));

        Assert.Equal(1, attempts);
        // BOTH failures ride along: the predicate bug to fix, and the fault it was judging.
        Assert.Contains(thrown.InnerExceptions, ex => ReferenceEquals(ex, original));
        Assert.Contains(thrown.InnerExceptions, ex => ex is NullReferenceException);
    }

    [Fact]
    public async Task AWellBehavedIsTransientPredicate_StillDrivesTheRetryLadder()
    {
        // The predicate still decides; moving it out of the filter must not change that.
        var attempts = 0;

        var result = await AsyncResponseRetry.ExecuteAsync(
            _ =>
            {
                if (++attempts < 3)
                    throw new TimeoutException("blip");
                return Task.FromResult(attempts);
            },
            isTransient: static ex => ex is TimeoutException,
            maxAttempts: 4,
            baseDelay: TimeSpan.FromMilliseconds(1),
            maxDelay: TimeSpan.FromMilliseconds(2),
            CancellationToken.None);

        Assert.Equal(3, result);
    }

    [Fact]
    public async Task ANonTransientFault_StillRethrowsTheOriginalUnwrapped()
    {
        // The predicate returning false must keep propagating the ORIGINAL exception, not the
        // aggregate the throwing-predicate path now raises.
        await Assert.ThrowsAsync<InvalidOperationException>(() => AsyncResponseRetry.ExecuteAsync<bool>(
            _ => throw new InvalidOperationException("permanent"),
            isTransient: static _ => false,
            maxAttempts: 4,
            baseDelay: TimeSpan.FromMilliseconds(1),
            maxDelay: TimeSpan.FromMilliseconds(2),
            CancellationToken.None));
    }

    // -----------------------------------------------------------------------------------------
    // Finding 11 — the failure-callback retry treated EVERY fault as transient, so a permanently
    // mis-wired callback burned the whole backoff ladder on the publish path for every message.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public async Task AFailureCallbackWhoseTargetIsNotRegistered_FailsFastWithoutBurningTheRetryLadder()
    {
        // Pre-fix isTransient was `_ => true`, so a callback whose service is missing from DI —
        // deterministic on every attempt — still ran 4 invocations and 3 backoff delays. Those
        // delays are armed on the injected clock, so on a virtual clock nobody advances the
        // publish never completes at all: the assertion below times out pre-fix and returns
        // promptly post-fix.
        var time = new VirtualTimeProvider();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<TimeProvider>(time);
        services.AddAsyncResponse().WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();

        var recoveryStateStore = provider.GetRequiredService<IRecoveryStateStore>();
        await recoveryStateStore.SaveAsync(
            "unwired-callback",
            new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = "unwired-callback",
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                FailureCallback = new ReflectionCallDto
                {
                    // Never registered: InvokeAsync fails while WIRING UP, before any user code.
                    ServiceInterfaceFullName = "AsyncResponse.Tests.INeverRegisteredFailureTarget",
                    MethodName = "OnFailure",
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
                }
            },
            TimeSpan.FromMinutes(5));

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        await publisher
            .SetResponse(new OperationResult { Status = OperationStatus.Failed, Message = "remote step failed" }, "unwired-callback")
            .WaitAsync(TimeSpan.FromSeconds(5));

        // No backoff timer was ever armed — the ladder was skipped, not merely fast.
        Assert.Null(time.NextTimerDueAt);
    }

    // -----------------------------------------------------------------------------------------
    // Findings 7 and 16 — the executor registry's tombstone clock and its second drain budget.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public async Task TombstoneExpiry_RunsOnTheInjectedTimeProvider()
    {
        // Pre-fix the tombstone deadline was stamped and compared against DateTime.UtcNow, so a
        // virtual clock could not advance past TombstoneLifetime and the drop-a-delivery branch
        // was unreachable without a 30-second wall-clock sleep.
        var time = new VirtualTimeProvider();
        var registry = new SerialExecutorRegistry(NullLogger.Instance, timeProvider: time);

        // An executor only exists once something is actually enqueued, and only an existing
        // executor can be retired — which is what lays the tombstone.
        registry.OnSubscriptionRegistered("ch");
        Assert.True(await registry.EnqueueAsync("ch", () => Task.CompletedTask));
        registry.OnSubscriptionRetired("ch");
        await registry.RemoveAsync("ch");

        // Inside the tombstone window with no registration left: the delivery is suppressed.
        Assert.False(await registry.EnqueueAsync("ch", () => Task.CompletedTask));

        time.Advance(SerialExecutorRegistry.TombstoneLifetime + TimeSpan.FromSeconds(1));

        // Past it, the executor is legitimately recreated — only reachable if expiry reads the
        // injected clock.
        var ran = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Assert.True(await registry.EnqueueAsync("ch", () =>
        {
            ran.TrySetResult();
            return Task.CompletedTask;
        }));
        await ran.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void BothDrainBudgets_AreConstructorOverridable()
    {
        // Pre-fix only disposeDrainLimit was overridable, so a caller that shortened it still paid
        // the hard-coded 30s enqueue drain and the worst case became "my value + 30s".
        var registry = new SerialExecutorRegistry(
            NullLogger.Instance,
            disposeDrainLimit: TimeSpan.FromMilliseconds(40),
            enqueueDrainLimit: TimeSpan.FromMilliseconds(20));

        Assert.Equal(TimeSpan.FromMilliseconds(40), PrivateField<TimeSpan>(registry, "_disposeDrainLimit"));
        Assert.Equal(TimeSpan.FromMilliseconds(20), PrivateField<TimeSpan>(registry, "_enqueueDrainLimit"));
    }

    [Fact]
    public void DrainBudgets_DefaultToTheDocumentedThirtySeconds()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);

        Assert.Equal(SerialExecutorRegistry.DisposeDrainLimit, PrivateField<TimeSpan>(registry, "_disposeDrainLimit"));
        Assert.Equal(SerialExecutorRegistry.EnqueueDrainLimit, PrivateField<TimeSpan>(registry, "_enqueueDrainLimit"));
    }

    // -----------------------------------------------------------------------------------------
    // Findings 9 and 12 — the derived-name rule (one implementation per package family) must
    // reject a suffix that leaves no stem instead of slicing a negative length.
    // -----------------------------------------------------------------------------------------

    public static TheoryData<string, Func<string, string, string>> DerivedNameSites => new()
    {
        { "PostgreSQL transport", (table, suffix) => PostgreSqlTransportStore.IndexName(table, suffix) },
        { "SQL Server transport", (table, suffix) => SqlServerTransportStore.IndexName(table, suffix) },
    };

    [Theory]
    [MemberData(nameof(DerivedNameSites))]
    public void ADerivedNameSuffixThatLeavesNoStem_FailsWithAnActionableMessage(string site, Func<string, string, string> indexName)
    {
        // Pre-fix every site computed table[..(cap - tail.Length)] unguarded, so an oversized
        // suffix failed schema creation with a bare index-out-of-range naming neither the suffix
        // nor the cap. The type is unchanged; the DIAGNOSIS is the fix.
        var thrown = Assert.Throws<ArgumentOutOfRangeException>(() => indexName("queue", new string('x', 200)));

        Assert.Equal("suffix", thrown.ParamName);
        Assert.Contains("no room for a stem", thrown.Message, StringComparison.Ordinal);
        Assert.Contains("Shorten the suffix", thrown.Message, StringComparison.Ordinal);
        Assert.False(string.IsNullOrWhiteSpace(site));
    }

    [Theory]
    [MemberData(nameof(DerivedNameSites))]
    public void DerivedNames_StillReserveSuffixSpaceByTruncatingTheStem(string site, Func<string, string, string> indexName)
    {
        // The behaviour the consolidation must preserve: a maximum-length table can never derive
        // its own name back, or CREATE ... IF NOT EXISTS silently creates nothing.
        var derived = indexName(new string('t', 200), "claim");

        Assert.EndsWith("_claim_idx", derived, StringComparison.Ordinal);
        Assert.NotEqual(new string('t', 200), derived);
        Assert.False(string.IsNullOrWhiteSpace(site));
    }

    [Theory]
    [MemberData(nameof(FlowStoreAssemblies))]
    public void TheFlowStoreCopyOfDerivedName_CarriesTheSameGuard(Type providerOptionsType)
    {
        // DurableFlowStoreShared is source-linked into all nine flow stores while
        // RelationalNamePlan is linked into four channel/transport packages, so the rule
        // legitimately has two homes — and both must carry the guard or they drift again.
        var shared = providerOptionsType.Assembly.GetType("AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared", throwOnError: true)!;
        var derivedName = shared.GetMethod("DerivedName", BindingFlags.Public | BindingFlags.Static)!;

        var thrown = Assert.IsType<ArgumentOutOfRangeException>(
            Assert.Throws<TargetInvocationException>(
                () => derivedName.Invoke(null, ["flows", new string('x', 200), 63])).InnerException);

        Assert.Equal("suffix", thrown.ParamName);
        Assert.Contains("no room for a", thrown.Message, StringComparison.Ordinal);

        // …and the ordinary path is untouched.
        Assert.Equal("flows_expires_idx", derivedName.Invoke(null, ["flows", "_expires_idx", 63]));
    }

    /// <summary>
    /// All nine stores: DurableFlowStoreShared is source-linked, so the guard compiles separately
    /// into each assembly and covering one leaves the same lines unguarded in the other eight.
    /// </summary>
    public static TheoryData<Type> FlowStoreAssemblies =>
    [
        typeof(DurableFlows.Cosmos.CosmosDurableFlowOptions),
        typeof(DurableFlows.DynamoDB.DynamoDbDurableFlowOptions),
        typeof(DurableFlows.EFCore.EFCoreDurableFlowOptions),
        typeof(DurableFlows.MongoDB.MongoDbDurableFlowOptions),
        typeof(DurableFlows.MySql.MySqlDurableFlowOptions),
        typeof(DurableFlows.Oracle.OracleDurableFlowOptions),
        typeof(DurableFlows.PostgreSQL.PostgreSqlDurableFlowOptions),
        typeof(DurableFlows.Sqlite.SqliteDurableFlowOptions),
        typeof(DurableFlows.SqlServer.SqlServerDurableFlowOptions)
    ];

    // -----------------------------------------------------------------------------------------
    // Finding 18 — a case-only name collision reported one spelling, so the error read as a false
    // positive against the operator's own configuration.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public void ACaseOnlyNamePlanCollision_NamesBothSpellings_PostgreSql()
    {
        var options = new PostgreSqlAsyncResponseChannelOptions
        {
            RecoveryStateTable = "Messages",
            MessageTable = "messages"
        };

        var thrown = Assert.Throws<InvalidOperationException>(() => PostgreSqlChannelSql.ValidateNamePlan(options));

        // Pre-fix only one spelling appeared, so an operator reading their own 'Messages' config
        // saw an error about 'messages' and had nothing pointing at the case-fold rule.
        Assert.Contains("'Messages'", thrown.Message, StringComparison.Ordinal);
        Assert.Contains("'messages'", thrown.Message, StringComparison.Ordinal);
        Assert.Contains("case-insensitively", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ACaseOnlyNamePlanCollision_NamesBothSpellings_SqlServer()
    {
        var options = new SqlServerAsyncResponseChannelOptions
        {
            RecoveryStateTable = "Messages",
            MessageTable = "messages"
        };

        var thrown = Assert.Throws<InvalidOperationException>(() => SqlServerChannelSql.ValidateNamePlan(options));

        Assert.Contains("'Messages'", thrown.Message, StringComparison.Ordinal);
        Assert.Contains("'messages'", thrown.Message, StringComparison.Ordinal);
        Assert.Contains("case-insensitively", thrown.Message, StringComparison.Ordinal);
    }

    // -----------------------------------------------------------------------------------------
    // Finding 3 — the ancestor TTL refresh went through MutateAsync, whose eight-attempt retry
    // fought a live ancestor's checkpoints for a write whose only purpose was a TTL stamp.
    // -----------------------------------------------------------------------------------------

    [Fact]
    public async Task AncestorLedgerRefresh_AgainstAConcurrentWriter_RetriesBoundedly_ThenAbandonsThePark()
    {
        // Pre-round-26 MutateAsync retried the compare-and-swap eight times. Every round that WON
        // advanced the ancestor's revision, so a parent that was genuinely executing lost its next
        // checkpoint's CAS, called MarkLost() and abandoned its delivery for redelivery —
        // re-running everything since its last checkpoint. Round 26 made a lost CAS mean "the
        // ancestor is alive and re-stamping its own expiry" and stopped after one attempt; round
        // 38 found the competing write stamps the plain StateExpiry (it knows nothing about the
        // park), so ceding the race expired the parent under the child's wait. The extension now
        // re-reads and retries a small, fixed number of times — a re-read that already carries a
        // floor reaching the park ends it without a write — and a walk that loses every attempt
        // abandons the park (nothing published; the delivery retries) instead of either fighting
        // without end or parking on unproven retention.
        var store = new CasRefusingFlowStateStore("parent");
        var context = CreateContextForAncestorWalk(store, childFlowId: "child", parentFlowId: "parent");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeExtendAncestorLedgersAsync(context, TimeSpan.FromDays(30)));

        Assert.Equal(DurableFlowContext.MaxAncestorExtensionAttempts, store.UpdateAttempts);
        Assert.Contains("abandoned", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AncestorLedgerRefresh_StillExtendsARunningAncestor()
    {
        // The behaviour the single-shot rewrite must keep: a parked ancestor's row is refreshed to
        // cover the descendant's park, and the walk continues up the chain.
        var store = new RecordingFlowStateStore();
        store.Seed("parent", new FlowState { FlowId = "parent", Status = FlowRunStatus.Running, ParentFlowId = "grandparent" });
        store.Seed("grandparent", new FlowState { FlowId = "grandparent", Status = FlowRunStatus.Running });
        var context = CreateContextForAncestorWalk(store, childFlowId: "child", parentFlowId: "parent");

        await InvokeExtendAncestorLedgersAsync(context, TimeSpan.FromDays(30));

        Assert.Equal(TimeSpan.FromDays(30), store.LastTtl["parent"]);
        Assert.Equal(TimeSpan.FromDays(30), store.LastTtl["grandparent"]);
    }

    [Fact]
    public async Task AncestorLedgerRefresh_StopsAtATerminalAncestor()
    {
        // A terminal ancestor is not waiting on this chain, so it is neither stamped nor walked past.
        var store = new RecordingFlowStateStore();
        store.Seed("parent", new FlowState { FlowId = "parent", Status = FlowRunStatus.Succeeded, ParentFlowId = "grandparent" });
        store.Seed("grandparent", new FlowState { FlowId = "grandparent", Status = FlowRunStatus.Running });
        var context = CreateContextForAncestorWalk(store, childFlowId: "child", parentFlowId: "parent");

        await InvokeExtendAncestorLedgersAsync(context, TimeSpan.FromDays(30));

        Assert.Empty(store.LastTtl);
    }

    // -----------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------

    private static T PrivateField<T>(object target, string name)
        => (T)target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(target)!;

    private static Task InvokeExtendAncestorLedgersAsync(DurableFlowContext context, TimeSpan ttl)
        => (Task)typeof(DurableFlowContext)
            .GetMethod("ExtendAncestorLedgersAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(context, [ttl, CancellationToken.None])!;

    /// <summary>
    /// A context wired with only what the ancestor walk touches (state, store, options, clock and
    /// a lease); the builder/subscriber seams stay null because that path never reaches them.
    /// </summary>
    private static DurableFlowContext CreateContextForAncestorWalk(IFlowStateStore store, string childFlowId, string parentFlowId)
    {
        var options = new DurableFlowOptions();
        var state = new FlowState
        {
            FlowId = childFlowId,
            Status = FlowRunStatus.Running,
            ParentFlowId = parentFlowId
        };

        var lease = new FlowExecutionLease(store, childFlowId, "lease", options, NullLogger.Instance, TimeProvider.System);
        return new DurableFlowContext(
            state,
            store,
            builder: null!,
            propagation: new AsyncResponseContextPropagation([]),
            options,
            subscriber: null!,
            recoverableSubscriber: null,
            NullLogger.Instance,
            lease,
            TimeProvider.System);
    }

    /// <summary>Loads the ancestor fine but never lets the compare-and-swap land, as a live ancestor's own checkpoints would.</summary>
    private sealed class CasRefusingFlowStateStore(string ancestorId) : IFlowStateStore
    {
        public int UpdateAttempts;

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult<FlowState?>(flowId == ancestorId
                ? new FlowState { FlowId = ancestorId, Status = FlowRunStatus.Running }
                : null);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref UpdateAttempts);
            return Task.FromResult(false);
        }

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default) => Task.FromResult(true);
        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default) => Task.FromResult(true);
        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default) => Task.FromResult(true);
        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default) => Task.FromResult(true);
    }

    /// <summary>Accepts writes and records the TTL each ancestor was stamped with.</summary>
    private sealed class RecordingFlowStateStore : IFlowStateStore
    {
        private readonly Dictionary<string, FlowState> _states = new(StringComparer.Ordinal);

        public Dictionary<string, TimeSpan> LastTtl { get; } = new(StringComparer.Ordinal);

        public void Seed(string flowId, FlowState state) => _states[flowId] = state;

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult(_states.TryGetValue(flowId, out var state) ? state : null);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId, CancellationToken cancellationToken = default)
        {
            LastTtl[flowId] = ttl;
            _states[flowId] = state;
            return Task.FromResult(true);
        }

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default) => Task.FromResult(true);
        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default) => Task.FromResult(true);
        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default) => Task.FromResult(true);
        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default) => Task.FromResult(true);
    }
}
