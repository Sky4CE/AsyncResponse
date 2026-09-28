using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using AsyncResponse.Channels.NATS;
using AsyncResponse.DurableFlows.DynamoDB;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.KeyValueStore;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 50 (external review of 8e7420f4): recovery reads served by a lagging NATS replica
/// (F1), a DynamoDB ledger refused before its growth warning could fire (F2), and operation spans
/// whose raw disposal let a throwing telemetry listener fail work that had already happened (F3).
/// </summary>
public sealed class Round50RegressionTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    // ---------------------------------------------------------------------------------------------
    // F1 — NATS.Net creates every KV stream with AllowDirect and reads it with Direct Get, which
    //      any replica of a replicated bucket may answer. A follower that had not applied an
    //      acknowledged registration answered "not found": the dispatcher ran no callback and the
    //      transport acknowledged the response while the registration stayed armed.

    private const string Bucket = "asyncresponse-recovery";

    /// <summary>
    /// A replicated recovery bucket whose replica lags: the client's Direct Get (what the adapter
    /// used to call) answers "not found" for <paramref name="key"/>, while the stream's leader has
    /// the entry. Returns the mocks so a test can script the leader.
    /// </summary>
    private static (NatsKvStoreAdapter Adapter, Mock<INatsKVStore> Store, NatsKvLeaderStream Leader) LaggingReplicaAdapter(string key)
    {
        var context = new Mock<INatsKVContext>();
        var store = new Mock<INatsKVStore>();
        context.Setup(c => c.GetStoreAsync(Bucket, It.IsAny<CancellationToken>())).ReturnsAsync(store.Object);
        store.Setup(s => s.GetEntryAsync<string>(key, It.IsAny<ulong>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsKVKeyNotFoundException());
        store.Setup(s => s.TryGetEntryAsync<string>(key, It.IsAny<ulong>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsResult<NatsKVEntry<string>>(new NatsKVKeyNotFoundException()));
        var leader = new NatsKvLeaderStream().Attach(context);
        return (new NatsKvStoreAdapter(context.Object, new NatsAsyncResponseChannelOptions()), store, leader);
    }

    /// <summary>Pre-fix failure: the lagging replica's "not found" came back as absence (<c>null</c>).</summary>
    [Fact]
    public async Task NatsKv_GetAsync_ReadsTheStreamLeader_NotADirectGetAReplicaMayAnswer()
    {
        var (adapter, store, leader) = LaggingReplicaAdapter("r50-key");
        leader.Value("r50-key", "registration", revision: 4);

        var entry = await adapter.GetAsync("r50-key", CancellationToken.None);

        Assert.NotNull(entry);
        Assert.Equal(("registration", 4UL), (entry.Value.Value, entry.Value.Revision));
        store.Verify(s => s.GetEntryAsync<string>(It.IsAny<string>(), It.IsAny<ulong>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()), Times.Never);
        store.Verify(s => s.TryGetEntryAsync<string>(It.IsAny<string>(), It.IsAny<ulong>(), It.IsAny<INatsDeserialize<string>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    /// <summary>
    /// A create whose revision-0 write conflicts reads the key back to tell a live value from the
    /// delete marker it may write over. Pre-fix failure: a lagging replica's "not found" read as a
    /// purged key — a conflict — and the save retried blind instead of writing over the marker.
    /// </summary>
    [Fact]
    public async Task NatsKv_TryCreate_JudgesItsConflictOnTheLeader()
    {
        var (adapter, store, leader) = LaggingReplicaAdapter("r50-reused");
        store.Setup(s => s.TryUpdateAsync("r50-reused", "v", 0UL, It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsResult<ulong>(new NatsKVWrongLastRevisionException(new ApiError { Code = 400, ErrCode = 10071, Description = "wrong last sequence: 9" })));
        store.Setup(s => s.TryUpdateAsync("r50-reused", "v", 9UL, It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new NatsResult<ulong>(10UL));
        leader.Marker("r50-reused", revision: 9, "KV-Operation: DEL");

        Assert.True(await adapter.TryCreateAsync("r50-reused", "v", CancellationToken.None));
        store.Verify(s => s.TryUpdateAsync("r50-reused", "v", 9UL, It.IsAny<INatsSerialize<string>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    /// <summary>
    /// The review's reproduction, end to end through the recovery store and the dispatcher: the
    /// waiter is gone, a terminal response arrives, and the replica that answers the read has not
    /// applied the registration yet. Pre-fix failure: the dispatch returned normally with no
    /// callback run and the registration left behind (0 callbacks, 1 registration remaining).
    /// </summary>
    [Fact]
    public async Task NatsRecovery_ARegistrationALaggingReplicaHasNotApplied_IsStillFound_AndItsCallbackRunsOnce()
    {
        const string correlationId = "r50-nats-lagging";
        var key = NatsSubjectSchema.RecoveryKey(correlationId);
        var registrationJson = await SerializedRegistrationAsync(correlationId);
        var (adapter, store, leader) = LaggingReplicaAdapter(key);
        leader.Value(key, registrationJson, revision: 4);
        store.Setup(s => s.DeleteAsync(key, It.Is<NatsKVDeleteOpts>(o => o.Revision == 4UL), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);
        var recovery = NatsRecoveryStore(adapter);
        var spy = new R50Spy();
        var (provider, dispatcher) = CreateDispatcher(spy);
        await using var _ = provider;

        var result = await dispatcher.DispatchLostResponses(recovery, correlationId, new OperationResult { Status = OperationStatus.Completed }, "r50", CancellationToken.None);

        Assert.True(result.CallbackInvoked);
        Assert.Equal(1, spy.Resumed);
        store.Verify(s => s.DeleteAsync(key, It.Is<NatsKVDeleteOpts>(o => o.Revision == 4UL), It.IsAny<CancellationToken>()), Times.Once);
    }

    /// <summary>
    /// A read that cannot establish a current answer — no leader, a JetStream error — is not
    /// absence: it propagates, so the delivery stays unacknowledged and retries. Pre-fix failure:
    /// the lagging replica's answer was taken, and the lookup returned an empty list.
    /// </summary>
    [Fact]
    public async Task NatsRecovery_ALeaderReadThatFails_PropagatesInsteadOfReadingAsAbsence()
    {
        const string correlationId = "r50-nats-no-leader";
        var key = NatsSubjectSchema.RecoveryKey(correlationId);
        var (adapter, _, leader) = LaggingReplicaAdapter(key);
        var unavailable = new NatsJSApiException(new ApiError { Code = 503, ErrCode = 10008, Description = "JetStream system temporarily unavailable" });
        leader.Fails(key, unavailable);
        var recovery = NatsRecoveryStore(adapter);
        var spy = new R50Spy();
        var (provider, dispatcher) = CreateDispatcher(spy);
        await using var _ = provider;

        Assert.Same(unavailable, await Assert.ThrowsAsync<NatsJSApiException>(() => recovery.GetAllAsync(correlationId)));
        Assert.Same(unavailable, await Assert.ThrowsAsync<NatsJSApiException>(() => dispatcher.DispatchLostResponses(
            recovery, correlationId, new OperationResult { Status = OperationStatus.Completed }, "r50", CancellationToken.None)));
        Assert.Equal(0, spy.Resumed);
    }

    /// <summary>
    /// The leader read keeps the client's reading of a KV entry: a <c>KV-Operation</c> of
    /// <c>DEL</c> or <c>PURGE</c> — or, without one, a server subject-delete marker — is a
    /// deleted key, read as absence.
    /// </summary>
    [Theory]
    [InlineData("KV-Operation: DEL", null)]
    [InlineData("KV-Operation: PURGE", "Nats-Rollup: sub")]
    [InlineData("kv-operation: del", null)]
    [InlineData("Nats-Marker-Reason: MaxAge", null)]
    [InlineData("Nats-Marker-Reason: Purge", null)]
    [InlineData("Nats-Marker-Reason: Remove", null)]
    public async Task NatsKv_DeleteAndPurgeMarkers_ReadAsAbsent(string header, string? second)
    {
        var (adapter, _, leader) = LaggingReplicaAdapter("r50-marker");
        leader.Marker("r50-marker", revision: 3, second is null ? [header] : [header, second]);

        Assert.Null(await adapter.GetAsync("r50-marker", CancellationToken.None));
    }

    [Theory]
    [InlineData("KV-Operation: PUT")]
    [InlineData("Nats-Expected-Last-Subject-Sequence: 2")]
    public async Task NatsKv_AValueWhoseHeadersMarkNoDelete_IsAValue(string header)
    {
        var (adapter, _, leader) = LaggingReplicaAdapter("r50-put");
        leader.Latest("r50-put", 3, Encoding.UTF8.GetBytes("value"), NatsKvLeaderStream.EncodeHeaders(header));

        var entry = await adapter.GetAsync("r50-put", CancellationToken.None);

        Assert.NotNull(entry);
        Assert.Equal(("value", 3UL), (entry.Value.Value, entry.Value.Revision));
    }

    /// <summary>A marker this build cannot interpret (an unknown value, a repeated operation header) throws rather than reading as a value or as absence.</summary>
    [Theory]
    [InlineData("KV-Operation: MERGE", null)]
    [InlineData("Nats-Marker-Reason: Compacted", null)]
    [InlineData("KV-Operation: DEL", "KV-Operation: PUT")]
    public async Task NatsKv_AnUnknownMarker_Throws(string header, string? second)
    {
        var (adapter, _, leader) = LaggingReplicaAdapter("r50-unknown");
        leader.Marker("r50-unknown", revision: 3, second is null ? [header] : [header, second]);

        await Assert.ThrowsAsync<InvalidOperationException>(() => adapter.GetAsync("r50-unknown", CancellationToken.None));
    }

    /// <summary>
    /// A value that is present but empty is present: the recovery store refuses it as unreadable
    /// rather than reading it as "no registration was ever armed".
    /// </summary>
    [Fact]
    public async Task NatsRecovery_AnEmptyValue_IsUnreadable_NotAbsent()
    {
        const string correlationId = "r50-nats-empty";
        var key = NatsSubjectSchema.RecoveryKey(correlationId);
        var (adapter, _, leader) = LaggingReplicaAdapter(key);
        leader.Latest(key, 2, ReadOnlyMemory<byte>.Empty, headers: null);

        await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => NatsRecoveryStore(adapter).GetAllAsync(correlationId));
    }

    /// <summary>The bucket's stream is looked up once, and a lookup that failed is retried by the next read.</summary>
    [Fact]
    public async Task NatsKv_TheBucketStream_IsResolvedOnce_AndAFailedLookupIsRetried()
    {
        var (adapter, _, leader) = LaggingReplicaAdapter("r50-lookup");
        leader.Value("r50-lookup", "v", revision: 1);
        leader.JetStream.SetupSequence(j => j.GetStreamAsync("KV_" + Bucket, It.IsAny<StreamInfoRequest?>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NatsJSApiException(new ApiError { Code = 503, ErrCode = 10008, Description = "JetStream system temporarily unavailable" }))
            .ReturnsAsync(leader.Stream.Object);

        await Assert.ThrowsAsync<NatsJSApiException>(() => adapter.GetAsync("r50-lookup", CancellationToken.None));
        Assert.NotNull(await adapter.GetAsync("r50-lookup", CancellationToken.None));
        Assert.NotNull(await adapter.GetAsync("r50-lookup", CancellationToken.None));

        leader.JetStream.Verify(j => j.GetStreamAsync("KV_" + Bucket, It.IsAny<StreamInfoRequest?>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    private static NatsRecoveryStateStore NatsRecoveryStore(INatsKvStore kv)
        => new(kv, Options.Create(new NatsAsyncResponseChannelOptions()), NullLogger<NatsRecoveryStateStore>.Instance);

    /// <summary>A registration exactly as the NATS recovery store writes it.</summary>
    private static async Task<string> SerializedRegistrationAsync(string correlationId)
    {
        var kv = new FakeNatsKvStore();
        await NatsRecoveryStore(kv).SaveAsync(correlationId, Registration(correlationId), TimeSpan.FromMinutes(30));
        return kv.Entries[NatsSubjectSchema.RecoveryKey(correlationId)];
    }

    // ---------------------------------------------------------------------------------------------
    // F2 — LedgerSizeWarningBytes defaulted to 512 KiB for every store, above the DynamoDB store's
    //      350 000-byte MaxStateBytes, and the warning judged a character estimate that JSON
    //      escaping can leave far below the bytes the cap judges: a growing DynamoDB ledger was
    //      refused before it was ever warned about.

    /// <summary>
    /// A run whose steps each retain a ~4 KiB result, checkpointed after every step, against the
    /// DynamoDB store's own defaults and its own write preflight (the serializer and the cap check
    /// the store runs before every write). Pre-fix failure: the checkpoint was refused at ~354 KB
    /// with no warning logged.
    /// </summary>
    [Fact]
    public async Task AGrowingLedger_UnderTheDynamoDbDefaults_IsWarnedBeforeTheCapRefusesIt()
    {
        var options = DynamoDbOptions();
        var warnings = await GrowUntilRefusedAsync(options, "r50-dynamo-growth", step => new string('r', 4096));

        var first = Assert.Single(warnings);
        Assert.InRange(WarnedSize(first), options.LedgerSizeWarningBytes!.Value, options.MaxStateBytes!.Value);
    }

    /// <summary>
    /// The warning judges the bytes the store writes, not the character estimate. Results made of
    /// many short JSON strings serialize much larger than their characters (each quote is escaped
    /// inside the ledger), so the estimate stayed under a threshold the real ledger had long
    /// passed. Pre-fix failure: no warning before the cap refused the checkpoint.
    /// </summary>
    [Fact]
    public async Task TheGrowthWarning_JudgesTheBytesTheStoreWrites_NotTheCharacterEstimate()
    {
        var options = DynamoDbOptions();
        options.LedgerSizeWarningBytes = 300_000;
        var result = "[" + string.Join(",", Enumerable.Repeat("\"a\"", 1000)) + "]";

        var warnings = await GrowUntilRefusedAsync(options, "r50-escaped-growth", _ => result);

        var first = Assert.Single(warnings);
        Assert.InRange(WarnedSize(first), 300_000, 350_000);
    }

    [Fact]
    public void TheDynamoDbDefaults_PutTheWarningBelowTheCap()
    {
        var options = new DynamoDbDurableFlowOptions();

        Assert.Equal(256 * 1024, options.LedgerSizeWarningBytes);
        Assert.True(options.EffectiveLedgerSizeWarningBytes < options.MaxStateBytes);
        options.Validate();
    }

    /// <summary>
    /// A default threshold at or above the store's cap is fitted under it (three quarters of the
    /// cap) rather than refused: a small <c>MaxStateBytes</c> left the 512 KiB default above it on
    /// any store, and the warning could never fire.
    /// </summary>
    [Theory]
    [MemberData(nameof(ProviderOptionTypes))]
    public void ADefaultWarningThreshold_AtOrAboveTheCap_IsFittedUnderIt(Type providerOptionsType)
    {
        var options = (DurableFlowOptions)Activator.CreateInstance(providerOptionsType)!;
        SetCap(options, 100_000);

        Assert.Equal(75_000, options.EffectiveLedgerSizeWarningBytes);
        ValidateAgainstCap(providerOptionsType, options, 100_000);

        // Under the cap, and without a cap, the default stands as it is.
        SetCap(options, 10_000_000);
        Assert.Equal(options.LedgerSizeWarningBytes, options.EffectiveLedgerSizeWarningBytes);
        SetCap(options, null);
        Assert.Equal(options.LedgerSizeWarningBytes, options.EffectiveLedgerSizeWarningBytes);
    }

    /// <summary>A threshold the application set at or above the store's cap can never fire first: refused when the store validates its options.</summary>
    [Theory]
    [MemberData(nameof(ProviderOptionTypes))]
    public void AnExplicitWarningThreshold_AtOrAboveTheCap_IsRefused_AndNullDisablesTheCheck(Type providerOptionsType)
    {
        var options = (DurableFlowOptions)Activator.CreateInstance(providerOptionsType)!;
        options.LedgerSizeWarningBytes = 350_000;

        var refused = Assert.Throws<TargetInvocationException>(() => ValidateAgainstCap(providerOptionsType, options, 350_000));
        Assert.IsType<InvalidOperationException>(refused.InnerException);
        Assert.Contains("LedgerSizeWarningBytes", refused.InnerException!.Message, StringComparison.Ordinal);

        ValidateAgainstCap(providerOptionsType, options, 350_001);
        ValidateAgainstCap(providerOptionsType, options, null);
        options.LedgerSizeWarningBytes = null;
        ValidateAgainstCap(providerOptionsType, options, 1_000);
        Assert.Null(options.EffectiveLedgerSizeWarningBytes);
    }

    [Fact]
    public void TheDynamoDbOptions_RefuseAnExplicitWarningAtOrAboveTheirCap()
    {
        var options = new DynamoDbDurableFlowOptions { MaxStateBytes = 200_000, LedgerSizeWarningBytes = 200_000 };

        Assert.Contains("LedgerSizeWarningBytes", Assert.Throws<InvalidOperationException>(options.Validate).Message, StringComparison.Ordinal);

        // Left at its default, the same cap is accepted and the warning fitted under it.
        options = new DynamoDbDurableFlowOptions { MaxStateBytes = 200_000 };
        options.Validate();
        Assert.Equal(150_000, options.EffectiveLedgerSizeWarningBytes);
    }

    /// <summary>
    /// The end-to-end form of the fitted default: a store given a cap under the default threshold
    /// still warns before the cap refuses the ledger.
    /// </summary>
    [Fact]
    public async Task AGrowingLedger_UnderACapBelowTheDefaultThreshold_IsWarnedBeforeTheCapRefusesIt()
    {
        var options = DynamoDbOptions();
        options.MaxStateBytes = 200_000;

        var first = Assert.Single(await GrowUntilRefusedAsync(options, "r50-small-cap-growth", _ => new string('r', 4096)));

        Assert.InRange(WarnedSize(first), 150_000, 200_000);
    }

    private static void SetCap(DurableFlowOptions options, long? cap)
        => options.GetType().GetProperty("MaxStateBytes")!.SetValue(options, cap);

    private static void ValidateAgainstCap(Type providerOptionsType, DurableFlowOptions options, long? cap)
        => providerOptionsType.Assembly.GetType("AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared", throwOnError: true)!
            .GetMethod("ValidateLedgerWarningBelowCap", BindingFlags.Public | BindingFlags.Static)!
            .Invoke(null, [options, cap, providerOptionsType.Name]);

    public static TheoryData<Type> ProviderOptionTypes() =>
    [
        typeof(AsyncResponse.DurableFlows.Cosmos.CosmosDurableFlowOptions),
        typeof(DynamoDbDurableFlowOptions),
        typeof(AsyncResponse.DurableFlows.EFCore.EFCoreDurableFlowOptions),
        typeof(AsyncResponse.DurableFlows.MongoDB.MongoDbDurableFlowOptions),
        typeof(AsyncResponse.DurableFlows.MySql.MySqlDurableFlowOptions),
        typeof(AsyncResponse.DurableFlows.Oracle.OracleDurableFlowOptions),
        typeof(AsyncResponse.DurableFlows.PostgreSQL.PostgreSqlDurableFlowOptions),
        typeof(AsyncResponse.DurableFlows.Sqlite.SqliteDurableFlowOptions),
        typeof(AsyncResponse.DurableFlows.SqlServer.SqlServerDurableFlowOptions),
    ];

    private static DynamoDbDurableFlowOptions DynamoDbOptions() => new()
    {
        ExecutionLeaseDuration = TimeSpan.FromDays(30),
        ExecutionLeaseRenewInterval = TimeSpan.FromDays(10),
        MaxRetainedSteps = null
    };

    /// <summary>
    /// Completes one step after another, each checkpointing the whole ledger through the DynamoDB
    /// store's write preflight, until the cap refuses a checkpoint; returns the growth warnings
    /// logged before that.
    /// </summary>
    private static async Task<string[]> GrowUntilRefusedAsync(DynamoDbDurableFlowOptions options, string flowId, Func<int, string> result)
    {
        var clock = new VirtualTimeProvider();
        var transport = new DurableFlowContextTestSupport.RecordingTransport();
        await using var provider = DurableFlowContextTestSupport.BuildProvider(transport, clock);
        var store = new DynamoDbPreflightStore((IFlowStateStore)provider.GetRequiredService<IFlowStateStore>(), options);
        var state = DurableFlowContextTestSupport.State(flowId);
        Assert.True(await store.TryCreateAsync(flowId, state, options.StateExpiry));
        await using var lease = await DurableFlowContextTestSupport.AcquireAsync(store, flowId, options, clock);
        var logger = new CollectingLogger();
        var context = DurableFlowContextTestSupport.CreateContext(provider, state, store, lease, options, clock, transport, logger);

        for (var step = 0; ; step++)
        {
            var value = result(step);
            try
            {
                await context.StepAsync($"step-{step}", () => Task.FromResult(value));
            }
            catch (FlowStateTooLargeException)
            {
                Assert.True(step > 10, $"The cap refused step {step}: the run never grew gradually.");
                return logger.Messages.Where(m => m.Contains("LedgerSizeWarningBytes threshold", StringComparison.Ordinal)).ToArray();
            }

            Assert.True(step < 10_000, "The ledger never reached the cap.");
        }
    }

    private static long WarnedSize(string warning)
        => long.Parse(Regex.Match(warning, @"roughly (\d+) bytes").Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// The in-memory store behind the DynamoDB store's own write preflight: every create and
    /// checkpoint is first serialized and checked against <c>MaxStateBytes</c> exactly as
    /// <see cref="DynamoDbFlowStateStore"/> does before it writes.
    /// </summary>
    private sealed class DynamoDbPreflightStore(IFlowStateStore inner, DynamoDbDurableFlowOptions options) : IFlowStateStore
    {
        private static readonly MethodInfo SerializeBounded = typeof(DynamoDbFlowStateStore).Assembly
            .GetType("AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared", throwOnError: true)!
            .GetMethod("SerializeBounded", BindingFlags.Public | BindingFlags.Static)!;

        private void Preflight(string flowId, FlowState state)
        {
            try
            {
                SerializeBounded.Invoke(null, [flowId, state, options.MaxStateBytes, "DynamoDB"]);
            }
            catch (TargetInvocationException ex) when (ex.InnerException is not null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(ex.InnerException);
            }
        }

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
        {
            Preflight(flowId, state);
            return inner.TryCreateAsync(flowId, state, ttl, cancellationToken);
        }

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
        {
            Preflight(flowId, state);
            return inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);
        }

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default) => inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default) => inner.TryDeleteAsync(flowId, cancellationToken);
    }

    // ---------------------------------------------------------------------------------------------
    // F3 — operation spans ended with a raw Activity.Dispose(), which runs every ActivityListener's
    //      stopped callback inline: one that threw escaped after the work was done, so a job that
    //      had run, a response that was delivered or a recovery callback that was invoked all
    //      reported failure and were redelivered — and ran again.

    /// <summary>
    /// The review's reproduction: two deliveries of one job used to run the handler twice and fail
    /// both. Now the first delivery succeeds, so there is no second.
    /// </summary>
    [Fact]
    public async Task AWorkerJob_WithAThrowingExecuteSpanListener_RunsOnce_AndItsDeliverySucceeds()
    {
        using var telemetry = new ThrowingSpanStopListener("asyncresponse.worker.execute");
        var worker = new R50Worker();
        await using var provider = new ServiceCollection().AddSingleton<IR50Worker>(worker).BuildServiceProvider();
        var executor = new WorkerJobExecutor(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<WorkerJobExecutor>.Instance);

        await executor.ExecuteAsync(WorkerJob("r50-worker"));

        Assert.Equal(1, worker.Runs);
        Assert.Equal(1, telemetry.Thrown);
    }

    [Theory]
    [InlineData("asyncresponse.ingress.worker")]
    [InlineData("asyncresponse.worker.execute")]
    public async Task AWorkerMessage_WithAThrowingSpanListener_IsHandledOnce_WithoutFailingTheDelivery(string span)
    {
        using var telemetry = new ThrowingSpanStopListener(span);
        var worker = new R50Worker();
        await using var provider = Services(services => services.AddSingleton<IR50Worker>(worker));
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        await ingress.HandleWorkerMessageAsync(AsyncResponseJson.Serialize(WorkerJob("r50-ingress-worker")));

        Assert.Equal(1, worker.Runs);
        Assert.Equal(1, telemetry.Thrown);
    }

    [Theory]
    [InlineData("asyncresponse.ingress.response")]
    [InlineData("asyncresponse.ingress.raw_response")]
    public async Task AResponseMessage_WithAThrowingSpanListener_ReachesItsWaiter_WithoutFailingTheDelivery(string span)
    {
        using var telemetry = new ThrowingSpanStopListener(span);
        await using var provider = Services();
        var channel = provider.GetRequiredService<InMemoryAsyncResponseChannel>();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();
        var waiter = await channel.CreateResponseWaiter<OperationResult>("r50-ingress-response");

        await ingress.HandleResponseMessageAsync(
            AsyncResponseJson.Serialize(new OperationResult { Status = OperationStatus.Completed, Message = "done" }),
            "r50-ingress-response");

        Assert.Equal("done", (await waiter.ResponseTask.WaitAsync(Wait)).Message);
        Assert.Equal(1, telemetry.Thrown);
        await waiter.DisposeAsync();
    }

    /// <summary>
    /// A recovery dispatch whose span stop throws after the callback ran. Pre-fix failure: the
    /// dispatch failed, the registration stayed, and the redelivery ran the callback again.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ARecoveryDispatch_WithAThrowingSpanListener_InvokesOnce_AndConsumesTheRegistration(bool exceptionRoute)
    {
        using var telemetry = new ThrowingSpanStopListener("asyncresponse.lost_subscriber.dispatch");
        var spy = new R50Spy();
        var (provider, dispatcher) = CreateDispatcher(spy);
        await using var _ = provider;
        var store = new InMemoryRecoveryStateStore();
        const string correlationId = "r50-dispatch";
        await store.SaveAsync(correlationId, Registration(correlationId), TimeSpan.FromMinutes(5));

        Task<LostSubscriberDispatchResult> Dispatch() => exceptionRoute
            ? dispatcher.DispatchLostExceptions(store, correlationId, new TimeoutException("gave up"), "r50", CancellationToken.None)
            : dispatcher.DispatchLostResponses(store, correlationId, new OperationResult { Status = OperationStatus.Completed }, "r50", CancellationToken.None);

        Assert.True((await Dispatch()).CallbackInvoked);
        Assert.Empty(await store.GetAllAsync(correlationId));

        // A redelivery finds nothing left to run.
        Assert.False((await Dispatch()).CallbackInvoked);
        Assert.Equal(1, exceptionRoute ? spy.Failed : spy.Resumed);
        Assert.True(telemetry.Thrown >= 1);
    }

    [Theory]
    [InlineData("asyncresponse.set_response")]
    [InlineData("asyncresponse.set_exception")]
    public async Task APublish_WithAThrowingSpanListener_ReachesTheWaiter_AndThePublisherSucceeds(string span)
    {
        using var telemetry = new ThrowingSpanStopListener(span);
        await using var provider = Services();
        var channel = provider.GetRequiredService<InMemoryAsyncResponseChannel>();
        var waiter = await channel.CreateResponseWaiter<OperationResult>("r50-publish");

        if (span == "asyncresponse.set_response")
        {
            await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "done" }, "r50-publish").WaitAsync(Wait);
            Assert.Equal("done", (await waiter.ResponseTask.WaitAsync(Wait)).Message);
        }
        else
        {
            await channel.SetException(new TimeoutException("gave up"), "r50-publish").WaitAsync(Wait);
            await Assert.ThrowsAnyAsync<Exception>(() => waiter.ResponseTask.WaitAsync(Wait));
        }

        Assert.Equal(1, telemetry.Thrown);
        await waiter.DisposeAsync();
    }

    /// <summary>
    /// An enqueue whose job was handed to the transport. Pre-fix failure: the caller saw the
    /// listener's exception for a job that was already queued — and ran — so a retry ran it twice.
    /// </summary>
    [Theory]
    [InlineData("asyncresponse.enqueue_worker")]
    [InlineData("asyncresponse.worker.publish")]
    public async Task AnEnqueue_WithAThrowingSpanListener_Succeeds_AndItsJobRunsOnce(string span)
    {
        using var telemetry = new ThrowingSpanStopListener(span);
        var worker = new R50Worker();
        await using var provider = Services(services => services.AddSingleton<IR50Worker>(worker), inMemoryTransport: true);
        var host = provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>().OfType<InMemoryWorkerHost>().Single();
        await host.StartAsync(CancellationToken.None);
        try
        {
            await provider.GetRequiredService<IAsyncResponseBuilder>().EnqueueWorkerAsync<IR50Worker>(w => w.RunAsync());
            await worker.FirstRun.WaitAsync(Wait);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        Assert.Equal(1, worker.Runs);
        Assert.Equal(1, telemetry.Thrown);
    }

    /// <summary>
    /// Ending a span through the scope restores the ambient span it started under even when the
    /// stopped callback threw before the stop could — a sync caller shares the ambient context.
    /// </summary>
    [Fact]
    public void StopOnExit_WithAThrowingStoppedListener_EndsTheSpan_AndRestoresTheAmbientOne()
    {
        using var telemetry = new ThrowingSpanStopListener("asyncresponse.test.scope");
        var ambient = Activity.Current;
        Activity? span = null;

        void Operation()
        {
            span = AsyncResponseDiagnostics.StartActivity("asyncresponse.test.scope");
            using var spanStop = AsyncResponseDiagnostics.StopOnExit(span);
            Assert.Same(span, Activity.Current);
        }

        Operation();

        Assert.Equal(1, telemetry.Thrown);
        Assert.NotNull(span);
        Assert.True(span.IsStopped);
        Assert.Same(ambient, Activity.Current);
        using (AsyncResponseDiagnostics.StopOnExit(null))
        {
            // Nothing sampled: nothing to end.
        }
    }

    /// <summary>
    /// Every operation span in the shipped code ends through the nonthrowing stop — the bundled
    /// transports' receive and publish spans included, which no unit test here drives with a
    /// throwing listener. A raw <c>using var activity = …StartActivity(…)</c> puts the listener's
    /// exception back on the delivery path.
    /// </summary>
    [Fact]
    public void NoShippedOperationSpan_IsEndedByARawDispose()
    {
        var src = Path.Combine(FindRepoRoot(), "src");
        var rawUsing = new Regex(@"using\s*(\(\s*)?var\s+\w+\s*=\s*AsyncResponseDiagnostics\.StartActivity\(", RegexOptions.CultureInvariant);
        var offenders = new List<string>();
        var sites = 0;

        foreach (var path in Directory.EnumerateFiles(src, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(src, path);
            if (relative.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                || relative.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                continue;

            var text = File.ReadAllText(path);
            if (rawUsing.IsMatch(text))
                offenders.Add(relative);

            var starts = Regex.Matches(text, @"AsyncResponseDiagnostics\.StartActivity\(").Count;
            if (starts == 0)
                continue;

            sites += starts;
            var stops = Regex.Matches(text, @"AsyncResponseDiagnostics\.(StopOnExit|StopActivity)\(").Count;
            if (stops < starts)
                offenders.Add($"{relative} ({starts} spans started, {stops} ended through the nonthrowing stop)");
        }

        Assert.True(sites > 40, $"Only {sites} span sites found under {src}.");
        Assert.True(offenders.Count == 0, "Operation spans ended by a raw Activity.Dispose():" + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    private static string FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "AsyncResponse.slnx")))
                return directory.FullName;
            directory = directory.Parent!;
        }

        throw new InvalidOperationException("Repository root (AsyncResponse.slnx) not found above the test base directory.");
    }

    // ---------------------------------------------------------------------------------------------
    // Shared assets

    public interface IR50Worker
    {
        Task RunAsync();
    }

    private sealed class R50Worker : IR50Worker
    {
        private readonly TaskCompletionSource _firstRun = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _runs;

        public int Runs => Volatile.Read(ref _runs);

        public Task FirstRun => _firstRun.Task;

        public Task RunAsync()
        {
            Interlocked.Increment(ref _runs);
            _firstRun.TrySetResult();
            return Task.CompletedTask;
        }
    }

    public interface IR50Spy
    {
        Task Resume(OperationResult payload);
        Task Fail(Exception exception);
    }

    private sealed class R50Spy : IR50Spy
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

    private static WorkerJobEnvelope WorkerJob(string correlationId) => new()
    {
        CorrelationId = correlationId,
        Call = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IR50Worker).FullName!,
            MethodName = nameof(IR50Worker.RunAsync),
            Params = []
        }
    };

    private static RecoveryState Registration(string correlationId) => new()
    {
        RegistrationId = Guid.NewGuid(),
        CorrelationId = correlationId,
        PayloadTypeFullName = typeof(OperationResult).FullName,
        RegisteredAtUtc = DateTime.UtcNow,
        ResumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IR50Spy).FullName!,
            MethodName = nameof(IR50Spy.Resume),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
        },
        FailureCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IR50Spy).FullName!,
            MethodName = nameof(IR50Spy.Fail),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
        }
    };

    private static ServiceProvider Services(Action<IServiceCollection>? configure = null, bool inMemoryTransport = false)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        configure?.Invoke(services);
        var registration = services.AddAsyncResponse().WithInMemoryChannel();
        if (inMemoryTransport)
            registration.WithInMemoryTransport();
        return services.BuildServiceProvider();
    }

    private static (ServiceProvider Provider, LostSubscriberCallbackDispatcher Dispatcher) CreateDispatcher(R50Spy spy)
    {
        var provider = Services(services => services.AddSingleton<IR50Spy>(spy));
        return (provider, new LostSubscriberCallbackDispatcher(
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>(),
            NullLogger.Instance));
    }
}
