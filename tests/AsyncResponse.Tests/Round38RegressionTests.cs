using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Channels.NATS;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.Redis;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Testing;
using AsyncResponse.Transports.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using System.Collections;
using System.Reflection;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regressions for round 38 (external holistic review of 94c3ddb): behavior pins that compile
/// against the pre-fix tree and fail there. Pins over API this round introduced
/// (<c>FlowState.RetainUntilUtc</c>, <c>InMemoryWorkerTransportOptions.DelayedJobCapacity</c>,
/// <c>KafkaSubscriberOptions.FaultDrainTimeout</c>) live in <see cref="Round38NewApiTests"/>.
/// </summary>
public sealed class Round38RegressionTests
{
    public sealed record R38Input(string Value);

    public sealed class R38NoopFlow : IDurableFlow<R38Input>
    {
        public static int Runs;

        public Task ExecuteAsync(IDurableFlowContext flow, R38Input input)
        {
            Interlocked.Increment(ref Runs);
            return Task.CompletedTask;
        }
    }

    private static FlowState State(string id, string? parent = null) => new()
    {
        FlowId = id,
        ParentFlowId = parent,
        Status = FlowRunStatus.Running,
        FlowTypeName = typeof(R38NoopFlow).FullName,
        InputTypeName = typeof(R38Input).FullName,
        InputJson = JsonSerializer.Serialize(new R38Input("x")),
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    private static ServiceProvider BuildProvider(IWorkerTransport transport, TimeProvider clock)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(clock);
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<R38NoopFlow, R38Input>();
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

    /// <summary>A delayed-capable transport that only records what it was asked to publish.</summary>
    internal sealed class CapturingDelayedTransport : IDelayedWorkerTransport
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

    // ---------------------------------------------------------------------------------------------
    // F1 — the ancestor extension ceded a lost revision race: a concurrent write to the parent
    //      (its own replay re-parking on a pre-park snapshot of the child, or the executor's
    //      per-attempt save) stamped the plain StateExpiry, the child's extension lost the
    //      compare-and-swap, logged "the ancestor is live and re-stamping its own expiry", and
    //      parked with its wake-up published — on a parent that expired under the hour-long wait.

    /// <summary>
    /// Wraps a store and, once, lets a competing writer win the parent's revision right between
    /// the child's read of the parent and its extension write — the competing write carrying the
    /// plain one-minute expiry of a checkpoint that knows nothing about the park.
    /// </summary>
    internal sealed class CollidingFlowStateStore(IFlowStateStore inner, string collideOn) : IFlowStateStore
    {
        private int _collisionsLeft = 1;

        public int ConcurrentWrites { get; private set; }
        public int LostRaces { get; private set; }

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public async Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
        {
            if (leaseId is null && string.Equals(flowId, collideOn, StringComparison.Ordinal) && _collisionsLeft-- > 0)
            {
                var current = (await inner.LoadAsync(flowId, cancellationToken))!;
                var old = current.Revision;
                current.Revision++;
                if (await inner.TryUpdateAsync(flowId, current, old, TimeSpan.FromMinutes(1), null, cancellationToken))
                    ConcurrentWrites++;
            }

            var written = await inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);
            if (!written && leaseId is null)
                LostRaces++;
            return written;
        }

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);
    }

    private static readonly DurableFlowOptions ShortLedgerOptions = new()
    {
        StateExpiry = TimeSpan.FromMinutes(1),
        DefaultStepTimeout = TimeSpan.FromSeconds(10),
        TimerInProcessThreshold = TimeSpan.Zero
    };

    /// <summary>
    /// Pre-fix failure: the child parks and publishes its wake-up (one job), the lost race is
    /// logged and ignored, and two virtual minutes later the root is gone while the child's
    /// hour-long park lives on. Now the lost race is retried against the re-read revision, so the
    /// root outlives its one-minute expiry — and the park still publishes exactly one wake-up.
    /// <para>
    /// The child is a REPLAYED timer (its due time already persisted): a first-pass park extends
    /// the chain twice — breadcrumb save, then suspend — so a single lost race there was healed
    /// by the second pass, while a replay parks through the suspend save alone and a lost race
    /// there was final. A redelivered wake-up is the ordinary way a parked timer re-executes.
    /// </para>
    /// </summary>
    [Fact]
    public async Task AncestorExtension_ALostRevisionRace_IsRetried_SoTheParentOutlivesTheChildsPark()
    {
        var clock = new VirtualTimeProvider();
        var transport = new CapturingDelayedTransport();
        await using var provider = BuildProvider(transport, clock);
        var inner = provider.GetRequiredService<IFlowStateStore>();
        var store = new CollidingFlowStateStore(inner, collideOn: "r38-root");

        var root = State("r38-root");
        root.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal) { ["child"] = new() { ChildFlowId = "r38-root:child" } };
        var child = State("r38-root:child", "r38-root");
        child.ParentStepName = "child";
        child.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
        {
            ["long-wait"] = new() { WakeAtUtc = clock.GetUtcNow().UtcDateTime.AddHours(1) }
        };
        Assert.True(await inner.TryCreateAsync("r38-root", root, TimeSpan.FromMinutes(1)));
        Assert.True(await inner.TryCreateAsync("r38-root:child", child, TimeSpan.FromMinutes(1)));

        await using (var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, child.FlowId!, ShortLedgerOptions, NullLogger.Instance, clock))!)
        {
            var context = CreateContext(provider, child, store, lease, ShortLedgerOptions, clock, transport);
            await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => context.DelayAsync("long-wait", TimeSpan.FromHours(1)));
        }

        Assert.Equal(1, store.ConcurrentWrites);
        Assert.Equal(1, store.LostRaces);
        Assert.Equal(1, transport.Count);

        clock.Advance(TimeSpan.FromMinutes(2));
        Assert.NotNull(await inner.LoadAsync("r38-root:child"));
        Assert.True(await inner.LoadAsync("r38-root") is not null, "the root expired under the child's hour-long park after a lost extension race");
    }

    // ---------------------------------------------------------------------------------------------
    // F4 — "corrupt means absent": a ledger whose JSON disagreed with its stored revision or key
    //      loaded as null, the executor acknowledged the wake-up as belonging to a deleted flow,
    //      and the physically present run lost its only wake-up.

    private static IDictionary Entries(IFlowStateStore store)
        => (IDictionary)store.GetType().GetField("_entries", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(store)!;

    private static void CorruptRevision(IFlowStateStore store, string flowId)
    {
        var entry = Entries(store)[flowId]!;
        var property = entry.GetType().GetProperty("StateJson")!;
        var json = (string)property.GetValue(entry)!;
        Assert.Contains("\"Revision\":0", json, StringComparison.Ordinal);
        property.SetValue(entry, json.Replace("\"Revision\":0", "\"Revision\":7", StringComparison.Ordinal));
    }

    /// <summary>
    /// Pre-fix failure: LoadAsync returned null for an entry that is still in the dictionary.
    /// Now it throws FlowStateUnreadableException naming both revisions.
    /// </summary>
    [Fact]
    public async Task InMemoryFlowStore_ARevisionMismatchInsideTheLedger_IsUnreadable_NotAbsent()
    {
        await using var provider = BuildProvider(new CapturingDelayedTransport(), new VirtualTimeProvider());
        var store = provider.GetRequiredService<IFlowStateStore>();
        Assert.True(await store.TryCreateAsync("r38-corrupt", State("r38-corrupt"), TimeSpan.FromDays(1)));
        CorruptRevision(store, "r38-corrupt");

        var ex = await Assert.ThrowsAsync<FlowStateUnreadableException>(() => store.LoadAsync("r38-corrupt"));
        Assert.Equal("r38-corrupt", ex.FlowId);
        Assert.Contains("revision", ex.Reason, StringComparison.OrdinalIgnoreCase);
        Assert.True(Entries(store).Contains("r38-corrupt"));
    }

    /// <summary>
    /// Pre-fix failure: ExecuteAsync returned normally (the wake-up acknowledged) without running
    /// the flow, with the row still present. Now the unreadable ledger propagates to the transport's
    /// retry/dead-letter path, which is the operator alarm.
    /// </summary>
    [Fact]
    public async Task Executor_AnInconsistentLedger_PropagatesUnreadable_InsteadOfAcknowledgingTheWakeUp()
    {
        await using var provider = BuildProvider(new CapturingDelayedTransport(), new VirtualTimeProvider());
        var store = provider.GetRequiredService<IFlowStateStore>();
        Assert.True(await store.TryCreateAsync("r38-corrupt-exec", State("r38-corrupt-exec"), TimeSpan.FromDays(1)));
        CorruptRevision(store, "r38-corrupt-exec");
        var runsBefore = R38NoopFlow.Runs;

        await Assert.ThrowsAsync<FlowStateUnreadableException>(
            () => provider.GetRequiredService<IDurableFlowExecutor>().ExecuteAsync("r38-corrupt-exec"));

        Assert.Equal(runsBefore, R38NoopFlow.Runs);
        Assert.True(Entries(store).Contains("r38-corrupt-exec"));
    }

    // ---------------------------------------------------------------------------------------------
    // F2 — the recovery-state readers logged the raw JsonException, whose Path is built from the
    //      stored registration's Context keys — tenant and auth baggage — so a malformed blob
    //      copied them into the application log. The readers now go through JsonSafety, and the
    //      logged failure carries size and position only.

    private const string Marker = "private_customer_42@example.invalid";

    /// <summary>A registration whose Context has the marker as a KEY with a non-string value: STJ's failure path names the key.</summary>
    private static string MalformedState()
        => "{\"SchemaVersion\":1,\"RegistrationId\":\"" + Guid.NewGuid() + "\",\"CorrelationId\":\"corr\",\"Context\":{\"" + Marker + "\":123}}";

    private static void AssertNothingLeaked(CollectingLogger logger)
    {
        Assert.NotEmpty(logger.Entries);
        foreach (var (message, exception) in logger.Entries)
        {
            Assert.DoesNotContain(Marker, message, StringComparison.Ordinal);
            Assert.DoesNotContain(Marker, exception?.ToString() ?? string.Empty, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void NatsRecoveryReader_AMalformedEnvelope_LogsNoContextKey()
    {
        var logger = new CollectingLogger();
        var store = new NatsRecoveryStateStore(new FakeNatsKvStore(), Options.Create(new NatsAsyncResponseChannelOptions()), logger.For<NatsRecoveryStateStore>());
        var json = "{\"States\":[" + MalformedState() + "]}";

        var result = typeof(NatsRecoveryStateStore)
            .GetMethod("TryDeserialize", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(store, [json, "recovery-key"]);

        Assert.Null(result);
        AssertNothingLeaked(logger);
    }

    [Fact]
    public void RedisRecoveryReader_AMalformedEnvelope_LogsNoContextKey()
    {
        var logger = new CollectingLogger();
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(new Mock<IDatabase>().Object);
        var store = new RedisRecoveryStateStore(
            multiplexer.Object,
            Options.Create(new RedisAsyncResponseOptions { KeyPrefix = "ar" }),
            logger.For<RedisRecoveryStateStore>());
        var json = "{\"Registrations\":[{\"State\":" + MalformedState() + ",\"ExpiresAtUtc\":\"2100-01-01T00:00:00Z\"}]}";

        var deserialize = typeof(RedisRecoveryStateStore).GetMethod("DeserializeEntries", BindingFlags.Instance | BindingFlags.NonPublic)!;
        deserialize.Invoke(store, [(RedisValue)json, "ar:recovery:corr", "corr", true, DateTimeOffset.UtcNow, false, false]);

        AssertNothingLeaked(logger);
    }

    [Fact]
    public void PostgreSqlRecoveryReader_AMalformedRow_LogsNoContextKey()
        => AssertDatabaseReaderLeaksNothing(
            logger => new PostgreSqlRecoveryStateStore(null!, logger.For<PostgreSqlRecoveryStateStore>()));

    [Fact]
    public void SqlServerRecoveryReader_AMalformedRow_LogsNoContextKey()
        => AssertDatabaseReaderLeaksNothing(
            logger => new SqlServerRecoveryStateStore(null!, logger.For<SqlServerRecoveryStateStore>()));

    [Fact]
    public void MongoDbRecoveryReader_AMalformedRow_LogsNoContextKey()
        => AssertDatabaseReaderLeaksNothing(
            logger => new MongoDbRecoveryStateStore(null!, logger.For<MongoDbRecoveryStateStore>()));

    private static void AssertDatabaseReaderLeaksNothing(Func<CollectingLogger, object> createStore)
    {
        var logger = new CollectingLogger();
        var store = createStore(logger);

        object?[] args = [MalformedState(), "corr", 0];
        var result = store.GetType()
            .GetMethod("DeserializeState", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(store, args);

        Assert.Null(result);
        Assert.Equal(1, (int)args[2]!);   // still counted as unreadable
        AssertNothingLeaked(logger);
    }

    // ---------------------------------------------------------------------------------------------
    // F3 — delayed in-memory jobs were held against no bound at all: every scheduled publish
    //      retained its envelope and captured execution context, and when the timers fired each
    //      started a channel write that pended outside the bounded queue. Ten thousand scheduled
    //      jobs were accepted with QueueCapacity = 1 and InJobOverflowCapacity = 1.

    private static WorkerJobEnvelope DelayedJob() => new()
    {
        Call = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "AsyncResponse.Tests.IRound38Probe",
            MethodName = "RunAsync",
            Params = []
        }
    };

    /// <summary>
    /// Pre-fix failure: the 4 097th delayed publish completes like the 4 096 before it. Now it
    /// waits for a slot (the default DelayedJobCapacity is 4 096), honors its cancellation token,
    /// and is admitted once a scheduled job fires and enters the queue.
    /// </summary>
    [Fact]
    public async Task InMemoryTransport_DelayedPublishesFromOutsideAJob_WaitPastTheDefaultBound()
    {
        var clock = new VirtualTimeProvider();
        var transport = new InMemoryWorkerTransport(
            Options.Create(new InMemoryWorkerTransportOptions { QueueCapacity = 1, InJobOverflowCapacity = 1 }),
            clock);

        for (var i = 0; i < 4_096; i++)
            await transport.PublishAsync(DelayedJob(), TimeSpan.FromHours(1));
        Assert.Equal(4_096, transport.SnapshotDelayedJobs().Count);

        using var cancel = new CancellationTokenSource();
        var overflowing = transport.PublishAsync(DelayedJob(), TimeSpan.FromHours(1), cancel.Token);
        await Task.Delay(100);
        Assert.False(overflowing.IsCompleted, "the delayed publish past the bound was accepted");

        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => overflowing);
        Assert.Equal(4_096, transport.SnapshotDelayedJobs().Count);

        // One scheduled job fires and enters the (one-slot) queue: its slot frees and the next
        // delayed publish is admitted.
        clock.Advance(TimeSpan.FromHours(1));
        await Task.Delay(50);
        Assert.Equal(1, transport.Reader.Count);
        await transport.PublishAsync(DelayedJob(), TimeSpan.FromHours(1)).WaitAsync(TimeSpan.FromSeconds(5));
    }

    // ---------------------------------------------------------------------------------------------
    // F5 — a Kafka message that could not be parsed into a delivery was dead-lettered and its
    //      offset stored at once, bypassing the dispatcher's per-partition order. Consumed behind
    //      a detached handler of the same partition (a rebalance handing the partition back with
    //      its pause reset delivers the next record), that stored the partition PAST the
    //      unfinished message; the auto-committer committed it, and a crash skipped the valid job
    //      for good — with only the malformed record's copy in the dead-letter topic.

    private sealed class GateIngress : IAsyncResponseIngress
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task HandleResponseMessageAsync(string json, string? correlationId) => Task.CompletedTask;

        public Task HandleWorkerMessageAsync(string json)
        {
            if (json != "slow-job")
                return Task.CompletedTask;

            Entered.TrySetResult();
            return Release.Task;
        }
    }

    private static KafkaAsyncResponseTransportOptions KafkaOptions(Action<KafkaAsyncResponseTransportOptions>? configure = null)
    {
        var options = KafkaTestData.NewOptions();
        options.WorkerTopic = "workers";
        options.CreateTopics = false;
        options.SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1);
        options.SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(2);
        options.WorkerSubscriber.DetachHandlerAfter = TimeSpan.FromMilliseconds(10);
        configure?.Invoke(options);
        return options;
    }

    /// <summary>
    /// Pre-fix failure: with the handler for offset 7 still running, offset 8 (empty payload) is
    /// stored at once — <c>[8]</c> — and one dead-letter copy exists. Now nothing is stored while
    /// 7 is pending; once it settles, 7 and then 8 are stored in order and the malformed record is
    /// buried in its turn.
    /// </summary>
    [Fact]
    public async Task KafkaWorker_AMalformedMessageBehindADetachedHandler_IsNotCommittedAheadOfIt()
    {
        var consumer = new FakeKafkaConsumerClient { IgnorePartitionPause = true };
        consumer.Enqueue(KafkaTestData.Message("workers", offset: 7, payload: "slow-job"));
        consumer.Enqueue(KafkaTestData.Message("workers", offset: 8, payload: ""));
        var ingress = new GateIngress();
        var producer = new FakeKafkaProducerClient();
        using var subscriber = new KafkaWorkerSubscriber(
            Options.Create(KafkaOptions()),
            new FakeKafkaConsumerClientFactory(consumer),
            producer,
            new FakeKafkaAdminClient(),
            ingress,
            NullLogger<KafkaWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await Task.Delay(300);

            Assert.False(ingress.Release.Task.IsCompleted);
            Assert.Empty(consumer.StoredOffsets);
            Assert.Empty(producer.Publishes);

            ingress.Release.SetResult();
            await KafkaTestData.WaitUntilAsync(() => consumer.StoredOffsets.Count == 2);
            Assert.Equal(
                [new FakeKafkaConsumerClient.StoredOffset("workers", 0, 7), new FakeKafkaConsumerClient.StoredOffset("workers", 0, 8)],
                consumer.StoredOffsets);
            Assert.Single(producer.Publishes);
            await KafkaTestData.WaitUntilAsync(() => !consumer.IsPartitionPaused(0));
        }
        finally
        {
            ingress.Release.TrySetResult();
            await subscriber.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>The ordinary case is unchanged: with nothing detached on the partition, a malformed record is buried and stored at once.</summary>
    [Fact]
    public async Task KafkaWorker_AMalformedMessageWithNothingDetached_IsStillDiscardedAtOnce()
    {
        var consumer = new FakeKafkaConsumerClient();
        consumer.Enqueue(KafkaTestData.Message("workers", offset: 3, payload: ""));
        var producer = new FakeKafkaProducerClient();
        using var subscriber = new KafkaWorkerSubscriber(
            Options.Create(KafkaOptions()),
            new FakeKafkaConsumerClientFactory(consumer),
            producer,
            new FakeKafkaAdminClient(),
            new GateIngress(),
            NullLogger<KafkaWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await KafkaTestData.WaitUntilAsync(() => consumer.StoredOffsets.Count == 1);
            Assert.Equal(new FakeKafkaConsumerClient.StoredOffset("workers", 0, 3), Assert.Single(consumer.StoredOffsets));
            Assert.Single(producer.Publishes);
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
        }
    }
}
