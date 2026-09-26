using AsyncResponse.DurableFlows.MongoDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;
using Moq;
using System.Globalization;
using System.Net;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Covers the MongoDB durable-flow store's creation clock authority: a plain insert cannot
/// evaluate <c>$$NOW</c>, so <see cref="MongoDbFlowStateStore"/> stamps a fresh ledger from the
/// server's <c>hello.localTime</c> — the same authority every later <c>$$NOW</c> expiry/lease
/// comparison runs on — instead of the app clock.
/// </summary>
public sealed class MongoDbFlowStateStoreTests
{
    [Fact]
    public async Task TryCreate_StampsInsertedLedgerFromServerClock()
    {
        // Deliberately years away from the app clock so an app-clock stamp cannot pass by luck.
        var serverNow = new DateTime(2031, 3, 14, 9, 26, 53, 589, DateTimeKind.Utc);
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1, ["localTime"] = serverNow });

        Assert.True(await harness.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));

        Assert.NotNull(harness.Inserted);
        Assert.Equal(serverNow, harness.Inserted!.UpdatedAtUtc);
        Assert.Equal(serverNow.AddMinutes(5), harness.Inserted.ExpiresAtUtc);
        // One cheap round-trip per create, not one per attempt/step.
        harness.Database.Verify(
            item => item.RunCommandAsync(
                It.IsAny<Command<BsonDocument>>(),
                It.IsAny<ReadPreference>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task TryCreate_MissingLocalTime_FallsBackToAppClock()
    {
        // A mongo-compatible endpoint that omits localTime must not fail creates; the app clock
        // restores the pre-server-clock behavior.
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 });
        var before = DateTime.UtcNow;

        Assert.True(await harness.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));

        Assert.NotNull(harness.Inserted);
        Assert.InRange(harness.Inserted!.UpdatedAtUtc, before, DateTime.UtcNow);
    }

    [Theory]
    [MemberData(nameof(NonDateLocalTimes))]
    public async Task TryCreate_NonDateLocalTime_FallsBackToAppClock(BsonValue localTime)
    {
        // Only BsonDateTime implements ToUniversalTime — every other BsonValue throws
        // NotSupportedException. A mongo-compatible endpoint (Cosmos Mongo API, DocumentDB,
        // FerretDB) answering hello with a PRESENT non-date localTime must fall back exactly
        // like an absent one, not fail every create.
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1, ["localTime"] = localTime });
        var before = DateTime.UtcNow;

        Assert.True(await harness.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));

        Assert.NotNull(harness.Inserted);
        Assert.InRange(harness.Inserted!.UpdatedAtUtc, before, DateTime.UtcNow);
    }

    public static TheoryData<BsonValue> NonDateLocalTimes =>
    [
        BsonNull.Value,
        new BsonString("2031-03-14T09:26:53.589Z"),
        new BsonInt64(1_900_000_000_000)
    ];

    private static FlowState CreateState(string flowId) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(TestOnboardingFlow).FullName,
        InputTypeName = typeof(TestFlowInput).FullName,
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    [Fact]
    public void Construction_PinsLedgerReadsToThePrimary()
    {
        // Regression: the collection inherited whatever read preference the host-supplied
        // IMongoDatabase carried, so a readPreference=secondaryPreferred connection string routed
        // every ledger load to a possibly-lagging secondary — a stale revision replays an
        // already-checkpointed step, and a not-yet-replicated ledger reads as null, the one
        // answer callers ACK a wake-up on. Reads are pinned to the primary at construction
        // (DynamoDB pins ConsistentRead for the same reason).
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 });

        harness.Collection.Verify(c => c.WithReadPreference(ReadPreference.Primary), Times.Once);
    }

    [Fact]
    public async Task DisabledIndexCreation_WithoutAProvisionedTtlIndex_FailsWithActionableError()
    {
        // Regression: AutoCreateIndexes = false skipped index creation AND verification, so an
        // operator-provisioned collection without expireAfterSeconds lost the store's only
        // cleanup mechanism — the ledger collection grew without bound, with no error and no log
        // line (loads filter on ExpiresAtUtc, so every functional test stayed green). Cosmos and
        // DynamoDB hard-fail the same way when their server-side reaper is missing.
        var collection = new Mock<IMongoCollection<MongoFlowStateDocument>>();
        collection
            .Setup(c => c.WithReadPreference(It.IsAny<ReadPreference>()))
            .Returns(collection.Object);
        collection
            .Setup(c => c.WithWriteConcern(It.IsAny<WriteConcern>()))
            .Returns(collection.Object);

        // The provisioned collection lists ONLY the default _id index — no TTL reaper.
        var cursor = new Mock<IAsyncCursor<BsonDocument>>();
        cursor.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);
        cursor.SetupGet(c => c.Current).Returns(
        [
            new BsonDocument { ["name"] = "_id_", ["key"] = new BsonDocument("_id", 1) }
        ]);
        var indexes = new Mock<IMongoIndexManager<MongoFlowStateDocument>>();
        indexes.Setup(m => m.ListAsync(It.IsAny<CancellationToken>())).ReturnsAsync(cursor.Object);
        collection.SetupGet(c => c.Indexes).Returns(indexes.Object);

        var database = new Mock<IMongoDatabase>().WithTestNamespace();
        database
            .Setup(d => d.GetCollection<MongoFlowStateDocument>("flows", It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        using var store = new MongoDbFlowStateStore(database.Object, Options.Create(new MongoDbDurableFlowOptions
        {
            CollectionName = "flows",
            AutoCreateIndexes = false
        }));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        Assert.Contains("TTL index", error.Message, StringComparison.Ordinal);
        Assert.Contains("expires_at_utc", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DisabledIndexCreationAndOwnershipLedger_StillVerifiesTheTtlIndex()
    {
        // Regression (round 31): the round-30 TTL verification sat behind an early-out that
        // skipped EnsureCreatedAsync entirely when BOTH AutoCreateIndexes and UseOwnershipLedger
        // were false — the natural locked-down configuration (operator-provisioned indexes, no
        // ledger writes) the check exists to protect. With both off, a collection provisioned
        // without expireAfterSeconds grew without bound again, with no error and no log line.
        var collection = new Mock<IMongoCollection<MongoFlowStateDocument>>();
        collection
            .Setup(c => c.WithReadPreference(It.IsAny<ReadPreference>()))
            .Returns(collection.Object);
        collection
            .Setup(c => c.WithWriteConcern(It.IsAny<WriteConcern>()))
            .Returns(collection.Object);

        // The provisioned collection lists ONLY the default _id index — no TTL reaper.
        var cursor = new Mock<IAsyncCursor<BsonDocument>>();
        cursor.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);
        cursor.SetupGet(c => c.Current).Returns(
        [
            new BsonDocument { ["name"] = "_id_", ["key"] = new BsonDocument("_id", 1) }
        ]);
        var indexes = new Mock<IMongoIndexManager<MongoFlowStateDocument>>();
        indexes.Setup(m => m.ListAsync(It.IsAny<CancellationToken>())).ReturnsAsync(cursor.Object);
        collection.SetupGet(c => c.Indexes).Returns(indexes.Object);

        var database = new Mock<IMongoDatabase>().WithTestNamespace();
        database
            .Setup(d => d.GetCollection<MongoFlowStateDocument>("flows", It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        using var store = new MongoDbFlowStateStore(database.Object, Options.Create(new MongoDbDurableFlowOptions
        {
            CollectionName = "flows",
            AutoCreateIndexes = false,
            UseOwnershipLedger = false
        }));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        Assert.Contains("TTL index", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AutoCreatedIndex_ConflictingWithTheSameTtlIndexUnderAnotherName_IsAccepted()
    {
        // Regression: the TTL index the docs and the AutoCreateIndexes = false error prescribe —
        // createIndex({ expires_at_utc: 1 }, { expireAfterSeconds: 0 }) — is named
        // "expires_at_utc_1". With AutoCreateIndexes left at its default, the store's CreateOne
        // under "{collection}_expires_idx" then met 85 IndexOptionsConflict ("already exists with a
        // different name"), EnsureCreated failed, and every store operation failed with it. The
        // reaper the store needs is there; accept it without dropping anything.
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 }, autoCreateIndexes: true);
        var indexes = harness.IndexesConflicting(code: 85, listed: new BsonDocument
        {
            ["name"] = "expires_at_utc_1",
            ["key"] = new BsonDocument("expires_at_utc", 1),
            ["expireAfterSeconds"] = 0
        });

        Assert.True(await harness.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        indexes.Verify(m => m.DropOneAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);

        // A conflict that is NOT an equivalent reaper still fails startup, loudly.
        using var conflicting = new MongoHarness(new BsonDocument { ["ok"] = 1 }, autoCreateIndexes: true);
        conflicting.IndexesConflicting(code: 85, listed: new BsonDocument
        {
            ["name"] = "expires_at_utc_1",
            ["key"] = new BsonDocument("expires_at_utc", 1)
        });
        Assert.Equal(85, (await Assert.ThrowsAsync<MongoCommandException>(
            () => conflicting.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)))).Code);
    }

    /// <summary>
    /// Regression (r2 S10#5): <c>LoadCurrentAsync</c> fell back to <c>LoadAsync</c> with the
    /// inherited read concern. A partitioned primary that has not yet noticed it was deposed still
    /// serves reads, and a process that can reach only it missed a majority-acknowledged write the
    /// new primary took — a breadcrumb, a status set back to Running — on exactly the paths that
    /// act on the answer with no fence behind it. It reads with linearizable read concern now,
    /// bounded like the store's writes (maxTimeMS = the 10 s default majority bound), and only
    /// the no-write paths pay for it.
    /// </summary>
    [Fact]
    public async Task LoadCurrent_ReadsLinearizably_WithTheMajorityBound()
    {
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 });
        harness.FindsNothing();
        FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>? currentOptions = null;
        harness.Current
            .Setup(item => item.FindAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoFlowStateDocument> _, FindOptions<MongoFlowStateDocument, MongoFlowStateDocument> options, CancellationToken _) => currentOptions = options)
            .ReturnsAsync(() => new MongoListCursor<MongoFlowStateDocument>([]));

        Assert.Null(await ((IFlowStateStore)harness.Store).LoadCurrentAsync("flow"));

        harness.Collection.Verify(item => item.WithReadConcern(ReadConcern.Linearizable), Times.Once);
        Assert.NotNull(currentOptions);
        Assert.Equal(TimeSpan.FromSeconds(10), currentOptions!.MaxTime);
        harness.Collection.Verify(
            item => item.FindAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Never);

        // The plain load reads with the inherited read concern, and confirms an absence with the
        // same bounded linearizable read before reporting it (see the stale-absence facts below).
        currentOptions = null;
        Assert.Null(await harness.Store.LoadAsync("flow"));
        harness.Collection.Verify(
            item => item.FindAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
        harness.VerifyLinearizableReads(Times.Exactly(2));
        Assert.Equal(TimeSpan.FromSeconds(10), currentOptions!.MaxTime);
    }

    /// <summary>
    /// Regression (round 45, F1): LoadAsync answered "absent" from a plain primary read. A primary
    /// that a partition has deposed without its noticing still serves such reads and misses a
    /// ledger the new primary created or extended — and null is the one answer callers acknowledge
    /// a wake-up on (the recovery path deleted its registration on it, consuming the response). An
    /// absence is now confirmed with the linearizable read before it is reported: the ledger the
    /// plain read missed is returned, and a set that cannot confirm fails the load instead of
    /// answering null. A load whose plain read finds the ledger pays nothing extra.
    /// </summary>
    [Fact]
    public async Task Load_APlainReadThatMissesTheLedger_ConfirmsLinearizably_BeforeReportingAbsence()
    {
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 });
        var document = LedgerDocument(PendingRun("flow", "corr"));
        harness.OnlyTheLinearizableReadFinds(document);

        var loaded = await harness.Store.LoadAsync("flow");

        Assert.NotNull(loaded);
        Assert.Equal(document.Revision, loaded!.Revision);
        Assert.Equal("corr", loaded.Steps!["step"].PendingCorrelationId);

        // A set that cannot confirm the absence (degraded, or this node was deposed) fails the load.
        var connectionId = new ConnectionId(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017)));
        harness.Current
            .Setup(item => item.FindAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new MongoExecutionTimeoutException(connectionId, "operation exceeded time limit"));
        await Assert.ThrowsAsync<MongoExecutionTimeoutException>(() => harness.Store.LoadAsync("flow"));

        // A plain read that finds the ledger never asks for the confirmation.
        harness.Collection
            .Setup(item => item.FindAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new MongoListCursor<MongoFlowStateDocument>([document]));
        harness.Current.Invocations.Clear();
        Assert.NotNull(await harness.Store.LoadAsync("flow"));
        harness.VerifyLinearizableReads(Times.Never());
    }

    /// <summary>
    /// Regression (round 45, F1), end to end: a lost subscriber's response reaches
    /// <see cref="DurableFlowExecutor.RecoverAsync"/> on a process whose plain reads miss the
    /// Running ledger that holds the matching breadcrumb. RecoverAsync used to log "no state found"
    /// and RETURN — the dispatcher reads a normal return as a settled callback and deletes the
    /// registration, so the response was gone and the pending step never checkpointed. The step is
    /// now checkpointed and the run woken; and when the absence cannot be confirmed either way, the
    /// callback throws, which leaves the registration for redelivery.
    /// </summary>
    [Fact]
    public async Task RecoverAsync_OnAStaleAbsentPlainRead_CheckpointsTheResponse_InsteadOfConsumingIt()
    {
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 });
        harness.OnlyTheLinearizableReadFinds(LedgerDocument(PendingRun("flow", "corr")));
        UpdateDefinition<MongoFlowStateDocument>? checkpoint = null;
        harness.Collection
            .Setup(item => item.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoFlowStateDocument> _, UpdateDefinition<MongoFlowStateDocument> update, UpdateOptions _, CancellationToken _) => checkpoint = update)
            .ReturnsAsync(new UpdateResult.Acknowledged(1, 1, BsonNull.Value));

        var services = new ServiceCollection();
        services.AddSingleton<IFlowStateStore>(harness.Store);
        await using var provider = services.BuildServiceProvider();
        var builder = new Mock<IAsyncResponseBuilder>();
        builder
            .Setup(instance => instance.EnqueueWorkerAsync(
                It.IsAny<System.Linq.Expressions.Expression<Func<IDurableFlowExecutor, Task>>>(),
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

        await executor.RecoverAsync("flow", new TestFlowInput(7), "corr");

        Assert.NotNull(checkpoint);
        var stages = checkpoint!.Render(new RenderArgs<MongoFlowStateDocument>(
            BsonSerializer.LookupSerializer<MongoFlowStateDocument>(), BsonSerializer.SerializerRegistry)).AsBsonArray;
        var written = FlowStateJson.Deserialize(stages[0]["$set"]["state_json"]["$literal"].AsString, "flow");
        var step = written.Steps!["step"];
        Assert.True(step.Completed);
        Assert.Null(step.PendingCorrelationId);
        Assert.NotNull(step.ResultJson);
        builder.Verify(
            instance => instance.EnqueueWorkerAsync(
                It.IsAny<System.Linq.Expressions.Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);

        // No confirmation possible: the callback fails, so the dispatcher keeps the registration.
        var connectionId = new ConnectionId(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017)));
        harness.Current
            .Setup(item => item.FindAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new MongoNotPrimaryException(connectionId, new BsonDocument("find", "flows"), new BsonDocument { ["ok"] = 0, ["code"] = 10107 }));
        await Assert.ThrowsAsync<MongoNotPrimaryException>(() => executor.RecoverAsync("flow", new TestFlowInput(7), "corr"));
    }

    private static FlowState PendingRun(string flowId, string correlationId) => new()
    {
        FlowId = flowId,
        Status = FlowRunStatus.Running,
        Revision = 3,
        Steps = new Dictionary<string, FlowStepState>
        {
            ["step"] = new() { PendingCorrelationId = correlationId, PendingPayloadTypeFullName = typeof(TestFlowInput).FullName }
        }
    };

    private static MongoFlowStateDocument LedgerDocument(FlowState state) => new()
    {
        FlowId = state.FlowId!,
        StateJson = FlowStateJson.Serialize(state),
        Revision = state.Revision,
        ExpiresAtUtc = DateTime.UtcNow.AddHours(1),
        UpdatedAtUtc = DateTime.UtcNow
    };

    /// <summary>
    /// A standalone server — supported, and with no second primary to be stale against — rejects
    /// the read concern with <c>NotAReplicaSet</c> (123). The store then reads plainly, and keeps
    /// doing so without asking again.
    /// </summary>
    [Fact]
    public async Task LoadCurrent_OnAStandalone_FallsBackToThePlainRead_AndRemembers()
    {
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 });
        harness.FindsNothing();
        harness.Current
            .Setup(item => item.FindAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new MongoCommandException(
                new ConnectionId(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017))),
                "find failed",
                new BsonDocument("find", "flows"),
                new BsonDocument { ["ok"] = 0, ["code"] = 123, ["errmsg"] = "node needs to be a replica set member to use read concern" }));

        Assert.Null(await ((IFlowStateStore)harness.Store).LoadCurrentAsync("flow"));
        Assert.Null(await ((IFlowStateStore)harness.Store).LoadCurrentAsync("flow"));
        // Nor does a plain load that finds nothing ask again to confirm the absence.
        Assert.Null(await harness.Store.LoadAsync("flow"));

        harness.Current.Verify(
            item => item.FindAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
        harness.Collection.Verify(
            item => item.FindAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Exactly(3));
    }

    /// <summary>
    /// Every load that finds no ledger now confirms the absence linearizably (round 45, F1), so a
    /// server that refuses the read concern ITSELF — a standalone, or a Mongo-compatible service
    /// without linearizable reads (Amazon DocumentDB answers with its own "unsupported" error) —
    /// would otherwise fail each one, a child flow's first start included. Such a refusal is a fixed
    /// answer: the store reads plainly and remembers.
    /// </summary>
    [Theory]
    [InlineData(123, "node needs to be a replica set member to use read concern")]
    [InlineData(72, "readConcern level not supported")]
    [InlineData(115, "command not supported")]
    [InlineData(238, "not implemented")]
    [InlineData(2, "Unsupported read concern level: linearizable")]
    public async Task Load_OnAServerThatRefusesTheReadConcern_ReadsPlainly_AndRemembers(int code, string message)
    {
        var logger = new CollectingLogger();
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 }, logger: logger.For<MongoDbFlowStateStore>());
        harness.FindsNothing();
        harness.Current
            .Setup(item => item.FindAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(CommandFailure(code, message));

        Assert.Null(await harness.Store.LoadAsync("flow"));
        Assert.Null(await harness.Store.LoadAsync("flow"));
        Assert.Null(await ((IFlowStateStore)harness.Store).LoadCurrentAsync("flow"));

        harness.VerifyLinearizableReads(Times.Once());
        // A permanent, process-wide downgrade of the check: said once.
        Assert.Single(logger.Messages, entry => entry.Contains("refused the linearizable read concern", StringComparison.Ordinal));
    }

    /// <summary>
    /// The other side of the fact above: what a replica set answers while it CANNOT confirm — a
    /// step-down, a recovering node, an exceeded time limit, majority reads not available yet — is
    /// never taken for "unsupported". It fails the load (null would acknowledge a wake-up on a
    /// ledger that may exist) and is not remembered: the next load asks again.
    /// </summary>
    [Theory]
    [MemberData(nameof(TransientLinearizableFailures))]
    public async Task Load_ATransientLinearizableFailure_FailsTheLoad_AndIsNotRemembered(Exception failure)
    {
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 });
        harness.FindsNothing();
        harness.Current
            .Setup(item => item.FindAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(failure);

        await Assert.ThrowsAsync(failure.GetType(), () => harness.Store.LoadAsync("flow"));
        await Assert.ThrowsAsync(failure.GetType(), () => harness.Store.LoadAsync("flow"));

        harness.VerifyLinearizableReads(Times.Exactly(2));
    }

    public static TheoryData<Exception> TransientLinearizableFailures()
    {
        var connectionId = new ConnectionId(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017)));
        var command = new BsonDocument("find", "flows");
        return
        [
            new MongoNotPrimaryException(connectionId, command, new BsonDocument { ["ok"] = 0, ["code"] = 10107, ["errmsg"] = "not primary; cannot satisfy linearizable read concern" }),
            new MongoNodeIsRecoveringException(connectionId, command, new BsonDocument { ["ok"] = 0, ["code"] = 11602, ["errmsg"] = "operation was interrupted" }),
            new MongoExecutionTimeoutException(connectionId, "operation exceeded time limit"),
            CommandFailure(134, "Read concern majority reads are currently not possible; linearizable read failed"),
            CommandFailure(262, "linearizable read exceeded time limit"),
            // MongoDB's own "could not confirm" answer (LinearizableReadConcernError) mentions the
            // level without refusing it, as does a parse error about an incompatible option —
            // neither is an "unsupported" answer (precommit critic, round 45).
            CommandFailure(187, "Failed to confirm that read was linearizable."),
            CommandFailure(9, "afterOpTime not compatible with linearizable read concern")
        ];
    }

    private static MongoCommandException CommandFailure(int code, string message)
        => new(
            new ConnectionId(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017))),
            "find failed",
            new BsonDocument("find", "flows"),
            new BsonDocument { ["ok"] = 0, ["code"] = code, ["errmsg"] = message });

    /// <summary>
    /// Regression (r2 GS6#2): nothing checked the collection's collation, while the docs promise
    /// the built-in stores refuse a folding one. A collection created with a default collation
    /// builds its <c>_id_</c> index — the flow id — under it, so two case-variant ids collide: the
    /// second run's create sees the first run's ledger, its start job dead-letters on the
    /// unreadable load, and a delete removes the other run. Startup refuses it now, on both index
    /// branches, from the index listing (no listCollections privilege needed); a simple collation
    /// passes.
    /// </summary>
    [Theory]
    [InlineData(false, "en", true)]
    [InlineData(true, "en", true)]
    [InlineData(false, "simple", false)]
    [InlineData(true, null, false)]
    public async Task EnsureCreated_RefusesAFoldingIdCollation(bool autoCreateIndexes, string? locale, bool refused)
    {
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 }, autoCreateIndexes);
        var idIndex = new BsonDocument { ["v"] = 2, ["key"] = new BsonDocument("_id", 1), ["name"] = "_id_" };
        if (locale is not null)
            idIndex["collation"] = new BsonDocument { ["locale"] = locale, ["strength"] = 2 };
        harness.Collection.WithListedIndexes(
            idIndex,
            new BsonDocument { ["name"] = "flows_expires_idx", ["key"] = new BsonDocument("expires_at_utc", 1), ["expireAfterSeconds"] = 0 });

        var create = harness.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5));

        if (!refused)
        {
            Assert.True(await create);
            return;
        }

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => create);
        Assert.Contains("collation", error.Message, StringComparison.Ordinal);
        Assert.Contains("'flows'", error.Message, StringComparison.Ordinal);
        Assert.Null(harness.Inserted);
    }

    [Fact]
    public async Task Load_ADocumentWhoseExpiryIsMissingOrNotADate_IsUnreadable_NotAbsent()
    {
        // Regression: a missing, string or numeric expires_at_utc sorts below any date, so the
        // `$gt $$NOW` live filter missed the document and LoadAsync answered "absent" — the one
        // answer the executor acknowledges a wake-up on — while the TTL monitor, which reaps only
        // dates, never removes it. A live-filter miss now asks, by id alone, whether such a
        // document is there.
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 });
        harness.FindsNothing();
        harness.Collection
            .Setup(item => item.CountDocumentsAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<CountOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);

        var unreadable = await Assert.ThrowsAsync<FlowStateUnreadableException>(() => harness.Store.LoadAsync("flow"));
        Assert.Equal(MongoDbFlowStateStore.MalformedExpiryReason("flows"), unreadable.Reason);

        // Nothing under the id at all: genuinely absent.
        harness.Collection
            .Setup(item => item.CountDocumentsAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<CountOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(0);
        Assert.Null(await harness.Store.LoadAsync("flow"));
    }

    [Fact]
    public async Task TryCreate_WhenTheCollidingLedgerExpiredDuringTheClockRead_ReplacesItOnASecondLook()
    {
        // Regression (store half of the lost-start window): step 1 found the old ledger still
        // live, it expired during the `hello` round trip, and the insert's duplicate key was
        // answered "exists" — the executor then loaded an absent ledger and acknowledged the
        // start job, so the new run was never created. The replace is tried once more first.
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 });
        harness.Collection
            .SetupSequence(item => item.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new UpdateResult.Acknowledged(0, 0, BsonNull.Value))
            .ReturnsAsync(new UpdateResult.Acknowledged(1, 1, BsonNull.Value));
        harness.Collection
            .Setup(item => item.InsertOneAsync(
                It.IsAny<MongoFlowStateDocument>(),
                It.IsAny<InsertOneOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(DuplicateKey());

        Assert.True(await harness.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));

        // A live owner still wins: the second look matches nothing either.
        using var owned = new MongoHarness(new BsonDocument { ["ok"] = 1 });
        owned.Collection
            .Setup(item => item.InsertOneAsync(
                It.IsAny<MongoFlowStateDocument>(),
                It.IsAny<InsertOneOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(DuplicateKey());
        Assert.False(await owned.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        owned.Collection.Verify(
            item => item.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    [Fact]
    public void LedgerDates_ArePinnedToBsonDates_WhateverDateTimeSerializerTheHostRegistered()
    {
        // Regression: the date fields used the process-wide DateTime serializer, so a host that
        // registered `new DateTimeSerializer(BsonType.String)` made every insert write a string
        // expiry — which never compares above $$NOW, so each new ledger read as absent and was
        // never reaped. The members carry their own serializer instead of asking the registry
        // (the process-wide case itself runs in an isolated load context, below).
        var classMap = BsonClassMap.LookupClassMap(typeof(MongoFlowStateDocument));
        Assert.IsType<UtcBsonDateSerializer>(classMap.GetMemberMap(nameof(MongoFlowStateDocument.ExpiresAtUtc)).GetSerializer());
        Assert.IsType<UtcBsonDateSerializer>(classMap.GetMemberMap(nameof(MongoFlowStateDocument.UpdatedAtUtc)).GetSerializer());
        Assert.IsType<NullableUtcBsonDateSerializer>(classMap.GetMemberMap(nameof(MongoFlowStateDocument.LeaseExpiresAtUtc)).GetSerializer());

        var now = new DateTime(2031, 3, 14, 9, 26, 53, 589, DateTimeKind.Utc);
        var document = new MongoFlowStateDocument { FlowId = "flow", ExpiresAtUtc = now, UpdatedAtUtc = now, LeaseExpiresAtUtc = now, Revision = 0 }
            .ToBsonDocument();
        Assert.Equal(BsonType.DateTime, document["expires_at_utc"].BsonType);
        Assert.Equal(BsonType.DateTime, document["updated_at_utc"].BsonType);
        Assert.Equal(BsonType.DateTime, document["lease_expires_at_utc"].BsonType);

        var read = BsonSerializer.Deserialize<MongoFlowStateDocument>(document);
        Assert.Equal(DateTimeKind.Utc, read.ExpiresAtUtc.Kind);
        Assert.Equal(now, read.ExpiresAtUtc);
        Assert.Equal(now, read.LeaseExpiresAtUtc);
        document.Remove("lease_expires_at_utc");
        Assert.Null(BsonSerializer.Deserialize<MongoFlowStateDocument>(document).LeaseExpiresAtUtc);
    }

    [Fact]
    public void LedgerDates_UnderAHostRegisteredCustomDateTimeSerializer_StillMapAndWriteBsonDates()
    {
        // Regression: the round that pinned the dates did it with [BsonDateTimeOptions], which does
        // not replace the member's serializer but RECONFIGURES whatever the global registry returns
        // for DateTime — and throws NotSupportedException ("… is not configurable using an attribute
        // of type BsonDateTimeOptionsAttribute") for anything but the driver's DateTimeSerializer.
        // A host with its own IBsonSerializer<DateTime> then failed the ledger's class map on first
        // use: every flow-store operation threw. The registry and class maps are process-wide and
        // cannot be reset, so the host's registration happens against PRIVATE copies of the driver
        // and the store, in their own load context — nothing leaks into the rest of the run.
        var context = new IsolatedBsonLoadContext();
        try
        {
            var probe = context.LoadFromAssemblyPath(typeof(MongoDbFlowStateStoreTests).Assembly.Location)
                .GetType(typeof(HostRegisteredDateSerializerProbe).FullName!, throwOnError: true)!
                .GetMethod(nameof(HostRegisteredDateSerializerProbe.Run), BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)!;
            var observed = (string[])probe.Invoke(null, BindingFlags.DoNotWrapExceptions, binder: null, parameters: null, culture: null)!;

            // The host's serializer really is what the (private) registry answers for DateTime…
            Assert.Equal(nameof(HostRegisteredDateSerializerProbe.StringDateSerializer), observed[0]);
            // …and the ledger still maps, writes BSON dates, and reads them back as UTC.
            Assert.Equal(["DateTime", "DateTime", "DateTime", "True"], observed[1..]);
        }
        finally
        {
            context.Unload();
        }

        // The registration stayed inside the isolated context.
        Assert.IsType<DateTimeSerializer>(BsonSerializer.LookupSerializer<DateTime>());
    }

    [Fact]
    public void MalformedExpiryReason_CarriesTheCleanupThatFreesTheId()
    {
        // Documents written by the old date bug (a non-date expires_at_utc under a host-wide
        // String/Document/Int64 DateTime representation) used to be replaced by the next create;
        // now nothing ever removes them, so the reason has to tell the operator how to.
        var reason = MongoDbFlowStateStore.MalformedExpiryReason("flows");

        Assert.Contains("expires_at_utc", reason, StringComparison.Ordinal);
        Assert.Contains("""db.getCollection("flows").deleteMany({ expires_at_utc: { $not: { $type: "date" } } })""", reason, StringComparison.Ordinal);
    }

    private static MongoWriteException DuplicateKey()
    {
        var connectionId = new ConnectionId(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017)));
        var writeError = (WriteError)Activator.CreateInstance(
            typeof(WriteError),
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            [ServerErrorCategory.DuplicateKey, 11000, "E11000 duplicate key error", new BsonDocument()],
            culture: null)!;
        return new MongoWriteException(connectionId, writeError, writeConcernError: null, innerException: null);
    }

    private sealed class MongoHarness : IDisposable
    {
        public MongoHarness(BsonDocument helloReply, bool autoCreateIndexes = false, Microsoft.Extensions.Logging.ILogger<MongoDbFlowStateStore>? logger = null)
        {
            Database
                .Setup(item => item.GetCollection<MongoFlowStateDocument>("flows", It.IsAny<MongoCollectionSettings>()))
                .Returns(Collection.Object);
            // The store pins ledger reads to the primary at construction; the derived handle is
            // this same mock. The TTL-index stub satisfies the operator-schema verification that
            // AutoCreateIndexes = false now performs.
            Collection
                .Setup(item => item.WithReadPreference(It.IsAny<ReadPreference>()))
                .Returns(Collection.Object);
            Collection
                .Setup(item => item.WithWriteConcern(It.IsAny<WriteConcern>()))
                .Returns(Collection.Object);
            // LoadCurrentAsync's linearizable-read handle.
            Collection
                .Setup(item => item.WithReadConcern(It.IsAny<ReadConcern>()))
                .Returns(Current.Object);
            Collection.WithProvisionedTtlIndex();
            Database
                .Setup(item => item.RunCommandAsync(
                    It.IsAny<Command<BsonDocument>>(),
                    It.IsAny<ReadPreference>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(helloReply);
            // Step 1 (expired-ledger replace) matches nothing: the id is free, forcing the insert.
            Collection
                .Setup(item => item.UpdateOneAsync(
                    It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                    It.IsAny<UpdateDefinition<MongoFlowStateDocument>>(),
                    It.IsAny<UpdateOptions>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new UpdateResult.Acknowledged(0, 0, BsonNull.Value));
            Collection
                .Setup(item => item.InsertOneAsync(
                    It.IsAny<MongoFlowStateDocument>(),
                    It.IsAny<InsertOneOptions>(),
                    It.IsAny<CancellationToken>()))
                .Callback((MongoFlowStateDocument document, InsertOneOptions _, CancellationToken _) => Inserted = document)
                .Returns(Task.CompletedTask);
            Database.WithTestNamespace();
            Store = new MongoDbFlowStateStore(Database.Object, Options.Create(new MongoDbDurableFlowOptions
            {
                CollectionName = "flows",
                AutoCreateIndexes = autoCreateIndexes
            }), ownedClient: null, namespaceRegistry: null, logger);
        }

        /// <summary>CreateOne fails with <paramref name="code"/>; the listing then shows <paramref name="listed"/>.</summary>
        public Mock<IMongoIndexManager<MongoFlowStateDocument>> IndexesConflicting(int code, BsonDocument listed)
        {
            var cursor = new Mock<IAsyncCursor<BsonDocument>>();
            cursor.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(true)
                .ReturnsAsync(false);
            cursor.SetupGet(c => c.Current).Returns([new BsonDocument { ["name"] = "_id_", ["key"] = new BsonDocument("_id", 1) }, listed]);
            var indexes = new Mock<IMongoIndexManager<MongoFlowStateDocument>>();
            indexes.Setup(m => m.ListAsync(It.IsAny<CancellationToken>())).ReturnsAsync(cursor.Object);
            indexes
                .Setup(m => m.CreateOneAsync(
                    It.IsAny<CreateIndexModel<MongoFlowStateDocument>>(),
                    It.IsAny<CreateOneIndexOptions>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new MongoCommandException(
                    new ConnectionId(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017))),
                    "createIndexes failed",
                    new BsonDocument("createIndexes", "flows"),
                    new BsonDocument { ["ok"] = 0, ["code"] = code, ["errmsg"] = "Index already exists with a different name" }));
            Collection.SetupGet(c => c.Indexes).Returns(indexes.Object);
            return indexes;
        }

        /// <summary>The live-filter read finds no document — the plain read and the linearizable one alike.</summary>
        public void FindsNothing()
        {
            var cursor = new Mock<IAsyncCursor<MongoFlowStateDocument>>();
            cursor.Setup(c => c.MoveNextAsync(It.IsAny<CancellationToken>())).ReturnsAsync(false);
            Collection
                .Setup(item => item.FindAsync(
                    It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                    It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(cursor.Object);
            Current
                .Setup(item => item.FindAsync(
                    It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                    It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(cursor.Object);
        }

        /// <summary>The linearizable read — and only it — finds <paramref name="document"/>.</summary>
        public void OnlyTheLinearizableReadFinds(MongoFlowStateDocument document)
        {
            FindsNothing();
            Current
                .Setup(item => item.FindAsync(
                    It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                    It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => new MongoListCursor<MongoFlowStateDocument>([document]));
        }

        public void VerifyLinearizableReads(Times times)
            => Current.Verify(
                item => item.FindAsync(
                    It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                    It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                    It.IsAny<CancellationToken>()),
                times);

        public Mock<IMongoDatabase> Database { get; } = new();
        public Mock<IMongoCollection<MongoFlowStateDocument>> Collection { get; } = new();
        public Mock<IMongoCollection<MongoFlowStateDocument>> Current { get; } = new();
        public MongoDbFlowStateStore Store { get; }
        public MongoFlowStateDocument? Inserted { get; private set; }

        public void Dispose() => Store.Dispose();
    }
}

/// <summary>
/// Private copies of the MongoDB driver and the Mongo flow store (their process-wide serializer
/// registry and class maps included); the runtime and everything else is shared with the default
/// context.
/// </summary>
internal sealed class IsolatedBsonLoadContext() : System.Runtime.Loader.AssemblyLoadContext($"asyncresponse-bson-{Guid.NewGuid():N}", isCollectible: true)
{
    private static readonly string Directory = Path.GetDirectoryName(typeof(IsolatedBsonLoadContext).Assembly.Location)!;
    private static readonly string StoreAssembly = typeof(MongoDbFlowStateStore).Assembly.GetName().Name!;

    protected override Assembly? Load(AssemblyName assemblyName)
    {
        var name = assemblyName.Name!;
        if (!name.StartsWith("MongoDB.", StringComparison.Ordinal) && name != StoreAssembly)
            return null;

        var path = Path.Combine(Directory, name + ".dll");
        return File.Exists(path) ? LoadFromAssemblyPath(path) : null;
    }
}

/// <summary>
/// Runs inside <see cref="IsolatedBsonLoadContext"/>: registers a host-wide DateTime serializer
/// that is NOT the driver's <see cref="DateTimeSerializer"/> (and writes strings), then maps,
/// writes, and reads a ledger document. Returns plain strings so nothing crosses the context
/// boundary as a driver type.
/// </summary>
internal static class HostRegisteredDateSerializerProbe
{
    public static string[] Run()
    {
        BsonSerializer.RegisterSerializer(typeof(DateTime), new StringDateSerializer());

        var now = new DateTime(2031, 3, 14, 9, 26, 53, 589, DateTimeKind.Utc);
        var document = new MongoFlowStateDocument { FlowId = "flow", ExpiresAtUtc = now, UpdatedAtUtc = now, LeaseExpiresAtUtc = now, Revision = 0 }
            .ToBsonDocument();
        var read = BsonSerializer.Deserialize<MongoFlowStateDocument>(document);

        return
        [
            BsonSerializer.LookupSerializer<DateTime>().GetType().Name,
            document["expires_at_utc"].BsonType.ToString(),
            document["updated_at_utc"].BsonType.ToString(),
            document["lease_expires_at_utc"].BsonType.ToString(),
            (read.ExpiresAtUtc == now && read.ExpiresAtUtc.Kind == DateTimeKind.Utc && read.LeaseExpiresAtUtc == now).ToString()
        ];
    }

    /// <summary>A host's own DateTime serializer, of the kind that wrote string expiries.</summary>
    internal sealed class StringDateSerializer : SerializerBase<DateTime>
    {
        public override DateTime Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
            => DateTime.Parse(context.Reader.ReadString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, DateTime value)
            => context.Writer.WriteString(value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture));
    }
}
