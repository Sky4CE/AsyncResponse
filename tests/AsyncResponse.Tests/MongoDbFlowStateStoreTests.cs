using AsyncResponse.DurableFlows.MongoDB;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
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

    private sealed class MongoHarness : IDisposable
    {
        public MongoHarness(BsonDocument helloReply)
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
                AutoCreateIndexes = false
            }));
        }

        public Mock<IMongoDatabase> Database { get; } = new();
        public Mock<IMongoCollection<MongoFlowStateDocument>> Collection { get; } = new();
        public MongoDbFlowStateStore Store { get; }
        public MongoFlowStateDocument? Inserted { get; private set; }

        public void Dispose() => Store.Dispose();
    }
}
