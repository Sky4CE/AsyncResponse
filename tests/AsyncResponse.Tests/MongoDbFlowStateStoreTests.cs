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

    private sealed class MongoHarness : IDisposable
    {
        public MongoHarness(BsonDocument helloReply)
        {
            Database
                .Setup(item => item.GetCollection<MongoFlowStateDocument>("flows", It.IsAny<MongoCollectionSettings>()))
                .Returns(Collection.Object);
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
