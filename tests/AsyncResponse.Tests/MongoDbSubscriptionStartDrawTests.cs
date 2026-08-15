using AsyncResponse.Channels.MongoDB;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The MongoDB subscription-start watermark draw. PG and SQL Server read the server clock and the
/// next ack-sequence value in ONE statement, so nothing can interleave; Mongo used to draw them in
/// two round trips (<c>hello</c>, then the counter <c>findOneAndUpdate</c>), and a delivery claim
/// landing between them could pair a same-millisecond <c>acked_at</c> with a lower sequence —
/// the same-tick tie-breaker then filed a legitimate fan-out delivery as history.
/// </summary>
public sealed class MongoDbSubscriptionStartDrawTests
{
    private static (MongoDbChannelStore Store, Mock<IMongoDatabase> Database, Mock<IMongoCollection<BsonDocument>> Counters) CreateStore(
        BsonDocument counterDocument,
        DateTime helloTime,
        Action<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>? captureOptions = null)
    {
        var options = new MongoDbAsyncResponseChannelOptions { AutoCreateIndexes = false };
        var counters = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose);
        counters
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<BsonDocument> _, UpdateDefinition<BsonDocument> _, FindOneAndUpdateOptions<BsonDocument, BsonDocument> o, CancellationToken _) =>
                captureOptions?.Invoke(o))
            .ReturnsAsync(counterDocument);
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose);
        database.SetupGet(d => d.DatabaseNamespace).Returns(new DatabaseNamespace("tests"));
        database
            .Setup(d => d.GetCollection<BsonDocument>(
                MongoDbChannelStore.CountersCollectionName(options.MessageCollection),
                It.IsAny<MongoCollectionSettings>()))
            .Returns(counters.Object);
        // The hello round-trip answers with a DIFFERENT clock, so an implementation that still
        // consults it is caught by value, not just by call count.
        database
            .Setup(d => d.RunCommandAsync(
                It.IsAny<Command<BsonDocument>>(),
                It.IsAny<ReadPreference>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new BsonDocument("localTime", new BsonDateTime(helloTime)));
        return (new MongoDbChannelStore(database.Object, Options.Create(options)), database, counters);
    }

    [Fact]
    public async Task GetSubscriptionStart_DrawsClockAndSequenceInOneAtomicCounterUpdate()
    {
        var drawnAt = new DateTime(2026, 8, 15, 12, 0, 0, DateTimeKind.Utc);
        FindOneAndUpdateOptions<BsonDocument, BsonDocument>? capturedOptions = null;
        var (store, database, counters) = CreateStore(
            new BsonDocument { ["_id"] = "ack_seq", ["seq"] = 42L, ["drawn_at"] = new BsonDateTime(drawnAt) },
            helloTime: drawnAt.AddMinutes(7),
            options => capturedOptions = options);

        var (serverTimeUtc, startSeq) = await store.GetSubscriptionStartAsync(CancellationToken.None);

        // Both values come out of the single returned counter document.
        Assert.Equal(42L, startSeq);
        Assert.Equal(new DateTimeOffset(drawnAt, TimeSpan.Zero), serverTimeUtc);

        // One command, atomically: the separate server-clock read must not run at all.
        database.Verify(
            d => d.RunCommandAsync(
                It.IsAny<Command<BsonDocument>>(),
                It.IsAny<ReadPreference>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
        counters.Verify(
            c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
        Assert.NotNull(capturedOptions);
        Assert.True(capturedOptions!.IsUpsert);
        Assert.Equal(ReturnDocument.After, capturedOptions.ReturnDocument);
    }

    [Fact]
    public async Task GetSubscriptionStart_FailsInsteadOfFabricatingAnAppClockWatermark()
    {
        // A counter document missing the server stamp is a driver anomaly. Substituting the app
        // clock would silently feed a client clock into the server-clock watermark, so the draw
        // must fail and let the caller's error path run.
        var (store, _, _) = CreateStore(
            new BsonDocument { ["_id"] = "ack_seq", ["seq"] = 1L },
            helloTime: DateTime.UtcNow);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.GetSubscriptionStartAsync(CancellationToken.None));
    }
}
