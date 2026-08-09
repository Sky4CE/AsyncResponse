using MongoDB.Bson;
using MongoDB.Driver;
using Moq;

namespace AsyncResponse.Tests;

/// <summary>
/// Counters-collection mock for Mongo store fixtures: the channel store draws its monotonic ack
/// sequence from <c>{messages}_counters</c> via <c>findOneAndUpdate($inc)</c>, so any fixture
/// whose database mock leaves that collection unset NREs on the first delivery claim or waiter
/// registration. The mock hands out a process-local increasing sequence, which is exactly the
/// contract the store needs.
/// </summary>
internal static class MongoTestCounters
{
    public static IMongoCollection<BsonDocument> Collection()
    {
        var counters = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose);
        var seq = 0L;
        counters
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new BsonDocument
            {
                ["_id"] = "ack_seq",
                ["seq"] = Interlocked.Increment(ref seq)
            });
        return counters.Object;
    }

    /// <summary>Registers the counters collection on a database mock, matching any collection name.</summary>
    public static Mock<IMongoDatabase> WithCounters(this Mock<IMongoDatabase> database)
    {
        database
            .Setup(d => d.GetCollection<BsonDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(Collection());
        return database.WithTestNamespace();
    }

    /// <summary>
    /// Gives a database mock a real <see cref="DatabaseNamespace"/>: the stores validate the
    /// effective "database.collection" namespace byte length at construction, so a loose mock
    /// with a null namespace NREs before any operation runs.
    /// </summary>
    public static Mock<IMongoDatabase> WithTestNamespace(this Mock<IMongoDatabase> database, string name = "tests")
    {
        database.SetupGet(d => d.DatabaseNamespace).Returns(new DatabaseNamespace(name));
        return database;
    }
}
