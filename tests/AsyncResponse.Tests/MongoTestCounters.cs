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
    /// Gives a database mock a real <see cref="DatabaseNamespace"/> and a client with cluster
    /// settings: the stores validate the effective "database.collection" namespace byte length
    /// at construction, and DI-hosted stores additionally derive a cluster key from
    /// <c>Client.Settings.Servers</c> for the cross-component ownership ledger — a loose mock
    /// NREs on either before any operation runs. Tests that need their own client re-setup
    /// <c>Client</c> afterwards (last setup wins).
    /// </summary>
    public static Mock<IMongoDatabase> WithTestNamespace(this Mock<IMongoDatabase> database, string name = "tests")
    {
        database.SetupGet(d => d.DatabaseNamespace).Returns(new DatabaseNamespace(name));
        var client = new Mock<IMongoClient>();
        client.SetupGet(c => c.Settings).Returns(MongoClientSettings.FromConnectionString("mongodb://localhost:27017"));
        database.SetupGet(d => d.Client).Returns(client.Object);
        return database;
    }
}
