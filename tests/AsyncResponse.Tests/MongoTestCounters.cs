using MongoDB.Bson;
using MongoDB.Driver;
using Moq;

namespace AsyncResponse.Tests;

/// <summary>
/// Counters-collection mock for Mongo store fixtures: the channel store draws its monotonic ack
/// sequence from <c>{messages}_counters</c> via <c>findOneAndUpdate</c> (the delivery claim's
/// <c>$inc</c>, the registration's stamping pipeline), so any fixture whose database mock leaves
/// that collection unset NREs on the first delivery claim or waiter registration. The mock hands
/// out a process-local increasing sequence plus the <c>drawn_at</c> server stamp the registration
/// draw reads, which is exactly the contract the store needs.
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
                ["seq"] = Interlocked.Increment(ref seq),
                ["drawn_at"] = new BsonDateTime(DateTime.UtcNow)
            });
        return counters.Object;
    }

    /// <summary>Registers the counters collection on a database mock, matching any collection name.</summary>
    public static Mock<IMongoDatabase> WithCounters(this Mock<IMongoDatabase> database)
    {
        // After WithTestNamespace so this counters collection overrides its generic
        // BsonDocument catch-all (Moq: the last matching setup wins).
        database.WithTestNamespace();
        database
            .Setup(d => d.GetCollection<BsonDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(Collection());
        return database;
    }

    /// <summary>
    /// Gives a database mock a real <see cref="DatabaseNamespace"/>, a client with cluster
    /// settings, and a generic BsonDocument collection: the stores validate the effective
    /// namespace byte length at construction, DI-hosted stores derive a cluster key from
    /// <c>Client.Settings.Servers</c>, and EnsureCreated upserts into the persisted ownership
    /// ledger — a loose mock NREs on any of these before the behavior under test runs. The
    /// generic collection answers the ledger upsert with a document carrying no ownership
    /// fields, which the ledger treats as unowned. Tests that need their own client re-setup
    /// <c>Client</c> afterwards (last setup wins).
    /// </summary>
    public static Mock<IMongoDatabase> WithTestNamespace(this Mock<IMongoDatabase> database, string name = "tests")
    {
        database.SetupGet(d => d.DatabaseNamespace).Returns(new DatabaseNamespace(name));
        var client = new Mock<IMongoClient>();
        client.SetupGet(c => c.Settings).Returns(MongoClientSettings.FromConnectionString("mongodb://localhost:27017"));
        database.SetupGet(d => d.Client).Returns(client.Object);
        database
            .Setup(d => d.GetCollection<BsonDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(Collection());
        return database;
    }
}
