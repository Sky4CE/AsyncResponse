using System.Net;
using System.Reflection;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;
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
    /// <summary>
    /// Stubs the driver's fluent <c>WithReadPreference</c>, <c>WithWriteConcern</c> and
    /// <c>WithReadConcern</c> to return the mock itself. The stores pin <c>ReadPreference.Primary</c>
    /// and a write concern on every collection handle at construction (the flow store also derives
    /// a linearizable-read handle), so a loose mock returning null there would replace the test's
    /// stubbed collection with null.
    /// </summary>
    public static Mock<IMongoCollection<T>> SelfPinning<T>(this Mock<IMongoCollection<T>> collection)
    {
        collection
            .Setup(c => c.WithReadPreference(It.IsAny<ReadPreference>()))
            .Returns(collection.Object);
        collection
            .Setup(c => c.WithWriteConcern(It.IsAny<WriteConcern>()))
            .Returns(collection.Object);
        collection
            .Setup(c => c.WithReadConcern(It.IsAny<ReadConcern>()))
            .Returns(collection.Object);
        return collection;
    }

    /// <summary>Answers every <c>Find</c> projecting to <typeparamref name="TProjection"/> with <paramref name="results"/>.</summary>
    public static Mock<IMongoCollection<T>> FindsReturning<T, TProjection>(this Mock<IMongoCollection<T>> collection, params TProjection[] results)
    {
        collection
            .Setup(c => c.FindAsync(
                It.IsAny<FilterDefinition<T>>(),
                It.IsAny<FindOptions<T, TProjection>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new MongoListCursor<TProjection>(results));
        return collection;
    }

    /// <summary>
    /// Stubs the UNTYPED handle the MongoDB transport store claims through (it reads the stamped
    /// document as raw BSON and maps it itself, so an unreadable document is buried instead of
    /// throwing inside findOneAndUpdate). Register AFTER <see cref="WithTestNamespace"/>: this
    /// name-specific setup must override its BsonDocument catch-all.
    /// </summary>
    public static Mock<IMongoCollection<BsonDocument>> WithRawTransportMessages(
        this Mock<IMongoDatabase> database,
        string collectionName = "asyncresponse_transport_messages")
    {
        var raw = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        database
            .Setup(d => d.GetCollection<BsonDocument>(collectionName, It.IsAny<MongoCollectionSettings>()))
            .Returns(raw.Object);
        return raw;
    }

    /// <summary>Answers successive transport claims with <paramref name="documents"/> (<c>null</c> = queue empty).</summary>
    public static Mock<IMongoCollection<BsonDocument>> ClaimsInOrder(
        this Mock<IMongoCollection<BsonDocument>> raw,
        params BsonDocument?[] documents)
    {
        var sequence = raw.SetupSequence(c => c.FindOneAndUpdateAsync(
            It.IsAny<FilterDefinition<BsonDocument>>(),
            It.IsAny<UpdateDefinition<BsonDocument>>(),
            It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
            It.IsAny<CancellationToken>()));
        foreach (var document in documents)
            sequence = sequence.ReturnsAsync(document!);
        return raw;
    }

    /// <summary>
    /// Stubs <c>GetCollection&lt;T&gt;</c> with a fresh self-pinning loose mock and returns that
    /// mock, for collections the store constructor touches but the test never exercises.
    /// </summary>
    public static Mock<IMongoCollection<T>> WithLooseCollection<T>(this Mock<IMongoDatabase> database)
    {
        var collection = new Mock<IMongoCollection<T>>(MockBehavior.Loose).SelfPinning();
        database
            .Setup(db => db.GetCollection<T>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        return collection;
    }

    public static IMongoCollection<BsonDocument> Collection()
    {
        var counters = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
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

    /// <summary>
    /// Stubs the TTL-index listing the flow store verifies under <c>AutoCreateIndexes = false</c>:
    /// one single-field index on <c>expires_at_utc</c> with <c>expireAfterSeconds</c> — a
    /// correctly provisioned reaper, so mocked stores pass the operator-schema check.
    /// </summary>
    public static Mock<IMongoCollection<T>> WithProvisionedTtlIndex<T>(this Mock<IMongoCollection<T>> collection)
        => collection.WithListedIndexes(new BsonDocument
        {
            ["name"] = "flows_expires_idx",
            ["key"] = new BsonDocument("expires_at_utc", 1),
            ["expireAfterSeconds"] = 0
        });

    /// <summary>Stubs the collection's index listing with <paramref name="indexes"/>, answering every listing call.</summary>
    public static Mock<IMongoCollection<T>> WithListedIndexes<T>(this Mock<IMongoCollection<T>> collection, params BsonDocument[] indexes)
    {
        var manager = new Mock<IMongoIndexManager<T>>();
        manager
            .Setup(m => m.ListAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new MongoListCursor<BsonDocument>(indexes));
        collection.SetupGet(c => c.Indexes).Returns(manager.Object);
        return collection;
    }
}

/// <summary>A driver cursor over a fixed list: one batch, then the end.</summary>
internal sealed class MongoListCursor<T>(IEnumerable<T> items) : IAsyncCursor<T>
{
    private bool _moved;

    public IEnumerable<T> Current { get; private set; } = [];

    public bool MoveNext(CancellationToken cancellationToken = default)
    {
        if (_moved)
            return false;
        _moved = true;
        Current = items;
        return true;
    }

    public Task<bool> MoveNextAsync(CancellationToken cancellationToken = default) => Task.FromResult(MoveNext(cancellationToken));

    public void Dispose()
    {
    }
}

/// <summary>
/// The shapes a lapsed majority <c>wtimeout</c> reaches a store in (MongoDB.Driver 3.12): a
/// command write (<c>findAndModify</c>) throws <see cref="MongoWriteConcernException"/> before
/// reading its reply; a CRUD write goes through the bulk path and throws
/// <see cref="MongoWriteException"/> with a write-concern error and no write error.
/// </summary>
internal static class MongoReplicationTimeouts
{
    public static readonly ConnectionId Connection = new(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017)));

    public static BsonDocument WriteConcernErrorDocument(int code = 64, bool wtimeout = true) => new()
    {
        ["code"] = code,
        ["codeName"] = code == 64 ? "WriteConcernFailed" : "UnsatisfiableWriteConcern",
        ["errmsg"] = "waiting for replication timed out",
        ["errInfo"] = wtimeout ? new BsonDocument("wtimeout", true) : new BsonDocument()
    };

    /// <summary>What a <c>findAndModify</c> whose replication wait lapsed throws.</summary>
    public static MongoWriteConcernException Command(int code = 64, bool wtimeout = true)
        => new(Connection, "waiting for replication timed out", new WriteConcernResult(new BsonDocument
        {
            ["ok"] = 1,
            ["n"] = 1,
            ["writeConcernError"] = WriteConcernErrorDocument(code, wtimeout)
        }));

    /// <summary>What an <c>updateOne</c>/<c>deleteOne</c> whose replication wait lapsed throws.</summary>
    public static MongoWriteException Write(int code = 64, WriteError? writeError = null, bool wtimeout = true)
        => new(Connection, writeError, WriteConcernError(code, wtimeout), innerException: null);

    public static WriteConcernError WriteConcernError(int code = 64, bool wtimeout = true)
    {
        var details = WriteConcernErrorDocument(code, wtimeout);
        return (WriteConcernError)Activator.CreateInstance(
            typeof(WriteConcernError),
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            [code, details["codeName"].AsString, details["errmsg"].AsString, details["errInfo"].AsBsonDocument, Array.Empty<string>()],
            culture: null)!;
    }

    public static WriteError DuplicateKeyError()
        => (WriteError)Activator.CreateInstance(
            typeof(WriteError),
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            [ServerErrorCategory.DuplicateKey, 11000, "E11000 duplicate key error", new BsonDocument()],
            culture: null)!;
}
