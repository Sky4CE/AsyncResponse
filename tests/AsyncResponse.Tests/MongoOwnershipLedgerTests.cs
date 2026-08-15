using AsyncResponse.Channels.MongoDB;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;
using Moq;
using System.Net;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The persisted cross-host collection-ownership ledger. The ledger type is source-linked into
/// the channel, transport, and durable-flow packages, so these tests bind to the CHANNEL
/// assembly's copy via reflection to avoid a three-way CS0433 ambiguity.
/// </summary>
public sealed class MongoOwnershipLedgerTests
{
    private static Task ClaimAsync(IMongoDatabase database, string componentName, params (string Collection, string Purpose)[] claims)
    {
        var ledger = typeof(MongoDbAsyncResponseChannelOptions).Assembly
            .GetType("AsyncResponse.Internal.MongoOwnershipLedger", throwOnError: true)!;
        var claim = ledger.GetMethod("ClaimAsync", BindingFlags.Public | BindingFlags.Static)!;
        return (Task)claim.Invoke(
            null,
            [database, componentName, (IReadOnlyList<(string, string)>)claims, CancellationToken.None])!;
    }

    private static Mock<IMongoDatabase> Database(BsonDocument? existingClaim)
    {
        var collection = new Mock<IMongoCollection<BsonDocument>>();
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(existingClaim!);
        return Database(collection);
    }

    private static Mock<IMongoDatabase> Database(Mock<IMongoCollection<BsonDocument>> collection)
    {
        var database = new Mock<IMongoDatabase>();
        database
            .Setup(d => d.GetCollection<BsonDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        database.SetupGet(d => d.DatabaseNamespace).Returns(new DatabaseNamespace("appdb"));
        return database;
    }

    /// <summary>
    /// The two surfaces the server's E11000 reaches the driver through: findAndModify reports a
    /// command error; write commands report a categorized write error (whose <c>WriteError</c>
    /// has an internal constructor, hence the reflection).
    /// </summary>
    private static MongoException DuplicateKey(string surface)
    {
        var connectionId = new ConnectionId(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017)));
        if (surface == "command")
        {
            return new MongoCommandException(
                connectionId,
                "findAndModify failed",
                new BsonDocument("findAndModify", "asyncresponse_ownership"),
                new BsonDocument { ["ok"] = 0, ["code"] = 11000, ["errmsg"] = "E11000 duplicate key error" });
        }

        var writeError = (WriteError)Activator.CreateInstance(
            typeof(WriteError),
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            [ServerErrorCategory.DuplicateKey, 11000, "E11000 duplicate key error", new BsonDocument()],
            culture: null)!;
        return new MongoWriteException(connectionId, writeError, writeConcernError: null, innerException: null);
    }

    /// <summary>
    /// Regression: MongoDB's upsert is not atomic against a concurrent upsert on the same
    /// <c>_id</c> when nothing matches — the loser of two hosts' FIRST claim got a raw E11000
    /// instead of the winner's document. The identical retry must find the winner's claim and
    /// treat a same-component winner as idempotent success.
    /// </summary>
    [Theory]
    [InlineData("write")]
    [InlineData("command")]
    public async Task ClaimAsync_RetriesTheFirstClaimRace_AndAcceptsTheSameComponentWinner(string surface)
    {
        var collection = new Mock<IMongoCollection<BsonDocument>>();
        collection
            .SetupSequence(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(DuplicateKey(surface))
            .ReturnsAsync(new BsonDocument
            {
                { "_id", "flow_state" },
                { "component", "MongoDB durable-flow store" },
                { "purpose", "state" }
            });

        // Must NOT surface the raw duplicate-key error.
        await ClaimAsync(Database(collection).Object, "MongoDB durable-flow store", ("flow_state", "state"));

        collection.Verify(
            c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    /// <summary>
    /// Losing the first-claim race to a DIFFERENT component must still produce the actionable
    /// conflict error naming both claimants — not the raw E11000 the loser observed.
    /// </summary>
    [Fact]
    public async Task ClaimAsync_RetriesTheFirstClaimRace_AndReportsAForeignWinnerActionably()
    {
        var collection = new Mock<IMongoCollection<BsonDocument>>();
        collection
            .SetupSequence(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(DuplicateKey("write"))
            .ReturnsAsync(new BsonDocument
            {
                { "_id", "flow_state" },
                { "component", "MongoDB channel" },
                { "purpose", "messages" }
            });

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ClaimAsync(Database(collection).Object, "MongoDB durable-flow store", ("flow_state", "state")));

        Assert.Contains("already claimed by the MongoDB channel (messages)", exception.Message);
    }

    /// <summary>A second duplicate-key failure is a real error, not a race — it must propagate.</summary>
    [Fact]
    public async Task ClaimAsync_DoesNotRetryTheDuplicateKeyFailureMoreThanOnce()
    {
        var collection = new Mock<IMongoCollection<BsonDocument>>();
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(DuplicateKey("write"));

        await Assert.ThrowsAsync<MongoWriteException>(
            () => ClaimAsync(Database(collection).Object, "MongoDB durable-flow store", ("flow_state", "state")));

        collection.Verify(
            c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    [Theory]
    [InlineData("null")]
    [InlineData("int")]
    public async Task ClaimAsync_ToleratesForeignNonStringOwnershipValues(string kind)
    {
        // Regression (r24): the conflict guard evaluated owner.AsString, which throws
        // InvalidCastException for any non-string BSON value — a hand-repaired claim with
        // component: null (which the conflict error text itself invites operators to create) or a
        // migration-written int enum crashed EVERY EnsureCreated with a bare InvalidCastException,
        // forever, and the actionable message was never reached. Non-string values are foreign
        // writes: tolerated by the documented contract, exactly like a missing field.
        BsonValue foreign = kind == "null" ? BsonNull.Value : new BsonInt32(3);
        var database = Database(new BsonDocument
        {
            { "_id", "flow_state" },
            { "component", foreign },
            { "purpose", "state" }
        });

        // Must NOT throw.
        await ClaimAsync(database.Object, "MongoDB durable-flow store", ("flow_state", "state"));
    }

    [Fact]
    public async Task ClaimAsync_StillRejectsAGenuineForeignClaim_WithTheActionableMessage()
    {
        var database = Database(new BsonDocument
        {
            { "_id", "flow_state" },
            { "component", "MongoDB channel" },
            { "purpose", "messages" }
        });

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ClaimAsync(database.Object, "MongoDB durable-flow store", ("flow_state", "state")));

        Assert.Contains("already claimed by the MongoDB channel (messages)", exception.Message);
        Assert.Contains("asyncresponse_ownership", exception.Message);
    }

    [Fact]
    public async Task ClaimAsync_TreatsARepeatedClaimByTheSameComponent_AsIdempotent()
    {
        var database = Database(new BsonDocument
        {
            { "_id", "flow_state" },
            { "component", "MongoDB durable-flow store" },
            { "purpose", "state" }
        });

        await ClaimAsync(database.Object, "MongoDB durable-flow store", ("flow_state", "state"));
    }
}
