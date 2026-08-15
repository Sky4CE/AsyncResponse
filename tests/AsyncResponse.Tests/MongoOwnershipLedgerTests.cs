using AsyncResponse.Channels.MongoDB;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
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
        var database = new Mock<IMongoDatabase>();
        database
            .Setup(d => d.GetCollection<BsonDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        database.SetupGet(d => d.DatabaseNamespace).Returns(new DatabaseNamespace("appdb"));
        return database;
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
