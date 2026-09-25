using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Moq;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression (r23): the DB transports rebuilt a claimed message's headers with the
/// case-insensitive copying CONSTRUCTOR, whose internal Add throws on keys differing only in
/// case — legal JSON/BSON from a foreign producer. The throw fired AFTER the claim had already
/// committed attempts+1/lock_id and BEFORE any delivery object existed, so the row never reached
/// HandleFailureAsync or dead-lettering: an unkillable poison row that tore down the subscriber
/// on every re-claim. The copy is now indexer-based, last-wins, like the ASB/SQS receive
/// adapters.
/// </summary>
public sealed class TransportStoreHeaderRegressionTests
{
    [Theory]
    [InlineData(typeof(AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportStore))]
    [InlineData(typeof(AsyncResponse.Transports.SqlServer.SqlServerTransportStore))]
    public void DeserializeHeaders_CaseVariantKeys_LastWinsInsteadOfThrowing(System.Type storeType)
    {
        var method = storeType.GetMethod("DeserializeHeaders", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var headers = (IReadOnlyDictionary<string, string>)method!.Invoke(
            null,
            ["""{"AR-Correlation-Id":"first","ar-correlation-id":"second"}"""])!;

        Assert.Single(headers);
        Assert.Equal("second", headers["AR-CORRELATION-ID"]);
    }

    /// <summary>
    /// Regression (r25): a wrong-typed header VALUE — legal JSON in the headers column, e.g.
    /// <c>{"AR-CorrelationId": 123}</c> — used to throw <c>JsonException</c> from the typed
    /// deserialize, with the same after-the-claim/before-any-delivery consequence as the key case
    /// above: an unkillable poison row. Materialization is now lenient — scalars keep their raw
    /// JSON text, nulls are skipped, object/array values keep their raw JSON — so the delivery is
    /// always constructible and a genuinely poison message dead-letters through the normal path.
    /// </summary>
    [Theory]
    [InlineData(typeof(AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportStore))]
    [InlineData(typeof(AsyncResponse.Transports.SqlServer.SqlServerTransportStore))]
    public void DeserializeHeaders_WrongTypedValues_AreCoercedInsteadOfThrowing(System.Type storeType)
    {
        var headers = InvokeDeserializeHeaders(
            storeType,
            """{"num":123,"frac":1.5,"flag":true,"gone":null,"obj":{"a":1},"arr":[1,"x"],"text":"plain"}""");

        Assert.Equal("123", headers["num"]);
        Assert.Equal("1.5", headers["frac"]);
        Assert.Equal("true", headers["flag"]);
        Assert.False(headers.ContainsKey("gone"));
        Assert.Equal("""{"a":1}""", headers["obj"]);
        Assert.Equal("""[1,"x"]""", headers["arr"]);
        Assert.Equal("plain", headers["text"]);
    }

    /// <summary>
    /// A headers column holding a non-object root (or, on SQL Server's unchecked nvarchar, text
    /// that is not JSON at all) degrades to no headers rather than a throw: correlation extraction
    /// falls through to the body paths and the message still reaches the handler.
    /// </summary>
    [Theory]
    [InlineData(typeof(AsyncResponse.Transports.PostgreSQL.PostgreSqlTransportStore))]
    [InlineData(typeof(AsyncResponse.Transports.SqlServer.SqlServerTransportStore))]
    public void DeserializeHeaders_NonObjectOrMalformedContent_DegradesToNoHeaders(System.Type storeType)
    {
        Assert.Empty(InvokeDeserializeHeaders(storeType, """[1,2]"""));
        Assert.Empty(InvokeDeserializeHeaders(storeType, """"text""""));
        Assert.Empty(InvokeDeserializeHeaders(storeType, "null"));
        Assert.Empty(InvokeDeserializeHeaders(storeType, "not json at all"));
    }

    private static IReadOnlyDictionary<string, string> InvokeDeserializeHeaders(System.Type storeType, string json)
    {
        var method = storeType.GetMethod("DeserializeHeaders", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        return (IReadOnlyDictionary<string, string>)method!.Invoke(null, [json])!;
    }

    /// <summary>
    /// Regression (r25), MongoDB flavor: the driver's default dictionary serializer throws
    /// <c>FormatException</c> ("Cannot deserialize a 'String' from BsonType 'Int32'") on a
    /// wrong-typed header value — inside the claim's <c>findOneAndUpdate</c>, after the server
    /// already stamped attempts+1/lock_id. The lenient serializer coerces scalars to their
    /// culture-free string form, keeps document/array values as JSON text, and skips nulls.
    /// </summary>
    [Fact]
    public void MongoHeaderMaterialization_WrongTypedValues_AreCoercedInsteadOfThrowing()
    {
        var claimed = BsonSerializer.Deserialize<MongoTransportMessageDocument>(new BsonDocument
        {
            ["_id"] = new BsonBinaryData(Guid.NewGuid(), GuidRepresentation.Standard),
            ["queue"] = "worker",
            ["payload"] = "{}",
            ["headers"] = new BsonArray
            {
                new BsonDocument { ["k"] = "num", ["v"] = 123 },
                new BsonDocument { ["k"] = "frac", ["v"] = 1.5 },
                new BsonDocument { ["k"] = "flag", ["v"] = true },
                new BsonDocument { ["k"] = "gone", ["v"] = BsonNull.Value },
                new BsonDocument { ["k"] = "doc", ["v"] = new BsonDocument("a", 1) },
                new BsonDocument { ["k"] = "text", ["v"] = "plain" },
                new BsonDocument("only-k", "no v member"),
                new BsonDocument { ["k"] = "dup", ["v"] = "first" },
                new BsonDocument { ["k"] = "dup", ["v"] = "second" }
            }
        });

        var headers = claimed.Headers!;
        Assert.Equal("123", headers["num"]);
        Assert.Equal("1.5", headers["frac"]);
        Assert.Equal("true", headers["flag"]);
        Assert.False(headers.ContainsKey("gone"));
        Assert.Equal("""{ "a" : 1 }""", headers["doc"]);
        Assert.Equal("plain", headers["text"]);
        Assert.Equal("second", headers["dup"]);
    }

    /// <summary>A headers field that is not even an array degrades to no headers, not a throw.</summary>
    [Fact]
    public void MongoHeaderMaterialization_NonArrayHeaders_DegradeToNoHeaders()
    {
        var claimed = BsonSerializer.Deserialize<MongoTransportMessageDocument>(new BsonDocument
        {
            ["_id"] = new BsonBinaryData(Guid.NewGuid(), GuidRepresentation.Standard),
            ["queue"] = "worker",
            ["payload"] = "{}",
            ["headers"] = "garbage"
        });

        Assert.Null(claimed.Headers);
    }

    /// <summary>
    /// The lenient serializer must keep the driver's array-of-documents wire shape byte-for-byte:
    /// existing documents (and foreign readers) rely on <c>[{ "k": …, "v": … }, …]</c>.
    /// </summary>
    [Fact]
    public void MongoHeaderSerialization_KeepsTheArrayOfDocumentsShape()
    {
        var document = new MongoTransportMessageDocument
        {
            Id = Guid.NewGuid(),
            Queue = "worker",
            Payload = "{}",
            Headers = new Dictionary<string, string> { ["AR-CorrelationId"] = "abc" }
        };

        var rendered = document.ToBsonDocument();

        Assert.Equal(
            new BsonArray { new BsonDocument { ["k"] = "AR-CorrelationId", ["v"] = "abc" } },
            rendered["headers"].AsBsonArray);
    }

    [Fact]
    public async Task MongoClaim_CaseVariantHeaderFields_LastWinsInsteadOfThrowing()
    {
        // BSON legally carries field names differing only in case; the driver deserializes them
        // into an ordinal dictionary. The claim-side copy must not throw after the document's
        // lock was already stamped server-side.
        var claimed = new MongoTransportMessageDocument
        {
            Id = Guid.NewGuid(),
            Queue = "worker",
            Payload = "{}",
            Headers = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["AR-Correlation-Id"] = "first",
                ["ar-correlation-id"] = "second"
            },
            Attempts = 1
        };

        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose);
        database.Setup(d => d.DatabaseNamespace).Returns(new DatabaseNamespace("asyncresponse_tests"));
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        database.WithRawTransportMessages().ClaimsInOrder(claimed.ToBsonDocument());

        // AutoCreateIndexes/UseOwnershipLedger off: EnsureCreatedAsync short-circuits, so the
        // claim runs against the mocked collection alone.
        var store = new MongoDbTransportStore(
            database.Object,
            Options.Create(new MongoDbAsyncResponseTransportOptions
            {
                AutoCreateIndexes = false,
                UseOwnershipLedger = false
            }));

        var delivery = await store.TryClaimAsync("worker", TimeSpan.FromSeconds(30), CancellationToken.None);

        Assert.NotNull(delivery);
        Assert.Single(delivery!.Headers);
        Assert.Equal("second", delivery.Headers["AR-CORRELATION-ID"]);
    }
    public static TheoryData<string> UnreadableDocuments =>
    [
        // A foreign producer's insert with the driver-generated ObjectId _id every Mongo driver
        // defaults to (GuidSerializer cannot read an ObjectId).
        "objectid-id",
        // A payload written as an embedded document, natural for Mongo (the class maps a string).
        "document-payload"
    ];

    /// <summary>
    /// Regression: the claim deserialized the stamped document INSIDE findOneAndUpdate, so a field
    /// the class map cannot read — a driver-generated ObjectId <c>_id</c>, a payload written as an
    /// embedded document — threw FormatException after the server had already stamped
    /// attempts+1/lock_id and before any delivery existed: never dead-lettered, re-claimed every
    /// LockTimeout forever, faulting the subscriber each time. The claim now reads raw BSON, maps it
    /// in a try, buries an unreadable document by the lock_id it just stamped (under a dead-letter id
    /// derived from the raw <c>_id</c>), and moves on to the next document.
    /// </summary>
    [Theory]
    [MemberData(nameof(UnreadableDocuments))]
    public async Task MongoClaim_UnreadableDocument_IsBuriedByItsLockFence_AndTheClaimMovesOn(string shape)
    {
        var rawId = shape == "objectid-id" ? (BsonValue)ObjectId.GenerateNewId() : new BsonBinaryData(Guid.NewGuid(), GuidRepresentation.Standard);
        var unreadable = new BsonDocument
        {
            ["_id"] = rawId,
            ["queue"] = "worker",
            ["payload"] = shape == "document-payload" ? new BsonDocument("job", 1) : "{\"job\":1}",
            ["headers"] = new BsonArray { new BsonDocument { ["k"] = "AR-CorrelationId", ["v"] = "corr-foreign" } },
            ["attempts"] = 1
        };
        Assert.ThrowsAny<Exception>(() => BsonSerializer.Deserialize<MongoTransportMessageDocument>(unreadable));

        var next = new MongoTransportMessageDocument { Id = Guid.NewGuid(), Queue = "worker", Payload = "{}", Attempts = 1 };
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var upserts = new List<(FilterDefinition<MongoTransportMessageDocument> Filter, UpdateDefinition<MongoTransportMessageDocument> Update)>();
        collection
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoTransportMessageDocument> filter, UpdateDefinition<MongoTransportMessageDocument> update, UpdateOptions _, CancellationToken _) => upserts.Add((filter, update)))
            .ReturnsAsync(new UpdateResult.Acknowledged(0, 0, BsonNull.Value));
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose).WithTestNamespace();
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        var raw = database.WithRawTransportMessages();
        var claimUpdates = new List<UpdateDefinition<BsonDocument>>();
        var claims = new Queue<BsonDocument>([unreadable, next.ToBsonDocument()]);
        raw
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<BsonDocument> _, UpdateDefinition<BsonDocument> update, FindOneAndUpdateOptions<BsonDocument, BsonDocument> _, CancellationToken _) => claimUpdates.Add(update))
            .ReturnsAsync(() => claims.Dequeue());
        var deletes = new List<BsonDocument>();
        raw
            .Setup(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<BsonDocument>>(), It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<BsonDocument> filter, CancellationToken _) => deletes.Add(filter.Render(RawRenderArgs())))
            .ReturnsAsync(new DeleteResult.Acknowledged(1));
        var store = new MongoDbTransportStore(
            database.Object,
            Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false, UseOwnershipLedger = false }));

        var delivery = await store.TryClaimAsync("worker", TimeSpan.FromSeconds(30), CancellationToken.None);

        // The claim moved on to the next, readable document.
        Assert.NotNull(delivery);
        Assert.Equal(next.Id, delivery!.Id);

        // The unreadable one was removed by the lock_id its own claim stamped — no typed _id needed.
        var stampedLock = claimUpdates[0].Render(RawRenderArgs()).AsBsonArray[0]["$set"]["lock_id"];
        var fenced = Assert.Single(deletes);
        Assert.Equal(stampedLock, fenced["lock_id"]);

        // ...after a dead-letter copy under an id derived from its raw _id, keeping payload and headers.
        var (deadFilter, deadUpdate) = Assert.Single(upserts);
        Assert.Equal(
            new BsonBinaryData(MongoDbTransportStore.UnreadableDeadLetterId(rawId), GuidRepresentation.Standard),
            deadFilter.Render(TypedRenderArgs())["_id"]);
        var set = deadUpdate.Render(TypedRenderArgs()).AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal("deadletter", set["queue"]["$literal"].AsString);
        Assert.Equal(
            shape == "document-payload" ? new BsonDocument("job", 1).ToJson() : "{\"job\":1}",
            set["payload"]["$literal"].AsString);
        var headers = set["headers"]["$ifNull"].AsBsonArray[1]["$literal"].AsBsonArray
            .Select(entry => entry.AsBsonDocument)
            .ToDictionary(entry => entry["k"].AsString, entry => entry["v"].AsString);
        Assert.Equal("corr-foreign", headers["AR-CorrelationId"]);
        Assert.Equal("worker", headers["AR-DeadLetter-Source-Queue"]);
        Assert.Contains("could not be read", headers["AR-DeadLetter-Reason"], StringComparison.Ordinal);
    }

    private static RenderArgs<BsonDocument> RawRenderArgs()
        => new(BsonSerializer.LookupSerializer<BsonDocument>(), BsonSerializer.SerializerRegistry);

    private static RenderArgs<MongoTransportMessageDocument> TypedRenderArgs()
        => new(BsonSerializer.LookupSerializer<MongoTransportMessageDocument>(), BsonSerializer.SerializerRegistry);
}
