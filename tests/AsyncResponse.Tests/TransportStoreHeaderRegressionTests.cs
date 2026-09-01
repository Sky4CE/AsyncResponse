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
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(claimed);

        var database = new Mock<IMongoDatabase>(MockBehavior.Loose);
        database.Setup(d => d.DatabaseNamespace).Returns(new DatabaseNamespace("asyncresponse_tests"));
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);

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
}
