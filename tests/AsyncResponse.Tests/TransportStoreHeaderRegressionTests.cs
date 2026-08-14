using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.Options;
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

        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose);
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
