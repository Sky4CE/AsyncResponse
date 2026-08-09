using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;
using System.Net;
using System.Reflection;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Covers the MongoDB query shapes that carry the delivery contract: the change-stream watch
/// pipelines, the atomic <c>findOneAndUpdate</c> claim filters/updates, and the claim-loop
/// plumbing over a mocked collection.
/// </summary>
public sealed class MongoDbStoreQueryTests
{
    [Fact]
    public void ChannelWatchPipeline_MatchesInsertEventsOnly()
    {
        var rendered = Render(MongoDbChannelStore.BuildMessageWatchPipeline());

        var stage = Assert.Single(rendered);
        Assert.Equal("insert", stage["$match"]["operationType"].AsString);
    }

    [Fact]
    public void TransportWatchPipeline_MatchesInsertsIntoOneQueue()
    {
        var rendered = Render(MongoDbTransportStore.BuildQueueWatchPipeline("worker"));

        var stage = Assert.Single(rendered);
        var conditions = stage["$match"]["$and"].AsBsonArray;
        Assert.Contains(conditions, condition => condition.AsBsonDocument.Contains("operationType") && condition["operationType"] == "insert");
        Assert.Contains(conditions, condition => condition.AsBsonDocument.Contains("fullDocument.queue") && condition["fullDocument.queue"] == "worker");
    }

    /// <summary>
    /// Fenced renewal extends locked_until from the server clock, so a renewal can never be stamped
    /// from a skewed app clock the way a client-side timestamp would be.
    /// </summary>
    [Fact]
    public void TransportRenewUpdate_ExtendsTheLeaseFromTheServerClock()
    {
        var rendered = MongoDbTransportStore.BuildRenewUpdate(TimeSpan.FromSeconds(30))
            .Render(TransportRenderArgs());

        var set = rendered.AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal(new BsonArray { "$$NOW", 30000d }, set["locked_until"]["$add"].AsBsonArray);
    }

    [Fact]
    public void TransportClaimFilter_GatesOnQueueAvailabilityAndLockExpiry_UsingServerClock()
    {
        var rendered = MongoDbTransportStore.BuildClaimFilter("worker")
            .Render(TransportRenderArgs());

        Assert.Equal("worker", rendered["queue"].AsString);

        // Availability and lock expiry are compared against $$NOW (the server clock), so publisher
        // and consumer clock skew can never fence messages in or out of the claim.
        var expr = rendered["$expr"]["$and"].AsBsonArray;
        Assert.Equal(2, expr.Count);
        Assert.Equal(new BsonArray { "$available_at", "$$NOW" }, expr[0]["$lte"].AsBsonArray);
        var lockConditions = expr[1]["$or"].AsBsonArray;
        Assert.Equal(new BsonArray { "$locked_until", BsonNull.Value }, lockConditions[0]["$eq"].AsBsonArray);
        Assert.Equal(new BsonArray { "$locked_until", "$$NOW" }, lockConditions[1]["$lte"].AsBsonArray);
    }

    [Fact]
    public void TransportClaimUpdate_IncrementsAttemptsAndFencesWithLockIdAndServerClockLease()
    {
        var lockId = Guid.NewGuid();
        var rendered = MongoDbTransportStore.BuildClaimUpdate(lockId, TimeSpan.FromSeconds(30))
            .Render(TransportRenderArgs());

        var set = rendered.AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal(
            new BsonArray { new BsonDocument("$ifNull", new BsonArray { "$attempts", 0 }), 1 },
            set["attempts"]["$add"].AsBsonArray);
        Assert.Equal(new BsonArray { "$$NOW", 30_000d }, set["locked_until"]["$add"].AsBsonArray);
        Assert.Equal(new BsonBinaryData(lockId, GuidRepresentation.Standard), set["lock_id"].AsBsonBinaryData);
    }

    [Fact]
    public void TransportNakUpdate_ReleasesLockAndDelaysAvailability()
    {
        var rendered = MongoDbTransportStore.BuildNakUpdate(TimeSpan.FromSeconds(5))
            .Render(TransportRenderArgs());

        var set = rendered.AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal(new BsonArray { "$$NOW", 5_000d }, set["available_at"]["$add"].AsBsonArray);
        Assert.Equal(BsonNull.Value, set["locked_until"]);
        Assert.Equal(BsonNull.Value, set["lock_id"]);
    }

    [Fact]
    public void ChannelInsertPipeline_PreservesOriginalTimestampsAndClaimFlagsOnRetry()
    {
        var rendered = MongoDbChannelStore.BuildInsertMessagePipeline("corr", "{}", TimeSpan.FromHours(1))
            .Render(ChannelRenderArgs());

        var set = rendered.AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal("corr", set["correlation_id"].AsString);
        Assert.Equal("{}", set["envelope_json"].AsString);
        // $ifNull keeps the first (server-stamped) values, making a retried publish idempotent.
        Assert.Equal(new BsonArray { "$created_at", "$$NOW" }, set["created_at"]["$ifNull"].AsBsonArray);
        Assert.Equal("$expires_at", set["expires_at"]["$ifNull"].AsBsonArray[0]);
        Assert.Equal(new BsonArray { "$acked_at", BsonNull.Value }, set["acked_at"]["$ifNull"].AsBsonArray);
        Assert.Equal(new BsonArray { "$recovery_claimed", false }, set["recovery_claimed"]["$ifNull"].AsBsonArray);
    }

    [Fact]
    public void ChannelDeliveryClaim_GatesOnRecoveryClaimAndAcksWithServerClock()
    {
        var messageId = Guid.NewGuid();
        var filter = MongoDbChannelStore.BuildDeliveryClaimFilter(messageId).Render(ChannelRenderArgs());
        var update = MongoDbChannelStore.BuildDeliveryClaimUpdate(ackSeq: 42).Render(ChannelRenderArgs());

        // The live-delivery claim gates only on recovery_claimed (not acked_at), preserving
        // cross-process fan-out: multiple processes may each win delivery.
        Assert.Equal(new BsonBinaryData(messageId, GuidRepresentation.Standard), filter["_id"].AsBsonBinaryData);
        Assert.False(filter["recovery_claimed"].AsBoolean);
        Assert.True(filter.Contains("expires_at"));

        var set = update.AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal(new BsonArray { "$acked_at", "$$NOW" }, set["acked_at"]["$ifNull"].AsBsonArray);
        // The sequence stamps ONLY on the null→set acked_at transition of this same update: a
        // legacy-acked row (pre-sequence build) must stay permanently unsequenced, or a later
        // fan-out re-claim pairs the OLD acked_at with a FRESH sequence and a tick-tied waiter
        // replays its predecessor's response.
        var condition = set["acked_seq"]["$cond"].AsBsonArray;
        Assert.Equal(
            new BsonDocument("$eq", new BsonArray { new BsonDocument("$ifNull", new BsonArray { "$acked_at", BsonNull.Value }), BsonNull.Value }),
            condition[0].AsBsonDocument);
        Assert.Equal(42L, condition[1].AsInt64);
        Assert.Equal("$acked_seq", condition[2].AsString);
    }

    [Fact]
    public void ChannelRecoveryClaim_RequiresUnackedMessage()
    {
        var messageId = Guid.NewGuid();
        var filter = MongoDbChannelStore.BuildRecoveryClaimFilter(messageId).Render(ChannelRenderArgs());

        // Recovery wins only while no waiter has delivered: acked_at must still be null.
        Assert.Equal(new BsonBinaryData(messageId, GuidRepresentation.Standard), filter["_id"].AsBsonBinaryData);
        Assert.Equal(BsonNull.Value, filter["acked_at"]);
    }

    [Fact]
    public async Task TransportClaimLoop_MapsClaimedDocumentToDelivery_AndFencesAckNakByLockId()
    {
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose);
        var claimed = new MongoTransportMessageDocument
        {
            Id = Guid.NewGuid(),
            Queue = "worker",
            Payload = """{"job":1}""",
            Headers = new Dictionary<string, string> { ["AR-Correlation-Id"] = "corr-claim" },
            Attempts = 3
        };
        FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument>? capturedOptions = null;
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoTransportMessageDocument> _, UpdateDefinition<MongoTransportMessageDocument> _, FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument> options, CancellationToken _) => capturedOptions = options)
            .ReturnsAsync(claimed);
        collection
            .Setup(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteResult.Acknowledged(1));
        collection
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new UpdateResult.Acknowledged(1, 1, BsonNull.Value));
        var store = CreateTransportStore(collection.Object);

        var delivery = await store.TryClaimAsync("worker", TimeSpan.FromSeconds(30), CancellationToken.None);

        Assert.NotNull(delivery);
        Assert.Equal(claimed.Id, delivery!.Id);
        Assert.Equal("worker", delivery.Queue);
        Assert.Equal(claimed.Payload, delivery.Payload);
        Assert.Equal(3, delivery.Attempt);
        Assert.Equal("corr-claim", delivery.Headers["ar-correlation-id"]); // case-insensitive header lookup
        Assert.NotNull(capturedOptions);
        Assert.NotNull(capturedOptions!.Sort); // oldest-first claim ordering
        Assert.Equal(ReturnDocument.After, capturedOptions.ReturnDocument);

        await delivery.AckAsync();
        collection.Verify(
            c => c.DeleteOneAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<CancellationToken>()),
            Times.Once);

        await delivery.NakAsync(TimeSpan.FromSeconds(5));
        collection.Verify(
            c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task TransportClaimBatch_StopsAtEmptyClaimAndHonorsBatchSize()
    {
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose);
        collection
            .SetupSequence(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MongoTransportMessageDocument { Id = Guid.NewGuid(), Queue = "worker", Payload = "{}" })
            .ReturnsAsync(new MongoTransportMessageDocument { Id = Guid.NewGuid(), Queue = "worker", Payload = "{}" })
            .ReturnsAsync((MongoTransportMessageDocument)null!)
            .ReturnsAsync(new MongoTransportMessageDocument { Id = Guid.NewGuid(), Queue = "worker", Payload = "{}" });
        var store = CreateTransportStore(collection.Object);

        var claimed = 0;
        await foreach (var _ in store.ClaimBatchAsync("worker", batchSize: 16, TimeSpan.FromSeconds(30), CancellationToken.None))
            claimed++;

        // The loop must stop on the first empty claim (queue drained), not keep issuing round-trips.
        Assert.Equal(2, claimed);

        claimed = 0;
        await foreach (var _ in store.ClaimBatchAsync("worker", batchSize: 1, TimeSpan.FromSeconds(30), CancellationToken.None))
            claimed++;
        Assert.Equal(1, claimed);
    }

    [Fact]
    public async Task ChannelDeliveryAndRecoveryClaims_ReportWhetherTheClaimWon()
    {
        var collection = new Mock<IMongoCollection<MongoChannelMessageDocument>>(MockBehavior.Loose);
        collection
            .SetupSequence(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MongoChannelMessageDocument { Id = Guid.NewGuid(), CorrelationId = "corr" })
            .ReturnsAsync((MongoChannelMessageDocument)null!)
            .ReturnsAsync(new MongoChannelMessageDocument { Id = Guid.NewGuid(), CorrelationId = "corr" })
            .ReturnsAsync((MongoChannelMessageDocument)null!);
        var store = CreateChannelStore(collection.Object);

        Assert.True(await store.TryClaimForDeliveryAsync(Guid.NewGuid(), CancellationToken.None));
        Assert.False(await store.TryClaimForDeliveryAsync(Guid.NewGuid(), CancellationToken.None));
        Assert.True(await store.TryClaimForRecoveryAsync(Guid.NewGuid(), CancellationToken.None));
        Assert.False(await store.TryClaimForRecoveryAsync(Guid.NewGuid(), CancellationToken.None));
    }

    [Fact]
    public void IsChangeStreamUnsupported_FlagsReplicaSetRequirementOnly()
    {
        Assert.False(MongoDbChannelStore.IsChangeStreamUnsupported(new TimeoutException()));
        Assert.False(MongoDbTransportStore.IsChangeStreamUnsupported(new InvalidOperationException("only supported on replica sets")));

        var connectionId = new ConnectionId(
            new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017)));
        var codeFailure = new MongoCommandException(
            connectionId,
            "command",
            new BsonDocument(),
            new BsonDocument { ["ok"] = 0, ["code"] = 40573, ["errmsg"] = "unsupported" });
        var messageFailure = new MongoCommandException(
            connectionId,
            "command",
            new BsonDocument(),
            new BsonDocument
            {
                ["ok"] = 0,
                ["code"] = 1,
                ["errmsg"] = "only supported on replica sets"
            });
        typeof(Exception).GetField("_message", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(messageFailure, "only supported on replica sets");

        Assert.True(MongoDbChannelStore.IsChangeStreamUnsupported(codeFailure));
        Assert.True(MongoDbTransportStore.IsChangeStreamUnsupported(codeFailure));
        Assert.True(MongoDbChannelStore.IsChangeStreamUnsupported(messageFailure));
        Assert.True(MongoDbTransportStore.IsChangeStreamUnsupported(messageFailure));
    }

    private static MongoDbTransportStore CreateTransportStore(IMongoCollection<MongoTransportMessageDocument> collection)
    {
        var options = new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false };
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose);
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(options.MessageCollection, It.IsAny<MongoCollectionSettings>()))
            .Returns(collection);
        return new MongoDbTransportStore(database.Object, Options.Create(options));
    }

    private static MongoDbChannelStore CreateChannelStore(IMongoCollection<MongoChannelMessageDocument> collection)
    {
        var options = new MongoDbAsyncResponseChannelOptions { AutoCreateIndexes = false };
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose);
        database
            .Setup(d => d.GetCollection<MongoChannelMessageDocument>(options.MessageCollection, It.IsAny<MongoCollectionSettings>()))
            .Returns(collection);
        database
            .Setup(d => d.GetCollection<MongoRecoveryStateDocument>(options.RecoveryStateCollection, It.IsAny<MongoCollectionSettings>()))
            .Returns(Mock.Of<IMongoCollection<MongoRecoveryStateDocument>>());
        database
            .Setup(d => d.GetCollection<MongoChannelSubscriberDocument>(options.SubscriberCollection, It.IsAny<MongoCollectionSettings>()))
            .Returns(Mock.Of<IMongoCollection<MongoChannelSubscriberDocument>>());
        database.WithCounters();
        return new MongoDbChannelStore(database.Object, Options.Create(options));
    }

    private static RenderArgs<TDocument> RenderArgsFor<TDocument>()
        => new(BsonSerializer.LookupSerializer<TDocument>(), BsonSerializer.SerializerRegistry);

    private static RenderArgs<MongoTransportMessageDocument> TransportRenderArgs()
        => RenderArgsFor<MongoTransportMessageDocument>();

    private static RenderArgs<MongoChannelMessageDocument> ChannelRenderArgs()
        => RenderArgsFor<MongoChannelMessageDocument>();

    private static IList<BsonDocument> Render<TDocument>(
        PipelineDefinition<ChangeStreamDocument<TDocument>, ChangeStreamDocument<TDocument>> pipeline)
    {
        var serializer = new ChangeStreamDocumentSerializer<TDocument>(BsonSerializer.LookupSerializer<TDocument>());
        var rendered = pipeline.Render(new RenderArgs<ChangeStreamDocument<TDocument>>(serializer, BsonSerializer.SerializerRegistry));
        return rendered.Documents;
    }
}
