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

        Assert.Equal("insert", rendered[0]["$match"]["operationType"].AsString);
    }

    /// <summary>
    /// Fixpoint r1 S5#17: every process watches every insert, and the stream shipped each full
    /// envelope to every watcher although the wake only reads its correlation id. The pipeline now
    /// projects the event down to that id (the <c>_id</c> resume token stays: an inclusion
    /// projection keeps it). Pre-fix: a single $match stage, no $project.
    /// </summary>
    [Fact]
    public void ChannelWatchPipeline_ProjectsEachEventDownToItsCorrelationId()
    {
        var rendered = Render(MongoDbChannelStore.BuildMessageWatchPipeline());

        Assert.Equal(2, rendered.Count);
        var projection = rendered[1]["$project"].AsBsonDocument;
        Assert.Equal(1, projection["fullDocument.correlation_id"].ToInt32());
        Assert.False(projection.Contains("fullDocument"), "the whole document must not be projected");
        Assert.False(projection.Contains("_id") && projection["_id"].ToInt32() == 0, "the resume token must survive the projection");
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
            .Render(RenderArgsFor<BsonDocument>());

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
            .Render(RenderArgsFor<BsonDocument>());

        // attempts through $convert (onError/onNull 0), not a bare $add: a foreign producer's
        // non-numeric attempts made the server-side update itself fail, so the document was never
        // locked, stayed at the head of the claim order, and every claim of the queue failed.
        var set = rendered.AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal(
            new BsonArray
            {
                new BsonDocument("$convert", new BsonDocument { ["input"] = "$attempts", ["to"] = "int", ["onError"] = 0, ["onNull"] = 0 }),
                1
            },
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

    /// <summary>
    /// created_at — the claim Sort key and the dead-letter prune cutoff — is stamped from the
    /// server clock ($ifNull → $$NOW, kept on retry), so a skewed publisher cannot sort its rows
    /// to the queue head; user strings ride inside $literal so a value starting with '$' is never
    /// read as a field path; and attempts/lease fields of an already-claimed document are never
    /// reset by a publish retry.
    /// </summary>
    [Fact]
    public void TransportInsertPipeline_StampsServerClockAndPreservesExistingStateOnRetry()
    {
        var rendered = MongoDbTransportStore.BuildInsertPipeline(
                "worker",
                """{"job":1}""",
                new Dictionary<string, string> { ["AR-CorrelationId"] = "corr" },
                deadLetterReason: null,
                delay: null)
            .Render(TransportRenderArgs());

        var set = rendered.AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal("worker", set["queue"]["$literal"].AsString);
        Assert.Equal("""{"job":1}""", set["payload"]["$literal"].AsString);
        Assert.Equal(new BsonArray { "$created_at", "$$NOW" }, set["created_at"]["$ifNull"].AsBsonArray);
        // Immediately due on the server clock — the claim order's first key (r2 S9#5: an epoch stamp
        // would sort every immediate document ahead of every due NAKed or delayed one).
        Assert.Equal(
            new BsonArray { "$available_at", "$$NOW" },
            set["available_at"]["$ifNull"].AsBsonArray);
        Assert.Equal(new BsonArray { "$attempts", 0 }, set["attempts"]["$ifNull"].AsBsonArray);
        Assert.Equal("$headers", set["headers"]["$ifNull"].AsBsonArray[0]);
        Assert.Equal(
            new BsonArray { new BsonDocument { ["k"] = "AR-CorrelationId", ["v"] = "corr" } },
            set["headers"]["$ifNull"].AsBsonArray[1]["$literal"].AsBsonArray);
        Assert.Equal(new BsonArray { "$dead_letter_reason", BsonNull.Value }, set["dead_letter_reason"]["$ifNull"].AsBsonArray);
        // Never touched by a publish: a retry must not reset a claimed document's lease or count.
        Assert.False(set.Contains("locked_until"));
        Assert.False(set.Contains("lock_id"));
    }

    /// <summary>A delayed publish computes its due time server-relative, mirroring the NAK update.</summary>
    [Fact]
    public void TransportInsertPipeline_DelayedPublish_ComputesDueTimeOnTheServerClock()
    {
        var rendered = MongoDbTransportStore.BuildInsertPipeline("worker", "{}", headers: null, deadLetterReason: null, delay: TimeSpan.FromSeconds(30))
            .Render(TransportRenderArgs());

        var availableAt = rendered.AsBsonArray[0]["$set"]["available_at"]["$ifNull"].AsBsonArray;
        Assert.Equal("$available_at", availableAt[0]);
        Assert.Equal(new BsonArray { "$$NOW", 30_000d }, availableAt[1]["$add"].AsBsonArray);
    }

    /// <summary>
    /// The dead-letter prune cutoff is evaluated ENTIRELY on the server clock ($$NOW against the
    /// server-stamped created_at), like the claim filter — an app-clock cutoff mixed two
    /// instances' clocks, so a behind-clock pruner deleted fresh dead letters on arrival.
    /// </summary>
    [Fact]
    public void TransportDeadLetterPruneFilter_EvaluatesAgeOnTheServerClock()
    {
        var rendered = MongoDbTransportStore.BuildDeadLetterPruneFilter("dead", TimeSpan.FromMinutes(30))
            .Render(TransportRenderArgs());

        Assert.Equal("dead", rendered["queue"].AsString);
        var age = rendered["$expr"]["$lt"].AsBsonArray;
        Assert.Equal("$created_at", age[0]);
        Assert.Equal(new BsonArray { "$$NOW", 1_800_000d }, age[1]["$subtract"].AsBsonArray);
    }

    [Fact]
    public void ChannelInsertPipeline_PreservesOriginalTimestampsAndClaimFlagsOnRetry()
    {
        var rendered = MongoDbChannelStore.BuildInsertMessagePipeline("corr", "{}", TimeSpan.FromHours(1))
            .Render(ChannelRenderArgs());

        var set = rendered.AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal("corr", set["correlation_id"]["$literal"].AsString);
        Assert.Equal("{}", set["envelope_json"]["$literal"].AsString);
        // $ifNull keeps the first (server-stamped) values, making a retried publish idempotent.
        Assert.Equal(new BsonArray { "$created_at", "$$NOW" }, set["created_at"]["$ifNull"].AsBsonArray);
        Assert.Equal("$expires_at", set["expires_at"]["$ifNull"].AsBsonArray[0]);
        Assert.Equal(new BsonArray { "$acked_at", BsonNull.Value }, set["acked_at"]["$ifNull"].AsBsonArray);
        Assert.Equal(new BsonArray { "$recovery_claimed", false }, set["recovery_claimed"]["$ifNull"].AsBsonArray);
    }

    /// <summary>
    /// Fixpoint r1 GS3#1: the channel's upserts are aggregation pipelines, where a plain string
    /// beginning with <c>$</c> is a field path — a correlation id like <c>"$order-17"</c> resolved
    /// to a missing field and was never stored, so every lookup for the id matched nothing and
    /// each response for it was silently dropped. Every caller-supplied string is now a
    /// <c>$literal</c>. Pre-fix: the bare string was rendered.
    /// </summary>
    [Fact]
    public void ChannelUpsertPipelines_WrapEveryCallerSuppliedStringInALiteral()
    {
        const string correlationId = "$order-17";
        var state = new RecoveryState { CorrelationId = correlationId, RegistrationId = Guid.NewGuid() };

        var recovery = MongoDbChannelStore.BuildRecoveryStateUpsertPipeline(correlationId, state, TimeSpan.FromHours(1))
            .Render(RenderArgsFor<MongoRecoveryStateDocument>()).AsBsonArray[0]["$set"].AsBsonDocument;
        var message = MongoDbChannelStore.BuildInsertMessagePipeline(correlationId, "$envelope", TimeSpan.FromHours(1))
            .Render(ChannelRenderArgs()).AsBsonArray[0]["$set"].AsBsonDocument;
        var subscriber = MongoDbChannelStore.BuildSubscriberUpsertPipeline(correlationId, Guid.NewGuid(), "$instance", TimeSpan.FromMinutes(1))
            .Render(RenderArgsFor<MongoChannelSubscriberDocument>()).AsBsonArray[0]["$set"].AsBsonDocument;

        Assert.Equal(correlationId, recovery["correlation_id"]["$literal"].AsString);
        Assert.StartsWith("{", recovery["state_json"]["$literal"].AsString, StringComparison.Ordinal);
        Assert.Equal(correlationId, message["correlation_id"]["$literal"].AsString);
        Assert.Equal("$envelope", message["envelope_json"]["$literal"].AsString);
        Assert.Equal(correlationId, subscriber["correlation_id"]["$literal"].AsString);
        Assert.Equal("$instance", subscriber["instance_id"]["$literal"].AsString);
    }

    /// <summary>
    /// Fixpoint r1 GS3#5: the channel's document classes lacked <c>[BsonIgnoreExtraElements]</c>
    /// (the transport and flow-store documents carry it), so the next element a newer build adds
    /// would throw on older hosts mid rolling deploy — after a claim had committed. Pre-fix:
    /// FormatException on the unknown element.
    /// </summary>
    [Fact]
    public void ChannelDocuments_TolerateElementsANewerBuildAdded()
    {
        var id = Guid.NewGuid();
        var message = BsonSerializer.Deserialize<MongoChannelMessageDocument>(new BsonDocument
        {
            ["_id"] = new BsonBinaryData(id, GuidRepresentation.Standard),
            ["correlation_id"] = "corr",
            ["envelope_json"] = "{}",
            ["created_at"] = DateTime.UtcNow,
            ["expires_at"] = DateTime.UtcNow,
            ["future_field"] = 1
        });
        var subscriber = BsonSerializer.Deserialize<MongoChannelSubscriberDocument>(new BsonDocument
        {
            ["_id"] = "corr:1",
            ["correlation_id"] = "corr",
            ["future_field"] = 1
        });
        var recovery = BsonSerializer.Deserialize<MongoRecoveryStateDocument>(new BsonDocument
        {
            ["_id"] = "corr:1",
            ["correlation_id"] = "corr",
            ["future_field"] = 1
        });

        Assert.Equal(id, message.Id);
        Assert.Equal("corr", subscriber.CorrelationId);
        Assert.Equal("corr", recovery.CorrelationId);
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
        // Expiry is compared on the SERVER clock ($expr: expires_at > $$NOW), matching the
        // server-stamped expires_at — an app-clock comparison would refuse live claims under skew.
        Assert.Equal(
            new BsonDocument("$gt", new BsonArray { "$expires_at", "$$NOW" }),
            filter["$expr"].AsBsonDocument);

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
    public void ChannelSubscriptionStartPipeline_DrawsSequenceAndServerClockInOneUpdate()
    {
        var rendered = MongoDbChannelStore.BuildSubscriptionStartPipeline()
            .Render(RenderArgsFor<BsonDocument>());

        // One $set stage advances the sequence AND stamps the server clock — the atomicity the
        // same-tick tie-breaker relies on (a claim landing between a separate clock read and
        // sequence draw would resolve a legitimate fan-out delivery as history).
        var set = rendered.AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal(
            new BsonArray { new BsonDocument("$ifNull", new BsonArray { "$seq", 0L }), 1L },
            set["seq"]["$add"].AsBsonArray);
        Assert.Equal("$$NOW", set["drawn_at"].AsString);
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
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var claimed = new MongoTransportMessageDocument
        {
            Id = Guid.NewGuid(),
            Queue = "worker",
            Payload = """{"job":1}""",
            Headers = new Dictionary<string, string> { ["AR-Correlation-Id"] = "corr-claim" },
            Attempts = 3
        };
        FindOneAndUpdateOptions<BsonDocument, BsonDocument>? capturedOptions = null;
        var raw = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        raw
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<BsonDocument> _, UpdateDefinition<BsonDocument> _, FindOneAndUpdateOptions<BsonDocument, BsonDocument> options, CancellationToken _) => capturedOptions = options)
            .ReturnsAsync(claimed.ToBsonDocument());
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
        var store = CreateTransportStore(collection.Object, raw.Object);

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
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var raw = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning().ClaimsInOrder(
            new MongoTransportMessageDocument { Id = Guid.NewGuid(), Queue = "worker", Payload = "{}" }.ToBsonDocument(),
            new MongoTransportMessageDocument { Id = Guid.NewGuid(), Queue = "worker", Payload = "{}" }.ToBsonDocument(),
            null,
            new MongoTransportMessageDocument { Id = Guid.NewGuid(), Queue = "worker", Payload = "{}" }.ToBsonDocument());
        var store = CreateTransportStore(collection.Object, raw.Object);

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
        var projections = new List<BsonDocument?>();
        var replies = new Queue<MongoChannelMessageDocument?>(
        [
            new MongoChannelMessageDocument { Id = Guid.NewGuid(), CorrelationId = "corr" },
            null,
            new MongoChannelMessageDocument { Id = Guid.NewGuid(), CorrelationId = "corr" },
            null
        ]);
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns((FilterDefinition<MongoChannelMessageDocument> _, UpdateDefinition<MongoChannelMessageDocument> _, FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument> options, CancellationToken _) =>
            {
                projections.Add(options.Projection?.Render(new RenderArgs<MongoChannelMessageDocument>(
                    BsonSerializer.LookupSerializer<MongoChannelMessageDocument>(), BsonSerializer.SerializerRegistry)).Document);
                return Task.FromResult(replies.Dequeue()!);
            });
        var store = CreateChannelStore(collection);

        Assert.True(await store.TryClaimForDeliveryAsync(Guid.NewGuid(), CancellationToken.None));
        Assert.False(await store.TryClaimForDeliveryAsync(Guid.NewGuid(), CancellationToken.None));
        Assert.True(await store.TryClaimForRecoveryAsync(Guid.NewGuid(), CancellationToken.None));
        Assert.False(await store.TryClaimForRecoveryAsync(Guid.NewGuid(), CancellationToken.None));

        // Fixpoint r1 S5#17: a claim's caller only checks that a document came back, so only the
        // id travels — not the envelope. Pre-fix: no projection, the whole document returned.
        Assert.Equal(4, projections.Count);
        Assert.All(projections, projection => Assert.Equal(new BsonDocument("_id", 1), projection));
    }

    /// <summary>
    /// Fixpoint r1 S5#17: the insert returned the whole document although the caller already holds
    /// the envelope; it projects the envelope out. Pre-fix: no projection.
    /// </summary>
    [Fact]
    public async Task ChannelInsert_ReturnsTheStampsWithoutTheEnvelope()
    {
        var collection = new Mock<IMongoCollection<MongoChannelMessageDocument>>(MockBehavior.Loose);
        BsonDocument? projection = null;
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoChannelMessageDocument> _, UpdateDefinition<MongoChannelMessageDocument> _, FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument> options, CancellationToken _) =>
                projection = options.Projection?.Render(new RenderArgs<MongoChannelMessageDocument>(
                    BsonSerializer.LookupSerializer<MongoChannelMessageDocument>(), BsonSerializer.SerializerRegistry)).Document)
            .ReturnsAsync(new MongoChannelMessageDocument { Id = Guid.NewGuid(), CorrelationId = "corr", CreatedAtUtc = DateTime.UtcNow });
        var store = CreateChannelStore(collection);

        var message = await store.InsertMessageAsync(Guid.NewGuid(), "corr", "{\"envelope\":1}", TimeSpan.FromHours(1), CancellationToken.None);

        Assert.Equal(new BsonDocument("envelope_json", 0), projection);
        // The caller's own envelope, not the (projected-away) stored one.
        Assert.Equal("{\"envelope\":1}", message.EnvelopeJson);
    }

    /// <summary>
    /// Regression (r2 S5#1): under the bounded majority an insert whose wtimeout lapsed was already
    /// stored on the primary, but it failed the publish — not transient, so no same-id retry — and
    /// the ingress re-published it under a NEW message id: an Until waiter received the response
    /// once per attempt, then a failure envelope on top. The stored document is read back by id on
    /// the primary (a read waits on no replication) and reported stored, with its own stamps.
    /// </summary>
    [Fact]
    public async Task ChannelInsert_WhoseReplicationWaitLapsed_ReadsTheStoredDocumentBack()
    {
        var id = Guid.NewGuid();
        var createdAt = new DateTime(2031, 3, 14, 9, 26, 53, 589, DateTimeKind.Utc);
        var collection = new Mock<IMongoCollection<MongoChannelMessageDocument>>(MockBehavior.Loose);
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(MongoReplicationTimeouts.Command());
        collection.FindsReturning(new MongoChannelMessageDocument { Id = id, CorrelationId = "corr", CreatedAtUtc = createdAt });
        var store = CreateChannelStore(collection);

        var message = await store.InsertMessageAsync(id, "corr", "{\"envelope\":1}", TimeSpan.FromHours(1), CancellationToken.None);

        Assert.Equal(id, message.Id);
        Assert.Equal(new DateTimeOffset(createdAt, TimeSpan.Zero), message.CreatedAtUtc);
        Assert.Null(message.AckedAtUtc);
        Assert.Equal("{\"envelope\":1}", message.EnvelopeJson);
        collection.Verify(
            c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    /// <summary>
    /// Regression (r2 S5#1): a delivery claim whose wtimeout lapsed had already stamped acked_at on
    /// the primary — which the publisher's acknowledgement poll and its recovery claim both read as
    /// "delivered" — yet it threw, so the response was never dispatched: acknowledged and lost. The
    /// stamp the claim leaves decides it: acked_at present and not recovery-claimed. Fixpoint r2
    /// precommit E2: the read-back used to re-evaluate the claim's own filter, whose server-clock
    /// expiry check ran a wtimeout (~10 s) after the claim — a response that expired in between
    /// read back as unclaimed although the claim had stamped it, so it was acknowledged and never
    /// dispatched. The read-back filter carries no expiry clause.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ChannelDeliveryClaim_WhoseReplicationWaitLapsed_IsDecidedByTheClaimStamp(bool stillMatches)
    {
        var id = Guid.NewGuid();
        FilterDefinition<MongoChannelMessageDocument>? readBack = null;
        var collection = new Mock<IMongoCollection<MongoChannelMessageDocument>>(MockBehavior.Loose);
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(MongoReplicationTimeouts.Command());
        collection
            .Setup(c => c.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoChannelMessageDocument> filter, FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument> _, CancellationToken _) => readBack = filter)
            .ReturnsAsync(() => new MongoListCursor<MongoChannelMessageDocument>(
                stillMatches ? [new MongoChannelMessageDocument { Id = id }] : []));
        var store = CreateChannelStore(collection);

        Assert.Equal(stillMatches, await store.TryClaimForDeliveryAsync(id, CancellationToken.None));

        var rendered = readBack!.Render(ChannelRenderArgs());
        Assert.Equal(new BsonBinaryData(id, GuidRepresentation.Standard), rendered["_id"]);
        Assert.Equal(BsonBoolean.False, rendered["recovery_claimed"]);
        Assert.Equal(new BsonDocument("$ne", BsonNull.Value), rendered["acked_at"]);
        Assert.False(rendered.Contains("$expr"), rendered.ToJson());
        Assert.False(rendered.Contains("expires_at"), rendered.ToJson());
    }

    /// <summary>
    /// Fixpoint r2 precommit E1: the replication-timeout read-backs inherited the database's read
    /// concern. Under readConcernLevel=majority a read-back taken while the set is degraded reads
    /// the majority snapshot, which by definition lacks the write whose majority acknowledgement
    /// just lapsed: a recovery claim that won read back as lost (the publisher then treated the
    /// response as delivered, and no delivery claim could take it any more — lost with no record),
    /// a delivery claim as unclaimed, a stored insert as missing. The read-backs go through a
    /// local-concern handle. The mocks model it: the inherited handle cannot see the write, the
    /// local-concern handle can. Pre-fix every read-back went to the inherited handle.
    /// </summary>
    [Theory]
    [InlineData("insert")]
    [InlineData("delivery claim")]
    [InlineData("recovery claim")]
    public async Task ChannelReadBacks_AfterAReplicationTimeout_ReadAtLocalConcern(string operation)
    {
        var id = Guid.NewGuid();
        var collection = new Mock<IMongoCollection<MongoChannelMessageDocument>>(MockBehavior.Loose);
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(MongoReplicationTimeouts.Command());
        collection.FindsReturning<MongoChannelMessageDocument, MongoChannelMessageDocument>();
        var local = new Mock<IMongoCollection<MongoChannelMessageDocument>>(MockBehavior.Loose)
            .FindsReturning(new MongoChannelMessageDocument
            {
                Id = id,
                CorrelationId = "corr",
                CreatedAtUtc = DateTime.UtcNow,
                AckedAtUtc = operation == "delivery claim" ? DateTime.UtcNow : null,
                RecoveryClaimed = operation == "recovery claim"
            });
        var store = CreateChannelStore(collection, localConcernReads: local);

        switch (operation)
        {
            case "insert":
                Assert.Equal(id, (await store.InsertMessageAsync(id, "corr", "{}", TimeSpan.FromHours(1), CancellationToken.None)).Id);
                break;
            case "delivery claim":
                Assert.True(await store.TryClaimForDeliveryAsync(id, CancellationToken.None));
                break;
            default:
                Assert.True(await store.TryClaimForRecoveryAsync(id, CancellationToken.None));
                break;
        }

        collection.Verify(
            c => c.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    /// <summary>
    /// The claim's ack-sequence draw lapses the same way (the counter is bounded-majority too), and
    /// throwing there stalled every delivery claim for as long as the set stayed degraded. The $inc
    /// applied, so the counter's current value — read before the claim lands — stands in for it.
    /// </summary>
    [Fact]
    public async Task ChannelDeliveryClaim_WhoseSequenceDrawLapsed_UsesTheCounterReadBack()
    {
        UpdateDefinition<MongoChannelMessageDocument>? claimUpdate = null;
        var collection = new Mock<IMongoCollection<MongoChannelMessageDocument>>(MockBehavior.Loose);
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoChannelMessageDocument> _, UpdateDefinition<MongoChannelMessageDocument> update, FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument> _, CancellationToken _) => claimUpdate = update)
            .ReturnsAsync(new MongoChannelMessageDocument { Id = Guid.NewGuid() });
        var counters = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        counters
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(MongoReplicationTimeouts.Command());
        counters.FindsReturning(new BsonDocument { ["_id"] = "ack_seq", ["seq"] = 42L });
        var store = CreateChannelStore(collection, counters);

        Assert.True(await store.TryClaimForDeliveryAsync(Guid.NewGuid(), CancellationToken.None));

        var set = claimUpdate!.Render(ChannelRenderArgs()).AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal(42L, set["acked_seq"]["$cond"].AsBsonArray[1].ToInt64());
    }

    /// <summary>
    /// Fixpoint r2 precommit E1, the counter: its read-back inherited the database's read concern
    /// too, and under readConcernLevel=majority it read a value from before the lapsed draw (or no
    /// counter at all, which threw) — breaking the "at or past that draw" premise the ordering
    /// relies on. It reads through a local-concern handle. Pre-fix: the inherited handle answered
    /// (here: no counter yet), and the claim threw the replication timeout.
    /// </summary>
    [Fact]
    public async Task ChannelDeliveryClaim_WhoseSequenceDrawLapsed_ReadsTheCounterBackAtLocalConcern()
    {
        UpdateDefinition<MongoChannelMessageDocument>? claimUpdate = null;
        var collection = new Mock<IMongoCollection<MongoChannelMessageDocument>>(MockBehavior.Loose);
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoChannelMessageDocument> _, UpdateDefinition<MongoChannelMessageDocument> update, FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument> _, CancellationToken _) => claimUpdate = update)
            .ReturnsAsync(new MongoChannelMessageDocument { Id = Guid.NewGuid() });
        var counters = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        counters
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(MongoReplicationTimeouts.Command());
        counters.FindsReturning<BsonDocument, BsonDocument>();
        var localCounters = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning()
            .FindsReturning(new BsonDocument { ["_id"] = "ack_seq", ["seq"] = 42L });
        counters.Setup(c => c.WithReadConcern(ReadConcern.Local)).Returns(localCounters.Object);
        var store = CreateChannelStore(collection, counters);

        Assert.True(await store.TryClaimForDeliveryAsync(Guid.NewGuid(), CancellationToken.None));

        var set = claimUpdate!.Render(ChannelRenderArgs()).AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal(42L, set["acked_seq"]["$cond"].AsBsonArray[1].ToInt64());
    }

    /// <summary>
    /// Regression (r2 S5#1): the recovery claim likewise — it ran on the primary, and throwing
    /// failed a publish whose response was already stored, which the ingress re-published under a
    /// new id. recovery_claimed is set only by that claim and only while acked_at is null, so a
    /// document carrying it is one recovery won.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ChannelRecoveryClaim_WhoseReplicationWaitLapsed_IsDecidedByTheRecoveryFlag(bool recoveryClaimed)
    {
        var id = Guid.NewGuid();
        FilterDefinition<MongoChannelMessageDocument>? readBack = null;
        var collection = new Mock<IMongoCollection<MongoChannelMessageDocument>>(MockBehavior.Loose);
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(MongoReplicationTimeouts.Command());
        collection
            .Setup(c => c.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoChannelMessageDocument> filter, FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument> _, CancellationToken _) => readBack = filter)
            .ReturnsAsync(() => new MongoListCursor<MongoChannelMessageDocument>(
                recoveryClaimed ? [new MongoChannelMessageDocument { Id = id, RecoveryClaimed = true }] : []));
        var store = CreateChannelStore(collection);

        Assert.Equal(recoveryClaimed, await store.TryClaimForRecoveryAsync(id, CancellationToken.None));

        var rendered = readBack!.Render(ChannelRenderArgs());
        Assert.Equal(new BsonBinaryData(id, GuidRepresentation.Standard), rendered["_id"]);
        Assert.Equal(BsonBoolean.True, rendered["recovery_claimed"]);
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

    private static MongoDbTransportStore CreateTransportStore(
        IMongoCollection<MongoTransportMessageDocument> collection,
        IMongoCollection<BsonDocument>? raw = null)
    {
        var options = new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false };
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose).WithTestNamespace();
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(options.MessageCollection, It.IsAny<MongoCollectionSettings>()))
            .Returns(collection);
        if (raw is not null)
        {
            database
                .Setup(d => d.GetCollection<BsonDocument>(options.MessageCollection, It.IsAny<MongoCollectionSettings>()))
                .Returns(raw);
        }

        return new MongoDbTransportStore(database.Object, Options.Create(options));
    }

    private static MongoDbChannelStore CreateChannelStore(
        Mock<IMongoCollection<MongoChannelMessageDocument>> collection,
        Mock<IMongoCollection<BsonDocument>>? counters = null,
        Mock<IMongoCollection<MongoChannelMessageDocument>>? localConcernReads = null)
    {
        var options = new MongoDbAsyncResponseChannelOptions { AutoCreateIndexes = false };
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose);
        collection.SelfPinning();
        if (localConcernReads is not null)
            collection.Setup(c => c.WithReadConcern(ReadConcern.Local)).Returns(localConcernReads.SelfPinning().Object);
        database
            .Setup(d => d.GetCollection<MongoChannelMessageDocument>(options.MessageCollection, It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        database.WithLooseCollection<MongoRecoveryStateDocument>();
        database.WithLooseCollection<MongoChannelSubscriberDocument>();
        database.WithCounters();
        if (counters is not null)
        {
            database
                .Setup(d => d.GetCollection<BsonDocument>(options.MessageCollection + "_counters", It.IsAny<MongoCollectionSettings>()))
                .Returns(counters.Object);
        }

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
