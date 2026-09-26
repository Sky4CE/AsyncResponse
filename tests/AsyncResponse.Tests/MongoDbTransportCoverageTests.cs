using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using Moq;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class MongoDbTransportCoverageTests
{
    [Fact]
    public async Task WorkerTransport_PublishesCorrelatedAndUncorrelatedJobs_AndReportsFailures()
    {
        var upserts = new List<(UpdateDefinition<MongoTransportMessageDocument> Update, UpdateOptions Options)>();
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        collection
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoTransportMessageDocument> _, UpdateDefinition<MongoTransportMessageDocument> update, UpdateOptions updateOptions, CancellationToken _) => upserts.Add((update, updateOptions)))
            .ReturnsAsync(new UpdateResult.Acknowledged(0, 0, BsonNull.Value));
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions
        {
            AutoCreateIndexes = false,
            PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
        });
        using var store = CreateStore(collection.Object, options);
        var transport = new MongoDbWorkerTransport(options, store);

        await transport.PublishAsync(Job("corr-mongo"));
        await transport.PublishAsync(Job(null));

        // A publish is an insert-if-absent upsert whose pipeline stamps created_at on the SERVER
        // clock; the rendered $set carries the headers as the {k,v} array-of-documents shape.
        Assert.Equal(2, upserts.Count);
        Assert.All(upserts, upsert => Assert.True(upsert.Options.IsUpsert));
        var sets = upserts
            .Select(upsert => upsert.Update.Render(TransportRenderArgs()).AsBsonArray[0]["$set"].AsBsonDocument)
            .ToArray();
        Assert.Equal(
            new BsonArray { new BsonDocument { ["k"] = options.Value.CorrelationIdHeader, ["v"] = "corr-mongo" } },
            sets[0]["headers"]["$ifNull"].AsBsonArray[1]["$literal"].AsBsonArray);
        Assert.Empty(sets[1]["headers"]["$ifNull"].AsBsonArray[1]["$literal"].AsBsonArray);
        // Fresh publishes must be claimable regardless of client/server clock skew: the claim
        // filter compares available_at against the server's $$NOW, so inserts stamp the server's
        // $$NOW too — the claim order's first key (r2 S9#5; they stamped epoch before).
        Assert.All(sets, set => Assert.Equal(
            new BsonArray { "$available_at", "$$NOW" },
            set["available_at"]["$ifNull"].AsBsonArray));

        collection
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("publish failed"));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(Job("corr-error")));
        Assert.Equal("publish failed", error.Message);
        await Assert.ThrowsAsync<ArgumentNullException>(() => transport.PublishAsync(null!));
    }

    /// <summary>
    /// The dead-letter prune must age rows on the SERVER clock ($$NOW), not this instance's: the
    /// dead letters it deletes were stamped by OTHER instances' publishes, and an app-clock cutoff
    /// let a behind-clock pruner destroy fresh dead letters the moment they arrived.
    /// <para>
    /// r2 S9#8: and it deletes in bounded batches — the ids of at most
    /// <c>OpportunisticPrune.BatchSize</c> eligible documents, then a <c>deleteMany</c> of those ids —
    /// like the PostgreSQL / SQL Server siblings. It was one unbounded <c>deleteMany</c>, which the
    /// first publish after <c>DeadLetterRetention</c> was enabled over a large backlog waited out
    /// in full, with the drain budget never applying.
    /// </para>
    /// </summary>
    [Fact]
    public async Task Publish_PrunesDeadLettersInBoundedBatchesWithAServerClockCutoff()
    {
        FilterDefinition<MongoTransportMessageDocument>? findFilter = null;
        FindOptions<MongoTransportMessageDocument, Guid>? findOptions = null;
        FilterDefinition<MongoTransportMessageDocument>? pruneFilter = null;
        DeleteOptions? pruneOptions = null;
        var expired = new[] { Guid.NewGuid(), Guid.NewGuid() };
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        collection
            .Setup(c => c.FindAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<FindOptions<MongoTransportMessageDocument, Guid>>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoTransportMessageDocument> filter, FindOptions<MongoTransportMessageDocument, Guid> options, CancellationToken _) =>
            {
                findFilter = filter;
                findOptions = options;
            })
            .ReturnsAsync(() => new MongoListCursor<Guid>(expired));
        collection
            .Setup(c => c.DeleteManyAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<DeleteOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoTransportMessageDocument> filter, DeleteOptions deleteOptions, CancellationToken _) =>
            {
                pruneFilter = filter;
                pruneOptions = deleteOptions;
            })
            .ReturnsAsync(new DeleteResult.Acknowledged(expired.Length));
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions
        {
            AutoCreateIndexes = false,
            DeadLetterRetention = TimeSpan.FromMinutes(30)
        });
        using var store = CreateStore(collection.Object, options);

        await store.PublishAsync(Guid.NewGuid(), "worker", "{}", headers: null, CancellationToken.None);

        // The lookup: bounded by the batch size, binary collation like the claim (under a folding
        // collection collation the prune matched live-queue documents whose name differed only by case).
        Assert.NotNull(findOptions);
        Assert.Equal(1000, findOptions!.Limit);
        Assert.Same(Collation.Simple, findOptions.Collation);
        var rendered = findFilter!.Render(TransportRenderArgs());
        Assert.Equal(options.Value.DeadLetterQueue, rendered["queue"].AsString);
        Assert.True(rendered.Contains("$expr"), $"prune cutoff is not server-clock based: {rendered}");
        Assert.Equal("$created_at", rendered["$expr"]["$lt"].AsBsonArray[0]);
        Assert.Equal(
            new BsonArray { "$$NOW", 1_800_000d },
            rendered["$expr"]["$lt"].AsBsonArray[1]["$subtract"].AsBsonArray);

        // The delete: exactly those ids, the eligibility filter re-applied.
        Assert.NotNull(pruneOptions);
        Assert.Same(Collation.Simple, pruneOptions!.Collation);
        var deleted = pruneFilter!.Render(TransportRenderArgs()).ToJson();
        Assert.Contains("$in", deleted, StringComparison.Ordinal);
        Assert.Contains("$created_at", deleted, StringComparison.Ordinal);
        // Short of a full batch: the backlog is gone, one pass.
        collection.Verify(
            c => c.DeleteManyAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<DeleteOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    /// <summary>
    /// Regression: the dead-letter prune runs AFTER the publish's upsert committed, and it was
    /// awaited bare (only PostgreSQL had been guarded) — so a prune that threw (a connection error,
    /// the caller's token firing mid-delete) reported a FAILED publish for a job that was already
    /// claimable, and the caller's retry ran it twice. The prune now swallows its own failure.
    /// </summary>
    [Fact]
    public async Task Publish_ThatCommitted_IsNotFailedByAThrowingDeadLetterPrune()
    {
        var upserts = 0;
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        collection
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback(() => upserts++)
            .ReturnsAsync(new UpdateResult.Acknowledged(0, 0, BsonNull.Value));
        collection.FindsReturning(Guid.NewGuid());
        collection
            .Setup(c => c.DeleteManyAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<DeleteOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TimeoutException("prune lost its connection"));
        var logger = new CollectingLogger();
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions
        {
            AutoCreateIndexes = false,
            UseOwnershipLedger = false,
            DeadLetterRetention = TimeSpan.FromMinutes(30)
        });
        using var store = new MongoDbTransportStore(Database(collection.Object).Object, options, logger: logger.For<MongoDbTransportStore>());

        await store.PublishAsync(Guid.NewGuid(), "worker", "{}", headers: null, CancellationToken.None);

        Assert.Equal(1, upserts);
        collection.Verify(
            c => c.DeleteManyAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<DeleteOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
        Assert.Contains(logger.Entries, entry => entry.Exception is TimeoutException);
    }

    /// <summary>
    /// Regression (round 33): the claim's FindOneAndUpdateOptions carried no collation. The three
    /// logical queues share one collection and are told apart by nothing but the queue field, so
    /// on an operator-created collection with a case- or accent-folding default collation the
    /// WORKER subscriber claimed RESPONSE-queue documents — which the ingress then dropped and
    /// ACKed with no dead-letter record. The claim now pins the binary (simple) collation, like
    /// the prune above (SQL Server BIN2 / PostgreSQL deterministic-collation parity).
    /// </summary>
    [Fact]
    public async Task Claim_PinsTheBinaryCollation_SoAFoldingCollectionCannotCrossRouteQueues()
    {
        FindOneAndUpdateOptions<BsonDocument, BsonDocument>? claimOptions = null;
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var database = Database(collection.Object);
        var raw = database.WithRawTransportMessages();
        raw
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .Callback((
                FilterDefinition<BsonDocument> _,
                UpdateDefinition<BsonDocument> _,
                FindOneAndUpdateOptions<BsonDocument, BsonDocument> options,
                CancellationToken _) => claimOptions = options)
            .ReturnsAsync((BsonDocument)null!);
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false });
        using var store = new MongoDbTransportStore(database.Object, options);

        Assert.Null(await store.TryClaimAsync("worker", TimeSpan.FromSeconds(1), CancellationToken.None));

        Assert.NotNull(claimOptions);
        Assert.Same(Collation.Simple, claimOptions!.Collation);
        Assert.Equal(ReturnDocument.After, claimOptions.ReturnDocument);
    }

    /// <summary>
    /// Regression (r2 S9#5): the claim sorted by <c>created_at</c> behind the claim index
    /// <c>(queue, available_at, created_at)</c> — a sort key after a range key the index cannot
    /// order by, so every claim sorted all of the queue's due documents in memory to take one, and
    /// a NAKed or delayed document jumped ahead of everything published while it waited. It sorts
    /// by the index's own key order now (PostgreSQL / SQL Server parity), which immediate publishes
    /// support by stamping <c>available_at = $$NOW</c> instead of epoch.
    /// </summary>
    [Fact]
    public async Task Claim_SortsByTheClaimIndexKeyOrder()
    {
        FindOneAndUpdateOptions<BsonDocument, BsonDocument>? claimOptions = null;
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var database = Database(collection.Object);
        var raw = database.WithRawTransportMessages();
        raw
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .Callback((
                FilterDefinition<BsonDocument> _,
                UpdateDefinition<BsonDocument> _,
                FindOneAndUpdateOptions<BsonDocument, BsonDocument> options,
                CancellationToken _) => claimOptions = options)
            .ReturnsAsync((BsonDocument)null!);
        using var store = new MongoDbTransportStore(database.Object, Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false }));

        Assert.Null(await store.TryClaimAsync("worker", TimeSpan.FromSeconds(1), CancellationToken.None));

        var sort = claimOptions!.Sort.Render(new RenderArgs<BsonDocument>(BsonDocumentSerializer.Instance, BsonSerializer.SerializerRegistry));
        Assert.Equal(new BsonDocument { ["available_at"] = 1, ["created_at"] = 1 }, sort);
    }

    /// <summary>
    /// Regression (r2 S9#3/S4#3): under the bounded majority a publish whose wtimeout lapsed was
    /// already applied on the primary — subscribers claim from there, so the job runs — yet it
    /// surfaced as a failed publish: not transient, so not retried, and the caller's own retry
    /// published a NEW job (the flow engine re-parks the same way). A same-id retry would be no
    /// safer, since the job may be claimed, run and deleted inside the retry window and the upsert
    /// then re-creates it. It now counts as published, with a warning, and is never retried.
    /// </summary>
    [Fact]
    public async Task Publish_WhoseReplicationWaitLapsed_CountsAsPublished_WithoutARetry()
    {
        var upserts = 0;
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        collection
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback(() => upserts++)
            .ThrowsAsync(MongoReplicationTimeouts.Write());
        var logger = new CollectingLogger();
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions
        {
            AutoCreateIndexes = false,
            UseOwnershipLedger = false,
            PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
        });
        using var store = new MongoDbTransportStore(Database(collection.Object).Object, options, logger: logger.For<MongoDbTransportStore>());
        var transport = new MongoDbWorkerTransport(options, store);

        await transport.PublishAsync(Job("corr-lapsed"));

        Assert.Equal(1, upserts);
        Assert.Contains(logger.Messages, message => message.Contains("majority acknowledgement timed out", StringComparison.Ordinal));
    }

    /// <summary>
    /// The burial's dead-letter insert takes the same path: a copy written on the primary under a
    /// lapsed wtimeout is a copy, and the burial proceeds to delete the source — it reported "no
    /// dead-letter copy could be written" and released the job for retry before.
    /// </summary>
    [Fact]
    public async Task DeadLetter_WhoseInsertReplicationWaitLapsed_StillBuries()
    {
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        collection
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(MongoReplicationTimeouts.Write());
        collection
            .Setup(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteResult.Acknowledged(1));
        var database = Database(collection.Object);
        database.WithRawTransportMessages().ClaimsInOrder(
            new MongoTransportMessageDocument { Id = Guid.NewGuid(), Queue = "worker", Payload = "{}", Attempts = 5 }.ToBsonDocument());
        using var store = new MongoDbTransportStore(
            database.Object,
            Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false, UseOwnershipLedger = false }));

        var delivery = await store.TryClaimAsync("worker", TimeSpan.FromSeconds(30), CancellationToken.None);

        Assert.True(await delivery!.DeadLetterAsync(new InvalidOperationException("poison"), true, CancellationToken.None));
        collection.Verify(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    /// <summary>
    /// Regression (r2 GS3#4): every insert event woke every subscriber of the queue on every
    /// process — a DELAYED publish too (a flow timer, a redelay hop), for a document none of them
    /// could claim yet. An insert whose <c>available_at</c> is later than its write time
    /// (<c>clusterTime</c>, second resolution, plus a second of slack) no longer wakes; the claim
    /// poll picks it up once due. Anything missing or unreadable still wakes.
    /// </summary>
    [Fact]
    public void QueueWake_SkipsDocumentsNotYetDue()
    {
        var writtenAt = new BsonTimestamp(1_900_000_000, 1);
        var at = DateTimeOffset.FromUnixTimeSeconds(1_900_000_000);
        BsonDocument Event(BsonValue? availableAt, bool withClusterTime = true)
        {
            var change = new BsonDocument { ["operationType"] = "insert" };
            if (withClusterTime)
                change["clusterTime"] = writtenAt;
            change["fullDocument"] = availableAt is null ? new BsonDocument() : new BsonDocument("available_at", availableAt);
            return change;
        }

        Assert.True(MongoDbTransportStore.IsClaimableOnArrival(Event(new BsonDateTime(at.AddMilliseconds(400).UtcDateTime))));
        Assert.True(MongoDbTransportStore.IsClaimableOnArrival(Event(new BsonDateTime(at.AddSeconds(1).UtcDateTime))));
        Assert.True(MongoDbTransportStore.IsClaimableOnArrival(Event(new BsonDateTime(DateTime.UnixEpoch))));
        Assert.False(MongoDbTransportStore.IsClaimableOnArrival(Event(new BsonDateTime(at.AddSeconds(30).UtcDateTime))));

        // Unknown shapes wake: an extra wake costs one empty claim, a skipped one a poll interval.
        Assert.True(MongoDbTransportStore.IsClaimableOnArrival(Event(availableAt: null)));
        Assert.True(MongoDbTransportStore.IsClaimableOnArrival(Event("soon")));
        Assert.True(MongoDbTransportStore.IsClaimableOnArrival(Event(new BsonDateTime(at.AddSeconds(30).UtcDateTime), withClusterTime: false)));
        Assert.True(MongoDbTransportStore.IsClaimableOnArrival(new BsonDocument("operationType", "insert")));
        Assert.True(MongoDbTransportStore.IsClaimableOnArrival(null));
    }

    /// <summary>
    /// ...and the watch applies it per event: one due and one delayed insert wake the queue once.
    /// </summary>
    [Fact]
    public async Task WatchQueue_WakesOnlyForDocumentsDueOnArrival()
    {
        var writtenAt = new BsonTimestamp(1_900_000_000, 1);
        var at = DateTimeOffset.FromUnixTimeSeconds(1_900_000_000);
        ChangeStreamDocument<MongoTransportMessageDocument> Change(DateTimeOffset availableAt) => new(
            new BsonDocument
            {
                ["operationType"] = "insert",
                ["clusterTime"] = writtenAt,
                // A foreign ObjectId _id: the wake must not read the typed full document.
                ["fullDocument"] = new BsonDocument { ["_id"] = ObjectId.GenerateNewId(), ["available_at"] = new BsonDateTime(availableAt.UtcDateTime) }
            },
            BsonSerializer.LookupSerializer<MongoTransportMessageDocument>());
        var cursor = new Mock<IChangeStreamCursor<ChangeStreamDocument<MongoTransportMessageDocument>>>();
        cursor.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>())).ReturnsAsync(true).ReturnsAsync(false);
        cursor.SetupGet(c => c.Current).Returns([Change(at), Change(at.AddMinutes(5))]);
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        collection
            .Setup(c => c.WatchAsync(
                It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoTransportMessageDocument>, ChangeStreamDocument<MongoTransportMessageDocument>>>(),
                It.IsAny<ChangeStreamOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursor.Object);
        using var store = CreateStore(collection.Object, Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false, UseOwnershipLedger = false }));
        var wakes = 0;

        await store.WatchQueueAsync("worker", () => { wakes++; return Task.CompletedTask; }, CancellationToken.None);

        Assert.Equal(1, wakes);
    }

    /// <summary>
    /// Regression (r2 GS3#5): with AutoCreateIndexes on, a claim index the collection already
    /// carries under ANOTHER name — the default-named one the AutoCreateIndexes = false warning
    /// tells operators to create — made createIndexes fail with 85 IndexOptionsConflict, and every
    /// operation rethrew it (each runs EnsureCreated first). An equivalent index is accepted; one
    /// that differs in what it indexes still fails loudly — and so does a HIDDEN one (fixpoint r2
    /// precommit): the planner never uses it, so accepting it silently turned every claim into a
    /// collection scan. Pre-fix the hidden case was accepted.
    /// </summary>
    [Theory]
    [InlineData(false, false, false)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    public async Task EnsureCreated_AcceptsAnEquivalentClaimIndexUnderAnotherName(bool unique, bool hidden, bool fails)
    {
        var existing = new BsonDocument
        {
            ["name"] = "queue_1_available_at_1_created_at_1",
            ["key"] = new BsonDocument { ["queue"] = 1.0, ["available_at"] = 1.0, ["created_at"] = 1.0 }
        };
        if (unique)
            existing["unique"] = true;
        if (hidden)
            existing["hidden"] = true;
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning().WithListedIndexes(existing);
        var indexes = Mock.Get(collection.Object.Indexes);
        indexes
            .Setup(m => m.CreateOneAsync(
                It.Is<CreateIndexModel<MongoTransportMessageDocument>>(model => model.Options.Name!.EndsWith("_claim_idx", StringComparison.Ordinal)),
                It.IsAny<CreateOneIndexOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(IndexConflict());
        using var store = CreateStore(collection.Object, Options.Create(new MongoDbAsyncResponseTransportOptions { UseOwnershipLedger = false }));

        if (fails)
        {
            Assert.Equal(85, (await Assert.ThrowsAsync<MongoCommandException>(() => store.EnsureCreatedAsync())).Code);
            return;
        }

        await store.EnsureCreatedAsync();
        indexes.Verify(m => m.DropOneAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        indexes.Verify(m => m.CreateOneAsync(
            It.Is<CreateIndexModel<MongoTransportMessageDocument>>(model => model.Options.Name!.EndsWith("_created_idx", StringComparison.Ordinal)),
            It.IsAny<CreateOneIndexOptions>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    private static MongoCommandException IndexConflict()
        => new(
            MongoReplicationTimeouts.Connection,
            "createIndexes failed",
            new BsonDocument("createIndexes", "asyncresponse_transport_messages"),
            new BsonDocument { ["ok"] = 0, ["code"] = 85, ["errmsg"] = "Index already exists with a different name" });

    private static RenderArgs<MongoTransportMessageDocument> TransportRenderArgs()
        => new(BsonSerializer.LookupSerializer<MongoTransportMessageDocument>(), BsonSerializer.SerializerRegistry);

    [Fact]
    public void WorkerTransport_PublicConstructor_UsesTheProvidedDatabase()
    {
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false });
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>().SelfPinning();
        var database = Database(collection.Object);

        Assert.NotNull(new MongoDbWorkerTransport(options, database.Object));
    }

    [Fact]
    public async Task Store_DeadLetterDisabledDeletesOriginal_AndInsertFailureReturnsFalse()
    {
        var message = new MongoTransportMessageDocument
        {
            Id = Guid.NewGuid(),
            Queue = "worker",
            Payload = "{}",
            Headers = new Dictionary<string, string>()
        };
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var claims = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        claims
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => message.ToBsonDocument());
        collection
            .Setup(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteResult.Acknowledged(1));

        var disabledOptions = Options.Create(new MongoDbAsyncResponseTransportOptions
        {
            AutoCreateIndexes = false,
            DeadLetterEnabled = false
        });
        using (var disabledStore = CreateStore(collection.Object, disabledOptions, claims.Object))
        {
            var delivery = Assert.IsType<MongoDbTransportDelivery>(
                await disabledStore.TryClaimAsync("worker", TimeSpan.FromSeconds(1), CancellationToken.None));
            Assert.True(await delivery.DeadLetterAsync(new InvalidOperationException("ignored"), true, CancellationToken.None));
        }
        collection.Verify(
            c => c.DeleteOneAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<CancellationToken>()),
            Times.Once);

        collection
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("dlq unavailable"));
        var enabledOptions = Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false });
        using var enabledStore = CreateStore(collection.Object, enabledOptions, claims.Object);
        var enabledDelivery = Assert.IsType<MongoDbTransportDelivery>(
            await enabledStore.TryClaimAsync("worker", TimeSpan.FromSeconds(1), CancellationToken.None));

        Assert.False(await enabledDelivery.DeadLetterAsync(
            new InvalidOperationException("poison"),
            false,
            CancellationToken.None));
    }

    [Fact]
    public async Task Store_DeadLetterOnAStaleClaim_NoOpsAndKeepsTheDlqCopy()
    {
        // Round 29 made a stale claim's burial no-op (fenced delete result honored); round 31
        // removed the COMPENSATING delete of the DLQ copy that no-op used to run: the id is
        // deterministic, so a peer that also reached the cap buried into the SAME document, and
        // compensating away "our" copy erased the peer's just-logged burial — the message
        // vanished from both the live queue and the DLQ. The copy is kept: the worst it can be
        // is a spurious, prunable DLQ entry for a message whose new owner later succeeds.
        var sourceId = Guid.NewGuid();
        var message = new MongoTransportMessageDocument
        {
            Id = sourceId,
            Queue = "worker",
            Payload = "{}",
            Headers = new Dictionary<string, string>()
        };
        var deletes = new List<BsonDocument>();
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var claims = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        claims
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => message.ToBsonDocument());
        collection
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new UpdateResult.Acknowledged(0, 1, BsonNull.Value));
        collection
            .Setup(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoTransportMessageDocument> filter, CancellationToken _)
                => deletes.Add(filter.Render(TransportRenderArgs())))
            // The fence lost: the lease lapsed and a peer re-claimed the document.
            .ReturnsAsync(new DeleteResult.Acknowledged(0));

        var options = Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false });
        using var store = CreateStore(collection.Object, options, claims.Object);
        var delivery = Assert.IsType<MongoDbTransportDelivery>(
            await store.TryClaimAsync("worker", TimeSpan.FromSeconds(1), CancellationToken.None));

        Assert.False(await delivery.DeadLetterAsync(new InvalidOperationException("poison"), true, CancellationToken.None));

        // One delete only: the fenced source removal that did not match. The DLQ copy written a
        // moment earlier is deliberately NOT compensated away — it may be a racing peer's burial.
        var fenced = Assert.Single(deletes);
        Assert.Equal(sourceId, fenced["_id"].AsGuid);
    }

    [Fact]
    public async Task SubscriberAdapters_ExposeConfiguredRolesAndForwardPayloads()
    {
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false });
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        using var store = CreateStore(collection.Object, options);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync("worker-json")).Returns(Task.CompletedTask);
        ingress.Setup(i => i.HandleResponseMessageAsync("response-json", "corr-response")).Returns(Task.CompletedTask);
        var worker = new MongoDbWorkerSubscriber(options, store, ingress.Object, NullLogger<MongoDbWorkerSubscriber>.Instance);
        var response = new MongoDbResponseIngressSubscriber(options, store, ingress.Object, NullLogger<MongoDbResponseIngressSubscriber>.Instance);

        Assert.Equal(options.Value.WorkerQueue, GetProperty<string>(worker, "Queue"));
        Assert.Same(options.Value.WorkerSubscriber, GetProperty<MongoDbSubscriberOptions>(worker, "SubscriberOptions"));
        Assert.Equal(MongoDbSubscriberRole.Worker, GetProperty<MongoDbSubscriberRole>(worker, "Role"));
        await InvokeHandlerAsync(worker, Delivery("worker-json"));

        Assert.Equal(options.Value.ResponseQueue, GetProperty<string>(response, "Queue"));
        Assert.Same(options.Value.ResponseSubscriber, GetProperty<MongoDbSubscriberOptions>(response, "SubscriberOptions"));
        Assert.Equal(MongoDbSubscriberRole.ResponseIngress, GetProperty<MongoDbSubscriberRole>(response, "Role"));
        await InvokeHandlerAsync(response, Delivery(
            "response-json",
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [options.Value.CorrelationIdHeader] = "corr-response"
            }));

        ingress.VerifyAll();
    }

    [Fact]
    public void Registration_UsesSharedClientOrOwnedClient_WhenNoDatabaseIsRegistered()
    {
        var database = new Mock<IMongoDatabase>().WithTestNamespace();
        // The store pins its collection handle to the primary at construction, so the database
        // mock must hand back a (self-pinning) collection rather than Moq's null default.
        database.WithLooseCollection<MongoTransportMessageDocument>();
        var client = new Mock<IMongoClient>();
        client.Setup(c => c.GetDatabase("shared_db", It.IsAny<MongoDatabaseSettings>())).Returns(database.Object);
        var sharedServices = Services();
        sharedServices.AddSingleton(client.Object);
        sharedServices.AddAsyncResponse().WithInMemoryChannel().WithMongoDbTransport(options => options.DatabaseName = "shared_db");
        using (var sharedProvider = sharedServices.BuildServiceProvider())
            Assert.NotNull(sharedProvider.GetRequiredService<MongoDbTransportStore>());

        var ownedServices = Services();
        ownedServices.AddAsyncResponse().WithInMemoryChannel().WithMongoDbTransport(options =>
        {
            options.DatabaseName = "owned_db";
            options.ConnectionString = "mongodb://localhost:27017";
        });
        using var ownedProvider = ownedServices.BuildServiceProvider();
        Assert.NotNull(ownedProvider.GetRequiredService<MongoDbTransportStore>());
    }

    [Fact]
    public void Store_PinsTheMessageCollectionToThePrimary()
    {
        // Regression (channel / flow-store parity): the transport's handle was not pinned, so a
        // secondaryPreferred client routed the change-stream wake to a lagging secondary and
        // worker jobs woke at replication lag — delivery quietly degraded to EmptyPollDelay polling.
        var database = new Mock<IMongoDatabase>().WithTestNamespace();
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);

        _ = new MongoDbTransportStore(database.Object, Options.Create(new MongoDbAsyncResponseTransportOptions { UseOwnershipLedger = false }));

        collection.Verify(c => c.WithReadPreference(ReadPreference.Primary), Times.Once);
    }

    [Fact]
    public async Task EnsureCreated_WithoutIndexDdl_WarnsWhenTheClaimIndexIsMissing()
    {
        // Regression: with AutoCreateIndexes = false the transport set _created and returned —
        // both MongoDB siblings verify (the channel warns, the flow store throws). A least-privilege
        // deployment whose migration omitted the claim index then paid a full collection scan on
        // every poll tick, per subscriber, with no error and no log line.
        var database = new Mock<IMongoDatabase>().WithTestNamespace();
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var indexes = new Mock<IMongoIndexManager<MongoTransportMessageDocument>>(MockBehavior.Loose);
        indexes
            .Setup(m => m.ListAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new BsonListCursor([]));
        collection.SetupGet(c => c.Indexes).Returns(indexes.Object);
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        var logger = new CollectingLogger();
        var store = new MongoDbTransportStore(
            database.Object,
            Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false, UseOwnershipLedger = false }),
            logger: logger.For<MongoDbTransportStore>());

        await store.EnsureCreatedAsync();

        Assert.Contains(logger.Messages, message => message.Contains("no index leading on 'queue'", StringComparison.Ordinal));
        indexes.Verify(m => m.CreateOneAsync(
            It.IsAny<CreateIndexModel<MongoTransportMessageDocument>>(),
            It.IsAny<CreateOneIndexOptions>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    private static WorkerJobEnvelope Job(string? correlationId) => new()
    {
        CorrelationId = correlationId,
        Call = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(MongoDbTransportCoverageTests).FullName!,
            MethodName = nameof(Job),
            Params = []
        }
    };

    private static MongoDbTransportDelivery Delivery(
        string payload,
        IReadOnlyDictionary<string, string>? headers = null) => new(
        Guid.NewGuid(),
        "queue",
        payload,
        headers ?? new Dictionary<string, string>(),
        1,
        () => ValueTask.CompletedTask,
        _ => ValueTask.CompletedTask,
        (_, _, _) => ValueTask.FromResult(true),
        _ => ValueTask.FromResult(true));

    private static MongoDbTransportStore CreateStore(
        IMongoCollection<MongoTransportMessageDocument> collection,
        IOptions<MongoDbAsyncResponseTransportOptions> options,
        IMongoCollection<BsonDocument>? claims = null,
        ILogger<MongoDbTransportStore>? logger = null)
    {
        var database = Database(collection);
        if (claims is not null)
        {
            database
                .Setup(d => d.GetCollection<BsonDocument>(options.Value.MessageCollection, It.IsAny<MongoCollectionSettings>()))
                .Returns(claims);
        }

        return new(database.Object, options, logger: logger);
    }

    /// <summary>
    /// Fixpoint r2 precommit J7: the unreadable-document burial logged its Error BEFORE burying,
    /// unguarded, so a throwing logging provider (Microsoft.Extensions.Logging rethrows a provider's
    /// failure) aborted the burial: the poison document stayed claimed and came back every
    /// LockTimeout, faulting the claim each time. The burial now runs first and the line after it,
    /// guarded. Red on the old code: the claim threw and the document was never deleted.
    /// </summary>
    [Fact]
    public async Task UnreadableDocumentBurial_UnderAThrowingLogger_StillBuriesAndDeletesTheDocument()
    {
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var claims = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        // A foreign producer's ObjectId _id: unreadable by the class map, then the queue is empty.
        claims.ClaimsInOrder(new BsonDocument { ["_id"] = ObjectId.GenerateNewId(), ["queue"] = "worker", ["payload"] = "{}" }, null);
        claims
            .Setup(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<BsonDocument>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteResult.Acknowledged(1));
        var logger = new CollectingLogger { ThrowOnMessageContaining = "could not be read as a transport message" };
        using var store = CreateStore(
            collection.Object,
            Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false, UseOwnershipLedger = false }),
            claims.Object,
            logger.For<MongoDbTransportStore>());

        Assert.Null(await store.TryClaimAsync("worker", TimeSpan.FromSeconds(30), CancellationToken.None));

        claims.Verify(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<BsonDocument>>(), It.IsAny<CancellationToken>()), Times.Once);
        Assert.Contains(logger.Messages, message => message.Contains("dead-lettered it without executing it", StringComparison.Ordinal));
    }

    /// <summary>
    /// Fixpoint r2 precommit pass 2: moving the unreadable-document Error after the burial meant a
    /// burial that fails on every claim (a dead-letter copy over the document size limit) never
    /// logged which document is poison or why it could not be read — the claim just threw once per
    /// LockTimeout. A failed burial now logs the document and the read error, then propagates.
    /// Red on the pass-1 code: no line named the document.
    /// </summary>
    [Fact]
    public async Task UnreadableDocumentBurial_ThatFails_StillNamesTheDocumentBeforeItPropagates()
    {
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        collection
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("dead-letter copy too large"));
        var claims = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        var poisonId = ObjectId.GenerateNewId();
        claims.ClaimsInOrder(new BsonDocument { ["_id"] = poisonId, ["queue"] = "worker", ["payload"] = "{}" }, null);
        var logger = new CollectingLogger();
        using var store = CreateStore(
            collection.Object,
            Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false, UseOwnershipLedger = false }),
            claims.Object,
            logger.For<MongoDbTransportStore>());

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.TryClaimAsync("worker", TimeSpan.FromSeconds(30), CancellationToken.None));

        Assert.Equal("dead-letter copy too large", failure.Message);
        claims.Verify(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<BsonDocument>>(), It.IsAny<CancellationToken>()), Times.Never);
        Assert.Contains(
            logger.Messages,
            message => message.Contains(poisonId.ToString(), StringComparison.Ordinal)
                && message.Contains("burying it failed", StringComparison.Ordinal));
    }

    /// <summary>
    /// Fixpoint r2 precommit J7: a failed dead-letter write is reported to its caller as <c>false</c>
    /// (the redelivery consequence is decided from it), but the Error logged on the way ran
    /// unguarded, so a throwing logging provider turned that <c>false</c> into an exception out of
    /// the settlement. Red on the old code: DeadLetterAsync threw.
    /// </summary>
    [Fact]
    public async Task DeadLetterWriteFailure_UnderAThrowingLogger_StillReportsFalse()
    {
        var message = new MongoTransportMessageDocument
        {
            Id = Guid.NewGuid(),
            Queue = "worker",
            Payload = "{}",
            Headers = new Dictionary<string, string>()
        };
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        collection
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("dlq unavailable"));
        var claims = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        claims
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => message.ToBsonDocument());
        var logger = new CollectingLogger { ThrowOnMessageContaining = "Failed to write MongoDB dead-letter document" };
        using var store = CreateStore(
            collection.Object,
            Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false }),
            claims.Object,
            logger.For<MongoDbTransportStore>());
        var delivery = Assert.IsType<MongoDbTransportDelivery>(
            await store.TryClaimAsync("worker", TimeSpan.FromSeconds(1), CancellationToken.None));

        Assert.False(await delivery.DeadLetterAsync(new InvalidOperationException("poison"), true, CancellationToken.None));
    }

    private static Mock<IMongoDatabase> Database(IMongoCollection<MongoTransportMessageDocument> collection)
    {
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose).WithTestNamespace();
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(
                It.IsAny<string>(),
                It.IsAny<MongoCollectionSettings>()))
            .Returns(collection);
        return database;
    }

    private static ServiceCollection Services()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        return services;
    }

    [Fact]
    public async Task WorkerSubscriber_InvalidOptions_FailHostStartupSynchronously()
    {
        // Red-on-old (Hosting 10.0.10+): validation used to sit at the top of ExecuteAsync, which
        // BackgroundService.StartAsync no longer runs inline — StartAsync returned without
        // throwing and the misconfiguration surfaced late or never. Validation now runs in
        // StartAsync so a misconfigured subscriber fails host startup synchronously.
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions
        {
            AutoCreateIndexes = false,
            WorkerSubscriber = { AckMode = MongoDbAckMode.AckAfterEnqueue }
        });
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        using var store = CreateStore(collection.Object, options);
        var subscriber = new MongoDbWorkerSubscriber(options, store, Mock.Of<IAsyncResponseIngress>(), NullLogger<MongoDbWorkerSubscriber>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.StartAsync(CancellationToken.None));
        Assert.Contains("BackgroundWorkerCount", ex.Message, StringComparison.Ordinal);
    }

    private static T GetProperty<T>(object target, string name)
        => (T)target.GetType().GetProperty(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(target)!;

    private static Task InvokeHandlerAsync(object target, MongoDbTransportDelivery delivery)
        => (Task)target.GetType()
            .GetMethod("HandleMessageAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(target, [delivery, CancellationToken.None])!;
}
