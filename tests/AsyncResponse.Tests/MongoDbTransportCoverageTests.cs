using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
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
        // filter compares available_at against the server's $$NOW, so inserts stamp epoch.
        Assert.All(sets, set => Assert.Equal(
            new BsonArray { "$available_at", new BsonDateTime(DateTime.UnixEpoch) },
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
    /// </summary>
    [Fact]
    public async Task Publish_PrunesDeadLettersWithAServerClockCutoff()
    {
        FilterDefinition<MongoTransportMessageDocument>? pruneFilter = null;
        DeleteOptions? pruneOptions = null;
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
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
            .ReturnsAsync(new DeleteResult.Acknowledged(0));
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions
        {
            AutoCreateIndexes = false,
            DeadLetterRetention = TimeSpan.FromMinutes(30)
        });
        using var store = CreateStore(collection.Object, options);

        await store.PublishAsync(Guid.NewGuid(), "worker", "{}", headers: null, CancellationToken.None);

        Assert.NotNull(pruneFilter);
        // Binary collation, like the claim: under a folding collection collation the prune matched
        // live-queue documents whose name differed only by case.
        Assert.NotNull(pruneOptions);
        Assert.Same(Collation.Simple, pruneOptions!.Collation);
        var rendered = pruneFilter!.Render(TransportRenderArgs());
        Assert.Equal(options.Value.DeadLetterQueue, rendered["queue"].AsString);
        Assert.True(rendered.Contains("$expr"), $"prune cutoff is not server-clock based: {rendered}");
        Assert.Equal("$created_at", rendered["$expr"]["$lt"].AsBsonArray[0]);
        Assert.Equal(
            new BsonArray { "$$NOW", 1_800_000d },
            rendered["$expr"]["$lt"].AsBsonArray[1]["$subtract"].AsBsonArray);
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
        FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument>? claimOptions = null;
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Callback((
                FilterDefinition<MongoTransportMessageDocument> _,
                UpdateDefinition<MongoTransportMessageDocument> _,
                FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument> options,
                CancellationToken _) => claimOptions = options)
            .ReturnsAsync((MongoTransportMessageDocument)null!);
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false });
        using var store = CreateStore(collection.Object, options);

        Assert.Null(await store.TryClaimAsync("worker", TimeSpan.FromSeconds(1), CancellationToken.None));

        Assert.NotNull(claimOptions);
        Assert.Same(Collation.Simple, claimOptions!.Collation);
        Assert.Equal(ReturnDocument.After, claimOptions.ReturnDocument);
    }

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
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(message);
        collection
            .Setup(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteResult.Acknowledged(1));

        var disabledOptions = Options.Create(new MongoDbAsyncResponseTransportOptions
        {
            AutoCreateIndexes = false,
            DeadLetterEnabled = false
        });
        using (var disabledStore = CreateStore(collection.Object, disabledOptions))
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
        using var enabledStore = CreateStore(collection.Object, enabledOptions);
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
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(message);
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
        using var store = CreateStore(collection.Object, options);
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
        () => ValueTask.FromResult(true));

    private static MongoDbTransportStore CreateStore(
        IMongoCollection<MongoTransportMessageDocument> collection,
        IOptions<MongoDbAsyncResponseTransportOptions> options)
        => new(Database(collection).Object, options);

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
