using AsyncResponse.DurableFlows.MongoDB;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;
using Moq;
using System.Globalization;
using System.Net;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Covers the MongoDB durable-flow store's creation clock authority: a plain insert cannot
/// evaluate <c>$$NOW</c>, so <see cref="MongoDbFlowStateStore"/> stamps a fresh ledger from the
/// server's <c>hello.localTime</c> — the same authority every later <c>$$NOW</c> expiry/lease
/// comparison runs on — instead of the app clock.
/// </summary>
public sealed class MongoDbFlowStateStoreTests
{
    [Fact]
    public async Task TryCreate_StampsInsertedLedgerFromServerClock()
    {
        // Deliberately years away from the app clock so an app-clock stamp cannot pass by luck.
        var serverNow = new DateTime(2031, 3, 14, 9, 26, 53, 589, DateTimeKind.Utc);
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1, ["localTime"] = serverNow });

        Assert.True(await harness.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));

        Assert.NotNull(harness.Inserted);
        Assert.Equal(serverNow, harness.Inserted!.UpdatedAtUtc);
        Assert.Equal(serverNow.AddMinutes(5), harness.Inserted.ExpiresAtUtc);
        // One cheap round-trip per create, not one per attempt/step.
        harness.Database.Verify(
            item => item.RunCommandAsync(
                It.IsAny<Command<BsonDocument>>(),
                It.IsAny<ReadPreference>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task TryCreate_MissingLocalTime_FallsBackToAppClock()
    {
        // A mongo-compatible endpoint that omits localTime must not fail creates; the app clock
        // restores the pre-server-clock behavior.
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 });
        var before = DateTime.UtcNow;

        Assert.True(await harness.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));

        Assert.NotNull(harness.Inserted);
        Assert.InRange(harness.Inserted!.UpdatedAtUtc, before, DateTime.UtcNow);
    }

    [Theory]
    [MemberData(nameof(NonDateLocalTimes))]
    public async Task TryCreate_NonDateLocalTime_FallsBackToAppClock(BsonValue localTime)
    {
        // Only BsonDateTime implements ToUniversalTime — every other BsonValue throws
        // NotSupportedException. A mongo-compatible endpoint (Cosmos Mongo API, DocumentDB,
        // FerretDB) answering hello with a PRESENT non-date localTime must fall back exactly
        // like an absent one, not fail every create.
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1, ["localTime"] = localTime });
        var before = DateTime.UtcNow;

        Assert.True(await harness.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));

        Assert.NotNull(harness.Inserted);
        Assert.InRange(harness.Inserted!.UpdatedAtUtc, before, DateTime.UtcNow);
    }

    public static TheoryData<BsonValue> NonDateLocalTimes =>
    [
        BsonNull.Value,
        new BsonString("2031-03-14T09:26:53.589Z"),
        new BsonInt64(1_900_000_000_000)
    ];

    private static FlowState CreateState(string flowId) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(TestOnboardingFlow).FullName,
        InputTypeName = typeof(TestFlowInput).FullName,
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    [Fact]
    public void Construction_PinsLedgerReadsToThePrimary()
    {
        // Regression: the collection inherited whatever read preference the host-supplied
        // IMongoDatabase carried, so a readPreference=secondaryPreferred connection string routed
        // every ledger load to a possibly-lagging secondary — a stale revision replays an
        // already-checkpointed step, and a not-yet-replicated ledger reads as null, the one
        // answer callers ACK a wake-up on. Reads are pinned to the primary at construction
        // (DynamoDB pins ConsistentRead for the same reason).
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 });

        harness.Collection.Verify(c => c.WithReadPreference(ReadPreference.Primary), Times.Once);
    }

    [Fact]
    public async Task DisabledIndexCreation_WithoutAProvisionedTtlIndex_FailsWithActionableError()
    {
        // Regression: AutoCreateIndexes = false skipped index creation AND verification, so an
        // operator-provisioned collection without expireAfterSeconds lost the store's only
        // cleanup mechanism — the ledger collection grew without bound, with no error and no log
        // line (loads filter on ExpiresAtUtc, so every functional test stayed green). Cosmos and
        // DynamoDB hard-fail the same way when their server-side reaper is missing.
        var collection = new Mock<IMongoCollection<MongoFlowStateDocument>>();
        collection
            .Setup(c => c.WithReadPreference(It.IsAny<ReadPreference>()))
            .Returns(collection.Object);
        collection
            .Setup(c => c.WithWriteConcern(It.IsAny<WriteConcern>()))
            .Returns(collection.Object);

        // The provisioned collection lists ONLY the default _id index — no TTL reaper.
        var cursor = new Mock<IAsyncCursor<BsonDocument>>();
        cursor.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);
        cursor.SetupGet(c => c.Current).Returns(
        [
            new BsonDocument { ["name"] = "_id_", ["key"] = new BsonDocument("_id", 1) }
        ]);
        var indexes = new Mock<IMongoIndexManager<MongoFlowStateDocument>>();
        indexes.Setup(m => m.ListAsync(It.IsAny<CancellationToken>())).ReturnsAsync(cursor.Object);
        collection.SetupGet(c => c.Indexes).Returns(indexes.Object);

        var database = new Mock<IMongoDatabase>().WithTestNamespace();
        database
            .Setup(d => d.GetCollection<MongoFlowStateDocument>("flows", It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        using var store = new MongoDbFlowStateStore(database.Object, Options.Create(new MongoDbDurableFlowOptions
        {
            CollectionName = "flows",
            AutoCreateIndexes = false
        }));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        Assert.Contains("TTL index", error.Message, StringComparison.Ordinal);
        Assert.Contains("expires_at_utc", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DisabledIndexCreationAndOwnershipLedger_StillVerifiesTheTtlIndex()
    {
        // Regression (round 31): the round-30 TTL verification sat behind an early-out that
        // skipped EnsureCreatedAsync entirely when BOTH AutoCreateIndexes and UseOwnershipLedger
        // were false — the natural locked-down configuration (operator-provisioned indexes, no
        // ledger writes) the check exists to protect. With both off, a collection provisioned
        // without expireAfterSeconds grew without bound again, with no error and no log line.
        var collection = new Mock<IMongoCollection<MongoFlowStateDocument>>();
        collection
            .Setup(c => c.WithReadPreference(It.IsAny<ReadPreference>()))
            .Returns(collection.Object);
        collection
            .Setup(c => c.WithWriteConcern(It.IsAny<WriteConcern>()))
            .Returns(collection.Object);

        // The provisioned collection lists ONLY the default _id index — no TTL reaper.
        var cursor = new Mock<IAsyncCursor<BsonDocument>>();
        cursor.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);
        cursor.SetupGet(c => c.Current).Returns(
        [
            new BsonDocument { ["name"] = "_id_", ["key"] = new BsonDocument("_id", 1) }
        ]);
        var indexes = new Mock<IMongoIndexManager<MongoFlowStateDocument>>();
        indexes.Setup(m => m.ListAsync(It.IsAny<CancellationToken>())).ReturnsAsync(cursor.Object);
        collection.SetupGet(c => c.Indexes).Returns(indexes.Object);

        var database = new Mock<IMongoDatabase>().WithTestNamespace();
        database
            .Setup(d => d.GetCollection<MongoFlowStateDocument>("flows", It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        using var store = new MongoDbFlowStateStore(database.Object, Options.Create(new MongoDbDurableFlowOptions
        {
            CollectionName = "flows",
            AutoCreateIndexes = false,
            UseOwnershipLedger = false
        }));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        Assert.Contains("TTL index", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AutoCreatedIndex_ConflictingWithTheSameTtlIndexUnderAnotherName_IsAccepted()
    {
        // Regression: the TTL index the docs and the AutoCreateIndexes = false error prescribe —
        // createIndex({ expires_at_utc: 1 }, { expireAfterSeconds: 0 }) — is named
        // "expires_at_utc_1". With AutoCreateIndexes left at its default, the store's CreateOne
        // under "{collection}_expires_idx" then met 85 IndexOptionsConflict ("already exists with a
        // different name"), EnsureCreated failed, and every store operation failed with it. The
        // reaper the store needs is there; accept it without dropping anything.
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 }, autoCreateIndexes: true);
        var indexes = harness.IndexesConflicting(code: 85, listed: new BsonDocument
        {
            ["name"] = "expires_at_utc_1",
            ["key"] = new BsonDocument("expires_at_utc", 1),
            ["expireAfterSeconds"] = 0
        });

        Assert.True(await harness.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        indexes.Verify(m => m.DropOneAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);

        // A conflict that is NOT an equivalent reaper still fails startup, loudly.
        using var conflicting = new MongoHarness(new BsonDocument { ["ok"] = 1 }, autoCreateIndexes: true);
        conflicting.IndexesConflicting(code: 85, listed: new BsonDocument
        {
            ["name"] = "expires_at_utc_1",
            ["key"] = new BsonDocument("expires_at_utc", 1)
        });
        Assert.Equal(85, (await Assert.ThrowsAsync<MongoCommandException>(
            () => conflicting.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)))).Code);
    }

    [Fact]
    public async Task Load_ADocumentWhoseExpiryIsMissingOrNotADate_IsUnreadable_NotAbsent()
    {
        // Regression: a missing, string or numeric expires_at_utc sorts below any date, so the
        // `$gt $$NOW` live filter missed the document and LoadAsync answered "absent" — the one
        // answer the executor acknowledges a wake-up on — while the TTL monitor, which reaps only
        // dates, never removes it. A live-filter miss now asks, by id alone, whether such a
        // document is there.
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 });
        harness.FindsNothing();
        harness.Collection
            .Setup(item => item.CountDocumentsAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<CountOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);

        var unreadable = await Assert.ThrowsAsync<FlowStateUnreadableException>(() => harness.Store.LoadAsync("flow"));
        Assert.Equal(MongoDbFlowStateStore.MalformedExpiryReason("flows"), unreadable.Reason);

        // Nothing under the id at all: genuinely absent.
        harness.Collection
            .Setup(item => item.CountDocumentsAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<CountOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(0);
        Assert.Null(await harness.Store.LoadAsync("flow"));
    }

    [Fact]
    public async Task TryCreate_WhenTheCollidingLedgerExpiredDuringTheClockRead_ReplacesItOnASecondLook()
    {
        // Regression (store half of the lost-start window): step 1 found the old ledger still
        // live, it expired during the `hello` round trip, and the insert's duplicate key was
        // answered "exists" — the executor then loaded an absent ledger and acknowledged the
        // start job, so the new run was never created. The replace is tried once more first.
        using var harness = new MongoHarness(new BsonDocument { ["ok"] = 1 });
        harness.Collection
            .SetupSequence(item => item.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new UpdateResult.Acknowledged(0, 0, BsonNull.Value))
            .ReturnsAsync(new UpdateResult.Acknowledged(1, 1, BsonNull.Value));
        harness.Collection
            .Setup(item => item.InsertOneAsync(
                It.IsAny<MongoFlowStateDocument>(),
                It.IsAny<InsertOneOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(DuplicateKey());

        Assert.True(await harness.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));

        // A live owner still wins: the second look matches nothing either.
        using var owned = new MongoHarness(new BsonDocument { ["ok"] = 1 });
        owned.Collection
            .Setup(item => item.InsertOneAsync(
                It.IsAny<MongoFlowStateDocument>(),
                It.IsAny<InsertOneOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(DuplicateKey());
        Assert.False(await owned.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        owned.Collection.Verify(
            item => item.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    [Fact]
    public void LedgerDates_ArePinnedToBsonDates_WhateverDateTimeSerializerTheHostRegistered()
    {
        // Regression: the date fields used the process-wide DateTime serializer, so a host that
        // registered `new DateTimeSerializer(BsonType.String)` made every insert write a string
        // expiry — which never compares above $$NOW, so each new ledger read as absent and was
        // never reaped. The members carry their own serializer instead of asking the registry
        // (the process-wide case itself runs in an isolated load context, below).
        var classMap = BsonClassMap.LookupClassMap(typeof(MongoFlowStateDocument));
        Assert.IsType<UtcBsonDateSerializer>(classMap.GetMemberMap(nameof(MongoFlowStateDocument.ExpiresAtUtc)).GetSerializer());
        Assert.IsType<UtcBsonDateSerializer>(classMap.GetMemberMap(nameof(MongoFlowStateDocument.UpdatedAtUtc)).GetSerializer());
        Assert.IsType<NullableUtcBsonDateSerializer>(classMap.GetMemberMap(nameof(MongoFlowStateDocument.LeaseExpiresAtUtc)).GetSerializer());

        var now = new DateTime(2031, 3, 14, 9, 26, 53, 589, DateTimeKind.Utc);
        var document = new MongoFlowStateDocument { FlowId = "flow", ExpiresAtUtc = now, UpdatedAtUtc = now, LeaseExpiresAtUtc = now, Revision = 0 }
            .ToBsonDocument();
        Assert.Equal(BsonType.DateTime, document["expires_at_utc"].BsonType);
        Assert.Equal(BsonType.DateTime, document["updated_at_utc"].BsonType);
        Assert.Equal(BsonType.DateTime, document["lease_expires_at_utc"].BsonType);

        var read = BsonSerializer.Deserialize<MongoFlowStateDocument>(document);
        Assert.Equal(DateTimeKind.Utc, read.ExpiresAtUtc.Kind);
        Assert.Equal(now, read.ExpiresAtUtc);
        Assert.Equal(now, read.LeaseExpiresAtUtc);
        document.Remove("lease_expires_at_utc");
        Assert.Null(BsonSerializer.Deserialize<MongoFlowStateDocument>(document).LeaseExpiresAtUtc);
    }

    [Fact]
    public void LedgerDates_UnderAHostRegisteredCustomDateTimeSerializer_StillMapAndWriteBsonDates()
    {
        // Regression: the round that pinned the dates did it with [BsonDateTimeOptions], which does
        // not replace the member's serializer but RECONFIGURES whatever the global registry returns
        // for DateTime — and throws NotSupportedException ("… is not configurable using an attribute
        // of type BsonDateTimeOptionsAttribute") for anything but the driver's DateTimeSerializer.
        // A host with its own IBsonSerializer<DateTime> then failed the ledger's class map on first
        // use: every flow-store operation threw. The registry and class maps are process-wide and
        // cannot be reset, so the host's registration happens against PRIVATE copies of the driver
        // and the store, in their own load context — nothing leaks into the rest of the run.
        var context = new IsolatedBsonLoadContext();
        try
        {
            var probe = context.LoadFromAssemblyPath(typeof(MongoDbFlowStateStoreTests).Assembly.Location)
                .GetType(typeof(HostRegisteredDateSerializerProbe).FullName!, throwOnError: true)!
                .GetMethod(nameof(HostRegisteredDateSerializerProbe.Run), BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)!;
            var observed = (string[])probe.Invoke(null, BindingFlags.DoNotWrapExceptions, binder: null, parameters: null, culture: null)!;

            // The host's serializer really is what the (private) registry answers for DateTime…
            Assert.Equal(nameof(HostRegisteredDateSerializerProbe.StringDateSerializer), observed[0]);
            // …and the ledger still maps, writes BSON dates, and reads them back as UTC.
            Assert.Equal(["DateTime", "DateTime", "DateTime", "True"], observed[1..]);
        }
        finally
        {
            context.Unload();
        }

        // The registration stayed inside the isolated context.
        Assert.IsType<DateTimeSerializer>(BsonSerializer.LookupSerializer<DateTime>());
    }

    [Fact]
    public void MalformedExpiryReason_CarriesTheCleanupThatFreesTheId()
    {
        // Documents written by the old date bug (a non-date expires_at_utc under a host-wide
        // String/Document/Int64 DateTime representation) used to be replaced by the next create;
        // now nothing ever removes them, so the reason has to tell the operator how to.
        var reason = MongoDbFlowStateStore.MalformedExpiryReason("flows");

        Assert.Contains("expires_at_utc", reason, StringComparison.Ordinal);
        Assert.Contains("""db.getCollection("flows").deleteMany({ expires_at_utc: { $not: { $type: "date" } } })""", reason, StringComparison.Ordinal);
    }

    private static MongoWriteException DuplicateKey()
    {
        var connectionId = new ConnectionId(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017)));
        var writeError = (WriteError)Activator.CreateInstance(
            typeof(WriteError),
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            [ServerErrorCategory.DuplicateKey, 11000, "E11000 duplicate key error", new BsonDocument()],
            culture: null)!;
        return new MongoWriteException(connectionId, writeError, writeConcernError: null, innerException: null);
    }

    private sealed class MongoHarness : IDisposable
    {
        public MongoHarness(BsonDocument helloReply, bool autoCreateIndexes = false)
        {
            Database
                .Setup(item => item.GetCollection<MongoFlowStateDocument>("flows", It.IsAny<MongoCollectionSettings>()))
                .Returns(Collection.Object);
            // The store pins ledger reads to the primary at construction; the derived handle is
            // this same mock. The TTL-index stub satisfies the operator-schema verification that
            // AutoCreateIndexes = false now performs.
            Collection
                .Setup(item => item.WithReadPreference(It.IsAny<ReadPreference>()))
                .Returns(Collection.Object);
            Collection
                .Setup(item => item.WithWriteConcern(It.IsAny<WriteConcern>()))
                .Returns(Collection.Object);
            Collection.WithProvisionedTtlIndex();
            Database
                .Setup(item => item.RunCommandAsync(
                    It.IsAny<Command<BsonDocument>>(),
                    It.IsAny<ReadPreference>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(helloReply);
            // Step 1 (expired-ledger replace) matches nothing: the id is free, forcing the insert.
            Collection
                .Setup(item => item.UpdateOneAsync(
                    It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                    It.IsAny<UpdateDefinition<MongoFlowStateDocument>>(),
                    It.IsAny<UpdateOptions>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new UpdateResult.Acknowledged(0, 0, BsonNull.Value));
            Collection
                .Setup(item => item.InsertOneAsync(
                    It.IsAny<MongoFlowStateDocument>(),
                    It.IsAny<InsertOneOptions>(),
                    It.IsAny<CancellationToken>()))
                .Callback((MongoFlowStateDocument document, InsertOneOptions _, CancellationToken _) => Inserted = document)
                .Returns(Task.CompletedTask);
            Database.WithTestNamespace();
            Store = new MongoDbFlowStateStore(Database.Object, Options.Create(new MongoDbDurableFlowOptions
            {
                CollectionName = "flows",
                AutoCreateIndexes = autoCreateIndexes
            }));
        }

        /// <summary>CreateOne fails with <paramref name="code"/>; the listing then shows <paramref name="listed"/>.</summary>
        public Mock<IMongoIndexManager<MongoFlowStateDocument>> IndexesConflicting(int code, BsonDocument listed)
        {
            var cursor = new Mock<IAsyncCursor<BsonDocument>>();
            cursor.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(true)
                .ReturnsAsync(false);
            cursor.SetupGet(c => c.Current).Returns([new BsonDocument { ["name"] = "_id_", ["key"] = new BsonDocument("_id", 1) }, listed]);
            var indexes = new Mock<IMongoIndexManager<MongoFlowStateDocument>>();
            indexes.Setup(m => m.ListAsync(It.IsAny<CancellationToken>())).ReturnsAsync(cursor.Object);
            indexes
                .Setup(m => m.CreateOneAsync(
                    It.IsAny<CreateIndexModel<MongoFlowStateDocument>>(),
                    It.IsAny<CreateOneIndexOptions>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new MongoCommandException(
                    new ConnectionId(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017))),
                    "createIndexes failed",
                    new BsonDocument("createIndexes", "flows"),
                    new BsonDocument { ["ok"] = 0, ["code"] = code, ["errmsg"] = "Index already exists with a different name" }));
            Collection.SetupGet(c => c.Indexes).Returns(indexes.Object);
            return indexes;
        }

        /// <summary>The live-filter read finds no document.</summary>
        public void FindsNothing()
        {
            var cursor = new Mock<IAsyncCursor<MongoFlowStateDocument>>();
            cursor.Setup(c => c.MoveNextAsync(It.IsAny<CancellationToken>())).ReturnsAsync(false);
            Collection
                .Setup(item => item.FindAsync(
                    It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                    It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(cursor.Object);
        }

        public Mock<IMongoDatabase> Database { get; } = new();
        public Mock<IMongoCollection<MongoFlowStateDocument>> Collection { get; } = new();
        public MongoDbFlowStateStore Store { get; }
        public MongoFlowStateDocument? Inserted { get; private set; }

        public void Dispose() => Store.Dispose();
    }
}

/// <summary>
/// Private copies of the MongoDB driver and the Mongo flow store (their process-wide serializer
/// registry and class maps included); the runtime and everything else is shared with the default
/// context.
/// </summary>
internal sealed class IsolatedBsonLoadContext() : System.Runtime.Loader.AssemblyLoadContext($"asyncresponse-bson-{Guid.NewGuid():N}", isCollectible: true)
{
    private static readonly string Directory = Path.GetDirectoryName(typeof(IsolatedBsonLoadContext).Assembly.Location)!;
    private static readonly string StoreAssembly = typeof(MongoDbFlowStateStore).Assembly.GetName().Name!;

    protected override Assembly? Load(AssemblyName assemblyName)
    {
        var name = assemblyName.Name!;
        if (!name.StartsWith("MongoDB.", StringComparison.Ordinal) && name != StoreAssembly)
            return null;

        var path = Path.Combine(Directory, name + ".dll");
        return File.Exists(path) ? LoadFromAssemblyPath(path) : null;
    }
}

/// <summary>
/// Runs inside <see cref="IsolatedBsonLoadContext"/>: registers a host-wide DateTime serializer
/// that is NOT the driver's <see cref="DateTimeSerializer"/> (and writes strings), then maps,
/// writes, and reads a ledger document. Returns plain strings so nothing crosses the context
/// boundary as a driver type.
/// </summary>
internal static class HostRegisteredDateSerializerProbe
{
    public static string[] Run()
    {
        BsonSerializer.RegisterSerializer(typeof(DateTime), new StringDateSerializer());

        var now = new DateTime(2031, 3, 14, 9, 26, 53, 589, DateTimeKind.Utc);
        var document = new MongoFlowStateDocument { FlowId = "flow", ExpiresAtUtc = now, UpdatedAtUtc = now, LeaseExpiresAtUtc = now, Revision = 0 }
            .ToBsonDocument();
        var read = BsonSerializer.Deserialize<MongoFlowStateDocument>(document);

        return
        [
            BsonSerializer.LookupSerializer<DateTime>().GetType().Name,
            document["expires_at_utc"].BsonType.ToString(),
            document["updated_at_utc"].BsonType.ToString(),
            document["lease_expires_at_utc"].BsonType.ToString(),
            (read.ExpiresAtUtc == now && read.ExpiresAtUtc.Kind == DateTimeKind.Utc && read.LeaseExpiresAtUtc == now).ToString()
        ];
    }

    /// <summary>A host's own DateTime serializer, of the kind that wrote string expiries.</summary>
    internal sealed class StringDateSerializer : SerializerBase<DateTime>
    {
        public override DateTime Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
            => DateTime.Parse(context.Reader.ReadString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, DateTime value)
            => context.Writer.WriteString(value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture));
    }
}
