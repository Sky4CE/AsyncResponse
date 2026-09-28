using System.Reflection;
using AsyncResponse.Channels.MongoDB;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.Transports.MongoDB;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The MongoDB shared sources (<c>src/Shared/Mongo*.cs</c>) are compiled into the channel, transport
/// and durable-flow packages separately, so a branch one package's tests reach is still dead code in
/// the other two. Each fact here runs against every compilation that links the file.
/// </summary>
public sealed class MongoSharedCoverageGapTests
{
    public static TheoryData<Type> AllAnchors =>
    [
        typeof(MongoDbAsyncResponseChannelOptions),
        typeof(MongoDbAsyncResponseTransportOptions),
        typeof(MongoDbDurableFlowOptions)
    ];

    /// <summary><c>MongoIndexes</c> is linked into the channel and transport only.</summary>
    public static TheoryData<Type> IndexAnchors =>
    [
        typeof(MongoDbAsyncResponseChannelOptions),
        typeof(MongoDbAsyncResponseTransportOptions)
    ];

    // ---- MongoOwnershipLedger ----

    /// <summary>
    /// A lapsed majority bound on the claim upsert reads the claim back at local concern and judges
    /// it like the before-image: our own claim passes, a foreign one is the actionable conflict.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllAnchors))]
    public async Task OwnershipLedger_AfterAReplicationTimeout_JudgesTheClaimReadBack(Type anchor)
    {
        var own = LedgerCollection(upsert: _ => throw MongoReplicationTimeouts.Command());
        var local = new Mock<IMongoCollection<BsonDocument>>().SelfPinning().FindsReturning(Claim("state-store", "state"));
        own.Setup(c => c.WithReadConcern(ReadConcern.Local)).Returns(local.Object);

        await ClaimAsync(anchor, LedgerDatabase(own).Object, "state-store", ("flows", "state"));

        var foreign = LedgerCollection(upsert: _ => throw MongoReplicationTimeouts.Command());
        var foreignLocal = new Mock<IMongoCollection<BsonDocument>>().SelfPinning().FindsReturning(Claim("channel", "messages"));
        foreign.Setup(c => c.WithReadConcern(ReadConcern.Local)).Returns(foreignLocal.Object);

        var conflict = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ClaimAsync(anchor, LedgerDatabase(foreign).Object, "state-store", ("flows", "state")));
        Assert.Contains("already claimed by the channel (messages)", conflict.Message, StringComparison.Ordinal);
        Assert.Contains("'appdb.flows'", conflict.Message, StringComparison.Ordinal);
        Assert.Contains("this state-store configured it as state", conflict.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The first-claim race's E11000, on either surface (a categorized write error, a command
    /// error code), is retried once; the retry resolves through the ownership check. A second
    /// E11000 is a real failure and propagates, as does any other command error.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllAnchors))]
    public async Task OwnershipLedger_RetriesTheDuplicateKeyRaceOnce_OnEitherSurface(Type anchor)
    {
        foreach (var duplicate in new MongoException[]
                 {
                     MongoReplicationTimeouts.Write(writeError: MongoReplicationTimeouts.DuplicateKeyError()),
                     CommandError(11000),
                     CommandError(11001),
                     CommandError(12582)
                 })
        {
            var calls = 0;
            var collection = LedgerCollection(upsert: _ => ++calls == 1 ? throw duplicate : Claim("state-store", "state"));

            await ClaimAsync(anchor, LedgerDatabase(collection).Object, "state-store", ("flows", "state"));
            Assert.Equal(2, calls);
        }

        var alwaysDuplicate = 0;
        var stuck = LedgerCollection(upsert: _ => { alwaysDuplicate++; throw CommandError(11000); });
        Assert.Equal(11000, (await Assert.ThrowsAsync<MongoCommandException>(
            () => ClaimAsync(anchor, LedgerDatabase(stuck).Object, "state-store", ("flows", "state")))).Code);
        Assert.Equal(2, alwaysDuplicate);

        // Not a duplicate key: no retry.
        var other = 0;
        var failing = LedgerCollection(upsert: _ => { other++; throw CommandError(13); });
        Assert.Equal(13, (await Assert.ThrowsAsync<MongoCommandException>(
            () => ClaimAsync(anchor, LedgerDatabase(failing).Object, "state-store", ("flows", "state")))).Code);
        Assert.Equal(1, other);
    }

    /// <summary>
    /// Every claim in the list is checked: the first is ours, the second is held by another
    /// component under another purpose, and the error names the second. The same component
    /// claiming the same collection for a DIFFERENT purpose is a conflict too.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllAnchors))]
    public async Task OwnershipLedger_RejectsAForeignClaim_AndTheSameComponentInAnotherRole(Type anchor)
    {
        var collection = LedgerCollection(upsert: filter => filter == "messages"
            ? Claim("worker", "jobs")
            : Claim("channel", filter));

        var conflict = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ClaimAsync(anchor, LedgerDatabase(collection).Object, "channel", ("responses", "responses"), ("messages", "messages")));
        Assert.Contains("'appdb.messages' is already claimed by the worker (jobs)", conflict.Message, StringComparison.Ordinal);
        Assert.Contains("asyncresponse_ownership", conflict.Message, StringComparison.Ordinal);

        var sameComponent = LedgerCollection(upsert: _ => Claim("channel", "counters"));
        var role = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ClaimAsync(anchor, LedgerDatabase(sameComponent).Object, "channel", ("messages", "messages")));
        Assert.Contains("already claimed by the channel (counters)", role.Message, StringComparison.Ordinal);
    }

    // ---- MongoNamespaceRegistry ----

    /// <summary>
    /// The in-container registry: a second component claiming a collection another component
    /// holds fails with both named; the same component re-claiming (a restart of the same
    /// registration) is idempotent; the same name in another database or cluster is independent.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllAnchors))]
    public void NamespaceRegistry_RejectsASecondComponent_ButNotTheSameOneOrAnotherNamespace(Type anchor)
    {
        var registry = (AsyncResponse.Internal.IMongoNamespaceRegistry)Activator.CreateInstance(
            anchor.Assembly.GetType("AsyncResponse.Internal.MongoNamespaceRegistry", throwOnError: true)!,
            nonPublic: true)!;

        registry.Claim("cluster-a", "appdb", "MongoDB channel", [("messages", "MessageCollection"), ("messages_counters", "counters")]);
        registry.Claim("cluster-a", "appdb", "MongoDB channel", [("messages", "MessageCollection")]);
        registry.Claim("cluster-a", "otherdb", "MongoDB durable-flow store", [("messages_counters", "CollectionName")]);
        registry.Claim("cluster-b", "appdb", "MongoDB durable-flow store", [("messages_counters", "CollectionName")]);

        var conflict = Assert.Throws<InvalidOperationException>(
            () => registry.Claim("cluster-a", "appdb", "MongoDB durable-flow store", [("flows", "CollectionName"), ("messages_counters", "CollectionName")]));
        Assert.Contains("'appdb.messages_counters' is used by both the MongoDB channel (counters)", conflict.Message, StringComparison.Ordinal);
        Assert.Contains("the MongoDB durable-flow store (CollectionName)", conflict.Message, StringComparison.Ordinal);
    }

    // ---- MongoWriteConcerns ----

    /// <summary>
    /// The database overload reads the inherited concern from the handle's settings: w=1 whatever
    /// was inherited, keeping the journal; a handle without settings reads as the server default.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllAnchors))]
    public void PrimaryAcknowledged_FromADatabase_KeepsItsJournal(Type anchor)
    {
        var method = WriteConcerns(anchor).GetMethod("PrimaryAcknowledged", BindingFlags.Public | BindingFlags.Static, [typeof(IMongoDatabase)])!;

        var journaled = new Mock<IMongoDatabase>();
        journaled.SetupGet(d => d.Settings).Returns(new MongoDatabaseSettings
        {
            WriteConcern = WriteConcern.WMajority.With(wTimeout: TimeSpan.FromSeconds(3), journal: true)
        });
        var concern = (WriteConcern)method.Invoke(null, [journaled.Object])!;
        Assert.Equal(WriteConcern.W1.W, concern.W);
        Assert.Null(concern.WTimeout);
        Assert.True(concern.Journal);

        var bare = new Mock<IMongoDatabase>();
        var unset = (WriteConcern)method.Invoke(null, [bare.Object])!;
        Assert.Equal(WriteConcern.W1.W, unset.W);
        Assert.Null(unset.Journal);
    }

    /// <summary>
    /// The third surface: a bulk write whose only error is a lapsed replication bound is a
    /// replication timeout; one that also carries a write error, or a different write-concern
    /// error, is not.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllAnchors))]
    public void IsReplicationTimeout_RecognisesABulkWriteWithOnlyALapsedBound(Type anchor)
    {
        var method = WriteConcerns(anchor).GetMethod("IsReplicationTimeout", BindingFlags.Public | BindingFlags.Static, [typeof(Exception)])!;
        bool IsTimeout(Exception exception) => (bool)method.Invoke(null, [exception])!;

        Assert.True(IsTimeout(BulkWrite(MongoReplicationTimeouts.WriteConcernError(), withWriteError: false)));
        Assert.True(IsTimeout(BulkWrite(MongoReplicationTimeouts.WriteConcernError(code: 100, wtimeout: true), withWriteError: false)));
        Assert.False(IsTimeout(BulkWrite(MongoReplicationTimeouts.WriteConcernError(code: 100, wtimeout: false), withWriteError: false)));
        Assert.False(IsTimeout(BulkWrite(MongoReplicationTimeouts.WriteConcernError(), withWriteError: true)));
        Assert.False(IsTimeout(BulkWrite(writeConcernError: null, withWriteError: false)));
    }

    // ---- MongoIndexes ----

    /// <summary>
    /// A same-named index with different options (85) is dropped and recreated when the caller
    /// replaces conflicts; a peer that dropped it first (27 on the drop) is tolerated. Without
    /// replacement, the conflict propagates and nothing is dropped.
    /// </summary>
    [Theory]
    [MemberData(nameof(IndexAnchors))]
    public async Task CreateOrAcceptEquivalent_ReplacesAConflictingSameNamedIndex_OnlyWhenAsked(Type anchor)
    {
        foreach (var peerDroppedFirst in new[] { false, true })
        {
            var (collection, indexes) = IndexedCollection(
                new BsonDocument { ["name"] = "jobs_expires_idx", ["key"] = new BsonDocument("expires_at", 1), ["expireAfterSeconds"] = 3600 });
            var creates = 0;
            indexes
                .Setup(m => m.CreateOneAsync(It.IsAny<CreateIndexModel<BsonDocument>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()))
                .Returns(() => ++creates == 1 ? Task.FromException<string>(CommandError(85)) : Task.FromResult("jobs_expires_idx"));
            var drop = indexes.Setup(m => m.DropOneAsync("jobs_expires_idx", It.IsAny<CancellationToken>()));
            if (peerDroppedFirst)
                drop.ThrowsAsync(CommandError(27));
            else
                drop.Returns(Task.CompletedTask);

            await CreateOrAcceptAsync(anchor, collection.Object, ExpiryIndex(), replaceConflicting: true);

            Assert.Equal(2, creates);
            indexes.Verify(m => m.DropOneAsync("jobs_expires_idx", It.IsAny<CancellationToken>()), Times.Once);
        }

        var (strict, strictIndexes) = IndexedCollection(
            new BsonDocument { ["name"] = "jobs_expires_idx", ["key"] = new BsonDocument("expires_at", 1), ["expireAfterSeconds"] = 3600 });
        strictIndexes
            .Setup(m => m.CreateOneAsync(It.IsAny<CreateIndexModel<BsonDocument>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(CommandError(86));
        Assert.Equal(86, (await Assert.ThrowsAsync<MongoCommandException>(
            () => CreateOrAcceptAsync(anchor, strict.Object, ExpiryIndex(), replaceConflicting: false))).Code);
        strictIndexes.Verify(m => m.DropOneAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);

        // A drop failing for any other reason is not swallowed.
        var (denied, deniedIndexes) = IndexedCollection();
        deniedIndexes
            .Setup(m => m.CreateOneAsync(It.IsAny<CreateIndexModel<BsonDocument>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(CommandError(85));
        deniedIndexes.Setup(m => m.DropOneAsync("jobs_expires_idx", It.IsAny<CancellationToken>())).ThrowsAsync(CommandError(13));
        Assert.Equal(13, (await Assert.ThrowsAsync<MongoCommandException>(
            () => CreateOrAcceptAsync(anchor, denied.Object, ExpiryIndex(), replaceConflicting: true))).Code);
    }

    /// <summary>
    /// Key equivalence is exact on field count, field names in order, and direction — numbers by
    /// value (a shell's 1.0 is the driver's 1), other key types by equality.
    /// </summary>
    [Theory]
    [MemberData(nameof(IndexAnchors))]
    public void IsEquivalentUnderAnotherName_ComparesKeysByCountNameOrderAndDirection(Type anchor)
    {
        var requested = new BsonDocument { ["queue"] = 1, ["available_at"] = 1 };
        bool Equivalent(BsonDocument keys) => IsEquivalent(anchor, new BsonDocument { ["name"] = "other", ["key"] = keys }, "mine", requested, null);

        Assert.True(Equivalent(new BsonDocument { ["queue"] = 1.0, ["available_at"] = 1 }));
        Assert.False(Equivalent(new BsonDocument { ["queue"] = 1 }));
        Assert.False(Equivalent(new BsonDocument { ["queue"] = 1, ["available_at"] = 1, ["priority"] = 1 }));
        Assert.False(Equivalent(new BsonDocument { ["available_at"] = 1, ["queue"] = 1 }));
        Assert.False(Equivalent(new BsonDocument { ["queue"] = 1, ["available_at"] = -1 }));
        Assert.False(Equivalent(new BsonDocument { ["queue"] = "hashed", ["available_at"] = 1 }));
        Assert.True(IsEquivalent(
            anchor,
            new BsonDocument { ["name"] = "other", ["key"] = new BsonDocument { ["queue"] = "hashed" } },
            "mine",
            new BsonDocument { ["queue"] = "hashed" },
            null));
    }

    private static Type WriteConcerns(Type anchor)
        => anchor.Assembly.GetType("AsyncResponse.Internal.MongoWriteConcerns", throwOnError: true)!;

    private static Task ClaimAsync(Type anchor, IMongoDatabase database, string componentName, params (string Collection, string Purpose)[] claims)
    {
        var claim = anchor.Assembly
            .GetType("AsyncResponse.Internal.MongoOwnershipLedger", throwOnError: true)!
            .GetMethod("ClaimAsync", BindingFlags.Public | BindingFlags.Static)!;
        return (Task)claim.Invoke(null, [database, componentName, (IReadOnlyList<(string, string)>)claims, CancellationToken.None])!;
    }

    private static Task CreateOrAcceptAsync(Type anchor, IMongoCollection<BsonDocument> collection, CreateIndexModel<BsonDocument> model, bool replaceConflicting)
    {
        var method = anchor.Assembly
            .GetType("AsyncResponse.Internal.MongoIndexes", throwOnError: true)!
            .GetMethod("CreateOrAcceptEquivalentAsync", BindingFlags.Public | BindingFlags.Static)!
            .MakeGenericMethod(typeof(BsonDocument));
        return (Task)method.Invoke(null, [collection, model, replaceConflicting, CancellationToken.None])!;
    }

    private static bool IsEquivalent(Type anchor, BsonDocument index, string? name, BsonDocument keys, TimeSpan? expireAfter)
        => (bool)anchor.Assembly
            .GetType("AsyncResponse.Internal.MongoIndexes", throwOnError: true)!
            .GetMethod("IsEquivalentUnderAnotherName", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static)!
            .Invoke(null, [index, name, keys, expireAfter])!;

    private static CreateIndexModel<BsonDocument> ExpiryIndex()
        => new(
            Builders<BsonDocument>.IndexKeys.Ascending("expires_at"),
            new CreateIndexOptions { Name = "jobs_expires_idx", ExpireAfter = TimeSpan.Zero });

    private static (Mock<IMongoCollection<BsonDocument>> Collection, Mock<IMongoIndexManager<BsonDocument>> Indexes) IndexedCollection(params BsonDocument[] listed)
    {
        var indexes = new Mock<IMongoIndexManager<BsonDocument>>();
        indexes
            .Setup(m => m.ListAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new MongoListCursor<BsonDocument>([new BsonDocument { ["name"] = "_id_", ["key"] = new BsonDocument("_id", 1) }, .. listed]));
        var collection = new Mock<IMongoCollection<BsonDocument>>();
        collection.SetupGet(c => c.Indexes).Returns(indexes.Object);
        return (collection, indexes);
    }

    private static BsonDocument Claim(string component, string purpose)
        => new() { ["_id"] = "claimed", ["component"] = component, ["purpose"] = purpose };

    /// <summary>A ledger handle whose claim upsert answers <paramref name="upsert"/> for the claimed collection name.</summary>
    private static Mock<IMongoCollection<BsonDocument>> LedgerCollection(Func<string, BsonDocument?> upsert)
    {
        var collection = new Mock<IMongoCollection<BsonDocument>>().SelfPinning();
        collection
            .Setup(c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns((FilterDefinition<BsonDocument> filter, UpdateDefinition<BsonDocument> _, FindOneAndUpdateOptions<BsonDocument, BsonDocument> _, CancellationToken _) =>
            {
                var id = ((BsonDocumentFilterDefinition<BsonDocument>)filter).Document["_id"].AsString;
                return Task.FromResult(upsert(id)!);
            });
        return collection;
    }

    private static Mock<IMongoDatabase> LedgerDatabase(Mock<IMongoCollection<BsonDocument>> ledger)
    {
        var database = new Mock<IMongoDatabase>();
        database
            .Setup(d => d.GetCollection<BsonDocument>("asyncresponse_ownership", It.IsAny<MongoCollectionSettings>()))
            .Returns(ledger.Object);
        database.SetupGet(d => d.DatabaseNamespace).Returns(new DatabaseNamespace("appdb"));
        return database;
    }

    private static MongoCommandException CommandError(int code)
        => new(
            MongoReplicationTimeouts.Connection,
            $"command failed with {code}",
            new BsonDocument("test", 1),
            new BsonDocument { ["ok"] = 0, ["code"] = code, ["errmsg"] = $"code {code}" });

    private static MongoBulkWriteException<BsonDocument> BulkWrite(WriteConcernError? writeConcernError, bool withWriteError)
    {
        var request = new InsertOneModel<BsonDocument>(new BsonDocument("_id", 1));
        var writeErrors = withWriteError
            ? new[]
            {
                (BulkWriteError)Activator.CreateInstance(
                    typeof(BulkWriteError),
                    BindingFlags.Instance | BindingFlags.NonPublic,
                    binder: null,
                    [0, ServerErrorCategory.DuplicateKey, 11000, "E11000 duplicate key error", new BsonDocument()],
                    culture: null)!
            }
            : [];
        return new MongoBulkWriteException<BsonDocument>(
            MongoReplicationTimeouts.Connection,
            new BulkWriteResult<BsonDocument>.Acknowledged(1, 0, 0, 1, 0, [request], []),
            writeErrors,
            writeConcernError,
            []);
    }
}
