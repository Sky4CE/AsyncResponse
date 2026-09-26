using System.Reflection;
using AsyncResponse.Channels.MongoDB;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The write concern every MongoDB store pins (<c>src/Shared/MongoWriteConcerns.cs</c>, source-linked
/// into the channel, transport, and durable-flow packages — every fact on the helper runs against all
/// three compilations by reflection).
/// </summary>
public sealed class MongoWriteConcernTests
{
    public static TheoryData<Type> AnchorTypes =>
    [
        typeof(MongoDbAsyncResponseTransportOptions),
        typeof(MongoDbAsyncResponseChannelOptions),
        typeof(MongoDbDurableFlowOptions)
    ];

    /// <summary>
    /// Regression (flow store): a bare <c>WriteConcern.WMajority</c> carried no <c>wtimeout</c>, so on a
    /// primary-secondary-arbiter set with its secondary down every ledger write blocked indefinitely —
    /// and it replaced the inherited concern wholesale. Unset → majority with the default bound.
    /// </summary>
    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void BoundedMajority_WithNothingInherited_IsMajorityWithTheDefaultBound(Type anchor)
    {
        var concern = BoundedMajority(anchor, inherited: null, TimeSpan.FromSeconds(10));

        Assert.Equal(WriteConcern.WMajority.W, concern.W);
        Assert.Equal(TimeSpan.FromSeconds(10), concern.WTimeout);
    }

    /// <summary>An operator's own wtimeoutMS and journal survive; only w is raised to majority.</summary>
    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void BoundedMajority_KeepsTheInheritedTimeoutAndJournal_AndRaisesW(Type anchor)
    {
        var inherited = WriteConcern.W1.With(wTimeout: TimeSpan.FromSeconds(3), journal: true);

        var concern = BoundedMajority(anchor, inherited, TimeSpan.FromSeconds(10));

        Assert.Equal(WriteConcern.WMajority.W, concern.W);
        Assert.Equal(TimeSpan.FromSeconds(3), concern.WTimeout);
        Assert.True(concern.Journal);
    }

    /// <summary>
    /// Regression: the transport's handle inherited the database's write concern, so under w=1 (a
    /// connection-string setting, the PSA default, any pre-5.0 server) the primary acknowledged a job
    /// — or a durable flow's wake-up — that a failover then rolled back. The handle now pins a bounded
    /// majority, keeping the operator's inherited wtimeout.
    /// </summary>
    [Fact]
    public void TransportStore_PinsBoundedMajorityWrites_KeepingTheInheritedTimeout()
    {
        var database = DatabaseInheriting(WriteConcern.W1.With(wTimeout: TimeSpan.FromSeconds(3)));
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);

        _ = new MongoDbTransportStore(database.Object, Options.Create(new MongoDbAsyncResponseTransportOptions { UseOwnershipLedger = false }));

        collection.Verify(c => c.WithWriteConcern(It.Is<WriteConcern>(IsBoundedMajority(TimeSpan.FromSeconds(3)))), Times.Once);
    }

    /// <summary>
    /// Regression: the transport's LEASE writes rode the bounded-majority handles too. A claim whose
    /// wtimeout lapsed had already stamped attempts+1 on the primary but threw before any delivery
    /// existed, so a replication stall of a few LockTimeouts dead-lettered (or, with the DLQ off,
    /// deleted) a job that never ran; and a claim acknowledged only after the replication wait could
    /// return after its own lease expired, with a peer running the same document.
    /// <para>
    /// r2 S9#4: the lease handles then INHERITED the database's concern — which is itself majority
    /// on an Atlas-style <c>w=majority</c> string or a 5.0+ primary-secondary-secondary set — so the
    /// hazards stayed there. They now pin w=1, keeping the inherited journal. r2 GS3#1: the deletes
    /// (ack, burial, prune) moved to them too — a majority delete held the claim loop for the whole
    /// wtimeout and then reported an applied delete as failed; a rolled-back one only makes the
    /// document claimable again. Only the inserts (publish upsert, dead-letter insert) keep the
    /// bounded majority. The index DDL every claim runs first rides the w=1 handle as well.
    /// </para>
    /// </summary>
    [Fact]
    public async Task TransportStore_LeaseWritesAndDeletesPinW1_WhileInsertsPinTheBoundedMajority()
    {
        var database = DatabaseInheriting(WriteConcern.WMajority.With(journal: true));
        var typedPrimary = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose);
        var typedLease = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var typedMajority = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        typedPrimary.Setup(c => c.WithReadPreference(It.IsAny<ReadPreference>())).Returns(typedPrimary.Object);
        typedPrimary.Setup(c => c.WithWriteConcern(It.Is(IsPrimaryAcknowledgedKeepingJournal))).Returns(typedLease.Object);
        typedPrimary.Setup(c => c.WithWriteConcern(It.Is(IsBoundedMajority(TimeSpan.FromSeconds(10))))).Returns(typedMajority.Object);
        var rawPrimary = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose);
        var rawLease = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        var rawMajority = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        rawPrimary.Setup(c => c.WithReadPreference(It.IsAny<ReadPreference>())).Returns(rawPrimary.Object);
        rawPrimary.Setup(c => c.WithWriteConcern(It.Is(IsPrimaryAcknowledgedKeepingJournal))).Returns(rawLease.Object);
        rawPrimary.Setup(c => c.WithWriteConcern(It.Is(IsBoundedMajority(TimeSpan.FromSeconds(10))))).Returns(rawMajority.Object);
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(typedPrimary.Object);
        database
            .Setup(d => d.GetCollection<BsonDocument>("asyncresponse_transport_messages", It.IsAny<MongoCollectionSettings>()))
            .Returns(rawPrimary.Object);

        // Every handle answers every operation, so a write on the wrong handle fails the Verify below
        // rather than some unrelated null.
        var document = new MongoTransportMessageDocument { Id = Guid.NewGuid(), Queue = "worker", Payload = "{}", Attempts = 1 }.ToBsonDocument();
        foreach (var raw in new[] { rawPrimary, rawLease, rawMajority })
        {
            raw.Setup(c => c.FindOneAndUpdateAsync(
                    It.IsAny<FilterDefinition<BsonDocument>>(),
                    It.IsAny<UpdateDefinition<BsonDocument>>(),
                    It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(document);
        }

        foreach (var typed in new[] { typedPrimary, typedLease, typedMajority })
        {
            typed.Setup(c => c.UpdateOneAsync(
                    It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                    It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                    It.IsAny<UpdateOptions>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new UpdateResult.Acknowledged(1, 1, BsonNull.Value));
            typed.Setup(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeleteResult.Acknowledged(1));
        }

        var store = new MongoDbTransportStore(
            database.Object,
            Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false, UseOwnershipLedger = false }));

        // Lease writes: claim, renew, NAK.
        var delivery = await store.TryClaimAsync("worker", TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.NotNull(delivery);
        Assert.True(await delivery!.RenewAsync(CancellationToken.None));
        await delivery.NakAsync(TimeSpan.FromSeconds(1));

        rawLease.Verify(ClaimCall(), Times.Once);
        rawMajority.Verify(ClaimCall(), Times.Never);
        rawPrimary.Verify(ClaimCall(), Times.Never);
        typedLease.Verify(UpdateCall(), Times.Exactly(2));
        typedMajority.Verify(UpdateCall(), Times.Never);

        // The publish upsert and a burial's dead-letter insert: majority. The ack and the burial's
        // source delete: w=1.
        await store.PublishAsync(Guid.NewGuid(), "worker", "{}", headers: null, CancellationToken.None);
        await delivery.AckAsync();
        Assert.True(await delivery.DeadLetterAsync(new InvalidOperationException("poison"), true, CancellationToken.None));

        typedLease.Verify(UpdateCall(), Times.Exactly(2));
        typedMajority.Verify(UpdateCall(), Times.Exactly(2));
        typedLease.Verify(DeleteCall(), Times.Exactly(2));
        typedMajority.Verify(DeleteCall(), Times.Never);
        typedPrimary.Verify(UpdateCall(), Times.Never);
        typedPrimary.Verify(DeleteCall(), Times.Never);

        // The index DDL every claim runs first (EnsureCreated) rides the w=1 handle too: under the
        // bounded majority a host that started during a replication stall could not claim at all.
        var leaseIndexes = new Mock<IMongoIndexManager<MongoTransportMessageDocument>>(MockBehavior.Loose);
        var majorityIndexes = new Mock<IMongoIndexManager<MongoTransportMessageDocument>>(MockBehavior.Loose);
        typedLease.SetupGet(c => c.Indexes).Returns(leaseIndexes.Object);
        typedMajority.SetupGet(c => c.Indexes).Returns(majorityIndexes.Object);
        using var indexing = new MongoDbTransportStore(
            database.Object,
            Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = true, UseOwnershipLedger = false }));
        await indexing.EnsureCreatedAsync();

        leaseIndexes.Verify(CreateIndexCall(), Times.Exactly(2));
        majorityIndexes.Verify(CreateIndexCall(), Times.Never);

        static System.Linq.Expressions.Expression<Func<IMongoIndexManager<MongoTransportMessageDocument>, Task<string>>> CreateIndexCall()
            => indexes => indexes.CreateOneAsync(
                It.IsAny<CreateIndexModel<MongoTransportMessageDocument>>(),
                It.IsAny<CreateOneIndexOptions>(),
                It.IsAny<CancellationToken>());

        static System.Linq.Expressions.Expression<Func<IMongoCollection<MongoTransportMessageDocument>, Task<UpdateResult>>> UpdateCall()
            => c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>());

        static System.Linq.Expressions.Expression<Func<IMongoCollection<MongoTransportMessageDocument>, Task<DeleteResult>>> DeleteCall()
            => c => c.DeleteOneAsync(It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<CancellationToken>());
    }

    /// <summary>
    /// r2 GS3#1: the delete that removes a document the class map cannot read (buried on sight by
    /// the claim) went through the bounded-majority handle, and — unguarded — a lapsed wtimeout
    /// threw out of the claim itself. It rides the w=1 lease handle now.
    /// </summary>
    [Fact]
    public async Task TransportStore_UnreadableDocumentDelete_RidesTheW1LeaseHandle()
    {
        var database = DatabaseInheriting(WriteConcern.WMajority.With(journal: true));
        var typed = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        typed.Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new UpdateResult.Acknowledged(1, 1, BsonNull.Value));
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(typed.Object);
        var rawPrimary = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose);
        var rawLease = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        var rawMajority = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        rawPrimary.Setup(c => c.WithReadPreference(It.IsAny<ReadPreference>())).Returns(rawPrimary.Object);
        rawPrimary.Setup(c => c.WithWriteConcern(It.Is(IsPrimaryAcknowledgedKeepingJournal))).Returns(rawLease.Object);
        rawPrimary.Setup(c => c.WithWriteConcern(It.Is(IsBoundedMajority(TimeSpan.FromSeconds(10))))).Returns(rawMajority.Object);
        database
            .Setup(d => d.GetCollection<BsonDocument>("asyncresponse_transport_messages", It.IsAny<MongoCollectionSettings>()))
            .Returns(rawPrimary.Object);
        // A foreign producer's ObjectId _id: unreadable by the class map, then the queue is empty.
        rawLease.ClaimsInOrder(new BsonDocument { ["_id"] = ObjectId.GenerateNewId(), ["queue"] = "worker", ["payload"] = "{}" }, null);
        rawLease.Setup(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<BsonDocument>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteResult.Acknowledged(1));

        var store = new MongoDbTransportStore(
            database.Object,
            Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false, UseOwnershipLedger = false }));

        Assert.Null(await store.TryClaimAsync("worker", TimeSpan.FromSeconds(30), CancellationToken.None));

        rawLease.Verify(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<BsonDocument>>(), It.IsAny<CancellationToken>()), Times.Once);
        rawMajority.Verify(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<BsonDocument>>(), It.IsAny<CancellationToken>()), Times.Never);
        rawPrimary.Verify(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<BsonDocument>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private static System.Linq.Expressions.Expression<Func<IMongoCollection<BsonDocument>, Task<BsonDocument>>> ClaimCall()
        => c => c.FindOneAndUpdateAsync(
            It.IsAny<FilterDefinition<BsonDocument>>(),
            It.IsAny<UpdateDefinition<BsonDocument>>(),
            It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
            It.IsAny<CancellationToken>());

    private static readonly System.Linq.Expressions.Expression<Func<WriteConcern, bool>> IsPrimaryAcknowledgedKeepingJournal
        = concern => concern.W == WriteConcern.W1.W && concern.WTimeout == null && concern.Journal == true;

    /// <summary>
    /// r2 S9#4: the lease concern states w=1 whatever is inherited — an inherited majority (the
    /// implicit default of a 5.0+ primary-secondary-secondary set, an Atlas-style string) gave the
    /// lease writes exactly the hazards their handle exists to avoid — dropping the inherited
    /// wtimeout (there is no replication wait left to bound) and keeping the journal.
    /// </summary>
    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void PrimaryAcknowledged_StatesW1_KeepingTheInheritedJournal(Type anchor)
    {
        var concern = PrimaryAcknowledged(anchor, WriteConcern.WMajority.With(wTimeout: TimeSpan.FromSeconds(5), journal: true));

        Assert.Equal(WriteConcern.W1.W, concern.W);
        Assert.Null(concern.WTimeout);
        Assert.True(concern.Journal);
        Assert.Equal(WriteConcern.W1.W, PrimaryAcknowledged(anchor, inherited: null).W);
    }

    /// <summary>
    /// r2 S5#1/S9#3/S4#3: a lapsed bound — the write applied on the primary, only its majority
    /// acknowledgement timed out — reaches the stores as a <see cref="MongoWriteConcernException"/>
    /// (findAndModify) or a <see cref="MongoWriteException"/> carrying only a write-concern error
    /// (the CRUD bulk path). Nothing recognised it. A write that itself failed, or a different
    /// write-concern error, is not one.
    /// </summary>
    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void IsReplicationTimeout_RecognisesOnlyALapsedBoundOnAnAppliedWrite(Type anchor)
    {
        Assert.True(IsReplicationTimeout(anchor, MongoReplicationTimeouts.Command()));
        Assert.True(IsReplicationTimeout(anchor, MongoReplicationTimeouts.Command(code: 91, wtimeout: true)));
        Assert.True(IsReplicationTimeout(anchor, MongoReplicationTimeouts.Write()));

        Assert.False(IsReplicationTimeout(anchor, MongoReplicationTimeouts.Command(code: 100, wtimeout: false)));
        Assert.False(IsReplicationTimeout(anchor, MongoReplicationTimeouts.Write(writeError: MongoReplicationTimeouts.DuplicateKeyError())));
        Assert.False(IsReplicationTimeout(anchor, MongoReplicationTimeouts.Write(code: 100, wtimeout: false)));
        Assert.False(IsReplicationTimeout(anchor, new TimeoutException()));
        Assert.False(IsReplicationTimeout(anchor, new MongoCommandException(
            MongoReplicationTimeouts.Connection, "failed", new BsonDocument(), new BsonDocument { ["ok"] = 0, ["code"] = 64 })));
    }

    /// <summary>
    /// ...and it is deliberately not transient: a retry re-waits on the same replication (lapsing
    /// again while the set stays degraded) or, for a transport job, re-creates one a subscriber had
    /// already run and deleted. The stores settle it where they write instead.
    /// </summary>
    [Theory]
    [InlineData(typeof(MongoDbAsyncResponseTransportOptions))]
    [InlineData(typeof(MongoDbAsyncResponseChannelOptions))]
    public void ReplicationTimeout_IsNotClassifiedTransient(Type anchor)
    {
        var isTransient = anchor.Assembly
            .GetType("AsyncResponse.Internal.MongoTransientFaults", throwOnError: true)!
            .GetMethod("IsTransient", BindingFlags.Public | BindingFlags.Static)!;

        Assert.False((bool)isTransient.Invoke(null, [MongoReplicationTimeouts.Command()])!);
        Assert.False((bool)isTransient.Invoke(null, [MongoReplicationTimeouts.Write()])!);
    }

    /// <summary>
    /// The channel's four handles had the same gap: a rolled-back response, recovery registration, or
    /// ack-sequence draw was lost silently. Every handle pins the same bounded majority.
    /// </summary>
    [Fact]
    public void ChannelStore_PinsBoundedMajorityWritesOnEveryHandle()
    {
        var database = DatabaseInheriting(inherited: null);
        var recovery = new Mock<IMongoCollection<MongoRecoveryStateDocument>>(MockBehavior.Loose).SelfPinning();
        var messages = new Mock<IMongoCollection<MongoChannelMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var subscribers = new Mock<IMongoCollection<MongoChannelSubscriberDocument>>(MockBehavior.Loose).SelfPinning();
        var counters = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        database.Setup(d => d.GetCollection<MongoRecoveryStateDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(recovery.Object);
        database.Setup(d => d.GetCollection<MongoChannelMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(messages.Object);
        database.Setup(d => d.GetCollection<MongoChannelSubscriberDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(subscribers.Object);
        database.Setup(d => d.GetCollection<BsonDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>())).Returns(counters.Object);

        _ = new MongoDbChannelStore(database.Object, Options.Create(new MongoDbAsyncResponseChannelOptions { AutoCreateIndexes = false }));

        var bounded = IsBoundedMajority(TimeSpan.FromSeconds(10));
        recovery.Verify(c => c.WithWriteConcern(It.Is(bounded)), Times.Once);
        messages.Verify(c => c.WithWriteConcern(It.Is(bounded)), Times.Once);
        subscribers.Verify(c => c.WithWriteConcern(It.Is(bounded)), Times.Once);
        counters.Verify(c => c.WithWriteConcern(It.Is(bounded)), Times.Once);
    }

    /// <summary>
    /// Regression: the flow store pinned an UNBOUNDED majority (no wtimeout), so a PSA set with its
    /// secondary down blocked every uncancellable checkpoint forever.
    /// </summary>
    [Fact]
    public void FlowStore_PinsAMajorityWriteConcernWithABound()
    {
        var database = DatabaseInheriting(inherited: null);
        var collection = new Mock<IMongoCollection<MongoFlowStateDocument>>(MockBehavior.Loose).SelfPinning();
        database
            .Setup(d => d.GetCollection<MongoFlowStateDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);

        using var store = new MongoDbFlowStateStore(database.Object, Options.Create(new MongoDbDurableFlowOptions { CollectionName = "flows" }));

        collection.Verify(c => c.WithWriteConcern(It.Is<WriteConcern>(IsBoundedMajority(TimeSpan.FromSeconds(10)))), Times.Once);
    }

    private static System.Linq.Expressions.Expression<Func<WriteConcern, bool>> IsBoundedMajority(TimeSpan wTimeout)
        => concern => concern.W == WriteConcern.WMajority.W && concern.WTimeout == wTimeout;

    private static Mock<IMongoDatabase> DatabaseInheriting(WriteConcern? inherited)
    {
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose).WithTestNamespace();
        if (inherited is not null)
            database.SetupGet(d => d.Settings).Returns(new MongoDatabaseSettings { WriteConcern = inherited });
        return database;
    }

    private static WriteConcern PrimaryAcknowledged(Type anchor, WriteConcern? inherited)
    {
        var method = anchor.Assembly
            .GetType("AsyncResponse.Internal.MongoWriteConcerns", throwOnError: true)!
            .GetMethod("PrimaryAcknowledged", BindingFlags.Public | BindingFlags.Static, [typeof(WriteConcern)])!;
        return (WriteConcern)method.Invoke(null, [inherited])!;
    }

    private static bool IsReplicationTimeout(Type anchor, Exception exception)
    {
        var method = anchor.Assembly
            .GetType("AsyncResponse.Internal.MongoWriteConcerns", throwOnError: true)!
            .GetMethod("IsReplicationTimeout", BindingFlags.Public | BindingFlags.Static, [typeof(Exception)])!;
        return (bool)method.Invoke(null, [exception])!;
    }

    private static WriteConcern BoundedMajority(Type anchor, WriteConcern? inherited, TimeSpan defaultWTimeout)
    {
        var method = anchor.Assembly
            .GetType("AsyncResponse.Internal.MongoWriteConcerns", throwOnError: true)!
            .GetMethod("BoundedMajority", BindingFlags.Public | BindingFlags.Static, [typeof(WriteConcern), typeof(TimeSpan)])!;
        return (WriteConcern)method.Invoke(null, [inherited, defaultWTimeout])!;
    }
}
