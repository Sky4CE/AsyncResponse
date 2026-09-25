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
    /// return after its own lease expired, with a peer running the same document. Claim, renew, and
    /// NAK go through handles carrying the inherited concern — and so does the index DDL every claim
    /// runs first; the publish upsert, the dead-letter insert, and the deletes through the
    /// bounded-majority ones.
    /// </summary>
    [Fact]
    public async Task TransportStore_LeaseWritesKeepTheInheritedConcern_WhileDurableWritesPinTheBoundedMajority()
    {
        var database = DatabaseInheriting(WriteConcern.W1);
        var typedLease = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose);
        var typedMajority = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        typedLease.Setup(c => c.WithReadPreference(It.IsAny<ReadPreference>())).Returns(typedLease.Object);
        typedLease.Setup(c => c.WithWriteConcern(It.IsAny<WriteConcern>())).Returns(typedMajority.Object);
        var rawLease = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose);
        var rawMajority = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        rawLease.Setup(c => c.WithReadPreference(It.IsAny<ReadPreference>())).Returns(rawLease.Object);
        rawLease.Setup(c => c.WithWriteConcern(It.IsAny<WriteConcern>())).Returns(rawMajority.Object);
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(typedLease.Object);
        database
            .Setup(d => d.GetCollection<BsonDocument>("asyncresponse_transport_messages", It.IsAny<MongoCollectionSettings>()))
            .Returns(rawLease.Object);

        // Every handle answers every operation, so a write on the wrong handle fails the Verify below
        // rather than some unrelated null.
        var document = new MongoTransportMessageDocument { Id = Guid.NewGuid(), Queue = "worker", Payload = "{}", Attempts = 1 }.ToBsonDocument();
        foreach (var raw in new[] { rawLease, rawMajority })
        {
            raw.Setup(c => c.FindOneAndUpdateAsync(
                    It.IsAny<FilterDefinition<BsonDocument>>(),
                    It.IsAny<UpdateDefinition<BsonDocument>>(),
                    It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(document);
        }

        foreach (var typed in new[] { typedLease, typedMajority })
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

        // The majority handles are derived from the lease handles with the bounded concern.
        typedLease.Verify(c => c.WithWriteConcern(It.Is(IsBoundedMajority(TimeSpan.FromSeconds(10)))), Times.Once);
        rawLease.Verify(c => c.WithWriteConcern(It.Is(IsBoundedMajority(TimeSpan.FromSeconds(10)))), Times.Once);

        // Lease writes: claim, renew, NAK.
        var delivery = await store.TryClaimAsync("worker", TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.NotNull(delivery);
        Assert.True(await delivery!.RenewAsync(CancellationToken.None));
        await delivery.NakAsync(TimeSpan.FromSeconds(1));

        rawLease.Verify(ClaimCall(), Times.Once);
        rawMajority.Verify(ClaimCall(), Times.Never);
        typedLease.Verify(UpdateCall(), Times.Exactly(2));
        typedMajority.Verify(UpdateCall(), Times.Never);

        // Durable writes: the publish upsert, the ack, and a burial (dead-letter insert + delete).
        await store.PublishAsync(Guid.NewGuid(), "worker", "{}", headers: null, CancellationToken.None);
        await delivery.AckAsync();
        Assert.True(await delivery.DeadLetterAsync(new InvalidOperationException("poison"), true, CancellationToken.None));

        typedLease.Verify(UpdateCall(), Times.Exactly(2));
        typedLease.Verify(DeleteCall(), Times.Never);
        typedMajority.Verify(UpdateCall(), Times.Exactly(2));
        typedMajority.Verify(DeleteCall(), Times.Exactly(2));

        // The index DDL every claim runs first (EnsureCreated) keeps the inherited concern too: under
        // the bounded majority a host that started during a replication stall could not claim at all.
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

        static System.Linq.Expressions.Expression<Func<IMongoCollection<BsonDocument>, Task<BsonDocument>>> ClaimCall()
            => c => c.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
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

    private static WriteConcern BoundedMajority(Type anchor, WriteConcern? inherited, TimeSpan defaultWTimeout)
    {
        var method = anchor.Assembly
            .GetType("AsyncResponse.Internal.MongoWriteConcerns", throwOnError: true)!
            .GetMethod("BoundedMajority", BindingFlags.Public | BindingFlags.Static, [typeof(WriteConcern), typeof(TimeSpan)])!;
        return (WriteConcern)method.Invoke(null, [inherited, defaultWTimeout])!;
    }
}
