using AsyncResponse.Channels.MongoDB;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;
using Moq;
using System.Net;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The MongoDB channel store's one-time read-only index check under
/// <c>AutoCreateIndexes = false</c>. There is no collection shape to verify (documents are
/// schemaless), so the silent failure modes are all indexes — above all a missing TTL index,
/// which means nothing ever reaps expired documents. Absence must warn (naming the index and the
/// unbounded-growth consequence) and still latch, never fail startup: indexes degrade retention
/// and performance, not correctness, and a least-privilege operator may provision them out of
/// band — the same reason an unverifiable deployment (no listIndexes privilege) skips the check.
/// </summary>
public sealed class MongoDbManagedIndexVerificationTests
{
    private static BsonDocument TtlIndex()
        => new()
        {
            ["v"] = 2,
            ["key"] = new BsonDocument("expires_at", 1),
            ["name"] = "operator_expires_ttl",
            ["expireAfterSeconds"] = 0
        };

    private static BsonDocument LookupIndex()
        => new()
        {
            ["v"] = 2,
            ["key"] = new BsonDocument { ["correlation_id"] = 1, ["created_at"] = 1 },
            ["name"] = "operator_correlation_lookup"
        };

    private sealed class Fixture
    {
        public Fixture()
        {
            var options = Options.Create(new MongoDbAsyncResponseChannelOptions
            {
                AutoCreateIndexes = false,
                UseOwnershipLedger = false
            });
            Database.SetupGet(d => d.DatabaseNamespace).Returns(new DatabaseNamespace("tests"));
            Database
                .Setup(d => d.GetCollection<MongoRecoveryStateDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
                .Returns(Recovery.Object);
            Database
                .Setup(d => d.GetCollection<MongoChannelMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
                .Returns(Messages.Object);
            Database
                .Setup(d => d.GetCollection<MongoChannelSubscriberDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
                .Returns(Subscribers.Object);
            Store = new MongoDbChannelStore(Database.Object, options, logger: Logger);
        }

        public Mock<IMongoDatabase> Database { get; } = new(MockBehavior.Loose);
        public Mock<IMongoCollection<MongoRecoveryStateDocument>> Recovery { get; } = new(MockBehavior.Loose);
        public Mock<IMongoCollection<MongoChannelMessageDocument>> Messages { get; } = new(MockBehavior.Loose);
        public Mock<IMongoCollection<MongoChannelSubscriberDocument>> Subscribers { get; } = new(MockBehavior.Loose);
        public CollectingLogger Logger { get; } = new();
        public MongoDbChannelStore Store { get; }
    }

    private static Mock<IMongoIndexManager<TDocument>> SetupIndexList<TDocument>(
        Mock<IMongoCollection<TDocument>> collection,
        params BsonDocument[] indexes)
    {
        var manager = new Mock<IMongoIndexManager<TDocument>>(MockBehavior.Loose);
        manager
            .Setup(m => m.ListAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new ListCursor([.. indexes]));
        collection.SetupGet(c => c.Indexes).Returns(manager.Object);
        return manager;
    }

    private static Mock<IMongoIndexManager<TDocument>> SetupIndexListFailure<TDocument>(
        Mock<IMongoCollection<TDocument>> collection,
        Exception exception)
    {
        var manager = new Mock<IMongoIndexManager<TDocument>>(MockBehavior.Loose);
        manager
            .Setup(m => m.ListAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);
        collection.SetupGet(c => c.Indexes).Returns(manager.Object);
        return manager;
    }

    private static MongoCommandException NamespaceNotFound()
        => new(
            new ConnectionId(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017))),
            "listIndexes failed",
            new BsonDocument("listIndexes", "collection"),
            new BsonDocument { ["ok"] = 0, ["code"] = 26, ["errmsg"] = "ns does not exist" });

    [Fact]
    public async Task EnsureCreated_ManagedIndexes_WarnsPerMissingIndex_AndStillLatches()
    {
        var fixture = new Fixture();
        // Recovery: no indexes at all. Messages: fully provisioned (operator naming). Subscribers:
        // the collection does not even exist yet (NamespaceNotFound) — same verdict as no indexes.
        var recoveryIndexes = SetupIndexList(fixture.Recovery);
        var messageIndexes = SetupIndexList(fixture.Messages, TtlIndex(), LookupIndex());
        SetupIndexListFailure(fixture.Subscribers, NamespaceNotFound());

        await fixture.Store.EnsureCreatedAsync();

        var warnings = fixture.Logger.Messages;
        Assert.Contains(warnings, m =>
            m.Contains("tests.asyncresponse_recovery_state has no TTL index on 'expires_at'") && m.Contains("grows without bound"));
        Assert.Contains(warnings, m => m.Contains("tests.asyncresponse_recovery_state has no index leading on 'correlation_id'"));
        Assert.Contains(warnings, m => m.Contains("tests.asyncresponse_channel_subscribers has no TTL index on 'expires_at'"));
        // The fully provisioned collection stays silent, whatever the operator named its indexes.
        Assert.DoesNotContain(warnings, m => m.Contains("tests.asyncresponse_channel_messages has no"));

        // The check is one-time: absence latches (unbounded growth is an operator concern, not a
        // startup failure), so a second call lists nothing again.
        await fixture.Store.EnsureCreatedAsync();
        recoveryIndexes.Verify(m => m.ListAsync(It.IsAny<CancellationToken>()), Times.Once);
        messageIndexes.Verify(m => m.ListAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task EnsureCreated_ManagedIndexes_IsSilentWhenTheExpectedIndexesExist()
    {
        var fixture = new Fixture();
        SetupIndexList(fixture.Recovery, TtlIndex(), LookupIndex());
        SetupIndexList(fixture.Messages, TtlIndex(), LookupIndex());
        SetupIndexList(fixture.Subscribers, TtlIndex(), LookupIndex());

        await fixture.Store.EnsureCreatedAsync();

        Assert.DoesNotContain(fixture.Logger.Messages, m => m.Contains("has no"));
    }

    [Fact]
    public async Task EnsureCreated_ManagedIndexes_SkipsAVerificationItCannotPerform_AndStillLatches()
    {
        // A least-privilege user without listIndexes (or a server unreachable at first use) must
        // not lose the actual operation — or startup — to the check.
        var fixture = new Fixture();
        var recoveryIndexes = SetupIndexListFailure(
            fixture.Recovery,
            new MongoCommandException(
                new ConnectionId(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017))),
                "listIndexes failed",
                new BsonDocument("listIndexes", "collection"),
                new BsonDocument { ["ok"] = 0, ["code"] = 13, ["errmsg"] = "not authorized" }));

        await fixture.Store.EnsureCreatedAsync();
        await fixture.Store.EnsureCreatedAsync();

        Assert.DoesNotContain(fixture.Logger.Messages, m => m.Contains("has no"));
        Assert.Contains(fixture.Logger.Messages, m => m.Contains("Skipping index verification"));
        recoveryIndexes.Verify(m => m.ListAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    private sealed class ListCursor(List<BsonDocument> items) : IAsyncCursor<BsonDocument>
    {
        private bool _moved;

        public IEnumerable<BsonDocument> Current => items;

        public bool MoveNext(CancellationToken cancellationToken = default)
        {
            if (_moved)
                return false;
            _moved = true;
            return true;
        }

        public Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(MoveNext(cancellationToken));

        public void Dispose()
        {
        }
    }
}
