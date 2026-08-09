using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;
using System.Net;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The MongoDB subscriber hosted service: its claim/dispatch loop, the empty-queue wait, the
/// change-stream wake loop and the retry-with-backoff wrapper. The store is a concrete type, so the
/// seam is the mocked <see cref="IMongoCollection{TDocument}"/> underneath it.
/// </summary>
public sealed class MongoDbSubscriberServiceTests
{
    [Fact]
    public async Task Subscriber_ClaimsDispatchesThenWaitsOnTheEmptyQueue()
    {
        var fixture = new Fixture();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Ingress
            .Setup(ingress => ingress.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(Task.CompletedTask)
            .Callback(() => handled.TrySetResult());

        // One document available, then an empty queue — so the loop dispatches, comes back round,
        // claims nothing and falls through to the empty-poll wait.
        fixture.QueueDocuments(new MongoTransportMessageDocument
        {
            Id = Guid.NewGuid(),
            Queue = "workers",
            Payload = """{"JobId":"1"}""",
            Attempts = 1
        });

        var subscriber = fixture.WorkerSubscriber();
        await subscriber.StartAsync(CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await subscriber.StopAsync(CancellationToken.None);

        fixture.Ingress.Verify(
            ingress => ingress.HandleWorkerMessageAsync("""{"JobId":"1"}"""),
            Times.AtLeastOnce);
    }

    /// <summary>The response subscriber routes on the extracted correlation id rather than the raw payload.</summary>
    [Fact]
    public async Task ResponseSubscriber_RoutesOnTheExtractedCorrelationId()
    {
        var fixture = new Fixture();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Ingress
            .Setup(ingress => ingress.HandleResponseMessageAsync(It.IsAny<string>(), It.IsAny<string?>()))
            .Returns(Task.CompletedTask)
            .Callback(() => handled.TrySetResult());

        fixture.QueueDocuments(new MongoTransportMessageDocument
        {
            Id = Guid.NewGuid(),
            Queue = "responses",
            Payload = """{"CorrelationId":"corr-1"}""",
            Headers = new Dictionary<string, string> { ["asyncresponse-correlation-id"] = "corr-1" },
            Attempts = 1
        });

        var subscriber = fixture.ResponseSubscriber();
        await subscriber.StartAsync(CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await subscriber.StopAsync(CancellationToken.None);

        fixture.Ingress.Verify(
            ingress => ingress.HandleResponseMessageAsync(It.IsAny<string>(), "corr-1"),
            Times.AtLeastOnce);
    }

    /// <summary>A failing claim is retried with backoff rather than killing the hosted service.</summary>
    [Fact]
    public async Task Subscriber_RetriesWithBackoffWhenTheStoreFails()
    {
        var fixture = new Fixture();
        fixture.Messages
            .Setup(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("mongo offline"));

        var subscriber = fixture.WorkerSubscriber();
        await subscriber.StartAsync(CancellationToken.None);
        await fixture.Logger.WaitForAsync("MongoDB subscriber failed for queue", occurrences: 2);
        await subscriber.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// A server that is not a replica set rejects the change stream outright; the wake loop reports
    /// it once at information level and leaves polling in charge instead of retrying forever.
    /// </summary>
    [Fact]
    public async Task ChangeStreamWake_ReportsAnUnsupportedServerAndLeavesPollingInCharge()
    {
        var fixture = new Fixture(useChangeStreamWake: true);
        fixture.Messages
            .Setup(collection => collection.WatchAsync(
                It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoTransportMessageDocument>, ChangeStreamDocument<MongoTransportMessageDocument>>>(),
                It.IsAny<ChangeStreamOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(ChangeStreamsUnsupported());

        var subscriber = fixture.WorkerSubscriber();
        await subscriber.StartAsync(CancellationToken.None);
        await fixture.Logger.WaitForAsync("MongoDB change streams are unavailable");
        await subscriber.StopAsync(CancellationToken.None);
    }

    /// <summary>Any other change-stream fault degrades quietly to polling.</summary>
    [Fact]
    public async Task ChangeStreamWake_DegradesToPollingOnAnyOtherFault()
    {
        var fixture = new Fixture(useChangeStreamWake: true);
        fixture.Messages
            .Setup(collection => collection.WatchAsync(
                It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoTransportMessageDocument>, ChangeStreamDocument<MongoTransportMessageDocument>>>(),
                It.IsAny<ChangeStreamOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("cursor died"));

        var subscriber = fixture.WorkerSubscriber();
        await subscriber.StartAsync(CancellationToken.None);
        await fixture.Logger.WaitForAsync("change-stream wake for queue");
        await subscriber.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// A change-stream notification wakes the claim loop, which is the whole point of the wake: the
    /// subscriber picks the document up without waiting out the empty-poll delay.
    /// </summary>
    [Fact]
    public async Task ChangeStreamWake_SignalsTheClaimLoop()
    {
        // A poll delay far longer than the test's patience: only the wake signal can deliver in time.
        var fixture = new Fixture(useChangeStreamWake: true, emptyPollDelay: TimeSpan.FromMinutes(10));
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Ingress
            .Setup(ingress => ingress.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns(Task.CompletedTask)
            .Callback(() => handled.TrySetResult());

        var cursor = new FakeChangeStreamCursor();
        fixture.Messages
            .Setup(collection => collection.WatchAsync(
                It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoTransportMessageDocument>, ChangeStreamDocument<MongoTransportMessageDocument>>>(),
                It.IsAny<ChangeStreamOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursor);

        var subscriber = fixture.WorkerSubscriber();
        await subscriber.StartAsync(CancellationToken.None);

        // Let the first (empty) claim pass parks the loop on the wait, then publish and wake it.
        await fixture.ClaimAttempted.Task.WaitAsync(TimeSpan.FromSeconds(30));
        fixture.QueueDocuments(new MongoTransportMessageDocument
        {
            Id = Guid.NewGuid(),
            Queue = "workers",
            Payload = """{"JobId":"2"}""",
            Attempts = 1
        });
        cursor.Notify();

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await subscriber.StopAsync(CancellationToken.None);
    }

    /// <summary>Stopping before any work arrives takes the cancellation exit, not the retry path.</summary>
    [Fact]
    public async Task Subscriber_StopsCleanlyWhileWaitingOnAnEmptyQueue()
    {
        var fixture = new Fixture(emptyPollDelay: TimeSpan.FromMinutes(10));
        var subscriber = fixture.WorkerSubscriber();
        await subscriber.StartAsync(CancellationToken.None);
        await fixture.ClaimAttempted.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await subscriber.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// A claim already in flight when the host stops throws <see cref="OperationCanceledException"/>
    /// from the driver; that is a shutdown, not a fault, so it exits without arming the retry.
    /// </summary>
    [Fact]
    public async Task Subscriber_TreatsACancelledClaimAsShutdownRatherThanFailure()
    {
        var fixture = new Fixture(honourClaimCancellation: true);
        var subscriber = fixture.WorkerSubscriber();
        await subscriber.StartAsync(CancellationToken.None);
        await fixture.ClaimAttempted.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await subscriber.StopAsync(CancellationToken.None);

        Assert.DoesNotContain(fixture.Logger.Messages, message => message.Contains("subscriber failed", StringComparison.Ordinal));
    }

    /// <summary>
    /// A wake loop that will not come back stops being waited on after
    /// <see cref="MongoDbAsyncResponseTransportOptions.ShutdownTimeout"/>, so shutdown cannot hang on it.
    /// </summary>
    [Fact]
    public async Task ChangeStreamWake_IsAbandonedWhenItOutlastsTheShutdownTimeout()
    {
        var fixture = new Fixture(useChangeStreamWake: true);
        var cursor = new FakeChangeStreamCursor(ignoreCancellation: true);
        fixture.Messages
            .Setup(collection => collection.WatchAsync(
                It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoTransportMessageDocument>, ChangeStreamDocument<MongoTransportMessageDocument>>>(),
                It.IsAny<ChangeStreamOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursor);

        var subscriber = fixture.WorkerSubscriber();
        await subscriber.StartAsync(CancellationToken.None);
        await fixture.ClaimAttempted.Task.WaitAsync(TimeSpan.FromSeconds(30));

        // Returns despite the wake loop still being parked: the wait timed out and was swallowed.
        await subscriber.StopAsync(CancellationToken.None);
        cursor.Release();
    }

    /// <summary>A change stream that simply ends leaves the wake loop to return normally.</summary>
    [Fact]
    public async Task ChangeStreamWake_ReturnsWhenTheStreamEnds()
    {
        var fixture = new Fixture(useChangeStreamWake: true);
        var cursor = new FakeChangeStreamCursor();
        fixture.Messages
            .Setup(collection => collection.WatchAsync(
                It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoTransportMessageDocument>, ChangeStreamDocument<MongoTransportMessageDocument>>>(),
                It.IsAny<ChangeStreamOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursor);

        var subscriber = fixture.WorkerSubscriber();
        await subscriber.StartAsync(CancellationToken.None);
        await fixture.ClaimAttempted.Task.WaitAsync(TimeSpan.FromSeconds(30));
        cursor.EndStream();

        await subscriber.StopAsync(CancellationToken.None);
    }

    /// <summary>The server's "not a replica set" rejection, as the driver surfaces it.</summary>
    private static MongoCommandException ChangeStreamsUnsupported()
        => new(
            new ConnectionId(new ServerId(new ClusterId(), new DnsEndPoint("localhost", 27017))),
            "aggregate",
            new BsonDocument(),
            new BsonDocument { ["ok"] = 0, ["code"] = 40573, ["errmsg"] = "unsupported" });

    // ---------------------------------------------------------------------------------------

    private sealed class Fixture
    {
        private readonly IOptions<MongoDbAsyncResponseTransportOptions> _options;
        private readonly MongoDbTransportStore _store;
        private readonly Queue<MongoTransportMessageDocument> _pending = new();
        private readonly object _gate = new();

        public Fixture(
            bool useChangeStreamWake = false,
            TimeSpan? emptyPollDelay = null,
            bool honourClaimCancellation = false)
        {
            _options = Options.Create(new MongoDbAsyncResponseTransportOptions
            {
                // Index creation would need a whole second mock surface and is covered elsewhere.
                AutoCreateIndexes = false,
                UseChangeStreamWake = useChangeStreamWake,
                WorkerQueue = "workers",
                ResponseQueue = "responses",
                ShutdownTimeout = TimeSpan.FromSeconds(1),
                SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(5),
                SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(20),
                WorkerSubscriber = { BatchSize = 2, EmptyPollDelay = emptyPollDelay ?? TimeSpan.FromMilliseconds(10) },
                ResponseSubscriber = { BatchSize = 2, EmptyPollDelay = emptyPollDelay ?? TimeSpan.FromMilliseconds(10) }
            });

            Database
                .Setup(database => database.GetCollection<MongoTransportMessageDocument>(
                    It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
                .Returns(Messages.Object);

            // The claim: hand back the next queued document, or null for "queue empty". With
            // honourClaimCancellation it also throws on a cancelled token, as the real driver does.
            Messages
                .Setup(collection => collection.FindOneAndUpdateAsync(
                    It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                    It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                    It.IsAny<FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument>>(),
                    It.IsAny<CancellationToken>()))
                .Returns((
                    FilterDefinition<MongoTransportMessageDocument> _,
                    UpdateDefinition<MongoTransportMessageDocument> __,
                    FindOneAndUpdateOptions<MongoTransportMessageDocument, MongoTransportMessageDocument> ___,
                    CancellationToken cancellationToken) =>
                {
                    ClaimAttempted.TrySetResult();
                    if (honourClaimCancellation)
                    {
                        // Park until shutdown so the cancellation lands mid-claim.
                        cancellationToken.WaitHandle.WaitOne(TimeSpan.FromSeconds(30));
                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    lock (_gate)
                    {
                        // The driver's FindOneAndUpdateAsync return is non-null-annotated, but a
                        // null document IS the real empty-queue result — forgive it deliberately
                        // instead of widening the mock signature (which trips CS8619).
                        return Task.FromResult(_pending.Count > 0 ? _pending.Dequeue() : null!);
                    }
                });

            // Acks delete the document; nothing else in these tests inspects the result.
            Messages
                .Setup(collection => collection.DeleteOneAsync(
                    It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeleteResult.Acknowledged(1));

            Database.WithTestNamespace();
            _store = new MongoDbTransportStore(Database.Object, _options);
        }

        public Mock<IMongoDatabase> Database { get; } = new(MockBehavior.Loose);
        public Mock<IMongoCollection<MongoTransportMessageDocument>> Messages { get; } = new(MockBehavior.Loose);
        public Mock<IAsyncResponseIngress> Ingress { get; } = new();
        public CollectingLogger Logger { get; } = new();

        /// <summary>Completes once the loop has reached the store, so a test can act after the first sweep.</summary>
        public TaskCompletionSource ClaimAttempted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void QueueDocuments(params MongoTransportMessageDocument[] documents)
        {
            lock (_gate)
            {
                foreach (var document in documents)
                    _pending.Enqueue(document);
            }
        }

        public MongoDbWorkerSubscriber WorkerSubscriber()
            => new(_options, _store, Ingress.Object, Logger.For<MongoDbWorkerSubscriber>());

        public MongoDbResponseIngressSubscriber ResponseSubscriber()
            => new(_options, _store, Ingress.Object, Logger.For<MongoDbResponseIngressSubscriber>());
    }

    /// <summary>
    /// A change-stream cursor driven by the test: <see cref="Notify"/> yields a batch,
    /// <see cref="EndStream"/> ends it, and <c>ignoreCancellation</c> models a stream that will not
    /// come back when asked to stop.
    /// </summary>
    private sealed class FakeChangeStreamCursor(bool ignoreCancellation = false)
        : IChangeStreamCursor<ChangeStreamDocument<MongoTransportMessageDocument>>
    {
        private readonly SemaphoreSlim _batches = new(0);
        private volatile bool _disposed;
        private volatile bool _ended;

        public IEnumerable<ChangeStreamDocument<MongoTransportMessageDocument>> Current { get; private set; } = [];

        public void Notify() => _batches.Release();

        /// <summary>Ends the stream, so the wake loop's MoveNext returns false.</summary>
        public void EndStream()
        {
            _ended = true;
            _batches.Release();
        }

        /// <summary>Unparks an ignore-cancellation cursor so the test process can settle.</summary>
        public void Release() => _batches.Release();

        public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
        {
            await _batches.WaitAsync(ignoreCancellation ? CancellationToken.None : cancellationToken).ConfigureAwait(false);
            if (_disposed || _ended)
                return false;

            // One notification per batch is all the wake loop needs; the document itself is unread.
            Current = [null!];
            return true;
        }

        public bool MoveNext(CancellationToken cancellationToken = default)
            => MoveNextAsync(cancellationToken).GetAwaiter().GetResult();

        // Resume tokens only matter to a cursor that reconnects; this one is driven by the test.
        public BsonDocument GetResumeToken() => new();

        public void Dispose()
        {
            _disposed = true;
            _batches.Release();
        }
    }

}
