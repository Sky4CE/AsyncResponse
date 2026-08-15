using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Npgsql;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The DB subscribers acquire resources BEFORE their claim loop — SQL Server subscribes a handler
/// on the singleton store's <c>MessagePublished</c>, PostgreSQL and MongoDB start a listen/wake
/// task whose only stop signal is a linked CTS that <c>Dispose</c> does NOT cancel. An escape
/// between that acquisition and the loop's try (the dispatcher constructor, a throwing logger
/// provider) must still run the releasing finally: the retry wrapper rebuilds the run, so a leak
/// repeats per retry — dead closures invoked on every later publish, or a parked listen loop (and
/// its pooled connection / change-stream cursor) per attempt.
/// </summary>
public sealed class TransportSubscriberTeardownTests
{
    [Fact]
    public async Task SqlServerRun_ThatEscapesBeforeTheLoop_UnsubscribesThePublishHandler()
    {
        var options = Options.Create(new SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = "Server=tcp:127.0.0.1,1;Database=none;User ID=sa;Password=unused;Encrypt=False;Connect Timeout=1",
            // Hostile subscriber options: valid for the service constructor (which validates only
            // the common options) but rejected by the dispatcher constructor — the deterministic
            // escape between the += and the claim loop.
            WorkerSubscriber = { BatchSize = 0 }
        });
        var store = new SqlServerTransportStore(options);
        Prelatch(store);
        var subscriber = new SqlServerWorkerSubscriber(
            options, store, Mock.Of<IAsyncResponseIngress>(), NullLogger<SqlServerWorkerSubscriber>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => RunSubscriberAsync(subscriber));
        Assert.Contains("BatchSize", ex.Message, StringComparison.Ordinal);

        // The finally must have unsubscribed: the store is a singleton, so a leaked closure would
        // survive this run and every retry, invoked by every later publish.
        var handler = typeof(SqlServerTransportStore)
            .GetField("MessagePublished", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(store);
        Assert.Null(handler);
    }

    [Fact]
    public async Task PostgreSqlRun_ThatEscapesBeforeTheLoop_StopsTheListenTask()
    {
        await using var dataSource = NpgsqlDataSource.Create(
            "Host=127.0.0.1;Port=1;Username=unused;Password=unused;Database=none;Timeout=1;Pooling=false");
        var options = Options.Create(new PostgreSqlAsyncResponseTransportOptions
        {
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(25),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(25),
            ShutdownTimeout = TimeSpan.FromSeconds(5),
            WorkerSubscriber = { BatchSize = 0 }
        });
        var store = new PostgreSqlTransportStore(dataSource, options);
        Prelatch(store);
        var logger = new CollectingLogger();
        var subscriber = new PostgreSqlWorkerSubscriber(
            options, store, Mock.Of<IAsyncResponseIngress>(), logger.For<PostgreSqlWorkerSubscriber>());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => RunSubscriberAsync(subscriber));
        Assert.Contains("BatchSize", ex.Message, StringComparison.Ordinal);

        // The finally cancelled AND joined the listen task before the fault surfaced, so its
        // failure-retry loop (unreachable server, 25 ms backoff) must be silent from here on. A
        // leaked task keeps retrying and logging forever.
        var settled = ListenFailureCount(logger);
        await Task.Delay(500);
        Assert.Equal(settled, ListenFailureCount(logger));
    }

    [Fact]
    public async Task MongoDbRun_ThatEscapesBeforeTheLoop_CancelsTheChangeStreamWake()
    {
        var watchStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var watchCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cursor = new ParkedChangeStreamCursor(watchStarted, watchCancelled);

        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose);
        collection
            .Setup(c => c.WatchAsync(
                It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoTransportMessageDocument>, ChangeStreamDocument<MongoTransportMessageDocument>>>(),
                It.IsAny<ChangeStreamOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursor);
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose).WithTestNamespace();
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);

        var options = Options.Create(new MongoDbAsyncResponseTransportOptions
        {
            AutoCreateIndexes = false,
            UseChangeStreamWake = true,
            ShutdownTimeout = TimeSpan.FromSeconds(5)
        });
        using var store = new MongoDbTransportStore(database.Object, options);
        // The escape here is a throwing logger provider — it blocks until the wake loop is
        // genuinely parked on the change stream, then throws from the "subscriber started" log
        // call, i.e. after the wake task started and before the claim loop's try.
        var subscriber = new MongoDbWorkerSubscriber(
            options, store, Mock.Of<IAsyncResponseIngress>(), new ExplodingStartupLogger<MongoDbWorkerSubscriber>(watchStarted.Task));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => RunSubscriberAsync(subscriber));
        Assert.Equal("logger exploded", ex.Message);

        // The finally must cancel the wake task's token; disposing the linked CTS alone does not,
        // leaving the loop parked on the cursor forever (one leaked cursor per retry).
        await watchCancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static Task RunSubscriberAsync(object subscriber)
    {
        var method = subscriber.GetType().BaseType!
            .GetMethod("RunSubscriberAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);
        return (Task)method!.Invoke(subscriber, [CancellationToken.None])!;
    }

    /// <summary>Latches the store's <c>_created</c> so EnsureCreated never dials the (bogus) server.</summary>
    private static void Prelatch(object store)
        => store.GetType().GetField("_created", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(store, true);

    private static int ListenFailureCount(CollectingLogger logger)
        => logger.Messages.Count(message => message.StartsWith("PostgreSQL LISTEN helper", StringComparison.Ordinal));

    /// <summary>
    /// Parks MoveNext on the caller's token: <paramref name="started"/> completes once the wake
    /// loop is genuinely inside the cursor, and <paramref name="cancelled"/> completes only if
    /// that token is ever cancelled — the observable difference between a stopped and a leaked
    /// wake loop.
    /// </summary>
    private sealed class ParkedChangeStreamCursor(TaskCompletionSource started, TaskCompletionSource cancelled)
        : IChangeStreamCursor<ChangeStreamDocument<MongoTransportMessageDocument>>
    {
        public IEnumerable<ChangeStreamDocument<MongoTransportMessageDocument>> Current { get; } = [];

        public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
        {
            using var registration = cancellationToken.Register(() => cancelled.TrySetResult());
            started.TrySetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            return false;
        }

        public bool MoveNext(CancellationToken cancellationToken = default)
            => MoveNextAsync(cancellationToken).GetAwaiter().GetResult();

        public BsonDocument GetResumeToken() => new();

        public void Dispose()
        {
        }
    }

    /// <summary>
    /// Real logger failure shape: throws from the "subscriber started" information log — the one
    /// call sitting between the wake-task start and the claim loop — after first waiting for the
    /// wake loop to be parked, so the escape is deterministic.
    /// </summary>
    private sealed class ExplodingStartupLogger<T>(Task waitBeforeThrowing) : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => NullLogger.Instance.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!formatter(state, exception).Contains("subscriber started", StringComparison.OrdinalIgnoreCase))
                return;

            waitBeforeThrowing.Wait(TimeSpan.FromSeconds(30));
            throw new InvalidOperationException("logger exploded");
        }
    }
}
