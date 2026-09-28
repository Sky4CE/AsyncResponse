using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Moq;
using Npgsql;
using System.Data.Common;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Store, subscriber and registration paths of the PostgreSQL, SQL Server and MongoDB transports
/// that need no database: relational stores point at a closed port, the MongoDB store runs over
/// driver mocks.
/// </summary>
public sealed class DbTransportStoreCoverageGapTests
{
    private const string ClosedSqlServer = "Server=tcp:127.0.0.1,1;Database=none;User ID=sa;Password=unused;Encrypt=False;Connect Timeout=1";
    private const string ClosedPostgreSql = "Host=127.0.0.1;Port=1;Username=unused;Password=unused;Database=none;Timeout=1;Pooling=false";

    // ---------------------------------------------------------------- SQL Server store

    /// <summary>
    /// The table-work backoff logs WHY the host now refuses transport operations for the window: a
    /// lost lock wait names the lock and its timeout, anything else names the failed index build and
    /// its error number — each with the retry-after the host will wait.
    /// </summary>
    [Theory]
    [InlineData(1222, true)]
    [InlineData(51000, true)]
    [InlineData(9002, false)]
    public void SqlServer_TableWorkBackoff_LogsTheCauseAndTheRetryAfter(int number, bool lockWait)
    {
        var logger = new CollectingLogger();
        var store = new SqlServerTransportStore(
            Options.Create(new SqlServerAsyncResponseTransportOptions { ConnectionString = ClosedSqlServer }),
            logger.For<SqlServerTransportStore>());
        var failure = RelationalSharedHelperTests.SqlExceptionWith(number);

        var window = store.BackOffDdlAfterTableWorkFailure(failure);

        var (message, exception) = Assert.Single(logger.Entries);
        Assert.Same(failure, exception);
        Assert.Contains(window.ToString(), message, StringComparison.Ordinal);
        if (lockWait)
        {
            Assert.Contains("could not take its table lock within 5000 ms", message, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains("index build", message, StringComparison.Ordinal);
            Assert.Contains("failed (error 9002)", message, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// A caller that queued on the ensure gate behind an attempt that SUCCEEDED returns once it gets
    /// the gate, without starting (or dialling for) another attempt.
    /// </summary>
    [Fact]
    public async Task SqlServer_EnsureCreated_QueuedBehindASuccessfulAttempt_ReturnsWithoutAnotherAttempt()
    {
        var store = new SqlServerTransportStore(Options.Create(new SqlServerAsyncResponseTransportOptions { ConnectionString = ClosedSqlServer }));

        await ReturnsOnceTheGateOpensOnACreatedStore(store);
    }

    /// <summary>
    /// Operator-managed schema: a catalog that cannot be read skips the dequeue-index check (at Debug)
    /// rather than failing startup — the index is a performance concern, never correctness.
    /// </summary>
    [Fact]
    public async Task SqlServer_ClaimIndexCheck_OnAnUnreadableCatalog_IsSkippedAtDebug()
    {
        var logger = new CollectingLogger();
        var store = new SqlServerTransportStore(
            Options.Create(new SqlServerAsyncResponseTransportOptions { ConnectionString = ClosedSqlServer, AutoCreateSchema = false }),
            logger.For<SqlServerTransportStore>());
        await using var neverOpened = new SqlConnection(ClosedSqlServer);

        await (Task)typeof(SqlServerTransportStore)
            .GetMethod("WarnIfClaimIndexMissingAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(store, [neverOpened, CancellationToken.None])!;

        var (message, exception) = Assert.Single(logger.Entries);
        Assert.StartsWith("Skipping the dequeue-index check", message, StringComparison.Ordinal);
        Assert.NotNull(exception);
        Assert.DoesNotContain(logger.Messages, line => line.Contains("has no usable index", StringComparison.Ordinal));
    }

    /// <summary>
    /// The renew command's backstop timeout is the heartbeat's per-attempt bound (LockTimeout / 3)
    /// rounded UP to whole seconds, never 0 (which SqlClient reads as "no limit") and never past
    /// <see cref="int.MaxValue"/>.
    /// </summary>
    [Theory]
    [InlineData(30_000, 10)]
    [InlineData(31_000, 11)]
    [InlineData(100, 1)]
    [InlineData(1, 1)]
    public void SqlServer_RenewCommandTimeout_IsTheBeatRoundedUpToWholeSeconds(int lockTimeoutMs, int expectedSeconds)
        => Assert.Equal(expectedSeconds, SqlServerTransportStore.RenewCommandTimeoutSeconds(TimeSpan.FromMilliseconds(lockTimeoutMs)));

    [Fact]
    public void SqlServer_RenewCommandTimeout_ClampsAtIntMaxValue()
        => Assert.Equal(int.MaxValue, SqlServerTransportStore.RenewCommandTimeoutSeconds(TimeSpan.MaxValue));

    /// <summary>
    /// A lease renewal that cannot reach the server surfaces the connection failure to the heartbeat
    /// (which retries it on its backoff) rather than reporting the fence lost.
    /// </summary>
    [Fact]
    public async Task SqlServer_RenewLease_AgainstAnUnreachableServer_Throws()
    {
        var store = new SqlServerTransportStore(Options.Create(new SqlServerAsyncResponseTransportOptions { ConnectionString = ClosedSqlServer }));

        await Assert.ThrowsAnyAsync<DbException>(() => InvokeRenewAsync(store));
    }

    /// <summary>The public constructor builds its own store over the configured connection string.</summary>
    [Fact]
    public async Task SqlServer_WorkerTransport_PublicConstructor_PublishesThroughTheConfiguredServer()
    {
        var transport = new SqlServerWorkerTransport(Options.Create(new SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = ClosedSqlServer,
            PublishMaxAttempts = 1
        }));

        Assert.Equal(AsyncResponseChannelOptions.MaxPersistenceTtl, transport.MaxPublishDelay);
        await Assert.ThrowsAnyAsync<DbException>(() => transport.PublishAsync(Job()));
    }

    /// <summary>
    /// A subscriber attempt that fails (the queue table is unreachable) is retried by the supervisor
    /// on the configured backoff, with the SQL Server line naming the queue, the role and the delay.
    /// </summary>
    [Fact]
    public async Task SqlServer_SubscriberFailure_IsLoggedAndRetriedOnTheBackoff()
    {
        var logger = new CollectingLogger();
        var options = Options.Create(new SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = ClosedSqlServer,
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(20),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(20)
        });
        var store = new SqlServerTransportStore(options);
        Prelatch(store);
        var subscriber = new SqlServerWorkerSubscriber(options, store, Mock.Of<IAsyncResponseIngress>(), logger.For<SqlServerWorkerSubscriber>());

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await logger.WaitForAsync("SQL Server subscriber failed for queue worker (Worker); retrying in", occurrences: 2);
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
        }

        Assert.All(
            logger.Entries.Where(entry => entry.Message.StartsWith("SQL Server subscriber failed", StringComparison.Ordinal)),
            entry => Assert.IsAssignableFrom<DbException>(entry.Exception));
    }

    // ---------------------------------------------------------------- PostgreSQL store

    [Fact]
    public async Task PostgreSql_EnsureCreated_QueuedBehindASuccessfulAttempt_ReturnsWithoutAnotherAttempt()
    {
        await using var dataSource = NpgsqlDataSource.Create(ClosedPostgreSql);
        var store = new PostgreSqlTransportStore(dataSource, Options.Create(new PostgreSqlAsyncResponseTransportOptions()));

        await ReturnsOnceTheGateOpensOnACreatedStore(store);
    }

    /// <summary>The DDL retry-after clock seam reads back what was set on the store's DDL guard.</summary>
    [Fact]
    public async Task PostgreSql_Clock_RoundTripsThroughTheDdlGuard()
    {
        await using var dataSource = NpgsqlDataSource.Create(ClosedPostgreSql);
        var store = new PostgreSqlTransportStore(dataSource, Options.Create(new PostgreSqlAsyncResponseTransportOptions()));
        Assert.Same(TimeProvider.System, store.Clock);

        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        store.Clock = clock;

        Assert.Same(clock, store.Clock);
    }

    [Fact]
    public async Task PostgreSql_RenewLease_AgainstAnUnreachableServer_Throws()
    {
        await using var dataSource = NpgsqlDataSource.Create(ClosedPostgreSql);
        var store = new PostgreSqlTransportStore(dataSource, Options.Create(new PostgreSqlAsyncResponseTransportOptions()));

        await Assert.ThrowsAnyAsync<DbException>(() => InvokeRenewAsync(store));
    }

    /// <summary>The public constructor builds its own store over the host's data source.</summary>
    [Fact]
    public async Task PostgreSql_WorkerTransport_PublicConstructor_PublishesThroughTheGivenDataSource()
    {
        await using var dataSource = NpgsqlDataSource.Create(ClosedPostgreSql);
        var transport = new PostgreSqlWorkerTransport(
            Options.Create(new PostgreSqlAsyncResponseTransportOptions { PublishMaxAttempts = 1 }),
            dataSource);

        Assert.Equal(AsyncResponseChannelOptions.MaxPersistenceTtl, transport.MaxPublishDelay);
        await Assert.ThrowsAnyAsync<DbException>(() => transport.PublishAsync(Job()));
    }

    [Fact]
    public async Task PostgreSql_SubscriberFailure_IsLoggedAndRetriedOnTheBackoff()
    {
        var logger = new CollectingLogger();
        await using var dataSource = NpgsqlDataSource.Create(ClosedPostgreSql);
        var options = Options.Create(new PostgreSqlAsyncResponseTransportOptions
        {
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(20),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(20),
            ShutdownTimeout = TimeSpan.FromSeconds(5)
        });
        var store = new PostgreSqlTransportStore(dataSource, options);
        Prelatch(store);
        var subscriber = new PostgreSqlWorkerSubscriber(options, store, Mock.Of<IAsyncResponseIngress>(), logger.For<PostgreSqlWorkerSubscriber>());

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await logger.WaitForAsync("PostgreSQL subscriber failed for queue worker (Worker); retrying in", occurrences: 2);
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
        }

        Assert.All(
            logger.Entries.Where(entry => entry.Message.StartsWith("PostgreSQL subscriber failed", StringComparison.Ordinal)),
            entry => Assert.IsAssignableFrom<DbException>(entry.Exception));
    }

    /// <summary>
    /// The LISTEN helper retries a failed LISTEN on its own backoff while the claim side carries on:
    /// here the claim loop is parked at host stop (so the attempt stays up) and the unreachable server
    /// fails every LISTEN — the helper keeps coming back after each wait instead of dying on the
    /// first failure and leaving the subscriber poll-only.
    /// </summary>
    [Fact]
    public async Task PostgreSql_ListenHelper_KeepsRetryingAfterEachBackoff_WhileTheAttemptIsUp()
    {
        using var host = new DurableFlowContextTestSupport.StoppingHost();
        host.StopApplication();
        var logger = new CollectingLogger();
        await using var dataSource = NpgsqlDataSource.Create(ClosedPostgreSql);
        var options = Options.Create(new PostgreSqlAsyncResponseTransportOptions
        {
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(10),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(10),
            ShutdownTimeout = TimeSpan.FromSeconds(5)
        });
        var store = new PostgreSqlTransportStore(dataSource, options);
        Prelatch(store);
        var subscriber = new PostgreSqlWorkerSubscriber(options, store, Mock.Of<IAsyncResponseIngress>(), logger.For<PostgreSqlWorkerSubscriber>(), host);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await logger.WaitForAsync("PostgreSQL LISTEN helper for queue worker failed; retrying in", occurrences: 3);
            await logger.WaitForAsync("stopped claiming");
        }
        finally
        {
            await subscriber.StopAsync(CancellationToken.None);
        }

        // The attempt itself never failed: only the helper did, and it was retried in place.
        Assert.DoesNotContain(logger.Messages, message => message.Contains("subscriber failed for queue", StringComparison.Ordinal));
    }

    // ---------------------------------------------------------------- MongoDB store

    /// <summary>
    /// Index DDL disabled and the collection not created yet: <c>listIndexes</c> answers
    /// NamespaceNotFound (26), which is exactly "no claim index" — the warning still fires instead of
    /// the check being skipped as unreadable.
    /// </summary>
    [Fact]
    public async Task Mongo_ClaimIndexCheck_OnAMissingCollection_WarnsThatTheClaimIndexIsMissing()
    {
        var database = new Mock<IMongoDatabase>().WithTestNamespace();
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var indexes = new Mock<IMongoIndexManager<MongoTransportMessageDocument>>(MockBehavior.Loose);
        indexes
            .Setup(m => m.ListAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new MongoCommandException(
                MongoReplicationTimeouts.Connection,
                "ns does not exist",
                new BsonDocument("listIndexes", "asyncresponse_transport_messages"),
                new BsonDocument { ["ok"] = 0, ["code"] = 26, ["errmsg"] = "ns does not exist" }));
        collection.SetupGet(c => c.Indexes).Returns(indexes.Object);
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        var logger = new CollectingLogger();
        using var store = new MongoDbTransportStore(
            database.Object,
            Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false, UseOwnershipLedger = false }),
            logger: logger.For<MongoDbTransportStore>());

        await store.EnsureCreatedAsync();

        Assert.Contains(logger.Messages, message => message.Contains("no index leading on 'queue'", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Messages, message => message.Contains("listIndexes was not available", StringComparison.Ordinal));
    }

    /// <summary>
    /// An unreadable document whose <c>payload</c> is BSON null is buried with an EMPTY payload (not
    /// the text "null"), then deleted.
    /// </summary>
    [Fact]
    public async Task Mongo_UnreadableDocument_WithANullPayload_IsBuriedWithAnEmptyPayload()
    {
        var upserts = new List<UpdateDefinition<MongoTransportMessageDocument>>();
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        collection
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoTransportMessageDocument> _, UpdateDefinition<MongoTransportMessageDocument> update, UpdateOptions _, CancellationToken _) => upserts.Add(update))
            .ReturnsAsync(new UpdateResult.Acknowledged(0, 0, BsonNull.Value));
        var claims = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        claims.ClaimsInOrder(new BsonDocument { ["_id"] = ObjectId.GenerateNewId(), ["queue"] = "worker", ["payload"] = BsonNull.Value }, null);
        claims
            .Setup(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<BsonDocument>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteResult.Acknowledged(1));
        var logger = new CollectingLogger();
        using var store = CreateMongoStore(
            collection.Object,
            claims.Object,
            new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false, UseOwnershipLedger = false },
            logger.For<MongoDbTransportStore>());

        Assert.Null(await store.TryClaimAsync("worker", TimeSpan.FromSeconds(30), CancellationToken.None));

        var set = Assert.Single(upserts)
            .Render(new RenderArgs<MongoTransportMessageDocument>(BsonSerializer.LookupSerializer<MongoTransportMessageDocument>(), BsonSerializer.SerializerRegistry))
            .AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal("", set["payload"]["$literal"].AsString);
        Assert.Equal("deadletter", set["queue"]["$literal"].AsString);
        claims.Verify(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<BsonDocument>>(), It.IsAny<CancellationToken>()), Times.Once);
        Assert.Contains(logger.Messages, message => message.Contains("dead-lettered it without executing it", StringComparison.Ordinal));
    }

    /// <summary>
    /// With <c>DeadLetterEnabled = false</c> an unreadable document is removed without writing any
    /// copy, and the Error says so instead of claiming a dead-letter.
    /// </summary>
    [Fact]
    public async Task Mongo_UnreadableDocument_WithDeadLetteringDisabled_IsRemovedWithoutACopy()
    {
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var claims = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        var poisonId = ObjectId.GenerateNewId();
        claims.ClaimsInOrder(new BsonDocument { ["_id"] = poisonId, ["queue"] = "worker", ["payload"] = "{}" }, null);
        claims
            .Setup(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<BsonDocument>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteResult.Acknowledged(1));
        var logger = new CollectingLogger();
        using var store = CreateMongoStore(
            collection.Object,
            claims.Object,
            new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false, UseOwnershipLedger = false, DeadLetterEnabled = false },
            logger.For<MongoDbTransportStore>());

        Assert.Null(await store.TryClaimAsync("worker", TimeSpan.FromSeconds(30), CancellationToken.None));

        collection.Verify(c => c.UpdateOneAsync(
            It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
            It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
            It.IsAny<UpdateOptions>(),
            It.IsAny<CancellationToken>()), Times.Never);
        claims.Verify(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<BsonDocument>>(), It.IsAny<CancellationToken>()), Times.Once);
        var (message, exception) = Assert.Single(logger.Entries, entry => entry.Message.Contains(poisonId.ToString(), StringComparison.Ordinal));
        Assert.Contains("removed it without executing it (DeadLetterEnabled is false", message, StringComparison.Ordinal);
        Assert.NotNull(exception);
    }

    /// <summary>
    /// A publish whose upsert loses a same-id race reports duplicate key: the job is already there,
    /// which is exactly what the (idempotent, retried) publish asked for — success, no retry.
    /// </summary>
    [Fact]
    public async Task Mongo_Publish_ThatLosesADuplicateKeyRace_IsIdempotentSuccess()
    {
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();
        collection
            .Setup(c => c.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new MongoWriteException(MongoReplicationTimeouts.Connection, MongoReplicationTimeouts.DuplicateKeyError(), writeConcernError: null, innerException: null));
        var options = Options.Create(new MongoDbAsyncResponseTransportOptions { AutoCreateIndexes = false, UseOwnershipLedger = false });
        using var store = CreateMongoStore(collection.Object, claims: null, options.Value, logger: null);
        var transport = new MongoDbWorkerTransport(options, store);

        await transport.PublishAsync(Job());

        collection.Verify(c => c.UpdateOneAsync(
            It.IsAny<FilterDefinition<MongoTransportMessageDocument>>(),
            It.IsAny<UpdateDefinition<MongoTransportMessageDocument>>(),
            It.IsAny<UpdateOptions>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    /// <summary>The message collection may not take the cross-component ownership ledger's name.</summary>
    [Fact]
    public void Mongo_MessageCollection_NamedLikeTheOwnershipLedger_IsRejected()
    {
        var collection = new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).SelfPinning();

        var error = Assert.Throws<InvalidOperationException>(() => CreateMongoStore(
            collection.Object,
            claims: null,
            new MongoDbAsyncResponseTransportOptions { MessageCollection = "asyncresponse_ownership" },
            logger: null));

        Assert.Contains("MessageCollection 'asyncresponse_ownership' is reserved for the cross-component ownership ledger", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// With neither an <see cref="IMongoDatabase"/> nor an <see cref="IMongoClient"/> registered, the
    /// store needs a connection string to own a client: resolving it without one names the option.
    /// </summary>
    [Fact]
    public void Mongo_Registration_WithoutADatabaseClientOrConnectionString_NamesTheMissingOption()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse().WithInMemoryChannel().WithMongoDbTransport(options => options.DatabaseName = "db");
        using var provider = services.BuildServiceProvider();

        var error = Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<MongoDbTransportStore>());

        Assert.Contains("ConnectionString must be configured when no IMongoDatabase or IMongoClient is registered", error.Message, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------- helpers

    private static async Task ReturnsOnceTheGateOpensOnACreatedStore(object store)
    {
        var type = store.GetType();
        var gate = (SemaphoreSlim)type.GetField("_ensureGate", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(store)!;
        var created = type.GetField("_created", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var attempt = type.GetField("_ensureAttempt", BindingFlags.Instance | BindingFlags.NonPublic)!;

        // Another caller's attempt holds the gate; this caller queues on it.
        await gate.WaitAsync();
        var ensuring = ((Func<CancellationToken, Task>)Delegate.CreateDelegate(
            typeof(Func<CancellationToken, Task>), store, type.GetMethod("EnsureCreatedAsync")!))(CancellationToken.None);
        Assert.False(ensuring.IsCompleted);

        // That attempt succeeds and releases the gate.
        created.SetValue(store, true);
        gate.Release();

        await ensuring.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Null(attempt.GetValue(store));
        Assert.Equal(1, gate.CurrentCount);
    }

    private static Task<bool> InvokeRenewAsync(object store)
        => ((ValueTask<bool>)store.GetType()
            .GetMethod("RenewLeaseAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(store, [Guid.NewGuid(), Guid.NewGuid(), TimeSpan.FromSeconds(30), CancellationToken.None])!).AsTask();

    private static void Prelatch(object store)
        => store.GetType().GetField("_created", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(store, true);

    private static MongoDbTransportStore CreateMongoStore(
        IMongoCollection<MongoTransportMessageDocument> collection,
        IMongoCollection<BsonDocument>? claims,
        MongoDbAsyncResponseTransportOptions options,
        ILogger<MongoDbTransportStore>? logger)
    {
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose).WithTestNamespace();
        database
            .Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(collection);
        if (claims is not null)
        {
            database
                .Setup(d => d.GetCollection<BsonDocument>(options.MessageCollection, It.IsAny<MongoCollectionSettings>()))
                .Returns(claims);
        }

        return new MongoDbTransportStore(database.Object, Options.Create(options), logger: logger);
    }

    private static WorkerJobEnvelope Job() => new()
    {
        Call = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(DbTransportStoreCoverageGapTests).FullName!,
            MethodName = nameof(Job),
            Params = []
        }
    };
}
