using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Transports.PostgreSQL;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Npgsql;
using System.Reflection;
using Xunit;
using PgServer = AsyncResponse.Tests.DbChannelCoverageGapPostgresServer;
using SqlServer = AsyncResponse.Tests.DbChannelCoverageGapSqlServer;

namespace AsyncResponse.Tests;

/// <summary>
/// The database channels' stores below the shared base — <c>PostgreSqlChannelSql</c>,
/// <c>SqlServerChannelSql</c>, <c>MongoDbChannelStore</c> — and the LISTEN connection release
/// shared by the PostgreSQL channel and transport, on the branches their container-backed suites
/// cannot reach on cue: a schema check another caller finished while this one queued, a
/// duplicate insert whose original was pruned, driver type variations, replication timeouts
/// with nothing to read back, and pool clears. The relational stores run against scripted wire
/// servers; the MongoDB store against mocked collections.
/// </summary>
public sealed class DbChannelStoreCoverageGapTests
{
    private const string ClosedPostgres = "Host=localhost;Port=1;Database=unused;Username=unused;Password=unused;Timeout=1;Pooling=false";
    private const string ClosedSqlServer = "Server=localhost,1;Database=unused;User Id=sa;Password=unused;Encrypt=False;Connect Timeout=1";

    // ---------------------------------------------------------------------------------------
    // One-time schema work
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// A caller that queued on the schema gate behind the one that finished the DDL (or the
    /// manual-schema validation) returns without a round trip of its own: the store here points at
    /// a closed port, so any attempt would fail.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PostgreSql_EnsureCreated_ACallerQueuedBehindTheFinishedSchemaWork_ReturnsWithoutARoundTrip(bool autoCreateSchema)
    {
        await using var dataSource = NpgsqlDataSource.Create(ClosedPostgres);
        var sql = new PostgreSqlChannelSql(dataSource, Options.Create(new PostgreSqlAsyncResponseChannelOptions { AutoCreateSchema = autoCreateSchema }));

        await QueuedBehindTheGateAsync(sql, () => sql.EnsureCreatedAsync());
    }

    /// <inheritdoc cref="PostgreSql_EnsureCreated_ACallerQueuedBehindTheFinishedSchemaWork_ReturnsWithoutARoundTrip"/>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SqlServer_EnsureCreated_ACallerQueuedBehindTheFinishedSchemaWork_ReturnsWithoutARoundTrip(bool autoCreateSchema)
    {
        var sql = new SqlServerChannelSql(Options.Create(new SqlServerAsyncResponseChannelOptions { ConnectionString = ClosedSqlServer, AutoCreateSchema = autoCreateSchema }));

        await QueuedBehindTheGateAsync(sql, () => sql.EnsureCreatedAsync());
    }

    private static async Task QueuedBehindTheGateAsync(object store, Func<Task> ensureCreated)
    {
        var gate = (SemaphoreSlim)Field(store, "_ensureGate").GetValue(store)!;
        await gate.WaitAsync();
        Task ensure;
        try
        {
            ensure = ensureCreated();
            Assert.False(ensure.IsCompleted);
            // The holder finishes the schema work.
            Field(store, "_created").SetValue(store, true);
        }
        finally
        {
            gate.Release();
        }

        await ensure.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(ensure.IsCompletedSuccessfully);
    }

    /// <summary>
    /// The auto-create DDL owes only what the catalog shows missing: the two message-table
    /// columns an older build's table lacks and the one-time jsonb → text conversion of both
    /// document columns, while indexes the catalog shows usable are left alone.
    /// </summary>
    [Fact]
    public async Task PostgreSql_TableWork_IsWhatTheCatalogShowsMissing()
    {
        await using var server = new PgServer();
        server.Respond = statement => Task.FromResult(statement switch
        {
            _ when statement.Contains("information_schema.columns") => PgServer.Reply.Rows(
                [("recovery_claimed", PgServer.Bool), ("acked_seq", PgServer.Bool), ("envelope_jsonb", PgServer.Bool), ("state_jsonb", PgServer.Bool)],
                [false, false, true, true]),
            _ when statement.Contains("pg_catalog.pg_index") => PgServer.Reply.Rows([("usable", PgServer.Bool)], [true]),
            _ => PgServer.Reply.Command(statement.Sql)
        });
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var sql = new PostgreSqlChannelSql(dataSource, Options.Create(new PostgreSqlAsyncResponseChannelOptions()));
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        var work = await (Task<string>)typeof(PostgreSqlChannelSql)
            .GetMethod("ReadTableWorkAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(sql, [connection, transaction, CancellationToken.None])!;

        Assert.Contains("ADD COLUMN IF NOT EXISTS recovery_claimed boolean", work, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS acked_seq bigint", work, StringComparison.Ordinal);
        Assert.Contains("ALTER COLUMN envelope_json TYPE text", work, StringComparison.Ordinal);
        Assert.Contains("ALTER COLUMN state_json TYPE text", work, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", work, StringComparison.Ordinal);
    }

    /// <summary>The startup-DDL retry-after window's clock is a settable seam that reads back what was set.</summary>
    [Fact]
    public async Task PostgreSql_TheDdlClock_ReadsBackWhatWasSet()
    {
        await using var dataSource = NpgsqlDataSource.Create(ClosedPostgres);
        var sql = new PostgreSqlChannelSql(dataSource, Options.Create(new PostgreSqlAsyncResponseChannelOptions()));
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();

        Assert.Same(TimeProvider.System, sql.Clock);
        sql.Clock = clock;

        Assert.Same(clock, sql.Clock);
    }

    // ---------------------------------------------------------------------------------------
    // The idempotent insert
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// A duplicate insert whose original row is already gone (pruned mid-publish) is a failed
    /// publish, not a success with a fabricated timestamp.
    /// </summary>
    [Fact]
    public async Task PostgreSql_ADuplicateInsertWhoseOriginalWasPruned_FailsThePublish()
    {
        await using var server = new PgServer();
        server.Respond = statement => Task.FromResult(statement switch
        {
            _ when statement.Contains("WITH inserted AS") => PgServer.Reply.Rows([("created_at", PgServer.TimestampTz), ("pg_notify", PgServer.Text)], new object?[] { null, null }),
            _ when statement.Contains("SELECT created_at, acked_at, acked_seq") => PgServer.Reply.Rows([("created_at", PgServer.TimestampTz), ("acked_at", PgServer.TimestampTz), ("acked_seq", PgServer.Int8)]),
            _ => PgServer.Reply.Command(statement.Sql)
        });
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var sql = Created(new PostgreSqlChannelSql(dataSource, Options.Create(new PostgreSqlAsyncResponseChannelOptions { AutoCreateSchema = false })));
        var id = Guid.NewGuid();

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => sql.InsertMessageAsync(id, "corr", "{}", TimeSpan.FromMinutes(5), CancellationToken.None));

        Assert.Contains($"message {id} found no row after a duplicate", failure.Message, StringComparison.Ordinal);
    }

    /// <inheritdoc cref="PostgreSql_ADuplicateInsertWhoseOriginalWasPruned_FailsThePublish"/>
    [Fact]
    public async Task SqlServer_ADuplicateInsertWhoseOriginalWasPruned_FailsThePublish()
    {
        await using var server = new SqlServer();
        server.Respond = statement => Task.FromResult(statement switch
        {
            _ when statement.Contains("OUTPUT inserted.created_at") => SqlServer.Reply.Rows([("created_at", SqlServer.Type.DateTime2)]),
            _ when statement.Contains("SELECT created_at, acked_at, acked_seq") => SqlServer.Reply.Rows([("created_at", SqlServer.Type.DateTime2), ("acked_at", SqlServer.Type.DateTime2), ("acked_seq", SqlServer.Type.BigInt)]),
            _ => SqlServer.Reply.Affected(1)
        });
        var sql = Created(new SqlServerChannelSql(Options.Create(new SqlServerAsyncResponseChannelOptions { ConnectionString = server.ConnectionString(), AutoCreateSchema = false })));
        var id = Guid.NewGuid();

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => sql.InsertMessageAsync(id, "corr", "{}", TimeSpan.FromMinutes(5), CancellationToken.None));

        Assert.Contains($"message {id} found no row after a duplicate", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// An insert that lost the key race to a concurrent publish of the same id (a primary-key
    /// violation) is a duplicate, not a failure: the original row is read back, settlement
    /// columns included.
    /// </summary>
    [Theory]
    [InlineData(2627)]
    [InlineData(2601)]
    public async Task SqlServer_AnInsertThatLostTheKeyRace_ReturnsTheOriginalRow(int errorNumber)
    {
        var createdAt = new DateTime(2026, 9, 1, 12, 0, 0, DateTimeKind.Utc);
        var ackedAt = createdAt.AddSeconds(1);
        await using var server = new SqlServer();
        server.Respond = statement => Task.FromResult(statement switch
        {
            _ when statement.Contains("OUTPUT inserted.created_at") => SqlServer.Reply.Error(errorNumber, "Violation of PRIMARY KEY constraint"),
            _ when statement.Contains("SELECT created_at, acked_at, acked_seq") => SqlServer.Reply.Rows(
                [("created_at", SqlServer.Type.DateTime2), ("acked_at", SqlServer.Type.DateTime2), ("acked_seq", SqlServer.Type.BigInt)],
                [createdAt, ackedAt, 7L]),
            _ => SqlServer.Reply.Affected(1)
        });
        var sql = Created(new SqlServerChannelSql(Options.Create(new SqlServerAsyncResponseChannelOptions { ConnectionString = server.ConnectionString(), AutoCreateSchema = false })));

        var message = await sql.InsertMessageAsync(Guid.NewGuid(), "corr", "{}", TimeSpan.FromMinutes(5), CancellationToken.None);

        Assert.Equal(new DateTimeOffset(createdAt), message.CreatedAtUtc);
        Assert.Equal(new DateTimeOffset(ackedAt), message.AckedAtUtc);
        Assert.Equal(7L, message.AckedSeq);
    }

    // ---------------------------------------------------------------------------------------
    // The server clock
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// The server-time read normalises whatever the driver hands back: a time-with-zone comes back
    /// as a DateTimeOffset (converted to UTC), and a NULL falls back to the local clock.
    /// </summary>
    [Fact]
    public async Task PostgreSql_ServerTime_NormalisesAnOffsetValue_AndFallsBackOnNull()
    {
        var answers = new Queue<PgServer.Reply>(
        [
            PgServer.Reply.Rows([("now", PgServer.TimeTz)], [new DateTimeOffset(1, 1, 1, 12, 30, 0, TimeSpan.Zero)]),
            PgServer.Reply.Rows([("now", PgServer.Text)], new object?[] { null })
        ]);
        await using var server = new PgServer();
        server.Respond = statement => Task.FromResult(statement.Contains("SELECT now()") ? answers.Dequeue() : PgServer.Reply.Command(statement.Sql));
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var sql = Created(new PostgreSqlChannelSql(dataSource, Options.Create(new PostgreSqlAsyncResponseChannelOptions { AutoCreateSchema = false })));

        var offset = await sql.GetServerTimeUtcAsync(CancellationToken.None);
        var before = DateTimeOffset.UtcNow;
        var fallback = await sql.GetServerTimeUtcAsync(CancellationToken.None);

        Assert.Equal(TimeSpan.Zero, offset.Offset);
        Assert.Equal(new TimeSpan(12, 30, 0), offset.TimeOfDay);
        Assert.InRange(fallback, before, DateTimeOffset.UtcNow);
    }

    /// <inheritdoc cref="PostgreSql_ServerTime_NormalisesAnOffsetValue_AndFallsBackOnNull"/>
    [Fact]
    public async Task SqlServer_ServerTime_NormalisesAnOffsetValue_AndFallsBackOnNull()
    {
        var stamped = new DateTimeOffset(2026, 9, 1, 14, 0, 0, TimeSpan.FromHours(2));
        var answers = new Queue<SqlServer.Reply>(
        [
            SqlServer.Reply.Rows([("now", SqlServer.Type.DateTimeOffset)], [stamped]),
            SqlServer.Reply.Rows([("now", SqlServer.Type.DateTime2)], new object?[] { null })
        ]);
        await using var server = new SqlServer();
        server.Respond = statement => Task.FromResult(statement.Contains("SELECT SYSUTCDATETIME();") ? answers.Dequeue() : SqlServer.Reply.Affected(1));
        var sql = Created(new SqlServerChannelSql(Options.Create(new SqlServerAsyncResponseChannelOptions { ConnectionString = server.ConnectionString(), AutoCreateSchema = false })));

        var offset = await sql.GetServerTimeUtcAsync(CancellationToken.None);
        var before = DateTimeOffset.UtcNow;
        var fallback = await sql.GetServerTimeUtcAsync(CancellationToken.None);

        Assert.Equal(TimeSpan.Zero, offset.Offset);
        Assert.Equal(stamped, offset);
        Assert.InRange(fallback, before, DateTimeOffset.UtcNow);
    }

    // ---------------------------------------------------------------------------------------
    // MongoDB
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// An insert whose majority acknowledgement lapsed is read back from the primary; when even
    /// the primary holds no document, persistence is unknown and the replication timeout is
    /// rethrown instead of reporting the response stored.
    /// </summary>
    [Fact]
    public async Task Mongo_AnInsertWhoseReplicationTimedOut_WithNothingOnThePrimary_Rethrows()
    {
        var (store, messages, _) = MongoStore();
        messages
            .Setup(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(MongoReplicationTimeouts.Command());
        messages.FindsReturning<MongoChannelMessageDocument, MongoChannelMessageDocument>();

        await Assert.ThrowsAsync<MongoWriteConcernException>(() => store.InsertMessageAsync(Guid.NewGuid(), "corr", "{}", TimeSpan.FromMinutes(5), CancellationToken.None));
    }

    /// <summary>
    /// The delivery claim's ack-sequence draw whose majority acknowledgement lapsed reads the
    /// counter back; with no counter on the primary there is no value to order the claim by, so
    /// the timeout is rethrown.
    /// </summary>
    [Fact]
    public async Task Mongo_AnAckSequenceDrawWhoseReplicationTimedOut_WithNoCounter_Rethrows()
    {
        var (store, _, counters) = MongoStore();
        counters
            .Setup(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<UpdateDefinition<BsonDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<BsonDocument, BsonDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(MongoReplicationTimeouts.Command());
        counters.FindsReturning<BsonDocument, BsonDocument>();

        await Assert.ThrowsAsync<MongoWriteConcernException>(() => store.TryClaimForDeliveryAsync(Guid.NewGuid(), CancellationToken.None));
    }

    /// <summary>Hydrating no ids costs no round trip.</summary>
    [Fact]
    public async Task Mongo_HydratingNoIds_ReturnsEmptyWithoutAQuery()
    {
        var (store, messages, _) = MongoStore();

        Assert.Empty(await store.LoadMessagesByIdAsync("corr", [], CancellationToken.None));

        messages.Verify(
            collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    /// <summary>The cross-component ownership ledger's collection cannot be configured as a channel collection.</summary>
    [Fact]
    public void Mongo_TheOwnershipLedgerCollection_IsReserved()
    {
        var failure = Assert.Throws<InvalidOperationException>(() => MongoDbChannelStore.ValidateCollectionName("asyncresponse_ownership", "MessageCollection"));

        Assert.Contains("is reserved for the cross-component ownership ledger", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>Disposing the store disposes the client it owns (and only then).</summary>
    [Fact]
    public void Mongo_Dispose_DisposesTheOwnedClient()
    {
        var client = new Mock<IMongoClient>();
        var disposable = client.As<IDisposable>();
        var (store, _, _) = MongoStore(client.Object);

        store.Dispose();

        disposable.Verify(owned => owned.Dispose(), Times.Once);
    }

    /// <summary>
    /// The recovery-state and subscriber documents keep their wire shape: snake_case element
    /// names and standard-representation registration ids (what an index or an operator query
    /// written against the stored documents relies on).
    /// </summary>
    [Fact]
    public void Mongo_RecoveryAndSubscriberDocuments_KeepTheirWireShape()
    {
        var registrationId = Guid.NewGuid();
        var expiresAt = new DateTime(2026, 9, 1, 12, 0, 0, DateTimeKind.Utc);
        var recovery = new MongoRecoveryStateDocument
        {
            Id = "corr:1", CorrelationId = "corr", RegistrationId = registrationId, StateJson = "{}",
            ExpiresAtUtc = expiresAt, RegisteredAtUtc = expiresAt.AddMinutes(-5)
        }.ToBsonDocument();
        var subscriber = new MongoChannelSubscriberDocument
        {
            Id = "corr:1", CorrelationId = "corr", RegistrationId = registrationId, InstanceId = "host", ExpiresAtUtc = expiresAt
        }.ToBsonDocument();

        Assert.Equal(new BsonBinaryData(registrationId, GuidRepresentation.Standard), recovery["registration_id"]);
        Assert.Equal(expiresAt, recovery["expires_at"].ToUniversalTime());
        Assert.Equal(expiresAt.AddMinutes(-5), recovery["registered_at"].ToUniversalTime());
        Assert.Equal(new BsonBinaryData(registrationId, GuidRepresentation.Standard), subscriber["registration_id"]);
        Assert.Equal(expiresAt, subscriber["expires_at"].ToUniversalTime());
    }

    private static (MongoDbChannelStore Store, Mock<IMongoCollection<MongoChannelMessageDocument>> Messages, Mock<IMongoCollection<BsonDocument>> Counters) MongoStore(IMongoClient? ownedClient = null)
    {
        var database = new Mock<IMongoDatabase>(MockBehavior.Loose);
        database.WithTestNamespace();
        var messages = new Mock<IMongoCollection<MongoChannelMessageDocument>>(MockBehavior.Loose).SelfPinning();
        var counters = new Mock<IMongoCollection<BsonDocument>>(MockBehavior.Loose).SelfPinning();
        database
            .Setup(db => db.GetCollection<MongoChannelMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(messages.Object);
        database
            .Setup(db => db.GetCollection<BsonDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(counters.Object);
        database.WithLooseCollection<MongoChannelSubscriberDocument>();
        database.WithLooseCollection<MongoRecoveryStateDocument>();
        var store = new MongoDbChannelStore(
            database.Object,
            Options.Create(new MongoDbAsyncResponseChannelOptions { AutoCreateIndexes = false, UseOwnershipLedger = false }),
            ownedClient);
        return (store, messages, counters);
    }

    // ---------------------------------------------------------------------------------------
    // The LISTEN connection release (PostgreSQL channel and transport)
    // ---------------------------------------------------------------------------------------

    public enum Package
    {
        Channel,
        Transport
    }

    /// <summary>A connection that never opened has nothing to UNLISTEN: the release sends nothing and disposes it.</summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.Transport)]
    public async Task Release_AConnectionThatNeverOpened_SendsNothing(Package package)
    {
        await using var server = new FakePostgresWireServer();
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var connection = dataSource.CreateConnection();

        await Release(package, dataSource, connection, listening: true, logger: null).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(0, server.Sessions);
        Assert.Empty(server.Statements);
    }

    /// <summary>
    /// A still-listening connection whose data source was disposed first (a host shutting down
    /// its data source before the listener's teardown finished): the release still clears,
    /// disposes and reports without throwing — it runs while another exception may be unwinding.
    /// </summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.Transport)]
    public async Task Release_AfterItsDataSourceWasDisposed_StillClearsAndReportsWithoutThrowing(Package package)
    {
        await using var server = new FakePostgresWireServer();
        server.Respond = (_, sql) => Task.FromResult(sql.StartsWith("UNLISTEN", StringComparison.Ordinal)
            ? FakePostgresWireServer.Reply.Error("unlisten refused")
            : FakePostgresWireServer.Reply.Complete(sql));
        var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var connection = await dataSource.OpenConnectionAsync();
        await dataSource.DisposeAsync();
        var logger = new CollectingLogger();

        await Release(package, dataSource, connection, listening: true, logger).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Single(logger.Messages, message => message.Contains("cleared the NpgsqlDataSource's connection pool", StringComparison.Ordinal));
        await connection.DisposeAsync();
    }

    /// <summary>
    /// Pool clears are rate-limited at Warning: a second one inside the warning interval logs at
    /// Debug that the pool was cleared again.
    /// </summary>
    [Theory]
    [InlineData(Package.Channel)]
    [InlineData(Package.Transport)]
    public async Task Release_ASecondPoolClearInsideTheWarningInterval_LogsAtDebug(Package package)
    {
        await using var server = new FakePostgresWireServer();
        server.Respond = (_, sql) => Task.FromResult(sql.StartsWith("UNLISTEN", StringComparison.Ordinal)
            ? FakePostgresWireServer.Reply.Error("unlisten refused")
            : FakePostgresWireServer.Reply.Complete(sql));
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var logger = new CollectingLogger();

        await Release(package, dataSource, await dataSource.OpenConnectionAsync(), listening: true, logger).WaitAsync(TimeSpan.FromSeconds(30));
        await Release(package, dataSource, await dataSource.OpenConnectionAsync(), listening: true, logger).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(2, logger.Messages.Count);
        Assert.Contains("cleared the NpgsqlDataSource's connection pool again", logger.Messages[1], StringComparison.Ordinal);
    }

    private static Task Release(Package package, NpgsqlDataSource dataSource, NpgsqlConnection connection, bool listening, Microsoft.Extensions.Logging.ILogger? logger)
    {
        var type = (package == Package.Channel ? typeof(PostgreSqlChannelSql).Assembly : typeof(PostgreSqlTransportStore).Assembly)
            .GetType("AsyncResponse.Internal.PostgreSqlListenConnection", throwOnError: true)!;
        var release = type.GetMethod(
            "ReleaseAsync",
            BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public,
            [typeof(NpgsqlDataSource), typeof(NpgsqlConnection), typeof(bool), typeof(Microsoft.Extensions.Logging.ILogger), typeof(TimeSpan), typeof(int), typeof(int)])!;
        return (Task)release.Invoke(null, [dataSource, connection, listening, logger, TimeSpan.FromSeconds(30), 0, 0])!;
    }

    // ---------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------

    private static T Created<T>(T store)
    {
        Field(store!, "_created").SetValue(store, true);
        return store;
    }

    private static FieldInfo Field(object target, string name)
        => target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!;
}
