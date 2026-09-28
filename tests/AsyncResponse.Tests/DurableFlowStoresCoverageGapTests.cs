using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Net;
using System.Reflection;
using System.Text.Json;
using AsyncResponse.DurableFlows.Cosmos;
using AsyncResponse.DurableFlows.DynamoDB;
using AsyncResponse.DurableFlows.EFCore;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.DurableFlows.PostgreSQL;
using AsyncResponse.DurableFlows.Sqlite;
using AsyncResponse.Testing;
using Microsoft.Azure.Cosmos;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Npgsql;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Durable-flow store branches no other unit test reached: the shared helpers in the store
/// assemblies that never call them, and per-store edge cases (Cosmos host serializers and
/// write-path answers, Mongo index-listing refusals, SQLite affinities, the EF Core create's last
/// attempt, option collisions).
/// </summary>
public sealed class DurableFlowStoresCoverageGapTests
{
    private const string SharedTypeName = "AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared";

    public static TheoryData<Type> ProviderOptionTypes => DurableFlowStoreSharedTests.ProviderOptionTypes;

    // ---- DurableFlowStoreShared, in every store assembly ----

    /// <summary>
    /// A prune batch that fails while the create it rides on is being cancelled is the
    /// cancellation, not a prune failure: it surfaces as an <see cref="OperationCanceledException"/>
    /// carrying the driver's error and the token, and is not counted as a failure.
    /// </summary>
    [Theory]
    [MemberData(nameof(ProviderOptionTypes))]
    public async Task PruneQuietly_ADriverErrorDuringCancellation_SurfacesAsTheCancellation(Type providerOptionsType)
    {
        var shared = Shared(providerOptionsType);
        var pruneQuietly = shared.GetMethod("PruneQuietlyAsync", BindingFlags.Public | BindingFlags.Static)!;
        var provider = $"cancelled-prune-{providerOptionsType.Name}";
        var failures = 0L;
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == AsyncResponseDiagnostics.MeterName && instrument.Name == "asyncresponse.flow_state.prune_failures")
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "provider" && Equals(tag.Value, provider))
                    Interlocked.Add(ref failures, value);
            }
        });
        listener.Start();

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var driverError = new InvalidOperationException("Operation cancelled by user.");
        Func<Task<int>> prune = () => Task.FromException<int>(driverError);

        var cancelled = await Assert.ThrowsAsync<OperationCanceledException>(
            () => (Task)pruneQuietly.Invoke(null, [prune, TimeSpan.Zero, provider, null, cts.Token])!);

        Assert.Same(driverError, cancelled.InnerException);
        Assert.Equal(cts.Token, cancelled.CancellationToken);
        Assert.Contains($"The {provider} durable-flow prune was cancelled.", cancelled.Message, StringComparison.Ordinal);
        Assert.Equal(0, Interlocked.Read(ref failures));
    }

    /// <summary>The option guards every store's Validate shares.</summary>
    [Theory]
    [MemberData(nameof(ProviderOptionTypes))]
    public void OptionGuards_RejectANegativePruneBudgetAndAMissingConnectionString(Type providerOptionsType)
    {
        var shared = Shared(providerOptionsType);

        Invoke(shared, "ValidatePruneBudget", TimeSpan.Zero, "StoreOptions");
        Invoke(shared, "ValidatePruneBudget", TimeSpan.FromSeconds(2), "StoreOptions");
        var negative = AssertInner<InvalidOperationException>(shared, "ValidatePruneBudget", TimeSpan.FromTicks(-1), "StoreOptions");
        Assert.Equal("StoreOptions.PruneBudget cannot be negative (zero limits each prune to one batch).", negative.Message);

        Invoke(shared, "ValidateConnectionString", "Data Source=db", "StoreOptions");
        foreach (var missing in new[] { null, "", "   " })
        {
            var error = AssertInner<InvalidOperationException>(shared, "ValidateConnectionString", missing, "StoreOptions");
            Assert.Equal("StoreOptions.ConnectionString must be configured.", error.Message);
        }
    }

    /// <summary>
    /// A connection that opens is handed back open and undisposed; one that fails to open is
    /// disposed before the failure propagates, so its allocation is not leaked.
    /// </summary>
    [Theory]
    [MemberData(nameof(ProviderOptionTypes))]
    public async Task OpenConnection_ReturnsAnOpenConnection_AndDisposesOneThatFailsToOpen(Type providerOptionsType)
    {
        var open = Shared(providerOptionsType)
            .GetMethod("OpenConnectionAsync", BindingFlags.Public | BindingFlags.Static)!
            .MakeGenericMethod(typeof(GapConnection));

        var healthy = $"ok-{Guid.NewGuid():N}";
        var connection = await (Task<GapConnection>)open.Invoke(null, [healthy, CancellationToken.None])!;
        Assert.Same(GapConnection.Created[healthy], connection);
        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.False(connection.WasDisposed);
        await connection.DisposeAsync();

        var refused = $"fail-{Guid.NewGuid():N}";
        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => (Task<GapConnection>)open.Invoke(null, [refused, CancellationToken.None])!);
        Assert.Equal("refused", error.Message);
        Assert.True(GapConnection.Created[refused].WasDisposed);
        Assert.Equal(ConnectionState.Closed, GapConnection.Created[refused].State);
    }

    // ---- Option validation ----

    [Fact]
    public void CosmosOptions_RefuseAPartitionKeyPathTheLedgerCannotSatisfy()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new CosmosDurableFlowOptions
        {
            DatabaseName = "flows",
            PartitionKeyPath = "/tenantId"
        }.Validate());
        Assert.Contains("must be '/flowId' or '/id'", error.Message, StringComparison.Ordinal);
        Assert.Contains("partitioned on '/tenantId'", error.Message, StringComparison.Ordinal);

        new CosmosDurableFlowOptions { DatabaseName = "flows", PartitionKeyPath = "/id" }.Validate();
    }

    [Theory]
    [InlineData(DynamoDbFlowStateStore.FlowIdAttribute)]
    [InlineData(DynamoDbFlowStateStore.StateJsonAttribute)]
    [InlineData(DynamoDbFlowStateStore.UpdatedAtAttribute)]
    [InlineData(DynamoDbFlowStateStore.RevisionAttribute)]
    [InlineData(DynamoDbFlowStateStore.LeaseIdAttribute)]
    [InlineData(DynamoDbFlowStateStore.LeaseExpiresAtAttribute)]
    public void DynamoDbOptions_RefuseATtlAttributeThatCollidesWithAStoreAttribute(string attribute)
    {
        var error = Assert.Throws<InvalidOperationException>(
            () => new DynamoDbDurableFlowOptions { TimeToLiveAttributeName = attribute }.Validate());
        Assert.Contains("must not collide with one of the store's own attributes", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MongoOptions_RefuseTheOwnershipLedgerCollection()
    {
        var error = Assert.Throws<InvalidOperationException>(
            () => new MongoDbDurableFlowOptions { CollectionName = "asyncresponse_ownership" }.Validate());
        Assert.Contains("is reserved for the cross-component ownership ledger", error.Message, StringComparison.Ordinal);
    }

    // ---- Cosmos ----

    [Fact]
    public async Task Cosmos_ALiveDocumentWithoutItsStateJson_IsUnreadable_NotAbsent()
    {
        using var cosmos = new CosmosGapHarness();
        var document = CosmosDocument("flow");
        document.StateJson = "";
        cosmos.Reads(document);

        var unreadable = await Assert.ThrowsAsync<FlowStateUnreadableException>(() => cosmos.Store.LoadAsync("flow"));
        Assert.Equal("flow", unreadable.FlowId);
        Assert.Contains("no state JSON", unreadable.Reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// The write-path confirmation's conditional patch never matches against the service, but a
    /// patch that DID apply still proves the ledger exists: the current load goes on to read it.
    /// </summary>
    [Fact]
    public async Task Cosmos_AWritePathPatchThatApplied_StillReportsTheLedgerPresent()
    {
        using var cosmos = new CosmosGapHarness();
        cosmos.Container
            .Setup(c => c.PatchItemAsync<CosmosFlowStateDocument>(
                It.IsAny<string>(),
                It.IsAny<PartitionKey>(),
                It.IsAny<IReadOnlyList<PatchOperation>>(),
                It.IsAny<PatchItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Mock.Of<ItemResponse<CosmosFlowStateDocument>>());
        cosmos.Reads(CosmosDocument("flow"));

        var state = await cosmos.Store.LoadCurrentAsync("flow");

        Assert.Equal("flow", Assert.IsType<FlowState>(state).FlowId);
        cosmos.Container.Verify(
            c => c.PatchItemAsync<CosmosFlowStateDocument>(
                "flow",
                It.IsAny<PartitionKey>(),
                It.IsAny<IReadOnlyList<PatchOperation>>(),
                It.Is<PatchItemRequestOptions>(options => options.IfMatchEtag == CosmosFlowStateStore.NeverMatchingEtag),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    /// <summary>A host serializer that drops the document's wire names fails provisioning with the reason.</summary>
    [Fact]
    public async Task Cosmos_AHostSerializerThatIgnoresTheWireNames_FailsProvisioning()
    {
        using var cosmos = new CosmosGapHarness(serializer: new GapCosmosSerializer(seekable: true, honorNames: false));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => cosmos.Store.LoadAsync("flow"));

        Assert.Contains("(GapCosmosSerializer) does not honor", error.Message, StringComparison.Ordinal);
        cosmos.Container.Verify(
            c => c.ReadItemAsync<CosmosFlowStateDocument>(It.IsAny<string>(), It.IsAny<PartitionKey>(), It.IsAny<ItemRequestOptions>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    /// <summary>With no MaxStateBytes the document is not measured at all; the create goes straight out.</summary>
    [Fact]
    public async Task Cosmos_WithoutAStateBudget_CreatesWithoutMeasuring()
    {
        var serializer = new GapCosmosSerializer(seekable: false, honorNames: true);
        using var cosmos = new CosmosGapHarness(serializer: serializer, maxStateBytes: null);
        var serializedBeforeCreate = 0;
        cosmos.Container
            .Setup(c => c.CreateItemAsync(It.IsAny<CosmosFlowStateDocument>(), It.IsAny<PartitionKey?>(), It.IsAny<ItemRequestOptions>(), It.IsAny<CancellationToken>()))
            .Callback(() => serializedBeforeCreate = serializer.DocumentsSerialized)
            .ReturnsAsync(Mock.Of<ItemResponse<CosmosFlowStateDocument>>());

        Assert.True(await cosmos.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));

        // Only provisioning's probe went through the serializer — no measurement of the ledger.
        Assert.Equal(1, serializedBeforeCreate);
    }

    /// <summary>
    /// A host serializer whose stream cannot seek is measured by reading it through: the whole
    /// document counts, so a ledger that fits the budget but whose document does not is refused
    /// with the document's size, and nothing is written.
    /// </summary>
    [Fact]
    public async Task Cosmos_AHostSerializerWithANonSeekableStream_IsMeasuredByReadingItThrough()
    {
        var state = CreateState("flow");
        state.LastMessage = new string('"', 20_000);
        var ledgerBytes = System.Text.Encoding.UTF8.GetByteCount(
            (string)Shared(typeof(CosmosDurableFlowOptions)).GetMethod("Serialize", BindingFlags.Public | BindingFlags.Static)!.Invoke(null, [state])!);
        var budget = ledgerBytes + 100;

        var serializer = new GapCosmosSerializer(seekable: false, honorNames: true);
        using var cosmos = new CosmosGapHarness(serializer: serializer, maxStateBytes: budget);
        cosmos.CreatesSuccessfully();

        var tooLarge = await Assert.ThrowsAnyAsync<InvalidOperationException>(
            () => cosmos.Store.TryCreateAsync("flow", state, TimeSpan.FromMinutes(5)));

        Assert.Equal("FlowStateTooLargeException", tooLarge.GetType().Name);
        var measured = (long)tooLarge.GetType().GetProperty("SerializedSizeBytes")!.GetValue(tooLarge)!;
        Assert.Equal(serializer.LastLength, measured);
        Assert.True(measured > 16 * 1024, "the document spans several read buffers");
        Assert.True(measured > budget);
        cosmos.Container.Verify(
            c => c.CreateItemAsync(It.IsAny<CosmosFlowStateDocument>(), It.IsAny<PartitionKey?>(), It.IsAny<ItemRequestOptions>(), It.IsAny<CancellationToken>()),
            Times.Never);

        // The same serializer under the default budget measures and writes.
        using var roomy = new CosmosGapHarness(serializer: new GapCosmosSerializer(seekable: false, honorNames: true));
        roomy.CreatesSuccessfully();
        Assert.True(await roomy.Store.TryCreateAsync("flow", state, TimeSpan.FromMinutes(5)));
    }

    /// <summary>
    /// A create that conflicts with a document the TTL sweep purges before the follow-up read
    /// (404) takes the free slot on the next attempt.
    /// </summary>
    [Fact]
    public async Task Cosmos_ACreateConflictWhoseDocumentIsPurgedBeforeTheRead_CreatesOnTheNextAttempt()
    {
        using var cosmos = new CosmosGapHarness();
        cosmos.Container
            .SetupSequence(c => c.CreateItemAsync(It.IsAny<CosmosFlowStateDocument>(), It.IsAny<PartitionKey?>(), It.IsAny<ItemRequestOptions>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new CosmosException("conflict", HttpStatusCode.Conflict, 0, "activity", 0))
            .ReturnsAsync(Mock.Of<ItemResponse<CosmosFlowStateDocument>>());
        cosmos.Container
            .Setup(c => c.ReadItemAsync<CosmosFlowStateDocument>(It.IsAny<string>(), It.IsAny<PartitionKey>(), It.IsAny<ItemRequestOptions>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new CosmosException("gone", HttpStatusCode.NotFound, 0, "activity", 0));

        Assert.True(await cosmos.Store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        cosmos.Container.Verify(
            c => c.CreateItemAsync(It.IsAny<CosmosFlowStateDocument>(), It.IsAny<PartitionKey?>(), It.IsAny<ItemRequestOptions>(), It.IsAny<CancellationToken>()),
            Times.Exactly(2));
        cosmos.Container.Verify(
            c => c.ReplaceItemAsync(It.IsAny<CosmosFlowStateDocument>(), It.IsAny<string>(), It.IsAny<PartitionKey?>(), It.IsAny<ItemRequestOptions>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    // ---- MongoDB ----

    [Fact]
    public async Task Mongo_ALiveDocumentWithoutItsStateJson_IsUnreadable_NotAbsent()
    {
        var (database, collection) = MongoStore();
        collection.WithProvisionedTtlIndex();
        collection.FindsReturning(new MongoFlowStateDocument
        {
            FlowId = "flow",
            StateJson = "",
            Revision = 0,
            ExpiresAtUtc = DateTime.UtcNow.AddHours(1),
            UpdatedAtUtc = DateTime.UtcNow
        });
        using var store = new MongoDbFlowStateStore(database.Object, Options.Create(new MongoDbDurableFlowOptions
        {
            CollectionName = "flows",
            AutoCreateIndexes = false,
            UseOwnershipLedger = false
        }));

        var unreadable = await Assert.ThrowsAsync<FlowStateUnreadableException>(() => store.LoadAsync("flow"));
        Assert.Contains("no state JSON", unreadable.Reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// The startup index listing on a collection that does not exist yet (26) reads as empty —
    /// nothing to refuse, and the first write creates it with the simple collation — so the store
    /// builds its TTL index and serves. A listing the credentials may not run (13) is tolerated
    /// only while the store creates its own index; verifying an operator-provisioned one needs it.
    /// </summary>
    [Theory]
    [InlineData(26, true, true)]
    [InlineData(26, false, false)]
    [InlineData(13, true, true)]
    [InlineData(13, false, false)]
    public async Task Mongo_StartupIndexListing_RefusalsByCode(int code, bool autoCreateIndexes, bool serves)
    {
        var (database, collection) = MongoStore();
        var indexes = new Mock<IMongoIndexManager<MongoFlowStateDocument>>();
        indexes.Setup(m => m.ListAsync(It.IsAny<CancellationToken>())).ThrowsAsync(MongoCommand(code));
        indexes
            .Setup(m => m.CreateOneAsync(It.IsAny<CreateIndexModel<MongoFlowStateDocument>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("flows_expires_idx");
        collection.SetupGet(c => c.Indexes).Returns(indexes.Object);
        collection.FindsReturning(new MongoFlowStateDocument
        {
            FlowId = "flow",
            StateJson = JsonSerializer.Serialize(CreateState("flow")),
            Revision = 0,
            ExpiresAtUtc = DateTime.UtcNow.AddHours(1),
            UpdatedAtUtc = DateTime.UtcNow
        });
        using var store = new MongoDbFlowStateStore(database.Object, Options.Create(new MongoDbDurableFlowOptions
        {
            CollectionName = "flows",
            AutoCreateIndexes = autoCreateIndexes,
            UseOwnershipLedger = false
        }));

        if (serves)
        {
            Assert.Equal("flow", (await store.LoadAsync("flow"))!.FlowId);
            indexes.Verify(
                m => m.CreateOneAsync(It.IsAny<CreateIndexModel<MongoFlowStateDocument>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()),
                Times.Once);
            return;
        }

        // Without auto-creation an empty listing has no TTL reaper to verify, and an unlistable
        // collection cannot be verified at all: both fail the operation.
        var error = await Record.ExceptionAsync(() => store.LoadAsync("flow"));
        if (code == 13)
            Assert.Equal(13, Assert.IsType<MongoCommandException>(error).Code);
        else
            Assert.Contains("TTL index", Assert.IsType<InvalidOperationException>(error).Message, StringComparison.Ordinal);
        indexes.Verify(
            m => m.CreateOneAsync(It.IsAny<CreateIndexModel<MongoFlowStateDocument>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    // ---- PostgreSQL ----

    [Fact]
    public async Task PostgreSql_TheDdlClockSeam_RoundTrips_AndAnOwnedDataSourceIsDisposedAsynchronously()
    {
        var owned = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=unused;Password=unused;Database=none;Timeout=1;Pooling=false");
        var store = new PostgreSqlFlowStateStore(owned, Options.Create(new PostgreSqlDurableFlowOptions()), ownsDataSource: true);
        var clock = new VirtualTimeProvider();
        store.Clock = clock;
        Assert.Same(clock, store.Clock);

        await store.DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await owned.OpenConnectionAsync());

        // A data source the host owns outlives the store.
        await using var hosted = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=unused;Password=unused;Database=none;Timeout=1;Pooling=false");
        await new PostgreSqlFlowStateStore(hosted, Options.Create(new PostgreSqlDurableFlowOptions())).DisposeAsync();
        var stillUsable = await Record.ExceptionAsync(async () => await hosted.OpenConnectionAsync());
        Assert.IsNotType<ObjectDisposedException>(stillUsable);
    }

    // ---- SQLite ----

    /// <summary>
    /// The expiry column must have TEXT affinity (ISO-8601 strings compare lexicographically).
    /// REAL-family and BLOB (including an untyped column) declarations are named as what they
    /// resolve to.
    /// </summary>
    [Theory]
    [InlineData("REAL", "REAL")]
    [InlineData("FLOAT", "REAL")]
    [InlineData("DOUBLE PRECISION", "REAL")]
    [InlineData("BLOB", "BLOB")]
    [InlineData("", "BLOB")]
    public async Task Sqlite_AnExpiryColumnWithoutTextAffinity_IsRefusedByTheAffinityItResolvesTo(string declaredType, string affinity)
    {
        var path = Path.Combine(Path.GetTempPath(), $"ar-gap-flow-state-{Guid.NewGuid():N}.db");
        var connectionString = $"Data Source={path}";
        try
        {
            await using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    CREATE TABLE asyncresponse_flow_state (
                        flow_id TEXT NOT NULL PRIMARY KEY,
                        state_json TEXT NOT NULL,
                        expires_at_utc {declaredType} NOT NULL,
                        updated_at_utc TEXT NOT NULL,
                        revision INTEGER NOT NULL DEFAULT 0,
                        lease_id TEXT NULL,
                        lease_expires_at_utc TEXT NULL
                    );
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
            {
                ConnectionString = connectionString,
                AutoCreateSchema = false
            }));

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => store.LoadAsync("flow"));
            Assert.Contains("expires_at_utc", error.Message, StringComparison.Ordinal);
            Assert.Contains($"resolves to {affinity} affinity where TEXT is required", error.Message, StringComparison.Ordinal);
        }
        finally
        {
            SqliteConnection.ClearPool(new SqliteConnection(connectionString));
            foreach (var file in new[] { path, path + "-wal", path + "-shm" })
            {
                try
                {
                    File.Delete(file);
                }
                catch (IOException)
                {
                }
            }
        }
    }

    // ---- EF Core ----

    /// <summary>
    /// The create's LAST attempt: its insert collides, and by the time it asks, a peer holds the
    /// id with a live ledger — the answer is "exists" (false), not the raw duplicate-key error.
    /// The first attempt's collision was with an expired row (retried); the peer extended it
    /// before the second attempt's check.
    /// </summary>
    [Fact]
    public async Task EFCore_ALastAttemptCollisionWithAPeersLiveLedger_ReportsExists()
    {
        await using var database = new GapEfDatabase();
        await database.EnsureSchemaAsync();
        var races = new GapRaceInterceptor();
        var services = new ServiceCollection();
        services.AddDbContextFactory<TestFlowDbContext>(options => options.UseSqlite(database.ConnectionString).AddInterceptors(races));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var store = new EFCoreFlowStateStore<TestFlowDbContext>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new EFCoreDurableFlowOptions { PruneInterval = TimeSpan.FromHours(1) }));

        var peer = CreateState("contested");
        peer.LastMessage = "peer run";
        Assert.True(await store.TryCreateAsync("contested", peer, TimeSpan.FromMinutes(5)));
        await database.SetExpiryAsync("contested", new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        races.MissReplaces = 2;
        races.BeforeExistenceCheck = check => check == 2
            ? database.SetExpiryAsync("contested", DateTime.UtcNow.AddHours(1))
            : Task.CompletedTask;

        var mine = CreateState("contested");
        mine.LastMessage = "my run";
        Assert.False(await store.TryCreateAsync("contested", mine, TimeSpan.FromMinutes(5)));

        Assert.Equal(2, races.ExistenceChecks);
        Assert.Equal("peer run", (await store.LoadAsync("contested"))!.LastMessage);
    }

    /// <summary>The lease columns round-trip through the mapped entity: set on acquire, cleared on release.</summary>
    [Fact]
    public async Task EFCore_TheLeaseOwnerIsPersistedOnTheMappedRecord()
    {
        await using var database = new GapEfDatabase();
        await database.EnsureSchemaAsync();
        var services = new ServiceCollection();
        services.AddDbContextFactory<TestFlowDbContext>(options => options.UseSqlite(database.ConnectionString));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var store = new EFCoreFlowStateStore<TestFlowDbContext>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new EFCoreDurableFlowOptions()));

        Assert.True(await store.TryCreateAsync("leased", CreateState("leased"), TimeSpan.FromMinutes(5)));
        Assert.True(await store.TryAcquireLeaseAsync("leased", "worker-1", TimeSpan.FromMinutes(1)));

        var held = await database.ReadAsync("leased");
        Assert.Equal("worker-1", held.LeaseId);
        Assert.NotNull(held.LeaseExpiresAtUtc);

        await store.ReleaseLeaseAsync("leased", "worker-1");
        var released = await database.ReadAsync("leased");
        Assert.Null(released.LeaseId);
        Assert.Null(released.LeaseExpiresAtUtc);
    }

    // ---- helpers ----

    private static FlowState CreateState(string flowId) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(TestOnboardingFlow).FullName,
        InputTypeName = typeof(TestFlowInput).FullName,
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    private static Type Shared(Type providerOptionsType)
        => providerOptionsType.Assembly.GetType(SharedTypeName, throwOnError: true)!;

    private static object? Invoke(Type shared, string methodName, params object?[] arguments)
        => shared.GetMethod(methodName, BindingFlags.Public | BindingFlags.Static)!.Invoke(null, arguments);

    private static TException AssertInner<TException>(Type shared, string methodName, params object?[] arguments)
        where TException : Exception
    {
        var exception = Assert.Throws<TargetInvocationException>(() => Invoke(shared, methodName, arguments));
        return Assert.IsType<TException>(exception.InnerException);
    }

    private static CosmosFlowStateDocument CosmosDocument(string flowId) => new()
    {
        Id = flowId,
        FlowId = flowId,
        StateJson = JsonSerializer.Serialize(CreateState(flowId)),
        ExpiresAtUtc = DateTime.UtcNow.AddMinutes(5),
        UpdatedAtUtc = DateTime.UtcNow,
        Revision = 0
    };

    private static (Mock<IMongoDatabase> Database, Mock<IMongoCollection<MongoFlowStateDocument>> Collection) MongoStore()
    {
        var collection = new Mock<IMongoCollection<MongoFlowStateDocument>>().SelfPinning();
        var database = new Mock<IMongoDatabase>().WithTestNamespace();
        database
            .Setup(d => d.GetCollection<MongoFlowStateDocument>("flows", It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        return (database, collection);
    }

    private static MongoCommandException MongoCommand(int code)
        => new(
            MongoReplicationTimeouts.Connection,
            "listIndexes failed",
            new BsonDocument("listIndexes", "flows"),
            new BsonDocument { ["ok"] = 0, ["code"] = code, ["errmsg"] = $"code {code}" });

    /// <summary>A provider connection that opens unless its connection string starts with "fail".</summary>
    private sealed class GapConnection : DbConnection
    {
        public static readonly ConcurrentDictionary<string, GapConnection> Created = new(StringComparer.Ordinal);

        private string _connectionString = "";
        private ConnectionState _state = ConnectionState.Closed;

        public bool WasDisposed { get; private set; }

        [AllowNull]
        public override string ConnectionString
        {
            get => _connectionString;
            set
            {
                _connectionString = value ?? "";
                Created[_connectionString] = this;
            }
        }

        public override string Database => "gap";
        public override string DataSource => "gap";
        public override string ServerVersion => "1";
        public override ConnectionState State => _state;

        public override void Open()
        {
            if (_connectionString.StartsWith("fail", StringComparison.Ordinal))
                throw new InvalidOperationException("refused");
            _state = ConnectionState.Open;
        }

        public override void Close() => _state = ConnectionState.Closed;
        public override void ChangeDatabase(string databaseName) => throw new NotSupportedException();
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            WasDisposed = true;
            _state = ConnectionState.Closed;
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// A System.Text.Json host serializer (which honors the document's [JsonPropertyName]s), or one
    /// that writes the document without its wire names; seekable or not.
    /// </summary>
    private sealed class GapCosmosSerializer(bool seekable, bool honorNames) : CosmosSerializer
    {
        public int DocumentsSerialized;
        public long LastLength;

        public override T FromStream<T>(Stream stream)
        {
            using (stream)
                return JsonSerializer.Deserialize<T>(stream)!;
        }

        public override Stream ToStream<T>(T input)
        {
            var bytes = honorNames ? JsonSerializer.SerializeToUtf8Bytes(input) : "{\"Identifier\":\"x\"}"u8.ToArray();
            Interlocked.Increment(ref DocumentsSerialized);
            LastLength = bytes.Length;
            return seekable ? new MemoryStream(bytes) : new ForwardOnlyStream(bytes);
        }
    }

    private sealed class ForwardOnlyStream(byte[] bytes) : Stream
    {
        private readonly MemoryStream _inner = new(bytes);

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _inner.Dispose();
            base.Dispose(disposing);
        }
    }

    /// <summary>A Session, single-write-region account with an existing TTL-enabled container.</summary>
    private sealed class CosmosGapHarness : IDisposable
    {
        public CosmosGapHarness(CosmosSerializer? serializer = null, long? maxStateBytes = 1_900_000)
        {
            Client.Setup(c => c.ReadAccountAsync()).ReturnsAsync(SessionAccount());
            if (serializer is not null)
                Client.SetupGet(c => c.ClientOptions).Returns(new CosmosClientOptions { Serializer = serializer });
            Client.Setup(c => c.GetContainer("flows", "states")).Returns(Container.Object);
            var response = new Mock<ContainerResponse>();
            response.SetupGet(r => r.Resource).Returns(new ContainerProperties("states", "/flowId") { DefaultTimeToLive = -1 });
            Container
                .Setup(c => c.ReadContainerAsync(It.IsAny<ContainerRequestOptions>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(response.Object);
            Store = new CosmosFlowStateStore(Client.Object, Options.Create(new CosmosDurableFlowOptions
            {
                DatabaseName = "flows",
                ContainerName = "states",
                AutoCreateContainer = false,
                MaxStateBytes = maxStateBytes
            }));
        }

        public Mock<CosmosClient> Client { get; } = new();
        public Mock<Container> Container { get; } = new();
        public CosmosFlowStateStore Store { get; }

        public void Reads(CosmosFlowStateDocument document)
        {
            var response = new Mock<ItemResponse<CosmosFlowStateDocument>>();
            response.SetupGet(r => r.Resource).Returns(document);
            response.SetupGet(r => r.ETag).Returns("etag");
            Container
                .Setup(c => c.ReadItemAsync<CosmosFlowStateDocument>(It.IsAny<string>(), It.IsAny<PartitionKey>(), It.IsAny<ItemRequestOptions>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(response.Object);
        }

        public void CreatesSuccessfully()
            => Container
                .Setup(c => c.CreateItemAsync(It.IsAny<CosmosFlowStateDocument>(), It.IsAny<PartitionKey?>(), It.IsAny<ItemRequestOptions>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(Mock.Of<ItemResponse<CosmosFlowStateDocument>>());

        public void Dispose() => Store.Dispose();

        // AccountProperties has no public constructor: built from the account JSON the SDK parses.
        private static AccountProperties SessionAccount()
            => Newtonsoft.Json.JsonConvert.DeserializeObject<AccountProperties>(
                """
                {"id":"account","userConsistencyPolicy":{"defaultConsistencyLevel":"Session"},
                 "writableLocations":[{"name":"West Europe","databaseAccountEndpoint":"https://account-westeurope.documents.azure.com:443/"}],
                 "readableLocations":[{"name":"West Europe","databaseAccountEndpoint":"https://account-westeurope.documents.azure.com:443/"}]}
                """)!;
    }

    /// <summary>
    /// Makes the EF Core create's in-place replace miss (as a stale clock read makes it miss) and
    /// runs a peer's write just before the n-th duplicate-key existence check.
    /// </summary>
    private sealed class GapRaceInterceptor : DbCommandInterceptor
    {
        private int _missReplaces;
        private int _existenceChecks;

        public int MissReplaces { set => Volatile.Write(ref _missReplaces, value); }

        public int ExistenceChecks => Volatile.Read(ref _existenceChecks);

        public Func<int, Task>? BeforeExistenceCheck { get; set; }

        public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(
            DbCommand command,
            CommandEventData eventData,
            InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
        {
            if (command.CommandText.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase)
                && command.CommandText.Contains("state_json", StringComparison.Ordinal)
                && Interlocked.Decrement(ref _missReplaces) >= 0)
            {
                return ValueTask.FromResult(InterceptionResult<int>.SuppressWithResult(0));
            }

            return base.NonQueryExecutingAsync(command, eventData, result, cancellationToken);
        }

        public override async ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
            DbCommand command,
            CommandEventData eventData,
            InterceptionResult<DbDataReader> result,
            CancellationToken cancellationToken = default)
        {
            if (BeforeExistenceCheck is { } peer && command.CommandText.Contains("EXISTS", StringComparison.OrdinalIgnoreCase))
                await peer(Interlocked.Increment(ref _existenceChecks));

            return result;
        }
    }

    private sealed class GapEfDatabase : IAsyncDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"ar-gap-efcore-flow-state-{Guid.NewGuid():N}.db");

        public string ConnectionString => $"Data Source={_path}";

        public async Task EnsureSchemaAsync()
        {
            await using var context = CreateContext();
            await context.Database.EnsureCreatedAsync();
            await context.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");
        }

        public async Task SetExpiryAsync(string flowId, DateTime expiresAtUtc)
        {
            await using var context = CreateContext();
            Assert.Equal(1, await context.Set<DurableFlowStateRecord>()
                .Where(r => r.FlowId == flowId)
                .ExecuteUpdateAsync(setters => setters.SetProperty(r => r.ExpiresAtUtc, expiresAtUtc)));
        }

        public async Task<DurableFlowStateRecord> ReadAsync(string flowId)
        {
            await using var context = CreateContext();
            return await context.Set<DurableFlowStateRecord>().AsNoTracking().SingleAsync(r => r.FlowId == flowId);
        }

        private TestFlowDbContext CreateContext()
            => new(new DbContextOptionsBuilder<TestFlowDbContext>().UseSqlite(ConnectionString).Options);

        public ValueTask DisposeAsync()
        {
            SqliteConnection.ClearPool(new SqliteConnection(ConnectionString));
            foreach (var file in new[] { _path, _path + "-wal", _path + "-shm" })
            {
                try
                {
                    File.Delete(file);
                }
                catch (IOException)
                {
                }
            }

            return ValueTask.CompletedTask;
        }
    }
}
