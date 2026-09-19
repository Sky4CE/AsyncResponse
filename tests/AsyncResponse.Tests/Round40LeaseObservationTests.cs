using System.Globalization;
using System.Reflection;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using AsyncResponse.DurableFlows.Cosmos;
using AsyncResponse.DurableFlows.DynamoDB;
using AsyncResponse.DurableFlows.EFCore;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.DurableFlows.MySql;
using AsyncResponse.DurableFlows.Oracle;
using AsyncResponse.DurableFlows.PostgreSQL;
using AsyncResponse.DurableFlows.Sqlite;
using AsyncResponse.DurableFlows.SqlServer;
using Microsoft.Azure.Cosmos;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Moq;
using Npgsql;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 40: <see cref="IFlowStateStore.ObserveLeaseAsync"/> in the nine provider stores.
/// <para>
/// A wake-up that finds the execution lease held tells a live holder from a dead one by comparing
/// two observations of the PERSISTED lease: a different owner, or a later expiry under the same
/// owner, proves a worker acquired or renewed in between; a lease that never changes is a dead
/// holder's and is waited out to its persisted expiry. That only works when every store reports
/// the raw pair — never judged against a clock, <see cref="FlowLeaseObservation.Unheld"/> (not
/// <c>null</c>, which means "unsupported") when nobody holds it, and a UTC expiry at the store's
/// full precision so two renewals compare strictly increasing.
/// </para>
/// <para>
/// SQLite and EF Core (SQLite provider) run for real and pin the whole behaviour; DynamoDB and
/// MongoDB are pinned through their client mocks (request shape and every response shape); the
/// Cosmos cases live next to that store's private harness in
/// <see cref="CosmosDurableFlowStateStoreTests"/>. PostgreSQL, SQL Server, MySQL and Oracle have no
/// client seam — their behaviour is pinned by the shared real-container contract
/// (<c>FlowStoreContract.AssertLeaseObservationContractAsync</c>); here they get the argument
/// guard and the shared shaper every store funnels its answer through.
/// </para>
/// </summary>
public sealed class Round40LeaseObservationTests
{
    private const string SharedTypeName = "AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared";

    // ---- The shared shaper, in every store assembly (the source is compiled into each). ----

    [Theory]
    [MemberData(nameof(DurableFlowStoreSharedTests.ProviderOptionTypes), MemberType = typeof(DurableFlowStoreSharedTests))]
    public void SharedShaper_ReportsUnheldForNoOwner_AndAlwaysAUtcExpiry(Type providerOptionsType)
    {
        var shared = providerOptionsType.Assembly.GetType(SharedTypeName, throwOnError: true)!;
        var shape = shared.GetMethod("LeaseObservation", BindingFlags.Public | BindingFlags.Static)!;
        FlowLeaseObservation Shape(string? leaseId, DateTime? expiry)
            => Assert.IsType<FlowLeaseObservation>(shape.Invoke(null, [leaseId, expiry]));

        var instant = new DateTime(2031, 3, 14, 9, 26, 53, DateTimeKind.Utc).AddTicks(1_234_567);

        // No owner is "nobody holds it" whatever the expiry column still says — and it is the
        // Unheld singleton, never null: null tells the executor the store cannot report leases.
        Assert.Same(FlowLeaseObservation.Unheld, Shape(null, null));
        Assert.Same(FlowLeaseObservation.Unheld, Shape(null, instant));

        // A holder without a readable expiry is still a holder.
        var noExpiry = Shape("owner", null);
        Assert.Equal("owner", noExpiry.LeaseId);
        Assert.Null(noExpiry.ExpiresAtUtc);

        // UTC passes through tick for tick.
        var utc = Shape("owner", instant);
        Assert.Equal(instant, utc.ExpiresAtUtc);
        Assert.Equal(DateTimeKind.Utc, utc.ExpiresAtUtc!.Value.Kind);

        // Zone-less SQL columns read back Unspecified; the stored digits ARE UTC, so they are
        // stamped, not shifted.
        var unspecified = Shape("owner", DateTime.SpecifyKind(instant, DateTimeKind.Unspecified));
        Assert.Equal(instant.Ticks, unspecified.ExpiresAtUtc!.Value.Ticks);
        Assert.Equal(DateTimeKind.Utc, unspecified.ExpiresAtUtc.Value.Kind);

        // A driver that really did convert to local time (legacy-timestamp Npgsql, a custom Cosmos
        // serializer) is converted back to the same instant.
        var local = Shape("owner", instant.ToLocalTime());
        Assert.Equal(instant.Ticks, local.ExpiresAtUtc!.Value.Ticks);
        Assert.Equal(DateTimeKind.Utc, local.ExpiresAtUtc.Value.Kind);
    }

    // ---- Argument guard: all nine, before any backend is touched. ----

    [Fact]
    public async Task EveryStore_RejectsABlankFlowId_BeforeTouchingItsBackend()
    {
        // Strict mocks and unreachable endpoints: anything but the argument check throws something
        // other than ArgumentException (or hangs on a connect), so passing proves the guard ran first.
        var dynamo = new Mock<IAmazonDynamoDB>(MockBehavior.Strict);
        var cosmos = new Mock<CosmosClient>(MockBehavior.Strict);
        using var mongo = new MongoLeaseHarness();
        await using var npgsql = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=unused;Password=unused;Timeout=1");
        await using var noContext = new ServiceCollection().BuildServiceProvider();

        IFlowStateStore[] stores =
        [
            new PostgreSqlFlowStateStore(npgsql, Options.Create(new PostgreSqlDurableFlowOptions { AutoCreateSchema = false })),
            new SqlServerFlowStateStore(Options.Create(new SqlServerDurableFlowOptions
            {
                ConnectionString = "Server=unused;Database=flows;Integrated Security=true",
                AutoCreateSchema = false
            })),
            new MySqlFlowStateStore(Options.Create(new MySqlDurableFlowOptions
            {
                ConnectionString = "Server=unused;Database=flows;User ID=root",
                AutoCreateSchema = false
            })),
            new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
            {
                ConnectionString = "Data Source=:memory:",
                AutoCreateSchema = false
            })),
            new OracleFlowStateStore(Options.Create(new OracleDurableFlowOptions
            {
                ConnectionString = "User Id=flows;Password=unused;Data Source=unused",
                AutoCreateSchema = false
            })),
            mongo.Store,
            new DynamoDbFlowStateStore(dynamo.Object, Options.Create(new DynamoDbDurableFlowOptions { TableName = "flows" })),
            new CosmosFlowStateStore(cosmos.Object, Options.Create(new CosmosDurableFlowOptions { DatabaseName = "flows" })),
            new EFCoreFlowStateStore<TestFlowDbContext>(
                noContext.GetRequiredService<IServiceScopeFactory>(),
                Options.Create(new EFCoreDurableFlowOptions()))
        ];

        foreach (var store in stores)
        {
            await Assert.ThrowsAsync<ArgumentException>(() => store.ObserveLeaseAsync(" "));
            await Assert.ThrowsAsync<ArgumentException>(() => store.ObserveLeaseAsync(""));
            await Assert.ThrowsAsync<ArgumentNullException>(() => store.ObserveLeaseAsync(null!));
        }

        mongo.Collection.Verify(
            item => item.FindAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    // ---- SQLite: the real store, the whole behaviour. ----

    [Fact]
    public async Task Sqlite_ObservesTheRawPersistedLease_ThroughItsWholeLifecycle()
    {
        await using var database = new TempSqliteFile("ar-r40-sqlite");
        var store = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString
        }));

        await AssertLeaseLifecycleAsync(store);
    }

    [Fact]
    public async Task Sqlite_ReportsTheStoredExpiryTickForTick_AsUtc()
    {
        // The observation must be what UpdateLeaseAsync persisted, not a rounded or re-zoned copy:
        // Microsoft.Data.Sqlite stores the bound UTC DateTime as zone-less text with all seven
        // fractional digits, and a reader that parsed it as local time would shift every expiry by
        // the host's UTC offset — invisible on a UTC CI runner.
        await using var database = new TempSqliteFile("ar-r40-sqlite-raw");
        var store = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString
        }));
        Assert.True(await store.TryCreateAsync("flow", NewState("flow"), TimeSpan.FromMinutes(5)));
        Assert.True(await store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));

        var observed = await store.ObserveLeaseAsync("flow");

        var storedText = await database.ScalarAsync(
            """SELECT lease_expires_at_utc FROM "asyncresponse_flow_state" WHERE flow_id = 'flow';""");
        var stored = DateTime.Parse(
            Assert.IsType<string>(storedText),
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        Assert.Equal(stored.Ticks, observed!.ExpiresAtUtc!.Value.Ticks);
        Assert.Equal(DateTimeKind.Utc, observed.ExpiresAtUtc.Value.Kind);
    }

    [Fact]
    public async Task Sqlite_HonoursCancellation()
    {
        await using var database = new TempSqliteFile("ar-r40-sqlite-cancel");
        var store = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString
        }));
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        // Both before the schema exists (the ensure gate) and after (the read itself).
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.ObserveLeaseAsync("flow", cancelled.Token));
        Assert.Same(FlowLeaseObservation.Unheld, await store.ObserveLeaseAsync("flow"));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.ObserveLeaseAsync("flow", cancelled.Token));
    }

    // ---- EF Core over the SQLite provider: the real store, both context-lease shapes. ----

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EFCore_ObservesTheRawPersistedLease_ThroughItsWholeLifecycle(bool useContextFactory)
    {
        await using var database = new TempSqliteFile("ar-r40-efcore");
        await using (var context = NewContext(database.ConnectionString))
            await context.Database.EnsureCreatedAsync();

        var services = new ServiceCollection();
        if (useContextFactory)
            services.AddDbContextFactory<TestFlowDbContext>(options => options.UseSqlite(database.ConnectionString));
        else
            services.AddDbContext<TestFlowDbContext>(options => options.UseSqlite(database.ConnectionString));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var store = new EFCoreFlowStateStore<TestFlowDbContext>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new EFCoreDurableFlowOptions()));

        await AssertLeaseLifecycleAsync(store);

        // The projection selects the two lease columns only and tracks nothing, so the stored
        // value comes back tick for tick.
        Assert.True(await store.TryCreateAsync("flow-raw", NewState("flow-raw"), TimeSpan.FromMinutes(5)));
        Assert.True(await store.TryAcquireLeaseAsync("flow-raw", "owner", TimeSpan.FromMinutes(1)));
        var observed = await store.ObserveLeaseAsync("flow-raw");
        await using var verify = NewContext(database.ConnectionString);
        var stored = await verify.Set<DurableFlowStateRecord>().AsNoTracking().SingleAsync(r => r.FlowId == "flow-raw");
        Assert.Equal(stored.LeaseExpiresAtUtc!.Value.Ticks, observed!.ExpiresAtUtc!.Value.Ticks);

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.ObserveLeaseAsync("flow-raw", cancelled.Token));
    }

    /// <summary>
    /// The behaviour the executor's liveness proof rests on, asserted against a real store. Kept
    /// in step with <c>FlowStoreContract.AssertLeaseObservationContractAsync</c>, which runs the
    /// same sequence against every store's real server.
    /// </summary>
    private static async Task AssertLeaseLifecycleAsync(IFlowStateStore store)
    {
        // A flow that was never created: Unheld, never null (null = "this store cannot report").
        Assert.Same(FlowLeaseObservation.Unheld, await store.ObserveLeaseAsync("flow-missing"));

        const string flowId = "flow-lease";
        Assert.True(await store.TryCreateAsync(flowId, NewState(flowId), TimeSpan.FromMinutes(5)));
        Assert.Same(FlowLeaseObservation.Unheld, await store.ObserveLeaseAsync(flowId));

        // Acquire: the owner, and an expiry one lease duration ahead, in UTC.
        var before = DateTime.UtcNow;
        Assert.True(await store.TryAcquireLeaseAsync(flowId, "owner-a", TimeSpan.FromSeconds(30)));
        var after = DateTime.UtcNow;
        var acquired = await store.ObserveLeaseAsync(flowId);
        Assert.NotNull(acquired);
        Assert.Equal("owner-a", acquired!.LeaseId);
        Assert.NotNull(acquired.ExpiresAtUtc);
        Assert.Equal(DateTimeKind.Utc, acquired.ExpiresAtUtc!.Value.Kind);
        Assert.InRange(acquired.ExpiresAtUtc.Value, before.AddSeconds(30), after.AddSeconds(30));

        // Nothing changed, so nothing may read as changed: two observations of an untouched lease
        // are identical — the "dead holder" signal.
        var again = await store.ObserveLeaseAsync(flowId);
        Assert.Equal(acquired.LeaseId, again!.LeaseId);
        Assert.Equal(acquired.ExpiresAtUtc, again.ExpiresAtUtc);

        // A renewal with the SAME duration a moment later still moves the expiry strictly forward:
        // the store keeps enough precision for a heartbeat to be visible as one.
        await Task.Delay(20);
        Assert.True(await store.TryRenewLeaseAsync(flowId, "owner-a", TimeSpan.FromSeconds(30)));
        var heartbeat = await store.ObserveLeaseAsync(flowId);
        Assert.Equal("owner-a", heartbeat!.LeaseId);
        Assert.True(
            heartbeat.ExpiresAtUtc > acquired.ExpiresAtUtc,
            $"a renewal must move the persisted expiry strictly forward ({acquired.ExpiresAtUtc:O} -> {heartbeat.ExpiresAtUtc:O})");

        // A renewal with a longer duration: same owner, strictly later again.
        Assert.True(await store.TryRenewLeaseAsync(flowId, "owner-a", TimeSpan.FromMinutes(5)));
        var renewed = await store.ObserveLeaseAsync(flowId);
        Assert.Equal("owner-a", renewed!.LeaseId);
        Assert.True(renewed.ExpiresAtUtc > heartbeat.ExpiresAtUtc);
        Assert.Equal(DateTimeKind.Utc, renewed.ExpiresAtUtc!.Value.Kind);

        // A failed acquire by someone else writes nothing.
        Assert.False(await store.TryAcquireLeaseAsync(flowId, "owner-b", TimeSpan.FromMinutes(1)));
        Assert.Equal(renewed.ExpiresAtUtc, (await store.ObserveLeaseAsync(flowId))!.ExpiresAtUtc);

        // Release: Unheld again.
        await store.ReleaseLeaseAsync(flowId, "owner-a");
        Assert.Same(FlowLeaseObservation.Unheld, await store.ObserveLeaseAsync(flowId));

        // An EXPIRED lease is still reported, raw: the row keeps the dead holder's pair until
        // someone acquires or releases it, and the observation must not judge it against a clock.
        Assert.True(await store.TryAcquireLeaseAsync(flowId, "dead-worker", TimeSpan.FromMilliseconds(1)));
        await Task.Delay(50);
        var expired = await store.ObserveLeaseAsync(flowId);
        Assert.Equal("dead-worker", expired!.LeaseId);
        Assert.True(expired.ExpiresAtUtc < DateTime.UtcNow, "the lease under observation has lapsed");
        // ...and the store agrees it has lapsed: its own holder can no longer renew it.
        Assert.False(await store.TryRenewLeaseAsync(flowId, "dead-worker", TimeSpan.FromMinutes(1)));
        var stillExpired = await store.ObserveLeaseAsync(flowId);
        Assert.Equal("dead-worker", stillExpired!.LeaseId);
        Assert.Equal(expired.ExpiresAtUtc, stillExpired.ExpiresAtUtc);

        // A takeover is visible as a different owner — the "live holder" signal.
        Assert.True(await store.TryAcquireLeaseAsync(flowId, "live-worker", TimeSpan.FromMinutes(1)));
        var takenOver = await store.ObserveLeaseAsync(flowId);
        Assert.Equal("live-worker", takenOver!.LeaseId);
        Assert.True(takenOver.ExpiresAtUtc > expired.ExpiresAtUtc);

        // Deleting the ledger leaves nothing to hold.
        Assert.True(await store.TryDeleteAsync(flowId));
        Assert.Same(FlowLeaseObservation.Unheld, await store.ObserveLeaseAsync(flowId));
    }

    // ---- DynamoDB: request shape and every response shape, through the client mock. ----

    [Fact]
    public async Task DynamoDb_ReadsOnlyTheLeaseAttributes_Consistently()
    {
        var client = ReadyDynamoClient();
        GetItemRequest? sent = null;
        client
            .Setup(database => database.GetItemAsync(It.IsAny<GetItemRequest>(), It.IsAny<CancellationToken>()))
            .Callback((GetItemRequest request, CancellationToken _) => sent = request)
            .ReturnsAsync(new GetItemResponse { Item = [] });
        using var store = CreateDynamoStore(client);

        Assert.Same(FlowLeaseObservation.Unheld, await store.ObserveLeaseAsync("flow-1"));

        Assert.NotNull(sent);
        Assert.Equal("flows", sent!.TableName);
        Assert.Equal("flow-1", Assert.Single(sent.Key).Value.S);
        Assert.Equal("flow_id", Assert.Single(sent.Key).Key);
        // Consistent, for the reason LoadAsync is: a stale replica would replay a lease a renewal
        // has already moved, and "unchanged" is the one answer that must never be stale.
        Assert.True(sent.ConsistentRead);
        // Projected: state_json (up to the 400 KB item cap) never crosses the wire.
        Assert.Equal("#lease_id, #lease_expires", sent.ProjectionExpression);
        Assert.Equal("lease_id", sent.ExpressionAttributeNames["#lease_id"]);
        Assert.Equal("lease_expires_at_ms", sent.ExpressionAttributeNames["#lease_expires"]);
        Assert.Equal(2, sent.ExpressionAttributeNames.Count);
    }

    [Fact]
    public async Task DynamoDb_MapsEveryItemShape_WithoutJudgingExpiry()
    {
        var client = ReadyDynamoClient();
        var liveMs = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeMilliseconds();
        var lapsedMs = DateTimeOffset.UtcNow.AddHours(-3).ToUnixTimeMilliseconds();
        client
            .SetupSequence(database => database.GetItemAsync(It.IsAny<GetItemRequest>(), It.IsAny<CancellationToken>()))
            // SDK v4 leaves collections null when the service sends none: no such item.
            .ReturnsAsync(new GetItemResponse { Item = null })
            // The item exists but carries neither projected attribute: never leased, or released.
            .ReturnsAsync(new GetItemResponse { Item = [] })
            .ReturnsAsync(DynamoItem(("lease_expires_at_ms", Number(liveMs))))
            .ReturnsAsync(DynamoItem(("lease_id", new AttributeValue { S = "" }), ("lease_expires_at_ms", Number(liveMs))))
            .ReturnsAsync(DynamoItem(("lease_id", new AttributeValue { S = "owner-a" }), ("lease_expires_at_ms", Number(liveMs))))
            .ReturnsAsync(DynamoItem(("lease_id", new AttributeValue { S = "dead-worker" }), ("lease_expires_at_ms", Number(lapsedMs))))
            .ReturnsAsync(DynamoItem(("lease_id", new AttributeValue { S = "owner-a" })))
            .ReturnsAsync(DynamoItem(("lease_id", new AttributeValue { S = "owner-a" }), ("lease_expires_at_ms", new AttributeValue { N = "soon" })))
            .ReturnsAsync(DynamoItem(("lease_id", new AttributeValue { S = "owner-a" }), ("lease_expires_at_ms", new AttributeValue { S = "text" })))
            .ReturnsAsync(DynamoItem(("lease_id", new AttributeValue { S = "owner-a" }), ("lease_expires_at_ms", Number(long.MaxValue))))
            .ReturnsAsync(DynamoItem(("lease_id", new AttributeValue { S = "owner-a" }), ("lease_expires_at_ms", Number(long.MinValue))));
        using var store = CreateDynamoStore(client);

        Assert.Same(FlowLeaseObservation.Unheld, await store.ObserveLeaseAsync("flow"));   // no item
        Assert.Same(FlowLeaseObservation.Unheld, await store.ObserveLeaseAsync("flow"));   // no lease attributes
        Assert.Same(FlowLeaseObservation.Unheld, await store.ObserveLeaseAsync("flow"));   // expiry without an owner
        Assert.Same(FlowLeaseObservation.Unheld, await store.ObserveLeaseAsync("flow"));   // empty owner

        var held = await store.ObserveLeaseAsync("flow");
        Assert.Equal("owner-a", held!.LeaseId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(liveMs).UtcDateTime, held.ExpiresAtUtc);
        Assert.Equal(DateTimeKind.Utc, held.ExpiresAtUtc!.Value.Kind);

        // Three hours lapsed and still reported as stored: the app clock is never consulted.
        var lapsed = await store.ObserveLeaseAsync("flow");
        Assert.Equal("dead-worker", lapsed!.LeaseId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(lapsedMs).UtcDateTime, lapsed.ExpiresAtUtc);

        // A holder whose expiry is missing, not a number, or outside DateTime's range is still a
        // holder — reported with no expiry rather than as Unheld or as an exception.
        for (var i = 0; i < 5; i++)
        {
            var unreadableExpiry = await store.ObserveLeaseAsync("flow");
            Assert.Equal("owner-a", unreadableExpiry!.LeaseId);
            Assert.Null(unreadableExpiry.ExpiresAtUtc);
        }

        // Observation never writes.
        client.Verify(database => database.UpdateItemAsync(It.IsAny<UpdateItemRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        client.Verify(database => database.PutItemAsync(It.IsAny<PutItemRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task DynamoDb_RoundTripsWhatTheLeaseWriteStored()
    {
        // Invert exactly what UpdateLeaseAsync writes: feed the observation the attribute values
        // the acquire sent, and the expiry comes back as that instant (millisecond precision).
        var client = ReadyDynamoClient();
        UpdateItemRequest? written = null;
        client
            .Setup(database => database.UpdateItemAsync(It.IsAny<UpdateItemRequest>(), It.IsAny<CancellationToken>()))
            .Callback((UpdateItemRequest request, CancellationToken _) => written = request)
            .ReturnsAsync(new UpdateItemResponse());
        client
            .Setup(database => database.GetItemAsync(It.IsAny<GetItemRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => DynamoItem(
                ("lease_id", written!.ExpressionAttributeValues[":lease_id"]),
                ("lease_expires_at_ms", written.ExpressionAttributeValues[":lease_expires"])));
        using var store = CreateDynamoStore(client);

        var before = DateTimeOffset.UtcNow;
        Assert.True(await store.TryAcquireLeaseAsync("flow", "owner-a", TimeSpan.FromSeconds(30)));
        var after = DateTimeOffset.UtcNow;

        var observed = await store.ObserveLeaseAsync("flow");
        Assert.Equal("owner-a", observed!.LeaseId);
        Assert.InRange(
            observed.ExpiresAtUtc!.Value,
            DateTimeOffset.FromUnixTimeMilliseconds(before.ToUnixTimeMilliseconds()).UtcDateTime.AddSeconds(30),
            after.UtcDateTime.AddSeconds(30));
    }

    private static DynamoDbFlowStateStore CreateDynamoStore(Mock<IAmazonDynamoDB> client)
        => new(client.Object, Options.Create(new DynamoDbDurableFlowOptions
        {
            TableName = "flows",
            EnableTimeToLive = false,
            TimeToLiveAttributeName = "expires_at"
        }));

    private static Mock<IAmazonDynamoDB> ReadyDynamoClient()
    {
        var client = new Mock<IAmazonDynamoDB>();
        client
            .Setup(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTableResponse
            {
                Table = new TableDescription
                {
                    TableStatus = TableStatus.ACTIVE,
                    KeySchema = [new KeySchemaElement("flow_id", KeyType.HASH)],
                    AttributeDefinitions = [new AttributeDefinition("flow_id", ScalarAttributeType.S)]
                }
            });
        return client;
    }

    private static GetItemResponse DynamoItem(params (string Name, AttributeValue Value)[] attributes)
        => new() { Item = attributes.ToDictionary(attribute => attribute.Name, attribute => attribute.Value) };

    private static AttributeValue Number(long value) => new() { N = value.ToString(CultureInfo.InvariantCulture) };

    // ---- MongoDB: filter, projection and every document shape, through the collection mock. ----

    [Fact]
    public async Task MongoDb_ProjectsTheLeaseFields_WithAnIdOnlyFilter()
    {
        using var harness = new MongoLeaseHarness();
        harness.Finds(null);

        Assert.Same(FlowLeaseObservation.Unheld, await harness.Store.ObserveLeaseAsync("flow-1"));

        var render = new RenderArgs<MongoFlowStateDocument>(
            BsonSerializer.LookupSerializer<MongoFlowStateDocument>(),
            BsonSerializer.SerializerRegistry);
        // Id only — no $expr/$$NOW: every other read in this store filters on server-clock expiry,
        // and this one must not, or an expired lease would stop reading as the same lease.
        Assert.Equal(new BsonDocument("_id", "flow-1"), harness.LastFilter!.Render(render));
        // Only the two lease fields leave the server (plus the implicit _id); state_json does not.
        Assert.Equal(
            new BsonDocument { ["lease_id"] = 1, ["lease_expires_at_utc"] = 1 },
            harness.LastOptions!.Projection.Render(render).Document);
        Assert.Equal(1, harness.LastOptions.Limit);
        // Through the primary-pinned handle, like every ledger read.
        harness.Collection.Verify(item => item.WithReadPreference(ReadPreference.Primary), Times.Once);
    }

    [Fact]
    public async Task MongoDb_MapsEveryDocumentShape_WithoutJudgingExpiry()
    {
        using var harness = new MongoLeaseHarness();

        harness.Finds(null);
        Assert.Same(FlowLeaseObservation.Unheld, await harness.Store.ObserveLeaseAsync("flow"));

        // Present, never leased (or released: the release $unsets both fields).
        harness.Finds(new MongoFlowStateDocument { FlowId = "flow" });
        Assert.Same(FlowLeaseObservation.Unheld, await harness.Store.ObserveLeaseAsync("flow"));

        var live = new DateTime(2031, 3, 14, 9, 26, 53, 589, DateTimeKind.Utc);
        harness.Finds(new MongoFlowStateDocument { FlowId = "flow", LeaseId = "owner-a", LeaseExpiresAtUtc = live });
        var held = await harness.Store.ObserveLeaseAsync("flow");
        Assert.Equal("owner-a", held!.LeaseId);
        Assert.Equal(live, held.ExpiresAtUtc);
        Assert.Equal(DateTimeKind.Utc, held.ExpiresAtUtc!.Value.Kind);

        // Long lapsed, still reported as stored.
        var lapsed = new DateTime(2001, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        harness.Finds(new MongoFlowStateDocument { FlowId = "flow", LeaseId = "dead-worker", LeaseExpiresAtUtc = lapsed });
        var expired = await harness.Store.ObserveLeaseAsync("flow");
        Assert.Equal("dead-worker", expired!.LeaseId);
        Assert.Equal(lapsed, expired.ExpiresAtUtc);

        // A holder with no expiry field is still a holder.
        harness.Finds(new MongoFlowStateDocument { FlowId = "flow", LeaseId = "owner-a" });
        var noExpiry = await harness.Store.ObserveLeaseAsync("flow");
        Assert.Equal("owner-a", noExpiry!.LeaseId);
        Assert.Null(noExpiry.ExpiresAtUtc);

        // Observation never writes.
        harness.Collection.Verify(
            item => item.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task MongoDb_ProjectedDocument_DeserializesWithOnlyTheLeaseFields()
    {
        // What the server actually returns under the projection: _id and the lease fields, nothing
        // else. The class map must bind that (no required members) and hand back a UTC expiry.
        var projected = new BsonDocument
        {
            ["_id"] = "flow",
            ["lease_id"] = "owner-a",
            ["lease_expires_at_utc"] = new BsonDateTime(new DateTime(2031, 3, 14, 9, 26, 53, 589, DateTimeKind.Utc))
        };
        var document = BsonSerializer.Deserialize<MongoFlowStateDocument>(projected);

        using var harness = new MongoLeaseHarness();
        harness.Finds(document);
        var observed = await harness.Store.ObserveLeaseAsync("flow");

        Assert.Equal("owner-a", observed!.LeaseId);
        Assert.Equal(new DateTime(2031, 3, 14, 9, 26, 53, 589, DateTimeKind.Utc), observed.ExpiresAtUtc);
        Assert.Equal(DateTimeKind.Utc, observed.ExpiresAtUtc!.Value.Kind);
    }

    private sealed class MongoLeaseHarness : IDisposable
    {
        public MongoLeaseHarness()
        {
            Database
                .Setup(item => item.GetCollection<MongoFlowStateDocument>("flows", It.IsAny<MongoCollectionSettings>()))
                .Returns(Collection.Object);
            Collection.SelfPinning().WithProvisionedTtlIndex();
            Database.WithTestNamespace();
            Store = new MongoDbFlowStateStore(Database.Object, Options.Create(new MongoDbDurableFlowOptions
            {
                CollectionName = "flows",
                AutoCreateIndexes = false
            }));
        }

        public Mock<IMongoDatabase> Database { get; } = new();
        public Mock<IMongoCollection<MongoFlowStateDocument>> Collection { get; } = new();
        public MongoDbFlowStateStore Store { get; }
        public FilterDefinition<MongoFlowStateDocument>? LastFilter { get; private set; }
        public FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>? LastOptions { get; private set; }

        /// <summary>The next lease read answers with <paramref name="document"/> (or no rows).</summary>
        public void Finds(MongoFlowStateDocument? document)
            => Collection
                .Setup(item => item.FindAsync(
                    It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                    It.IsAny<FindOptions<MongoFlowStateDocument, MongoFlowStateDocument>>(),
                    It.IsAny<CancellationToken>()))
                .Callback((
                    FilterDefinition<MongoFlowStateDocument> filter,
                    FindOptions<MongoFlowStateDocument, MongoFlowStateDocument> options,
                    CancellationToken _) =>
                {
                    LastFilter = filter;
                    LastOptions = options;
                })
                .ReturnsAsync(() => Cursor(document));

        private static IAsyncCursor<MongoFlowStateDocument> Cursor(MongoFlowStateDocument? document)
        {
            IReadOnlyList<MongoFlowStateDocument> rows = document is null ? [] : [document];
            var cursor = new Mock<IAsyncCursor<MongoFlowStateDocument>>();
            var moved = false;
            cursor.Setup(c => c.MoveNextAsync(It.IsAny<CancellationToken>())).ReturnsAsync(() => !moved && (moved = true));
            cursor.Setup(c => c.MoveNext(It.IsAny<CancellationToken>())).Returns(() => !moved && (moved = true));
            cursor.SetupGet(c => c.Current).Returns(rows);
            return cursor.Object;
        }

        public void Dispose() => Store.Dispose();
    }

    // ---- Shared fixtures. ----

    private static FlowState NewState(string flowId) => new()
    {
        FlowId = flowId,
        FlowTypeName = "Round40LeaseObservationFlow",
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow,
        Steps = []
    };

    private static TestFlowDbContext NewContext(string connectionString)
        => new(new DbContextOptionsBuilder<TestFlowDbContext>().UseSqlite(connectionString).Options);

    private sealed class TempSqliteFile(string prefix) : IAsyncDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"{prefix}-{Guid.NewGuid():N}.db");

        public string ConnectionString => $"Data Source={_path}";

        public async Task<object?> ScalarAsync(string sql)
        {
            await using var connection = new SqliteConnection(ConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = sql;
            return await command.ExecuteScalarAsync();
        }

        public ValueTask DisposeAsync()
        {
            // Targeted pool clear (other tests' pools stay warm), then best-effort file cleanup.
            SqliteConnection.ClearPool(new SqliteConnection(ConnectionString));
            foreach (var suffix in new[] { "", "-wal", "-shm" })
            {
                try
                {
                    File.Delete(_path + suffix);
                }
                catch (IOException)
                {
                }
            }

            return ValueTask.CompletedTask;
        }
    }
}
