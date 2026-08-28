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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Moq;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Covers the durable-flow store review fixes: the MaxStateBytes ledger-size guard, server-clock
/// ($$NOW) time authority in the MongoDB query shapes, the matched-count lease-renewal signal,
/// the identity-mismatch load invariant, and TTL saturation for absurd state expiries.
/// </summary>
public sealed class DurableFlowStoreReviewFixesTests
{
    private const string SizeExceptionTypeName = "FlowStateTooLargeException";

    // ---------------------------------------------------------------------------------------
    // P2-2: MaxStateBytes guard
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void MaxStateBytes_Defaults_MatchProviderItemCaps()
    {
        // Document stores default to just under their hard item caps; the effectively-unbounded
        // text/blob stores default to unlimited but stay configurable as operator budgets.
        Assert.Equal(350_000, new DynamoDbDurableFlowOptions().MaxStateBytes);
        Assert.Equal(1_900_000, new CosmosDurableFlowOptions().MaxStateBytes);
        Assert.Equal(15_000_000, new MongoDbDurableFlowOptions().MaxStateBytes);
        Assert.Null(new PostgreSqlDurableFlowOptions().MaxStateBytes);
        Assert.Null(new SqlServerDurableFlowOptions().MaxStateBytes);
        Assert.Null(new MySqlDurableFlowOptions().MaxStateBytes);
        Assert.Null(new OracleDurableFlowOptions().MaxStateBytes);
        Assert.Null(new SqliteDurableFlowOptions().MaxStateBytes);
        Assert.Null(new EFCoreDurableFlowOptions().MaxStateBytes);
    }

    [Fact]
    public void ProviderOptions_RejectNonPositiveMaxStateBytes()
    {
        Assert.Throws<InvalidOperationException>(() => new DynamoDbDurableFlowOptions { MaxStateBytes = 0 }.Validate());
        Assert.Throws<InvalidOperationException>(() => new CosmosDurableFlowOptions { DatabaseName = "flows", MaxStateBytes = -1 }.Validate());
        Assert.Throws<InvalidOperationException>(() => new MongoDbDurableFlowOptions { MaxStateBytes = 0 }.Validate());
        Assert.Throws<InvalidOperationException>(() => new PostgreSqlDurableFlowOptions { MaxStateBytes = 0 }.Validate());
        Assert.Throws<InvalidOperationException>(() => new SqlServerDurableFlowOptions { ConnectionString = "Server=unused", MaxStateBytes = -5 }.Validate());
        Assert.Throws<InvalidOperationException>(() => new MySqlDurableFlowOptions { ConnectionString = "Server=unused", MaxStateBytes = 0 }.Validate());
        Assert.Throws<InvalidOperationException>(() => new OracleDurableFlowOptions { ConnectionString = "Data Source=unused", MaxStateBytes = 0 }.Validate());
        Assert.Throws<InvalidOperationException>(() => new SqliteDurableFlowOptions { MaxStateBytes = 0 }.Validate());

        // EFCore options carry no Validate(); the store constructor enforces the bound instead.
        var services = new ServiceCollection();
        using var provider = services.BuildServiceProvider();
        Assert.Throws<InvalidOperationException>(() => new EFCoreFlowStateStore<TestFlowDbContext>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new EFCoreDurableFlowOptions { MaxStateBytes = 0 })));
    }

    [Fact]
    public async Task DynamoDbStore_RejectsOversizedState_WithDiagnosableError_BeforeWriting()
    {
        var client = new Mock<IAmazonDynamoDB>();
        client
            .Setup(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTableResponse { Table = ValidDynamoTable() });
        using var store = new DynamoDbFlowStateStore(client.Object, Options.Create(new DynamoDbDurableFlowOptions
        {
            TableName = "flows",
            EnableTimeToLive = false
        }));

        // Default limit (350 KB) with a ~400 KB payload: the raw provider 400 KB item-cap error
        // would be retried into DLQ; the guard must fail fast and name the real cause.
        var exception = await Assert.ThrowsAnyAsync<InvalidOperationException>(() =>
            store.TryCreateAsync("outsized-flow", CreateState("outsized-flow", payloadBytes: 400_000), TimeSpan.FromMinutes(5)));

        Assert.Equal(SizeExceptionTypeName, exception.GetType().Name);
        Assert.Contains("outsized-flow", exception.Message, StringComparison.Ordinal);
        Assert.Contains("DynamoDB", exception.Message, StringComparison.Ordinal);
        Assert.Contains("350000", exception.Message, StringComparison.Ordinal);
        Assert.Contains("docs/durable-flows.md", exception.Message, StringComparison.Ordinal);
        client.Verify(database => database.PutItemAsync(It.IsAny<PutItemRequest>(), It.IsAny<CancellationToken>()), Times.Never);

        var updateException = await Assert.ThrowsAnyAsync<InvalidOperationException>(() =>
            store.TryUpdateAsync("outsized-flow", CreateState("outsized-flow", payloadBytes: 400_000, revision: 1), 0, TimeSpan.FromMinutes(5)));
        Assert.Equal(SizeExceptionTypeName, updateException.GetType().Name);
        client.Verify(database => database.UpdateItemAsync(It.IsAny<UpdateItemRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CosmosStore_RejectsOversizedState_BeforeAnyContainerCall()
    {
        using var store = new CosmosFlowStateStore(new Mock<CosmosClient>().Object, Options.Create(new CosmosDurableFlowOptions
        {
            DatabaseName = "flows",
            ContainerName = "states",
            AutoCreateContainer = false,
            MaxStateBytes = 1024
        }));

        var exception = await Assert.ThrowsAnyAsync<InvalidOperationException>(() =>
            store.TryCreateAsync("outsized-flow", CreateState("outsized-flow", payloadBytes: 4096), TimeSpan.FromMinutes(5)));

        Assert.Equal(SizeExceptionTypeName, exception.GetType().Name);
        Assert.Contains("Cosmos DB", exception.Message, StringComparison.Ordinal);
        Assert.Contains("1024", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MongoDbStore_RejectsOversizedState_BeforeAnyCollectionCall()
    {
        // The collection handle exists (the ctor derives a primary-read view of it), but the size
        // rejection must fire before any SERVER operation on it — the mock has no setups beyond
        // the read-preference passthrough, so any issued operation would fail the test.
        var collection = new Mock<IMongoCollection<MongoFlowStateDocument>>();
        collection
            .Setup(item => item.WithReadPreference(It.IsAny<ReadPreference>()))
            .Returns(collection.Object);
        collection.WithProvisionedTtlIndex();
        var database = new Mock<IMongoDatabase>().WithTestNamespace();
        database
            .Setup(item => item.GetCollection<MongoFlowStateDocument>("flows", It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        using var store = new MongoDbFlowStateStore(database.Object, Options.Create(new MongoDbDurableFlowOptions
        {
            CollectionName = "flows",
            AutoCreateIndexes = false,
            MaxStateBytes = 1024
        }));

        var exception = await Assert.ThrowsAnyAsync<InvalidOperationException>(() =>
            store.TryCreateAsync("outsized-flow", CreateState("outsized-flow", payloadBytes: 4096), TimeSpan.FromMinutes(5)));

        Assert.Equal(SizeExceptionTypeName, exception.GetType().Name);
        Assert.Contains("MongoDB", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SqliteStore_EnforcesConfiguredMaxStateBytes_AndStaysUnlimitedByDefault()
    {
        await using var database = new TempSqliteDatabase();
        var budgeted = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString,
            MaxStateBytes = 100_000
        }));

        Assert.True(await budgeted.TryCreateAsync("small-flow", CreateState("small-flow"), TimeSpan.FromMinutes(5)));

        var exception = await Assert.ThrowsAnyAsync<InvalidOperationException>(() =>
            budgeted.TryUpdateAsync("small-flow", CreateState("small-flow", payloadBytes: 150_000, revision: 1), 0, TimeSpan.FromMinutes(5)));
        Assert.Equal(SizeExceptionTypeName, exception.GetType().Name);
        Assert.Contains("SQLite", exception.Message, StringComparison.Ordinal);

        // Null (the SQL-store default) means unlimited: the same oversized state persists fine.
        var unlimited = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString
        }));
        Assert.True(await unlimited.TryCreateAsync("big-flow", CreateState("big-flow", payloadBytes: 150_000), TimeSpan.FromMinutes(5)));
        Assert.NotNull(await unlimited.LoadAsync("big-flow"));
    }

    // ---------------------------------------------------------------------------------------
    // P3-1: lease renewal must use the matched count (Mongo), not the modified count
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task MongoDbStore_TreatsSameMillisecondRenewalAsSuccess_AndKeepsModifiedSemanticsForCheckpoints()
    {
        var collection = new Mock<IMongoCollection<MongoFlowStateDocument>>();
        collection
            .Setup(item => item.WithReadPreference(It.IsAny<ReadPreference>()))
            .Returns(collection.Object);
        collection.WithProvisionedTtlIndex();
        var database = new Mock<IMongoDatabase>().WithTestNamespace();
        database
            .Setup(item => item.GetCollection<MongoFlowStateDocument>("flows", It.IsAny<MongoCollectionSettings>()))
            .Returns(collection.Object);
        // Matched-but-not-modified: a renewal landing in the same millisecond as the previous one
        // writes an identical lease expiry. That is a healthy owner, not a lost lease.
        collection
            .Setup(item => item.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new UpdateResult.Acknowledged(1, 0, BsonNull.Value));
        using var store = new MongoDbFlowStateStore(database.Object, Options.Create(new MongoDbDurableFlowOptions
        {
            CollectionName = "flows",
            AutoCreateIndexes = false
        }));

        Assert.True(await store.TryRenewLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
        Assert.True(await store.TryAcquireLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));

        // Checkpoints keep modified-count semantics: they always bump the revision, so a matched
        // document is always modified — matched-without-modified means the write did not land.
        var state = CreateState("flow", revision: 1);
        Assert.False(await store.TryUpdateAsync("flow", state, 0, TimeSpan.FromMinutes(5)));

        collection
            .Setup(item => item.UpdateOneAsync(
                It.IsAny<FilterDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateDefinition<MongoFlowStateDocument>>(),
                It.IsAny<UpdateOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new UpdateResult.Acknowledged(0, 0, BsonNull.Value));
        Assert.False(await store.TryRenewLeaseAsync("flow", "owner", TimeSpan.FromMinutes(1)));
    }

    // ---------------------------------------------------------------------------------------
    // P2-1: MongoDB query shapes run expiry/lease math on the server clock ($$NOW)
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void MongoDbStore_ExpiryFilters_CompareAgainstServerClock()
    {
        var live = Render(MongoDbFlowStateStore.BuildLiveFilter("flow"));
        Assert.Equal("flow", live["_id"].AsString);
        Assert.Equal(new BsonArray { "$expires_at_utc", "$$NOW" }, FindExpr(live, "$gt"));

        var expired = Render(MongoDbFlowStateStore.BuildExpiredReplaceFilter("flow"));
        Assert.Equal(new BsonArray { "$expires_at_utc", "$$NOW" }, FindExpr(expired, "$lte"));
    }

    [Fact]
    public void MongoDbStore_CheckpointFilter_FencesRevisionAndLeaseOnServerClock()
    {
        var unleased = Render(MongoDbFlowStateStore.BuildCheckpointFilter("flow", expectedRevision: 3, leaseId: null));
        Assert.Equal("flow", unleased["_id"].AsString);
        Assert.Equal(3L, unleased["revision"].AsInt64);
        Assert.Equal(new BsonArray { "$expires_at_utc", "$$NOW" }, FindExpr(unleased, "$gt"));

        // Two $expr clauses force the $and form, so assert on the rendered JSON.
        var leased = Render(MongoDbFlowStateStore.BuildCheckpointFilter("flow", expectedRevision: 3, leaseId: "owner")).ToJson();
        Assert.Contains("\"lease_id\" : \"owner\"", leased, StringComparison.Ordinal);
        Assert.Contains("$lease_expires_at_utc", leased, StringComparison.Ordinal);
        Assert.Contains("$$NOW", leased, StringComparison.Ordinal);
    }

    [Fact]
    public void MongoDbStore_LeaseFilterAndUpdate_RunOnServerClock()
    {
        var acquire = Render(MongoDbFlowStateStore.BuildLeaseFilter("flow", "owner", acquire: true)).ToJson();
        // Acquire steals only leases the SERVER considers expired (or absent / already ours).
        Assert.Contains("$lease_expires_at_utc", acquire, StringComparison.Ordinal);
        Assert.Contains("$$NOW", acquire, StringComparison.Ordinal);
        Assert.Contains("$or", acquire, StringComparison.Ordinal);

        // Renew carries two $expr clauses (ledger expiry + lease expiry), which render as $and.
        var renew = Render(MongoDbFlowStateStore.BuildLeaseFilter("flow", "owner", acquire: false)).ToJson();
        Assert.Contains("\"lease_id\" : \"owner\"", renew, StringComparison.Ordinal);
        Assert.Contains("$lease_expires_at_utc", renew, StringComparison.Ordinal);

        var update = RenderUpdate(MongoDbFlowStateStore.BuildLeaseUpdate("owner", TimeSpan.FromSeconds(30)));
        var set = update.AsBsonArray[0]["$set"].AsBsonDocument;
        Assert.Equal("owner", set["lease_id"]["$literal"].AsString);
        Assert.Equal(new BsonArray { "$$NOW", 30_000L }, set["lease_expires_at_utc"]["$add"].AsBsonArray);
    }

    [Fact]
    public void MongoDbStore_StateUpdate_StampsServerClockAndOptionallyResetsLease()
    {
        var checkpoint = RenderUpdate(MongoDbFlowStateStore.BuildStateUpdate("{\"j\":1}", revision: 4, TimeSpan.FromMinutes(1), resetLease: false));
        var stages = checkpoint.AsBsonArray;
        var set = Assert.Single(stages)["$set"].AsBsonDocument;
        // $literal keeps the JSON payload a value (a "$"-prefixed string would be a field path).
        Assert.Equal("{\"j\":1}", set["state_json"]["$literal"].AsString);
        Assert.Equal(new BsonArray { "$$NOW", 60_000L }, set["expires_at_utc"]["$add"].AsBsonArray);
        Assert.Equal("$$NOW", set["updated_at_utc"].AsString);
        Assert.Equal(4L, set["revision"].AsInt64);

        var createReplace = RenderUpdate(MongoDbFlowStateStore.BuildStateUpdate("{}", revision: 0, TimeSpan.FromMinutes(1), resetLease: true)).AsBsonArray;
        Assert.Equal(2, createReplace.Count);
        Assert.Equal(new BsonArray { "lease_id", "lease_expires_at_utc" }, createReplace[1]["$unset"].AsBsonArray);
    }

    // ---------------------------------------------------------------------------------------
    // P3-2: identity-mismatched ledgers load as absent
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task SqliteStore_LoadsIdentityMismatchedRowAsAbsent()
    {
        await using var database = new TempSqliteDatabase();
        var store = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString
        }));
        Assert.True(await store.TryCreateAsync("provision", CreateState("provision"), TimeSpan.FromMinutes(5)));

        // A row copied/restored under the wrong key: state_json says "other-flow" but the row key
        // is "hijacked-flow". Docs promise identity-mismatched records load as absent.
        await using (var connection = new SqliteConnection(database.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                """
                INSERT INTO "asyncresponse_flow_state" (flow_id, state_json, expires_at_utc, updated_at_utc, revision)
                VALUES ('hijacked-flow', $state_json, $expires_at_utc, $now_utc, 0);
                """;
            command.Parameters.AddWithValue("$state_json", JsonSerializer.Serialize(CreateState("other-flow")));
            command.Parameters.AddWithValue("$expires_at_utc", DateTime.UtcNow.AddMinutes(5));
            command.Parameters.AddWithValue("$now_utc", DateTime.UtcNow);
            await command.ExecuteNonQueryAsync();
        }

        Assert.Null(await store.LoadAsync("hijacked-flow"));
    }

    [Fact]
    public async Task DynamoDbStore_LoadsIdentityMismatchedItemAsAbsent()
    {
        var client = new Mock<IAmazonDynamoDB>();
        client
            .Setup(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTableResponse { Table = ValidDynamoTable() });
        var futureExpiry = DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeSeconds().ToString();
        client
            .Setup(database => database.GetItemAsync(It.IsAny<GetItemRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetItemResponse
            {
                Item = new Dictionary<string, AttributeValue>
                {
                    ["expires_at"] = new() { N = futureExpiry },
                    ["state_json"] = new() { S = JsonSerializer.Serialize(CreateState("other-flow")) },
                    ["revision"] = new() { N = "0" }
                }
            });
        using var store = new DynamoDbFlowStateStore(client.Object, Options.Create(new DynamoDbDurableFlowOptions
        {
            TableName = "flows",
            EnableTimeToLive = false
        }));

        Assert.Null(await store.LoadAsync("hijacked-flow"));
    }

    // ---------------------------------------------------------------------------------------
    // P3-6: absurd TTLs saturate instead of overflowing DateTime math
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task SqliteStore_SaturatesAbsurdTtlAndLeaseDuration_InsteadOfThrowing()
    {
        await using var database = new TempSqliteDatabase();
        var store = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString
        }));

        // DateTime.UtcNow + TimeSpan.MaxValue used to throw ArgumentOutOfRangeException on every
        // write; a saturated expiry means "effectively never expires".
        Assert.True(await store.TryCreateAsync("eternal-flow", CreateState("eternal-flow"), TimeSpan.MaxValue));
        Assert.NotNull(await store.LoadAsync("eternal-flow"));
        Assert.True(await store.TryAcquireLeaseAsync("eternal-flow", "owner", TimeSpan.MaxValue));
        Assert.True(await store.TryUpdateAsync("eternal-flow", CreateState("eternal-flow", revision: 1), 0, TimeSpan.MaxValue, "owner"));
    }

    [Fact]
    public async Task DynamoDbStore_SaturatesAbsurdTtl_InsteadOfThrowing()
    {
        var client = new Mock<IAmazonDynamoDB>();
        client
            .Setup(database => database.DescribeTableAsync("flows", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DescribeTableResponse { Table = ValidDynamoTable() });
        client
            .Setup(database => database.PutItemAsync(It.IsAny<PutItemRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PutItemResponse());
        using var store = new DynamoDbFlowStateStore(client.Object, Options.Create(new DynamoDbDurableFlowOptions
        {
            TableName = "flows",
            EnableTimeToLive = false
        }));

        Assert.True(await store.TryCreateAsync("eternal-flow", CreateState("eternal-flow"), TimeSpan.MaxValue));
    }

    // ---------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------

    private static BsonDocument Render(FilterDefinition<MongoFlowStateDocument> filter)
        => filter.Render(RenderArgsFor());

    private static BsonValue RenderUpdate(UpdateDefinition<MongoFlowStateDocument> update)
        => update.Render(RenderArgsFor());

    private static RenderArgs<MongoFlowStateDocument> RenderArgsFor()
        => new(BsonSerializer.LookupSerializer<MongoFlowStateDocument>(), BsonSerializer.SerializerRegistry);

    /// <summary>Finds the $expr comparison array for <paramref name="op"/> whether the filter rendered flat or under $and.</summary>
    private static BsonArray FindExpr(BsonDocument filter, string op)
    {
        if (filter.TryGetValue("$expr", out var expr))
            return expr[op].AsBsonArray;

        foreach (var clause in filter["$and"].AsBsonArray)
        {
            if (clause.AsBsonDocument.TryGetValue("$expr", out var nested) && nested.AsBsonDocument.Contains(op))
                return nested[op].AsBsonArray;
        }

        throw new Xunit.Sdk.XunitException($"No $expr {op} comparison found in {filter.ToJson()}.");
    }

    private static TableDescription ValidDynamoTable() => new()
    {
        TableStatus = TableStatus.ACTIVE,
        KeySchema = [new KeySchemaElement("flow_id", KeyType.HASH)],
        AttributeDefinitions = [new AttributeDefinition("flow_id", ScalarAttributeType.S)]
    };

    private static FlowState CreateState(string flowId, int payloadBytes = 0, long revision = 0) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(TestOnboardingFlow).FullName,
        InputTypeName = typeof(TestFlowInput).FullName,
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow,
        Revision = revision,
        Values = payloadBytes > 0
            ? new Dictionary<string, string>(StringComparer.Ordinal) { ["payload"] = new string('x', payloadBytes) }
            : null
    };

    private sealed class TempSqliteDatabase : IAsyncDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"ar-review-flow-state-{Guid.NewGuid():N}.db");

        public string ConnectionString => $"Data Source={_path}";

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
