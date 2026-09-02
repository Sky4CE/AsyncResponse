using System.Reflection;
using System.Text;
using System.Text.Json;
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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MongoDB.Driver;
using Npgsql;
using System.Security.Cryptography;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class DurableFlowStoreSharedTests
{
    private const string SharedTypeName = "AsyncResponse.DurableFlows.Internal.DurableFlowStoreShared";

    [Theory]
    [MemberData(nameof(ProviderOptionTypes))]
    public void SharedHelpers_ValidateSerializeAndBuildSchemaLocks(Type providerOptionsType)
    {
        var shared = providerOptionsType.Assembly.GetType(SharedTypeName, throwOnError: true)!;
        var state = CreateState("flow");

        Invoke(shared, "ValidateCreate", "flow", state, TimeSpan.FromMinutes(1));
        AssertInner<ArgumentException>(shared, "ValidateCreate", " ", state, TimeSpan.FromMinutes(1));
        AssertInner<ArgumentNullException>(shared, "ValidateCreate", "flow", null, TimeSpan.FromMinutes(1));
        AssertInner<ArgumentException>(shared, "ValidateCreate", "other", state, TimeSpan.FromMinutes(1));
        AssertInner<ArgumentOutOfRangeException>(shared, "ValidateCreate", "flow", state, TimeSpan.Zero);

        state.SchemaVersion = FlowStateSchema.Current + 1;
        AssertInner<ArgumentException>(shared, "ValidateCreate", "flow", state, TimeSpan.FromMinutes(1));
        state.SchemaVersion = FlowStateSchema.Current;
        state.Revision = 1;
        AssertInner<ArgumentException>(shared, "ValidateCreate", "flow", state, TimeSpan.FromMinutes(1));

        Invoke(shared, "ValidateUpdate", "flow", state, 0L, TimeSpan.FromMinutes(1));
        AssertInner<ArgumentOutOfRangeException>(shared, "ValidateUpdate", "flow", state, -1L, TimeSpan.FromMinutes(1));
        AssertInner<ArgumentException>(shared, "ValidateUpdate", "flow", state, 1L, TimeSpan.FromMinutes(1));
        AssertInner<OverflowException>(shared, "ValidateUpdate", "flow", state, long.MaxValue, TimeSpan.FromMinutes(1));

        var json = Assert.IsType<string>(Invoke(shared, "Serialize", state));
        Assert.Equal("flow", Assert.IsType<FlowState>(Invoke(shared, "Deserialize", json, "flow")).FlowId);

        // A row that is PRESENT and uninterpretable throws rather than returning null, in every
        // store package: null is reserved for genuine absence, which is the only case where the
        // executor may ack the wake-up (see FlowStateUnreadableException).
        Assert.Equal("flow", AssertInner<FlowStateUnreadableException>(shared, "Deserialize", "null", "flow").FlowId);
        AssertInner<FlowStateUnreadableException>(shared, "Deserialize", "{", "flow");
        state.SchemaVersion = FlowStateSchema.Current + 1;
        AssertInner<FlowStateUnreadableException>(shared, "Deserialize", JsonSerializer.Serialize(state), "flow");
        state.SchemaVersion = FlowStateSchema.Current;

        Invoke(shared, "ValidateIdentifier", "valid_name2", "Name", "provider", 0);
        AssertInner<InvalidOperationException>(shared, "ValidateIdentifier", null, "Name", "provider", 0);
        AssertInner<InvalidOperationException>(shared, "ValidateIdentifier", "1invalid", "Name", "provider", 0);
        AssertInner<InvalidOperationException>(shared, "ValidateIdentifier", "invalid-name", "Name", "provider", 0);

        // Identifier caps and derived-name suffix reservation: a cap of 0 disables the length
        // check, an over-cap name is rejected, and a derived name truncates the table STEM so a
        // maximum-length table can never derive its own name back.
        Invoke(shared, "ValidateIdentifier", new string('t', 64), "Name", "provider", 0);
        AssertInner<InvalidOperationException>(shared, "ValidateIdentifier", new string('t', 64), "Name", "provider", 63);
        Invoke(shared, "ValidateIdentifier", new string('t', 63), "Name", "provider", 63);
        Assert.Equal("short_expires_idx", Invoke(shared, "DerivedName", "short", "_expires_idx", 63));
        var derivedAtCap = Assert.IsType<string>(Invoke(shared, "DerivedName", new string('t', 63), "_expires_idx", 63));
        Assert.Equal(new string('t', 51) + "_expires_idx", derivedAtCap);

        var pruneArgs = new object?[] { 0L, TimeSpan.FromHours(1) };
        Assert.True(Assert.IsType<bool>(Invoke(shared, "ShouldPrune", pruneArgs)));
        Assert.NotEqual(0L, Assert.IsType<long>(pruneArgs[0]));
        Assert.False(Assert.IsType<bool>(Invoke(shared, "ShouldPrune", pruneArgs)));
        Assert.True(Assert.IsType<bool>(Invoke(shared, "ShouldPrune", 0L, TimeSpan.Zero)));

        Assert.Equal("asyncresponse:ddl:custom_schema", Invoke(shared, "SchemaLockResource", "custom_schema"));
        var lockKey = Assert.IsType<long>(Invoke(shared, "SchemaLockKey", "custom_schema"));
        Assert.Equal(lockKey, Assert.IsType<long>(Invoke(shared, "SchemaLockKey", "custom_schema")));
        Assert.NotEqual(lockKey, Assert.IsType<long>(Invoke(shared, "SchemaLockKey", "other_schema")));
    }

    /// <summary>
    /// The size guard, the read-side identity mirror and the saturating clock helpers, exercised in
    /// every store assembly. The shared source is compiled per package, so a helper only some
    /// providers happen to call is otherwise dead code in the rest of them — this walks all nine.
    /// </summary>
    [Theory]
    [MemberData(nameof(ProviderOptionTypes))]
    public void SharedHelpers_BoundSizeReadStateAndSaturateClocks(Type providerOptionsType)
    {
        var shared = providerOptionsType.Assembly.GetType(SharedTypeName, throwOnError: true)!;
        var state = CreateState("flow");
        var json = Assert.IsType<string>(Invoke(shared, "Serialize", state));

        // No budget configured, and a budget the ledger fits inside, both serialize unchanged.
        Assert.Equal(json, Invoke(shared, "SerializeBounded", "flow", state, null, "provider"));
        Assert.Equal(json, Invoke(shared, "SerializeBounded", "flow", state, (long?)json.Length * 4, "provider"));

        // Over budget names the cause instead of leaving the provider to reject opaque bytes.
        var tooLarge = AssertInnerAssignable<InvalidOperationException>(
            shared, "SerializeBounded", "flow", state, (long?)1, "provider");
        Assert.Equal("FlowStateTooLargeException", tooLarge.GetType().Name);
        Assert.Equal("flow", tooLarge.GetType().GetProperty("FlowId")!.GetValue(tooLarge));
        Assert.Equal(1L, tooLarge.GetType().GetProperty("MaxStateBytes")!.GetValue(tooLarge));
        Assert.Equal(
            (long)Encoding.UTF8.GetByteCount(json),
            tooLarge.GetType().GetProperty("SerializedSizeBytes")!.GetValue(tooLarge));
        Assert.Contains("exceeding the provider MaxStateBytes limit of 1 bytes", tooLarge.Message);

        // ReadState: only a readable ledger whose revision AND identity match loads as present.
        // The two mismatches still read as absent — a row under the wrong key or at the wrong
        // revision is a benign race or a misplaced restore, and treating it as this flow would be
        // worse. An UNREADABLE row is the case that changed: it throws, because reporting it as
        // absent told the executor to ack a live run's only wake-up.
        Assert.Equal("flow", Assert.IsType<FlowState>(Invoke(shared, "ReadState", "flow", json, 0L)).FlowId);
        AssertInner<FlowStateUnreadableException>(shared, "ReadState", "flow", "{", 0L);  // unreadable
        Assert.Null(Invoke(shared, "ReadState", "flow", json, 7L));        // revision mismatch
        Assert.Null(Invoke(shared, "ReadState", "other", json, 0L));       // identity mismatch

        // Saturating adds: an absurd expiry means "never" rather than an overflow on every write.
        var instant = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        Assert.Equal(
            instant.AddMinutes(5),
            InvokeOverload(shared, "AddSaturating", [typeof(DateTime), typeof(TimeSpan)], instant, TimeSpan.FromMinutes(5)));
        Assert.Equal(
            DateTime.MaxValue,
            InvokeOverload(shared, "AddSaturating", [typeof(DateTime), typeof(TimeSpan)], instant, TimeSpan.MaxValue));

        var offset = new DateTimeOffset(instant);
        Assert.Equal(
            offset.AddMinutes(5),
            InvokeOverload(shared, "AddSaturating", [typeof(DateTimeOffset), typeof(TimeSpan)], offset, TimeSpan.FromMinutes(5)));
        Assert.Equal(
            DateTimeOffset.MaxValue,
            InvokeOverload(shared, "AddSaturating", [typeof(DateTimeOffset), typeof(TimeSpan)], offset, TimeSpan.MaxValue));

        // Server-clock TTLs clamp at ~68 years so DATEADD/year-9999 arithmetic cannot overflow.
        var clamp = TimeSpan.FromSeconds(int.MaxValue);
        Assert.Equal(TimeSpan.FromMinutes(5), Invoke(shared, "ServerClockTtl", TimeSpan.FromMinutes(5)));
        Assert.Equal(clamp, Invoke(shared, "ServerClockTtl", TimeSpan.MaxValue));
        Assert.Equal(
            (long)TimeSpan.FromMinutes(5).TotalMilliseconds,
            Invoke(shared, "ServerClockTtlMilliseconds", TimeSpan.FromMinutes(5)));
        Assert.Equal(
            (long)clamp.TotalMilliseconds,
            Invoke(shared, "ServerClockTtlMilliseconds", TimeSpan.MaxValue));
    }

    /// <summary>
    /// Regression (round 33): the six relational stores awaited their opportunistic prune bare
    /// inside TryCreateAsync, so a prune DELETE chosen as the deadlock victim (1205) or hitting a
    /// lock-wait timeout against the store's own checkpoint traffic failed StartAsync for a flow
    /// whose row would have been created without incident — and ShouldPrune had already consumed
    /// the interval, so it was not retried either. PruneQuietlyAsync contains the prune's failure
    /// (the next interval retries); cancellation still propagates. Reflected: the helper is the
    /// fix, so an older build has none and this fact fails there instead of failing to compile.
    /// </summary>
    [Theory]
    [MemberData(nameof(ProviderOptionTypes))]
    public async Task PruneQuietly_ContainsPruneFailures_ButPropagatesCancellation(Type providerOptionsType)
    {
        var shared = providerOptionsType.Assembly.GetType(SharedTypeName, throwOnError: true)!;
        var pruneQuietly = shared.GetMethod("PruneQuietlyAsync", BindingFlags.Public | BindingFlags.Static);
        Assert.NotNull(pruneQuietly);

        var attempted = 0;
        await Quietly(() => { attempted++; throw new InvalidOperationException("deadlock victim"); });
        await Quietly(() => { attempted++; return Task.FromException(new TimeoutException("lock wait timeout")); });
        Assert.Equal(2, attempted);

        await Assert.ThrowsAsync<OperationCanceledException>(() => Quietly(() => throw new OperationCanceledException()));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Quietly(() => Task.FromCanceled(new CancellationToken(canceled: true))));

        Task Quietly(Func<Task> prune) => (Task)pruneQuietly!.Invoke(null, [prune])!;
    }

    public static TheoryData<Type> ProviderOptionTypes =>
    [
        typeof(CosmosDurableFlowOptions),
        typeof(DynamoDbDurableFlowOptions),
        typeof(EFCoreDurableFlowOptions),
        typeof(MongoDbDurableFlowOptions),
        typeof(MySqlDurableFlowOptions),
        typeof(OracleDurableFlowOptions),
        typeof(PostgreSqlDurableFlowOptions),
        typeof(SqliteDurableFlowOptions),
        typeof(SqlServerDurableFlowOptions)
    ];

    [Fact]
    public void ProviderOptions_RejectInvalidValues()
    {
        Assert.Throws<InvalidOperationException>(() => new CosmosDurableFlowOptions().Validate());
        Assert.Throws<InvalidOperationException>(() => new CosmosDurableFlowOptions
        {
            DatabaseName = "flows",
            ContainerName = " "
        }.Validate());
        Assert.Throws<InvalidOperationException>(() => new CosmosDurableFlowOptions
        {
            DatabaseName = "flows",
            PartitionKeyPath = "flowId"
        }.Validate());
        Assert.Throws<InvalidOperationException>(() => new CosmosDurableFlowOptions
        {
            DatabaseName = "flows",
            Throughput = 0
        }.Validate());

        Assert.Throws<InvalidOperationException>(() => new DynamoDbDurableFlowOptions { TableName = " " }.Validate());
        Assert.Throws<InvalidOperationException>(() => new DynamoDbDurableFlowOptions { TimeToLiveAttributeName = " " }.Validate());
        Assert.Throws<InvalidOperationException>(() => new MongoDbDurableFlowOptions { CollectionName = " " }.Validate());
        Assert.Throws<InvalidOperationException>(() => new MongoDbDurableFlowOptions { CollectionName = "bad$flows" }.Validate());
        Assert.Throws<InvalidOperationException>(() => new MongoDbDurableFlowOptions { CollectionName = "system.flows" }.Validate());
        Assert.Throws<InvalidOperationException>(() => new MongoDbDurableFlowOptions { CollectionName = "app.system.flows" }.Validate());
        Assert.Throws<InvalidOperationException>(() => new MySqlDurableFlowOptions().Validate());
        Assert.Throws<InvalidOperationException>(() => new OracleDurableFlowOptions().Validate());
        Assert.Throws<InvalidOperationException>(() => new SqliteDurableFlowOptions { ConnectionString = " " }.Validate());
        Assert.Throws<InvalidOperationException>(() => new SqlServerDurableFlowOptions().Validate());

        // Identifier caps + the PostgreSQL relation-namespace collision: a 64-character name would
        // be silently truncated server-side, and a 63-character table ending exactly where the
        // reserved "_expires_idx" stem truncates derives its own name (the index DDL would be
        // silently skipped).
        var overCap = Assert.Throws<InvalidOperationException>(
            () => new PostgreSqlDurableFlowOptions { TableName = new string('t', 64) }.Validate());
        Assert.Contains("limited to 63", overCap.Message);
        var selfDerived = Assert.Throws<InvalidOperationException>(
            () => new PostgreSqlDurableFlowOptions { TableName = new string('t', 51) + "_expires_idx" }.Validate());
        Assert.Contains("expiry-index", selfDerived.Message);
        new PostgreSqlDurableFlowOptions { TableName = new string('t', 63) }.Validate();
    }

    [Fact]
    public void OracleTableName_CannotDeriveItsOwnExpiryIndexName()
    {
        // Oracle tables and indexes share one schema-object namespace, and a 128-character table
        // name ending exactly where the reserved "_EXPIRES_IDX" stem truncates derives its own
        // name: CREATE INDEX raises ORA-00955, which the DDL path must swallow as the benign
        // already-exists race — the expiry index would silently never exist and every prune
        // would full-scan. Unquoted Oracle identifiers are case-insensitive, so the check is too.
        var selfDerived = Assert.Throws<InvalidOperationException>(() => new OracleDurableFlowOptions
        {
            ConnectionString = "Data Source=db",
            TableName = new string('T', 116) + "_EXPIRES_IDX"
        }.Validate());
        Assert.Contains("expiry-index", selfDerived.Message);

        var caseFolded = Assert.Throws<InvalidOperationException>(() => new OracleDurableFlowOptions
        {
            ConnectionString = "Data Source=db",
            TableName = new string('t', 116) + "_expires_idx"
        }.Validate());
        Assert.Contains("expiry-index", caseFolded.Message);

        // A maximum-length name that does not collide, and a short name carrying the suffix
        // (its derived name is longer, not itself), both stay valid.
        new OracleDurableFlowOptions { ConnectionString = "Data Source=db", TableName = new string('T', 128) }.Validate();
        new OracleDurableFlowOptions { ConnectionString = "Data Source=db", TableName = "T_EXPIRES_IDX" }.Validate();
    }

    [Fact]
    public void CosmosRegistration_RequiresOrOwnsClient()
    {
        using var missing = BuildProvider((_, builder) => builder.WithCosmosDurableFlows(options =>
            options.DatabaseName = "flows"));
        Assert.Throws<InvalidOperationException>(() => missing.GetRequiredService<IFlowStateStore>());

        using var owned = BuildProvider((_, builder) => builder.WithCosmosDurableFlows(options =>
        {
            options.DatabaseName = "flows";
            options.ConnectionString = $"AccountEndpoint=https://localhost:8081/;AccountKey={Convert.ToBase64String(RandomNumberGenerator.GetBytes(64))};";
        }));
        Assert.IsType<CosmosFlowStateStore>(owned.GetRequiredService<IFlowStateStore>());
    }

    [Fact]
    public void PostgreSqlRegistration_RequiresOrOwnsDataSource()
    {
        using var missing = BuildProvider((_, builder) => builder.WithPostgreSqlDurableFlows());
        Assert.Throws<InvalidOperationException>(() => missing.GetRequiredService<IFlowStateStore>());

        using var owned = BuildProvider((_, builder) => builder.WithPostgreSqlDurableFlows(options =>
            options.ConnectionString = "Host=localhost;Username=postgres;Password=postgres;Database=flows;Pooling=false"));
        Assert.IsType<PostgreSqlFlowStateStore>(owned.GetRequiredService<IFlowStateStore>());
    }

    [Fact]
    public void MongoDbRegistration_HandlesMissingAndSharedDependencies()
    {
        using var missingDatabase = BuildProvider((_, builder) => builder.WithMongoDbDurableFlows());
        Assert.Throws<InvalidOperationException>(() => missingDatabase.GetRequiredService<IFlowStateStore>());

        using var missingClient = BuildProvider((_, builder) => builder.WithMongoDbDurableFlows(options =>
            options.DatabaseName = "flows"));
        Assert.Throws<InvalidOperationException>(() => missingClient.GetRequiredService<IFlowStateStore>());

        using var sharedClient = BuildProvider((services, builder) =>
        {
            services.AddSingleton<IMongoClient>(new MongoClient("mongodb://localhost:27017"));
            builder.WithMongoDbDurableFlows(options => options.DatabaseName = "flows");
        });
        Assert.IsType<MongoDbFlowStateStore>(sharedClient.GetRequiredService<IFlowStateStore>());

        using var sharedDatabase = BuildProvider((services, builder) =>
        {
            services.AddSingleton(new MongoClient("mongodb://localhost:27017").GetDatabase("flows"));
            builder.WithMongoDbDurableFlows();
        });
        Assert.IsType<MongoDbFlowStateStore>(sharedDatabase.GetRequiredService<IFlowStateStore>());
    }

    private static FlowState CreateState(string flowId) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(TestOnboardingFlow).FullName,
        InputTypeName = typeof(TestFlowInput).FullName,
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    private static ServiceProvider BuildProvider(
        Action<IServiceCollection, AsyncResponseRegistrationBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        var builder = services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport();
        configure(services, builder);
        return services.BuildServiceProvider();
    }

    private static object? Invoke(Type shared, string methodName, params object?[] arguments)
        => shared.GetMethod(methodName, BindingFlags.Public | BindingFlags.Static)!.Invoke(null, arguments);

    /// <summary>Invoke by explicit signature, for the overloaded helpers (<c>AddSaturating</c>).</summary>
    private static object? InvokeOverload(
        Type shared,
        string methodName,
        Type[] parameterTypes,
        params object?[] arguments)
        => shared.GetMethod(methodName, BindingFlags.Public | BindingFlags.Static, binder: null, parameterTypes, modifiers: null)!
            .Invoke(null, arguments);

    /// <summary>
    /// Exact-type inner-exception assertion — the default, so an <c>ArgumentNullException</c> can
    /// never silently satisfy an <c>ArgumentException</c> expectation.
    /// </summary>
    private static TException AssertInner<TException>(
        Type shared,
        string methodName,
        params object?[] arguments)
        where TException : Exception
    {
        var exception = Assert.Throws<TargetInvocationException>(() => Invoke(shared, methodName, arguments));
        return Assert.IsType<TException>(exception.InnerException);
    }

    /// <remarks>
    /// Matches derived exceptions — ONLY for throws the test cannot name exactly: the shared
    /// source's <c>FlowStateTooLargeException</c> is internal to each store assembly, so tests can
    /// only name its <see cref="InvalidOperationException"/> base, which the exact-match helper
    /// above would reject. Every other assertion uses <see cref="AssertInner{TException}"/>.
    /// </remarks>
    private static TException AssertInnerAssignable<TException>(
        Type shared,
        string methodName,
        params object?[] arguments)
        where TException : Exception
    {
        var exception = Assert.Throws<TargetInvocationException>(() => Invoke(shared, methodName, arguments));
        return Assert.IsAssignableFrom<TException>(exception.InnerException);
    }
}
