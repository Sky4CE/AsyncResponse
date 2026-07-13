using System.Reflection;
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
        Assert.Equal("flow", Assert.IsType<FlowState>(Invoke(shared, "Deserialize", json)).FlowId);
        Assert.Null(Invoke(shared, "Deserialize", "null"));
        Assert.Null(Invoke(shared, "Deserialize", "{"));
        state.SchemaVersion = FlowStateSchema.Current + 1;
        Assert.Null(Invoke(shared, "Deserialize", JsonSerializer.Serialize(state)));

        Invoke(shared, "ValidateIdentifier", "valid_name2", "Name", "provider");
        AssertInner<InvalidOperationException>(shared, "ValidateIdentifier", null, "Name", "provider");
        AssertInner<InvalidOperationException>(shared, "ValidateIdentifier", "1invalid", "Name", "provider");
        AssertInner<InvalidOperationException>(shared, "ValidateIdentifier", "invalid-name", "Name", "provider");

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
        Assert.Throws<InvalidOperationException>(() => new MySqlDurableFlowOptions().Validate());
        Assert.Throws<InvalidOperationException>(() => new OracleDurableFlowOptions().Validate());
        Assert.Throws<InvalidOperationException>(() => new SqliteDurableFlowOptions { ConnectionString = " " }.Validate());
        Assert.Throws<InvalidOperationException>(() => new SqlServerDurableFlowOptions().Validate());
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

    private static TException AssertInner<TException>(
        Type shared,
        string methodName,
        params object?[] arguments)
        where TException : Exception
    {
        var exception = Assert.Throws<TargetInvocationException>(() => Invoke(shared, methodName, arguments));
        return Assert.IsType<TException>(exception.InnerException);
    }
}
