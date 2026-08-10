using AsyncResponse.Conformance;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The matrix is only "every combination" if nothing can be added to the library without being added
/// to it. These guards are that tie: they compare the enumerated axes against the registration
/// methods the packages actually expose, and the shards against the cells.
/// <para>
/// A new channel, transport, or store package fails here the day it lands — before anyone can ship it
/// with no cross-product coverage — and the failure names the missing enum member.
/// </para>
/// </summary>
[Trait(Batches.Trait, Batches.None)]
public sealed class MatrixCompletenessTests
{
    /// <summary>
    /// Registration methods are the library's own list of what exists. Anything matching
    /// <c>With…Channel</c> on the registration builder must have a <see cref="MatrixChannel"/>.
    /// </summary>
    [Fact]
    public void EveryChannelRegistration_HasAMatrixAxisMember()
        => AssertAxisCoversRegistrations(
            suffix: "Channel",
            axis: ProviderMatrix.Channels.Select(channel => channel.ToString()),
            axisName: nameof(MatrixChannel));

    [Fact]
    public void EveryTransportRegistration_HasAMatrixAxisMember()
        => AssertAxisCoversRegistrations(
            suffix: "Transport",
            axis: ProviderMatrix.Transports.Select(transport => transport.ToString()),
            axisName: nameof(MatrixTransport));

    [Fact]
    public void EveryDurableFlowStoreRegistration_HasAMatrixAxisMember()
        => AssertAxisCoversRegistrations(
            suffix: "DurableFlows",
            axis: ProviderMatrix.Stores.Select(store => store.ToString()),
            axisName: nameof(MatrixStore));

    /// <summary>The product is complete and nothing is enumerated twice.</summary>
    [Fact]
    public void AllCells_IsTheCompleteCrossProduct()
    {
        var expected = ProviderMatrix.Channels.Count * ProviderMatrix.Transports.Count * ProviderMatrix.Stores.Count;
        Assert.Equal(expected, ProviderMatrix.AllCells.Count);
        Assert.Equal(expected, ProviderMatrix.AllCells.Distinct().Count());
    }

    /// <summary>
    /// Every cell lands in exactly one shard, and the shards partition the product — a cell in no
    /// shard is a combination CI never runs, which is the silent gap this whole file exists to stop.
    /// </summary>
    [Fact]
    public void Shards_PartitionEveryCellExactlyOnce()
    {
        var sharded = Enum.GetValues<MatrixShard>()
            .SelectMany(ProviderMatrix.CellsFor)
            .ToArray();

        Assert.Equal(ProviderMatrix.AllCells.Count, sharded.Length);
        Assert.Equal(ProviderMatrix.AllCells.Count, sharded.Distinct().Count());
        Assert.Empty(ProviderMatrix.AllCells.Except(sharded));
    }

    /// <summary>
    /// Every shard has a test class that runs it. Without this, deleting a class (or forgetting one
    /// for a new shard) removes a fifth of the product from CI while the suite still reports green.
    /// </summary>
    [Fact]
    public void EveryShard_HasATestClassCarryingItsBatchTrait()
    {
        var traitsInUse = typeof(MatrixCompletenessTests).Assembly
            .GetTypes()
            .Where(type => type is { IsAbstract: false, IsPublic: true })
            .SelectMany(type => type.GetCustomAttributesData())
            .Where(attribute => attribute.AttributeType.Name == nameof(TraitAttribute))
            .Where(attribute => attribute.ConstructorArguments.Count == 2)
            .Where(attribute => (string?)attribute.ConstructorArguments[0].Value == Batches.Trait)
            .Select(attribute => (string?)attribute.ConstructorArguments[1].Value)
            .ToHashSet(StringComparer.Ordinal);

        var missing = Enum.GetValues<MatrixShard>()
            .Select(shard => $"matrix-{ProviderMatrix.TraitValueOf(shard)}")
            .Where(trait => !traitsInUse.Contains(trait))
            .ToArray();

        Assert.True(
            missing.Length == 0,
            "Every cross-product shard needs a test class tagged with its batch trait, or CI's matrix "
            + $"leg for it runs nothing. Missing: {string.Join(", ", missing)}");
    }

    /// <summary>
    /// The batch constants are spelled out (trait values must be compile-time constants) while the
    /// shard names are computed. This is the tie that keeps the two from drifting.
    /// </summary>
    [Fact]
    public void BatchConstants_MatchTheShardNames()
    {
        var constants = typeof(Batches)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(field => field.IsLiteral && field.Name.StartsWith("Matrix", StringComparison.Ordinal))
            .Select(field => (string)field.GetRawConstantValue()!)
            .ToHashSet(StringComparer.Ordinal);

        var expected = Enum.GetValues<MatrixShard>()
            .Select(shard => $"matrix-{ProviderMatrix.TraitValueOf(shard)}")
            .ToHashSet(StringComparer.Ordinal);

        Assert.Equal(expected, constants);
    }

    /// <summary>
    /// Reflects over the shipped package assemblies for public <c>With…{suffix}</c> extension methods
    /// on the registration builder and asserts the matrix axis names one per method. Matching on the
    /// method name is deliberate: it is the surface an application actually calls, so the guard tracks
    /// what is shippable rather than what happens to be in an enum.
    /// </summary>
    private static void AssertAxisCoversRegistrations(string suffix, IEnumerable<string> axis, string axisName)
    {
        var registrations = ProviderAssemblies()
            .SelectMany(assembly => assembly.GetTypes())
            .Where(type => type is { IsAbstract: true, IsSealed: true, IsPublic: true }) // static classes
            .SelectMany(type => type.GetMethods(BindingFlags.Public | BindingFlags.Static))
            .Where(method => method.GetParameters() is [{ ParameterType.Name: nameof(AsyncResponseRegistrationBuilder) }, ..])
            .Select(method => method.Name)
            .Where(name => name.StartsWith("With", StringComparison.Ordinal)
                && name.EndsWith(suffix, StringComparison.Ordinal))
            // WithDurableFlow<TFlow, TInput> registers a flow class, not a store; WithDurableFlows<T>
            // is the raw escape hatch for a custom IFlowStateStore. Neither names a shipped provider.
            .Where(name => name != $"With{suffix}" && name != $"With{suffix}s")
            .Select(name => name[4..^suffix.Length])
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        // A reflection guard that finds nothing passes for the wrong reason. Provider assemblies load
        // lazily, so touch one type per package first (see ForceProviderAssemblyLoad) and then insist
        // the scan actually found the shipped registrations.
        Assert.True(
            registrations.Count >= 5,
            $"Only {registrations.Count} With…{suffix} registrations were discovered, which means the "
            + "reflection scan — not the library — is what this guard is measuring.");

        // Every shipped registration must have an axis member. The reverse does not hold: InMemory
        // has no With…Channel/Transport suffix pattern of its own to match on in every case, and is
        // asserted by the containerless cells instead.
        var axisNames = axis.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var uncovered = registrations
            .Where(name => !axisNames.Contains(name) && !axisNames.Contains(Normalize(name)))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.True(
            uncovered.Length == 0,
            $"Every shipped With…{suffix} registration needs a {axisName} member, or its provider is "
            + $"absent from the cross product. Uncovered: {string.Join(", ", uncovered)}");
    }

    /// <summary>
    /// Package names and enum members differ in casing conventions (<c>WithSqsTransport</c> against
    /// <c>Sqs</c>, <c>WithNatsChannel</c> against <c>Nats</c>, <c>WithPostgreSql…</c> against
    /// <c>PostgreSql</c>), so the comparison is case-insensitive with the few spelled-out aliases here.
    /// </summary>
    private static string Normalize(string name) => name switch
    {
        "RabbitMq" => nameof(MatrixTransport.RabbitMq),
        "MongoDb" => nameof(MatrixChannel.MongoDb),
        "DynamoDb" => nameof(MatrixStore.DynamoDb),
        _ => name
    };

    /// <summary>
    /// The shipped provider assemblies, discovered through their registration builder rather than a
    /// hard-coded list — a new package is picked up as soon as the test project references it.
    /// </summary>
    private static IEnumerable<Assembly> ProviderAssemblies()
    {
        ForceProviderAssemblyLoad();
        return AppDomain.CurrentDomain.GetAssemblies()
            .Where(assembly => assembly.GetName().Name?.StartsWith("AsyncResponse.", StringComparison.Ordinal) == true)
            .Where(assembly => !assembly.GetName().Name!.Contains("Tests", StringComparison.Ordinal));
    }

    /// <summary>
    /// The runtime loads an assembly on first use, so a scan of <see cref="AppDomain.GetAssemblies"/>
    /// sees only what earlier code happened to touch. Naming one options type per package forces every
    /// provider assembly in — otherwise the guards would silently measure load order.
    /// </summary>
    private static void ForceProviderAssemblyLoad()
    {
        Type[] anchors =
        [
            typeof(Channels.Redis.RedisAsyncResponseOptions),
            typeof(Channels.NATS.NatsAsyncResponseChannelOptions),
            typeof(Channels.PostgreSQL.PostgreSqlAsyncResponseChannelOptions),
            typeof(Channels.SqlServer.SqlServerAsyncResponseChannelOptions),
            typeof(Channels.MongoDB.MongoDbAsyncResponseChannelOptions),
            typeof(Transports.Redis.RedisAsyncResponseTransportOptions),
            typeof(Transports.NATS.NatsAsyncResponseTransportOptions),
            typeof(Transports.PostgreSQL.PostgreSqlAsyncResponseTransportOptions),
            typeof(Transports.SqlServer.SqlServerAsyncResponseTransportOptions),
            typeof(Transports.MongoDB.MongoDbAsyncResponseTransportOptions),
            typeof(Transports.Kafka.KafkaAsyncResponseTransportOptions),
            typeof(Transports.RabbitMQ.RabbitMqAsyncResponseOptions),
            typeof(Transports.SQS.SqsAsyncResponseOptions),
            typeof(Transports.AzureServiceBus.AzureServiceBusAsyncResponseOptions),
            typeof(Transports.GooglePubSub.GooglePubSubAsyncResponseOptions),
            typeof(DurableFlows.Sqlite.SqliteDurableFlowOptions),
            typeof(DurableFlows.PostgreSQL.PostgreSqlDurableFlowOptions),
            typeof(DurableFlows.SqlServer.SqlServerDurableFlowOptions),
            typeof(DurableFlows.MySql.MySqlDurableFlowOptions),
            typeof(DurableFlows.MongoDB.MongoDbDurableFlowOptions),
            typeof(DurableFlows.DynamoDB.DynamoDbDurableFlowOptions),
            typeof(DurableFlows.EFCore.EFCoreDurableFlowOptions),
            typeof(DurableFlows.Cosmos.CosmosDurableFlowOptions),
            typeof(DurableFlows.Oracle.OracleDurableFlowOptions)
        ];

        // Referencing the array is enough: the typeof expressions already forced each load.
        Assert.Equal(24, anchors.Length);
    }
}
