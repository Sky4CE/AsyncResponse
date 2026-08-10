using Aspire.Hosting;
using Aspire.Hosting.Testing;
using AsyncResponse.Conformance;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Fixtures for the provider cross-product shards. A shard boots the containers its own cells touch
/// and starts no sample app at all — every matrix cell builds its DI provider inside the test
/// process, the same way <see cref="ChannelConformanceTests"/> already does for the channel contract.
/// <para>
/// The five channel containers are unconditional because the channel axis is complete in every shard;
/// what varies is the broker family the transports need and whether Oracle or Cosmos is up for the
/// store axis. See <see cref="ProviderMatrix.ShardOf"/> for the partition and the AppHost's
/// <c>AddMatrixFleet</c> for the fleets.
/// </para>
/// </summary>
public abstract class MatrixBatchFixture : DriverOnlyBatchFixture
{
    /// <summary>Which slice of the cross product this fixture serves.</summary>
    protected abstract MatrixShard Shard { get; }

    /// <inheritdoc />
    protected override string Batch => $"matrix-{ProviderMatrix.TraitValueOf(Shard)}";

    /// <summary>True when the shard's transports include Kafka and RabbitMQ.</summary>
    private bool NeedsBrokers => Shard is MatrixShard.BrokerLight or MatrixShard.BrokerOracle or MatrixShard.BrokerCosmos;

    /// <summary>True when the shard's transports include SQS, Service Bus, and Pub/Sub.</summary>
    private bool NeedsCloud => Shard is MatrixShard.CloudLight or MatrixShard.CloudOracle or MatrixShard.CloudCosmos;

    /// <summary>True when the shard runs the ordinary stores (MySQL and the DynamoDB LocalStack table).</summary>
    private bool NeedsLightStores => Shard is MatrixShard.DatabaseLight or MatrixShard.BrokerLight or MatrixShard.CloudLight;

    /// <summary>
    /// True when the shard's store axis is Oracle. Only the Oracle shards declare that container, and
    /// asking Aspire for an endpoint a batch never declared throws — which is the intended signal that
    /// a cell landed in the wrong shard, so the wiring has to be conditional.
    /// </summary>
    private bool NeedsOracle => Shard is MatrixShard.DatabaseOracle or MatrixShard.BrokerOracle or MatrixShard.CloudOracle;

    /// <summary>True when the shard's store axis is Cosmos. Never true at the same time as Oracle.</summary>
    private bool NeedsCosmos => Shard is MatrixShard.DatabaseCosmos or MatrixShard.BrokerCosmos or MatrixShard.CloudCosmos;

    /// <inheritdoc />
    protected override async ValueTask WireAsync()
    {
        await WireRedisConnectionStringAsync();
        WireNatsConnectionString();
        WirePostgreSqlConnectionString();
        WireSqlServerConnectionString();
        WireMongoDbConnectionString();

        // No sample app runs in a matrix shard, so the readiness work those apps normally do on the
        // suite's behalf — waiting out PostgreSQL, creating the SQL Server database — belongs here.
        await WaitForPostgreSqlAsync();
        await ProvisionSqlServerDatabaseAsync();

        // Each block wires only what its shard declared. The base fixture assembles these into
        // MatrixBackends, leaving anything unwired null — and a cell reaching for a null backend names
        // it in the failure, which is the signal that it landed in the wrong shard.
        if (NeedsBrokers)
            await WireBrokerConnectionStringsAsync();

        if (NeedsCloud)
        {
            WireAzureServiceBusConnectionString();
            WirePubSubEmulator(Batches.PubSubProjectIdValue);
            WireLocalStackServiceUrl();
        }

        if (NeedsLightStores)
        {
            WireMySqlConnectionString();
            if (!NeedsCloud)
                WireLocalStackServiceUrl(); // the DynamoDB store; the cloud shards already have it
        }

        if (NeedsOracle)
            WireOracleConnectionString();

        if (NeedsCosmos)
            WireCosmosConnectionString();
    }
}

public sealed class MatrixDatabaseLightFixture : MatrixBatchFixture
{
    protected override MatrixShard Shard => MatrixShard.DatabaseLight;
}

public sealed class MatrixBrokerLightFixture : MatrixBatchFixture
{
    protected override MatrixShard Shard => MatrixShard.BrokerLight;
}

public sealed class MatrixCloudLightFixture : MatrixBatchFixture
{
    protected override MatrixShard Shard => MatrixShard.CloudLight;
}

public sealed class MatrixDatabaseOracleFixture : MatrixBatchFixture
{
    protected override MatrixShard Shard => MatrixShard.DatabaseOracle;
}

public sealed class MatrixBrokerOracleFixture : MatrixBatchFixture
{
    protected override MatrixShard Shard => MatrixShard.BrokerOracle;
}

public sealed class MatrixCloudOracleFixture : MatrixBatchFixture
{
    protected override MatrixShard Shard => MatrixShard.CloudOracle;
}

public sealed class MatrixDatabaseCosmosFixture : MatrixBatchFixture
{
    protected override MatrixShard Shard => MatrixShard.DatabaseCosmos;
}

public sealed class MatrixBrokerCosmosFixture : MatrixBatchFixture
{
    protected override MatrixShard Shard => MatrixShard.BrokerCosmos;
}

public sealed class MatrixCloudCosmosFixture : MatrixBatchFixture
{
    protected override MatrixShard Shard => MatrixShard.CloudCosmos;
}

[CollectionDefinition(Name)]
public sealed class MatrixDatabaseLightCollection : ICollectionFixture<MatrixDatabaseLightFixture>
{
    public const string Name = "AsyncResponse matrix database-light";
}

[CollectionDefinition(Name)]
public sealed class MatrixBrokerLightCollection : ICollectionFixture<MatrixBrokerLightFixture>
{
    public const string Name = "AsyncResponse matrix broker-light";
}

[CollectionDefinition(Name)]
public sealed class MatrixCloudLightCollection : ICollectionFixture<MatrixCloudLightFixture>
{
    public const string Name = "AsyncResponse matrix cloud-light";
}

[CollectionDefinition(Name)]
public sealed class MatrixDatabaseOracleCollection : ICollectionFixture<MatrixDatabaseOracleFixture>
{
    public const string Name = "AsyncResponse matrix database-oracle";
}

[CollectionDefinition(Name)]
public sealed class MatrixBrokerOracleCollection : ICollectionFixture<MatrixBrokerOracleFixture>
{
    public const string Name = "AsyncResponse matrix broker-oracle";
}

[CollectionDefinition(Name)]
public sealed class MatrixCloudOracleCollection : ICollectionFixture<MatrixCloudOracleFixture>
{
    public const string Name = "AsyncResponse matrix cloud-oracle";
}

[CollectionDefinition(Name)]
public sealed class MatrixDatabaseCosmosCollection : ICollectionFixture<MatrixDatabaseCosmosFixture>
{
    public const string Name = "AsyncResponse matrix database-cosmos";
}

[CollectionDefinition(Name)]
public sealed class MatrixBrokerCosmosCollection : ICollectionFixture<MatrixBrokerCosmosFixture>
{
    public const string Name = "AsyncResponse matrix broker-cosmos";
}

[CollectionDefinition(Name)]
public sealed class MatrixCloudCosmosCollection : ICollectionFixture<MatrixCloudCosmosFixture>
{
    public const string Name = "AsyncResponse matrix cloud-cosmos";
}
