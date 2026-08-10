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

        await WaitForBackendsAsync();
    }

    /// <summary>
    /// Blocks until every backend this shard wired actually answers. This is the barrier the
    /// app-driven batches get for free and a matrix shard does not: their sample apps <c>WaitFor</c>
    /// the containers, and waiting for the apps to report healthy transitively waits for the servers.
    /// A shard starts no app, so without this the first cells run against containers that have a port
    /// open and nothing behind it.
    /// <para>
    /// That is not hypothetical — it is what CI failed on: the Cosmos emulator answers its gateway
    /// while still replying <c>503 pgcosmos extension is still starting</c>, MySQL accepts a socket
    /// before completing its handshake, NATS serves core requests before JetStream's API responds, and
    /// the Service Bus emulator opens AMQP before its entities exist.
    /// </para>
    /// </summary>
    private async Task WaitForBackendsAsync()
    {
        // Oracle and the Cosmos emulator are minutes-scale on a cold container; everything else is
        // seconds. See DriverOnlyBatchFixture.EventuallyAsync for the default.
        var heavyStoreBudget = TimeSpan.FromMinutes(6);

        // The five channel backends every shard declares.
        await EventuallyAsync(async () =>
        {
            await using var connection = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(
                RedisConnectionString);
            await connection.GetDatabase().PingAsync();
        });

        await EventuallyAsync(async () =>
        {
            await using var connection = new NATS.Client.Core.NatsConnection(
                new NATS.Client.Core.NatsOpts { Url = NatsConnectionString });
            await connection.ConnectAsync();

            // JetStream specifically: the NATS transport creates streams through the JetStream API,
            // which starts answering later than the core protocol does.
            var jetStream = new NATS.Client.JetStream.NatsJSContext(connection);
            await foreach (var _ in jetStream.ListStreamNamesAsync())
                break;
        });

        await EventuallyAsync(async () =>
        {
            var client = new MongoDB.Driver.MongoClient(MongoDbConnectionString);
            using var cursor = await client.ListDatabaseNamesAsync();
            _ = await cursor.MoveNextAsync();
        });

        // PostgreSQL readiness and the SQL Server database are handled by the base fixture above.

        if (NeedsBrokers)
        {
            await EventuallyAsync(async () =>
            {
                var factory = new RabbitMQ.Client.ConnectionFactory
                {
                    Uri = new Uri(RabbitMqConnectionString!)
                };
                await using var connection = await factory.CreateConnectionAsync();
            });

            // Kafka: creating and dropping nothing, just resolving metadata, proves the broker is
            // past its KRaft startup rather than merely listening.
            await EventuallyAsync(() =>
            {
                using var admin = new Confluent.Kafka.AdminClientBuilder(
                    new Confluent.Kafka.AdminClientConfig { BootstrapServers = KafkaBootstrapServers }).Build();
                admin.GetMetadata(TimeSpan.FromSeconds(10));
                return Task.CompletedTask;
            });
        }

        if (NeedsCloud)
        {
            await EventuallyAsync(async () =>
            {
                await using var client = new Azure.Messaging.ServiceBus.ServiceBusClient(
                    AzureServiceBusConnectionString);
                await using var receiver = client.CreateReceiver("arm-asb-worker-00");
                await receiver.PeekMessageAsync();
            });

            await EventuallyAsync(async () =>
            {
                var publisher = await new Google.Cloud.PubSub.V1.PublisherServiceApiClientBuilder
                {
                    EmulatorDetection = Google.Api.Gax.EmulatorDetection.EmulatorOnly
                }.BuildAsync();
                await foreach (var _ in publisher.ListTopicsAsync(
                    new Google.Cloud.PubSub.V1.ListTopicsRequest { Project = $"projects/{PubSubProjectId}" }))
                {
                    break;
                }
            });
        }

        if (NeedsCloud || NeedsLightStores)
            await EventuallyAsync(() => WaitForLocalStackAsync());

        if (NeedsLightStores)
        {
            await EventuallyAsync(async () =>
            {
                await using var connection = new MySqlConnector.MySqlConnection(MySqlConnectionString);
                await connection.OpenAsync();
            });
        }

        if (NeedsOracle && Environment.GetEnvironmentVariable("ASYNCRESPONSE_ITEST_ORACLE_CONNECTION_STRING") is { Length: > 0 } oracle)
        {
            await EventuallyAsync(async () =>
            {
                await using var connection = new Oracle.ManagedDataAccess.Client.OracleConnection(oracle);
                await connection.OpenAsync();
            }, heavyStoreBudget);
        }

        if (NeedsCosmos && Environment.GetEnvironmentVariable("ASYNCRESPONSE_ITEST_COSMOS_CONNECTION_STRING") is { Length: > 0 } cosmos)
        {
            await EventuallyAsync(async () =>
            {
                using var client = new Microsoft.Azure.Cosmos.CosmosClient(cosmos, CosmosReadinessOptions());

                // ReadAccountAsync is not enough on its own: the emulator answers it while the
                // pgcosmos extension is still starting and every write then fails with a 503. Creating
                // a database is the first operation a cell actually performs.
                var probe = $"arm_ready_{Guid.NewGuid():N}";
                await client.CreateDatabaseIfNotExistsAsync(probe);
                await client.GetDatabase(probe).DeleteAsync();
            }, heavyStoreBudget);
        }
    }

    /// <summary>LocalStack backs both the SQS transport and the DynamoDB store.</summary>
    private async Task WaitForLocalStackAsync()
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
        var health = await http.GetStringAsync($"{LocalStackServiceUrl}/_localstack/health");
        if (!health.Contains("\"sqs\": \"available\"", StringComparison.Ordinal) &&
            !health.Contains("\"sqs\": \"running\"", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"LocalStack SQS is not available yet: {health}");
        }
    }

    /// <summary>Gateway mode pinned to the endpoint, accepting the emulator's self-signed certificate.</summary>
    private static Microsoft.Azure.Cosmos.CosmosClientOptions CosmosReadinessOptions() => new()
    {
        ConnectionMode = Microsoft.Azure.Cosmos.ConnectionMode.Gateway,
        LimitToEndpoint = true,
        HttpClientFactory = () => new HttpClient(new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
        })
    };
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
