using Microsoft.Data.SqlClient;
using Npgsql;
using Xunit;

// Batches must not overlap in time: each one boots its own AppHost, and running two collections
// concurrently would stand every batch's containers up at once — the opposite of the point. xUnit
// parallelizes collections by default, so this is what makes batching mean anything.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace AsyncResponse.IntegrationTests;

// The suite splits along the one structural line that actually exists: a test either drives a sample
// app over HTTP, or it drives a driver directly. The direct half — 170 of the 315 tests — needs no
// sample app at all, so those batches start zero processes. The app-driven half splits by family.
//
// Batches are balanced on MEASURED MEMORY, not container count. Counting containers is misleading:
// the first cut had a 7-container batch at 5.8 GiB next to an 8-container batch at 2.5 GiB, and the
// 5.8 GiB one failed wholesale whenever anything else used the machine. Four containers dominate
// everything else — oracle 2,180 MiB, the two SQL Servers ~1,328 MiB each, cosmos 1,031 MiB — so the
// split is mostly about keeping those apart. Oracle and SQL Server are also capped in the AppHost.
//
//   data           8 containers,  9 apps, 224 tests   every database-backed test
//   oracle-cosmos  2 containers,  0 apps,   2 tests   the two heavyweight stores, isolated
//   brokers        5 containers, 10 apps,  55 tests   message brokers (all small)
//   cloud          4 containers,  4 apps,  18 tests   Service Bus + SQS emulators
//
// Tests needing no AppHost (in-memory suite, AOT publish gate, these guards) belong to no batch and
// boot nothing at all.

/// <summary>
/// Shared by the batches with no sample apps. Those apps normally do work on the suite's behalf —
/// waiting out a server's startup and provisioning SQL Server's database — so a batch without them
/// has to do it itself, or every test races an unready server.
/// </summary>
public abstract class DriverOnlyBatchFixture : IntegrationFixture
{
    private static readonly TimeSpan ProvisionBudget = TimeSpan.FromMinutes(3);
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(2);

    private protected async Task WaitForPostgreSqlAsync()
        => await EventuallyAsync(async () =>
        {
            await using var connection = new NpgsqlConnection(PostgreSqlConnectionString);
            await connection.OpenAsync();
        });

    /// <summary>
    /// SQL Server has no equivalent of POSTGRES_DB / MYSQL_DATABASE, so the container starts with no
    /// <c>asyncresponse</c> database. The sample app's SqlServerDatabaseProvisioner normally creates
    /// it at startup; with no sample app in this batch, we create it — and the retry doubles as the
    /// readiness wait, since SQL Server takes the longest of any container here to accept logins.
    /// </summary>
    private protected async Task ProvisionSqlServerDatabaseAsync()
    {
        var builder = new SqlConnectionStringBuilder(SqlServerConnectionString);
        var database = builder.InitialCatalog;
        builder.InitialCatalog = "master";

        await EventuallyAsync(async () =>
        {
            await using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"IF DB_ID(N'{database.Replace("'", "''")}') IS NULL CREATE DATABASE [{database.Replace("]", "]]")}];";
            await command.ExecuteNonQueryAsync();
        });
    }

    /// <summary>Retries until the action succeeds or the budget expires, then lets the failure through.</summary>
    private protected static async Task EventuallyAsync(Func<Task> action)
    {
        var deadline = DateTimeOffset.UtcNow + ProvisionBudget;
        while (true)
        {
            try
            {
                await action();
                return;
            }
            catch when (DateTimeOffset.UtcNow < deadline)
            {
                await Task.Delay(RetryDelay);
            }
        }
    }
}

/// <summary>
/// Everything that talks to a database: channel conformance, store contracts, the "direct" driver
/// tests, and the database channel/transport SUTs. One batch rather than three, because all three
/// wanted the same servers — and SQL Server, the slowest container in the suite to accept logins, was
/// paying that cost once per batch for no benefit.
/// </summary>
public sealed class DataBatchFixture : DriverOnlyBatchFixture
{
    protected override string Batch => "data";

    protected override async ValueTask WireAsync()
    {
        await WireRedisConnectionStringAsync();
        WireNatsConnectionString();
        WirePostgreSqlConnectionString();
        WireMySqlConnectionString();
        WireMongoDbConnectionString();
        WireSqlServerConnectionString();
        WireLocalStackServiceUrl();

        // The SUT apps below provision the SQL Server database themselves, but the driver-level tests
        // in this batch do not wait for any app — so the batch still owns readiness.
        await WaitForPostgreSqlAsync();
        await ProvisionSqlServerDatabaseAsync();

        var clients = await WireAppsAsync(
            "itest-app",
            "itest-app-postgresql",
            "itest-app-postgresql-early-ack",
            "itest-app-sqlserver",
            "itest-app-sqlserver-early-ack",
            "itest-app-mongodb",
            "itest-app-mongodb-early-ack",
            "itest-app-nats",
            "itest-app-sqs");

        Client = clients[0];
        PostgreSqlClient = clients[1];
        PostgreSqlEarlyAckClient = clients[2];
        SqlServerClient = clients[3];
        SqlServerEarlyAckClient = clients[4];
        MongoDbClient = clients[5];
        MongoDbEarlyAckClient = clients[6];
        NatsClient = clients[7];
        SqsClient = clients[8];
    }
}

/// <summary>
/// Oracle and Cosmos, alone. Together they measured 3.2 GiB — more than half a default Docker VM —
/// against exactly two tests, so they get a batch to themselves rather than making every other store
/// test share a fleet that cannot reliably start.
/// </summary>
public sealed class OracleCosmosBatchFixture : IntegrationFixture
{
    protected override string Batch => "oracle-cosmos";

    protected override ValueTask WireAsync()
    {
        // Both tests wait for their own server: Oracle takes minutes to provision its database on a
        // cold container, so there is nothing useful for the fixture to wait on here.
        WireOracleAndCosmosConnectionStrings();
        return ValueTask.CompletedTask;
    }
}

/// <summary>Message-broker transports, driven through sample apps.</summary>
public sealed class BrokersBatchFixture : IntegrationFixture
{
    protected override string Batch => "brokers";

    protected override async ValueTask WireAsync()
    {
        // The Redis-compatibility profile replaces this batch's fleet with Redis alone; CI filters the
        // run to the two Redis classes, so nothing else in the batch is touched.
        if (string.Equals(Env("ASYNCRESPONSE_ITEST_PROFILE", ""), "redis-compat", StringComparison.OrdinalIgnoreCase))
        {
            var redisOnly = await WireAppsAsync("itest-app-redis", "itest-app-redis-early-ack");

            // RedisChannelTests use the fixture's default client; in the focused profile the Redis
            // channel is paired with the Redis transport instead of booting Pub/Sub just for that role.
            Client = redisOnly[0];
            RedisTransportClient = redisOnly[0];
            RedisTransportEarlyAckClient = redisOnly[1];
            return;
        }

        var clients = await WireAppsAsync(
            "itest-app",
            "itest-app-early-ack",
            "itest-app-rabbitmq",
            "itest-app-rabbitmq-early-ack",
            "itest-app-kafka",
            "itest-app-kafka-early-ack",
            "itest-app-nats",
            "itest-app-nats-early-ack",
            "itest-app-redis",
            "itest-app-redis-early-ack");

        Client = clients[0];
        EarlyAckClient = clients[1];
        RabbitMqClient = clients[2];
        RabbitMqEarlyAckClient = clients[3];
        KafkaClient = clients[4];
        KafkaEarlyAckClient = clients[5];
        NatsClient = clients[6];
        NatsEarlyAckClient = clients[7];
        RedisTransportClient = clients[8];
        RedisTransportEarlyAckClient = clients[9];
    }
}

/// <summary>
/// The Azure Service Bus and SQS emulators. Split out of the brokers batch because the Service Bus
/// emulator brings its own full SQL Server — ~1.3 GiB for a message broker — which made brokers the
/// heaviest app-driven batch by itself.
/// </summary>
public sealed class CloudBatchFixture : IntegrationFixture
{
    protected override string Batch => "cloud";

    protected override async ValueTask WireAsync()
    {
        var clients = await WireAppsAsync(
            "itest-app-azure-servicebus",
            "itest-app-azure-servicebus-early-ack",
            "itest-app-sqs",
            "itest-app-sqs-early-ack");

        AzureServiceBusClient = clients[0];
        AzureServiceBusEarlyAckClient = clients[1];
        SqsClient = clients[2];
        SqsEarlyAckClient = clients[3];
    }
}

/// <summary>Everything database-backed. See <see cref="DataBatchFixture"/>.</summary>
[CollectionDefinition(Name)]
public sealed class DataCollection : ICollectionFixture<DataBatchFixture>
{
    public const string Name = "AsyncResponse data";
}

/// <summary>The two heavyweight stores. See <see cref="OracleCosmosBatchFixture"/>.</summary>
[CollectionDefinition(Name)]
public sealed class OracleCosmosCollection : ICollectionFixture<OracleCosmosBatchFixture>
{
    public const string Name = "AsyncResponse oracle-cosmos";
}

/// <summary>Service Bus and SQS emulators. See <see cref="CloudBatchFixture"/>.</summary>
[CollectionDefinition(Name)]
public sealed class CloudCollection : ICollectionFixture<CloudBatchFixture>
{
    public const string Name = "AsyncResponse cloud";
}

/// <summary>Message-broker transports. See <see cref="BrokersBatchFixture"/>.</summary>
[CollectionDefinition(Name)]
public sealed class BrokersCollection : ICollectionFixture<BrokersBatchFixture>
{
    public const string Name = "AsyncResponse brokers";
}

