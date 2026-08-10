using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace AsyncResponse.Conformance;

/// <summary>
/// The response channels the library ships. One entry per <c>With…Channel</c> registration, so a new
/// channel package fails the matrix-completeness guard until it is listed here.
/// </summary>
public enum MatrixChannel
{
    InMemory,
    Redis,
    Nats,
    PostgreSql,
    SqlServer,
    MongoDb
}

/// <summary>The worker transports the library ships. One entry per <c>With…Transport</c> registration.</summary>
public enum MatrixTransport
{
    InMemory,
    Redis,
    Nats,
    PostgreSql,
    SqlServer,
    MongoDb,
    Kafka,
    RabbitMq,
    Sqs,
    AzureServiceBus,
    GooglePubSub
}

/// <summary>The durable-flow state stores the library ships. One entry per <c>With…DurableFlows</c> registration.</summary>
public enum MatrixStore
{
    InMemory,
    Sqlite,
    PostgreSql,
    SqlServer,
    MySql,
    MongoDb,
    DynamoDb,
    EFCore,
    Cosmos,
    Oracle
}

/// <summary>
/// One point in the cross product: the three provider choices an application makes independently.
/// A cell is the unit the combination suite runs — build a host wired exactly this way, drive a real
/// flow through it, assert the outcome.
/// </summary>
public readonly record struct MatrixCell(MatrixChannel Channel, MatrixTransport Transport, MatrixStore Store)
{
    /// <summary>
    /// Stable, human-readable identity. This is what xUnit prints for a failing theory case, so it
    /// must name the combination precisely enough to reproduce it by hand.
    /// </summary>
    public override string ToString() => $"{Channel}+{Transport}+{Store}";

    /// <summary>
    /// A short, deterministic token derived from the cell identity, used to build isolated namespaces
    /// (schema, key prefix, topic, collection, table). Deterministic rather than random so a failing
    /// cell leaves a namespace you can go look at, and so a rerun of one cell reuses its own objects
    /// instead of leaking a new set every time.
    /// </summary>
    public string Token
    {
        get
        {
            var digest = SHA256.HashData(Encoding.UTF8.GetBytes(ToString()));
            return Convert.ToHexString(digest, 0, 5).ToLowerInvariant();
        }
    }
}

/// <summary>
/// The container fleet a group of cells needs. Cells are partitioned into shards so that no single
/// CI leg boots more of the fleet than its own cells actually touch — the whole matrix at once is
/// roughly 9 GiB of containers, and <see cref="AsyncResponse.IntegrationTests"/>' batching exists
/// because the fleet already failed wholesale at 5.8 GiB.
/// <para>
/// The partition is two-dimensional: the transport family decides which brokers boot, the store
/// family decides whether the two heavyweight stores boot. Every shard also needs the channel
/// containers (Redis, NATS, PostgreSQL, SQL Server, MongoDB), because the channel axis is complete
/// within every shard.
/// </para>
/// </summary>
public enum MatrixShard
{
    /// <summary>Database-and-in-process transports, ordinary stores. The largest shard.</summary>
    DatabaseLight,

    /// <summary>Kafka + RabbitMQ transports, ordinary stores.</summary>
    BrokerLight,

    /// <summary>SQS + Service Bus + Pub/Sub transports, ordinary stores.</summary>
    CloudLight,

    /// <summary>Database-and-in-process transports, Oracle store.</summary>
    DatabaseOracle,

    /// <summary>Kafka + RabbitMQ transports, Oracle store.</summary>
    BrokerOracle,

    /// <summary>SQS + Service Bus + Pub/Sub transports, Oracle store.</summary>
    CloudOracle,

    /// <summary>Database-and-in-process transports, Cosmos store.</summary>
    DatabaseCosmos,

    /// <summary>Kafka + RabbitMQ transports, Cosmos store.</summary>
    BrokerCosmos,

    /// <summary>SQS + Service Bus + Pub/Sub transports, Cosmos store.</summary>
    CloudCosmos
}

/// <summary>
/// The provider cross product, enumerated once so every consumer — the combination suite, the
/// per-shard test classes, and the completeness guard — agrees on what "every combination" means.
/// </summary>
public static class ProviderMatrix
{
    /// <summary>Every channel the library ships.</summary>
    public static IReadOnlyList<MatrixChannel> Channels { get; } = Enum.GetValues<MatrixChannel>();

    /// <summary>Every transport the library ships.</summary>
    public static IReadOnlyList<MatrixTransport> Transports { get; } = Enum.GetValues<MatrixTransport>();

    /// <summary>Every durable-flow store the library ships.</summary>
    public static IReadOnlyList<MatrixStore> Stores { get; } = Enum.GetValues<MatrixStore>();

    /// <summary>The complete cross product: every channel against every transport against every store.</summary>
    public static IReadOnlyList<MatrixCell> AllCells { get; } =
    [
        .. from channel in Channels
           from transport in Transports
           from store in Stores
           select new MatrixCell(channel, transport, store)
    ];

    /// <summary>The cells belonging to one shard, in a stable order.</summary>
    public static IReadOnlyList<MatrixCell> CellsFor(MatrixShard shard)
        => [.. AllCells.Where(cell => ShardOf(cell) == shard)];

    /// <summary>
    /// Which shard a cell runs in. Transport family picks the broker containers; store family picks
    /// whether Oracle or Cosmos — by far the two largest images in the fleet — have to be up.
    /// </summary>
    public static MatrixShard ShardOf(MatrixCell cell)
        => (TransportFamilyOf(cell.Transport), StoreFamilyOf(cell.Store)) switch
        {
            (TransportFamily.Database, StoreFamily.Light) => MatrixShard.DatabaseLight,
            (TransportFamily.Broker, StoreFamily.Light) => MatrixShard.BrokerLight,
            (TransportFamily.Cloud, StoreFamily.Light) => MatrixShard.CloudLight,
            (TransportFamily.Database, StoreFamily.Oracle) => MatrixShard.DatabaseOracle,
            (TransportFamily.Broker, StoreFamily.Oracle) => MatrixShard.BrokerOracle,
            (TransportFamily.Cloud, StoreFamily.Oracle) => MatrixShard.CloudOracle,
            (TransportFamily.Database, StoreFamily.Cosmos) => MatrixShard.DatabaseCosmos,
            (TransportFamily.Broker, StoreFamily.Cosmos) => MatrixShard.BrokerCosmos,
            (TransportFamily.Cloud, StoreFamily.Cosmos) => MatrixShard.CloudCosmos,
            _ => throw new ArgumentOutOfRangeException(nameof(cell), cell, "Unclassified matrix cell.")
        };

    /// <summary>
    /// The trait value CI filters a leg by (<c>--filter-trait "matrix=database-light"</c>). Kebab-case
    /// to match the existing <c>batch</c> trait values.
    /// </summary>
    public static string TraitValueOf(MatrixShard shard) => shard switch
    {
        MatrixShard.DatabaseLight => "database-light",
        MatrixShard.BrokerLight => "broker-light",
        MatrixShard.CloudLight => "cloud-light",
        MatrixShard.DatabaseOracle => "database-oracle",
        MatrixShard.BrokerOracle => "broker-oracle",
        MatrixShard.CloudOracle => "cloud-oracle",
        MatrixShard.DatabaseCosmos => "database-cosmos",
        MatrixShard.BrokerCosmos => "broker-cosmos",
        MatrixShard.CloudCosmos => "cloud-cosmos",
        _ => throw new ArgumentOutOfRangeException(nameof(shard), shard, null)
    };

    /// <summary>The trait name CI filters on, alongside the existing <c>batch</c> trait.</summary>
    public const string Trait = "matrix";

    private enum TransportFamily { Database, Broker, Cloud }

    private enum StoreFamily { Light, Oracle, Cosmos }

    private static TransportFamily TransportFamilyOf(MatrixTransport transport) => transport switch
    {
        MatrixTransport.InMemory or MatrixTransport.Redis or MatrixTransport.Nats or
        MatrixTransport.PostgreSql or MatrixTransport.SqlServer or MatrixTransport.MongoDb
            => TransportFamily.Database,
        MatrixTransport.Kafka or MatrixTransport.RabbitMq => TransportFamily.Broker,
        MatrixTransport.Sqs or MatrixTransport.AzureServiceBus or MatrixTransport.GooglePubSub
            => TransportFamily.Cloud,
        _ => throw new ArgumentOutOfRangeException(nameof(transport), transport, null)
    };

    private static StoreFamily StoreFamilyOf(MatrixStore store) => store switch
    {
        MatrixStore.Oracle => StoreFamily.Oracle,
        MatrixStore.Cosmos => StoreFamily.Cosmos,
        _ => StoreFamily.Light
    };
}

/// <summary>
/// Connection details for every backend a matrix cell can reach, resolved once per shard from the
/// running AppHost. A shard leaves the properties its cells never touch null; asking a harness to
/// build a cell whose backend is missing throws with the backend named, which is the signal that a
/// cell landed in the wrong shard.
/// </summary>
public sealed class MatrixBackends
{
    public string? Redis { get; init; }
    public string? Nats { get; init; }
    public string? PostgreSql { get; init; }
    public string? SqlServer { get; init; }
    public string? MongoDb { get; init; }
    public string? MySql { get; init; }
    public string? Kafka { get; init; }
    public string? RabbitMq { get; init; }
    public string? LocalStack { get; init; }
    public string? AzureServiceBus { get; init; }
    public string? PubSubProjectId { get; init; }
    public string? Oracle { get; init; }
    public string? Cosmos { get; init; }

    /// <summary>Reads a required backend, naming it in the failure when the shard did not wire it.</summary>
    public string Require(string? value, string backend)
        => string.IsNullOrWhiteSpace(value)
            ? throw new InvalidOperationException(
                $"The '{backend}' backend is not wired for this shard. A matrix cell needing it is in the wrong shard — " +
                "see ProviderMatrix.ShardOf.")
            : value;
}

/// <summary>
/// Per-cell namespace names, derived from <see cref="MatrixCell.Token"/> and clamped to each
/// backend's identifier limit. Isolation is per cell rather than per run so that cells never observe
/// each other's messages, flow state, or DDL — a shared queue would turn an unrelated cell's leftover
/// worker job into this cell's failure.
/// <para>
/// The limits are real and were each learned from a failure: PostgreSQL truncates identifiers at 63
/// bytes, Oracle object names are uppercase, SQS queue names cap at 80 characters, and a MongoDB
/// namespace (database + collection) caps at 235 bytes.
/// </para>
/// </summary>
/// <param name="cell">The combination whose namespaces these are.</param>
/// <param name="discriminator">
/// Optional extra input to the token. The combination matrix leaves it null so a cell's namespaces
/// stay deterministic and inspectable. The conformance suites pass a fresh value per host, because
/// their facts all run the same cell and would otherwise share one namespace — which is how a poison
/// message parked by one fact ends up being delivered to the next one.
/// </param>
public sealed class MatrixNames(MatrixCell cell, string? discriminator = null)
{
    private readonly string _token = discriminator is null
        ? cell.Token
        : new MatrixCell(cell.Channel, cell.Transport, cell.Store).Token[..4] +
          Convert.ToHexString(
              System.Security.Cryptography.SHA256.HashData(
                  System.Text.Encoding.UTF8.GetBytes(discriminator)), 0, 3).ToLowerInvariant();

    /// <summary>PostgreSQL/SQL Server schema for the channel's tables. Well inside the 63-byte limit.</summary>
    public string ChannelSchema => $"armc_{_token}";

    /// <summary>PostgreSQL/SQL Server schema for the transport's queue tables.</summary>
    public string TransportSchema => $"armt_{_token}";

    /// <summary>PostgreSQL/SQL Server schema for the durable-flow store.</summary>
    public string StoreSchema => $"armf_{_token}";

    /// <summary>Redis key prefix; also the Redis transport's stream prefix.</summary>
    public string RedisPrefix => $"arm:{_token}";

    /// <summary>NATS subject prefix. Dots are subject separators, so the token stays a single token.</summary>
    public string NatsSubjectPrefix => $"arm.{_token}";

    /// <summary>NATS JetStream KV bucket for recovery state. Buckets allow only [A-Za-z0-9_-].</summary>
    public string NatsRecoveryBucket => $"arm-{_token}";

    /// <summary>MongoDB database for the channel/transport/store collections. Cap is 63 bytes.</summary>
    public string MongoDatabase => $"arm_{_token}";

    /// <summary>Kafka topic prefix. Kafka allows [a-zA-Z0-9._-] up to 249 characters.</summary>
    public string KafkaTopicPrefix => $"arm-{_token}";

    /// <summary>Kafka consumer group; distinct per role so worker and response never share offsets.</summary>
    public string KafkaGroup(string role) => $"arm-{_token}-{role}";

    /// <summary>RabbitMQ exchange/queue base name.</summary>
    public string RabbitBase => $"arm.{_token}";

    /// <summary>SQS queue base name. SQS caps at 80 characters and allows only [a-zA-Z0-9_-].</summary>
    public string SqsBase => $"arm-{_token}";

    /// <summary>Google Pub/Sub topic/subscription base name. Must start with a letter.</summary>
    public string PubSubBase => $"arm-{_token}";

    /// <summary>Table name for the single-table stores (MySQL, Oracle, SQLite, DynamoDB).</summary>
    public string StoreTable => $"arm_f_{_token}";

    /// <summary>Oracle object names are uppercase and this build keeps them short by convention.</summary>
    public string OracleStoreTable => $"ARM_F_{_token}".ToUpperInvariant();

    /// <summary>Cosmos database name for the durable-flow container.</summary>
    public string CosmosDatabase => $"arm_{_token}";

    /// <summary>MongoDB collection for the durable-flow store, inside <see cref="MongoDatabase"/>.</summary>
    public string StoreCollection => "flow_state";

    /// <summary>
    /// File-backed SQLite database, one per harness instance rather than per cell. Every other name
    /// here is deterministic so a failing cell leaves something to inspect, but a SQLite file cannot
    /// be: Microsoft.Data.Sqlite pools connections by connection string, so a later run of the same
    /// cell can be handed a pooled handle to the file a previous run's teardown already deleted, and
    /// the store then reports "no such table" against a live-looking connection.
    /// </summary>
    public string SqliteDatabasePath { get; } =
        Path.Combine(Path.GetTempPath(), $"asyncresponse-matrix-{cell.Token}-{Guid.NewGuid():N}.db");

    /// <summary>A fresh correlation id scoped to this cell, so cross-cell leakage is visible if it happens.</summary>
    public string NewCorrelationId(string label)
        => string.Create(
            CultureInfo.InvariantCulture,
            $"{label}-{_token}-{Guid.NewGuid():N}");
}
