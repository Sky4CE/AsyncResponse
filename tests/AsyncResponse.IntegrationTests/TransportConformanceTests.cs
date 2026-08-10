using AsyncResponse.Conformance;
using Xunit;

namespace AsyncResponse.IntegrationTests;

// The transport behavioral contract (see TransportConformanceSuite) run against every real broker the
// fixtures boot. Each class builds its hosts in this test process — no sample app — and isolates
// itself with a per-cell namespace, so these run alongside the app-driven suites on the same fleet.
//
// The classes are spread across the existing batches by which container each transport needs, rather
// than getting batches of their own: the contract needs one broker per transport, and every one of
// them is already up in some batch.

/// <summary>Contract run against real Redis streams.</summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class RedisTransportConformanceTests(DataBatchFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.Redis;

    protected override MatrixBackends Backends => fixture.Backends;

    // Stream reads over loopback settle in well under a second; 15s absorbs a loaded shared container.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(15);
}

/// <summary>Contract run against real NATS JetStream.</summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class NatsTransportConformanceTests(DataBatchFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.Nats;

    protected override MatrixBackends Backends => fixture.Backends;

    protected override TimeSpan Generous => TimeSpan.FromSeconds(15);
}

/// <summary>Contract run against the real PostgreSQL FOR UPDATE SKIP LOCKED queue.</summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class PostgreSqlTransportConformanceTests(DataBatchFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.PostgreSql;

    protected override MatrixBackends Backends => fixture.Backends;

    // A polling database queue on a container shared with the whole data batch: 30s.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(30);
}

/// <summary>Contract run against the real SQL Server UPDLOCK/READPAST queue.</summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class SqlServerTransportConformanceTests(DataBatchFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.SqlServer;

    protected override MatrixBackends Backends => fixture.Backends;

    protected override TimeSpan Generous => TimeSpan.FromSeconds(30);
}

/// <summary>Contract run against the real MongoDB findOneAndUpdate queue.</summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class MongoDbTransportConformanceTests(DataBatchFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.MongoDb;

    protected override MatrixBackends Backends => fixture.Backends;

    protected override TimeSpan Generous => TimeSpan.FromSeconds(30);
}

/// <summary>Contract run against a real single-broker Kafka.</summary>
[Collection(BrokersCollection.Name)]
[Trait(Batches.Trait, Batches.Brokers)]
public sealed class KafkaTransportConformanceTests(BrokersBatchFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.Kafka;

    protected override MatrixBackends Backends => fixture.Backends;

    // Kafka pays consumer-group rebalance latency on every fresh subscriber, and each fact builds one.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(45);
}

/// <summary>Contract run against a real RabbitMQ broker.</summary>
[Collection(BrokersCollection.Name)]
[Trait(Batches.Trait, Batches.Brokers)]
public sealed class RabbitMqTransportConformanceTests(BrokersBatchFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.RabbitMq;

    protected override MatrixBackends Backends => fixture.Backends;

    protected override TimeSpan Generous => TimeSpan.FromSeconds(20);
}

/// <summary>Contract run against the Google Pub/Sub emulator.</summary>
[Collection(BrokersCollection.Name)]
[Trait(Batches.Trait, Batches.Brokers)]
public sealed class GooglePubSubTransportConformanceTests(BrokersBatchFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.GooglePubSub;

    protected override MatrixBackends Backends => fixture.Backends;

    // The emulator's default ack deadline is 10s and redelivery waits it out.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(45);
}

/// <summary>Contract run against LocalStack's SQS.</summary>
[Collection(CloudCollection.Name)]
[Trait(Batches.Trait, Batches.Cloud)]
public sealed class SqsTransportConformanceTests(CloudBatchFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.Sqs;

    protected override MatrixBackends Backends => fixture.Backends;

    // SQS redelivery is the visibility timeout, which the transport sets in whole seconds.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(60);
}

/// <summary>Contract run against the Azure Service Bus emulator.</summary>
[Collection(CloudCollection.Name)]
[Trait(Batches.Trait, Batches.Cloud)]
public sealed class AzureServiceBusTransportConformanceTests(CloudBatchFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.AzureServiceBus;

    protected override MatrixBackends Backends => fixture.Backends;

    // The emulator's queues have a 1-minute lock duration; redelivery cannot beat it.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(90);
}
