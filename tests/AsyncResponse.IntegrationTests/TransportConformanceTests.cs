using AsyncResponse.Conformance;
using Xunit;

namespace AsyncResponse.IntegrationTests;

// The transport behavioral contract (see TransportConformanceSuite) run against every real broker the
// fixtures boot. Each class builds its hosts in this test process and isolates itself with a
// per-host namespace, so the classes share a fleet without sharing a queue.
//
// They ride the cross-product shards rather than the app-driven batches, because those shards are
// driver-only: they boot exactly these containers and start no sample app. That matters for more than
// tidiness — the first CI run had every delivery-dependent Redis fact time out in the app-heavy
// "data" batch, which runs nine sample-app processes alongside eight containers on a four-core
// runner, while the same facts passed everywhere else.

/// <summary>Contract run against real Redis streams.</summary>
[Collection(MatrixDatabaseLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixDatabaseLight)]
public sealed class RedisTransportConformanceTests(MatrixDatabaseLightFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.Redis;

    protected override MatrixBackends Backends => fixture.Backends;

    // Stream reads over loopback settle in well under a second; 15s absorbs a loaded shared container.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(15);
}

/// <summary>Contract run against real NATS JetStream.</summary>
[Collection(MatrixDatabaseLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixDatabaseLight)]
public sealed class NatsTransportConformanceTests(MatrixDatabaseLightFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.Nats;

    protected override MatrixBackends Backends => fixture.Backends;

    protected override TimeSpan Generous => TimeSpan.FromSeconds(15);
}

/// <summary>Contract run against the real PostgreSQL FOR UPDATE SKIP LOCKED queue.</summary>
[Collection(MatrixDatabaseLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixDatabaseLight)]
public sealed class PostgreSqlTransportConformanceTests(MatrixDatabaseLightFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.PostgreSql;

    protected override MatrixBackends Backends => fixture.Backends;

    // A polling database queue on a container shared with the whole data batch: 30s.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(30);
}

/// <summary>Contract run against the real SQL Server UPDLOCK/READPAST queue.</summary>
[Collection(MatrixDatabaseLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixDatabaseLight)]
public sealed class SqlServerTransportConformanceTests(MatrixDatabaseLightFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.SqlServer;

    protected override MatrixBackends Backends => fixture.Backends;

    protected override TimeSpan Generous => TimeSpan.FromSeconds(30);
}

/// <summary>Contract run against the real MongoDB findOneAndUpdate queue.</summary>
[Collection(MatrixDatabaseLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixDatabaseLight)]
public sealed class MongoDbTransportConformanceTests(MatrixDatabaseLightFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.MongoDb;

    protected override MatrixBackends Backends => fixture.Backends;

    protected override TimeSpan Generous => TimeSpan.FromSeconds(30);
}

/// <summary>Contract run against a real single-broker Kafka.</summary>
[Collection(MatrixBrokerLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixBrokerLight)]
public sealed class KafkaTransportConformanceTests(MatrixBrokerLightFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.Kafka;

    protected override MatrixBackends Backends => fixture.Backends;

    // Kafka pays consumer-group rebalance latency on every fresh subscriber, and each fact builds one.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(45);
}

/// <summary>Contract run against a real RabbitMQ broker.</summary>
[Collection(MatrixBrokerLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixBrokerLight)]
public sealed class RabbitMqTransportConformanceTests(MatrixBrokerLightFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.RabbitMq;

    protected override MatrixBackends Backends => fixture.Backends;

    protected override TimeSpan Generous => TimeSpan.FromSeconds(20);
}

/// <summary>Contract run against the Google Pub/Sub emulator.</summary>
[Collection(MatrixCloudLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixCloudLight)]
public sealed class GooglePubSubTransportConformanceTests(MatrixCloudLightFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.GooglePubSub;

    protected override MatrixBackends Backends => fixture.Backends;

    // The emulator's default ack deadline is 10s and redelivery waits it out.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(45);
}

/// <summary>Contract run against LocalStack's SQS.</summary>
[Collection(MatrixCloudLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixCloudLight)]
public sealed class SqsTransportConformanceTests(MatrixCloudLightFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.Sqs;

    protected override MatrixBackends Backends => fixture.Backends;

    // SQS redelivery is the visibility timeout, which the transport sets in whole seconds.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(60);
}

/// <summary>Contract run against the Azure Service Bus emulator.</summary>
[Collection(MatrixCloudLightCollection.Name)]
[Trait(Batches.Trait, Batches.MatrixCloudLight)]
public sealed class AzureServiceBusTransportConformanceTests(MatrixCloudLightFixture fixture) : TransportConformanceSuite
{
    protected override MatrixTransport Transport => MatrixTransport.AzureServiceBus;

    protected override MatrixBackends Backends => fixture.Backends;

    // The emulator's queues have a 1-minute lock duration; redelivery cannot beat it.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(90);
}
