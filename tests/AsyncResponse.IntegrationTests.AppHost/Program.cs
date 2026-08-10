using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

const string ProjectId = "itest-project";
const string WorkerTopic = "worker-topic";
const string WorkerSubscription = "worker-sub";
const string ResponseTopic = "response-topic";
const string ResponseSubscription = "response-sub";
const string EarlyAckWorkerTopic = "worker-topic-early-ack";
const string EarlyAckWorkerSubscription = "worker-sub-early-ack";
const string EarlyAckResponseTopic = "response-topic-early-ack";
const string EarlyAckResponseSubscription = "response-sub-early-ack";
const string TestRedisKeyPrefix = "itest";
const string EarlyAckRedisKeyPrefix = "itest-early-ack";
const string RabbitMqRedisKeyPrefix = "itest-rabbitmq";
const string RabbitMqEarlyAckRedisKeyPrefix = "itest-rabbitmq-early-ack";
const string RabbitMqWorkerExchange = "asyncresponse.itest.worker";
const string RabbitMqWorkerQueue = "asyncresponse.itest.worker";
const string RabbitMqWorkerRoutingKey = "asyncresponse.itest.worker";
const string RabbitMqResponseExchange = "asyncresponse.itest.response";
const string RabbitMqResponseQueue = "asyncresponse.itest.response";
const string RabbitMqResponseRoutingKey = "asyncresponse.itest.response";
const string RabbitMqEarlyAckWorkerExchange = "asyncresponse.itest.worker.earlyack";
const string RabbitMqEarlyAckWorkerQueue = "asyncresponse.itest.worker.earlyack";
const string RabbitMqEarlyAckWorkerRoutingKey = "asyncresponse.itest.worker.earlyack";
const string RabbitMqEarlyAckResponseExchange = "asyncresponse.itest.response.earlyack";
const string RabbitMqEarlyAckResponseQueue = "asyncresponse.itest.response.earlyack";
const string RabbitMqEarlyAckResponseRoutingKey = "asyncresponse.itest.response.earlyack";
const string AzureServiceBusRedisKeyPrefix = "itest-azure-servicebus";
const string AzureServiceBusEarlyAckRedisKeyPrefix = "itest-azure-servicebus-early-ack";
const string AzureServiceBusWorkerQueue = "asyncresponse-itest-asb-worker";
const string AzureServiceBusResponseQueue = "asyncresponse-itest-asb-response";
const string AzureServiceBusEarlyAckWorkerQueue = "asyncresponse-itest-asb-worker-earlyack";
const string AzureServiceBusEarlyAckResponseQueue = "asyncresponse-itest-asb-response-earlyack";
const string SqsRedisKeyPrefix = "itest-sqs";
const string SqsEarlyAckRedisKeyPrefix = "itest-sqs-early-ack";
const string SqsWorkerQueue = "asyncresponse-itest-sqs-worker";
const string SqsResponseQueue = "asyncresponse-itest-sqs-response";
const string SqsEarlyAckWorkerQueue = "asyncresponse-itest-sqs-worker-earlyack";
const string SqsEarlyAckResponseQueue = "asyncresponse-itest-sqs-response-earlyack";
const string RedisTransportKeyPrefix = "itest-redistransport";
const string RedisTransportEarlyAckKeyPrefix = "itest-redistransport-early-ack";
const string NatsSubjectPrefix = "itest-nats";
const string NatsEarlyAckSubjectPrefix = "itest-nats-early-ack";
const string KafkaRedisKeyPrefix = "itest-kafka";
const string KafkaEarlyAckRedisKeyPrefix = "itest-kafka-early-ack";
const string KafkaWorkerTopic = "asyncresponse.itest.worker";
const string KafkaResponseTopic = "asyncresponse.itest.response";
const string KafkaEarlyAckWorkerTopic = "asyncresponse.itest.worker.earlyack";
const string KafkaEarlyAckResponseTopic = "asyncresponse.itest.response.earlyack";
const string PostgreSqlWorkerQueue = "worker";
const string PostgreSqlResponseQueue = "response";
const string PostgreSqlDeadLetterQueue = "deadletter";
const string PostgreSqlEarlyAckWorkerQueue = "worker_earlyack";
const string PostgreSqlEarlyAckResponseQueue = "response_earlyack";
const string PostgreSqlEarlyAckDeadLetterQueue = "deadletter_earlyack";
const string SqlServerWorkerQueue = "worker";
const string SqlServerResponseQueue = "response";
const string SqlServerDeadLetterQueue = "deadletter";
const string SqlServerEarlyAckWorkerQueue = "worker_earlyack";
const string SqlServerEarlyAckResponseQueue = "response_earlyack";
const string SqlServerEarlyAckDeadLetterQueue = "deadletter_earlyack";
// The two SQL Server app variants share one server and database; they isolate through distinct
// schemas (each package creates its own tables inside the configured schema).
const string SqlServerSchema = "itest";
const string SqlServerEarlyAckSchema = "itest_earlyack";
const string MongoDbWorkerQueue = "worker";
const string MongoDbResponseQueue = "response";
const string MongoDbDeadLetterQueue = "deadletter";
const string MongoDbEarlyAckWorkerQueue = "worker_earlyack";
const string MongoDbEarlyAckResponseQueue = "response_earlyack";
const string MongoDbEarlyAckDeadLetterQueue = "deadletter_earlyack";
// The two MongoDB app variants share one server; they isolate through distinct databases (each
// package creates its own collections inside the configured database).
const string MongoDbDatabase = "asyncresponse_itest";
const string MongoDbEarlyAckDatabase = "asyncresponse_itest_earlyack";
const string OracleAppUser = "asyncresponse";

// Oracle and Cosmos back durable-flow store contracts. They run by default everywhere: both images
// publish linux/arm64 manifests and both boot natively on Apple silicon (verified 2026-08-09 — ~25s
// to ready, 630 MiB and 310 MiB resident). This used to auto-skip on Apple-silicon macOS on the
// premise that the Oracle image was amd64-only; that premise was false, and the skip meant the two
// store contracts never ran locally at all. Set the flag to "true" to leave them out — CI's AOT and
// load-test jobs do, to avoid pulling two large images they have no use for.
static bool SkipOracleCosmos()
    => string.Equals(
        Env("ASYNCRESPONSE_ITEST_SKIP_ORACLE_COSMOS", "false"),
        "true",
        StringComparison.OrdinalIgnoreCase);

static string Env(string name, string fallback)
    => Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : fallback;

var builder = DistributedApplication.CreateBuilder(args);
builder.Services.Configure<LoggerFilterOptions>(options =>
    options.AddFilter("Microsoft.Extensions.Diagnostics.HealthChecks.DefaultHealthCheckService", LogLevel.Critical));

// --- SUT mode -------------------------------------------------------------------------------
// "project" (default) runs the sample from source (JIT); "aot" runs the Native AOT-published
// binary named by ASYNCRESPONSE_ITEST_SUT_PATH, so the *same* integration suite exercises the
// fully trimmed app against the real broker fleet. Resource names are identical in both modes,
// which is what keeps the test project completely unchanged.
//
// SUTs run natively only where the full driver stack is Native AOT-capable (vendor matrix in
// docs/aot.md). Verified natively today: the NATS and PostgreSQL channel/transport pairs. Pinned
// to JIT, each for a driver-level reason observed empirically in this harness:
//  - MongoDB: MongoDB.Driver serializes BSON through reflection (not trim/AOT-compatible).
//  - SqlServer: Microsoft.Data.SqlClient fails the TDS pre-login handshake in a native binary.
//  - Every Redis-channel SUT (including all broker-transport variants, which pair with the Redis
//    channel): StackExchange.Redis 3.x throws MissingFieldException('_invocationList') under
//    Native AOT — its net8+ Delegates helper UnsafeAccessor-reads CoreCLR's MulticastDelegate
//    internals, which do not exist in the Native AOT runtime. Revisit when fixed upstream; a
//    channel-remap mode (PostgreSQL channel under the broker transports) could verify the
//    transports natively before then.
var sutMode = Env("ASYNCRESPONSE_ITEST_SUT", "project").ToLowerInvariant();
var sutAotPath = Environment.GetEnvironmentVariable("ASYNCRESPONSE_ITEST_SUT_PATH");

IResourceBuilder<IResourceWithEnvironment> AddSutApp(string name, bool aotCapable = true, params IResourceBuilder<IResource>[] waitFor)
{
    if (string.Equals(sutMode, "aot", StringComparison.Ordinal) && aotCapable)
    {
        if (string.IsNullOrWhiteSpace(sutAotPath) || !File.Exists(sutAotPath))
        {
            throw new InvalidOperationException(
                "ASYNCRESPONSE_ITEST_SUT=aot requires ASYNCRESPONSE_ITEST_SUT_PATH to point at the published Native AOT " +
                "sample binary (dotnet publish samples/AsyncResponse.Sample/AsyncResponse.Sample.csproj -c Release -o <dir>).");
        }

        // Executables do not get the automatic ASP.NET endpoint wiring projects get; binding the
        // allocated port through ASPNETCORE_HTTP_PORTS keeps Kestrel on the proxied endpoint.
        var exe = builder.AddExecutable(name, sutAotPath, Path.GetDirectoryName(Path.GetFullPath(sutAotPath))!)
            .WithHttpEndpoint(env: "ASPNETCORE_HTTP_PORTS")
            .WithHttpHealthCheck("/alive");
        foreach (var dependency in waitFor)
            exe.WaitFor(dependency);
        return exe;
    }

    var project = builder.AddProject<Projects.AsyncResponse_Sample>(name, launchProfileName: null)
        .WithHttpEndpoint()
        .WithHttpHealthCheck("/alive");
    foreach (var dependency in waitFor)
        project.WaitFor(dependency);
    return project;
}

// The Redis channel + transport speak RESP via StackExchange.Redis, so they run unchanged on
// Redis-compatible servers. The CI compatibility matrix overrides the image via these env vars to run
// the focused Redis suite against Valkey; the default is the official Redis. Only servers that
// share the redis docker-entrypoint.sh + *-server launch contract work through this override — Valkey
// does. Dragonfly (different container entrypoint) and Garnet (no stream commands) are not drop-ins for
// this harness and are validated separately (see docs/configuration.md#redis-compatible-servers).
IResourceBuilder<RedisResource> AddRedisContainer()
{
    var redis = builder.AddRedis("redis");
    if (Env("ASYNCRESPONSE_ITEST_REDIS_IMAGE", "") is { Length: > 0 } redisImage)
    {
        if (Env("ASYNCRESPONSE_ITEST_REDIS_REGISTRY", "") is { Length: > 0 } redisRegistry)
            redis = redis.WithImageRegistry(redisRegistry);
        redis = redis.WithImage(redisImage, Env("ASYNCRESPONSE_ITEST_REDIS_TAG", "latest"));
    }

    return redis;
}

IResourceBuilder<IResourceWithEnvironment> AddRedisTransportApp(IResourceBuilder<RedisResource> redis, string name, bool earlyAck)
{
    var keyPrefix = earlyAck ? RedisTransportEarlyAckKeyPrefix : RedisTransportKeyPrefix;
    var consumerGroupSuffix = earlyAck ? "-earlyack" : "";
    var app = AddSutApp(name, aotCapable: false, waitFor: [redis])
        .WithReference(redis)
        .WithEnvironment("AsyncResponse:KeyPrefix", keyPrefix)
        .WithEnvironment("AsyncResponse:Channel", "Redis")
        .WithEnvironment("AsyncResponse:Transport", "Redis")
        .WithEnvironment("Redis:KeyPrefix", keyPrefix)
        .WithEnvironment("Redis:WorkerConsumerGroup", $"asyncresponse-itest-redis-workers{consumerGroupSuffix}")
        .WithEnvironment("Redis:ResponseConsumerGroup", $"asyncresponse-itest-redis-responses{consumerGroupSuffix}")
        .WithEnvironment("Redis:StreamMaxLength", Env("ASYNCRESPONSE_ITEST_REDIS_STREAM_MAX_LENGTH", "100000"))
        .WithEnvironment("Redis:PublishMaxAttempts", Env("ASYNCRESPONSE_ITEST_REDIS_PUBLISH_MAX_ATTEMPTS", "3"))
        .WithEnvironment("Redis:Worker:MaxDeliveryAttempts", Env("ASYNCRESPONSE_ITEST_REDIS_MAX_DELIVERY_ATTEMPTS", "5"))
        .WithEnvironment("Redis:Response:MaxDeliveryAttempts", Env("ASYNCRESPONSE_ITEST_REDIS_MAX_DELIVERY_ATTEMPTS", "5"))
        .WithEnvironment("Redis:Worker:PendingMessageMinIdleTimeSeconds", Env("ASYNCRESPONSE_ITEST_REDIS_PENDING_IDLE_SECONDS", "1"))
        .WithEnvironment("Redis:Response:PendingMessageMinIdleTimeSeconds", Env("ASYNCRESPONSE_ITEST_REDIS_PENDING_IDLE_SECONDS", "1"));

    if (earlyAck)
    {
        app.WithEnvironment("Redis:Worker:AckMode", Env("ASYNCRESPONSE_ITEST_REDIS_WORKER_ACK_MODE", "AckAfterEnqueue"))
            .WithEnvironment("Redis:Worker:BackgroundWorkerCount", Env("ASYNCRESPONSE_ITEST_REDIS_WORKER_BACKGROUND_WORKERS", "4"))
            .WithEnvironment("Redis:Worker:BackgroundQueueCapacity", Env("ASYNCRESPONSE_ITEST_REDIS_WORKER_QUEUE_CAPACITY", "256"))
            .WithEnvironment("Redis:Worker:BackgroundDrainTimeoutSeconds", Env("ASYNCRESPONSE_ITEST_REDIS_WORKER_DRAIN_SECONDS", "10"))
            .WithEnvironment("Redis:HostShutdownTimeoutSeconds", "30");
    }

    return app;
}

// The compatibility profile intentionally excludes every unrelated broker and database. This keeps a
// Redis/Valkey signal from failing because SQL Server, Kafka, Oracle, or another heavyweight fixture
// exhausts a hosted runner or loses a transient port race. It is a whole-run override rather than a
// batch: CI filters the run to the Redis classes, so no other batch's fixture ever initializes.
if (string.Equals(Env("ASYNCRESPONSE_ITEST_PROFILE", ""), "redis-compat", StringComparison.OrdinalIgnoreCase))
{
    var compatRedis = AddRedisContainer();
    AddRedisTransportApp(compatRedis, "itest-app-redis", earlyAck: false);
    AddRedisTransportApp(compatRedis, "itest-app-redis-early-ack", earlyAck: true);
    builder.Build().Run();
    return;
}

IResourceBuilder<ContainerResource> AddRabbitMqContainer()
    => builder.AddContainer("rabbitmq", "rabbitmq", "3.13-management")
        .WithEndpoint(targetPort: 5672, scheme: "tcp", name: "amqp")
        .WithEndpoint(targetPort: 15672, scheme: "http", name: "management");

IResourceBuilder<ContainerResource> AddPubSubContainer()
    => builder.AddContainer("pubsub", "gcr.io/google.com/cloudsdktool/google-cloud-cli", "446.0.1-emulators")
    .WithArgs("gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085", $"--project={ProjectId}")
    .WithEndpoint(targetPort: 8085, scheme: "tcp", name: "pubsub");

// `-js` enables JetStream, which the NATS channel's Key-Value recovery store and the NATS transport's
// streams both require.
IResourceBuilder<ContainerResource> AddNatsContainer()
    => builder.AddContainer("nats", "nats", "latest")
        .WithArgs("-js")
        .WithEndpoint(targetPort: 4222, scheme: "tcp", name: "nats");

// Single-broker KRaft Kafka (the Aspire integration uses the confluent-local image). One broker backs
// both Kafka app variants; they isolate through distinct topics and consumer groups. This container
// doubles as the roadmap's Redpanda-compatibility reference: everything speaks the Kafka protocol.
IResourceBuilder<KafkaServerResource> AddKafkaContainer() => builder.AddKafka("kafka");

// Two PostgreSQL app instances (default + early-ack) share this one server, each with its own Npgsql
// pool. The image default max_connections=100 is exhausted under the load-test profile ("FATAL: sorry,
// too many clients already"). Raise the server ceiling well above the combined pool budget below
// (2 apps x Maximum Pool Size=120 = 240) so neither the load test nor parallel integration apps starve.
IResourceBuilder<ContainerResource> AddPostgresContainer()
    => builder.AddContainer("postgres", "postgres", "16-alpine")
        .WithEnvironment("POSTGRES_DB", "asyncresponse")
        .WithEnvironment("POSTGRES_PASSWORD", "postgres")
        .WithArgs("-c", "max_connections=400")
        .WithEndpoint(targetPort: 5432, scheme: "tcp", name: "postgres");

IResourceBuilder<ContainerResource> AddMySqlContainer()
    => builder.AddContainer("mysql", "mysql", "8.4")
        .WithEnvironment("MYSQL_DATABASE", "asyncresponse")
        .WithEnvironment("MYSQL_ROOT_PASSWORD", "mysql")
        .WithEndpoint(targetPort: 3306, scheme: "tcp", name: "mysql");

// Single-node replica set: change streams — the MongoDB channel's waiter wake and the transport's
// subscriber wake — require one. The entrypoint wrapper starts mongod with --replSet and initiates
// the set as soon as the server answers; clients connect with directConnection=true so they never
// chase the replica-set-advertised container hostname, which is unreachable from the host network.
IResourceBuilder<ContainerResource> AddMongoDbContainer()
    => builder.AddContainer("mongodb", "mongo", "7")
        .WithEntrypoint("bash")
        .WithArgs(
            "-c",
            "mongod --replSet rs0 --bind_ip_all & MONGOD_PID=$!; " +
            "until mongosh --quiet --eval 'try { rs.status().ok } catch (e) { rs.initiate().ok }' >/dev/null 2>&1; do sleep 0.5; done; " +
            "wait $MONGOD_PID")
        .WithEndpoint(targetPort: 27017, scheme: "tcp", name: "mongodb");

// Oracle and Cosmos back durable-flow store contract tests only — no SUT app references them, so
// they live in the "stores" batch. See SkipOracleCosmos above for the opt-out.
// INIT_SGA_SIZE/INIT_PGA_SIZE: left to its own devices Oracle sizes its SGA from the host and
// measured 2,180 MiB here — the single largest container in the suite by a wide margin. The store
// contract is a handful of small tables, so a 1 GiB SGA is ample and keeps the batch inside a
// default Docker VM.
IResourceBuilder<ContainerResource> AddOracleContainer()
    => builder.AddContainer("oracle", "gvenzl/oracle-free", "23-slim")
        .WithEnvironment("ORACLE_PASSWORD", Env("ASYNCRESPONSE_ITEST_ORACLE_ADMIN_PASSWORD", "AsyncResponse12345"))
        .WithEnvironment("APP_USER", OracleAppUser)
        .WithEnvironment("APP_USER_PASSWORD", Env("ASYNCRESPONSE_ITEST_ORACLE_APP_PASSWORD", "AsyncResponse12345"))
        .WithEnvironment("INIT_SGA_SIZE", Env("ASYNCRESPONSE_ITEST_ORACLE_SGA_MB", "1024"))
        .WithEnvironment("INIT_PGA_SIZE", Env("ASYNCRESPONSE_ITEST_ORACLE_PGA_MB", "256"))
        .WithEndpoint(targetPort: 1521, scheme: "tcp", name: "oracle");

IResourceBuilder<ContainerResource> AddCosmosContainer()
    => builder.AddContainer("cosmos", "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator", "vnext-latest")
        .WithEnvironment("PROTOCOL", "https")
        .WithEndpoint(targetPort: 8081, scheme: "https", name: "gateway")
        .WithEndpoint(targetPort: 8080, scheme: "http", name: "health")
        .WithHttpHealthCheck("/ready", endpointName: "health");

// Dedicated SQL Server for the SqlServer channel + transport SUTs (separate from the one backing the
// Azure Service Bus emulator, so the two suites cannot interfere). Both SqlServer app variants share
// it; the sample app provisions the database and each variant isolates through its own schema.
var sqlServerPassword = Env("ASYNCRESPONSE_ITEST_SQLSERVER_PASSWORD", "P@ssword12345");

// MSSQL_MEMORY_LIMIT_MB: SQL Server grows its buffer pool to whatever the host allows and measured
// 1,328 MiB. Two of these run in the suite (this one and the Service Bus emulator's), so capping
// both keeps a batch from spending most of a Docker VM on database cache it never needs.
IResourceBuilder<ContainerResource> AddSqlServerContainer()
    => builder.AddContainer("sqlserver", "mcr.microsoft.com/mssql/server", "2022-latest")
        .WithEnvironment("ACCEPT_EULA", "Y")
        .WithEnvironment("MSSQL_SA_PASSWORD", Env("ASYNCRESPONSE_ITEST_SQLSERVER_PASSWORD", "P@ssword12345"))
        .WithEnvironment("MSSQL_MEMORY_LIMIT_MB", Env("ASYNCRESPONSE_ITEST_SQLSERVER_MEMORY_MB", "1024"))
        .WithEndpoint(targetPort: 1433, scheme: "tcp", name: "sqlserver");

// The Service Bus emulator needs its own SQL Server, so it costs two containers, not one — which is
// why it dominates whichever batch it lands in.
IResourceBuilder<ContainerResource> AddServiceBusContainer()
{
    var serviceBusSqlPassword = Env("ASYNCRESPONSE_ITEST_SERVICEBUS_SQL_PASSWORD", "P@ssword12345");
    var serviceBusSql = builder.AddContainer("servicebus-sql", "mcr.microsoft.com/mssql/server", "2022-latest")
        .WithEnvironment("ACCEPT_EULA", "Y")
        .WithEnvironment("MSSQL_SA_PASSWORD", serviceBusSqlPassword)
        .WithEnvironment("MSSQL_MEMORY_LIMIT_MB", Env("ASYNCRESPONSE_ITEST_SQLSERVER_MEMORY_MB", "1024"));
    var serviceBusConfigPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "servicebus-emulator-config.json"));
    return builder.AddContainer("servicebus", "mcr.microsoft.com/azure-messaging/servicebus-emulator", "latest")
        .WithBindMount(serviceBusConfigPath, "/ServiceBus_Emulator/ConfigFiles/Config.json", isReadOnly: true)
        .WithEnvironment("SQL_SERVER", "servicebus-sql")
        .WithEnvironment("MSSQL_SA_PASSWORD", serviceBusSqlPassword)
        .WithEnvironment("ACCEPT_EULA", "Y")
        .WithEnvironment("EMULATOR_HTTP_PORT", "5300")
        .WithEnvironment("SQL_WAIT_INTERVAL", "30")
        .WithEndpoint(targetPort: 5672, scheme: "tcp", name: "amqp")
        .WithEndpoint(targetPort: 5300, scheme: "http", name: "management")
        .WaitFor(serviceBusSql)
        .WithHttpHealthCheck("/health", endpointName: "management");
}

// LocalStack emulates AWS SQS for the SQS transport SUTs. Only the SQS service is enabled; the
// sample app provisions its queues (and redrive-policy dead-letter queues) through the transport's
// CreateQueues option, so no config file or init script is needed.
IResourceBuilder<ContainerResource> AddLocalStackContainer()
    => builder.AddContainer("localstack", "localstack/localstack", "3")
        .WithEnvironment("SERVICES", "sqs,dynamodb")
        .WithEnvironment("EAGER_SERVICE_LOADING", "1")
        .WithEndpoint(targetPort: 4566, scheme: "http", name: "edge")
        .WithHttpHealthCheck("/_localstack/health", endpointName: "edge");

// --- Connection strings, derived per container ------------------------------------------------
// Each takes the container it describes, so a batch can build only the ones it declared.

ReferenceExpression PubSubEmulatorHost(IResourceBuilder<ContainerResource> pubsub)
{
    var endpoint = pubsub.GetEndpoint("pubsub");
    return ReferenceExpression.Create(
        $"{endpoint.Property(EndpointProperty.Host)}:{endpoint.Property(EndpointProperty.Port)}");
}

ReferenceExpression RabbitMqConnectionString(IResourceBuilder<ContainerResource> rabbitmq)
{
    var endpoint = rabbitmq.GetEndpoint("amqp");
    return ReferenceExpression.Create(
        $"amqp://guest:guest@{endpoint.Property(EndpointProperty.Host)}:{endpoint.Property(EndpointProperty.Port)}/");
}

ReferenceExpression ServiceBusConnectionString(IResourceBuilder<ContainerResource> serviceBus)
{
    var endpoint = serviceBus.GetEndpoint("amqp");
    return ReferenceExpression.Create(
        $"Endpoint=sb://{endpoint.Property(EndpointProperty.Host)}:{endpoint.Property(EndpointProperty.Port)};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;");
}

ReferenceExpression LocalStackServiceUrl(IResourceBuilder<ContainerResource> localstack)
{
    var endpoint = localstack.GetEndpoint("edge");
    return ReferenceExpression.Create(
        $"http://{endpoint.Property(EndpointProperty.Host)}:{endpoint.Property(EndpointProperty.Port)}");
}

ReferenceExpression NatsConnectionString(IResourceBuilder<ContainerResource> nats)
{
    var endpoint = nats.GetEndpoint("nats");
    return ReferenceExpression.Create(
        $"nats://{endpoint.Property(EndpointProperty.Host)}:{endpoint.Property(EndpointProperty.Port)}");
}

// Cap each app's Npgsql pool so the two PostgreSQL instances sharing the server above cannot, even
// combined (2 x 120 = 240), exceed its max_connections=400 ceiling — bounding aggregate connection use
// rather than letting Npgsql's default (100 per app) race the server limit under load.
//
// "No Reset On Close=true" drops the per-checkin DISCARD ALL (the single most-executed statement under
// load) and lets "Max Auto Prepare" actually retain prepared statements across pooled reuse; together
// they roughly halve server-side statements and cut parse/plan CPU — decisive on the load-test runner
// where one small PostgreSQL server backs every PostgreSQL scenario at once. The channel only LISTENs on
// dedicated long-lived connections, so skipping reset on the pooled query connections is safe.
ReferenceExpression PostgresConnectionString(IResourceBuilder<ContainerResource> postgres)
{
    var endpoint = postgres.GetEndpoint("postgres");
    return ReferenceExpression.Create(
        $"Host={endpoint.Property(EndpointProperty.Host)};Port={endpoint.Property(EndpointProperty.Port)};Username=postgres;Password=postgres;Database=asyncresponse;Maximum Pool Size=120;No Reset On Close=true;Max Auto Prepare=20");
}

ReferenceExpression MongoDbConnectionString(IResourceBuilder<ContainerResource> mongodb)
{
    var endpoint = mongodb.GetEndpoint("mongodb");
    return ReferenceExpression.Create(
        $"mongodb://{endpoint.Property(EndpointProperty.Host)}:{endpoint.Property(EndpointProperty.Port)}/?directConnection=true");
}

// Cap each app's SqlClient pool (2 apps x 120 = 240) well under SQL Server's default connection
// ceiling, mirroring the PostgreSQL budget above. TrustServerCertificate accepts the container's
// self-signed certificate.
ReferenceExpression SqlServerConnectionString(IResourceBuilder<ContainerResource> sqlserver)
{
    var endpoint = sqlserver.GetEndpoint("sqlserver");
    return ReferenceExpression.Create(
        $"Server={endpoint.Property(EndpointProperty.Host)},{endpoint.Property(EndpointProperty.Port)};User ID=sa;Password={Env("ASYNCRESPONSE_ITEST_SQLSERVER_PASSWORD", "P@ssword12345")};Database=asyncresponse;TrustServerCertificate=True;Max Pool Size=120");
}

// --- Sample-app groups --------------------------------------------------------------------------
// One function per transport family, taking the containers it needs. Batches compose these.

// The integration SUT is the sample app itself (one app, no duplication), booted here with the
// Redis channel + Google Pub/Sub transport. AddSutApp owns the endpoint and the /alive health
// check, and switches between project (JIT) and Native AOT binary per ASYNCRESPONSE_ITEST_SUT.
// earlyAck: only the brokers batch owns PubSubTransportTests, which drives the early-ack variant.
// Other batches need "itest-app" alone, because it is the fixture's default Client.
void AddPubSubApps(IResourceBuilder<RedisResource> redis, IResourceBuilder<ContainerResource> pubsub, bool earlyAck)
{
    var emulatorHost = PubSubEmulatorHost(pubsub);

    AddSutApp("itest-app", aotCapable: false, waitFor: [redis, pubsub])
    .WithReference(redis)
    .WithEnvironment("PUBSUB_EMULATOR_HOST", emulatorHost)
    .WithEnvironment("PubSub:ProjectId", ProjectId)
    .WithEnvironment("PubSub:WorkerTopicId", WorkerTopic)
    .WithEnvironment("PubSub:WorkerSubscriptionId", WorkerSubscription)
    .WithEnvironment("PubSub:ResponseTopicId", ResponseTopic)
    .WithEnvironment("PubSub:ResponseSubscriptionId", ResponseSubscription)
    .WithEnvironment("AsyncResponse:KeyPrefix", TestRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "GooglePubSub");

    if (!earlyAck)
        return;

    AddSutApp("itest-app-early-ack", aotCapable: false, waitFor: [redis, pubsub])
    .WithReference(redis)
    .WithEnvironment("PUBSUB_EMULATOR_HOST", emulatorHost)
    .WithEnvironment("PubSub:ProjectId", ProjectId)
    .WithEnvironment("PubSub:WorkerTopicId", EarlyAckWorkerTopic)
    .WithEnvironment("PubSub:WorkerSubscriptionId", EarlyAckWorkerSubscription)
    .WithEnvironment("PubSub:ResponseTopicId", EarlyAckResponseTopic)
    .WithEnvironment("PubSub:ResponseSubscriptionId", EarlyAckResponseSubscription)
    .WithEnvironment("PubSub:Worker:AckMode", "AckAfterEnqueue")
    .WithEnvironment("PubSub:Worker:BackgroundWorkerCount", "4")
    .WithEnvironment("PubSub:Worker:BackgroundQueueCapacity", "256")
    .WithEnvironment("PubSub:Worker:BackgroundDrainTimeoutSeconds", "10")
    .WithEnvironment("PubSub:HostShutdownTimeoutSeconds", "30")
    .WithEnvironment("AsyncResponse:KeyPrefix", EarlyAckRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "GooglePubSub");
}

void AddRabbitMqApps(IResourceBuilder<RedisResource> redis, IResourceBuilder<ContainerResource> rabbitmq)
{
    var rabbitMqConnectionString = RabbitMqConnectionString(rabbitmq);

    AddSutApp("itest-app-rabbitmq", aotCapable: false, waitFor: [redis, rabbitmq])
    .WithReference(redis)
    .WithEnvironment("RabbitMQ:ConnectionString", rabbitMqConnectionString)
    .WithEnvironment("RabbitMQ:WorkerExchange", RabbitMqWorkerExchange)
    .WithEnvironment("RabbitMQ:WorkerQueue", RabbitMqWorkerQueue)
    .WithEnvironment("RabbitMQ:WorkerRoutingKey", RabbitMqWorkerRoutingKey)
    .WithEnvironment("RabbitMQ:ResponseExchange", RabbitMqResponseExchange)
    .WithEnvironment("RabbitMQ:ResponseQueue", RabbitMqResponseQueue)
    .WithEnvironment("RabbitMQ:ResponseRoutingKey", RabbitMqResponseRoutingKey)
    .WithEnvironment("AsyncResponse:KeyPrefix", RabbitMqRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "RabbitMQ");

AddSutApp("itest-app-rabbitmq-early-ack", aotCapable: false, waitFor: [redis, rabbitmq])
    .WithReference(redis)
    .WithEnvironment("RabbitMQ:ConnectionString", rabbitMqConnectionString)
    .WithEnvironment("RabbitMQ:WorkerExchange", RabbitMqEarlyAckWorkerExchange)
    .WithEnvironment("RabbitMQ:WorkerQueue", RabbitMqEarlyAckWorkerQueue)
    .WithEnvironment("RabbitMQ:WorkerRoutingKey", RabbitMqEarlyAckWorkerRoutingKey)
    .WithEnvironment("RabbitMQ:ResponseExchange", RabbitMqEarlyAckResponseExchange)
    .WithEnvironment("RabbitMQ:ResponseQueue", RabbitMqEarlyAckResponseQueue)
    .WithEnvironment("RabbitMQ:ResponseRoutingKey", RabbitMqEarlyAckResponseRoutingKey)
    .WithEnvironment("RabbitMQ:Worker:AckMode", "AckAfterEnqueue")
    .WithEnvironment("RabbitMQ:Worker:BackgroundWorkerCount", "4")
    .WithEnvironment("RabbitMQ:Worker:BackgroundQueueCapacity", "256")
    .WithEnvironment("RabbitMQ:Worker:BackgroundDrainTimeoutSeconds", "10")
    .WithEnvironment("RabbitMQ:HostShutdownTimeoutSeconds", "30")
    .WithEnvironment("AsyncResponse:KeyPrefix", RabbitMqEarlyAckRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "RabbitMQ");
}

void AddServiceBusApps(IResourceBuilder<RedisResource> redis, IResourceBuilder<ContainerResource> serviceBus)
{
    var serviceBusConnectionString = ServiceBusConnectionString(serviceBus);

    AddSutApp("itest-app-azure-servicebus", aotCapable: false, waitFor: [redis, serviceBus])
    .WithReference(redis)
    .WithEnvironment("ConnectionStrings:AzureServiceBus", serviceBusConnectionString)
    .WithEnvironment("AzureServiceBus:WorkerQueue", AzureServiceBusWorkerQueue)
    .WithEnvironment("AzureServiceBus:ResponseQueue", AzureServiceBusResponseQueue)
    .WithEnvironment("AsyncResponse:KeyPrefix", AzureServiceBusRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "AzureServiceBus");

AddSutApp("itest-app-azure-servicebus-early-ack", aotCapable: false, waitFor: [redis, serviceBus])
    .WithReference(redis)
    .WithEnvironment("ConnectionStrings:AzureServiceBus", serviceBusConnectionString)
    .WithEnvironment("AzureServiceBus:WorkerQueue", AzureServiceBusEarlyAckWorkerQueue)
    .WithEnvironment("AzureServiceBus:ResponseQueue", AzureServiceBusEarlyAckResponseQueue)
    .WithEnvironment("AzureServiceBus:Worker:AckMode", Env("ASYNCRESPONSE_ITEST_AZURE_SERVICEBUS_WORKER_ACK_MODE", "AckAfterEnqueue"))
    .WithEnvironment("AzureServiceBus:Worker:BackgroundWorkerCount", Env("ASYNCRESPONSE_ITEST_AZURE_SERVICEBUS_WORKER_BACKGROUND_WORKERS", "4"))
    .WithEnvironment("AzureServiceBus:Worker:BackgroundQueueCapacity", Env("ASYNCRESPONSE_ITEST_AZURE_SERVICEBUS_WORKER_QUEUE_CAPACITY", "256"))
    .WithEnvironment("AzureServiceBus:Worker:BackgroundDrainTimeoutSeconds", Env("ASYNCRESPONSE_ITEST_AZURE_SERVICEBUS_WORKER_DRAIN_SECONDS", "10"))
    .WithEnvironment("AzureServiceBus:HostShutdownTimeoutSeconds", "30")
    .WithEnvironment("AsyncResponse:KeyPrefix", AzureServiceBusEarlyAckRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "AzureServiceBus");
}

// earlyAck: the databases batch needs only the default SQS app (for the durable-flow scenarios); the
// brokers batch, which owns SqsTransportTests, needs both variants.
void AddSqsApps(IResourceBuilder<RedisResource> redis, IResourceBuilder<ContainerResource> localstack, bool earlyAck)
{
    var localstackServiceUrl = LocalStackServiceUrl(localstack);

    AddSutApp("itest-app-sqs", aotCapable: false, waitFor: [redis, localstack])
    .WithReference(redis)
    .WithEnvironment("SQS:ServiceUrl", localstackServiceUrl)
    .WithEnvironment("SQS:Region", "us-east-1")
    .WithEnvironment("SQS:AccessKey", "test")
    .WithEnvironment("SQS:SecretKey", "test")
    .WithEnvironment("SQS:WorkerQueue", SqsWorkerQueue)
    .WithEnvironment("SQS:ResponseQueue", SqsResponseQueue)
    .WithEnvironment("SQS:CreateQueues", "true")
    .WithEnvironment("SQS:ReceiveWaitTimeSeconds", Env("ASYNCRESPONSE_ITEST_SQS_RECEIVE_WAIT_SECONDS", "2"))
    .WithEnvironment("AsyncResponse:KeyPrefix", SqsRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "SQS");

    if (!earlyAck)
        return;

    AddSutApp("itest-app-sqs-early-ack", aotCapable: false, waitFor: [redis, localstack])
    .WithReference(redis)
    .WithEnvironment("SQS:ServiceUrl", localstackServiceUrl)
    .WithEnvironment("SQS:Region", "us-east-1")
    .WithEnvironment("SQS:AccessKey", "test")
    .WithEnvironment("SQS:SecretKey", "test")
    .WithEnvironment("SQS:WorkerQueue", SqsEarlyAckWorkerQueue)
    .WithEnvironment("SQS:ResponseQueue", SqsEarlyAckResponseQueue)
    .WithEnvironment("SQS:CreateQueues", "true")
    .WithEnvironment("SQS:ReceiveWaitTimeSeconds", Env("ASYNCRESPONSE_ITEST_SQS_RECEIVE_WAIT_SECONDS", "2"))
    .WithEnvironment("SQS:Worker:AckMode", Env("ASYNCRESPONSE_ITEST_SQS_WORKER_ACK_MODE", "AckAfterEnqueue"))
    .WithEnvironment("SQS:Worker:BackgroundWorkerCount", Env("ASYNCRESPONSE_ITEST_SQS_WORKER_BACKGROUND_WORKERS", "4"))
    .WithEnvironment("SQS:Worker:BackgroundQueueCapacity", Env("ASYNCRESPONSE_ITEST_SQS_WORKER_QUEUE_CAPACITY", "256"))
    .WithEnvironment("SQS:Worker:BackgroundDrainTimeoutSeconds", Env("ASYNCRESPONSE_ITEST_SQS_WORKER_DRAIN_SECONDS", "10"))
    .WithEnvironment("SQS:HostShutdownTimeoutSeconds", "30")
    .WithEnvironment("AsyncResponse:KeyPrefix", SqsEarlyAckRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "SQS");
}

// NATS channel + NATS transport on one connection. A single NATS server backs both the response
// rendezvous (Core request/reply + JetStream KV recovery) and the worker/response transport (JetStream).
// earlyAck: as with SQS, the databases batch needs only the default variant.
void AddNatsApps(IResourceBuilder<ContainerResource> nats, bool earlyAck)
{
    var natsConnectionString = NatsConnectionString(nats);

    AddSutApp("itest-app-nats", waitFor: [nats])
    .WithEnvironment("Nats:Url", natsConnectionString)
    .WithEnvironment("Nats:SubjectPrefix", NatsSubjectPrefix)
    .WithEnvironment("Nats:RecoveryBucket", "itest-nats-recovery")
    .WithEnvironment("Nats:WorkerConsumer", "asyncresponse-itest-nats-workers")
    .WithEnvironment("Nats:ResponseConsumer", "asyncresponse-itest-nats-responses")
    .WithEnvironment("AsyncResponse:Channel", "NATS")
    .WithEnvironment("AsyncResponse:Transport", "NATS");

    if (!earlyAck)
        return;

    AddSutApp("itest-app-nats-early-ack", waitFor: [nats])
    .WithEnvironment("Nats:Url", natsConnectionString)
    .WithEnvironment("Nats:SubjectPrefix", NatsEarlyAckSubjectPrefix)
    .WithEnvironment("Nats:RecoveryBucket", "itest-nats-earlyack-recovery")
    .WithEnvironment("Nats:WorkerConsumer", "asyncresponse-itest-nats-workers-earlyack")
    .WithEnvironment("Nats:ResponseConsumer", "asyncresponse-itest-nats-responses-earlyack")
    .WithEnvironment("Nats:Worker:AckMode", Env("ASYNCRESPONSE_ITEST_NATS_WORKER_ACK_MODE", "AckAfterEnqueue"))
    .WithEnvironment("Nats:Worker:BackgroundWorkerCount", Env("ASYNCRESPONSE_ITEST_NATS_WORKER_BACKGROUND_WORKERS", "4"))
    .WithEnvironment("Nats:Worker:BackgroundQueueCapacity", Env("ASYNCRESPONSE_ITEST_NATS_WORKER_QUEUE_CAPACITY", "256"))
    .WithEnvironment("Nats:Worker:BackgroundDrainTimeoutSeconds", Env("ASYNCRESPONSE_ITEST_NATS_WORKER_DRAIN_SECONDS", "10"))
    .WithEnvironment("AsyncResponse:Channel", "NATS")
    .WithEnvironment("AsyncResponse:Transport", "NATS");
}

void AddKafkaApps(IResourceBuilder<RedisResource> redis, IResourceBuilder<KafkaServerResource> kafka)
{
    AddSutApp("itest-app-kafka", aotCapable: false, waitFor: [redis, kafka])
    .WithReference(redis)
    .WithReference(kafka)
    .WithEnvironment("Kafka:WorkerTopic", KafkaWorkerTopic)
    .WithEnvironment("Kafka:ResponseTopic", KafkaResponseTopic)
    .WithEnvironment("Kafka:WorkerConsumerGroup", "asyncresponse-itest-kafka-workers")
    .WithEnvironment("Kafka:ResponseConsumerGroup", "asyncresponse-itest-kafka-responses")
    .WithEnvironment("Kafka:TopicNumPartitions", Env("ASYNCRESPONSE_ITEST_KAFKA_TOPIC_PARTITIONS", "3"))
    .WithEnvironment("Kafka:Worker:MaxDeliveryAttempts", Env("ASYNCRESPONSE_ITEST_KAFKA_MAX_DELIVERY_ATTEMPTS", "5"))
    .WithEnvironment("Kafka:Response:MaxDeliveryAttempts", Env("ASYNCRESPONSE_ITEST_KAFKA_MAX_DELIVERY_ATTEMPTS", "5"))
    .WithEnvironment("AsyncResponse:KeyPrefix", KafkaRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "Kafka");

AddSutApp("itest-app-kafka-early-ack", aotCapable: false, waitFor: [redis, kafka])
    .WithReference(redis)
    .WithReference(kafka)
    .WithEnvironment("Kafka:WorkerTopic", KafkaEarlyAckWorkerTopic)
    .WithEnvironment("Kafka:ResponseTopic", KafkaEarlyAckResponseTopic)
    .WithEnvironment("Kafka:WorkerConsumerGroup", "asyncresponse-itest-kafka-workers-earlyack")
    .WithEnvironment("Kafka:ResponseConsumerGroup", "asyncresponse-itest-kafka-responses-earlyack")
    .WithEnvironment("Kafka:TopicNumPartitions", Env("ASYNCRESPONSE_ITEST_KAFKA_TOPIC_PARTITIONS", "3"))
    .WithEnvironment("Kafka:Worker:AckMode", Env("ASYNCRESPONSE_ITEST_KAFKA_WORKER_ACK_MODE", "AckAfterEnqueue"))
    .WithEnvironment("Kafka:Worker:BackgroundWorkerCount", Env("ASYNCRESPONSE_ITEST_KAFKA_WORKER_BACKGROUND_WORKERS", "4"))
    .WithEnvironment("Kafka:Worker:BackgroundQueueCapacity", Env("ASYNCRESPONSE_ITEST_KAFKA_WORKER_QUEUE_CAPACITY", "256"))
    .WithEnvironment("Kafka:Worker:BackgroundDrainTimeoutSeconds", Env("ASYNCRESPONSE_ITEST_KAFKA_WORKER_DRAIN_SECONDS", "10"))
    .WithEnvironment("Kafka:Worker:MaxDeliveryAttempts", Env("ASYNCRESPONSE_ITEST_KAFKA_MAX_DELIVERY_ATTEMPTS", "5"))
    .WithEnvironment("Kafka:Response:MaxDeliveryAttempts", Env("ASYNCRESPONSE_ITEST_KAFKA_MAX_DELIVERY_ATTEMPTS", "5"))
    .WithEnvironment("Kafka:HostShutdownTimeoutSeconds", "30")
    .WithEnvironment("AsyncResponse:KeyPrefix", KafkaEarlyAckRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "Kafka");
}

void AddPostgreSqlApps(IResourceBuilder<ContainerResource> postgres)
{
    var postgresConnectionString = PostgresConnectionString(postgres);

    AddSutApp("itest-app-postgresql", waitFor: [postgres])
    .WithEnvironment("ConnectionStrings:PostgreSQL", postgresConnectionString)
    .WithEnvironment("PostgreSQL:WorkerQueue", PostgreSqlWorkerQueue)
    .WithEnvironment("PostgreSQL:ResponseQueue", PostgreSqlResponseQueue)
    .WithEnvironment("PostgreSQL:DeadLetterQueue", PostgreSqlDeadLetterQueue)
    .WithEnvironment("AsyncResponse:Channel", "PostgreSQL")
    .WithEnvironment("AsyncResponse:Transport", "PostgreSQL");

    AddSutApp("itest-app-postgresql-early-ack", waitFor: [postgres])
        .WithEnvironment("ConnectionStrings:PostgreSQL", postgresConnectionString)
        .WithEnvironment("PostgreSQL:WorkerQueue", PostgreSqlEarlyAckWorkerQueue)
        .WithEnvironment("PostgreSQL:ResponseQueue", PostgreSqlEarlyAckResponseQueue)
        .WithEnvironment("PostgreSQL:DeadLetterQueue", PostgreSqlEarlyAckDeadLetterQueue)
        .WithEnvironment("PostgreSQL:Worker:AckMode", Env("ASYNCRESPONSE_ITEST_POSTGRESQL_WORKER_ACK_MODE", "AckAfterEnqueue"))
        .WithEnvironment("PostgreSQL:Worker:BackgroundWorkerCount", Env("ASYNCRESPONSE_ITEST_POSTGRESQL_WORKER_BACKGROUND_WORKERS", "4"))
        .WithEnvironment("PostgreSQL:Worker:BackgroundQueueCapacity", Env("ASYNCRESPONSE_ITEST_POSTGRESQL_WORKER_QUEUE_CAPACITY", "256"))
        .WithEnvironment("PostgreSQL:Worker:BackgroundDrainTimeoutSeconds", Env("ASYNCRESPONSE_ITEST_POSTGRESQL_WORKER_DRAIN_SECONDS", "10"))
        .WithEnvironment("PostgreSQL:HostShutdownTimeoutSeconds", "30")
        .WithEnvironment("AsyncResponse:Channel", "PostgreSQL")
        .WithEnvironment("AsyncResponse:Transport", "PostgreSQL");
}

void AddSqlServerApps(IResourceBuilder<ContainerResource> sqlserver)
{
    var sqlServerConnectionString = SqlServerConnectionString(sqlserver);

    AddSutApp("itest-app-sqlserver", aotCapable: false, waitFor: [sqlserver])
    .WithEnvironment("ConnectionStrings:SqlServer", sqlServerConnectionString)
    .WithEnvironment("SqlServer:SchemaName", SqlServerSchema)
    .WithEnvironment("SqlServer:WorkerQueue", SqlServerWorkerQueue)
    .WithEnvironment("SqlServer:ResponseQueue", SqlServerResponseQueue)
    .WithEnvironment("SqlServer:DeadLetterQueue", SqlServerDeadLetterQueue)
    .WithEnvironment("AsyncResponse:Channel", "SqlServer")
    .WithEnvironment("AsyncResponse:Transport", "SqlServer");

AddSutApp("itest-app-sqlserver-early-ack", aotCapable: false, waitFor: [sqlserver])
    .WithEnvironment("ConnectionStrings:SqlServer", sqlServerConnectionString)
    .WithEnvironment("SqlServer:SchemaName", SqlServerEarlyAckSchema)
    .WithEnvironment("SqlServer:WorkerQueue", SqlServerEarlyAckWorkerQueue)
    .WithEnvironment("SqlServer:ResponseQueue", SqlServerEarlyAckResponseQueue)
    .WithEnvironment("SqlServer:DeadLetterQueue", SqlServerEarlyAckDeadLetterQueue)
    .WithEnvironment("SqlServer:Worker:AckMode", Env("ASYNCRESPONSE_ITEST_SQLSERVER_WORKER_ACK_MODE", "AckAfterEnqueue"))
    .WithEnvironment("SqlServer:Worker:BackgroundWorkerCount", Env("ASYNCRESPONSE_ITEST_SQLSERVER_WORKER_BACKGROUND_WORKERS", "4"))
    .WithEnvironment("SqlServer:Worker:BackgroundQueueCapacity", Env("ASYNCRESPONSE_ITEST_SQLSERVER_WORKER_QUEUE_CAPACITY", "256"))
    .WithEnvironment("SqlServer:Worker:BackgroundDrainTimeoutSeconds", Env("ASYNCRESPONSE_ITEST_SQLSERVER_WORKER_DRAIN_SECONDS", "10"))
    .WithEnvironment("SqlServer:HostShutdownTimeoutSeconds", "30")
    .WithEnvironment("AsyncResponse:Channel", "SqlServer")
    .WithEnvironment("AsyncResponse:Transport", "SqlServer");
}

// MongoDB channel + transport on one shared client. The default variant also persists durable-flow
// ledgers through the AsyncResponse.DurableFlows.MongoDB package, so flow checkpoints, resumes, and
// state reads ride the same replica set as the channel and the worker queue.
void AddMongoDbApps(IResourceBuilder<ContainerResource> mongodb)
{
    var mongoDbConnectionString = MongoDbConnectionString(mongodb);

    AddSutApp("itest-app-mongodb", aotCapable: false, waitFor: [mongodb])
    .WithEnvironment("ConnectionStrings:MongoDB", mongoDbConnectionString)
    .WithEnvironment("MongoDB:DatabaseName", MongoDbDatabase)
    .WithEnvironment("MongoDB:WorkerQueue", MongoDbWorkerQueue)
    .WithEnvironment("MongoDB:ResponseQueue", MongoDbResponseQueue)
    .WithEnvironment("MongoDB:DeadLetterQueue", MongoDbDeadLetterQueue)
    .WithEnvironment("AsyncResponse:DurableFlowStore", "mongodb")
    .WithEnvironment("AsyncResponse:Channel", "MongoDB")
    .WithEnvironment("AsyncResponse:Transport", "MongoDB");

AddSutApp("itest-app-mongodb-early-ack", aotCapable: false, waitFor: [mongodb])
    .WithEnvironment("ConnectionStrings:MongoDB", mongoDbConnectionString)
    .WithEnvironment("MongoDB:DatabaseName", MongoDbEarlyAckDatabase)
    .WithEnvironment("MongoDB:WorkerQueue", MongoDbEarlyAckWorkerQueue)
    .WithEnvironment("MongoDB:ResponseQueue", MongoDbEarlyAckResponseQueue)
    .WithEnvironment("MongoDB:DeadLetterQueue", MongoDbEarlyAckDeadLetterQueue)
    .WithEnvironment("MongoDB:Worker:AckMode", Env("ASYNCRESPONSE_ITEST_MONGODB_WORKER_ACK_MODE", "AckAfterEnqueue"))
    .WithEnvironment("MongoDB:Worker:BackgroundWorkerCount", Env("ASYNCRESPONSE_ITEST_MONGODB_WORKER_BACKGROUND_WORKERS", "4"))
    .WithEnvironment("MongoDB:Worker:BackgroundQueueCapacity", Env("ASYNCRESPONSE_ITEST_MONGODB_WORKER_QUEUE_CAPACITY", "256"))
    .WithEnvironment("MongoDB:Worker:BackgroundDrainTimeoutSeconds", Env("ASYNCRESPONSE_ITEST_MONGODB_WORKER_DRAIN_SECONDS", "10"))
    .WithEnvironment("MongoDB:HostShutdownTimeoutSeconds", "30")
    .WithEnvironment("AsyncResponse:Channel", "MongoDB")
    .WithEnvironment("AsyncResponse:Transport", "MongoDB");
}

// --- Batches ------------------------------------------------------------------------------------
// A batch declares only the resources its test collection touches. The test project boots one AppHost
// per batch and disposes it before the next batch starts (collections run sequentially), so peak
// footprint is the largest batch rather than the whole fleet.
//
// The split follows the one structural fact that matters: a test either drives a sample app over HTTP
// or it drives a driver directly. The direct tests need no sample app at all, which is why
// "conformance", "stores", and "oracle-cosmos" start zero processes. The app-driven half splits by
// transport family.
//
// Batches are balanced on measured MEMORY, not container count — counting containers hid a 2.3x
// spread (7 small containers can cost more than 8 large-sounding ones). The two SQL Servers, Oracle,
// and the Cosmos emulator dominate everything else, so the split is really about keeping those apart.
//
// Batch names are the contract with the fixtures in Batches.cs — keep them in sync.
switch (Env("ASYNCRESPONSE_ITEST_BATCH", "").ToLowerInvariant())
{
    // Everything that talks to a database: channel conformance, the store contracts, the "direct"
    // driver tests, and the database channel/transport SUTs. These were three separate batches, which
    // meant starting SQL Server, PostgreSQL, and MongoDB three times each — SQL Server alone takes the
    // better part of a minute to accept logins. They share one batch so each starts once; splitting
    // them bought nothing, because the batch's cost is dominated by SQL Server either way.
    case "data":
    {
        var redis = AddRedisContainer();
        var postgres = AddPostgresContainer();
        var sqlserver = AddSqlServerContainer();
        var mongodb = AddMongoDbContainer();
        var nats = AddNatsContainer();
        var localstack = AddLocalStackContainer();
        AddMySqlContainer(); // store contract only — no sample app uses MySQL

        AddPubSubApps(redis, AddPubSubContainer(), earlyAck: false);
        AddPostgreSqlApps(postgres);
        AddSqlServerApps(sqlserver);
        AddMongoDbApps(mongodb);
        AddNatsApps(nats, earlyAck: false);
        AddSqsApps(redis, localstack, earlyAck: false);
        break;
    }

    // Oracle and Cosmos alone. Measured 2,180 MiB and 1,031 MiB — together more than half a default
    // Docker VM, and between them they back exactly two tests. Kept in "stores" they made that batch
    // 5.8 GiB, which failed as soon as anything else was running. Isolated, they compete with nothing.
    case "oracle-cosmos":
        if (!SkipOracleCosmos())
        {
            AddOracleContainer();
            AddCosmosContainer();
        }

        break;

    // Message brokers proper: all small (Kafka is the largest at ~400 MiB), so they share a batch.
    case "brokers":
    {
        var redis = AddRedisContainer();
        AddPubSubApps(redis, AddPubSubContainer(), earlyAck: true);
        AddRabbitMqApps(redis, AddRabbitMqContainer());
        AddKafkaApps(redis, AddKafkaContainer());
        AddNatsApps(AddNatsContainer(), earlyAck: true);
        AddRedisTransportApp(redis, "itest-app-redis", earlyAck: false);
        AddRedisTransportApp(redis, "itest-app-redis-early-ack", earlyAck: true);
        break;
    }

    // The two cloud emulators. Split out of "brokers" because the Service Bus emulator drags in a
    // second full SQL Server, which made brokers the heaviest app-driven batch on its own.
    case "cloud":
    {
        var redis = AddRedisContainer();
        AddServiceBusApps(redis, AddServiceBusContainer());
        AddSqsApps(redis, AddLocalStackContainer(), earlyAck: true);
        break;
    }

    // Not a test batch: the whole fleet in one AppHost, for benchmarks/AsyncResponse.LoadTests, which
    // drives every transport at once and boots this AppHost directly. Batching exists to bound the
    // *test suite's* peak footprint; the load test wants everything up simultaneously by definition,
    // so it asks for this. It needs no MySQL, Oracle, or Cosmos — nothing load-tested touches them.
    case "loadtest":
    {
        var redis = AddRedisContainer();
        AddPubSubApps(redis, AddPubSubContainer(), earlyAck: true);
        AddRabbitMqApps(redis, AddRabbitMqContainer());
        AddKafkaApps(redis, AddKafkaContainer());
        AddNatsApps(AddNatsContainer(), earlyAck: true);
        AddServiceBusApps(redis, AddServiceBusContainer());
        AddSqsApps(redis, AddLocalStackContainer(), earlyAck: true);
        AddRedisTransportApp(redis, "itest-app-redis", earlyAck: false);
        AddRedisTransportApp(redis, "itest-app-redis-early-ack", earlyAck: true);
        AddPostgreSqlApps(AddPostgresContainer());
        AddSqlServerApps(AddSqlServerContainer());
        AddMongoDbApps(AddMongoDbContainer());
        break;
    }

    // Unknown or unset: fail loudly. A silent fallback would boot the wrong fleet and the tests would
    // fail far from the cause — which is exactly what a stale build of this file once did.
    case var unknown:
        throw new InvalidOperationException(
            $"ASYNCRESPONSE_ITEST_BATCH must be set. Got '{unknown}'. Test batches: data, oracle-cosmos, " +
            "brokers, cloud (the integration fixtures set these). The load test uses: loadtest. " +
            "If this fires from the test suite, the AppHost build is stale relative to the tests.");
}

builder.Build().Run();
