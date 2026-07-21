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
var redis = builder.AddRedis("redis");
if (Env("ASYNCRESPONSE_ITEST_REDIS_IMAGE", "") is { Length: > 0 } redisImage)
{
    if (Env("ASYNCRESPONSE_ITEST_REDIS_REGISTRY", "") is { Length: > 0 } redisRegistry)
        redis = redis.WithImageRegistry(redisRegistry);
    redis = redis.WithImage(redisImage, Env("ASYNCRESPONSE_ITEST_REDIS_TAG", "latest"));
}

IResourceBuilder<IResourceWithEnvironment> AddRedisTransportApp(string name, bool earlyAck)
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
// exhausts a hosted runner or loses a transient port race.
if (string.Equals(Env("ASYNCRESPONSE_ITEST_PROFILE", ""), "redis-compat", StringComparison.OrdinalIgnoreCase))
{
    AddRedisTransportApp("itest-app-redis", earlyAck: false);
    AddRedisTransportApp("itest-app-redis-early-ack", earlyAck: true);
    builder.Build().Run();
    return;
}

var rabbitmq = builder.AddContainer("rabbitmq", "rabbitmq", "3.13-management")
    .WithEndpoint(targetPort: 5672, scheme: "tcp", name: "amqp")
    .WithEndpoint(targetPort: 15672, scheme: "http", name: "management");

var pubsub = builder.AddContainer("pubsub", "gcr.io/google.com/cloudsdktool/google-cloud-cli", "446.0.1-emulators")
    .WithArgs("gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085", $"--project={ProjectId}")
    .WithEndpoint(targetPort: 8085, scheme: "tcp", name: "pubsub");

// `-js` enables JetStream, which the NATS channel's Key-Value recovery store and the NATS transport's
// streams both require.
var nats = builder.AddContainer("nats", "nats", "latest")
    .WithArgs("-js")
    .WithEndpoint(targetPort: 4222, scheme: "tcp", name: "nats");

// Single-broker KRaft Kafka (the Aspire integration uses the confluent-local image). One broker backs
// both Kafka app variants; they isolate through distinct topics and consumer groups. This container
// doubles as the roadmap's Redpanda-compatibility reference: everything speaks the Kafka protocol.
var kafka = builder.AddKafka("kafka");

// Two PostgreSQL app instances (default + early-ack) share this one server, each with its own Npgsql
// pool. The image default max_connections=100 is exhausted under the load-test profile ("FATAL: sorry,
// too many clients already"). Raise the server ceiling well above the combined pool budget below
// (2 apps x Maximum Pool Size=120 = 240) so neither the load test nor parallel integration apps starve.
var postgres = builder.AddContainer("postgres", "postgres", "16-alpine")
    .WithEnvironment("POSTGRES_DB", "asyncresponse")
    .WithEnvironment("POSTGRES_PASSWORD", "postgres")
    .WithArgs("-c", "max_connections=400")
    .WithEndpoint(targetPort: 5432, scheme: "tcp", name: "postgres");

var mysql = builder.AddContainer("mysql", "mysql", "8.4")
    .WithEnvironment("MYSQL_DATABASE", "asyncresponse")
    .WithEnvironment("MYSQL_ROOT_PASSWORD", "mysql")
    .WithEndpoint(targetPort: 3306, scheme: "tcp", name: "mysql");

// Single-node replica set: change streams — the MongoDB channel's waiter wake and the transport's
// subscriber wake — require one. The entrypoint wrapper starts mongod with --replSet and initiates
// the set as soon as the server answers; clients connect with directConnection=true so they never
// chase the replica-set-advertised container hostname, which is unreachable from the host network.
var mongodb = builder.AddContainer("mongodb", "mongo", "7")
    .WithEntrypoint("bash")
    .WithArgs(
        "-c",
        "mongod --replSet rs0 --bind_ip_all & MONGOD_PID=$!; " +
        "until mongosh --quiet --eval 'try { rs.status().ok } catch (e) { rs.initiate().ok }' >/dev/null 2>&1; do sleep 0.5; done; " +
        "wait $MONGOD_PID")
    .WithEndpoint(targetPort: 27017, scheme: "tcp", name: "mongodb");

// Oracle and Cosmos back the durable-flow store contract tests, which run in default CI. They are
// not part of the broker/load-test SUT, so workflows that don't need them (load tests, the weekly
// redis-compat matrix) skip their heavyweight containers via this flag.
if (!string.Equals(Env("ASYNCRESPONSE_ITEST_SKIP_ORACLE_COSMOS", "false"), "true", StringComparison.OrdinalIgnoreCase))
{
    var oracleAppPassword = Env("ASYNCRESPONSE_ITEST_ORACLE_APP_PASSWORD", "AsyncResponse12345");
    var oracle = builder.AddContainer("oracle", "gvenzl/oracle-free", "23-slim")
        .WithEnvironment("ORACLE_PASSWORD", Env("ASYNCRESPONSE_ITEST_ORACLE_ADMIN_PASSWORD", "AsyncResponse12345"))
        .WithEnvironment("APP_USER", OracleAppUser)
        .WithEnvironment("APP_USER_PASSWORD", oracleAppPassword)
        .WithEndpoint(targetPort: 1521, scheme: "tcp", name: "oracle");

    var cosmos = builder.AddContainer("cosmos", "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator", "vnext-latest")
        .WithEnvironment("PROTOCOL", "https")
        .WithEndpoint(targetPort: 8081, scheme: "https", name: "gateway")
        .WithEndpoint(targetPort: 8080, scheme: "http", name: "health")
        .WithHttpHealthCheck("/ready", endpointName: "health");
}

// Dedicated SQL Server for the SqlServer channel + transport SUTs (separate from the one backing the
// Azure Service Bus emulator, so the two suites cannot interfere). Both SqlServer app variants share
// it; the sample app provisions the database and each variant isolates through its own schema.
var sqlServerPassword = Env("ASYNCRESPONSE_ITEST_SQLSERVER_PASSWORD", "P@ssword12345");
var sqlserver = builder.AddContainer("sqlserver", "mcr.microsoft.com/mssql/server", "2022-latest")
    .WithEnvironment("ACCEPT_EULA", "Y")
    .WithEnvironment("MSSQL_SA_PASSWORD", sqlServerPassword)
    .WithEndpoint(targetPort: 1433, scheme: "tcp", name: "sqlserver");

var serviceBusSqlPassword = Env("ASYNCRESPONSE_ITEST_SERVICEBUS_SQL_PASSWORD", "P@ssword12345");
var serviceBusSql = builder.AddContainer("servicebus-sql", "mcr.microsoft.com/mssql/server", "2022-latest")
    .WithEnvironment("ACCEPT_EULA", "Y")
    .WithEnvironment("MSSQL_SA_PASSWORD", serviceBusSqlPassword);
var serviceBusConfigPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "servicebus-emulator-config.json"));
var serviceBus = builder.AddContainer("servicebus", "mcr.microsoft.com/azure-messaging/servicebus-emulator", "latest")
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

// LocalStack emulates AWS SQS for the SQS transport SUTs. Only the SQS service is enabled; the
// sample app provisions its queues (and redrive-policy dead-letter queues) through the transport's
// CreateQueues option, so no config file or init script is needed.
var localstack = builder.AddContainer("localstack", "localstack/localstack", "3")
    .WithEnvironment("SERVICES", "sqs,dynamodb")
    .WithEnvironment("EAGER_SERVICE_LOADING", "1")
    .WithEndpoint(targetPort: 4566, scheme: "http", name: "edge")
    .WithHttpHealthCheck("/_localstack/health", endpointName: "edge");

var pubsubEndpoint = pubsub.GetEndpoint("pubsub");
var emulatorHost = ReferenceExpression.Create(
    $"{pubsubEndpoint.Property(EndpointProperty.Host)}:{pubsubEndpoint.Property(EndpointProperty.Port)}");
var rabbitMqEndpoint = rabbitmq.GetEndpoint("amqp");
var rabbitMqConnectionString = ReferenceExpression.Create(
    $"amqp://guest:guest@{rabbitMqEndpoint.Property(EndpointProperty.Host)}:{rabbitMqEndpoint.Property(EndpointProperty.Port)}/");
var serviceBusEndpoint = serviceBus.GetEndpoint("amqp");
var serviceBusConnectionString = ReferenceExpression.Create(
    $"Endpoint=sb://{serviceBusEndpoint.Property(EndpointProperty.Host)}:{serviceBusEndpoint.Property(EndpointProperty.Port)};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;");
var localstackEndpoint = localstack.GetEndpoint("edge");
var localstackServiceUrl = ReferenceExpression.Create(
    $"http://{localstackEndpoint.Property(EndpointProperty.Host)}:{localstackEndpoint.Property(EndpointProperty.Port)}");
var natsEndpoint = nats.GetEndpoint("nats");
var natsConnectionString = ReferenceExpression.Create(
    $"nats://{natsEndpoint.Property(EndpointProperty.Host)}:{natsEndpoint.Property(EndpointProperty.Port)}");
var postgresEndpoint = postgres.GetEndpoint("postgres");
// Cap each app's Npgsql pool so the two PostgreSQL instances sharing the server above cannot, even
// combined (2 x 120 = 240), exceed its max_connections=400 ceiling — bounding aggregate connection use
// rather than letting Npgsql's default (100 per app) race the server limit under load.
//
// "No Reset On Close=true" drops the per-checkin DISCARD ALL (the single most-executed statement under
// load) and lets "Max Auto Prepare" actually retain prepared statements across pooled reuse; together
// they roughly halve server-side statements and cut parse/plan CPU — decisive on the load-test runner
// where one small PostgreSQL server backs every PostgreSQL scenario at once. The channel only LISTENs on
// dedicated long-lived connections, so skipping reset on the pooled query connections is safe.
var postgresConnectionString = ReferenceExpression.Create(
    $"Host={postgresEndpoint.Property(EndpointProperty.Host)};Port={postgresEndpoint.Property(EndpointProperty.Port)};Username=postgres;Password=postgres;Database=asyncresponse;Maximum Pool Size=120;No Reset On Close=true;Max Auto Prepare=20");

var mongoDbEndpoint = mongodb.GetEndpoint("mongodb");
var mongoDbConnectionString = ReferenceExpression.Create(
    $"mongodb://{mongoDbEndpoint.Property(EndpointProperty.Host)}:{mongoDbEndpoint.Property(EndpointProperty.Port)}/?directConnection=true");

// Cap each app's SqlClient pool (2 apps x 120 = 240) well under SQL Server's default connection
// ceiling, mirroring the PostgreSQL budget above. TrustServerCertificate accepts the container's
// self-signed certificate.
var sqlServerEndpoint = sqlserver.GetEndpoint("sqlserver");
var sqlServerConnectionString = ReferenceExpression.Create(
    $"Server={sqlServerEndpoint.Property(EndpointProperty.Host)},{sqlServerEndpoint.Property(EndpointProperty.Port)};User ID=sa;Password={sqlServerPassword};Database=asyncresponse;TrustServerCertificate=True;Max Pool Size=120");

// The integration SUT is the sample app itself (one app, no duplication), booted here with the
// Redis channel + Google Pub/Sub transport. AddSutApp owns the endpoint and the /alive health
// check, and switches between project (JIT) and Native AOT binary per ASYNCRESPONSE_ITEST_SUT.
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
    .WithEnvironment("AzureServiceBus:Worker:AckMode", Env("ASYNCRESPONSE_ITEST_AZURE_SERVICEBUS_WORKER_ACK_MODE", "AckAfterReceive"))
    .WithEnvironment("AzureServiceBus:Worker:BackgroundWorkerCount", Env("ASYNCRESPONSE_ITEST_AZURE_SERVICEBUS_WORKER_BACKGROUND_WORKERS", "4"))
    .WithEnvironment("AzureServiceBus:Worker:BackgroundQueueCapacity", Env("ASYNCRESPONSE_ITEST_AZURE_SERVICEBUS_WORKER_QUEUE_CAPACITY", "256"))
    .WithEnvironment("AzureServiceBus:Worker:BackgroundDrainTimeoutSeconds", Env("ASYNCRESPONSE_ITEST_AZURE_SERVICEBUS_WORKER_DRAIN_SECONDS", "10"))
    .WithEnvironment("AzureServiceBus:HostShutdownTimeoutSeconds", "30")
    .WithEnvironment("AsyncResponse:KeyPrefix", AzureServiceBusEarlyAckRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "AzureServiceBus");

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

AddRedisTransportApp("itest-app-redis", earlyAck: false);
AddRedisTransportApp("itest-app-redis-early-ack", earlyAck: true);

// NATS channel + NATS transport on one connection. A single NATS server backs both the response
// rendezvous (Core request/reply + JetStream KV recovery) and the worker/response transport (JetStream).
AddSutApp("itest-app-nats", waitFor: [nats])
    .WithEnvironment("Nats:Url", natsConnectionString)
    .WithEnvironment("Nats:SubjectPrefix", NatsSubjectPrefix)
    .WithEnvironment("Nats:RecoveryBucket", "itest-nats-recovery")
    .WithEnvironment("Nats:WorkerConsumer", "asyncresponse-itest-nats-workers")
    .WithEnvironment("Nats:ResponseConsumer", "asyncresponse-itest-nats-responses")
    .WithEnvironment("AsyncResponse:Channel", "NATS")
    .WithEnvironment("AsyncResponse:Transport", "NATS");

AddSutApp("itest-app-nats-early-ack", waitFor: [nats])
    .WithEnvironment("Nats:Url", natsConnectionString)
    .WithEnvironment("Nats:SubjectPrefix", NatsEarlyAckSubjectPrefix)
    .WithEnvironment("Nats:RecoveryBucket", "itest-nats-earlyack-recovery")
    .WithEnvironment("Nats:WorkerConsumer", "asyncresponse-itest-nats-workers-earlyack")
    .WithEnvironment("Nats:ResponseConsumer", "asyncresponse-itest-nats-responses-earlyack")
    .WithEnvironment("Nats:Worker:AckMode", Env("ASYNCRESPONSE_ITEST_NATS_WORKER_ACK_MODE", "AckAfterReceive"))
    .WithEnvironment("Nats:Worker:BackgroundWorkerCount", Env("ASYNCRESPONSE_ITEST_NATS_WORKER_BACKGROUND_WORKERS", "4"))
    .WithEnvironment("Nats:Worker:BackgroundQueueCapacity", Env("ASYNCRESPONSE_ITEST_NATS_WORKER_QUEUE_CAPACITY", "256"))
    .WithEnvironment("Nats:Worker:BackgroundDrainTimeoutSeconds", Env("ASYNCRESPONSE_ITEST_NATS_WORKER_DRAIN_SECONDS", "10"))
    .WithEnvironment("AsyncResponse:Channel", "NATS")
    .WithEnvironment("AsyncResponse:Transport", "NATS");

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

AddSutApp("itest-app-postgresql", waitFor: [postgres])
    .WithEnvironment("ConnectionStrings:PostgreSQL", postgresConnectionString)
    .WithEnvironment("PostgreSQL:WorkerQueue", PostgreSqlWorkerQueue)
    .WithEnvironment("PostgreSQL:ResponseQueue", PostgreSqlResponseQueue)
    .WithEnvironment("PostgreSQL:DeadLetterQueue", PostgreSqlDeadLetterQueue)
    .WithEnvironment("AsyncResponse:Channel", "PostgreSQL")
    .WithEnvironment("AsyncResponse:Transport", "PostgreSQL");

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

AddSutApp("itest-app-postgresql-early-ack", waitFor: [postgres])
    .WithEnvironment("ConnectionStrings:PostgreSQL", postgresConnectionString)
    .WithEnvironment("PostgreSQL:WorkerQueue", PostgreSqlEarlyAckWorkerQueue)
    .WithEnvironment("PostgreSQL:ResponseQueue", PostgreSqlEarlyAckResponseQueue)
    .WithEnvironment("PostgreSQL:DeadLetterQueue", PostgreSqlEarlyAckDeadLetterQueue)
    .WithEnvironment("PostgreSQL:Worker:AckMode", Env("ASYNCRESPONSE_ITEST_POSTGRESQL_WORKER_ACK_MODE", "AckAfterReceive"))
    .WithEnvironment("PostgreSQL:Worker:BackgroundWorkerCount", Env("ASYNCRESPONSE_ITEST_POSTGRESQL_WORKER_BACKGROUND_WORKERS", "4"))
    .WithEnvironment("PostgreSQL:Worker:BackgroundQueueCapacity", Env("ASYNCRESPONSE_ITEST_POSTGRESQL_WORKER_QUEUE_CAPACITY", "256"))
    .WithEnvironment("PostgreSQL:Worker:BackgroundDrainTimeoutSeconds", Env("ASYNCRESPONSE_ITEST_POSTGRESQL_WORKER_DRAIN_SECONDS", "10"))
    .WithEnvironment("PostgreSQL:HostShutdownTimeoutSeconds", "30")
    .WithEnvironment("AsyncResponse:Channel", "PostgreSQL")
    .WithEnvironment("AsyncResponse:Transport", "PostgreSQL");

// MongoDB channel + transport on one shared client. The default variant also persists durable-flow
// ledgers through the AsyncResponse.DurableFlows.MongoDB package, so flow checkpoints, resumes, and
// state reads ride the same replica set as the channel and the worker queue.
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

builder.Build().Run();
