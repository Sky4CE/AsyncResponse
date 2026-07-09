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
const string OracleAppUser = "asyncresponse";

static string Env(string name, string fallback)
    => Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : fallback;

var builder = DistributedApplication.CreateBuilder(args);
builder.Services.Configure<LoggerFilterOptions>(options =>
    options.AddFilter("Microsoft.Extensions.Diagnostics.HealthChecks.DefaultHealthCheckService", LogLevel.Critical));

// The Redis channel + transport speak RESP via StackExchange.Redis, so they run unchanged on
// Redis-compatible servers. The CI compatibility matrix overrides the image via these env vars to run
// the whole Redis-backed suite against Valkey; the default is the official Redis. Only servers that
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

var mongodb = builder.AddContainer("mongodb", "mongo", "7")
    .WithEndpoint(targetPort: 27017, scheme: "tcp", name: "mongodb");

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

// Cap each app's SqlClient pool (2 apps x 120 = 240) well under SQL Server's default connection
// ceiling, mirroring the PostgreSQL budget above. TrustServerCertificate accepts the container's
// self-signed certificate.
var sqlServerEndpoint = sqlserver.GetEndpoint("sqlserver");
var sqlServerConnectionString = ReferenceExpression.Create(
    $"Server={sqlServerEndpoint.Property(EndpointProperty.Host)},{sqlServerEndpoint.Property(EndpointProperty.Port)};User ID=sa;Password={sqlServerPassword};Database=asyncresponse;TrustServerCertificate=True;Max Pool Size=120");

// The integration SUT is the sample app itself (one app, no duplication), booted here with the
// Redis channel + Google Pub/Sub transport. launchProfileName: null disables the sample's launch
// profile so the AppHost owns the endpoint (WithHttpEndpoint), matching how it provisions ports.
builder.AddProject<Projects.AsyncResponse_Sample>("itest-app", launchProfileName: null)
    .WithReference(redis)
    .WaitFor(redis)
    .WaitFor(pubsub)
    .WithEnvironment("PUBSUB_EMULATOR_HOST", emulatorHost)
    .WithEnvironment("PubSub:ProjectId", ProjectId)
    .WithEnvironment("PubSub:WorkerTopicId", WorkerTopic)
    .WithEnvironment("PubSub:WorkerSubscriptionId", WorkerSubscription)
    .WithEnvironment("PubSub:ResponseTopicId", ResponseTopic)
    .WithEnvironment("PubSub:ResponseSubscriptionId", ResponseSubscription)
    .WithEnvironment("AsyncResponse:KeyPrefix", TestRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "GooglePubSub")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-early-ack", launchProfileName: null)
    .WithReference(redis)
    .WaitFor(redis)
    .WaitFor(pubsub)
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
    .WithEnvironment("AsyncResponse:Transport", "GooglePubSub")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-rabbitmq", launchProfileName: null)
    .WithReference(redis)
    .WaitFor(redis)
    .WaitFor(rabbitmq)
    .WithEnvironment("RabbitMQ:ConnectionString", rabbitMqConnectionString)
    .WithEnvironment("RabbitMQ:WorkerExchange", RabbitMqWorkerExchange)
    .WithEnvironment("RabbitMQ:WorkerQueue", RabbitMqWorkerQueue)
    .WithEnvironment("RabbitMQ:WorkerRoutingKey", RabbitMqWorkerRoutingKey)
    .WithEnvironment("RabbitMQ:ResponseExchange", RabbitMqResponseExchange)
    .WithEnvironment("RabbitMQ:ResponseQueue", RabbitMqResponseQueue)
    .WithEnvironment("RabbitMQ:ResponseRoutingKey", RabbitMqResponseRoutingKey)
    .WithEnvironment("AsyncResponse:KeyPrefix", RabbitMqRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "RabbitMQ")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-rabbitmq-early-ack", launchProfileName: null)
    .WithReference(redis)
    .WaitFor(redis)
    .WaitFor(rabbitmq)
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
    .WithEnvironment("AsyncResponse:Transport", "RabbitMQ")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-azure-servicebus", launchProfileName: null)
    .WithReference(redis)
    .WaitFor(redis)
    .WaitFor(serviceBus)
    .WithEnvironment("ConnectionStrings:AzureServiceBus", serviceBusConnectionString)
    .WithEnvironment("AzureServiceBus:WorkerQueue", AzureServiceBusWorkerQueue)
    .WithEnvironment("AzureServiceBus:ResponseQueue", AzureServiceBusResponseQueue)
    .WithEnvironment("AsyncResponse:KeyPrefix", AzureServiceBusRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "AzureServiceBus")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-azure-servicebus-early-ack", launchProfileName: null)
    .WithReference(redis)
    .WaitFor(redis)
    .WaitFor(serviceBus)
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
    .WithEnvironment("AsyncResponse:Transport", "AzureServiceBus")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-sqs", launchProfileName: null)
    .WithReference(redis)
    .WaitFor(redis)
    .WaitFor(localstack)
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
    .WithEnvironment("AsyncResponse:Transport", "SQS")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-sqs-early-ack", launchProfileName: null)
    .WithReference(redis)
    .WaitFor(redis)
    .WaitFor(localstack)
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
    .WithEnvironment("AsyncResponse:Transport", "SQS")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-redis", launchProfileName: null)
    .WithReference(redis)
    .WaitFor(redis)
    .WithEnvironment("AsyncResponse:KeyPrefix", RedisTransportKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "Redis")
    .WithEnvironment("Redis:KeyPrefix", RedisTransportKeyPrefix)
    .WithEnvironment("Redis:WorkerConsumerGroup", "asyncresponse-itest-redis-workers")
    .WithEnvironment("Redis:ResponseConsumerGroup", "asyncresponse-itest-redis-responses")
    .WithEnvironment("Redis:StreamMaxLength", Env("ASYNCRESPONSE_ITEST_REDIS_STREAM_MAX_LENGTH", "100000"))
    .WithEnvironment("Redis:PublishMaxAttempts", Env("ASYNCRESPONSE_ITEST_REDIS_PUBLISH_MAX_ATTEMPTS", "3"))
    .WithEnvironment("Redis:Worker:MaxDeliveryAttempts", Env("ASYNCRESPONSE_ITEST_REDIS_MAX_DELIVERY_ATTEMPTS", "5"))
    .WithEnvironment("Redis:Response:MaxDeliveryAttempts", Env("ASYNCRESPONSE_ITEST_REDIS_MAX_DELIVERY_ATTEMPTS", "5"))
    .WithEnvironment("Redis:Worker:PendingMessageMinIdleTimeSeconds", Env("ASYNCRESPONSE_ITEST_REDIS_PENDING_IDLE_SECONDS", "1"))
    .WithEnvironment("Redis:Response:PendingMessageMinIdleTimeSeconds", Env("ASYNCRESPONSE_ITEST_REDIS_PENDING_IDLE_SECONDS", "1"))
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-redis-early-ack", launchProfileName: null)
    .WithReference(redis)
    .WaitFor(redis)
    .WithEnvironment("AsyncResponse:KeyPrefix", RedisTransportEarlyAckKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "Redis")
    .WithEnvironment("Redis:KeyPrefix", RedisTransportEarlyAckKeyPrefix)
    .WithEnvironment("Redis:WorkerConsumerGroup", "asyncresponse-itest-redis-workers-earlyack")
    .WithEnvironment("Redis:ResponseConsumerGroup", "asyncresponse-itest-redis-responses-earlyack")
    .WithEnvironment("Redis:StreamMaxLength", Env("ASYNCRESPONSE_ITEST_REDIS_STREAM_MAX_LENGTH", "100000"))
    .WithEnvironment("Redis:PublishMaxAttempts", Env("ASYNCRESPONSE_ITEST_REDIS_PUBLISH_MAX_ATTEMPTS", "3"))
    .WithEnvironment("Redis:Worker:AckMode", Env("ASYNCRESPONSE_ITEST_REDIS_WORKER_ACK_MODE", "AckAfterEnqueue"))
    .WithEnvironment("Redis:Worker:BackgroundWorkerCount", Env("ASYNCRESPONSE_ITEST_REDIS_WORKER_BACKGROUND_WORKERS", "4"))
    .WithEnvironment("Redis:Worker:BackgroundQueueCapacity", Env("ASYNCRESPONSE_ITEST_REDIS_WORKER_QUEUE_CAPACITY", "256"))
    .WithEnvironment("Redis:Worker:BackgroundDrainTimeoutSeconds", Env("ASYNCRESPONSE_ITEST_REDIS_WORKER_DRAIN_SECONDS", "10"))
    .WithEnvironment("Redis:Worker:MaxDeliveryAttempts", Env("ASYNCRESPONSE_ITEST_REDIS_MAX_DELIVERY_ATTEMPTS", "5"))
    .WithEnvironment("Redis:Response:MaxDeliveryAttempts", Env("ASYNCRESPONSE_ITEST_REDIS_MAX_DELIVERY_ATTEMPTS", "5"))
    .WithEnvironment("Redis:Worker:PendingMessageMinIdleTimeSeconds", Env("ASYNCRESPONSE_ITEST_REDIS_PENDING_IDLE_SECONDS", "1"))
    .WithEnvironment("Redis:Response:PendingMessageMinIdleTimeSeconds", Env("ASYNCRESPONSE_ITEST_REDIS_PENDING_IDLE_SECONDS", "1"))
    .WithEnvironment("Redis:HostShutdownTimeoutSeconds", "30")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

// NATS channel + NATS transport on one connection. A single NATS server backs both the response
// rendezvous (Core request/reply + JetStream KV recovery) and the worker/response transport (JetStream).
builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-nats", launchProfileName: null)
    .WaitFor(nats)
    .WithEnvironment("Nats:Url", natsConnectionString)
    .WithEnvironment("Nats:SubjectPrefix", NatsSubjectPrefix)
    .WithEnvironment("Nats:RecoveryBucket", "itest-nats-recovery")
    .WithEnvironment("Nats:WorkerConsumer", "asyncresponse-itest-nats-workers")
    .WithEnvironment("Nats:ResponseConsumer", "asyncresponse-itest-nats-responses")
    .WithEnvironment("AsyncResponse:Channel", "NATS")
    .WithEnvironment("AsyncResponse:Transport", "NATS")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-nats-early-ack", launchProfileName: null)
    .WaitFor(nats)
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
    .WithEnvironment("AsyncResponse:Transport", "NATS")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-kafka", launchProfileName: null)
    .WithReference(redis)
    .WithReference(kafka)
    .WaitFor(redis)
    .WaitFor(kafka)
    .WithEnvironment("Kafka:WorkerTopic", KafkaWorkerTopic)
    .WithEnvironment("Kafka:ResponseTopic", KafkaResponseTopic)
    .WithEnvironment("Kafka:WorkerConsumerGroup", "asyncresponse-itest-kafka-workers")
    .WithEnvironment("Kafka:ResponseConsumerGroup", "asyncresponse-itest-kafka-responses")
    .WithEnvironment("Kafka:TopicNumPartitions", Env("ASYNCRESPONSE_ITEST_KAFKA_TOPIC_PARTITIONS", "3"))
    .WithEnvironment("Kafka:Worker:MaxDeliveryAttempts", Env("ASYNCRESPONSE_ITEST_KAFKA_MAX_DELIVERY_ATTEMPTS", "5"))
    .WithEnvironment("Kafka:Response:MaxDeliveryAttempts", Env("ASYNCRESPONSE_ITEST_KAFKA_MAX_DELIVERY_ATTEMPTS", "5"))
    .WithEnvironment("AsyncResponse:KeyPrefix", KafkaRedisKeyPrefix)
    .WithEnvironment("AsyncResponse:Channel", "Redis")
    .WithEnvironment("AsyncResponse:Transport", "Kafka")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-kafka-early-ack", launchProfileName: null)
    .WithReference(redis)
    .WithReference(kafka)
    .WaitFor(redis)
    .WaitFor(kafka)
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
    .WithEnvironment("AsyncResponse:Transport", "Kafka")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-postgresql", launchProfileName: null)
    .WaitFor(postgres)
    .WithEnvironment("ConnectionStrings:PostgreSQL", postgresConnectionString)
    .WithEnvironment("PostgreSQL:WorkerQueue", PostgreSqlWorkerQueue)
    .WithEnvironment("PostgreSQL:ResponseQueue", PostgreSqlResponseQueue)
    .WithEnvironment("PostgreSQL:DeadLetterQueue", PostgreSqlDeadLetterQueue)
    .WithEnvironment("AsyncResponse:Channel", "PostgreSQL")
    .WithEnvironment("AsyncResponse:Transport", "PostgreSQL")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-sqlserver", launchProfileName: null)
    .WaitFor(sqlserver)
    .WithEnvironment("ConnectionStrings:SqlServer", sqlServerConnectionString)
    .WithEnvironment("SqlServer:SchemaName", SqlServerSchema)
    .WithEnvironment("SqlServer:WorkerQueue", SqlServerWorkerQueue)
    .WithEnvironment("SqlServer:ResponseQueue", SqlServerResponseQueue)
    .WithEnvironment("SqlServer:DeadLetterQueue", SqlServerDeadLetterQueue)
    .WithEnvironment("AsyncResponse:Channel", "SqlServer")
    .WithEnvironment("AsyncResponse:Transport", "SqlServer")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-sqlserver-early-ack", launchProfileName: null)
    .WaitFor(sqlserver)
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
    .WithEnvironment("AsyncResponse:Transport", "SqlServer")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.AddProject<Projects.AsyncResponse_Sample>("itest-app-postgresql-early-ack", launchProfileName: null)
    .WaitFor(postgres)
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
    .WithEnvironment("AsyncResponse:Transport", "PostgreSQL")
    .WithHttpEndpoint()
    .WithHttpHealthCheck("/alive");

builder.Build().Run();
