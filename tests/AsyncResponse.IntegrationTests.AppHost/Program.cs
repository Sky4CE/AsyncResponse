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
const string RedisTransportKeyPrefix = "itest-redistransport";
const string RedisTransportEarlyAckKeyPrefix = "itest-redistransport-early-ack";
const string NatsSubjectPrefix = "itest-nats";
const string NatsEarlyAckSubjectPrefix = "itest-nats-early-ack";
const string PostgreSqlWorkerQueue = "worker";
const string PostgreSqlResponseQueue = "response";
const string PostgreSqlDeadLetterQueue = "deadletter";
const string PostgreSqlEarlyAckWorkerQueue = "worker_earlyack";
const string PostgreSqlEarlyAckResponseQueue = "response_earlyack";
const string PostgreSqlEarlyAckDeadLetterQueue = "deadletter_earlyack";

static string Env(string name, string fallback)
    => Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : fallback;

var builder = DistributedApplication.CreateBuilder(args);
builder.Services.Configure<LoggerFilterOptions>(options =>
    options.AddFilter("Microsoft.Extensions.Diagnostics.HealthChecks.DefaultHealthCheckService", LogLevel.Critical));

var redis = builder.AddRedis("redis");
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

// Two PostgreSQL app instances (default + early-ack) share this one server, each with its own Npgsql
// pool. The image default max_connections=100 is exhausted under the load-test profile ("FATAL: sorry,
// too many clients already"). Raise the server ceiling well above the combined pool budget below
// (2 apps x Maximum Pool Size=120 = 240) so neither the load test nor parallel integration apps starve.
var postgres = builder.AddContainer("postgres", "postgres", "16-alpine")
    .WithEnvironment("POSTGRES_DB", "asyncresponse")
    .WithEnvironment("POSTGRES_PASSWORD", "postgres")
    .WithArgs("-c", "max_connections=400")
    .WithEndpoint(targetPort: 5432, scheme: "tcp", name: "postgres");

var pubsubEndpoint = pubsub.GetEndpoint("pubsub");
var emulatorHost = ReferenceExpression.Create(
    $"{pubsubEndpoint.Property(EndpointProperty.Host)}:{pubsubEndpoint.Property(EndpointProperty.Port)}");
var rabbitMqEndpoint = rabbitmq.GetEndpoint("amqp");
var rabbitMqConnectionString = ReferenceExpression.Create(
    $"amqp://guest:guest@{rabbitMqEndpoint.Property(EndpointProperty.Host)}:{rabbitMqEndpoint.Property(EndpointProperty.Port)}/");
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
