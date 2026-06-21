using Aspire.Hosting.ApplicationModel;

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

var builder = DistributedApplication.CreateBuilder(args);

var redis = builder.AddRedis("redis");
var rabbitmq = builder.AddContainer("rabbitmq", "rabbitmq", "3.13-management")
    .WithEndpoint(targetPort: 5672, scheme: "tcp", name: "amqp")
    .WithEndpoint(targetPort: 15672, scheme: "http", name: "management");

var pubsub = builder.AddContainer("pubsub", "gcr.io/google.com/cloudsdktool/google-cloud-cli", "446.0.1-emulators")
    .WithArgs("gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085", $"--project={ProjectId}")
    .WithEndpoint(targetPort: 8085, scheme: "tcp", name: "pubsub");

var pubsubEndpoint = pubsub.GetEndpoint("pubsub");
var emulatorHost = ReferenceExpression.Create(
    $"{pubsubEndpoint.Property(EndpointProperty.Host)}:{pubsubEndpoint.Property(EndpointProperty.Port)}");
var rabbitMqEndpoint = rabbitmq.GetEndpoint("amqp");
var rabbitMqConnectionString = ReferenceExpression.Create(
    $"amqp://guest:guest@{rabbitMqEndpoint.Property(EndpointProperty.Host)}:{rabbitMqEndpoint.Property(EndpointProperty.Port)}/");

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

builder.Build().Run();
