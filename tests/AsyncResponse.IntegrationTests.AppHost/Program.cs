using Aspire.Hosting.ApplicationModel;

const string ProjectId = "itest-project";
const string WorkerTopic = "worker-topic";
const string WorkerSubscription = "worker-sub";
const string ResponseTopic = "response-topic";
const string ResponseSubscription = "response-sub";
const string TestRedisKeyPrefix = "itest";

var builder = DistributedApplication.CreateBuilder(args);

var redis = builder.AddRedis("redis");

var pubsub = builder.AddContainer("pubsub", "gcr.io/google.com/cloudsdktool/google-cloud-cli", "446.0.1-emulators")
    .WithArgs("gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085", $"--project={ProjectId}")
    .WithEndpoint(targetPort: 8085, scheme: "tcp", name: "pubsub");

var pubsubEndpoint = pubsub.GetEndpoint("pubsub");
var emulatorHost = ReferenceExpression.Create(
    $"{pubsubEndpoint.Property(EndpointProperty.Host)}:{pubsubEndpoint.Property(EndpointProperty.Port)}");

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

builder.Build().Run();
