using Aspire.Hosting.ApplicationModel;

var builder = DistributedApplication.CreateBuilder(args);

var redis = builder.AddRedis("redis");

builder.AddProject<Projects.AsyncResponse_Sample>("playground", launchProfileName: "http")
    .WithReference(redis)
    .WaitFor(redis)
    .WithUrlForEndpoint("http", endpoint => new()
    {
        Url = "/swagger",
        DisplayText = "Swagger"
    });

// Google Pub/Sub emulator + the integration system-under-test app. Running this AppHost shows redis,
// pubsub and itest-app in the dashboard; the integration tests boot this same model via
// Aspire.Hosting.Testing (so the dashboard and the tests share one orchestration).
var pubsub = builder.AddContainer("pubsub", "gcr.io/google.com/cloudsdktool/google-cloud-cli", "446.0.1-emulators")
    .WithArgs("gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085", "--project=itest-project")
    .WithEndpoint(targetPort: 8085, scheme: "tcp", name: "pubsub");

var pubsubEndpoint = pubsub.GetEndpoint("pubsub");
var emulatorHost = ReferenceExpression.Create(
    $"{pubsubEndpoint.Property(EndpointProperty.Host)}:{pubsubEndpoint.Property(EndpointProperty.Port)}");

builder.AddProject<Projects.AsyncResponse_IntegrationTests_App>("itest-app")
    .WithReference(redis)
    .WaitFor(redis)
    .WaitFor(pubsub)
    .WithEnvironment("PUBSUB_EMULATOR_HOST", emulatorHost)
    .WithEnvironment("PubSub:ProjectId", "itest-project")
    .WithEnvironment("PubSub:WorkerTopicId", "worker-topic")
    .WithEnvironment("PubSub:WorkerSubscriptionId", "worker-sub")
    .WithEnvironment("PubSub:ResponseTopicId", "response-topic")
    .WithEnvironment("PubSub:ResponseSubscriptionId", "response-sub")
    .WithEnvironment("AsyncResponse:KeyPrefix", "itest")
    .WithHttpEndpoint() // the SUT has no launch profile; declare an http endpoint explicitly
    .WithHttpHealthCheck("/alive"); // liveness probe → resource "healthy" regardless of recovery Degraded state

builder.Build().Run();
