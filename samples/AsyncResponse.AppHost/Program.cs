using Aspire.Hosting.ApplicationModel;

var builder = DistributedApplication.CreateBuilder(args);

var redis = builder.AddRedis("redis");

builder.AddProject<Projects.AsyncResponse_Sample>("playground", launchProfileName: "http")
    .WithReference(redis)
    .WaitFor(redis)
    .WithEnvironment("AsyncResponse:Channel", "Redis") // playground uses the durable Redis channel
    .WithEnvironment("AsyncResponse:Transport", "Redis")
    .WithEnvironment("Redis:KeyPrefix", "playground")
    .WithUrlForEndpoint("http", endpoint => new()
    {
        Url = "/swagger",
        DisplayText = "Swagger"
    });

builder.Build().Run();
