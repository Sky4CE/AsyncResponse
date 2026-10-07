using Aspire.Hosting.ApplicationModel;

var builder = DistributedApplication.CreateBuilder(args);

// Pinned by digest like the integration AppHost's containers (see its Program.cs for why and how
// to bump): Aspire runs image@sha256 once a digest is set; the tag says which release it is.
var redis = builder.AddRedis("redis")
    .WithImageTag("8.6")
    .WithImageSHA256("2f07354308a997554f9d676888ca927ada1293c7c2154dd01ae77f57dd9c6d5f");

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
