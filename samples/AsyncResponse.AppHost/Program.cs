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

builder.Build().Run();
