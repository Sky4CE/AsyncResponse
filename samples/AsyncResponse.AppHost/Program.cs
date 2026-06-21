using Aspire.Hosting.ApplicationModel;

var builder = DistributedApplication.CreateBuilder(args);

var redis = builder.AddRedis("redis");
var rabbitmq = builder.AddContainer("rabbitmq", "rabbitmq", "3.13-management")
    .WithEndpoint(targetPort: 5672, scheme: "tcp", name: "amqp")
    .WithEndpoint(targetPort: 15672, scheme: "http", name: "management");

var rabbitMqEndpoint = rabbitmq.GetEndpoint("amqp");
var rabbitMqConnectionString = ReferenceExpression.Create(
    $"amqp://guest:guest@{rabbitMqEndpoint.Property(EndpointProperty.Host)}:{rabbitMqEndpoint.Property(EndpointProperty.Port)}/");

builder.AddProject<Projects.AsyncResponse_Sample>("playground", launchProfileName: "http")
    .WithReference(redis)
    .WaitFor(redis)
    .WaitFor(rabbitmq)
    .WithEnvironment("AsyncResponse:Channel", "Redis") // playground uses the durable Redis channel
    .WithEnvironment("AsyncResponse:Transport", "RabbitMQ")
    .WithEnvironment("RabbitMQ:ConnectionString", rabbitMqConnectionString)
    .WithUrlForEndpoint("http", endpoint => new()
    {
        Url = "/swagger",
        DisplayText = "Swagger"
    });

builder.Build().Run();
