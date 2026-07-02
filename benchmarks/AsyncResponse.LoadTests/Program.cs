using System.Net;
using System.Text.Json;
using Aspire.Hosting;
using Aspire.Hosting.Testing;
using NBomber.Contracts;
using NBomber.CSharp;

// End-to-end HTTP load tests of the sample app over the REAL stack (durable channels + broker/table
// transports), driven with NBomber. By default it boots Redis + Pub/Sub emulator + Azure Service Bus
// emulator + RabbitMQ + NATS + PostgreSQL + Kafka sample SUTs via Aspire (Docker required) and loads a
// broad, non-destructive scenario mix; pass --url to target an already-running instance instead.
//
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile broad --rate 20 --duration 60
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile azure-servicebus
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile pubsub
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile kafka
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile rabbitmq
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile redis
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile postgresql
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile recovery
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile broad --scenario request_response_success_redis
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile broad --scenario rabbitmq_worker_default_ack_observed,rabbitmq_worker_ack_after_enqueue_observed
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --url http://localhost:5000
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --url http://localhost:5000 --early-ack-url http://localhost:5001 --profile pubsub
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --azure-servicebus-url http://localhost:5010 --azure-servicebus-early-ack-url http://localhost:5011 --profile azure-servicebus
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --rabbitmq-url http://localhost:5002 --rabbitmq-early-ack-url http://localhost:5003 --profile rabbitmq
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --redis-url http://localhost:5004 --redis-early-ack-url http://localhost:5005 --profile redis
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --kafka-url http://localhost:5012 --kafka-early-ack-url http://localhost:5013 --profile kafka
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --postgresql-url http://localhost:5008 --postgresql-early-ack-url http://localhost:5009 --profile postgresql
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --gh-json loadtest
//
// The process exits non-zero if any scenario records failed requests.
var rate = GetInt("--rate", 20);
var duration = TimeSpan.FromSeconds(GetInt("--duration", 30));
var warmup = TimeSpan.FromSeconds(GetInt("--warmup", 5));
var profile = (GetString("--profile") ?? "broad").Trim().ToLowerInvariant();
var scenarioFilter = GetString("--scenario");
var existingUrl = GetString("--url");
var existingEarlyAckUrl = GetString("--early-ack-url");
var existingAzureServiceBusUrl = GetString("--azure-servicebus-url");
var existingAzureServiceBusEarlyAckUrl = GetString("--azure-servicebus-early-ack-url");
var existingRabbitMqUrl = GetString("--rabbitmq-url");
var existingRabbitMqEarlyAckUrl = GetString("--rabbitmq-early-ack-url");
var existingRedisUrl = GetString("--redis-url");
var existingRedisEarlyAckUrl = GetString("--redis-early-ack-url");
var existingNatsUrl = GetString("--nats-url");
var existingNatsEarlyAckUrl = GetString("--nats-early-ack-url");
var existingPostgreSqlUrl = GetString("--postgresql-url");
var existingPostgreSqlEarlyAckUrl = GetString("--postgresql-early-ack-url");
var existingKafkaUrl = GetString("--kafka-url");
var existingKafkaEarlyAckUrl = GetString("--kafka-early-ack-url");
var ghJsonPrefix = GetString("--gh-json");

DistributedApplication? app = null;
Uri baseAddress;
Uri? earlyAckBaseAddress = null;
Uri? azureServiceBusBaseAddress = null;
Uri? azureServiceBusEarlyAckBaseAddress = null;
Uri? rabbitMqBaseAddress = null;
Uri? rabbitMqEarlyAckBaseAddress = null;
Uri? redisBaseAddress = null;
Uri? redisEarlyAckBaseAddress = null;
Uri? natsBaseAddress = null;
Uri? natsEarlyAckBaseAddress = null;
Uri? postgreSqlBaseAddress = null;
Uri? postgreSqlEarlyAckBaseAddress = null;
Uri? kafkaBaseAddress = null;
Uri? kafkaEarlyAckBaseAddress = null;

if (existingUrl is not null || existingAzureServiceBusUrl is not null || existingRabbitMqUrl is not null || existingRedisUrl is not null || existingNatsUrl is not null || existingPostgreSqlUrl is not null || existingKafkaUrl is not null)
{
    baseAddress = new Uri(existingUrl ?? existingAzureServiceBusUrl ?? existingRabbitMqUrl ?? existingRedisUrl ?? existingNatsUrl ?? existingPostgreSqlUrl ?? existingKafkaUrl!);
    Console.WriteLine($"Load testing existing instance at {baseAddress}.");
    if (existingEarlyAckUrl is not null)
    {
        earlyAckBaseAddress = new Uri(existingEarlyAckUrl);
        Console.WriteLine($"Load testing existing ACK-after-enqueue instance at {earlyAckBaseAddress}.");
    }

    if (existingAzureServiceBusUrl is not null || existingUrl is not null)
    {
        azureServiceBusBaseAddress = new Uri(existingAzureServiceBusUrl ?? existingUrl!);
        Console.WriteLine($"Load testing existing Azure Service Bus instance at {azureServiceBusBaseAddress}.");
        if (existingAzureServiceBusEarlyAckUrl is not null)
        {
            azureServiceBusEarlyAckBaseAddress = new Uri(existingAzureServiceBusEarlyAckUrl);
            Console.WriteLine($"Load testing existing Azure Service Bus ACK-after-receive instance at {azureServiceBusEarlyAckBaseAddress}.");
        }
    }

    rabbitMqBaseAddress = new Uri(existingRabbitMqUrl ?? existingUrl ?? existingAzureServiceBusUrl!);
    Console.WriteLine($"Load testing existing RabbitMQ instance at {rabbitMqBaseAddress}.");
    if (existingRabbitMqEarlyAckUrl is not null)
    {
        rabbitMqEarlyAckBaseAddress = new Uri(existingRabbitMqEarlyAckUrl);
        Console.WriteLine($"Load testing existing RabbitMQ ACK-after-enqueue instance at {rabbitMqEarlyAckBaseAddress}.");
    }

    redisBaseAddress = new Uri(existingRedisUrl ?? existingUrl ?? existingAzureServiceBusUrl ?? existingRabbitMqUrl!);
    Console.WriteLine($"Load testing existing Redis transport instance at {redisBaseAddress}.");
    if (existingRedisEarlyAckUrl is not null)
    {
        redisEarlyAckBaseAddress = new Uri(existingRedisEarlyAckUrl);
        Console.WriteLine($"Load testing existing Redis transport ACK-after-enqueue instance at {redisEarlyAckBaseAddress}.");
    }

    natsBaseAddress = new Uri(existingNatsUrl ?? existingUrl ?? existingAzureServiceBusUrl ?? existingRabbitMqUrl ?? existingRedisUrl ?? existingPostgreSqlUrl!);
    Console.WriteLine($"Load testing existing NATS instance at {natsBaseAddress}.");
    if (existingNatsEarlyAckUrl is not null)
    {
        natsEarlyAckBaseAddress = new Uri(existingNatsEarlyAckUrl);
        Console.WriteLine($"Load testing existing NATS ACK-after-receive instance at {natsEarlyAckBaseAddress}.");
    }

    postgreSqlBaseAddress = new Uri(existingPostgreSqlUrl ?? existingUrl ?? existingAzureServiceBusUrl ?? existingRabbitMqUrl ?? existingRedisUrl ?? existingNatsUrl ?? existingKafkaUrl!);
    Console.WriteLine($"Load testing existing PostgreSQL instance at {postgreSqlBaseAddress}.");
    if (existingPostgreSqlEarlyAckUrl is not null)
    {
        postgreSqlEarlyAckBaseAddress = new Uri(existingPostgreSqlEarlyAckUrl);
        Console.WriteLine($"Load testing existing PostgreSQL ACK-after-receive instance at {postgreSqlEarlyAckBaseAddress}.");
    }

    kafkaBaseAddress = new Uri(existingKafkaUrl ?? existingUrl ?? existingAzureServiceBusUrl ?? existingRabbitMqUrl ?? existingRedisUrl ?? existingNatsUrl ?? existingPostgreSqlUrl!);
    Console.WriteLine($"Load testing existing Kafka instance at {kafkaBaseAddress}.");
    if (existingKafkaEarlyAckUrl is not null)
    {
        kafkaEarlyAckBaseAddress = new Uri(existingKafkaEarlyAckUrl);
        Console.WriteLine($"Load testing existing Kafka ACK-after-enqueue instance at {kafkaEarlyAckBaseAddress}.");
    }
}
else
{
    Console.WriteLine("Booting Redis + Google Pub/Sub emulator + Azure Service Bus emulator + RabbitMQ + NATS + PostgreSQL + Kafka + sample SUTs via Aspire (Docker required)...");
    var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.AsyncResponse_IntegrationTests_AppHost>();
    app = await appHost.BuildAsync().WaitAsync(TimeSpan.FromMinutes(5));
    await app.StartAsync().WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app").WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app-early-ack").WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app-azure-servicebus").WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app-azure-servicebus-early-ack").WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app-rabbitmq").WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app-rabbitmq-early-ack").WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app-redis").WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app-redis-early-ack").WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app-nats").WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app-nats-early-ack").WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app-postgresql").WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app-postgresql-early-ack").WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app-kafka").WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app-kafka-early-ack").WaitAsync(TimeSpan.FromMinutes(5));

    using (var probe = app.CreateHttpClient("itest-app"))
        baseAddress = probe.BaseAddress!;
    using (var probe = app.CreateHttpClient("itest-app-early-ack"))
        earlyAckBaseAddress = probe.BaseAddress!;
    using (var probe = app.CreateHttpClient("itest-app-azure-servicebus"))
        azureServiceBusBaseAddress = probe.BaseAddress!;
    using (var probe = app.CreateHttpClient("itest-app-azure-servicebus-early-ack"))
        azureServiceBusEarlyAckBaseAddress = probe.BaseAddress!;
    using (var probe = app.CreateHttpClient("itest-app-rabbitmq"))
        rabbitMqBaseAddress = probe.BaseAddress!;
    using (var probe = app.CreateHttpClient("itest-app-rabbitmq-early-ack"))
        rabbitMqEarlyAckBaseAddress = probe.BaseAddress!;
    using (var probe = app.CreateHttpClient("itest-app-redis"))
        redisBaseAddress = probe.BaseAddress!;
    using (var probe = app.CreateHttpClient("itest-app-redis-early-ack"))
        redisEarlyAckBaseAddress = probe.BaseAddress!;
    using (var probe = app.CreateHttpClient("itest-app-nats"))
        natsBaseAddress = probe.BaseAddress!;
    using (var probe = app.CreateHttpClient("itest-app-nats-early-ack"))
        natsEarlyAckBaseAddress = probe.BaseAddress!;
    using (var probe = app.CreateHttpClient("itest-app-postgresql"))
        postgreSqlBaseAddress = probe.BaseAddress!;
    using (var probe = app.CreateHttpClient("itest-app-postgresql-early-ack"))
        postgreSqlEarlyAckBaseAddress = probe.BaseAddress!;
    using (var probe = app.CreateHttpClient("itest-app-kafka"))
        kafkaBaseAddress = probe.BaseAddress!;
    using (var probe = app.CreateHttpClient("itest-app-kafka-early-ack"))
        kafkaEarlyAckBaseAddress = probe.BaseAddress!;

    Console.WriteLine($"Stack ready; SUT at {baseAddress}.");
    Console.WriteLine($"ACK-after-enqueue SUT at {earlyAckBaseAddress}.");
    Console.WriteLine($"Azure Service Bus SUT at {azureServiceBusBaseAddress}.");
    Console.WriteLine($"Azure Service Bus ACK-after-receive SUT at {azureServiceBusEarlyAckBaseAddress}.");
    Console.WriteLine($"RabbitMQ SUT at {rabbitMqBaseAddress}.");
    Console.WriteLine($"RabbitMQ ACK-after-enqueue SUT at {rabbitMqEarlyAckBaseAddress}.");
    Console.WriteLine($"Redis transport SUT at {redisBaseAddress}.");
    Console.WriteLine($"Redis transport ACK-after-enqueue SUT at {redisEarlyAckBaseAddress}.");
    Console.WriteLine($"NATS SUT at {natsBaseAddress}.");
    Console.WriteLine($"NATS ACK-after-receive SUT at {natsEarlyAckBaseAddress}.");
    Console.WriteLine($"PostgreSQL SUT at {postgreSqlBaseAddress}.");
    Console.WriteLine($"PostgreSQL ACK-after-receive SUT at {postgreSqlEarlyAckBaseAddress}.");
    Console.WriteLine($"Kafka SUT at {kafkaBaseAddress}.");
    Console.WriteLine($"Kafka ACK-after-enqueue SUT at {kafkaEarlyAckBaseAddress}.");
}

var hadFailures = false;
try
{
    using var httpClient = new HttpClient
    {
        BaseAddress = baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };
    using var earlyAckHttpClient = new HttpClient
    {
        BaseAddress = earlyAckBaseAddress ?? baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };
    using var azureServiceBusHttpClient = new HttpClient
    {
        BaseAddress = azureServiceBusBaseAddress ?? baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };
    using var azureServiceBusEarlyAckHttpClient = new HttpClient
    {
        BaseAddress = azureServiceBusEarlyAckBaseAddress ?? azureServiceBusBaseAddress ?? baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };
    using var rabbitMqHttpClient = new HttpClient
    {
        BaseAddress = rabbitMqBaseAddress ?? baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };
    using var rabbitMqEarlyAckHttpClient = new HttpClient
    {
        BaseAddress = rabbitMqEarlyAckBaseAddress ?? rabbitMqBaseAddress ?? baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };
    using var redisHttpClient = new HttpClient
    {
        BaseAddress = redisBaseAddress ?? baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };
    using var redisEarlyAckHttpClient = new HttpClient
    {
        BaseAddress = redisEarlyAckBaseAddress ?? redisBaseAddress ?? baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };
    using var natsHttpClient = new HttpClient
    {
        BaseAddress = natsBaseAddress ?? baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };
    using var natsEarlyAckHttpClient = new HttpClient
    {
        BaseAddress = natsEarlyAckBaseAddress ?? natsBaseAddress ?? baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };
    using var postgreSqlHttpClient = new HttpClient
    {
        BaseAddress = postgreSqlBaseAddress ?? baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };
    using var postgreSqlEarlyAckHttpClient = new HttpClient
    {
        BaseAddress = postgreSqlEarlyAckBaseAddress ?? postgreSqlBaseAddress ?? baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };
    using var kafkaHttpClient = new HttpClient
    {
        BaseAddress = kafkaBaseAddress ?? baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };
    using var kafkaEarlyAckHttpClient = new HttpClient
    {
        BaseAddress = kafkaEarlyAckBaseAddress ?? kafkaBaseAddress ?? baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };

    await TryResetAsync(httpClient);
    if (earlyAckBaseAddress is not null)
        await TryResetAsync(earlyAckHttpClient);
    if (azureServiceBusBaseAddress is not null)
        await TryResetAsync(azureServiceBusHttpClient);
    if (azureServiceBusEarlyAckBaseAddress is not null)
        await TryResetAsync(azureServiceBusEarlyAckHttpClient);
    if (rabbitMqBaseAddress is not null)
        await TryResetAsync(rabbitMqHttpClient);
    if (rabbitMqEarlyAckBaseAddress is not null)
        await TryResetAsync(rabbitMqEarlyAckHttpClient);
    if (redisBaseAddress is not null)
        await TryResetAsync(redisHttpClient);
    if (redisEarlyAckBaseAddress is not null)
        await TryResetAsync(redisEarlyAckHttpClient);
    if (natsBaseAddress is not null)
        await TryResetAsync(natsHttpClient);
    if (natsEarlyAckBaseAddress is not null)
        await TryResetAsync(natsEarlyAckHttpClient);
    if (postgreSqlBaseAddress is not null)
        await TryResetAsync(postgreSqlHttpClient);
    if (postgreSqlEarlyAckBaseAddress is not null)
        await TryResetAsync(postgreSqlEarlyAckHttpClient);
    if (kafkaBaseAddress is not null)
        await TryResetAsync(kafkaHttpClient);
    if (kafkaEarlyAckBaseAddress is not null)
        await TryResetAsync(kafkaEarlyAckHttpClient);

    var definitions = FilterScenarios(SelectScenarios(
        profile,
        includeEarlyAckTarget: earlyAckBaseAddress is not null,
        includeAzureServiceBusTarget: app is not null || existingAzureServiceBusUrl is not null,
        includeAzureServiceBusEarlyAckTarget: azureServiceBusEarlyAckBaseAddress is not null,
        includeRabbitMqTarget: app is not null || existingRabbitMqUrl is not null,
        includeRabbitMqEarlyAckTarget: rabbitMqEarlyAckBaseAddress is not null,
        includeRedisTarget: app is not null || existingRedisUrl is not null,
        includeRedisEarlyAckTarget: redisEarlyAckBaseAddress is not null,
        includeNatsTarget: app is not null || existingNatsUrl is not null,
        includeNatsEarlyAckTarget: natsEarlyAckBaseAddress is not null,
        includePostgreSqlTarget: app is not null || existingPostgreSqlUrl is not null,
        includePostgreSqlEarlyAckTarget: postgreSqlEarlyAckBaseAddress is not null,
        includeKafkaTarget: app is not null || existingKafkaUrl is not null,
        includeKafkaEarlyAckTarget: kafkaEarlyAckBaseAddress is not null), scenarioFilter);
    Console.WriteLine($"NBomber profile={profile}; scenarios={definitions.Length}; rate={rate}/s per scenario; duration={duration.TotalSeconds:N0}s; warmup={warmup.TotalSeconds:N0}s.");
    foreach (var definition in definitions)
        Console.WriteLine($"  - {definition.Name}");

    var scenarios = definitions
        .Select(definition => BuildScenario(
            definition,
            httpClient,
            earlyAckHttpClient,
            azureServiceBusHttpClient,
            azureServiceBusEarlyAckHttpClient,
            rabbitMqHttpClient,
            rabbitMqEarlyAckHttpClient,
            redisHttpClient,
            redisEarlyAckHttpClient,
            natsHttpClient,
            natsEarlyAckHttpClient,
            postgreSqlHttpClient,
            postgreSqlEarlyAckHttpClient,
            kafkaHttpClient,
            kafkaEarlyAckHttpClient,
            rate,
            duration,
            warmup))
        .ToArray();

    var nodeStats = NBomberRunner
        .RegisterScenarios(scenarios)
        .WithReportFolder("nbomber-report")
        .Run();

    hadFailures = nodeStats.ScenarioStats.Any(s => s.Fail.Request.Count > 0);

    // Emit github-action-benchmark series so the CI dashboard tracks throughput and latency per commit.
    if (ghJsonPrefix is not null)
    {
        var throughput = nodeStats.ScenarioStats
            .Select(s => new { name = $"{s.ScenarioName} throughput", unit = "req/s", value = s.Ok.Request.RPS })
            .ToArray();

        var latency = nodeStats.ScenarioStats
            .SelectMany(s => new[]
            {
                new { name = $"{s.ScenarioName} p95 latency", unit = "ms", value = s.Ok.Latency.Percent95 },
                new { name = $"{s.ScenarioName} p99 latency", unit = "ms", value = s.Ok.Latency.Percent99 }
            })
            .ToArray();

        var jsonOptions = new JsonSerializerOptions { WriteIndented = true };
        var outputDirectory = Path.GetDirectoryName(Path.GetFullPath(ghJsonPrefix));
        if (!string.IsNullOrEmpty(outputDirectory))
            Directory.CreateDirectory(outputDirectory);

        File.WriteAllText($"{ghJsonPrefix}.bigger.json", JsonSerializer.Serialize(throughput, jsonOptions));
        File.WriteAllText($"{ghJsonPrefix}.smaller.json", JsonSerializer.Serialize(latency, jsonOptions));
        Console.WriteLine($"Wrote {ghJsonPrefix}.bigger.json + {ghJsonPrefix}.smaller.json for github-action-benchmark.");
    }
}
finally
{
    if (app is not null)
        await app.DisposeAsync();
}

return hadFailures ? 1 : 0;

static ScenarioProps BuildScenario(
    ScenarioDefinition definition,
    HttpClient httpClient,
    HttpClient earlyAckHttpClient,
    HttpClient azureServiceBusHttpClient,
    HttpClient azureServiceBusEarlyAckHttpClient,
    HttpClient rabbitMqHttpClient,
    HttpClient rabbitMqEarlyAckHttpClient,
    HttpClient redisHttpClient,
    HttpClient redisEarlyAckHttpClient,
    HttpClient natsHttpClient,
    HttpClient natsEarlyAckHttpClient,
    HttpClient postgreSqlHttpClient,
    HttpClient postgreSqlEarlyAckHttpClient,
    HttpClient kafkaHttpClient,
    HttpClient kafkaEarlyAckHttpClient,
    int rate,
    TimeSpan duration,
    TimeSpan warmup)
    => Scenario.Create(definition.Name, _ => RunWorkflowAsync(() => definition.RunAsync(
        httpClient,
        earlyAckHttpClient,
        azureServiceBusHttpClient,
        azureServiceBusEarlyAckHttpClient,
        rabbitMqHttpClient,
        rabbitMqEarlyAckHttpClient,
        redisHttpClient,
        redisEarlyAckHttpClient,
        natsHttpClient,
        natsEarlyAckHttpClient,
        postgreSqlHttpClient,
        postgreSqlEarlyAckHttpClient,
        kafkaHttpClient,
        kafkaEarlyAckHttpClient)))
        .WithWarmUpDuration(warmup)
        .WithLoadSimulations(Simulation.Inject(rate, TimeSpan.FromSeconds(1), duration));

static ScenarioDefinition[] SelectScenarios(
    string profile,
    bool includeEarlyAckTarget,
    bool includeAzureServiceBusTarget,
    bool includeAzureServiceBusEarlyAckTarget,
    bool includeRabbitMqTarget,
    bool includeRabbitMqEarlyAckTarget,
    bool includeRedisTarget,
    bool includeRedisEarlyAckTarget,
    bool includeNatsTarget,
    bool includeNatsEarlyAckTarget,
    bool includePostgreSqlTarget,
    bool includePostgreSqlEarlyAckTarget,
    bool includeKafkaTarget,
    bool includeKafkaEarlyAckTarget)
{
    var broad = new[]
    {
        new ScenarioDefinition("request_response_success_redis", RequestResponseSuccessAsync),
        new ScenarioDefinition("request_response_domain_failure_redis", RequestResponseDomainFailureAsync),
        new ScenarioDefinition("attach_redis", AttachAsync),
        new ScenarioDefinition("worker_pubsub_observed", WorkerObservedAsync),
        new ScenarioDefinition("multi_step_success_redis", MultiStepSuccessAsync),
        new ScenarioDefinition("multi_step_domain_failure_redis", MultiStepDomainFailureAsync),
        new ScenarioDefinition("ambient_exception_redis", AmbientExceptionAsync),
        new ScenarioDefinition("shared_exception_fanout_redis", SharedExceptionFanoutAsync),
        new ScenarioDefinition("reply_target_pubsub", ReplyTargetAsync)
    };
    if (includeRabbitMqTarget)
    {
        broad =
        [
            .. broad,
            new ScenarioDefinition("rabbitmq_worker_default_ack_observed", (_, _, rabbitHttp, _) => WorkerObservedAsync(rabbitHttp)),
            new ScenarioDefinition("rabbitmq_response_ingress_header", (_, _, rabbitHttp, _) => ResponseIngressAsync(rabbitHttp, useAttribute: true)),
            new ScenarioDefinition("rabbitmq_response_ingress_body", (_, _, rabbitHttp, _) => ResponseIngressAsync(rabbitHttp, useAttribute: false)),
            new ScenarioDefinition("rabbitmq_reply_target", (_, _, rabbitHttp, _) => ReplyTargetAsync(rabbitHttp))
        ];

        if (includeRabbitMqEarlyAckTarget)
        {
            broad =
            [
                .. broad,
                new ScenarioDefinition("rabbitmq_worker_ack_after_enqueue_observed", (_, _, _, rabbitEarlyAckHttp) => WorkerObservedAsync(rabbitEarlyAckHttp))
            ];
        }
    }

    if (includeAzureServiceBusTarget)
    {
        broad =
        [
            .. broad,
            new ScenarioDefinition("azure_servicebus_request_response_success", (_, _, asbHttp, _, _, _, _, _, _, _, _, _) => RequestResponseSuccessAsync(asbHttp)),
            new ScenarioDefinition("azure_servicebus_worker_default_ack_observed", (_, _, asbHttp, _, _, _, _, _, _, _, _, _) => WorkerObservedAsync(asbHttp)),
            new ScenarioDefinition("azure_servicebus_response_ingress_property", (_, _, asbHttp, _, _, _, _, _, _, _, _, _) => ResponseIngressAsync(asbHttp, useAttribute: true)),
            new ScenarioDefinition("azure_servicebus_response_ingress_body", (_, _, asbHttp, _, _, _, _, _, _, _, _, _) => ResponseIngressAsync(asbHttp, useAttribute: false)),
            new ScenarioDefinition("azure_servicebus_reply_target", (_, _, asbHttp, _, _, _, _, _, _, _, _, _) => ReplyTargetAsync(asbHttp))
        ];

        if (includeAzureServiceBusEarlyAckTarget)
        {
            broad =
            [
                .. broad,
                new ScenarioDefinition("azure_servicebus_worker_ack_after_receive_observed", (_, _, _, asbEarlyAckHttp, _, _, _, _, _, _, _, _) => WorkerObservedAsync(asbEarlyAckHttp))
            ];
        }
    }

    if (includeRedisTarget)
    {
        broad =
        [
            .. broad,
            new ScenarioDefinition("redis_worker_default_ack_observed", (_, _, _, _, redisHttp, _) => WorkerObservedAsync(redisHttp)),
            new ScenarioDefinition("redis_response_ingress_field", (_, _, _, _, redisHttp, _) => ResponseIngressAsync(redisHttp, useAttribute: true)),
            new ScenarioDefinition("redis_response_ingress_body", (_, _, _, _, redisHttp, _) => ResponseIngressAsync(redisHttp, useAttribute: false)),
            new ScenarioDefinition("redis_reply_target", (_, _, _, _, redisHttp, _) => ReplyTargetAsync(redisHttp))
        ];

        if (includeRedisEarlyAckTarget)
        {
            broad =
            [
                .. broad,
                new ScenarioDefinition("redis_worker_ack_after_enqueue_observed", (_, _, _, _, _, redisEarlyAckHttp) => WorkerObservedAsync(redisEarlyAckHttp))
            ];
        }
    }

    if (includeNatsTarget)
    {
        broad =
        [
            .. broad,
            new ScenarioDefinition("nats_request_response_success", (_, _, _, _, _, _, natsHttp, _) => RequestResponseSuccessAsync(natsHttp)),
            new ScenarioDefinition("nats_worker_default_ack_observed", (_, _, _, _, _, _, natsHttp, _) => WorkerObservedAsync(natsHttp)),
            new ScenarioDefinition("nats_response_ingress_header", (_, _, _, _, _, _, natsHttp, _) => ResponseIngressAsync(natsHttp, useAttribute: true)),
            new ScenarioDefinition("nats_response_ingress_body", (_, _, _, _, _, _, natsHttp, _) => ResponseIngressAsync(natsHttp, useAttribute: false)),
            new ScenarioDefinition("nats_reply_target", (_, _, _, _, _, _, natsHttp, _) => ReplyTargetAsync(natsHttp))
        ];

        if (includeNatsEarlyAckTarget)
        {
            broad =
            [
                .. broad,
                new ScenarioDefinition("nats_worker_ack_after_receive_observed", (_, _, _, _, _, _, _, natsEarlyAckHttp) => WorkerObservedAsync(natsEarlyAckHttp))
            ];
        }
    }

    if (includePostgreSqlTarget)
    {
        broad =
        [
            .. broad,
            new ScenarioDefinition("postgresql_request_response_success", (_, _, _, _, _, _, _, _, pgHttp, _) => RequestResponseSuccessAsync(pgHttp)),
            new ScenarioDefinition("postgresql_worker_default_ack_observed", (_, _, _, _, _, _, _, _, pgHttp, _) => WorkerObservedAsync(pgHttp)),
            new ScenarioDefinition("postgresql_response_ingress_header", (_, _, _, _, _, _, _, _, pgHttp, _) => ResponseIngressAsync(pgHttp, useAttribute: true)),
            new ScenarioDefinition("postgresql_response_ingress_body", (_, _, _, _, _, _, _, _, pgHttp, _) => ResponseIngressAsync(pgHttp, useAttribute: false)),
            new ScenarioDefinition("postgresql_reply_target", (_, _, _, _, _, _, _, _, pgHttp, _) => ReplyTargetAsync(pgHttp))
        ];

        if (includePostgreSqlEarlyAckTarget)
        {
            broad =
            [
                .. broad,
                new ScenarioDefinition("postgresql_worker_ack_after_receive_observed", (_, _, _, _, _, _, _, _, _, pgEarlyAckHttp) => WorkerObservedAsync(pgEarlyAckHttp))
            ];
        }
    }

    if (includeKafkaTarget)
    {
        broad =
        [
            .. broad,
            new ScenarioDefinition("kafka_worker_default_ack_observed", (_, _, _, _, _, _, _, _, _, _, _, _, kafkaHttp, _) => WorkerObservedAsync(kafkaHttp)),
            new ScenarioDefinition("kafka_response_ingress_header", (_, _, _, _, _, _, _, _, _, _, _, _, kafkaHttp, _) => ResponseIngressAsync(kafkaHttp, useAttribute: true)),
            new ScenarioDefinition("kafka_response_ingress_body", (_, _, _, _, _, _, _, _, _, _, _, _, kafkaHttp, _) => ResponseIngressAsync(kafkaHttp, useAttribute: false)),
            new ScenarioDefinition("kafka_reply_target", (_, _, _, _, _, _, _, _, _, _, _, _, kafkaHttp, _) => ReplyTargetAsync(kafkaHttp))
        ];

        if (includeKafkaEarlyAckTarget)
        {
            broad =
            [
                .. broad,
                new ScenarioDefinition("kafka_worker_ack_after_enqueue_observed", (_, _, _, _, _, _, _, _, _, _, _, _, _, kafkaEarlyAckHttp) => WorkerObservedAsync(kafkaEarlyAckHttp))
            ];
        }
    }

    var pubsub = new[]
    {
        new ScenarioDefinition("pubsub_worker_default_ack_observed", WorkerObservedAsync),
        new ScenarioDefinition("pubsub_response_ingress_attribute", http => ResponseIngressAsync(http, useAttribute: true)),
        new ScenarioDefinition("pubsub_response_ingress_body", http => ResponseIngressAsync(http, useAttribute: false))
    };
    if (includeEarlyAckTarget)
    {
        pubsub =
        [
            .. pubsub,
            new ScenarioDefinition("pubsub_worker_ack_after_enqueue_observed", (_, earlyAckHttp) => WorkerObservedAsync(earlyAckHttp))
        ];
    }

    var rabbitmq = new[]
    {
        new ScenarioDefinition("rabbitmq_worker_default_ack_observed", (_, _, rabbitHttp, _) => WorkerObservedAsync(rabbitHttp)),
        new ScenarioDefinition("rabbitmq_response_ingress_header", (_, _, rabbitHttp, _) => ResponseIngressAsync(rabbitHttp, useAttribute: true)),
        new ScenarioDefinition("rabbitmq_response_ingress_body", (_, _, rabbitHttp, _) => ResponseIngressAsync(rabbitHttp, useAttribute: false)),
        new ScenarioDefinition("rabbitmq_reply_target", (_, _, rabbitHttp, _) => ReplyTargetAsync(rabbitHttp))
    };
    if (includeRabbitMqEarlyAckTarget)
    {
        rabbitmq =
        [
            .. rabbitmq,
            new ScenarioDefinition("rabbitmq_worker_ack_after_enqueue_observed", (_, _, _, rabbitEarlyAckHttp) => WorkerObservedAsync(rabbitEarlyAckHttp))
        ];
    }

    var azureServiceBus = new[]
    {
        new ScenarioDefinition("azure_servicebus_request_response_success", (_, _, asbHttp, _, _, _, _, _, _, _, _, _) => RequestResponseSuccessAsync(asbHttp)),
        new ScenarioDefinition("azure_servicebus_request_response_domain_failure", (_, _, asbHttp, _, _, _, _, _, _, _, _, _) => RequestResponseDomainFailureAsync(asbHttp)),
        new ScenarioDefinition("azure_servicebus_worker_default_ack_observed", (_, _, asbHttp, _, _, _, _, _, _, _, _, _) => WorkerObservedAsync(asbHttp)),
        new ScenarioDefinition("azure_servicebus_response_ingress_property", (_, _, asbHttp, _, _, _, _, _, _, _, _, _) => ResponseIngressAsync(asbHttp, useAttribute: true)),
        new ScenarioDefinition("azure_servicebus_response_ingress_body", (_, _, asbHttp, _, _, _, _, _, _, _, _, _) => ResponseIngressAsync(asbHttp, useAttribute: false)),
        new ScenarioDefinition("azure_servicebus_reply_target", (_, _, asbHttp, _, _, _, _, _, _, _, _, _) => ReplyTargetAsync(asbHttp))
    };
    if (includeAzureServiceBusEarlyAckTarget)
    {
        azureServiceBus =
        [
            .. azureServiceBus,
            new ScenarioDefinition("azure_servicebus_worker_ack_after_receive_observed", (_, _, _, asbEarlyAckHttp, _, _, _, _, _, _, _, _) => WorkerObservedAsync(asbEarlyAckHttp))
        ];
    }

    var redis = new[]
    {
        new ScenarioDefinition("redis_worker_default_ack_observed", (_, _, _, _, redisHttp, _) => WorkerObservedAsync(redisHttp)),
        new ScenarioDefinition("redis_response_ingress_field", (_, _, _, _, redisHttp, _) => ResponseIngressAsync(redisHttp, useAttribute: true)),
        new ScenarioDefinition("redis_response_ingress_body", (_, _, _, _, redisHttp, _) => ResponseIngressAsync(redisHttp, useAttribute: false)),
        new ScenarioDefinition("redis_reply_target", (_, _, _, _, redisHttp, _) => ReplyTargetAsync(redisHttp))
    };
    if (includeRedisEarlyAckTarget)
    {
        redis =
        [
            .. redis,
            new ScenarioDefinition("redis_worker_ack_after_enqueue_observed", (_, _, _, _, _, redisEarlyAckHttp) => WorkerObservedAsync(redisEarlyAckHttp))
        ];
    }

    var nats = new[]
    {
        new ScenarioDefinition("nats_request_response_success", (_, _, _, _, _, _, natsHttp, _) => RequestResponseSuccessAsync(natsHttp)),
        new ScenarioDefinition("nats_request_response_domain_failure", (_, _, _, _, _, _, natsHttp, _) => RequestResponseDomainFailureAsync(natsHttp)),
        new ScenarioDefinition("nats_worker_default_ack_observed", (_, _, _, _, _, _, natsHttp, _) => WorkerObservedAsync(natsHttp)),
        new ScenarioDefinition("nats_response_ingress_header", (_, _, _, _, _, _, natsHttp, _) => ResponseIngressAsync(natsHttp, useAttribute: true)),
        new ScenarioDefinition("nats_response_ingress_body", (_, _, _, _, _, _, natsHttp, _) => ResponseIngressAsync(natsHttp, useAttribute: false)),
        new ScenarioDefinition("nats_reply_target", (_, _, _, _, _, _, natsHttp, _) => ReplyTargetAsync(natsHttp))
    };
    if (includeNatsEarlyAckTarget)
    {
        nats =
        [
            .. nats,
            new ScenarioDefinition("nats_worker_ack_after_receive_observed", (_, _, _, _, _, _, _, natsEarlyAckHttp) => WorkerObservedAsync(natsEarlyAckHttp))
        ];
    }

    var postgresql = new[]
    {
        new ScenarioDefinition("postgresql_request_response_success", (_, _, _, _, _, _, _, _, pgHttp, _) => RequestResponseSuccessAsync(pgHttp)),
        new ScenarioDefinition("postgresql_request_response_domain_failure", (_, _, _, _, _, _, _, _, pgHttp, _) => RequestResponseDomainFailureAsync(pgHttp)),
        new ScenarioDefinition("postgresql_worker_default_ack_observed", (_, _, _, _, _, _, _, _, pgHttp, _) => WorkerObservedAsync(pgHttp)),
        new ScenarioDefinition("postgresql_response_ingress_header", (_, _, _, _, _, _, _, _, pgHttp, _) => ResponseIngressAsync(pgHttp, useAttribute: true)),
        new ScenarioDefinition("postgresql_response_ingress_body", (_, _, _, _, _, _, _, _, pgHttp, _) => ResponseIngressAsync(pgHttp, useAttribute: false)),
        new ScenarioDefinition("postgresql_reply_target", (_, _, _, _, _, _, _, _, pgHttp, _) => ReplyTargetAsync(pgHttp))
    };
    if (includePostgreSqlEarlyAckTarget)
    {
        postgresql =
        [
            .. postgresql,
            new ScenarioDefinition("postgresql_worker_ack_after_receive_observed", (_, _, _, _, _, _, _, _, _, pgEarlyAckHttp) => WorkerObservedAsync(pgEarlyAckHttp))
        ];
    }

    var kafka = new[]
    {
        new ScenarioDefinition("kafka_request_response_success", (_, _, _, _, _, _, _, _, _, _, _, _, kafkaHttp, _) => RequestResponseSuccessAsync(kafkaHttp)),
        new ScenarioDefinition("kafka_request_response_domain_failure", (_, _, _, _, _, _, _, _, _, _, _, _, kafkaHttp, _) => RequestResponseDomainFailureAsync(kafkaHttp)),
        new ScenarioDefinition("kafka_worker_default_ack_observed", (_, _, _, _, _, _, _, _, _, _, _, _, kafkaHttp, _) => WorkerObservedAsync(kafkaHttp)),
        new ScenarioDefinition("kafka_response_ingress_header", (_, _, _, _, _, _, _, _, _, _, _, _, kafkaHttp, _) => ResponseIngressAsync(kafkaHttp, useAttribute: true)),
        new ScenarioDefinition("kafka_response_ingress_body", (_, _, _, _, _, _, _, _, _, _, _, _, kafkaHttp, _) => ResponseIngressAsync(kafkaHttp, useAttribute: false)),
        new ScenarioDefinition("kafka_reply_target", (_, _, _, _, _, _, _, _, _, _, _, _, kafkaHttp, _) => ReplyTargetAsync(kafkaHttp))
    };
    if (includeKafkaEarlyAckTarget)
    {
        kafka =
        [
            .. kafka,
            new ScenarioDefinition("kafka_worker_ack_after_enqueue_observed", (_, _, _, _, _, _, _, _, _, _, _, _, _, kafkaEarlyAckHttp) => WorkerObservedAsync(kafkaEarlyAckHttp))
        ];
    }

    var recovery = new[]
    {
        new ScenarioDefinition("lost_subscriber_resume_redis", http => LostSubscriberFlowAsync(http, "Completed")),
        new ScenarioDefinition("lost_subscriber_domain_failure_redis", http => LostSubscriberFlowAsync(http, "Failed")),
        new ScenarioDefinition("lost_subscriber_exception_redis", http => LostSubscriberFlowAsync(http, "Exception")),
        new ScenarioDefinition("stale_recovery_health_redis", StaleRecoveryHealthAsync)
    };

    return profile switch
    {
        "core" or "broad" => broad,
        "azure-servicebus" or "azureservicebus" or "asb" => azureServiceBus,
        "pubsub" => pubsub,
        "rabbitmq" => rabbitmq,
        "redis" => redis,
        "nats" => nats,
        "postgresql" => postgresql,
        "kafka" => kafka,
        "recovery" => recovery,
        _ => throw new ArgumentException(
            $"Unknown --profile '{profile}'. Use one of: broad, core, azure-servicebus, pubsub, rabbitmq, redis, nats, postgresql, kafka, recovery.")
    };
}

static ScenarioDefinition[] FilterScenarios(ScenarioDefinition[] definitions, string? filter)
{
    if (string.IsNullOrWhiteSpace(filter))
        return definitions;

    var names = filter
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .ToHashSet(StringComparer.OrdinalIgnoreCase);

    var filtered = definitions
        .Where(definition => names.Contains(definition.Name))
        .ToArray();

    if (filtered.Length == 0)
    {
        throw new ArgumentException(
            $"No scenarios matched --scenario '{filter}'. Available: {string.Join(", ", definitions.Select(d => d.Name))}");
    }

    return filtered;
}

static Task RequestResponseSuccessAsync(HttpClient http)
    => EnsureStatusAsync(http, HttpMethod.Post, $"/request-response?behavior=Succeed&trace={Trace()}", HttpStatusCode.OK);

static Task RequestResponseDomainFailureAsync(HttpClient http)
    => EnsureStatusAsync(http, HttpMethod.Post, $"/request-response?behavior=FailDomain&trace={Trace()}", HttpStatusCode.OK);

static Task AttachAsync(HttpClient http)
    => EnsureStatusAsync(http, HttpMethod.Post, "/attach", HttpStatusCode.OK);

static async Task WorkerObservedAsync(HttpClient http)
{
    var token = $"load-{Guid.NewGuid():N}";
    await EnsureStatusAsync(http, HttpMethod.Post, $"/worker?token={token}&trace={Trace()}", HttpStatusCode.OK);
    await EnsureStatusAsync(http, HttpMethod.Get, $"/calls?key=worker:{token}&timeoutMs=15000", HttpStatusCode.OK);
}

static Task MultiStepSuccessAsync(HttpClient http)
    => EnsureStatusAsync(http, HttpMethod.Post, $"/multi-step?first=Succeed&second=Succeed&trace={Trace()}", HttpStatusCode.OK);

static Task MultiStepDomainFailureAsync(HttpClient http)
    => EnsureStatusAsync(http, HttpMethod.Post, $"/multi-step?first=Succeed&second=FailDomain&trace={Trace()}", HttpStatusCode.OK);

static Task AmbientExceptionAsync(HttpClient http)
    => EnsureStatusAsync(http, HttpMethod.Post, $"/ambient-exception?message=load-{Guid.NewGuid():N}", HttpStatusCode.OK);

static Task SharedExceptionFanoutAsync(HttpClient http)
    => EnsureStatusAsync(http, HttpMethod.Post, $"/shared-correlation-exception?message=load-{Guid.NewGuid():N}", HttpStatusCode.OK);

static Task ReplyTargetAsync(HttpClient http)
    => EnsureStatusAsync(http, HttpMethod.Get, "/reply-target", HttpStatusCode.OK);

static async Task ResponseIngressAsync(HttpClient http, bool useAttribute)
{
    var correlationId = await PostForCorrelationIdAsync(http, $"/arm?trace={Trace()}");
    await EnsureStatusAsync(
        http,
        HttpMethod.Post,
        $"/emit-response?correlationId={Uri.EscapeDataString(correlationId)}&status=Completed&useAttribute={useAttribute.ToString().ToLowerInvariant()}&message=load",
        HttpStatusCode.Accepted);
    await EnsureStatusAsync(
        http,
        HttpMethod.Get,
        $"/calls?key=waiter:{Uri.EscapeDataString(correlationId)}&timeoutMs=30000",
        HttpStatusCode.OK);
}

static Task LostSubscriberFlowAsync(HttpClient http, string outcome)
    => EnsureStatusAsync(http, HttpMethod.Post, $"/lost-subscriber-flow?outcome={outcome}&trace={Trace()}", HttpStatusCode.OK);

static async Task StaleRecoveryHealthAsync(HttpClient http)
{
    var correlationId = $"stale-{Guid.NewGuid():N}";
    await EnsureStatusAsync(
        http,
        HttpMethod.Post,
        $"/seed-recovery?correlationId={correlationId}&ageMinutes=5",
        HttpStatusCode.Accepted);

    await Task.Delay(TimeSpan.FromSeconds(2));
    await EnsureStatusAsync(http, HttpMethod.Get, "/healthz", HttpStatusCode.OK);
    await EnsureStatusAsync(http, HttpMethod.Delete, $"/test/recovery/{correlationId}", HttpStatusCode.OK);
}

static async Task<IResponse> RunWorkflowAsync(Func<Task> workflow)
{
    try
    {
        await workflow();
        return Response.Ok();
    }
    catch (Exception ex)
    {
        return Response.Fail(statusCode: "exception", message: ex.Message);
    }
}

static async Task<string> PostForCorrelationIdAsync(HttpClient http, string path)
{
    using var document = await SendForJsonAsync(http, HttpMethod.Post, path, HttpStatusCode.OK);
    if (!document.RootElement.TryGetProperty("correlationId", out var property)
        || property.GetString() is not { Length: > 0 } correlationId)
    {
        throw new InvalidOperationException($"Response from {path} did not contain a correlationId.");
    }

    return correlationId;
}

static async Task EnsureStatusAsync(HttpClient http, HttpMethod method, string path, params HttpStatusCode[] expected)
{
    using var request = new HttpRequestMessage(method, path);
    using var response = await http.SendAsync(request);
    if (expected.Contains(response.StatusCode))
        return;

    var body = await response.Content.ReadAsStringAsync();
    throw new InvalidOperationException($"{method} {path} returned {(int)response.StatusCode} {response.StatusCode}: {body}");
}

static async Task<JsonDocument> SendForJsonAsync(HttpClient http, HttpMethod method, string path, params HttpStatusCode[] expected)
{
    using var request = new HttpRequestMessage(method, path);
    using var response = await http.SendAsync(request);
    var body = await response.Content.ReadAsStringAsync();
    if (!expected.Contains(response.StatusCode))
        throw new InvalidOperationException($"{method} {path} returned {(int)response.StatusCode} {response.StatusCode}: {body}");

    return JsonDocument.Parse(body);
}

static async Task TryResetAsync(HttpClient http)
{
    try
    {
        await EnsureStatusAsync(http, HttpMethod.Post, "/test/reset", HttpStatusCode.OK);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"SUT reset skipped/failed: {ex.Message}");
    }
}

static int GetInt(string name, int fallback)
{
    var args = Environment.GetCommandLineArgs();
    var index = Array.IndexOf(args, name);
    return index >= 0 && index + 1 < args.Length && int.TryParse(args[index + 1], out var value) ? value : fallback;
}

static string? GetString(string name)
{
    var args = Environment.GetCommandLineArgs();
    var index = Array.IndexOf(args, name);
    return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
}

static string Trace() => $"trace-{Guid.NewGuid():N}";

internal sealed record ScenarioDefinition(
    string Name,
    Func<HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, Task> RunAsync)
{
    public ScenarioDefinition(string name, Func<HttpClient, Task> runAsync)
        : this(name, (http, _, _, _, _, _, _, _, _, _, _, _, _, _) => runAsync(http))
    {
    }

    public ScenarioDefinition(string name, Func<HttpClient, HttpClient, Task> runAsync)
        : this(name, (http, earlyAckHttp, _, _, _, _, _, _, _, _, _, _, _, _) => runAsync(http, earlyAckHttp))
    {
    }

    public ScenarioDefinition(string name, Func<HttpClient, HttpClient, HttpClient, HttpClient, Task> runAsync)
        : this(name, (http, earlyAckHttp, _, _, rabbitMqHttp, rabbitMqEarlyAckHttp, _, _, _, _, _, _, _, _) =>
            runAsync(http, earlyAckHttp, rabbitMqHttp, rabbitMqEarlyAckHttp))
    {
    }

    public ScenarioDefinition(string name, Func<HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, Task> runAsync)
        : this(name, (http, earlyAckHttp, _, _, rabbitMqHttp, rabbitMqEarlyAckHttp, redisHttp, redisEarlyAckHttp, _, _, _, _, _, _) =>
            runAsync(http, earlyAckHttp, rabbitMqHttp, rabbitMqEarlyAckHttp, redisHttp, redisEarlyAckHttp))
    {
    }

    public ScenarioDefinition(string name, Func<HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, Task> runAsync)
        : this(name, (http, earlyAckHttp, _, _, rabbitMqHttp, rabbitMqEarlyAckHttp, redisHttp, redisEarlyAckHttp, natsHttp, natsEarlyAckHttp, _, _, _, _) =>
            runAsync(http, earlyAckHttp, rabbitMqHttp, rabbitMqEarlyAckHttp, redisHttp, redisEarlyAckHttp, natsHttp, natsEarlyAckHttp))
    {
    }

    public ScenarioDefinition(string name, Func<HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, Task> runAsync)
        : this(name, (http, earlyAckHttp, _, _, rabbitMqHttp, rabbitMqEarlyAckHttp, redisHttp, redisEarlyAckHttp, natsHttp, natsEarlyAckHttp, postgreSqlHttp, postgreSqlEarlyAckHttp, _, _) =>
            runAsync(http, earlyAckHttp, rabbitMqHttp, rabbitMqEarlyAckHttp, redisHttp, redisEarlyAckHttp, natsHttp, natsEarlyAckHttp, postgreSqlHttp, postgreSqlEarlyAckHttp))
    {
    }

    public ScenarioDefinition(string name, Func<HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, HttpClient, Task> runAsync)
        : this(name, (http, earlyAckHttp, azureServiceBusHttp, azureServiceBusEarlyAckHttp, rabbitMqHttp, rabbitMqEarlyAckHttp, redisHttp, redisEarlyAckHttp, natsHttp, natsEarlyAckHttp, postgreSqlHttp, postgreSqlEarlyAckHttp, _, _) =>
            runAsync(http, earlyAckHttp, azureServiceBusHttp, azureServiceBusEarlyAckHttp, rabbitMqHttp, rabbitMqEarlyAckHttp, redisHttp, redisEarlyAckHttp, natsHttp, natsEarlyAckHttp, postgreSqlHttp, postgreSqlEarlyAckHttp))
    {
    }
}
