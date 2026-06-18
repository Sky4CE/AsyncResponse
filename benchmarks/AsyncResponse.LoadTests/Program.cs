using System.Net;
using System.Text.Json;
using Aspire.Hosting;
using Aspire.Hosting.Testing;
using NBomber.Contracts;
using NBomber.CSharp;

// End-to-end HTTP load tests of the sample app over the REAL stack (Redis channel + Google Pub/Sub
// transport), driven with NBomber. By default it boots Redis + a Pub/Sub emulator + the sample SUT
// via Aspire (Docker required) and loads a broad, non-destructive scenario mix; pass --url to target
// an already-running instance instead.
//
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile broad --rate 20 --duration 60
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile pubsub
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --profile recovery
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --url http://localhost:5000
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --gh-json loadtest
//
// The process exits non-zero if any scenario records failed requests.
var rate = GetInt("--rate", 20);
var duration = TimeSpan.FromSeconds(GetInt("--duration", 30));
var warmup = TimeSpan.FromSeconds(GetInt("--warmup", 5));
var profile = (GetString("--profile") ?? "broad").Trim().ToLowerInvariant();
var existingUrl = GetString("--url");
var ghJsonPrefix = GetString("--gh-json");

DistributedApplication? app = null;
Uri baseAddress;

if (existingUrl is not null)
{
    baseAddress = new Uri(existingUrl);
    Console.WriteLine($"Load testing existing instance at {baseAddress}.");
}
else
{
    Console.WriteLine("Booting Redis + Google Pub/Sub emulator + sample SUT via Aspire (Docker required)...");
    var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.AsyncResponse_IntegrationTests_AppHost>();
    app = await appHost.BuildAsync().WaitAsync(TimeSpan.FromMinutes(5));
    await app.StartAsync().WaitAsync(TimeSpan.FromMinutes(5));
    await app.ResourceNotifications.WaitForResourceHealthyAsync("itest-app").WaitAsync(TimeSpan.FromMinutes(5));

    using (var probe = app.CreateHttpClient("itest-app"))
        baseAddress = probe.BaseAddress!;

    Console.WriteLine($"Stack ready; SUT at {baseAddress}.");
}

var hadFailures = false;
try
{
    using var httpClient = new HttpClient
    {
        BaseAddress = baseAddress,
        Timeout = TimeSpan.FromSeconds(120)
    };

    await TryResetAsync(httpClient);

    var definitions = SelectScenarios(profile);
    Console.WriteLine($"NBomber profile={profile}; scenarios={definitions.Length}; rate={rate}/s per scenario; duration={duration.TotalSeconds:N0}s; warmup={warmup.TotalSeconds:N0}s.");
    foreach (var definition in definitions)
        Console.WriteLine($"  - {definition.Name}");

    var scenarios = definitions
        .Select(definition => BuildScenario(definition, httpClient, rate, duration, warmup))
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
    int rate,
    TimeSpan duration,
    TimeSpan warmup)
    => Scenario.Create(definition.Name, _ => RunWorkflowAsync(() => definition.RunAsync(httpClient)))
        .WithWarmUpDuration(warmup)
        .WithLoadSimulations(Simulation.Inject(rate, TimeSpan.FromSeconds(1), duration));

static ScenarioDefinition[] SelectScenarios(string profile)
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

    var pubsub = new[]
    {
        new ScenarioDefinition("pubsub_response_ingress_attribute", http => PubSubResponseIngressAsync(http, useAttribute: true)),
        new ScenarioDefinition("pubsub_response_ingress_body", http => PubSubResponseIngressAsync(http, useAttribute: false))
    };

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
        "pubsub" => pubsub,
        "recovery" => recovery,
        _ => throw new ArgumentException(
            $"Unknown --profile '{profile}'. Use one of: broad, core, pubsub, recovery.")
    };
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

static async Task PubSubResponseIngressAsync(HttpClient http, bool useAttribute)
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

internal sealed record ScenarioDefinition(string Name, Func<HttpClient, Task> RunAsync);
