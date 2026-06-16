using System.Text.Json;
using Aspire.Hosting;
using Aspire.Hosting.Testing;
using NBomber.CSharp;
using NBomber.Http.CSharp;

// End-to-end HTTP load test of the sample app over the REAL stack (Redis channel + Google Pub/Sub
// transport), driven with NBomber. By default it boots Redis + a Pub/Sub emulator + the sample SUT
// via Aspire (Docker required) and loads them; pass --url to target an already-running instance.
//
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --rate 200 --duration 60
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --url http://localhost:5000
//   dotnet run -c Release --project benchmarks/AsyncResponse.LoadTests -- --gh-json loadtest   (emit github-action-benchmark JSON)
//
// The process exits non-zero if any scenario recorded failed requests.
var rate = GetInt("--rate", 100);
var duration = TimeSpan.FromSeconds(GetInt("--duration", 30));
var warmup = TimeSpan.FromSeconds(GetInt("--warmup", 5));
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
    Console.WriteLine("Booting Redis + Google Pub/Sub emulator + sample SUT via Aspire (Docker required)…");
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
    using var httpClient = new HttpClient { BaseAddress = baseAddress };

    // Each scenario drives one end-to-end path; NBomber runs them concurrently, each at `rate` req/s.
    // request-response exercises the full Redis subscribe→publish→await round-trip (its latency
    // includes the simulated remote work); worker exercises the Pub/Sub fire-and-forget enqueue;
    // attach exercises waiting on a known correlation id.
    var requestResponse = Scenario.Create("request_response_redis", async _ =>
        {
            var request = Http.CreateRequest("POST", "/request-response?behavior=Succeed");
            return await Http.Send(httpClient, request);
        })
        .WithWarmUpDuration(warmup)
        .WithLoadSimulations(Simulation.Inject(rate, TimeSpan.FromSeconds(1), duration));

    var worker = Scenario.Create("worker_pubsub", async _ =>
        {
            var request = Http.CreateRequest("POST", $"/worker?token={Guid.NewGuid():N}");
            return await Http.Send(httpClient, request);
        })
        .WithWarmUpDuration(warmup)
        .WithLoadSimulations(Simulation.Inject(rate, TimeSpan.FromSeconds(1), duration));

    var attach = Scenario.Create("attach_redis", async _ =>
        {
            var request = Http.CreateRequest("POST", "/attach");
            return await Http.Send(httpClient, request);
        })
        .WithWarmUpDuration(warmup)
        .WithLoadSimulations(Simulation.Inject(rate, TimeSpan.FromSeconds(1), duration));

    var nodeStats = NBomberRunner
        .RegisterScenarios(requestResponse, worker, attach)
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
