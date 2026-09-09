using AsyncResponse.Sample;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using System.Net;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Round 34, finding 3: the sample's test-only mutation routes (<c>/seed-recovery</c>,
/// <c>/test/recovery/{correlationId}</c>, <c>/test/reset</c>) were mapped unconditionally and
/// unauthenticated — a Production instance against a shared backend let any caller erase every
/// recovery registration. They are now mapped only in Development or when
/// <c>Sample:EnableTestEndpoints</c> says so (the integration AppHost and the load-test launcher
/// opt in). In-process, no Docker. Pre-fix failure: Production answers 200 on all three.
/// </summary>
[Trait(Batches.Trait, Batches.None)]
public sealed class SampleTestEndpointGateTests
{
    [Fact]
    public async Task ProductionByDefault_DoesNotMapTheTestMutationRoutes()
    {
        await using var factory = new GatedAppFactory(environment: "Production", enableTestEndpoints: null);
        using var client = factory.CreateClient();

        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync("/test/reset", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.DeleteAsync("/test/recovery/any")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync("/seed-recovery?correlationId=any", content: null)).StatusCode);

        // The app itself is up: the operational routes are unaffected.
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/healthz")).StatusCode);
    }

    /// <summary>
    /// Round 35: the switch covered three routes while the simulation, injection, and ledger
    /// routes stayed mapped and unauthenticated in Production — <c>/publish</c> injected responses
    /// for any correlation id (202), <c>/crash</c> was mapped (409 only because the in-memory
    /// channel cannot drop subscriptions), and <c>GET /durable-flow/{id}</c> returned the full
    /// ledger, input JSON included. Pre-fix failure: <c>/publish</c> answers 202 here.
    /// </summary>
    [Fact]
    public async Task ProductionByDefault_DoesNotMapTheSimulationInjectionOrLedgerRoutes()
    {
        await using var factory = new GatedAppFactory(environment: "Production", enableTestEndpoints: null);
        using var client = factory.CreateClient();

        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync("/publish?correlationId=any&status=Completed", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync("/crash", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync("/arm", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync("/lost-subscriber-flow?outcome=Completed", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync("/emit-response?correlationId=any&useAttribute=true", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/calls?key=any")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/durable-flow/any")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync("/durable-flow/any/resume", content: null)).StatusCode);

        // Starting a flow is the demo itself and stays operational.
        Assert.Equal(HttpStatusCode.OK, (await client.PostAsync("/durable-flow?name=acme", content: null)).StatusCode);
    }

    /// <summary>
    /// The whole route inventory, so a new unauthenticated affordance cannot slip into Production
    /// unnoticed: everything the sample maps there must be on this list.
    /// </summary>
    [Fact]
    public async Task ProductionByDefault_MapsExactlyTheOperationalRouteInventory()
    {
        await using var factory = new GatedAppFactory(environment: "Production", enableTestEndpoints: null);
        using var client = factory.CreateClient(); // forces the host to build

        var mapped = factory.Services.GetRequiredService<EndpointDataSource>().Endpoints
            .OfType<RouteEndpoint>()
            .Select(endpoint => endpoint.RoutePattern.RawText ?? string.Empty)
            .Select(pattern => pattern.StartsWith('/') ? pattern : "/" + pattern)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(pattern => pattern, StringComparer.Ordinal)
            .ToArray();

        string[] operational =
        [
            "/",
            "/alive",
            "/ambient-exception",
            "/attach",
            "/config",
            "/durable-flow",
            "/durable-flow-child",
            "/healthz",
            "/multi-step",
            "/openapi/{documentName}.json",
            "/reply-target",
            "/request-response",
            "/shared-correlation-exception",
            "/worker",
        ];
        string[] gated =
        [
            "/arm", "/calls", "/crash", "/durable-flow/{flowId}", "/durable-flow/{flowId}/resume", "/emit-response",
            "/lost-subscriber-flow", "/publish", "/seed-recovery", "/test/recovery/{correlationId}", "/test/reset",
        ];

        Assert.Empty(mapped.Intersect(gated, StringComparer.Ordinal));
        Assert.Equal(operational.OrderBy(p => p, StringComparer.Ordinal), mapped);
    }

    [Fact]
    public async Task ExplicitOptIn_MapsTheTestMutationRoutes_EvenInProduction()
    {
        await using var factory = new GatedAppFactory(environment: "Production", enableTestEndpoints: true);
        using var client = factory.CreateClient();

        Assert.Equal(HttpStatusCode.OK, (await client.PostAsync("/test/reset", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await client.DeleteAsync("/test/recovery/any")).StatusCode);
        // The simulation and ledger routes ride the same switch.
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/durable-flow/unknown-run")).StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, (await client.PostAsync("/publish?correlationId=nobody&status=Completed", content: null)).StatusCode);
    }

    [Fact]
    public async Task ExplicitOptOut_UnmapsTheTestMutationRoutes_EvenInDevelopment()
    {
        await using var factory = new GatedAppFactory(environment: "Development", enableTestEndpoints: false);
        using var client = factory.CreateClient();

        Assert.Equal(HttpStatusCode.NotFound, (await client.PostAsync("/test/reset", content: null)).StatusCode);
    }

    /// <summary>
    /// Boots the sample in-process on the fully in-memory providers. <see cref="SampleFlowService"/>
    /// only identifies the sample's assembly (the referenced Aspire AppHost also defines a
    /// <c>Program</c>).
    /// </summary>
    private sealed class GatedAppFactory(string environment, bool? enableTestEndpoints) : WebApplicationFactory<SampleFlowService>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment(environment);
            builder.UseSetting("AsyncResponse:Channel", "InMemory");
            builder.UseSetting("AsyncResponse:Transport", "InMemory");
            if (enableTestEndpoints is { } enable)
                builder.UseSetting("Sample:EnableTestEndpoints", enable ? "true" : "false");
        }
    }
}
