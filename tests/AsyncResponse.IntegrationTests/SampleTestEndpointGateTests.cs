using AsyncResponse.Sample;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
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

    [Fact]
    public async Task ExplicitOptIn_MapsTheTestMutationRoutes_EvenInProduction()
    {
        await using var factory = new GatedAppFactory(environment: "Production", enableTestEndpoints: true);
        using var client = factory.CreateClient();

        Assert.Equal(HttpStatusCode.OK, (await client.PostAsync("/test/reset", content: null)).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await client.DeleteAsync("/test/recovery/any")).StatusCode);
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
