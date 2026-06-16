using AsyncResponse.IntegrationTests.App;
using System.Net.Http.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The real Google Pub/Sub transport (emulator): worker jobs published and consumed over Pub/Sub,
/// and responses ingested from a Pub/Sub topic into active waiters.
/// </summary>
[Collection(IntegrationCollection.Name)]
public sealed class PubSubTransportTests(IntegrationFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task WorkerJob_RoundTripsThroughPubSub_WithRestoredCorrelationAndTrace()
    {
        var token = NewId("token");
        var trace = NewId("trace");

        var response = await Client.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync($"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId); // correlation id restored across the Pub/Sub hop
        Assert.Equal(trace, call.Trace);                 // trace baggage restored across the Pub/Sub hop
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaAttribute_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(NewId("trace"));

        (await Client.PostAsync($"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=true", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync($"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(ItestStatus.Completed, call.Status);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaJsonBody_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(NewId("trace"));

        // No correlation-id attribute → the extractor falls back to the JSON body (CorrelationId path).
        (await Client.PostAsync($"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=false", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync($"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(ItestStatus.Completed, call.Status);
    }

    private sealed record WorkerResponse(string CorrelationId);
}
