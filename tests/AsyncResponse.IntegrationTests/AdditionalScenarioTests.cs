using AsyncResponse.Sample;
using System.Net.Http.Json;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Additional end-to-end scenarios over the real stack (Redis channel + Google Pub/Sub transport):
/// attaching to an in-flight operation, the second context propagator surviving the broker hop, a
/// failed domain payload arriving through the Pub/Sub response ingress, and the watchdog health
/// returning to Healthy once stale recovery state is cleared.
/// </summary>
[Collection(BrokersCollection.Name)]
public sealed class AdditionalScenarioTests(BrokersBatchFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Attach_OverRedis_CompletesByCorrelationId()
    {
        var response = await Client.PostAsync("/attach", content: null);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<AttachResult>();
        Assert.Equal(OperationStatus.Completed, result!.Status);
    }

    [Fact]
    public async Task Worker_OverPubSub_RestoresBothTraceAndTenant()
    {
        var token = NewId("token");
        var trace = NewId("trace");

        (await Client.PostAsync($"/worker?token={token}&trace={trace}", content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync($"worker:{token}");
        Assert.Equal(trace, call.Trace);            // first propagator survives the serialized hop
        Assert.Equal("tenant-acme", call.Tenant);   // and so does the second — propagators compose
    }

    [Fact]
    public async Task PubSubResponseIngress_FailedDomainPayload_CompletesWaiterAsFailed()
    {
        var correlationId = await ArmAsync(NewId("trace"));

        // A Failed domain payload arriving through the Pub/Sub response ingress is still a valid
        // response for the live waiter (it is NOT routed to recovery) — the waiter completes with it.
        (await Client.PostAsync($"/emit-response?correlationId={correlationId}&status=Failed&useAttribute=true&message=remote%20failed", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync($"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Failed, call.Status);
    }

    [Fact]
    public async Task Watchdog_ReturnsToHealthy_AfterStaleRecoveryStateIsCleared()
    {
        var correlationId = NewId("stale");

        (await Client.PostAsync($"/seed-recovery?correlationId={correlationId}&ageMinutes=5", content: null))
            .EnsureSuccessStatusCode();

        var degraded = await PollAsync(GetHealthStatusAsync, s => s == "Degraded", TimeSpan.FromSeconds(20));
        Assert.Equal("Degraded", degraded);

        (await Client.DeleteAsync($"/test/recovery/{correlationId}")).EnsureSuccessStatusCode();

        var healthy = await PollAsync(GetHealthStatusAsync, s => s == "Healthy", TimeSpan.FromSeconds(20));
        Assert.Equal("Healthy", healthy);
    }

    [Fact]
    public async Task Config_ReportsRedisChannelAndPubSubTransport()
    {
        // Proves the Aspire-orchestrated SUT boots the sample with the Redis channel + Google Pub/Sub
        // transport (useRedis=true, useGooglePubSub=true) — the variation a same-process breakpoint
        // never sees, because this SUT runs as a separate child process.
        var config = await Client.GetFromJsonAsync<ProviderConfig>("/config");
        Assert.Equal("Redis", config!.Channel);
        Assert.Equal("GooglePubSub", config.Transport);
    }

    private async Task<string?> GetHealthStatusAsync()
    {
        using var document = JsonDocument.Parse(await Client.GetStringAsync("/healthz"));
        return document.RootElement.GetProperty("status").GetString();
    }

    private sealed record AttachResult(string CorrelationId, OperationStatus Status, string? Message);
    private sealed record ProviderConfig(string Channel, string Transport);
}
