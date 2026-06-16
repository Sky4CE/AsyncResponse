using AsyncResponse.IntegrationTests.App;
using System.Net.Http.Json;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>Watchdog/health surfacing, reply targets, and HTTP-driven end-to-end flows.</summary>
[Collection(IntegrationCollection.Name)]
public sealed class WatchdogReplyTargetHttpTests(IntegrationFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Watchdog_SurfacesStaleRecoveryStateAsDegradedHealth()
    {
        var correlationId = NewId("stale");

        try
        {
            // A recovery entry with no live subscriber, registered well before the (2s) stale threshold.
            (await Client.PostAsync($"/seed-recovery?correlationId={correlationId}&ageMinutes=5", content: null))
                .EnsureSuccessStatusCode();

            var status = await PollAsync(
                GetHealthStatusAsync,
                s => s == "Degraded",
                TimeSpan.FromSeconds(20));

            Assert.Equal("Degraded", status);
        }
        finally
        {
            await CleanupRecoveryStateAsync(correlationId);
        }
    }

    [Fact]
    public async Task WithReplyTarget_ExposesThePubSubResponseTopicAddress()
    {
        var response = await Client.GetAsync("/reply-target");
        response.EnsureSuccessStatusCode();

        var target = await response.Content.ReadFromJsonAsync<ReplyTargetResult>();
        Assert.Equal("google-pubsub", target!.Transport);
        Assert.Contains(IntegrationFixture.ResponseTopicId, target.Address, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Http_RequestResponse_Succeed_ReturnsCompleted()
    {
        var response = await Client.PostAsync("/request-response?behavior=Succeed", content: null);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<RequestResponseResult>();
        Assert.Equal(ItestStatus.Completed, result!.Status);
    }

    [Fact]
    public async Task Http_Worker_ExecutesJobThroughPubSub()
    {
        var token = NewId("token");
        var trace = NewId("trace");

        var response = await Client.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();

        var call = await WaitForCallAsync($"worker:{token}");
        Assert.Equal(token, call.Detail);
        Assert.Equal(trace, call.Trace);
    }

    private sealed record ReplyTargetResult(string Transport, string Address);

    private async Task<string?> GetHealthStatusAsync()
    {
        using var document = JsonDocument.Parse(await Client.GetStringAsync("/healthz"));
        return document.RootElement.GetProperty("status").GetString();
    }

    private async Task CleanupRecoveryStateAsync(string correlationId)
    {
        try
        {
            using var response = await Client.DeleteAsync($"/test/recovery/{correlationId}");
            response.EnsureSuccessStatusCode();

            await PollAsync(
                GetHealthStatusAsync,
                s => s == "Healthy",
                TimeSpan.FromSeconds(5));
        }
        catch
        {
            // Cleanup is best-effort; the assertion above should remain the failure signal.
        }
    }
}
