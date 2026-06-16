using AsyncResponse.Sample;
using System.Net;
using System.Net.Http.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>Active-waiter behavior over a real Redis pub/sub response channel, driven via the SUT.</summary>
[Collection(IntegrationCollection.Name)]
public sealed class RedisChannelTests(IntegrationFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task HappyPath_ConsumesProgress_AndCompletesOnTerminal()
    {
        var response = await Client.PostAsync("/request-response?behavior=Succeed", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<RequestResponseResult>();
        Assert.Equal(OperationStatus.Completed, result!.Status);
        Assert.Equal("done", result.Message);
    }

    [Fact]
    public async Task FailedDomainPayload_IsDeliveredToTheActiveWaiter()
    {
        // A failed *payload* is a valid response for an active waiter — it is NOT routed to recovery.
        var response = await Client.PostAsync("/request-response?behavior=FailDomain", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<RequestResponseResult>();
        Assert.Equal(OperationStatus.Failed, result!.Status);
        Assert.Equal("remote failed", result.Message);
    }

    [Fact]
    public async Task SetException_FaultsTheActiveWaiter()
    {
        var response = await Client.PostAsync("/request-response?behavior=Fail", content: null);

        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("remote technical error", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NoResponse_TimesOut()
    {
        var response = await Client.PostAsync("/request-response?behavior=Timeout", content: null);

        Assert.Equal(HttpStatusCode.GatewayTimeout, response.StatusCode);
    }
}
