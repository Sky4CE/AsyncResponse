using AsyncResponse.Sample;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Real lost-subscriber recovery: arm a waiter on Redis, drop subscriptions (simulated redeploy),
/// then deliver a late response and assert the persisted recovery callbacks fire correctly.
/// </summary>
[Collection(IntegrationCollection.Name)]
public sealed class LostSubscriberRecoveryTests(IntegrationFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task CompletedResponse_AfterCrash_InvokesResumeCallback_WithRestoredTrace()
    {
        var trace = NewId("trace");
        var correlationId = await ArmAsync(trace);
        (await Client.PostAsync("/crash", content: null)).EnsureSuccessStatusCode();

        (await Client.PostAsync($"/publish?correlationId={correlationId}&status=Completed&message=late", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync($"resume:{correlationId}");
        Assert.Equal("resume", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
        Assert.Equal(trace, call.Trace); // baggage restored from the persisted recovery state
    }

    [Fact]
    public async Task FailedDomainResponse_AfterCrash_InvokesFailureCallback()
    {
        var correlationId = await ArmAsync(NewId("trace"));
        (await Client.PostAsync("/crash", content: null)).EnsureSuccessStatusCode();

        (await Client.PostAsync($"/publish?correlationId={correlationId}&status=Failed&message=remote%20failed", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync($"fail:{correlationId}");
        Assert.Equal("fail", call.Kind);
        Assert.Equal("domain:Failed", call.Detail); // delivered as AsyncResponseDomainFailureException
    }

    [Fact]
    public async Task SetException_AfterCrash_InvokesFailureCallback()
    {
        var correlationId = await ArmAsync(NewId("trace"));
        (await Client.PostAsync("/crash", content: null)).EnsureSuccessStatusCode();

        (await Client.PostAsync($"/publish?correlationId={correlationId}&exception=technical", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync($"fail:{correlationId}");
        Assert.Equal("fail", call.Kind);
        Assert.Equal(nameof(InvalidOperationException), call.Detail);
    }
}
