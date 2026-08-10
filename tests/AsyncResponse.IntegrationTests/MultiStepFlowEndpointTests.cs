using AsyncResponse.Sample;
using System.Net.Http.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Endpoint-driven multi-step flows over the full Aspire stack (Redis channel + Google Pub/Sub
/// transport). These tests intentionally compose the same HTTP affordances the sample exposes,
/// because this is where subscribe-before-send, SetException, and recovery behavior meet.
/// </summary>
[Collection(BrokersCollection.Name)]
[Trait(Batches.Trait, Batches.Brokers)]
public sealed class MultiStepFlowEndpointTests(BrokersBatchFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task MultiStep_OverRedis_SucceedThenSucceed_CompletesBothStepsInOrder()
    {
        var response = await Client.PostAsync("/multi-step?first=Succeed&second=Succeed", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<MultiStepFlowResult>();
        Assert.True(result!.Completed);
        Assert.Null(result.FailedAt);
        Assert.Collection(
            result.Steps,
            first =>
            {
                Assert.Equal("first", first.Name);
                Assert.True(first.Succeeded);
                Assert.Equal(OperationStatus.Completed, first.Status);
            },
            second =>
            {
                Assert.Equal("second", second.Name);
                Assert.True(second.Succeeded);
                Assert.Equal(OperationStatus.Completed, second.Status);
            });
        Assert.NotEqual(result.Steps[0].CorrelationId, result.Steps[1].CorrelationId);
    }

    [Fact]
    public async Task MultiStep_OverRedis_SecondTechnicalFailure_StopsAtSecondStep()
    {
        var response = await Client.PostAsync("/multi-step?first=Succeed&second=Fail", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<MultiStepFlowResult>();
        Assert.False(result!.Completed);
        Assert.Equal("second", result.FailedAt);
        Assert.Equal(2, result.Steps.Count);
        Assert.True(result.Steps[0].Succeeded);
        Assert.False(result.Steps[1].Succeeded);
        Assert.Contains("second technical error", result.Steps[1].Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AmbientException_OverRedis_UsesPublisherAmbientCorrelationFallback()
    {
        var response = await Client.PostAsync("/ambient-exception?message=ambient%20redis%20boom", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<AmbientExceptionResult>();
        Assert.True(result!.Faulted);
        Assert.Contains("ambient redis boom", result.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SharedCorrelationException_OverRedis_FaultsBothAttachedWaiters()
    {
        var response = await Client.PostAsync("/shared-correlation-exception?message=redis%20fanout%20boom", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<SharedExceptionResult>();
        Assert.Equal(2, result!.Failures.Count);
        Assert.All(result.Failures, failure => Assert.Contains("redis fanout boom", failure, StringComparison.Ordinal));
    }

    [Fact]
    public async Task LostSubscriberFlow_Completed_ComposesArmCrashPublishAndResumeCallback()
    {
        var trace = NewId("trace");

        var response = await Client.PostAsync($"/lost-subscriber-flow?outcome=Completed&trace={trace}", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<LostSubscriberFlowResult>();
        Assert.Equal("Completed", result!.Outcome);
        Assert.Equal("resume", result.Callback.Kind);
        Assert.Equal(OperationStatus.Completed, result.Callback.Status);
        Assert.Equal(trace, result.Callback.Trace);
        Assert.Equal("tenant-acme", result.Callback.Tenant);
    }

    [Fact]
    public async Task LostSubscriberFlow_Exception_ComposesArmCrashPublishAndFailureCallback()
    {
        var trace = NewId("trace");

        var response = await Client.PostAsync($"/lost-subscriber-flow?outcome=Exception&trace={trace}", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<LostSubscriberFlowResult>();
        Assert.Equal("exception", result!.Outcome);
        Assert.Equal("fail", result.Callback.Kind);
        Assert.Equal(nameof(InvalidOperationException), result.Callback.Detail);
        Assert.Equal(trace, result.Callback.Trace);
        Assert.Equal("tenant-acme", result.Callback.Tenant);
    }

    [Fact]
    public async Task LostSubscriberFlow_FailedPayload_ComposesArmCrashPublishAndDomainFailureCallback()
    {
        var response = await Client.PostAsync("/lost-subscriber-flow?outcome=Failed", content: null);

        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<LostSubscriberFlowResult>();
        Assert.Equal("Failed", result!.Outcome);
        Assert.Equal("fail", result.Callback.Kind);
        Assert.Equal("domain-failure", result.Callback.Detail);
    }

    private sealed record AmbientExceptionResult(bool Faulted, string ExceptionType, string Detail);
}
