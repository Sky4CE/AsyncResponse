using AsyncResponse.Sample;
using System.Net.Http.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The real Azure Service Bus transport over the local emulator: worker jobs published and consumed
/// from Service Bus queues, and responses ingested from a Service Bus response queue into active or
/// recoverable waiters.
/// </summary>
[Collection(CloudCollection.Name)]
public sealed class AzureServiceBusTransportTests(CloudBatchFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Config_ReportsDefaultAndEarlyAckAzureServiceBusModes()
    {
        var defaultConfig = (await Fixture.AzureServiceBusClient.GetFromJsonAsync<ConfigResponse>("/config"))!;
        var earlyAckConfig = (await Fixture.AzureServiceBusEarlyAckClient.GetFromJsonAsync<ConfigResponse>("/config"))!;

        Assert.Equal("Redis", defaultConfig.Channel);
        Assert.Equal("AzureServiceBus", defaultConfig.Transport);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.AzureServiceBus!.WorkerAckMode);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.AzureServiceBus.ResponseAckMode);

        Assert.Equal("Redis", earlyAckConfig.Channel);
        Assert.Equal("AzureServiceBus", earlyAckConfig.Transport);
        Assert.Equal("AckAfterEnqueue", earlyAckConfig.AzureServiceBus!.WorkerAckMode);
        Assert.Equal(4, earlyAckConfig.AzureServiceBus.WorkerBackgroundWorkerCount);
        Assert.Equal(256, earlyAckConfig.AzureServiceBus.WorkerBackgroundQueueCapacity);
        Assert.Equal("AckAfterHandlerCompletes", earlyAckConfig.AzureServiceBus.ResponseAckMode);
    }

    [Fact]
    public async Task RequestResponse_Success_RoundTripsThroughAzureServiceBus()
    {
        var response = await Fixture.AzureServiceBusClient.PostAsync("/request-response?behavior=Succeed", content: null);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<RequestResponseResult>();
        Assert.Equal(OperationStatus.Completed, result!.Status);
    }

    [Fact]
    public async Task RequestResponse_DomainFailure_IsReturnedAsData()
    {
        var response = await Fixture.AzureServiceBusClient.PostAsync("/request-response?behavior=FailDomain", content: null);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<RequestResponseResult>();
        Assert.Equal(OperationStatus.Failed, result!.Status);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughAzureServiceBus_WithRestoredCorrelationAndTrace()
    {
        var token = NewId("asb-token");
        var trace = NewId("asb-trace");

        var response = await Fixture.AzureServiceBusClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.AzureServiceBusClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughAzureServiceBus_WithAckAfterEnqueueWorkerSubscriber()
    {
        var token = NewId("asb-early-token");
        var trace = NewId("asb-early-trace");

        var response = await Fixture.AzureServiceBusEarlyAckClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.AzureServiceBusEarlyAckClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WithReplyTarget_ExposesTheAzureServiceBusResponseQueue()
    {
        var response = await Fixture.AzureServiceBusClient.GetAsync("/reply-target");
        response.EnsureSuccessStatusCode();

        var target = await response.Content.ReadFromJsonAsync<ReplyTargetResult>();
        Assert.Equal("AzureServiceBus", target!.Transport);
        Assert.Equal("asyncresponse-itest-asb-response", target.Address);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaProperty_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.AzureServiceBusClient, NewId("asb-trace"));

        (await Fixture.AzureServiceBusClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=true",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.AzureServiceBusClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaJsonBody_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.AzureServiceBusClient, NewId("asb-trace"));

        (await Fixture.AzureServiceBusClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=false",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.AzureServiceBusClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task LateRecovery_CompletedResponse_ResumesCallback()
    {
        var response = await Fixture.AzureServiceBusClient.PostAsync(
            $"/lost-subscriber-flow?outcome=Completed&trace={NewId("asb-late-trace")}",
            content: null);

        response.EnsureSuccessStatusCode();
    }

    private sealed record WorkerResponse(string CorrelationId);
    private sealed record ReplyTargetResult(string Transport, string Address);
    private sealed record ConfigResponse(string Channel, string Transport, AzureServiceBusConfig? AzureServiceBus);
    private sealed record AzureServiceBusConfig(
        string WorkerAckMode,
        int WorkerBackgroundWorkerCount,
        int WorkerBackgroundQueueCapacity,
        string ResponseAckMode);
}
