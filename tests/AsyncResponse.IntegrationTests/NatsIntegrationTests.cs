using AsyncResponse.Sample;
using System.Net.Http.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The real NATS stack end to end: the NATS channel (Core request/reply + JetStream KV recovery) and
/// the NATS JetStream transport (worker dispatch + response ingress), both over one NATS server with
/// JetStream enabled. Every scenario is driven over HTTP against the Aspire-orchestrated sample app.
/// </summary>
[Collection(IntegrationCollection.Name)]
public sealed class NatsIntegrationTests(IntegrationFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Config_ReportsNatsChannelTransportAndEarlyAckMode()
    {
        var defaultConfig = (await Fixture.NatsClient.GetFromJsonAsync<ConfigResponse>("/config"))!;
        var earlyAckConfig = (await Fixture.NatsEarlyAckClient.GetFromJsonAsync<ConfigResponse>("/config"))!;

        Assert.Equal("NATS", defaultConfig.Channel);
        Assert.Equal("NATS", defaultConfig.Transport);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Nats!.WorkerAckMode);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Nats.ResponseAckMode);
        Assert.EndsWith(".transport.worker", defaultConfig.Nats.WorkerSubject, StringComparison.Ordinal);
        Assert.EndsWith(".transport.response", defaultConfig.Nats.ResponseSubject, StringComparison.Ordinal);

        Assert.Equal("NATS", earlyAckConfig.Channel);
        Assert.Equal("AckAfterEnqueue", earlyAckConfig.Nats!.WorkerAckMode);
        Assert.Equal(4, earlyAckConfig.Nats.WorkerBackgroundWorkerCount);
        Assert.Equal(256, earlyAckConfig.Nats.WorkerBackgroundQueueCapacity);
        Assert.Equal("AckAfterHandlerCompletes", earlyAckConfig.Nats.ResponseAckMode);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughNats_WithRestoredCorrelationAndTrace()
    {
        var token = NewId("nats-token");
        var trace = NewId("nats-trace");

        var response = await Fixture.NatsClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.NatsClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughNats_WithAckAfterEnqueueWorkerSubscriber()
    {
        var token = NewId("nats-early-token");
        var trace = NewId("nats-early-trace");

        var response = await Fixture.NatsEarlyAckClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.NatsEarlyAckClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WithReplyTarget_ExposesTheNatsResponseSubject()
    {
        var response = await Fixture.NatsClient.GetAsync("/reply-target");
        response.EnsureSuccessStatusCode();

        var target = await response.Content.ReadFromJsonAsync<ReplyTargetResult>();
        Assert.Equal("NATS", target!.Transport);
        Assert.EndsWith(".transport.response", target.Address, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaHeader_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.NatsClient, NewId("nats-trace"));

        (await Fixture.NatsClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=true",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.NatsClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaJsonBody_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.NatsClient, NewId("nats-trace"));

        (await Fixture.NatsClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=false",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.NatsClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task RequestResponse_Succeeds_OverNatsChannel()
    {
        var response = await Fixture.NatsClient.PostAsync("/request-response?behavior=Succeed", content: null);
        response.EnsureSuccessStatusCode();

        var result = (await response.Content.ReadFromJsonAsync<RequestResponseResult>())!;
        Assert.Equal(OperationStatus.Completed, result.Status);
    }

    [Fact]
    public async Task RequestResponse_DomainFailure_IsReturnedAsData()
    {
        var response = await Fixture.NatsClient.PostAsync("/request-response?behavior=FailDomain", content: null);
        response.EnsureSuccessStatusCode();

        var result = (await response.Content.ReadFromJsonAsync<RequestResponseResult>())!;
        Assert.Equal(OperationStatus.Failed, result.Status);
    }

    private sealed record WorkerResponse(string CorrelationId);
    private sealed record ReplyTargetResult(string Transport, string Address);
    private sealed record ConfigResponse(string Channel, string Transport, NatsConfig? Nats);
    private sealed record NatsConfig(
        string? SubjectPrefix,
        string? RecoveryBucket,
        string WorkerSubject,
        string ResponseSubject,
        string WorkerAckMode,
        int WorkerBackgroundWorkerCount,
        int WorkerBackgroundQueueCapacity,
        string ResponseAckMode);
}
