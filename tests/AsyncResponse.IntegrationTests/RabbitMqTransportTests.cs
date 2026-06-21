using AsyncResponse.Sample;
using System.Net.Http.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The real RabbitMQ transport: worker jobs published and consumed over RabbitMQ, and responses
/// ingested from a RabbitMQ queue into active waiters.
/// </summary>
[Collection(IntegrationCollection.Name)]
public sealed class RabbitMqTransportTests(IntegrationFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Config_ReportsDefaultAndEarlyAckRabbitMqModes()
    {
        var defaultConfig = (await Fixture.RabbitMqClient.GetFromJsonAsync<ConfigResponse>("/config"))!;
        var earlyAckConfig = (await Fixture.RabbitMqEarlyAckClient.GetFromJsonAsync<ConfigResponse>("/config"))!;

        Assert.Equal("Redis", defaultConfig.Channel);
        Assert.Equal("RabbitMQ", defaultConfig.Transport);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Rabbitmq!.WorkerAckMode);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Rabbitmq.ResponseAckMode);

        Assert.Equal("Redis", earlyAckConfig.Channel);
        Assert.Equal("RabbitMQ", earlyAckConfig.Transport);
        Assert.Equal("AckAfterEnqueue", earlyAckConfig.Rabbitmq!.WorkerAckMode);
        Assert.Equal(4, earlyAckConfig.Rabbitmq.WorkerBackgroundWorkerCount);
        Assert.Equal(256, earlyAckConfig.Rabbitmq.WorkerBackgroundQueueCapacity);
        Assert.Equal("AckAfterHandlerCompletes", earlyAckConfig.Rabbitmq.ResponseAckMode);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughRabbitMq_WithRestoredCorrelationAndTrace()
    {
        var token = NewId("rabbit-token");
        var trace = NewId("rabbit-trace");

        var response = await Fixture.RabbitMqClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.RabbitMqClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughRabbitMq_WithAckAfterEnqueueWorkerSubscriber()
    {
        var token = NewId("rabbit-early-token");
        var trace = NewId("rabbit-early-trace");

        var response = await Fixture.RabbitMqEarlyAckClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.RabbitMqEarlyAckClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WithReplyTarget_ExposesTheRabbitMqResponseDestination()
    {
        var response = await Fixture.RabbitMqClient.GetAsync("/reply-target");
        response.EnsureSuccessStatusCode();

        var target = await response.Content.ReadFromJsonAsync<ReplyTargetResult>();
        Assert.Equal("rabbitmq", target!.Transport);
        Assert.Equal(
            $"{IntegrationFixture.RabbitMqResponseExchange}:{IntegrationFixture.RabbitMqResponseRoutingKey}",
            target.Address);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaHeader_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.RabbitMqClient, NewId("rabbit-trace"));

        (await Fixture.RabbitMqClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=true",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.RabbitMqClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaJsonBody_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.RabbitMqClient, NewId("rabbit-trace"));

        (await Fixture.RabbitMqClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=false",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.RabbitMqClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    private sealed record WorkerResponse(string CorrelationId);
    private sealed record ReplyTargetResult(string Transport, string Address);
    private sealed record ConfigResponse(string Channel, string Transport, RabbitMqConfig? Rabbitmq);
    private sealed record RabbitMqConfig(
        string WorkerAckMode,
        int WorkerBackgroundWorkerCount,
        int WorkerBackgroundQueueCapacity,
        string ResponseAckMode);
}
