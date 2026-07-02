using AsyncResponse.Sample;
using System.Net.Http.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The real Kafka transport against a single-broker KRaft container: worker jobs published and
/// consumed over classic consumer groups with manual offset management, and responses ingested
/// from a Kafka response topic into active waiters.
/// </summary>
[Collection(IntegrationCollection.Name)]
public sealed class KafkaTransportTests(IntegrationFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Config_ReportsDefaultAndEarlyAckKafkaModes()
    {
        var defaultConfig = (await Fixture.KafkaClient.GetFromJsonAsync<ConfigResponse>("/config"))!;
        var earlyAckConfig = (await Fixture.KafkaEarlyAckClient.GetFromJsonAsync<ConfigResponse>("/config"))!;

        Assert.Equal("Redis", defaultConfig.Channel);
        Assert.Equal("Kafka", defaultConfig.Transport);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Kafka!.WorkerAckMode);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Kafka.ResponseAckMode);
        Assert.Equal("asyncresponse.itest.worker", defaultConfig.Kafka.WorkerTopic);
        Assert.Equal("asyncresponse.itest.response", defaultConfig.Kafka.ResponseTopic);
        Assert.Equal("asyncresponse.itest.worker.deadletter", defaultConfig.Kafka.DeadLetterTopic);

        Assert.Equal("Redis", earlyAckConfig.Channel);
        Assert.Equal("Kafka", earlyAckConfig.Transport);
        Assert.Equal("AckAfterEnqueue", earlyAckConfig.Kafka!.WorkerAckMode);
        Assert.Equal(4, earlyAckConfig.Kafka.WorkerBackgroundWorkerCount);
        Assert.Equal(256, earlyAckConfig.Kafka.WorkerBackgroundQueueCapacity);
        Assert.Equal("AckAfterHandlerCompletes", earlyAckConfig.Kafka.ResponseAckMode);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughKafka_WithRestoredCorrelationAndTrace()
    {
        var token = NewId("kafka-token");
        var trace = NewId("kafka-trace");

        var response = await Fixture.KafkaClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.KafkaClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughKafka_WithAckAfterEnqueueWorkerSubscriber()
    {
        var token = NewId("kafka-early-token");
        var trace = NewId("kafka-early-trace");

        var response = await Fixture.KafkaEarlyAckClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.KafkaEarlyAckClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WithReplyTarget_ExposesTheKafkaResponseTopic()
    {
        var response = await Fixture.KafkaClient.GetAsync("/reply-target");
        response.EnsureSuccessStatusCode();

        var target = await response.Content.ReadFromJsonAsync<ReplyTargetResult>();
        Assert.Equal("kafka", target!.Transport);
        Assert.Equal("asyncresponse.itest.response", target.Address);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaHeader_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.KafkaClient, NewId("kafka-trace"));

        (await Fixture.KafkaClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=true",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.KafkaClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaJsonBody_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.KafkaClient, NewId("kafka-trace"));

        (await Fixture.KafkaClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=false",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.KafkaClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task RequestResponse_DomainFailure_RoutesToWaiterOverKafka()
    {
        var response = await Fixture.KafkaClient.PostAsync("/request-response?behavior=FailDomain", content: null);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<RequestResponseResult>();
        Assert.Equal(OperationStatus.Failed, result!.Status);
    }

    private sealed record WorkerResponse(string CorrelationId);
    private sealed record ReplyTargetResult(string Transport, string Address);
    private sealed record ConfigResponse(string Channel, string Transport, KafkaConfig? Kafka);
    private sealed record KafkaConfig(
        string WorkerTopic,
        string ResponseTopic,
        string? DeadLetterTopic,
        string WorkerAckMode,
        int WorkerBackgroundWorkerCount,
        int WorkerBackgroundQueueCapacity,
        string ResponseAckMode);
}
