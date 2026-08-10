using AsyncResponse.Sample;
using System.Net.Http.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The real Google Pub/Sub transport (emulator): worker jobs published and consumed over Pub/Sub,
/// and responses ingested from a Pub/Sub topic into active waiters.
/// </summary>
[Collection(BrokersCollection.Name)]
    public sealed class PubSubTransportTests(BrokersBatchFixture fixture) : IntegrationTestBase(fixture)
    {
        [Fact]
        public async Task Config_ReportsDefaultAndEarlyAckPubSubModes()
        {
            var defaultConfig = (await Client.GetFromJsonAsync<ConfigResponse>("/config"))!;
            var earlyAckConfig = (await Fixture.EarlyAckClient.GetFromJsonAsync<ConfigResponse>("/config"))!;

            Assert.Equal("Redis", defaultConfig.Channel);
            Assert.Equal("GooglePubSub", defaultConfig.Transport);
            Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Pubsub!.WorkerAckMode);
            Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Pubsub.ResponseAckMode);

            Assert.Equal("Redis", earlyAckConfig.Channel);
            Assert.Equal("GooglePubSub", earlyAckConfig.Transport);
            Assert.Equal("AckAfterEnqueue", earlyAckConfig.Pubsub!.WorkerAckMode);
            Assert.Equal(4, earlyAckConfig.Pubsub.WorkerBackgroundWorkerCount);
            Assert.Equal(256, earlyAckConfig.Pubsub.WorkerBackgroundQueueCapacity);
            Assert.Equal("AckAfterHandlerCompletes", earlyAckConfig.Pubsub.ResponseAckMode);
        }

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
        public async Task WorkerJob_RoundTripsThroughPubSub_WithAckAfterEnqueueWorkerSubscriber()
        {
            var token = NewId("early-token");
            var trace = NewId("early-trace");

            var response = await Fixture.EarlyAckClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
            response.EnsureSuccessStatusCode();
            var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

            var call = await WaitForCallAsync(Fixture.EarlyAckClient, $"worker:{token}");
            Assert.Equal("worker", call.Kind);
            Assert.Equal(correlationId, call.CorrelationId);
            Assert.Equal(trace, call.Trace);
        }

    [Fact]
    public async Task ResponseIngress_CorrelationViaAttribute_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(NewId("trace"));

        (await Client.PostAsync($"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=true", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync($"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
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
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    private sealed record WorkerResponse(string CorrelationId);
    private sealed record ConfigResponse(string Channel, string Transport, PubSubConfig? Pubsub);
    private sealed record PubSubConfig(
        string WorkerAckMode,
        int WorkerBackgroundWorkerCount,
        int WorkerBackgroundQueueCapacity,
        string ResponseAckMode);
}
