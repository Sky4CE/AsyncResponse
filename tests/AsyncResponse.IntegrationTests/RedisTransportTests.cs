using AsyncResponse.Sample;
using System.Net.Http.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The real Redis Streams transport: worker jobs published and consumed over Redis consumer groups,
/// and responses ingested from a Redis response stream into active waiters.
/// </summary>
[Collection(IntegrationCollection.Name)]
public sealed class RedisTransportTests(IntegrationFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Config_ReportsDefaultAndEarlyAckRedisModes()
    {
        var defaultConfig = (await Fixture.RedisTransportClient.GetFromJsonAsync<ConfigResponse>("/config"))!;
        var earlyAckConfig = (await Fixture.RedisTransportEarlyAckClient.GetFromJsonAsync<ConfigResponse>("/config"))!;

        Assert.Equal("Redis", defaultConfig.Channel);
        Assert.Equal("Redis", defaultConfig.Transport);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Redis!.WorkerAckMode);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Redis.ResponseAckMode);
        Assert.EndsWith(":transport:worker", defaultConfig.Redis.WorkerStream, StringComparison.Ordinal);
        Assert.EndsWith(":transport:response", defaultConfig.Redis.ResponseStream, StringComparison.Ordinal);

        Assert.Equal("Redis", earlyAckConfig.Channel);
        Assert.Equal("Redis", earlyAckConfig.Transport);
        Assert.Equal("AckAfterEnqueue", earlyAckConfig.Redis!.WorkerAckMode);
        Assert.Equal(4, earlyAckConfig.Redis.WorkerBackgroundWorkerCount);
        Assert.Equal(256, earlyAckConfig.Redis.WorkerBackgroundQueueCapacity);
        Assert.Equal("AckAfterHandlerCompletes", earlyAckConfig.Redis.ResponseAckMode);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughRedis_WithRestoredCorrelationAndTrace()
    {
        var token = NewId("redis-token");
        var trace = NewId("redis-trace");

        var response = await Fixture.RedisTransportClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.RedisTransportClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughRedis_WithAckAfterEnqueueWorkerSubscriber()
    {
        var token = NewId("redis-early-token");
        var trace = NewId("redis-early-trace");

        var response = await Fixture.RedisTransportEarlyAckClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.RedisTransportEarlyAckClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WithReplyTarget_ExposesTheRedisResponseStream()
    {
        var response = await Fixture.RedisTransportClient.GetAsync("/reply-target");
        response.EnsureSuccessStatusCode();

        var target = await response.Content.ReadFromJsonAsync<ReplyTargetResult>();
        Assert.Equal("redis", target!.Transport);
        Assert.EndsWith(":transport:response", target.Address, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaStreamField_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.RedisTransportClient, NewId("redis-trace"));

        (await Fixture.RedisTransportClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=true",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.RedisTransportClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaJsonBody_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.RedisTransportClient, NewId("redis-trace"));

        (await Fixture.RedisTransportClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=false",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.RedisTransportClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    private sealed record WorkerResponse(string CorrelationId);
    private sealed record ReplyTargetResult(string Transport, string Address);
    private sealed record ConfigResponse(string Channel, string Transport, RedisConfig? Redis);
    private sealed record RedisConfig(
        string WorkerStream,
        string ResponseStream,
        string WorkerAckMode,
        int WorkerBackgroundWorkerCount,
        int WorkerBackgroundQueueCapacity,
        string ResponseAckMode);
}
