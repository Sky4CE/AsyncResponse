using AsyncResponse.Sample;
using System.Net.Http.Json;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The real MongoDB stack end to end: change-stream channel + durable TTL-indexed recovery
/// collections, and the findOneAndUpdate queue-collection transport for worker dispatch and
/// response ingress — all against a single-node replica set.
/// </summary>
[Collection(DataCollection.Name)]
public sealed class MongoDbIntegrationTests(DataBatchFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Config_ReportsMongoDbChannelTransportAndEarlyAckMode()
    {
        var defaultConfig = (await Fixture.MongoDbClient.GetFromJsonAsync<ConfigResponse>("/config"))!;
        var earlyAckConfig = (await Fixture.MongoDbEarlyAckClient.GetFromJsonAsync<ConfigResponse>("/config"))!;

        Assert.Equal("MongoDB", defaultConfig.Channel);
        Assert.Equal("MongoDB", defaultConfig.Transport);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Mongodb!.WorkerAckMode);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Mongodb.ResponseAckMode);
        Assert.Equal("worker", defaultConfig.Mongodb.WorkerQueue);
        Assert.Equal("response", defaultConfig.Mongodb.ResponseQueue);

        Assert.Equal("MongoDB", earlyAckConfig.Channel);
        Assert.Equal("MongoDB", earlyAckConfig.Transport);
        Assert.Equal("AckAfterEnqueue", earlyAckConfig.Mongodb!.WorkerAckMode);
        Assert.Equal(4, earlyAckConfig.Mongodb.WorkerBackgroundWorkerCount);
        Assert.Equal(256, earlyAckConfig.Mongodb.WorkerBackgroundQueueCapacity);
        Assert.Equal("AckAfterHandlerCompletes", earlyAckConfig.Mongodb.ResponseAckMode);
    }

    [Fact]
    public async Task RequestResponse_Succeeds_OverMongoDbChannel()
    {
        var response = await Fixture.MongoDbClient.PostAsync("/request-response?behavior=Succeed", content: null);
        response.EnsureSuccessStatusCode();

        var result = (await response.Content.ReadFromJsonAsync<RequestResponseResult>())!;
        Assert.Equal(OperationStatus.Completed, result.Status);
    }

    [Fact]
    public async Task RequestResponse_DomainFailure_IsReturnedAsData()
    {
        var response = await Fixture.MongoDbClient.PostAsync("/request-response?behavior=FailDomain", content: null);
        response.EnsureSuccessStatusCode();

        var result = (await response.Content.ReadFromJsonAsync<RequestResponseResult>())!;
        Assert.Equal(OperationStatus.Failed, result.Status);
    }

    [Fact]
    public async Task ConcurrentRequestResponse_SucceedsWithoutFalseRecovery()
    {
        var responses = await Task.WhenAll(Enumerable.Range(0, 12)
            .Select(_ => Fixture.MongoDbClient.PostAsync("/request-response?behavior=Succeed", content: null)));

        foreach (var response in responses)
        {
            response.EnsureSuccessStatusCode();
            var result = (await response.Content.ReadFromJsonAsync<RequestResponseResult>())!;
            Assert.Equal(OperationStatus.Completed, result.Status);
        }
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughMongoDb_WithRestoredCorrelationAndTrace()
    {
        var token = NewId("mongodb-token");
        var trace = NewId("mongodb-trace");

        var response = await Fixture.MongoDbClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.MongoDbClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughMongoDb_WithAckAfterEnqueueWorkerSubscriber()
    {
        var token = NewId("mongodb-early-token");
        var trace = NewId("mongodb-early-trace");

        var response = await Fixture.MongoDbEarlyAckClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.MongoDbEarlyAckClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task SharedCorrelationException_FaultsBothMongoDbWaiters()
    {
        var response = await Fixture.MongoDbClient.PostAsync("/shared-correlation-exception?message=mongodb%20fanout%20boom", content: null);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<SharedExceptionResult>();
        Assert.Equal(2, result!.Failures.Count);
        Assert.All(result.Failures, failure =>
            Assert.Contains("mongodb fanout boom", failure, StringComparison.Ordinal));
    }

    [Fact]
    public async Task WithReplyTarget_ExposesTheMongoDbResponseQueue()
    {
        var response = await Fixture.MongoDbClient.GetAsync("/reply-target");
        response.EnsureSuccessStatusCode();

        var target = await response.Content.ReadFromJsonAsync<ReplyTargetResult>();
        Assert.Equal("MongoDB", target!.Transport);
        Assert.Equal("response", target.Address);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaHeader_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.MongoDbClient, NewId("mongodb-trace"));

        (await Fixture.MongoDbClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=true",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.MongoDbClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaJsonBody_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.MongoDbClient, NewId("mongodb-trace"));

        (await Fixture.MongoDbClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=false",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.MongoDbClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task CompletedResponse_AfterCrash_InvokesResumeCallback_WithRestoredTrace()
    {
        var trace = NewId("mongodb-late-trace");
        var correlationId = await ArmAsync(Fixture.MongoDbClient, trace);
        (await Fixture.MongoDbClient.PostAsync("/crash", content: null)).EnsureSuccessStatusCode();

        (await Fixture.MongoDbClient.PostAsync($"/publish?correlationId={correlationId}&status=Completed&message=late", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.MongoDbClient, $"resume:{correlationId}");
        Assert.Equal("resume", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task LostSubscriberFlow_ComposesArmCrashPublishAndResumeCallback()
    {
        var trace = NewId("mongodb-composed-trace");

        var response = await Fixture.MongoDbClient.PostAsync($"/lost-subscriber-flow?outcome=Completed&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<LostSubscriberFlowResult>();
        Assert.Equal("Completed", result!.Outcome);
        Assert.Equal("resume", result.Callback.Kind);
        Assert.Equal(OperationStatus.Completed, result.Callback.Status);
        Assert.Equal(trace, result.Callback.Trace);
        Assert.Equal("tenant-acme", result.Callback.Tenant);
    }

    [Fact]
    public async Task FailedDomainResponse_AfterCrash_InvokesFailureCallback()
    {
        var correlationId = await ArmAsync(Fixture.MongoDbClient, NewId("mongodb-late-trace"));
        (await Fixture.MongoDbClient.PostAsync("/crash", content: null)).EnsureSuccessStatusCode();

        (await Fixture.MongoDbClient.PostAsync($"/publish?correlationId={correlationId}&status=Failed&message=remote%20failed", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.MongoDbClient, $"fail:{correlationId}");
        Assert.Equal("fail", call.Kind);
        Assert.Equal("domain-failure", call.Detail);
    }

    [Fact]
    public async Task SetException_AfterCrash_InvokesFailureCallback()
    {
        var correlationId = await ArmAsync(Fixture.MongoDbClient, NewId("mongodb-late-trace"));
        (await Fixture.MongoDbClient.PostAsync("/crash", content: null)).EnsureSuccessStatusCode();

        (await Fixture.MongoDbClient.PostAsync($"/publish?correlationId={correlationId}&exception=technical", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.MongoDbClient, $"fail:{correlationId}");
        Assert.Equal("fail", call.Kind);
        Assert.Equal(nameof(InvalidOperationException), call.Detail);
    }

    [Fact]
    public async Task Watchdog_ScansMongoDbRecoveryStateAndReturnsHealthyAfterCleanup()
    {
        var correlationId = NewId("mongodb-stale");

        (await Fixture.MongoDbClient.PostAsync($"/seed-recovery?correlationId={correlationId}&ageMinutes=5", content: null))
            .EnsureSuccessStatusCode();

        var degraded = await PollAsync(
            GetMongoDbHealthStatusAsync,
            status => status == "Degraded",
            TimeSpan.FromSeconds(20));
        Assert.Equal("Degraded", degraded);

        (await Fixture.MongoDbClient.DeleteAsync($"/test/recovery/{correlationId}")).EnsureSuccessStatusCode();

        var healthy = await PollAsync(
            GetMongoDbHealthStatusAsync,
            status => status == "Healthy",
            TimeSpan.FromSeconds(20));
        Assert.Equal("Healthy", healthy);
    }

    private async Task<string?> GetMongoDbHealthStatusAsync()
    {
        using var document = JsonDocument.Parse(await Fixture.MongoDbClient.GetStringAsync("/healthz"));
        return document.RootElement.GetProperty("status").GetString();
    }

    private sealed record WorkerResponse(string CorrelationId);
    private sealed record ReplyTargetResult(string Transport, string Address);
    private sealed record SharedExceptionResult(string CorrelationId, IReadOnlyList<string> Failures);
    private sealed record LostSubscriberFlowResult(string CorrelationId, string Outcome, FlowCall Callback);
    private sealed record ConfigResponse(string Channel, string Transport, MongoDbConfig? Mongodb);
    private sealed record MongoDbConfig(
        string? RecoveryStateCollection,
        string? ChannelMessageCollection,
        string? SubscriberCollection,
        string? TransportMessageCollection,
        string? WorkerQueue,
        string? ResponseQueue,
        string WorkerAckMode,
        int WorkerBackgroundWorkerCount,
        int WorkerBackgroundQueueCapacity,
        string ResponseAckMode);
}
