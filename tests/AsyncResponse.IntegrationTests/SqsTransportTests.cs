using AsyncResponse.Sample;
using System.Net.Http.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The real AWS SQS transport over LocalStack: worker jobs published and long-polled from SQS
/// queues (provisioned with redrive-policy dead-letter queues by the transport's CreateQueues
/// option), and responses ingested from an SQS response queue into active or recoverable waiters.
/// </summary>
[Collection(IntegrationCollection.Name)]
public sealed class SqsTransportTests(IntegrationFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Config_ReportsDefaultAndEarlyAckSqsModes()
    {
        var defaultConfig = (await Fixture.SqsClient.GetFromJsonAsync<ConfigResponse>("/config"))!;
        var earlyAckConfig = (await Fixture.SqsEarlyAckClient.GetFromJsonAsync<ConfigResponse>("/config"))!;

        Assert.Equal("Redis", defaultConfig.Channel);
        Assert.Equal("SQS", defaultConfig.Transport);
        Assert.True(defaultConfig.Sqs!.CreateQueues);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Sqs.WorkerAckMode);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Sqs.ResponseAckMode);

        Assert.Equal("Redis", earlyAckConfig.Channel);
        Assert.Equal("SQS", earlyAckConfig.Transport);
        Assert.Equal("AckAfterEnqueue", earlyAckConfig.Sqs!.WorkerAckMode);
        Assert.Equal(4, earlyAckConfig.Sqs.WorkerBackgroundWorkerCount);
        Assert.Equal(256, earlyAckConfig.Sqs.WorkerBackgroundQueueCapacity);
        Assert.Equal("AckAfterHandlerCompletes", earlyAckConfig.Sqs.ResponseAckMode);
    }

    [Fact]
    public async Task RequestResponse_Success_RoundTripsThroughSqs()
    {
        var response = await Fixture.SqsClient.PostAsync("/request-response?behavior=Succeed", content: null);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<RequestResponseResult>();
        Assert.Equal(OperationStatus.Completed, result!.Status);
    }

    [Fact]
    public async Task RequestResponse_DomainFailure_IsReturnedAsData()
    {
        var response = await Fixture.SqsClient.PostAsync("/request-response?behavior=FailDomain", content: null);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<RequestResponseResult>();
        Assert.Equal(OperationStatus.Failed, result!.Status);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughSqs_WithRestoredCorrelationAndTrace()
    {
        var token = NewId("sqs-token");
        var trace = NewId("sqs-trace");

        var response = await Fixture.SqsClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.SqsClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughSqs_WithAckAfterEnqueueWorkerSubscriber()
    {
        var token = NewId("sqs-early-token");
        var trace = NewId("sqs-early-trace");

        var response = await Fixture.SqsEarlyAckClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.SqsEarlyAckClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WithReplyTarget_ExposesTheSqsResponseQueue()
    {
        var response = await Fixture.SqsClient.GetAsync("/reply-target");
        response.EnsureSuccessStatusCode();

        var target = await response.Content.ReadFromJsonAsync<ReplyTargetResult>();
        Assert.Equal("AmazonSQS", target!.Transport);
        Assert.Equal("asyncresponse-itest-sqs-response", target.Address);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaAttribute_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.SqsClient, NewId("sqs-trace"));

        (await Fixture.SqsClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=true",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.SqsClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaJsonBody_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.SqsClient, NewId("sqs-trace"));

        (await Fixture.SqsClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=false",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.SqsClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task LateRecovery_CompletedResponse_ResumesCallback()
    {
        var response = await Fixture.SqsClient.PostAsync(
            $"/lost-subscriber-flow?outcome=Completed&trace={NewId("sqs-late-trace")}",
            content: null);

        response.EnsureSuccessStatusCode();
    }

    private sealed record WorkerResponse(string CorrelationId);
    private sealed record ReplyTargetResult(string Transport, string Address);
    private sealed record ConfigResponse(string Channel, string Transport, SqsConfig? Sqs);
    private sealed record SqsConfig(
        bool CreateQueues,
        string WorkerAckMode,
        int WorkerBackgroundWorkerCount,
        int WorkerBackgroundQueueCapacity,
        string ResponseAckMode);
}
