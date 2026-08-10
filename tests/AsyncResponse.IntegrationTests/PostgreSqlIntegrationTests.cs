using AsyncResponse.Sample;
using System.Net.Http.Json;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The real PostgreSQL stack end to end: PostgreSQL LISTEN/NOTIFY channel + durable recovery tables,
/// and PostgreSQL queue-table transport for worker dispatch and response ingress.
/// </summary>
[Collection(DataCollection.Name)]
public sealed class PostgreSqlIntegrationTests(DataBatchFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Config_ReportsPostgreSqlChannelTransportAndEarlyAckMode()
    {
        var defaultConfig = (await Fixture.PostgreSqlClient.GetFromJsonAsync<ConfigResponse>("/config"))!;
        var earlyAckConfig = (await Fixture.PostgreSqlEarlyAckClient.GetFromJsonAsync<ConfigResponse>("/config"))!;

        Assert.Equal("PostgreSQL", defaultConfig.Channel);
        Assert.Equal("PostgreSQL", defaultConfig.Transport);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Postgres!.WorkerAckMode);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Postgres.ResponseAckMode);
        Assert.Equal("worker", defaultConfig.Postgres.WorkerQueue);
        Assert.Equal("response", defaultConfig.Postgres.ResponseQueue);

        Assert.Equal("PostgreSQL", earlyAckConfig.Channel);
        Assert.Equal("PostgreSQL", earlyAckConfig.Transport);
        Assert.Equal("AckAfterEnqueue", earlyAckConfig.Postgres!.WorkerAckMode);
        Assert.Equal(4, earlyAckConfig.Postgres.WorkerBackgroundWorkerCount);
        Assert.Equal(256, earlyAckConfig.Postgres.WorkerBackgroundQueueCapacity);
        Assert.Equal("AckAfterHandlerCompletes", earlyAckConfig.Postgres.ResponseAckMode);
    }

    [Fact]
    public async Task RequestResponse_Succeeds_OverPostgreSqlChannel()
    {
        var response = await Fixture.PostgreSqlClient.PostAsync("/request-response?behavior=Succeed", content: null);
        response.EnsureSuccessStatusCode();

        var result = (await response.Content.ReadFromJsonAsync<RequestResponseResult>())!;
        Assert.Equal(OperationStatus.Completed, result.Status);
    }

    [Fact]
    public async Task RequestResponse_DomainFailure_IsReturnedAsData()
    {
        var response = await Fixture.PostgreSqlClient.PostAsync("/request-response?behavior=FailDomain", content: null);
        response.EnsureSuccessStatusCode();

        var result = (await response.Content.ReadFromJsonAsync<RequestResponseResult>())!;
        Assert.Equal(OperationStatus.Failed, result.Status);
    }

    [Fact]
    public async Task ConcurrentRequestResponse_SucceedsWithoutFalseRecovery()
    {
        var responses = await Task.WhenAll(Enumerable.Range(0, 12)
            .Select(_ => Fixture.PostgreSqlClient.PostAsync("/request-response?behavior=Succeed", content: null)));

        foreach (var response in responses)
        {
            response.EnsureSuccessStatusCode();
            var result = (await response.Content.ReadFromJsonAsync<RequestResponseResult>())!;
            Assert.Equal(OperationStatus.Completed, result.Status);
        }
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughPostgreSql_WithRestoredCorrelationAndTrace()
    {
        var token = NewId("postgres-token");
        var trace = NewId("postgres-trace");

        var response = await Fixture.PostgreSqlClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.PostgreSqlClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughPostgreSql_WithAckAfterEnqueueWorkerSubscriber()
    {
        var token = NewId("postgres-early-token");
        var trace = NewId("postgres-early-trace");

        var response = await Fixture.PostgreSqlEarlyAckClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.PostgreSqlEarlyAckClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task SharedCorrelationException_FaultsBothPostgreSqlWaiters()
    {
        var response = await Fixture.PostgreSqlClient.PostAsync("/shared-correlation-exception?message=postgres%20fanout%20boom", content: null);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<SharedExceptionResult>();
        Assert.Equal(2, result!.Failures.Count);
        Assert.All(result.Failures, failure =>
            Assert.Contains("postgres fanout boom", failure, StringComparison.Ordinal));
    }

    [Fact]
    public async Task WithReplyTarget_ExposesThePostgreSqlResponseQueue()
    {
        var response = await Fixture.PostgreSqlClient.GetAsync("/reply-target");
        response.EnsureSuccessStatusCode();

        var target = await response.Content.ReadFromJsonAsync<ReplyTargetResult>();
        Assert.Equal("PostgreSQL", target!.Transport);
        Assert.Equal("response", target.Address);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaHeader_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.PostgreSqlClient, NewId("postgres-trace"));

        (await Fixture.PostgreSqlClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=true",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.PostgreSqlClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaJsonBody_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.PostgreSqlClient, NewId("postgres-trace"));

        (await Fixture.PostgreSqlClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=false",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.PostgreSqlClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task CompletedResponse_AfterCrash_InvokesResumeCallback_WithRestoredTrace()
    {
        var trace = NewId("postgres-late-trace");
        var correlationId = await ArmAsync(Fixture.PostgreSqlClient, trace);
        (await Fixture.PostgreSqlClient.PostAsync("/crash", content: null)).EnsureSuccessStatusCode();

        (await Fixture.PostgreSqlClient.PostAsync($"/publish?correlationId={correlationId}&status=Completed&message=late", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.PostgreSqlClient, $"resume:{correlationId}");
        Assert.Equal("resume", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task LostSubscriberFlow_ComposesArmCrashPublishAndResumeCallback()
    {
        var trace = NewId("postgres-composed-trace");

        var response = await Fixture.PostgreSqlClient.PostAsync($"/lost-subscriber-flow?outcome=Completed&trace={trace}", content: null);
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
        var correlationId = await ArmAsync(Fixture.PostgreSqlClient, NewId("postgres-late-trace"));
        (await Fixture.PostgreSqlClient.PostAsync("/crash", content: null)).EnsureSuccessStatusCode();

        (await Fixture.PostgreSqlClient.PostAsync($"/publish?correlationId={correlationId}&status=Failed&message=remote%20failed", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.PostgreSqlClient, $"fail:{correlationId}");
        Assert.Equal("fail", call.Kind);
        Assert.Equal("domain-failure", call.Detail);
    }

    [Fact]
    public async Task SetException_AfterCrash_InvokesFailureCallback()
    {
        var correlationId = await ArmAsync(Fixture.PostgreSqlClient, NewId("postgres-late-trace"));
        (await Fixture.PostgreSqlClient.PostAsync("/crash", content: null)).EnsureSuccessStatusCode();

        (await Fixture.PostgreSqlClient.PostAsync($"/publish?correlationId={correlationId}&exception=technical", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.PostgreSqlClient, $"fail:{correlationId}");
        Assert.Equal("fail", call.Kind);
        Assert.Equal(nameof(InvalidOperationException), call.Detail);
    }

    [Fact]
    public async Task Watchdog_ScansPostgreSqlRecoveryStateAndReturnsHealthyAfterCleanup()
    {
        var correlationId = NewId("postgres-stale");

        (await Fixture.PostgreSqlClient.PostAsync($"/seed-recovery?correlationId={correlationId}&ageMinutes=5", content: null))
            .EnsureSuccessStatusCode();

        var degraded = await PollAsync(
            GetPostgreSqlHealthStatusAsync,
            status => status == "Degraded",
            TimeSpan.FromSeconds(20));
        Assert.Equal("Degraded", degraded);

        (await Fixture.PostgreSqlClient.DeleteAsync($"/test/recovery/{correlationId}")).EnsureSuccessStatusCode();

        var healthy = await PollAsync(
            GetPostgreSqlHealthStatusAsync,
            status => status == "Healthy",
            TimeSpan.FromSeconds(20));
        Assert.Equal("Healthy", healthy);
    }

    private async Task<string?> GetPostgreSqlHealthStatusAsync()
    {
        using var document = JsonDocument.Parse(await Fixture.PostgreSqlClient.GetStringAsync("/healthz"));
        return document.RootElement.GetProperty("status").GetString();
    }

    private sealed record WorkerResponse(string CorrelationId);
    private sealed record ReplyTargetResult(string Transport, string Address);
    private sealed record SharedExceptionResult(string CorrelationId, IReadOnlyList<string> Failures);
    private sealed record LostSubscriberFlowResult(string CorrelationId, string Outcome, FlowCall Callback);
    private sealed record ConfigResponse(string Channel, string Transport, PostgreSqlConfig? Postgres);
    private sealed record PostgreSqlConfig(
        string? SchemaName,
        string? RecoveryStateTable,
        string? ChannelMessageTable,
        string? TransportMessageTable,
        string? WorkerQueue,
        string? ResponseQueue,
        string WorkerAckMode,
        int WorkerBackgroundWorkerCount,
        int WorkerBackgroundQueueCapacity,
        string ResponseAckMode);
}
