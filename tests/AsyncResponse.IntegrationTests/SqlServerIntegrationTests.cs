using AsyncResponse.Sample;
using System.Net.Http.Json;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The real SQL Server stack end to end: SQL Server adaptive-polling channel + durable recovery
/// tables, and SQL Server queue-table transport for worker dispatch and response ingress.
/// </summary>
[Collection(IntegrationCollection.Name)]
public sealed class SqlServerIntegrationTests(IntegrationFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task Config_ReportsSqlServerChannelTransportAndEarlyAckMode()
    {
        var defaultConfig = (await Fixture.SqlServerClient.GetFromJsonAsync<ConfigResponse>("/config"))!;
        var earlyAckConfig = (await Fixture.SqlServerEarlyAckClient.GetFromJsonAsync<ConfigResponse>("/config"))!;

        Assert.Equal("SqlServer", defaultConfig.Channel);
        Assert.Equal("SqlServer", defaultConfig.Transport);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Sqlserver!.WorkerAckMode);
        Assert.Equal("AckAfterHandlerCompletes", defaultConfig.Sqlserver.ResponseAckMode);
        Assert.Equal("worker", defaultConfig.Sqlserver.WorkerQueue);
        Assert.Equal("response", defaultConfig.Sqlserver.ResponseQueue);
        Assert.Equal("itest", defaultConfig.Sqlserver.SchemaName);

        Assert.Equal("SqlServer", earlyAckConfig.Channel);
        Assert.Equal("SqlServer", earlyAckConfig.Transport);
        Assert.Equal("AckAfterEnqueue", earlyAckConfig.Sqlserver!.WorkerAckMode);
        Assert.Equal(4, earlyAckConfig.Sqlserver.WorkerBackgroundWorkerCount);
        Assert.Equal(256, earlyAckConfig.Sqlserver.WorkerBackgroundQueueCapacity);
        Assert.Equal("AckAfterHandlerCompletes", earlyAckConfig.Sqlserver.ResponseAckMode);
    }

    [Fact]
    public async Task RequestResponse_Succeeds_OverSqlServerChannel()
    {
        var response = await Fixture.SqlServerClient.PostAsync("/request-response?behavior=Succeed", content: null);
        response.EnsureSuccessStatusCode();

        var result = (await response.Content.ReadFromJsonAsync<RequestResponseResult>())!;
        Assert.Equal(OperationStatus.Completed, result.Status);
    }

    [Fact]
    public async Task RequestResponse_DomainFailure_IsReturnedAsData()
    {
        var response = await Fixture.SqlServerClient.PostAsync("/request-response?behavior=FailDomain", content: null);
        response.EnsureSuccessStatusCode();

        var result = (await response.Content.ReadFromJsonAsync<RequestResponseResult>())!;
        Assert.Equal(OperationStatus.Failed, result.Status);
    }

    [Fact]
    public async Task ConcurrentRequestResponse_SucceedsWithoutFalseRecovery()
    {
        var responses = await Task.WhenAll(Enumerable.Range(0, 12)
            .Select(_ => Fixture.SqlServerClient.PostAsync("/request-response?behavior=Succeed", content: null)));

        foreach (var response in responses)
        {
            response.EnsureSuccessStatusCode();
            var result = (await response.Content.ReadFromJsonAsync<RequestResponseResult>())!;
            Assert.Equal(OperationStatus.Completed, result.Status);
        }
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughSqlServer_WithRestoredCorrelationAndTrace()
    {
        var token = NewId("sqlserver-token");
        var trace = NewId("sqlserver-trace");

        var response = await Fixture.SqlServerClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.SqlServerClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task WorkerJob_RoundTripsThroughSqlServer_WithAckAfterEnqueueWorkerSubscriber()
    {
        var token = NewId("sqlserver-early-token");
        var trace = NewId("sqlserver-early-trace");

        var response = await Fixture.SqlServerEarlyAckClient.PostAsync($"/worker?token={token}&trace={trace}", content: null);
        response.EnsureSuccessStatusCode();
        var correlationId = (await response.Content.ReadFromJsonAsync<WorkerResponse>())!.CorrelationId;

        var call = await WaitForCallAsync(Fixture.SqlServerEarlyAckClient, $"worker:{token}");
        Assert.Equal("worker", call.Kind);
        Assert.Equal(correlationId, call.CorrelationId);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task SharedCorrelationException_FaultsBothSqlServerWaiters()
    {
        var response = await Fixture.SqlServerClient.PostAsync("/shared-correlation-exception?message=sqlserver%20fanout%20boom", content: null);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<SharedExceptionResult>();
        Assert.Equal(2, result!.Failures.Count);
        Assert.All(result.Failures, failure =>
            Assert.Contains("sqlserver fanout boom", failure, StringComparison.Ordinal));
    }

    [Fact]
    public async Task WithReplyTarget_ExposesTheSqlServerResponseQueue()
    {
        var response = await Fixture.SqlServerClient.GetAsync("/reply-target");
        response.EnsureSuccessStatusCode();

        var target = await response.Content.ReadFromJsonAsync<ReplyTargetResult>();
        Assert.Equal("SqlServer", target!.Transport);
        Assert.Equal("response", target.Address);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaHeader_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.SqlServerClient, NewId("sqlserver-trace"));

        (await Fixture.SqlServerClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=true",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.SqlServerClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task ResponseIngress_CorrelationViaJsonBody_CompletesActiveWaiter()
    {
        var correlationId = await ArmAsync(Fixture.SqlServerClient, NewId("sqlserver-trace"));

        (await Fixture.SqlServerClient.PostAsync(
            $"/emit-response?correlationId={correlationId}&status=Completed&useAttribute=false",
            content: null)).EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.SqlServerClient, $"waiter:{correlationId}");
        Assert.Equal("waiter", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
    }

    [Fact]
    public async Task CompletedResponse_AfterCrash_InvokesResumeCallback_WithRestoredTrace()
    {
        var trace = NewId("sqlserver-late-trace");
        var correlationId = await ArmAsync(Fixture.SqlServerClient, trace);
        (await Fixture.SqlServerClient.PostAsync("/crash", content: null)).EnsureSuccessStatusCode();

        (await Fixture.SqlServerClient.PostAsync($"/publish?correlationId={correlationId}&status=Completed&message=late", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.SqlServerClient, $"resume:{correlationId}");
        Assert.Equal("resume", call.Kind);
        Assert.Equal(OperationStatus.Completed, call.Status);
        Assert.Equal(trace, call.Trace);
    }

    [Fact]
    public async Task LostSubscriberFlow_ComposesArmCrashPublishAndResumeCallback()
    {
        var trace = NewId("sqlserver-composed-trace");

        var response = await Fixture.SqlServerClient.PostAsync($"/lost-subscriber-flow?outcome=Completed&trace={trace}", content: null);
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
        var correlationId = await ArmAsync(Fixture.SqlServerClient, NewId("sqlserver-late-trace"));
        (await Fixture.SqlServerClient.PostAsync("/crash", content: null)).EnsureSuccessStatusCode();

        (await Fixture.SqlServerClient.PostAsync($"/publish?correlationId={correlationId}&status=Failed&message=remote%20failed", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.SqlServerClient, $"fail:{correlationId}");
        Assert.Equal("fail", call.Kind);
        Assert.Equal("domain-failure", call.Detail);
    }

    [Fact]
    public async Task SetException_AfterCrash_InvokesFailureCallback()
    {
        var correlationId = await ArmAsync(Fixture.SqlServerClient, NewId("sqlserver-late-trace"));
        (await Fixture.SqlServerClient.PostAsync("/crash", content: null)).EnsureSuccessStatusCode();

        (await Fixture.SqlServerClient.PostAsync($"/publish?correlationId={correlationId}&exception=technical", content: null))
            .EnsureSuccessStatusCode();

        var call = await WaitForCallAsync(Fixture.SqlServerClient, $"fail:{correlationId}");
        Assert.Equal("fail", call.Kind);
        Assert.Equal(nameof(InvalidOperationException), call.Detail);
    }

    [Fact]
    public async Task Watchdog_ScansSqlServerRecoveryStateAndReturnsHealthyAfterCleanup()
    {
        var correlationId = NewId("sqlserver-stale");

        (await Fixture.SqlServerClient.PostAsync($"/seed-recovery?correlationId={correlationId}&ageMinutes=5", content: null))
            .EnsureSuccessStatusCode();

        var degraded = await PollAsync(
            GetSqlServerHealthStatusAsync,
            status => status == "Degraded",
            TimeSpan.FromSeconds(20));
        Assert.Equal("Degraded", degraded);

        (await Fixture.SqlServerClient.DeleteAsync($"/test/recovery/{correlationId}")).EnsureSuccessStatusCode();

        var healthy = await PollAsync(
            GetSqlServerHealthStatusAsync,
            status => status == "Healthy",
            TimeSpan.FromSeconds(20));
        Assert.Equal("Healthy", healthy);
    }

    private async Task<string?> GetSqlServerHealthStatusAsync()
    {
        using var document = JsonDocument.Parse(await Fixture.SqlServerClient.GetStringAsync("/healthz"));
        return document.RootElement.GetProperty("status").GetString();
    }

    private sealed record WorkerResponse(string CorrelationId);
    private sealed record ReplyTargetResult(string Transport, string Address);
    private sealed record SharedExceptionResult(string CorrelationId, IReadOnlyList<string> Failures);
    private sealed record LostSubscriberFlowResult(string CorrelationId, string Outcome, FlowCall Callback);
    private sealed record ConfigResponse(string Channel, string Transport, SqlServerConfig? Sqlserver);
    private sealed record SqlServerConfig(
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
