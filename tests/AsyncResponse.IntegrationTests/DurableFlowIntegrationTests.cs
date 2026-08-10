using AsyncResponse.Sample;
using System.Net;
using System.Net.Http.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Durable flows end-to-end over the full Aspire stack, against every durable channel: the
/// default app (Redis channel), NATS, PostgreSQL, and SQL Server — plus the Redis channel paired
/// with the SQS transport, so flow execution and resume kicks ride an SQS worker queue. Each run
/// exercises start → worker-transport execution → checkpointed local steps → awaited remote steps
/// (progress + terminal via the simulator) → state observation over HTTP — i.e. the flow state
/// round-trips through the real recovery store of each channel.
/// </summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class DurableFlowIntegrationTests(DataBatchFixture fixture) : IntegrationTestBase(fixture)
{
    public static TheoryData<string> DurableChannelVariants => new()
    {
        "redis",      // the default app: Redis channel
        "nats",       // NATS channel + JetStream transport
        "postgresql", // PostgreSQL channel + transport
        "sqlserver",  // SQL Server channel + transport
        "sqs",        // Redis channel + AWS SQS transport (LocalStack)
        "mongodb"     // MongoDB channel + transport, flow ledgers in the DurableFlows.MongoDB store
    };

    private HttpClient ClientFor(string variant) => variant switch
    {
        "redis" => Client,
        "nats" => Fixture.NatsClient,
        "postgresql" => Fixture.PostgreSqlClient,
        "sqlserver" => Fixture.SqlServerClient,
        "sqs" => Fixture.SqsClient,
        "mongodb" => Fixture.MongoDbClient,
        _ => throw new ArgumentOutOfRangeException(nameof(variant), variant, null)
    };

    [Theory]
    [MemberData(nameof(DurableChannelVariants))]
    public async Task DurableFlow_OverEachChannel_CompletesAllSteps(string variant)
    {
        var client = ClientFor(variant);

        var start = await client.PostAsync("/durable-flow?name=itest", content: null);
        start.EnsureSuccessStatusCode();
        var flowId = (await start.Content.ReadFromJsonAsync<StartFlowResult>())!.FlowId;

        // The final step records to the FlowRecorder — long-poll it like every other scenario.
        var done = await WaitForCallAsync(client, $"flow-done:{flowId}");
        Assert.Equal("flow-done", done.Kind);

        // Then the persisted state must read back Succeeded with every step checkpointed.
        var state = await PollAsync(
            () => client.GetFromJsonAsync<FlowStateDto>($"/durable-flow/{flowId}"),
            s => s!.Status == (int)FlowRunStatus.Succeeded,
            DefaultTimeout);

        Assert.Equal((int)FlowRunStatus.Succeeded, state!.Status);
        foreach (var step in new[] { "create-workspace", "run-migration", "import-data", "audit", "notify" })
        {
            Assert.True(state.Steps!.TryGetValue(step, out var checkpoint), $"step '{step}' missing from state");
            Assert.True(checkpoint!.Completed, $"step '{step}' not completed");
            Assert.Null(checkpoint.PendingCorrelationId);
        }

        Assert.Contains("workspace", state.Values!.Keys);
        Assert.True(state.Attempts >= 1);
    }

    [Fact]
    public async Task DurableFlow_DomainFailureAtImport_MarksRunTerminallyFailed()
    {
        var start = await Client.PostAsync("/durable-flow?name=itest&failAtImport=true", content: null);
        start.EnsureSuccessStatusCode();
        var flowId = (await start.Content.ReadFromJsonAsync<StartFlowResult>())!.FlowId;

        var state = await PollAsync(
            () => Client.GetFromJsonAsync<FlowStateDto>($"/durable-flow/{flowId}"),
            s => s!.Status == (int)FlowRunStatus.Failed,
            DefaultTimeout);

        Assert.Equal((int)FlowRunStatus.Failed, state!.Status);
        Assert.Contains("import failed", state.LastMessage);

        // Terminal runs stay terminal: a resume kick must not restart the flow.
        var resume = await Client.PostAsync($"/durable-flow/{flowId}/resume", content: null);
        resume.EnsureSuccessStatusCode();
        await Task.Delay(500);
        var after = await Client.GetFromJsonAsync<FlowStateDto>($"/durable-flow/{flowId}");
        Assert.Equal((int)FlowRunStatus.Failed, after!.Status);
    }

    [Fact]
    public async Task DurableFlow_StartWithCustomFlowId_IsIdempotent()
    {
        var flowId = NewId("itest-flow");

        var first = await Client.PostAsync($"/durable-flow?name=itest&flowId={flowId}", content: null);
        first.EnsureSuccessStatusCode();
        Assert.Equal(flowId, (await first.Content.ReadFromJsonAsync<StartFlowResult>())!.FlowId);

        var done = await WaitForCallAsync(Client, $"flow-done:{flowId}");
        Assert.Equal("flow-done", done.Kind);

        // An identical retry re-enqueues the existing (already succeeded) run: the state is neither
        // reset nor duplicated, and no step re-executes.
        var second = await Client.PostAsync($"/durable-flow?name=itest&flowId={flowId}", content: null);
        second.EnsureSuccessStatusCode();
        Assert.Equal(flowId, (await second.Content.ReadFromJsonAsync<StartFlowResult>())!.FlowId);

        // The id is permanently bound to its original flow contract. A different input is a
        // conflicting command, not an idempotent retry, and must never silently reuse old work.
        var conflicting = await Client.PostAsync($"/durable-flow?name=other&flowId={flowId}", content: null);
        Assert.Equal(System.Net.HttpStatusCode.InternalServerError, conflicting.StatusCode);

        await Task.Delay(500);
        var state = await Client.GetFromJsonAsync<FlowStateDto>($"/durable-flow/{flowId}");
        Assert.Equal((int)FlowRunStatus.Succeeded, state!.Status);
        Assert.Contains("itest", state.InputJson);
    }

    [Fact]
    public async Task DurableFlow_OverSqs_ResumeKickAfterCompletionKeepsCheckpointsTerminal()
    {
        // The flow executes over the SQS worker queue (Redis channel holds the checkpoints). A
        // resume kick is the exact path a crash-recovery scan takes: it re-enqueues the flow
        // through the SQS transport, and the re-execution must replay from checkpoints — the
        // succeeded run stays succeeded, no step loses its checkpoint, nothing re-executes into a
        // pending state.
        var flowId = NewId("itest-sqs-flow");
        var start = await Fixture.SqsClient.PostAsync($"/durable-flow?name=itest-sqs&flowId={flowId}", content: null);
        start.EnsureSuccessStatusCode();

        var done = await WaitForCallAsync(Fixture.SqsClient, $"flow-done:{flowId}");
        Assert.Equal("flow-done", done.Kind);

        var resume = await Fixture.SqsClient.PostAsync($"/durable-flow/{flowId}/resume", content: null);
        resume.EnsureSuccessStatusCode();
        await Task.Delay(500);

        var state = await PollAsync(
            () => Fixture.SqsClient.GetFromJsonAsync<FlowStateDto>($"/durable-flow/{flowId}"),
            s => s!.Status == (int)FlowRunStatus.Succeeded,
            DefaultTimeout);
        Assert.Equal((int)FlowRunStatus.Succeeded, state!.Status);
        foreach (var step in new[] { "create-workspace", "run-migration", "import-data", "audit", "notify" })
        {
            Assert.True(state.Steps!.TryGetValue(step, out var checkpoint), $"step '{step}' missing from state");
            Assert.True(checkpoint!.Completed, $"step '{step}' lost its checkpoint after the resume kick");
            Assert.Null(checkpoint.PendingCorrelationId);
        }
    }

    [Fact]
    public async Task DurableFlow_OverSqs_DomainFailure_MarksRunTerminallyFailed()
    {
        var start = await Fixture.SqsClient.PostAsync("/durable-flow?name=itest-sqs&failAtImport=true", content: null);
        start.EnsureSuccessStatusCode();
        var flowId = (await start.Content.ReadFromJsonAsync<StartFlowResult>())!.FlowId;

        var state = await PollAsync(
            () => Fixture.SqsClient.GetFromJsonAsync<FlowStateDto>($"/durable-flow/{flowId}"),
            s => s!.Status == (int)FlowRunStatus.Failed,
            DefaultTimeout);

        Assert.Equal((int)FlowRunStatus.Failed, state!.Status);
        Assert.Contains("import failed", state.LastMessage);
    }

    [Fact]
    public async Task DurableFlow_UnknownFlowId_Returns404()
    {
        var response = await Client.GetAsync($"/durable-flow/{NewId("missing")}");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    private sealed record FlowStateDto(
        int Status,
        string? LastMessage,
        string? InputJson,
        int Attempts,
        Dictionary<string, FlowStepDto>? Steps,
        Dictionary<string, string>? Values);

    private sealed record FlowStepDto(bool Completed, bool Faulted, string? PendingCorrelationId);
}
