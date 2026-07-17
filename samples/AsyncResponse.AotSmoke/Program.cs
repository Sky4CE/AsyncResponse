// Native AOT smoke test for AsyncResponse. Fully trimmed, no reflection-based System.Text.Json:
// payload types resolve through the source-generated SmokeJsonContext registered below. Exits 0
// and prints "AOT-SMOKE OK" only when every scenario behaved.

using AsyncResponse;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json.Serialization;

// Trimmed/AOT apps register their payload/flow-input types once at startup; everything the
// library serializes on our behalf resolves through this context.
AsyncResponseJsonSerialization.RegisterResolver(SmokeJsonContext.Default);

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.Services
    .AddAsyncResponse()
    .WithInMemoryChannel()
    .WithInMemoryTransport()
    .WithInMemoryDurableFlows()
    .WithDurableFlow<ProvisioningFlow, ProvisionRequest>();

using var host = builder.Build();
await host.StartAsync();

var responses = host.Services.GetRequiredService<IAsyncResponseBuilder>();
var publisher = host.Services.GetRequiredService<IAsyncResponsePublisher>();
var flows = host.Services.GetRequiredService<IDurableFlows>();

// --- 1) Plain request/response round-trip: envelope serialization + waiter dispatch. ---
var direct = await responses.For<WorkCompleted>()
    .WithTimeout(TimeSpan.FromSeconds(10))
    .WaitAsync(context => Task.Run(async () =>
    {
        await Task.Delay(25);
        await publisher.SetResponse(new WorkCompleted { Outcome = "direct-ok" }, context.CorrelationId);
    }));
Require(direct.Outcome == "direct-ok", $"direct round-trip returned '{direct.Outcome}'");

// --- 2) Durable flow: local step memoization, awaited remote step, values bag, worker dispatch. ---
var flowId = await flows.StartAsync<ProvisioningFlow, ProvisionRequest>(new ProvisionRequest { Name = "smoke" });

FlowState? state = null;
for (var i = 0; i < 200; i++)
{
    state = await flows.GetStateAsync(flowId);
    if (state is { Status: FlowRunStatus.Succeeded or FlowRunStatus.Failed })
        break;
    await Task.Delay(50);
}

Require(state is not null, "flow state disappeared");
Require(state!.Status == FlowRunStatus.Succeeded, $"flow ended as {state.Status}: {state.LastMessage}");
Require(state.Steps is { Count: 2 }, $"expected 2 checkpointed steps, found {state.Steps?.Count ?? 0}");
Require(state.Values is not null && state.Values.TryGetValue("greeting", out var greeting)
        && greeting.Contains("smoke-prep", StringComparison.Ordinal),
    "flow values bag did not round-trip");

await host.StopAsync();
Console.WriteLine("AOT-SMOKE OK");
return 0;

static void Require(bool condition, string failure)
{
    if (condition)
        return;

    Console.Error.WriteLine($"AOT-SMOKE FAILED: {failure}");
    Environment.Exit(1);
}

/// <summary>Flow input, persisted as JSON with the flow state.</summary>
public sealed class ProvisionRequest
{
    public string Name { get; set; } = "";
}

/// <summary>Local step result, memoized into the flow ledger.</summary>
public sealed class PrepareResult
{
    public string Slug { get; set; } = "";
}

/// <summary>Remote-work payload awaited by the flow and by the direct round-trip.</summary>
public sealed class WorkCompleted : IAsyncResponsePayload
{
    public string Outcome { get; set; } = "";

    public bool ShouldResumeOnRecovery() => true;
}

/// <summary>
/// The smoke flow: one memoized local step, the values bag, and one awaited remote step whose
/// trigger publishes the response through the in-memory channel.
/// </summary>
public sealed class ProvisioningFlow(IAsyncResponsePublisher _publisher) : IDurableFlow<ProvisionRequest>
{
    public async Task ExecuteAsync(IDurableFlowContext context, ProvisionRequest input)
    {
        var prepared = await context.StepAsync("prepare",
            () => Task.FromResult(new PrepareResult { Slug = $"{input.Name}-prep" }));

        await context.SetValueAsync("greeting", $"hello-{prepared.Slug}");

        var done = await context.AwaitStepAsync<WorkCompleted>(
            "remote-work",
            trigger: correlationId => Task.Run(async () =>
            {
                await Task.Delay(25);
                await _publisher.SetResponse(new WorkCompleted { Outcome = "flow-ok" }, correlationId);
            }),
            timeout: TimeSpan.FromSeconds(10));

        if (done.Outcome != "flow-ok")
            throw new DurableFlowFailedException($"unexpected remote outcome '{done.Outcome}'");
    }
}

/// <summary>Source-generated metadata for every type this app asks the library to serialize.</summary>
[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(ProvisionRequest))]
[JsonSerializable(typeof(PrepareResult))]
[JsonSerializable(typeof(WorkCompleted))]
internal sealed partial class SmokeJsonContext : JsonSerializerContext;
