using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 35 pins over API this round introduced — <c>IDurableFlowExecutor.CreateAndExecuteAsync</c>,
/// <c>DurableFlowOptions.LedgerSizeWarningBytes</c>, <c>FlowStateJson.EstimateLedgerChars</c>, and
/// <c>SerialExecutorRegistry.TryEnqueue</c>. They are compile-level red on the pre-fix tree, so
/// they live apart from <see cref="Round35RegressionTests"/>, whose behavior pins are copied onto
/// the old source for the red-on-old proof.
/// </summary>
public sealed class Round35NewApiTests
{
    private static ServiceProvider BuildFlowProvider(IWorkerTransport transport)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<Round35RegressionTests.R35MarkerFlow, Round35RegressionTests.R35Input>();
        services.AddSingleton(transport);
        return services.BuildServiceProvider();
    }

    private sealed class NullWorkerTransport : IWorkerTransport
    {
        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private static FlowState InitialState(string flowId, string inputJson) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(Round35RegressionTests.R35MarkerFlow).FullName,
        InputTypeName = typeof(Round35RegressionTests.R35Input).FullName,
        InputJson = inputJson,
        Status = FlowRunStatus.Running,
        LastMessage = "Flow started.",
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    // ---------------------------------------------------------------------------------------------
    // A1 — the executor-side half of the start's idempotency contract.

    /// <summary>A start job for an id already bound to different work is dropped, loudly, without touching the existing ledger.</summary>
    [Fact]
    public async Task CreateAndExecute_WhenTheIdIsBoundToDifferentWork_DropsTheJobAndLeavesTheLedgerAlone()
    {
        await using var provider = BuildFlowProvider(new NullWorkerTransport());
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var store = provider.GetRequiredService<InMemoryFlowStateStore>();

        const string id = "flow-r35-conflict";
        Assert.True(await store.TryCreateAsync(id, InitialState(id, """{"Name":"first"}"""), TimeSpan.FromDays(1)));

        var before = Volatile.Read(ref Round35RegressionTests.R35MarkerFlow.Executions);
        await executor.CreateAndExecuteAsync(id, FlowStateJson.Serialize(InitialState(id, """{"Name":"second"}""")));

        Assert.Equal(before, Volatile.Read(ref Round35RegressionTests.R35MarkerFlow.Executions));
        var untouched = await store.LoadAsync(id);
        Assert.NotNull(untouched);
        Assert.Equal("""{"Name":"first"}""", untouched!.InputJson);
        Assert.Equal(0, untouched.Attempts);
        Assert.Equal(FlowRunStatus.Running, untouched.Status);
    }

    /// <summary>The same start already persisted (a redelivery, or the starter's create winning) is simply executed.</summary>
    [Fact]
    public async Task CreateAndExecute_WhenTheSameStartAlreadyExists_ExecutesTheExistingRun()
    {
        await using var provider = BuildFlowProvider(new NullWorkerTransport());
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var store = provider.GetRequiredService<InMemoryFlowStateStore>();

        const string id = "flow-r35-same-start";
        Assert.True(await store.TryCreateAsync(id, InitialState(id, """{"Name":"acme"}"""), TimeSpan.FromDays(1)));

        var before = Volatile.Read(ref Round35RegressionTests.R35MarkerFlow.Executions);
        // Semantically identical input (different formatting) is the same start.
        await executor.CreateAndExecuteAsync(id, FlowStateJson.Serialize(InitialState(id, """{ "Name" : "acme" }""")));

        Assert.Equal(before + 1, Volatile.Read(ref Round35RegressionTests.R35MarkerFlow.Executions));
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(id))!.Status);
    }

    /// <summary>A carrier for a different id than the job names is refused (and the throw propagates to the transport).</summary>
    [Fact]
    public async Task CreateAndExecute_WithACarrierForAnotherId_Throws()
    {
        await using var provider = BuildFlowProvider(new NullWorkerTransport());
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => executor.CreateAndExecuteAsync("flow-r35-a", FlowStateJson.Serialize(InitialState("flow-r35-b", """{"Name":"x"}"""))));
    }

    // ---------------------------------------------------------------------------------------------
    // P2 — every checkpoint rewrites the whole ledger, so persistence cost grows with each completed
    //      step until the store's hard cap. An early warning at a configurable estimated size, once
    //      and then per doubling.

    public sealed record ChattyInput(string Name);

    public sealed class ChattyFlow : IDurableFlow<ChattyInput>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, ChattyInput input)
        {
            for (var i = 0; i < 6; i++)
                await flow.StepAsync($"step-{i}", () => Task.FromResult(new string('x', 1024)));
        }
    }

    [Fact]
    public async Task LedgerGrowth_PastTheWarningThreshold_IsLoggedOnceThenPerDoubling()
    {
        var logger = new CollectingLogger();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ILogger<DurableFlowExecutor>>(logger.For<DurableFlowExecutor>());
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows(options => options.LedgerSizeWarningBytes = 2048)
            .WithDurableFlow<ChattyFlow, ChattyInput>();
        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);

        try
        {
            var flows = provider.GetRequiredService<IDurableFlows>();
            var id = await flows.StartAsync<ChattyFlow, ChattyInput>(new ChattyInput("acme"), "flow-r35-chatty");

            var deadline = DateTime.UtcNow.AddSeconds(15);
            FlowState? state;
            do
            {
                state = await flows.GetStateAsync(id);
                if (state?.Status == FlowRunStatus.Succeeded)
                    break;
                await Task.Delay(20);
            }
            while (DateTime.UtcNow < deadline);
            Assert.Equal(FlowRunStatus.Succeeded, state?.Status);

            // ~6 KiB of results over a 2 KiB threshold: crossed once (≥2 KiB), then at the doubling
            // (≥4 KiB) — never once per step.
            var warnings = logger.Messages.Where(m => m.Contains("LedgerSizeWarningBytes threshold", StringComparison.Ordinal)).ToArray();
            Assert.InRange(warnings.Length, 1, 3);
            Assert.All(warnings, w => Assert.Contains(id, w, StringComparison.Ordinal));
        }
        finally
        {
            foreach (var service in hosted)
                await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public void LedgerSizeWarningBytes_MustBePositiveOrNull()
    {
        Assert.Throws<InvalidOperationException>(() => FlowStateConcurrency.ValidateOptions(new DurableFlowOptions { LedgerSizeWarningBytes = 0 }));
        Assert.Throws<InvalidOperationException>(() => FlowStateConcurrency.ValidateOptions(new DurableFlowOptions { LedgerSizeWarningBytes = -1 }));
        FlowStateConcurrency.ValidateOptions(new DurableFlowOptions { LedgerSizeWarningBytes = null });
        FlowStateConcurrency.ValidateOptions(new DurableFlowOptions { LedgerSizeWarningBytes = 1 });
    }

    [Fact]
    public void EstimateLedgerChars_CountsEveryStringTheLedgerCarries()
    {
        var state = new FlowState
        {
            InputJson = new string('i', 10),
            LastMessage = new string('m', 5),
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
            {
                ["ab"] = new() { ResultJson = new string('r', 100), Message = new string('s', 3) }
            },
            Values = new Dictionary<string, string>(StringComparer.Ordinal) { ["k"] = new string('v', 20) },
            Context = new Dictionary<string, string>(StringComparer.Ordinal) { ["c"] = new string('x', 4) }
        };

        Assert.Equal(10 + 5 + 2 + 100 + 3 + 1 + 20 + 1 + 4, FlowStateJson.EstimateLedgerChars(state));
    }

    // ---------------------------------------------------------------------------------------------
    // P1 — the registry's non-blocking admission the DB channels' dispatch sweep now uses. The
    //      sweep-level behavior pin runs against the shared channel source in
    //      DbChannelSharedCoverageTests.

    [Fact]
    public async Task SerialExecutorRegistry_TryEnqueue_AtCapacity_ReportsFullInsteadOfWaiting()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        registry.OnSubscriptionRegistered("corr");
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // The first item occupies the single reader for as long as the gate is held...
        Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Accepted, registry.TryEnqueue("corr", () => gate.Task));

        // ...so at most the queue capacity (plus that one item, if the reader has not pulled it
        // yet) can be admitted; the next call answers Full synchronously rather than parking.
        var accepted = 1;
        SerialExecutorRegistry.TryEnqueueOutcome last;
        do
        {
            last = registry.TryEnqueue("corr", static () => Task.CompletedTask);
            if (last == SerialExecutorRegistry.TryEnqueueOutcome.Accepted)
                accepted++;
        }
        while (last == SerialExecutorRegistry.TryEnqueueOutcome.Accepted && accepted <= ChannelSerialExecutor.DefaultCapacity + 2);

        Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Full, last);
        Assert.InRange(accepted, ChannelSerialExecutor.DefaultCapacity, ChannelSerialExecutor.DefaultCapacity + 1);

        gate.SetResult();
        registry.OnSubscriptionRetired("corr");
        await registry.RemoveAsync("corr");
    }

    [Fact]
    public async Task SerialExecutorRegistry_TryEnqueue_OnATombstonedChannel_IsSuppressed()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        registry.OnSubscriptionRegistered("corr");
        Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Accepted, registry.TryEnqueue("corr", static () => Task.CompletedTask));
        registry.OnSubscriptionRetired("corr");
        await registry.RemoveAsync("corr");

        Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Suppressed, registry.TryEnqueue("corr", static () => Task.CompletedTask));
    }
}
