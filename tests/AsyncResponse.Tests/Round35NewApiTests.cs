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
        services.AddSingleton<Round35RegressionTests.R35ExecutionCounter>();
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

        var counter = provider.GetRequiredService<Round35RegressionTests.R35ExecutionCounter>();
        var before = counter.Executions;
        await executor.CreateAndExecuteAsync(id, FlowStateJson.Serialize(InitialState(id, """{"Name":"second"}""")));

        Assert.Equal(before, counter.Executions);
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

        var counter = provider.GetRequiredService<Round35RegressionTests.R35ExecutionCounter>();
        var before = counter.Executions;
        // Semantically identical input (different formatting) is the same start.
        await executor.CreateAndExecuteAsync(id, FlowStateJson.Serialize(InitialState(id, """{ "Name" : "acme" }""")));

        Assert.Equal(before + 1, counter.Executions);
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

    public sealed class TinyChildFlow : IDurableFlow<ChattyInput>
    {
        public Task ExecuteAsync(IDurableFlowContext flow, ChattyInput input) => Task.CompletedTask;
    }

    /// <summary>Awaits ten children one after another: eleven executions of one already-large ledger.</summary>
    public sealed class ManyWakeUpsFlow : IDurableFlow<ChattyInput>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, ChattyInput input)
        {
            for (var i = 0; i < 10; i++)
                await flow.AwaitChildFlowAsync<TinyChildFlow, ChattyInput>($"child-{i}", new ChattyInput("c"));
        }
    }

    [Fact]
    public async Task LedgerGrowth_AcrossManyWakeUps_IsLoggedPerDoubling_NotOncePerExecution()
    {
        // Fixpoint r1 (GS1#5): every wake-up builds a fresh context, and each one started at the
        // bare threshold — a run already past it warned again on the first save of EVERY execution.
        var logger = new CollectingLogger();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ILogger<DurableFlowExecutor>>(logger.For<DurableFlowExecutor>());
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows(options =>
            {
                options.LedgerSizeWarningBytes = 512;
                // The run is real (in-memory transport, system clock): a parent wake-up that
                // beats the parked parent's lease release waits one contention poll, which is
                // min(renew interval, 2 s). At the default that was up to 2 s per child, ten
                // times over, against the deadline below; at 100 ms it is milliseconds.
                options.ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(100);
            })
            .WithDurableFlow<ManyWakeUpsFlow, ChattyInput>()
            .WithDurableFlow<TinyChildFlow, ChattyInput>();
        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);

        try
        {
            var flows = provider.GetRequiredService<IDurableFlows>();
            // A kilobyte of input: the ledger is past the 512-byte threshold from its first save.
            var id = await flows.StartAsync<ManyWakeUpsFlow, ChattyInput>(new ChattyInput(new string('x', 1024)), "flow-gs15-wakeups");

            var deadline = DateTime.UtcNow.AddSeconds(30);
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
            Assert.True(state!.Attempts >= 11, $"expected one execution per child wake-up, saw {state.Attempts}");

            // ~1 KiB growing to a few KiB: a handful of doublings, not one warning per execution.
            var warnings = logger.Messages.Where(m => m.Contains("LedgerSizeWarningBytes threshold", StringComparison.Ordinal)
                && m.Contains(id, StringComparison.Ordinal)).ToArray();
            Assert.InRange(warnings.Length, 1, 4);

            // Precommit review (A3): and the FIRST crossing is logged. The input alone put the
            // ledger past the threshold (~1.1 KiB), so the first warning reports that size, as the
            // first execution begins — seeding every execution at the next doubling, the first one
            // included, skipped it and warned only once the ledger reached 2 KiB.
            var firstWarnedSize = long.Parse(
                System.Text.RegularExpressions.Regex.Match(warnings[0], @"roughly (\d+) bytes").Groups[1].Value,
                System.Globalization.CultureInfo.InvariantCulture);
            Assert.InRange(firstWarnedSize, 1024, 2047);
        }
        finally
        {
            foreach (var service in hosted)
                await service.StopAsync(CancellationToken.None);
        }
    }

    [Theory]
    [InlineData(1, 1)]   // the run's first execution: no earlier one can have warned
    [InlineData(2, 0)]   // a later one: the first execution already did
    public async Task ALedgerItsStartAlreadyMadeLarge_IsWarnedAsItsFirstExecutionBegins_AndNotAgainLater(int attempts, int expectedWarnings)
    {
        // Precommit review (A3): each context seeded its first warning at the next doubling above
        // the ledger's current size, the first execution's included — so a ledger already past the
        // threshold when the run started (a 600 KiB input against the 512 KiB default) logged its
        // first crossing never, only a later doubling if one came. The step below adds nothing
        // like a doubling: the warning must not wait for one.
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        var transport = new DurableFlowContextTestSupport.RecordingTransport();
        await using var provider = DurableFlowContextTestSupport.BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var options = DurableFlowContextTestSupport.Options(o => o.LedgerSizeWarningBytes = 512);
        var state = DurableFlowContextTestSupport.State($"large-from-its-start-{attempts}");
        state.InputJson = System.Text.Json.JsonSerializer.Serialize(new ChattyInput(new string('x', 600)));
        state.Attempts = attempts;
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));
        await using var lease = await DurableFlowContextTestSupport.AcquireAsync(store, state.FlowId!, options, clock);
        var logger = new CollectingLogger();

        var context = DurableFlowContextTestSupport.CreateContext(provider, state, store, lease, options, clock, transport, logger);
        await context.StepAsync("small", () => Task.CompletedTask);

        var warnings = logger.Messages.Where(m => m.Contains("LedgerSizeWarningBytes threshold", StringComparison.Ordinal)).ToArray();
        Assert.Equal(expectedWarnings, warnings.Length);
        Assert.All(warnings, warning => Assert.Contains(state.FlowId!, warning, StringComparison.Ordinal));
    }

    /// <summary>One small step: a later execution that saves once, well short of any doubling.</summary>
    public sealed class OneSmallStepFlow : IDurableFlow<ChattyInput>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, ChattyInput input)
            => await flow.StepAsync("small", () => Task.FromResult(1));
    }

    private static ServiceProvider BuildLedgerWarningProvider(CollectingLogger logger)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ILogger<DurableFlowExecutor>>(logger.For<DurableFlowExecutor>());
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows(options => options.LedgerSizeWarningBytes = 512)
            .WithDurableFlow<OneSmallStepFlow, ChattyInput>();
        services.AddSingleton<IWorkerTransport>(new NullWorkerTransport());
        return services.BuildServiceProvider();
    }

    private static FlowState SmallLedger(string flowId, Type flowType, int attempts) => new()
    {
        FlowId = flowId,
        FlowTypeName = flowType.FullName,
        InputTypeName = typeof(ChattyInput).FullName,
        InputJson = System.Text.Json.JsonSerializer.Serialize(new ChattyInput("c")),
        Status = FlowRunStatus.Running,
        Attempts = attempts,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    private static string[] LedgerWarnings(CollectingLogger logger, string flowId)
        => logger.Messages.Where(m => m.Contains("LedgerSizeWarningBytes threshold", StringComparison.Ordinal)
            && m.Contains(flowId, StringComparison.Ordinal)).ToArray();

    [Fact]
    public async Task ALedgerARecoveredResponsePushesPastTheThreshold_BetweenLaterExecutions_IsWarnedExactlyOnce()
    {
        // Pass-2 precommit review (A3 residual): only the first execution warns at the bare
        // threshold; every later one seeds at the next doubling above the ledger's size. A crossing
        // made between executions by a lease-less write — here the recovery checkpoint of a large
        // recovered response after execution 2 died — was therefore never logged: execution 3
        // started above it. The write that crosses now warns, and execution 3 does not repeat it.
        var logger = new CollectingLogger();
        await using var provider = BuildLedgerWarningProvider(logger);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var ledger = SmallLedger("recovered-past-the-threshold", typeof(OneSmallStepFlow), attempts: 2);
        ledger.Steps = new Dictionary<string, FlowStepState> { ["remote"] = new() { PendingCorrelationId = "cid" } };
        Assert.True(await store.TryCreateAsync(ledger.FlowId!, ledger, TimeSpan.FromDays(1)));
        Assert.InRange(FlowStateJson.EstimateLedgerChars(ledger), 0, 511);

        await executor.RecoverAsync(ledger.FlowId!, new ChattyInput(new string('x', 700)), "cid");
        Assert.InRange(FlowStateJson.EstimateLedgerChars((await store.LoadAsync(ledger.FlowId!))!), 512, 1023);
        Assert.Single(LedgerWarnings(logger, ledger.FlowId!));

        await executor.ExecuteAsync(ledger.FlowId!);

        var finished = await store.LoadAsync(ledger.FlowId!);
        Assert.Equal(FlowRunStatus.Succeeded, finished!.Status);
        Assert.Equal(3, finished.Attempts);
        Assert.Single(LedgerWarnings(logger, ledger.FlowId!));
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
