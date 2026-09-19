using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// The durable-flow ledger's cost curve: every checkpoint rewrites the WHOLE ledger, so a run of
/// N steps with similar result sizes serializes about N²/2 step-results over its lifetime. This
/// compares raw single-ledger rewrites with bounded groups through the process-local store.
/// This deliberately bypasses the engine's MaxRetainedSteps budget to expose the unbounded
/// baseline; real-engine bounded-child-tree byte scaling is asserted in LedgerBudgetTests.
/// </summary>
[MemoryDiagnoser]
public class LedgerGrowthBenchmarks
{
    private ServiceProvider _serviceProvider = null!;
    private IFlowStateStore _store = null!;
    private string _resultJson = "";
    private int _sequence;

    /// <summary>Retained step results per run.</summary>
    [Params(50, 200, 400)]
    public int Steps { get; set; }

    /// <summary>Size of each step's result (a JSON string), in bytes.</summary>
    [Params(1024)]
    public int ResultBytes { get; set; }

    /// <summary>Zero keeps one unbounded baseline ledger; eight models bounded result groups.</summary>
    [Params(0, 8)]
    public int StepsPerLedger { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows();
        _serviceProvider = services.BuildServiceProvider();
        _store = _serviceProvider.GetRequiredService<IFlowStateStore>();
        _resultJson = "\"" + new string('r', Math.Max(0, ResultBytes - 2)) + "\"";
    }

    [GlobalCleanup]
    public async Task Cleanup()
        => await _serviceProvider.DisposeAsync();

    /// <summary>
    /// Measures all writes and cleanup for the requested results. Grouped ledgers cap the
    /// retained history in each write; the single-ledger baseline keeps every preceding result.
    /// </summary>
    [Benchmark]
    public async Task RunOfNSteps()
    {
        var batchSize = StepsPerLedger > 0 ? StepsPerLedger : Steps;
        for (var offset = 0; offset < Steps; offset += batchSize)
            await WriteLedgerAsync(Math.Min(batchSize, Steps - offset));
    }

    private async Task WriteLedgerAsync(int count)
    {
        var flowId = $"ledger-growth-{Interlocked.Increment(ref _sequence)}";
        var state = new FlowState
        {
            FlowId = flowId,
            FlowTypeName = "Bench.Flow",
            InputTypeName = "Bench.Input",
            InputJson = "{}",
            Status = FlowRunStatus.Running,
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow,
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
        };
        await _store.TryCreateAsync(flowId, state, TimeSpan.FromMinutes(30));

        for (var step = 0; step < count; step++)
        {
            state.Steps![$"step-{step}"] = new FlowStepState { Completed = true, ResultJson = _resultJson, CompletedAtUtc = DateTime.UtcNow };
            var expected = state.Revision;
            state.Revision = expected + 1;
            state.UpdatedAtUtc = DateTime.UtcNow;
            await _store.TryUpdateAsync(flowId, state, expected, TimeSpan.FromMinutes(30));
        }

        await _store.TryDeleteAsync(flowId);
    }
}
