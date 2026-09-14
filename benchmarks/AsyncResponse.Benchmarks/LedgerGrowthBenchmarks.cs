using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// The durable-flow ledger's cost curve: every checkpoint rewrites the WHOLE ledger, so a run of
/// N steps with similar result sizes serializes about N²/2 step-results over its lifetime. This
/// measures one full run — N checkpoints through the process-local store, each carrying every
/// earlier result — so the cumulative bytes written and the time per run can be compared
/// against the step count instead of inferred (docs/durable-flows.md, "Supported ledger budgets").
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
    /// One complete run: create, then one checkpoint per step, each rewriting the ledger with
    /// every result so far. Returns the bytes the LAST checkpoint serialized, so the growth is
    /// visible beside the per-run time and allocations.
    /// </summary>
    [Benchmark]
    public async Task<long> RunOfNSteps()
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

        long lastLedgerChars = 0;
        for (var step = 0; step < Steps; step++)
        {
            state.Steps![$"step-{step}"] = new FlowStepState { Completed = true, ResultJson = _resultJson, CompletedAtUtc = DateTime.UtcNow };
            var expected = state.Revision;
            state.Revision = expected + 1;
            state.UpdatedAtUtc = DateTime.UtcNow;
            await _store.TryUpdateAsync(flowId, state, expected, TimeSpan.FromMinutes(30));
            lastLedgerChars += _resultJson.Length; // the ledger retains every result so far
        }

        await _store.TryDeleteAsync(flowId);
        return lastLedgerChars;
    }
}
