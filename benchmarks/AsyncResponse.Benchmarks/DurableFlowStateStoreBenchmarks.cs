using AsyncResponse.DurableFlows.Sqlite;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// Durable-flow state-store baseline: save, load, and delete one ledger row through the SQLite
/// package store and, for comparison, through the default <see cref="RecoveryBackedFlowStateStore"/>
/// (backed by the in-memory recovery store) — quantifying the default-vs-package trade-off.
/// </summary>
[MemoryDiagnoser]
public class DurableFlowStateStoreBenchmarks
{
    private string _databasePath = "";
    private SqliteFlowStateStore _store = null!;
    private RecoveryBackedFlowStateStore _recoveryStore = null!;
    private FlowState _state = null!;
    private int _sequence;

    [GlobalSetup]
    public async Task Setup()
    {
        _databasePath = Path.Combine(Path.GetTempPath(), $"ar-flow-bench-{Guid.NewGuid():N}.db");
        _store = new SqliteFlowStateStore(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = $"Data Source={_databasePath}"
        }));
        _recoveryStore = new RecoveryBackedFlowStateStore(new InMemoryRecoveryStateStore());
        _state = CreateState("bench-load");
        await _store.SaveAsync(_state.FlowId!, _state, TimeSpan.FromMinutes(30));
        await _recoveryStore.SaveAsync(_state.FlowId!, _state, TimeSpan.FromMinutes(30));
    }

    [GlobalCleanup]
    public void Cleanup() => File.Delete(_databasePath);

    [Benchmark]
    public Task SaveAsync()
    {
        var id = "bench-save-" + Interlocked.Increment(ref _sequence).ToString("D8");
        _state.FlowId = id;
        return _store.SaveAsync(id, _state, TimeSpan.FromMinutes(30));
    }

    [Benchmark]
    public Task<FlowState?> LoadAsync() => _store.LoadAsync("bench-load");

    [Benchmark]
    public async Task SaveLoadDeleteAsync()
    {
        var id = "bench-roundtrip-" + Interlocked.Increment(ref _sequence).ToString("D8");
        _state.FlowId = id;
        await _store.SaveAsync(id, _state, TimeSpan.FromMinutes(30));
        _ = await _store.LoadAsync(id);
        await _store.TryDeleteAsync(id);
    }

    [Benchmark]
    public Task RecoveryBackedSaveAsync()
    {
        var id = "bench-save-" + Interlocked.Increment(ref _sequence).ToString("D8");
        _state.FlowId = id;
        return _recoveryStore.SaveAsync(id, _state, TimeSpan.FromMinutes(30));
    }

    [Benchmark]
    public Task<FlowState?> RecoveryBackedLoadAsync() => _recoveryStore.LoadAsync("bench-load");

    [Benchmark]
    public async Task RecoveryBackedSaveLoadDeleteAsync()
    {
        var id = "bench-roundtrip-" + Interlocked.Increment(ref _sequence).ToString("D8");
        _state.FlowId = id;
        await _recoveryStore.SaveAsync(id, _state, TimeSpan.FromMinutes(30));
        _ = await _recoveryStore.LoadAsync(id);
        await _recoveryStore.TryDeleteAsync(id);
    }

    private static FlowState CreateState(string flowId)
        => new()
        {
            FlowId = flowId,
            FlowTypeName = typeof(DurableFlowStateStoreBenchmarks).FullName,
            InputTypeName = typeof(int).FullName,
            InputJson = JsonSerializer.Serialize(42),
            Status = FlowRunStatus.Running,
            LastMessage = "started",
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow,
            Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
            {
                ["prepare"] = new() { Completed = true, ResultJson = "123", CompletedAtUtc = DateTime.UtcNow }
            },
            Values = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["tenant"] = "42"
            }
        };
}
