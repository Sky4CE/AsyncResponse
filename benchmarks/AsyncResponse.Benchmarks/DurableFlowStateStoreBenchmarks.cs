using AsyncResponse.DurableFlows.Sqlite;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// Durable-flow state-store baseline: create, load, and delete one ledger through SQLite and the
/// explicit process-local store.
/// </summary>
[MemoryDiagnoser]
public class DurableFlowStateStoreBenchmarks
{
    private string _databasePath = "";
    private SqliteFlowStateStore _store = null!;
    private ServiceProvider _serviceProvider = null!;
    private IFlowStateStore _memoryStore = null!;
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
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows();
        _serviceProvider = services.BuildServiceProvider();
        _memoryStore = _serviceProvider.GetRequiredService<IFlowStateStore>();
        _state = CreateState("bench-load");
        await _store.TryCreateAsync(_state.FlowId!, _state, TimeSpan.FromMinutes(30));
        await _memoryStore.TryCreateAsync(_state.FlowId!, _state, TimeSpan.FromMinutes(30));
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _serviceProvider.DisposeAsync();
        File.Delete(_databasePath);
    }

    [Benchmark]
    public Task<bool> CreateAsync()
    {
        var id = "bench-save-" + Interlocked.Increment(ref _sequence).ToString("D8");
        _state.FlowId = id;
        _state.Revision = 0;
        return _store.TryCreateAsync(id, _state, TimeSpan.FromMinutes(30));
    }

    [Benchmark]
    public Task<FlowState?> LoadAsync() => _store.LoadAsync("bench-load");

    [Benchmark]
    public async Task SaveLoadDeleteAsync()
    {
        var id = "bench-roundtrip-" + Interlocked.Increment(ref _sequence).ToString("D8");
        _state.FlowId = id;
        _state.Revision = 0;
        await _store.TryCreateAsync(id, _state, TimeSpan.FromMinutes(30));
        _ = await _store.LoadAsync(id);
        await _store.TryDeleteAsync(id);
    }

    [Benchmark]
    public Task<bool> InMemoryCreateAsync()
    {
        var id = "bench-save-" + Interlocked.Increment(ref _sequence).ToString("D8");
        _state.FlowId = id;
        _state.Revision = 0;
        return _memoryStore.TryCreateAsync(id, _state, TimeSpan.FromMinutes(30));
    }

    [Benchmark]
    public Task<FlowState?> InMemoryLoadAsync() => _memoryStore.LoadAsync("bench-load");

    [Benchmark]
    public async Task InMemoryCreateLoadDeleteAsync()
    {
        var id = "bench-roundtrip-" + Interlocked.Increment(ref _sequence).ToString("D8");
        _state.FlowId = id;
        _state.Revision = 0;
        await _memoryStore.TryCreateAsync(id, _state, TimeSpan.FromMinutes(30));
        _ = await _memoryStore.LoadAsync(id);
        await _memoryStore.TryDeleteAsync(id);
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
