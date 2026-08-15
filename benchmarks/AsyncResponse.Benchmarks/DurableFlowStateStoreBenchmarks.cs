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
    private readonly List<string> _createdIds = [];
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
        var seed = CreateState("bench-load");
        await _store.TryCreateAsync(seed.FlowId!, seed, TimeSpan.FromMinutes(30));
        await _memoryStore.TryCreateAsync(seed.FlowId!, seed, TimeSpan.FromMinutes(30));
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _serviceProvider.DisposeAsync();
        File.Delete(_databasePath);
    }

    // Each create gets a fresh per-op state: a shared mutated instance would alias every stored
    // entry (including the "bench-load" row the load benchmarks read), and the rows are deleted
    // OUTSIDE the measured op in [IterationCleanup] — left in place, later iterations run against
    // an ever-larger store, so the ns/op trend and the MemoryDiagnoser numbers drift with
    // iteration count instead of measuring the operation.
    [Benchmark]
    public Task<bool> CreateAsync()
    {
        var id = NextCreatedId();
        return _store.TryCreateAsync(id, CreateState(id), TimeSpan.FromMinutes(30));
    }

    [IterationCleanup(Target = nameof(CreateAsync))]
    public void CleanupCreated()
    {
        foreach (var id in _createdIds)
            _store.TryDeleteAsync(id).GetAwaiter().GetResult();
        _createdIds.Clear();
    }

    [Benchmark]
    public Task<FlowState?> LoadAsync() => _store.LoadAsync("bench-load");

    [Benchmark]
    public async Task SaveLoadDeleteAsync()
    {
        var id = "bench-roundtrip-" + Interlocked.Increment(ref _sequence).ToString("D8");
        await _store.TryCreateAsync(id, CreateState(id), TimeSpan.FromMinutes(30));
        _ = await _store.LoadAsync(id);
        await _store.TryDeleteAsync(id);
    }

    [Benchmark]
    public Task<bool> InMemoryCreateAsync()
    {
        var id = NextCreatedId();
        return _memoryStore.TryCreateAsync(id, CreateState(id), TimeSpan.FromMinutes(30));
    }

    [IterationCleanup(Target = nameof(InMemoryCreateAsync))]
    public void CleanupInMemoryCreated()
    {
        foreach (var id in _createdIds)
            _memoryStore.TryDeleteAsync(id).GetAwaiter().GetResult();
        _createdIds.Clear();
    }

    [Benchmark]
    public Task<FlowState?> InMemoryLoadAsync() => _memoryStore.LoadAsync("bench-load");

    [Benchmark]
    public async Task InMemoryCreateLoadDeleteAsync()
    {
        var id = "bench-roundtrip-" + Interlocked.Increment(ref _sequence).ToString("D8");
        await _memoryStore.TryCreateAsync(id, CreateState(id), TimeSpan.FromMinutes(30));
        _ = await _memoryStore.LoadAsync(id);
        await _memoryStore.TryDeleteAsync(id);
    }

    private string NextCreatedId()
    {
        var id = "bench-save-" + Interlocked.Increment(ref _sequence).ToString("D8");
        _createdIds.Add(id);
        return id;
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
