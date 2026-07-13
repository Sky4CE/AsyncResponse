using BenchmarkDotNet.Attributes;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// Recovery-state and watchdog costs that matter when many wait registrations exist at once.
/// </summary>
[MemoryDiagnoser]
public class RecoveryBenchmarks
{
    private InMemoryRecoveryStateStore _store = null!;
    private RecoveryStateObservation[] _observations = [];
    private AsyncResponseWatchdogSnapshot _healthySnapshot = null!;
    private AsyncResponseWatchdogSnapshot _degradedSnapshot = null!;
    private long _nextId;

    [Params(128, 1024, 8192)]
    public int Entries;

    [GlobalSetup]
    public void Setup()
    {
        _store = new InMemoryRecoveryStateStore();
        var now = DateTime.UtcNow;
        _observations = Enumerable.Range(0, Entries)
            .Select(i => new RecoveryStateObservation(
                $"bench-{i}",
                i % 7 == 0 ? null : now.AddMinutes(-(i % 60)),
                i % 5 == 0 ? 1 : 0,
                typeof(BenchPayload).FullName))
            .ToArray();

        _healthySnapshot = new AsyncResponseWatchdogSnapshot(
            now,
            TimeSpan.FromMinutes(1),
            AsyncResponseWatchdogReport.Evaluate(
                [new RecoveryStateObservation("active", now, 1, typeof(BenchPayload).FullName)],
                now,
                TimeSpan.FromMinutes(10)),
            Error: null);

        _degradedSnapshot = new AsyncResponseWatchdogSnapshot(
            now,
            TimeSpan.FromMinutes(1),
            AsyncResponseWatchdogReport.Evaluate(_observations, now, TimeSpan.FromMinutes(10)),
            Error: null);
    }

    [Benchmark]
    public async Task<bool> InMemoryStore_SaveGetDelete()
    {
        var correlationId = $"store-{Interlocked.Increment(ref _nextId)}";

        await _store.SaveAsync(correlationId, NewRecoveryState(correlationId), TimeSpan.FromMinutes(5))
            .ConfigureAwait(false);
        var loaded = await _store.GetAllAsync(correlationId).ConfigureAwait(false);
        var deleted = await _store.TryDeleteAsync(correlationId, loaded[0].RegistrationId).ConfigureAwait(false);

        return loaded.Count == 1 && deleted;
    }

    [Benchmark]
    public async Task<int> InMemoryStore_Scan()
    {
        var prefix = $"scan-{Interlocked.Increment(ref _nextId)}-";
        for (var i = 0; i < Entries; i++)
        {
            var correlationId = prefix + i;
            await _store.SaveAsync(correlationId, NewRecoveryState(correlationId), TimeSpan.FromMinutes(5))
                .ConfigureAwait(false);
        }

        var count = 0;
        await foreach (var _ in _store.ScanAsync().ConfigureAwait(false))
            count++;

        for (var i = 0; i < Entries; i++)
        {
            var correlationId = prefix + i;
            var states = await _store.GetAllAsync(correlationId).ConfigureAwait(false);
            if (states.Count > 0)
                await _store.TryDeleteAsync(correlationId, states[0].RegistrationId).ConfigureAwait(false);
        }

        return count;
    }

    [Benchmark]
    public AsyncResponseWatchdogReport Watchdog_Evaluate_MixedSnapshot()
        => AsyncResponseWatchdogReport.Evaluate(_observations, DateTime.UtcNow, TimeSpan.FromMinutes(10));

    [Benchmark]
    public string HealthCheck_Evaluate_Healthy()
        => AsyncResponseRecoveryHealthCheck.Evaluate(_healthySnapshot, DateTime.UtcNow).Status.ToString();

    [Benchmark]
    public string HealthCheck_Evaluate_Degraded()
        => AsyncResponseRecoveryHealthCheck.Evaluate(_degradedSnapshot, DateTime.UtcNow).Status.ToString();

    private static RecoveryState NewRecoveryState(string correlationId)
        => new()
        {
            RegistrationId = Guid.NewGuid(),
            CorrelationId = correlationId,
            PayloadTypeFullName = typeof(BenchPayload).FullName,
            RegisteredAtUtc = DateTime.UtcNow
        };
}
