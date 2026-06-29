using AsyncResponse.Transports.PostgreSQL;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// PostgreSQL transport subscriber dispatch overhead for the two ACK modes. Deliberately
/// in-process: integration/load tests cover the real PostgreSQL table and HTTP-facing paths.
/// </summary>
[MemoryDiagnoser]
public class PostgreSqlAckDispatchBenchmarks
{
    private static readonly IReadOnlyDictionary<string, string> Headers =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["AR-Correlation-Id"] = "benchmark-correlation" };

    private PostgreSqlMessageDispatcher _awaiting = null!;
    private PostgreSqlMessageDispatcher _queued = null!;

    [Params(1, 8)]
    public int BackgroundWorkers;

    [GlobalSetup]
    public void Setup()
    {
        var options = new PostgreSqlAsyncResponseTransportOptions();

        _awaiting = new PostgreSqlMessageDispatcher(
            (_, _) => Task.CompletedTask,
            options,
            new PostgreSqlSubscriberOptions(),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        _queued = new PostgreSqlMessageDispatcher(
            (_, _) => Task.CompletedTask,
            options,
            new PostgreSqlSubscriberOptions().UseAckAfterReceive(
                backgroundWorkerCount: BackgroundWorkers,
                backgroundQueueCapacity: 16_384,
                backgroundDrainTimeout: TimeSpan.FromSeconds(30)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _queued.DisposeAsync().ConfigureAwait(false);
        await _awaiting.DisposeAsync().ConfigureAwait(false);
    }

    [Benchmark(Baseline = true)]
    public Task AckAfterHandlerCompletes_Callback()
        => _awaiting.HandleAsync(Delivery(), CancellationToken.None);

    [Benchmark]
    public Task AckAfterReceive_Callback()
        => _queued.HandleAsync(Delivery(), CancellationToken.None);

    private static PostgreSqlTransportDelivery Delivery()
        => new(
            Guid.NewGuid(),
            "worker",
            "{}",
            Headers,
            Attempt: 1,
            () => ValueTask.CompletedTask,
            _ => ValueTask.CompletedTask,
            (_, _, _) => ValueTask.FromResult(true));
}
