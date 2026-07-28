using AsyncResponse.Transports.MongoDB;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// MongoDB transport subscriber dispatch overhead for the two ACK modes. Deliberately
/// in-process: integration/load tests cover the real MongoDB collection and HTTP-facing paths.
/// </summary>
[MemoryDiagnoser]
public class MongoDbAckDispatchBenchmarks
{
    private static readonly IReadOnlyDictionary<string, string> Headers =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["AR-Correlation-Id"] = "benchmark-correlation" };

    private MongoDbMessageDispatcher _awaiting = null!;
    private MongoDbMessageDispatcher _queued = null!;

    [Params(1, 8)]
    public int BackgroundWorkers;

    [GlobalSetup]
    public void Setup()
    {
        var options = new MongoDbAsyncResponseTransportOptions
        {
            HostShutdownTimeout = TimeSpan.FromSeconds(60)
        };

        _awaiting = new MongoDbMessageDispatcher(
            (_, _) => Task.CompletedTask,
            options,
            new MongoDbSubscriberOptions(),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        _queued = new MongoDbMessageDispatcher(
            (_, _) => Task.CompletedTask,
            options,
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: BackgroundWorkers,
                backgroundQueueCapacity: 16_384,
                backgroundDrainTimeout: TimeSpan.FromSeconds(30)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);
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
    public Task AckAfterEnqueue_Callback()
        => _queued.HandleAsync(Delivery(), CancellationToken.None);

    private static MongoDbTransportDelivery Delivery()
        => new(
            Guid.NewGuid(),
            "worker",
            "{}",
            Headers,
            Attempt: 1,
            () => ValueTask.CompletedTask,
            _ => ValueTask.CompletedTask,
            (_, _, _) => ValueTask.FromResult(true),
            () => ValueTask.FromResult(true));
}
