using AsyncResponse.Transports.Redis;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// Redis Streams subscriber callback dispatch overhead for the two ACK modes. These are deliberately
/// in-process microbenchmarks: integration/load tests cover the real Redis and HTTP-facing paths.
/// </summary>
[MemoryDiagnoser]
public class RedisAckDispatchBenchmarks
{
    private static readonly StreamEntry Entry = new(
        "1-0",
        [new NameValueEntry("payload", "{}"), new NameValueEntry("correlationId", "benchmark-correlation")]);

    private RedisMessageDispatcher _awaiting = null!;
    private RedisMessageDispatcher _queued = null!;
    private BenchmarkRedisStreamDatabase _database = null!;

    [Params(1, 8)]
    public int BackgroundWorkers;

    [GlobalSetup]
    public void Setup()
    {
        _database = new BenchmarkRedisStreamDatabase();
        var transportOptions = new RedisAsyncResponseTransportOptions
        {
            HostShutdownTimeout = TimeSpan.FromSeconds(60)
        };

        _awaiting = RedisMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            _database,
            transportOptions,
            new RedisSubscriberOptions(),
            NullLogger.Instance,
            "benchmark-stream",
            "benchmark-group",
            RedisSubscriberRole.Worker);

        _queued = RedisMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            _database,
            transportOptions,
            new RedisSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: BackgroundWorkers,
                backgroundQueueCapacity: 16_384,
                backgroundDrainTimeout: TimeSpan.FromSeconds(30)),
            NullLogger.Instance,
            "benchmark-stream",
            "benchmark-group",
            RedisSubscriberRole.Worker);
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

    private static RedisStreamDelivery Delivery()
        => new(
            "benchmark-stream",
            "benchmark-group",
            Entry.Id,
            "{}",
            "benchmark-correlation",
            Attempt: 1,
            Entry);
}
