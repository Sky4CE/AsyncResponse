using AsyncResponse.Transports.Kafka;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// Kafka subscriber callback dispatch overhead for the two ACK modes. These are deliberately
/// in-process microbenchmarks: integration/load tests cover the real broker and HTTP-facing paths.
/// </summary>
[MemoryDiagnoser]
public class KafkaAckDispatchBenchmarks
{
    private static readonly KafkaTransportHeader[] Headers =
        [KafkaTransportHeader.Utf8("correlationId", "benchmark-correlation")];

    private KafkaMessageDispatcher _awaiting = null!;
    private KafkaMessageDispatcher _queued = null!;
    private BenchmarkKafkaConsumerClient _consumer = null!;
    private BenchmarkKafkaProducerClient _producer = null!;
    private long _offset;

    [Params(1, 8)]
    public int BackgroundWorkers;

    [GlobalSetup]
    public void Setup()
    {
        _consumer = new BenchmarkKafkaConsumerClient();
        _producer = new BenchmarkKafkaProducerClient();
        var transportOptions = new KafkaAsyncResponseTransportOptions
        {
            BootstrapServers = "localhost:9092",
            HostShutdownTimeout = TimeSpan.FromSeconds(60)
        };

        _awaiting = KafkaMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            _consumer,
            _producer,
            transportOptions,
            new KafkaSubscriberOptions(),
            NullLogger.Instance,
            "benchmark-topic",
            "benchmark-group",
            KafkaSubscriberRole.Worker);

        _queued = KafkaMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            _consumer,
            _producer,
            transportOptions,
            new KafkaSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: BackgroundWorkers,
                backgroundQueueCapacity: 16_384,
                backgroundDrainTimeout: TimeSpan.FromSeconds(30)),
            NullLogger.Instance,
            "benchmark-topic",
            "benchmark-group",
            KafkaSubscriberRole.Worker);
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

    private KafkaDelivery Delivery()
        => new(
            "benchmark-topic",
            Partition: 0,
            Interlocked.Increment(ref _offset),
            "{}",
            "benchmark-correlation",
            Headers);
}
