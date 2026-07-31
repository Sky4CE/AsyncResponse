using AsyncResponse.Transports.NATS;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// NATS JetStream subscriber dispatch overhead for the two ACK modes, alongside the Redis and RabbitMQ
/// equivalents. Deliberately in-process microbenchmarks: integration/load tests cover the real NATS
/// and HTTP-facing paths.
/// </summary>
[MemoryDiagnoser]
public class NatsAckDispatchBenchmarks
{
    private static readonly IReadOnlyDictionary<string, string> Headers =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["AR-Correlation-Id"] = "benchmark-correlation" };

    private NatsMessageDispatcher _awaiting = null!;
    private NatsMessageDispatcher _queued = null!;

    [Params(1, 8)]
    public int BackgroundWorkers;

    [GlobalSetup]
    public void Setup()
    {
        var jetStream = new BenchmarkNatsJetStreamTransport();
        var options = new NatsAsyncResponseTransportOptions
        {
            HostShutdownTimeout = TimeSpan.FromSeconds(60)
        };
        var schema = new NatsTransportSubjectSchema(options);

        _awaiting = new NatsMessageDispatcher(
            (_, _) => Task.CompletedTask,
            jetStream,
            options,
            new NatsSubscriberOptions(),
            schema,
            NullLogger.Instance,
            NatsSubscriberRole.Worker,
            "benchmark-consumer");

        _queued = new NatsMessageDispatcher(
            (_, _) => Task.CompletedTask,
            jetStream,
            options,
            new NatsSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: BackgroundWorkers,
                backgroundQueueCapacity: 16_384,
                backgroundDrainTimeout: TimeSpan.FromSeconds(30)),
            schema,
            NullLogger.Instance,
            NatsSubscriberRole.Worker,
            "benchmark-consumer");
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

    private static NatsJobDelivery Delivery()
        => new(
            "asyncresponse.transport.worker",
            "{}",
            Headers,
            NumDelivered: 1,
            () => ValueTask.CompletedTask,
            _ => ValueTask.CompletedTask,
            () => ValueTask.CompletedTask);
}
