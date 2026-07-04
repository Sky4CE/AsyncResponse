using AsyncResponse.Transports.SQS;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// AWS SQS transport subscriber dispatch overhead for the two ACK modes. Deliberately in-process:
/// integration/load tests cover LocalStack and the HTTP-facing paths.
/// </summary>
[MemoryDiagnoser]
public class SqsAckDispatchBenchmarks
{
    private static readonly IReadOnlyDictionary<string, string> Attributes =
        new Dictionary<string, string>(StringComparer.Ordinal) { ["correlationId"] = "benchmark-correlation" };

    private SqsMessageDispatcher _awaiting = null!;
    private SqsMessageDispatcher _queued = null!;

    [Params(1, 8)]
    public int BackgroundWorkers;

    [GlobalSetup]
    public void Setup()
    {
        var options = new SqsAsyncResponseOptions
        {
            HostShutdownTimeout = TimeSpan.FromSeconds(60)
        };

        _awaiting = SqsMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            options,
            new SqsSubscriberOptions(),
            NullLogger.Instance,
            "worker",
            SqsSubscriberRole.Worker);

        _queued = SqsMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            options,
            new SqsSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: BackgroundWorkers,
                backgroundQueueCapacity: 16_384,
                backgroundDrainTimeout: TimeSpan.FromSeconds(30)),
            NullLogger.Instance,
            "worker",
            SqsSubscriberRole.Worker);
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

    private static SqsTransportDelivery Delivery()
        => new(
            "https://sqs.us-east-1.amazonaws.com/000000000000/worker",
            "{}",
            Guid.NewGuid().ToString("N"),
            "receipt-handle",
            ReceiveCount: 1,
            Attributes,
            () => ValueTask.CompletedTask,
            _ => ValueTask.CompletedTask);
}
