using AsyncResponse.Transports.GooglePubSub;
using BenchmarkDotNet.Attributes;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.Logging.Abstractions;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// Google Pub/Sub subscriber callback dispatch overhead for the two ACK modes. These are deliberately
/// in-process microbenchmarks: integration/load tests cover the emulator and HTTP-facing paths.
/// </summary>
[MemoryDiagnoser]
public class GooglePubSubAckDispatchBenchmarks
{
    private static readonly PubsubMessage Message = new()
    {
        MessageId = "benchmark-message",
        Data = ByteString.CopyFromUtf8("{}")
    };

    private GooglePubSubMessageDispatcher _awaiting = null!;
    private GooglePubSubMessageDispatcher _queued = null!;

    [Params(1, 8)]
    public int BackgroundWorkers;

    [GlobalSetup]
    public void Setup()
    {
        var transportOptions = new GooglePubSubAsyncResponseOptions();
        _awaiting = GooglePubSubMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            transportOptions,
            new GooglePubSubSubscriberOptions(),
            NullLogger.Instance,
            "benchmark-sub",
            GooglePubSubSubscriberRole.Worker);

        _queued = GooglePubSubMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            transportOptions,
            new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: BackgroundWorkers,
                backgroundQueueCapacity: 16_384,
                backgroundDrainTimeout: TimeSpan.FromSeconds(30)),
            NullLogger.Instance,
            "benchmark-sub",
            GooglePubSubSubscriberRole.Worker);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _queued.DisposeAsync().ConfigureAwait(false);
        await _awaiting.DisposeAsync().ConfigureAwait(false);
    }

    [Benchmark(Baseline = true)]
    public Task<SubscriberClient.Reply> AckAfterHandlerCompletes_Callback()
        => _awaiting.HandleAsync(Message, CancellationToken.None);

    [Benchmark]
    public Task<SubscriberClient.Reply> AckAfterEnqueue_Callback()
        => _queued.HandleAsync(Message, CancellationToken.None);
}
