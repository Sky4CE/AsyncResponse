using AsyncResponse.Transports.AzureServiceBus;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// Azure Service Bus transport subscriber dispatch overhead for the two ACK modes. Deliberately
/// in-process: integration/load tests cover the real Service Bus emulator and HTTP-facing paths.
/// </summary>
[MemoryDiagnoser]
public class AzureServiceBusAckDispatchBenchmarks
{
    private static readonly IReadOnlyDictionary<string, object?> Properties =
        new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase) { ["correlationId"] = "benchmark-correlation" };

    private AzureServiceBusMessageDispatcher _awaiting = null!;
    private AzureServiceBusMessageDispatcher _queued = null!;

    [Params(1, 8)]
    public int BackgroundWorkers;

    [GlobalSetup]
    public void Setup()
    {
        var options = new AzureServiceBusAsyncResponseOptions
        {
            HostShutdownTimeout = TimeSpan.FromSeconds(60)
        };

        _awaiting = AzureServiceBusMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            options,
            new AzureServiceBusSubscriberOptions(),
            NullLogger.Instance,
            "worker",
            AzureServiceBusSubscriberRole.Worker);

        _queued = AzureServiceBusMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            options,
            new AzureServiceBusSubscriberOptions().UseAckAfterReceive(
                backgroundWorkerCount: BackgroundWorkers,
                backgroundQueueCapacity: 16_384,
                backgroundDrainTimeout: TimeSpan.FromSeconds(30)),
            NullLogger.Instance,
            "worker",
            AzureServiceBusSubscriberRole.Worker);
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

    private static AzureServiceBusTransportDelivery Delivery()
        => new(
            "worker",
            "{}",
            Guid.NewGuid().ToString("N"),
            "benchmark-correlation",
            SequenceNumber: 42,
            DeliveryCount: 1,
            Properties,
            () => ValueTask.CompletedTask,
            () => ValueTask.CompletedTask,
            (_, _) => ValueTask.CompletedTask,
            () => ValueTask.CompletedTask);
}
