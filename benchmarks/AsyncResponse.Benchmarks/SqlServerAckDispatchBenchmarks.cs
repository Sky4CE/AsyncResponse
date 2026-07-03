using AsyncResponse.Transports.SqlServer;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// SQL Server transport subscriber dispatch overhead for the two ACK modes. Deliberately
/// in-process: integration/load tests cover the real SQL Server table and HTTP-facing paths.
/// </summary>
[MemoryDiagnoser]
public class SqlServerAckDispatchBenchmarks
{
    private static readonly IReadOnlyDictionary<string, string> Headers =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["AR-Correlation-Id"] = "benchmark-correlation" };

    private SqlServerMessageDispatcher _awaiting = null!;
    private SqlServerMessageDispatcher _queued = null!;

    [Params(1, 8)]
    public int BackgroundWorkers;

    [GlobalSetup]
    public void Setup()
    {
        var options = new SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = "Server=localhost;Database=asyncresponse_benchmarks;User ID=sa;Password=unused;TrustServerCertificate=True"
        };

        _awaiting = new SqlServerMessageDispatcher(
            (_, _) => Task.CompletedTask,
            options,
            new SqlServerSubscriberOptions(),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);

        _queued = new SqlServerMessageDispatcher(
            (_, _) => Task.CompletedTask,
            options,
            new SqlServerSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: BackgroundWorkers,
                backgroundQueueCapacity: 16_384,
                backgroundDrainTimeout: TimeSpan.FromSeconds(30)),
            NullLogger.Instance,
            SqlServerSubscriberRole.Worker);
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

    private static SqlServerTransportDelivery Delivery()
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
