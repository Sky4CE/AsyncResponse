using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// Per-operation cost of a full in-memory request/response round-trip: subscribe (+ persist recovery
/// state) → trigger publishes → consume + complete + clean up. The two variants compare the fluent
/// builder against the lower-level subscriber/publisher seam.
/// </summary>
[MemoryDiagnoser]
public class ChannelBenchmarks
{
    private ServiceProvider _provider = null!;
    private IAsyncResponseBuilder _builder = null!;
    private IAsyncResponsePublisher _publisher = null!;
    private IAsyncResponseSubscriber _subscriber = null!;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse(options => options.Watchdog.Enabled = false).WithInMemoryChannel();
        _provider = services.BuildServiceProvider();
        _builder = _provider.GetRequiredService<IAsyncResponseBuilder>();
        _publisher = _provider.GetRequiredService<IAsyncResponsePublisher>();
        _subscriber = _provider.GetRequiredService<IAsyncResponseSubscriber>();
    }

    [GlobalCleanup]
    public void Cleanup() => _provider.Dispose();

    [Benchmark(Baseline = true)]
    public async Task<BenchStatus> RoundTrip_ViaBuilder()
    {
        var result = await _builder
            .For<BenchPayload>()
            .WithTimeout(TimeSpan.FromSeconds(30))
            .WaitAsync(ctx => _publisher.SetResponse(new BenchPayload { Status = BenchStatus.Completed }, ctx.CorrelationId));
        return result.Status;
    }

    [Benchmark]
    public async Task<BenchStatus> RoundTrip_ViaSubscriber()
    {
        var correlationId = Guid.NewGuid().ToString();
        await using var waiter = await _subscriber.CreateResponseWaiter<BenchPayload>(correlationId, timeout: TimeSpan.FromSeconds(30));
        await _publisher.SetResponse(new BenchPayload { Status = BenchStatus.Completed }, correlationId);
        return (await waiter.ResponseTask).Status;
    }
}
