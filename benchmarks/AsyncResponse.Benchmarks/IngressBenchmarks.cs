using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// Transport-ingress and channel fanout hot paths: raw JSON entering through the broker-facing
/// ingress, typed publisher delivery, shared-correlation fanout, and exception fanout.
/// </summary>
[MemoryDiagnoser]
public class IngressBenchmarks
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private ServiceProvider _provider = null!;
    private IAsyncResponsePublisher _publisher = null!;
    private IAsyncResponseSubscriber _subscriber = null!;
    private IAsyncResponseIngress _ingress = null!;
    private string _payloadJson = null!;

    [Params(1, 4, 16)]
    public int Fanout;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse(options => options.Watchdog.Enabled = false).WithInMemoryChannel();

        _provider = services.BuildServiceProvider();
        _publisher = _provider.GetRequiredService<IAsyncResponsePublisher>();
        _subscriber = _provider.GetRequiredService<IAsyncResponseSubscriber>();
        _ingress = _provider.GetRequiredService<IAsyncResponseIngress>();
        _payloadJson = JsonSerializer.Serialize(new BenchPayload
        {
            Status = BenchStatus.Completed,
            Message = "done"
        });
    }

    [GlobalCleanup]
    public void Cleanup() => _provider.Dispose();

    [Benchmark(Baseline = true)]
    public async Task<BenchStatus> TypedPublisher_RoundTrip()
    {
        var correlationId = AsyncResponseContext.GenerateCorrelationId();
        await using var waiter = await _subscriber.CreateResponseWaiter<BenchPayload>(correlationId, timeout: Timeout);

        await _publisher.SetResponse(new BenchPayload { Status = BenchStatus.Completed, Message = "typed" }, correlationId);

        return (await waiter.ResponseTask.ConfigureAwait(false)).Status;
    }

    [Benchmark]
    public async Task<BenchStatus> RawIngress_RoundTrip()
    {
        var correlationId = AsyncResponseContext.GenerateCorrelationId();
        await using var waiter = await _subscriber.CreateResponseWaiter<BenchPayload>(correlationId, timeout: Timeout);

        await _ingress.HandleResponseMessageAsync(_payloadJson, correlationId).ConfigureAwait(false);

        return (await waiter.ResponseTask.ConfigureAwait(false)).Status;
    }

    [Benchmark]
    public async Task<int> TypedPublisher_Fanout()
    {
        var waiters = await CreateWaiters(Fanout).ConfigureAwait(false);
        try
        {
            await _publisher.SetResponse(new BenchPayload { Status = BenchStatus.Completed, Message = "fanout" }, waiters.CorrelationId)
                .ConfigureAwait(false);

            var completed = await Task.WhenAll(waiters.ResponseTasks).ConfigureAwait(false);
            return completed.Count(payload => payload.Status == BenchStatus.Completed);
        }
        finally
        {
            await waiters.DisposeAsync().ConfigureAwait(false);
        }
    }

    [Benchmark]
    public async Task<int> Exception_Fanout()
    {
        var waiters = await CreateWaiters(Fanout).ConfigureAwait(false);
        try
        {
            var faults = waiters.ResponseTasks.Select(CaptureFaultAsync).ToArray();

            await _publisher.SetException(new InvalidOperationException("benchmark failure"), waiters.CorrelationId)
                .ConfigureAwait(false);

            return (await Task.WhenAll(faults).ConfigureAwait(false)).Count(faulted => faulted);
        }
        finally
        {
            await waiters.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task<WaiterSet> CreateWaiters(int count)
    {
        var correlationId = AsyncResponseContext.GenerateCorrelationId();
        var waiters = new IAsyncResponseWaiter<BenchPayload>[count];
        var tasks = new Task<BenchPayload>[count];

        for (var i = 0; i < count; i++)
        {
            waiters[i] = await _subscriber.CreateResponseWaiter<BenchPayload>(correlationId, timeout: Timeout)
                .ConfigureAwait(false);
            tasks[i] = waiters[i].ResponseTask;
        }

        return new WaiterSet(correlationId, waiters, tasks);
    }

    private static async Task<bool> CaptureFaultAsync(Task<BenchPayload> task)
    {
        try
        {
            await task.ConfigureAwait(false);
            return false;
        }
        catch (InvalidOperationException)
        {
            return true;
        }
    }

    private sealed class WaiterSet(
        string correlationId,
        IAsyncResponseWaiter<BenchPayload>[] waiters,
        Task<BenchPayload>[] responseTasks) : IAsyncDisposable
    {
        public string CorrelationId { get; } = correlationId;
        public Task<BenchPayload>[] ResponseTasks { get; } = responseTasks;

        public async ValueTask DisposeAsync()
        {
            foreach (var waiter in waiters)
                await waiter.DisposeAsync().ConfigureAwait(false);
        }
    }
}
