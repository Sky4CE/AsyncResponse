using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Concurrency correctness under parallel load: each waiter receives exactly its own response (no
/// cross-correlation leakage), the subscribe-before-send guarantee holds so no response is lost
/// under contention, and every worker job runs exactly once. Moderate scale, so they stay fast and
/// deterministic enough to gate every CI run; the benchmarks project's <c>stress</c> mode pushes the
/// same paths far harder for manual profiling.
/// </summary>
public class ConcurrencyTests
{
    [Fact]
    public async Task ManyConcurrentWaiters_EachReceivesItsOwnResponse()
    {
        await using var provider = Build();
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        const int count = 500;

        var results = await Task.WhenAll(Enumerable.Range(0, count).Select(i =>
            builder.For<OperationResult>()
                .WithTimeout(TimeSpan.FromSeconds(30))
                .WaitAsync(ctx => publisher.SetResponse(
                    new OperationResult { Status = OperationStatus.Completed, Message = $"r{i}" }, ctx.CorrelationId))));

        Assert.All(results, r => Assert.Equal(OperationStatus.Completed, r.Status));
        // Every distinct message comes back exactly once — no waiter saw another correlation's payload.
        Assert.Equal(
            Enumerable.Range(0, count).Select(i => $"r{i}").ToHashSet(),
            results.Select(r => r.Message!).ToHashSet());
    }

    [Fact]
    public async Task SubscribeBeforeSend_UnderParallelism_LosesNoResponses()
    {
        await using var provider = Build();
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        const int count = 500;

        // The trigger publishes immediately; a response lost before its subscription existed would
        // surface here as a TimeoutException rather than a completed payload.
        var statuses = await Task.WhenAll(Enumerable.Range(0, count).Select(async _ =>
        {
            var result = await builder.For<OperationResult>()
                .WithTimeout(TimeSpan.FromSeconds(15))
                .WaitAsync(ctx => publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, ctx.CorrelationId));
            return result.Status;
        }));

        Assert.All(statuses, s => Assert.Equal(OperationStatus.Completed, s));
    }

    [Fact]
    public async Task ManyConcurrentWorkers_AllExecuteExactlyOnce()
    {
        await using var provider = Build(withWorker: true);
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();
        var counter = provider.GetRequiredService<WorkCounter>();
        const int count = 500;
        counter.Reset(count);

        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var hostedService in hosted)
            await hostedService.StartAsync(CancellationToken.None);
        try
        {
            await Task.WhenAll(Enumerable.Range(0, count).Select(i =>
                builder.EnqueueWorkerAsync<IConcurrencyWorker>(w => w.RunAsync(i))));
            await counter.Completion.WaitAsync(TimeSpan.FromSeconds(30));
        }
        finally
        {
            foreach (var hostedService in hosted)
                await hostedService.StopAsync(CancellationToken.None);
        }

        Assert.Equal(count, counter.Executed);
        Assert.Equal(0, counter.Duplicates);
    }

    private static ServiceProvider Build(bool withWorker = false)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        var builder = services.AddAsyncResponse(options => options.Watchdog.Enabled = false).WithInMemoryChannel();
        if (withWorker)
        {
            builder.WithInMemoryTransport();
            services.AddSingleton<WorkCounter>();
            services.AddSingleton<IConcurrencyWorker, ConcurrencyWorker>();
        }

        return services.BuildServiceProvider();
    }
}

public interface IConcurrencyWorker
{
    Task RunAsync(int id);
}

public sealed class ConcurrencyWorker(WorkCounter counter) : IConcurrencyWorker
{
    public Task RunAsync(int id)
    {
        counter.Record(id);
        return Task.CompletedTask;
    }
}

/// <summary>Counts worker executions and flags any id that ran more than once.</summary>
public sealed class WorkCounter
{
    private int[] _seen = [];
    private int _executed;
    private int _duplicates;
    private int _expected;
    private TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int Executed => Volatile.Read(ref _executed);
    public int Duplicates => Volatile.Read(ref _duplicates);
    public Task Completion => _completion.Task;

    public void Reset(int expected)
    {
        _expected = expected;
        _seen = new int[expected];
        _executed = 0;
        _duplicates = 0;
        _completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public void Record(int id)
    {
        if ((uint)id < (uint)_seen.Length && Interlocked.Exchange(ref _seen[id], 1) == 1)
            Interlocked.Increment(ref _duplicates);
        if (Interlocked.Increment(ref _executed) == _expected)
            _completion.TrySetResult();
    }
}
