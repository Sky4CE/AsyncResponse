using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AsyncResponse.Benchmarks;

/// <summary>Worker target for the worker-storm scenario.</summary>
public interface ICountingWorker
{
    Task CountAsync(int id);
}

/// <summary>
/// In-process load/stress harness for the in-memory channel + worker transport. It hammers the
/// library's own seams at high concurrency, <b>asserts correctness</b> (no lost/crossed responses,
/// no duplicate worker executions, no hangs), and reports throughput, latency percentiles,
/// allocations, GC counts and working set. Correctness violations make the process exit non-zero.
/// </summary>
internal static class StressRunner
{
    public static async Task<int> RunAsync(string[] args)
    {
        var concurrency = GetInt(args, "--concurrency", 256);
        var count = GetInt(args, "--count", 50_000);
        var progress = GetInt(args, "--progress", 5);

        Console.WriteLine($"AsyncResponse stress — concurrency={concurrency:N0}, count={count:N0}, progressPerFlow={progress}");
        Console.WriteLine($"runtime={Environment.Version}, cores={Environment.ProcessorCount}, serverGC={System.Runtime.GCSettings.IsServerGC}");
        Console.WriteLine(new string('=', 78));

        var failures = 0;
        failures += await WaiterStorm(concurrency, count);
        failures += await ProgressStorm(concurrency, Math.Max(1, count / 10), progress);
        failures += await WorkerStorm(concurrency, count);
        failures += await RaceBurst(concurrency, count);

        Console.WriteLine(new string('=', 78));
        if (failures == 0)
        {
            Console.WriteLine("PASS — all stress scenarios correct (no lost/crossed responses, no duplicate work, no hangs).");
            return 0;
        }

        Console.WriteLine($"FAIL — {failures} scenario(s) reported correctness violations.");
        return 1;
    }

    // 1) N independent waiters, each triggering a publish to its own correlation id. Each must
    //    receive exactly the payload its own trigger sent — anything else is cross-correlation leakage.
    private static async Task<int> WaiterStorm(int concurrency, int count)
    {
        using var provider = BuildProvider();
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var crosstalk = 0;
        var faults = 0;

        var metrics = await Measure("waiter-storm (isolation: each waiter gets its own response)", count, concurrency, async i =>
        {
            var expected = $"m{i}";
            try
            {
                var result = await builder.For<BenchPayload>()
                    .WithTimeout(TimeSpan.FromSeconds(30))
                    .WaitAsync(ctx => publisher.SetResponse(new BenchPayload { Status = BenchStatus.Completed, Message = expected }, ctx.CorrelationId));
                if (result.Message != expected) Interlocked.Increment(ref crosstalk);
            }
            catch
            {
                Interlocked.Increment(ref faults);
            }
        });

        metrics.Print();
        return Check("waiter-storm", ("crosstalk", crosstalk), ("faults", faults));
    }

    // 2) Per-flow message storm: K progress (Running) messages then a terminal (Completed),
    //    published from the trigger. The waiter's Until must consume progress and complete on terminal.
    private static async Task<int> ProgressStorm(int concurrency, int count, int progress)
    {
        using var provider = BuildProvider();
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var wrongTerminal = 0;
        var faults = 0;

        var metrics = await Measure($"progress-storm ({progress} progress msgs + terminal per flow)", count, concurrency, async i =>
        {
            try
            {
                var result = await builder.For<BenchPayload>()
                    .WithTimeout(TimeSpan.FromSeconds(30))
                    .Until(r => r.Status != BenchStatus.Running)
                    .WaitAsync(async ctx =>
                    {
                        for (var k = 0; k < progress; k++)
                            await publisher.SetResponse(new BenchPayload { Status = BenchStatus.Running, Message = $"p{k}" }, ctx.CorrelationId);
                        await publisher.SetResponse(new BenchPayload { Status = BenchStatus.Completed, Message = "done" }, ctx.CorrelationId);
                    });
                if (result.Status != BenchStatus.Completed) Interlocked.Increment(ref wrongTerminal);
            }
            catch
            {
                Interlocked.Increment(ref faults);
            }
        });

        metrics.Print();
        return Check("progress-storm", ("wrongTerminal", wrongTerminal), ("faults", faults));
    }

    // 3) N worker jobs enqueued concurrently; the in-memory host drains them. Every job must run
    //    exactly once. Measures end-to-end enqueue+drain throughput.
    private static async Task<int> WorkerStorm(int concurrency, int count)
    {
        using var provider = BuildProvider(withWorker: true);
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();
        var counter = provider.GetRequiredService<StressCounter>();
        counter.Reset(count);

        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var hostedService in hosted)
            await hostedService.StartAsync(CancellationToken.None);

        try
        {
            var allocBefore = GC.GetTotalAllocatedBytes();
            var sw = Stopwatch.StartNew();

            await ForEachAsync(count, concurrency, i => builder.EnqueueWorkerAsync<ICountingWorker>(w => w.CountAsync(i)));
            var drained = await counter.WaitAllAsync(TimeSpan.FromSeconds(120));

            sw.Stop();
            var alloc = GC.GetTotalAllocatedBytes() - allocBefore;

            Console.WriteLine("  worker-storm (fire-and-forget, exactly-once)");
            Console.WriteLine($"    jobs={count:N0}  elapsed={sw.Elapsed.TotalMilliseconds:N0}ms  throughput={count / sw.Elapsed.TotalSeconds:N0} jobs/s");
            Console.WriteLine($"    executed={counter.Executed:N0}  duplicates={counter.Duplicates}  drained={drained}  alloc={alloc / 1024.0 / 1024.0:N1}MB ({alloc / (double)count:N0} B/job)");

            return Check("worker-storm", ("notDrained", drained ? 0 : 1), ("missing", count - counter.Executed), ("duplicates", counter.Duplicates));
        }
        finally
        {
            foreach (var hostedService in hosted)
                await hostedService.StopAsync(CancellationToken.None);
        }
    }

    // 4) Subscribe-before-send race. A short timeout means a response lost before its subscription
    //    existed surfaces as a TimeoutException (counted) rather than a long hang.
    private static async Task<int> RaceBurst(int concurrency, int count)
    {
        using var provider = BuildProvider();
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var faults = 0;

        var metrics = await Measure("race-burst (subscribe-before-send under contention, 5s timeout)", count, concurrency, async i =>
        {
            try
            {
                await builder.For<BenchPayload>()
                    .WithTimeout(TimeSpan.FromSeconds(5))
                    .WaitAsync(ctx => publisher.SetResponse(new BenchPayload { Status = BenchStatus.Completed }, ctx.CorrelationId));
            }
            catch
            {
                Interlocked.Increment(ref faults);
            }
        });

        metrics.Print();
        return Check("race-burst", ("timeouts/faults", faults));
    }

    // -- infrastructure --------------------------------------------------------------------------

    private static ServiceProvider BuildProvider(bool withWorker = false)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        var builder = services.AddAsyncResponse(options => options.Watchdog.Enabled = false).WithInMemoryChannel();
        if (withWorker)
        {
            builder.WithInMemoryTransport();
            services.AddSingleton<StressCounter>();
            services.AddSingleton<ICountingWorker, CountingWorker>();
        }

        return services.BuildServiceProvider();
    }

    private static async Task<Metrics> Measure(string name, int count, int concurrency, Func<int, Task> operation)
    {
        var latencies = new double[count];
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var allocBefore = GC.GetTotalAllocatedBytes();
        var (g0, g1, g2) = (GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));
        var sw = Stopwatch.StartNew();

        await ForEachAsync(count, concurrency, async i =>
        {
            var start = Stopwatch.GetTimestamp();
            await operation(i);
            latencies[i] = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
        });

        sw.Stop();
        return new Metrics
        {
            Name = name,
            Count = count,
            Elapsed = sw.Elapsed,
            Latencies = latencies,
            AllocatedBytes = GC.GetTotalAllocatedBytes() - allocBefore,
            Gen0 = GC.CollectionCount(0) - g0,
            Gen1 = GC.CollectionCount(1) - g1,
            Gen2 = GC.CollectionCount(2) - g2,
            WorkingSetBytes = Process.GetCurrentProcess().WorkingSet64
        };
    }

    // Runs `operation` for indices [0, count) with at most `concurrency` in flight at once.
    private static async Task ForEachAsync(int count, int concurrency, Func<int, Task> operation)
    {
        using var throttle = new SemaphoreSlim(concurrency);
        var tasks = new Task[count];
        for (var i = 0; i < count; i++)
        {
            await throttle.WaitAsync().ConfigureAwait(false);
            var index = i;
            tasks[index] = Task.Run(async () =>
            {
                try { await operation(index).ConfigureAwait(false); }
                finally { throttle.Release(); }
            });
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private static int Check(string scenario, params (string Label, long Value)[] violations)
    {
        var bad = violations.Where(v => v.Value != 0).ToArray();
        if (bad.Length == 0)
        {
            Console.WriteLine($"    [ok] {scenario}: correct");
            return 0;
        }

        Console.WriteLine($"    [FAIL] {scenario}: {string.Join(", ", bad.Select(v => $"{v.Label}={v.Value}"))}");
        return 1;
    }

    private static int GetInt(string[] args, string name, int fallback)
    {
        var index = Array.IndexOf(args, name);
        return index >= 0 && index + 1 < args.Length && int.TryParse(args[index + 1], out var value) ? value : fallback;
    }

    private sealed class Metrics
    {
        public string Name = "";
        public int Count;
        public TimeSpan Elapsed;
        public double[] Latencies = [];
        public long AllocatedBytes;
        public int Gen0;
        public int Gen1;
        public int Gen2;
        public long WorkingSetBytes;

        public void Print()
        {
            Console.WriteLine($"  {Name}");
            Console.WriteLine($"    ops={Count:N0}  elapsed={Elapsed.TotalMilliseconds:N0}ms  throughput={Count / Elapsed.TotalSeconds:N0} ops/s");
            if (Latencies.Length > 0)
            {
                var sorted = (double[])Latencies.Clone();
                Array.Sort(sorted);
                Console.WriteLine($"    latency ms: p50={Percentile(sorted, 50):F3}  p95={Percentile(sorted, 95):F3}  p99={Percentile(sorted, 99):F3}  max={sorted[^1]:F3}");
            }

            Console.WriteLine($"    alloc={AllocatedBytes / 1024.0 / 1024.0:N1}MB ({AllocatedBytes / (double)Count:N0} B/op)  GC g0/g1/g2={Gen0}/{Gen1}/{Gen2}  workingSet={WorkingSetBytes / 1024.0 / 1024.0:N0}MB");
        }

        private static double Percentile(double[] sorted, double p)
        {
            var rank = (int)Math.Ceiling(p / 100.0 * sorted.Length) - 1;
            return sorted[Math.Clamp(rank, 0, sorted.Length - 1)];
        }
    }

    private sealed class CountingWorker(StressCounter counter) : ICountingWorker
    {
        public Task CountAsync(int id)
        {
            counter.Record(id);
            return Task.CompletedTask;
        }
    }

    // Tracks worker executions and detects duplicates via a per-id seen flag.
    internal sealed class StressCounter
    {
        private int[] _seen = [];
        private int _executed;
        private int _duplicates;
        private int _expected;
        private TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Executed => Volatile.Read(ref _executed);
        public int Duplicates => Volatile.Read(ref _duplicates);

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

        public async Task<bool> WaitAllAsync(TimeSpan timeout)
        {
            try
            {
                await _completion.Task.WaitAsync(timeout).ConfigureAwait(false);
                return true;
            }
            catch (TimeoutException)
            {
                return false;
            }
        }
    }
}
