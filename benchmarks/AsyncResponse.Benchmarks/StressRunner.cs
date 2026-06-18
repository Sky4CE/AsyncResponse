using System.Diagnostics;
using System.Text.Json;
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
/// <para>
/// With <c>--json &lt;prefix&gt;</c> it also writes <c>&lt;prefix&gt;.bigger.json</c> (throughput) and
/// <c>&lt;prefix&gt;.smaller.json</c> (latency + allocations) in github-action-benchmark's custom
/// format so the CI workflow can chart them over time.
/// </para>
/// </summary>
internal static class StressRunner
{
    private static readonly List<GhMetric> Series = [];

    public static async Task<int> RunAsync(string[] args)
    {
        var concurrency = GetInt(args, "--concurrency", 256);
        var count = GetInt(args, "--count", 50_000);
        var progress = GetInt(args, "--progress", 5);
        var fanout = Math.Max(1, GetInt(args, "--fanout", 4));
        var timeoutCount = Math.Max(1, GetInt(args, "--timeout-count", Math.Max(100, count / 20)));
        var timeoutMs = Math.Max(1, GetInt(args, "--timeout-ms", 25));
        var jsonPrefix = GetString(args, "--json");

        Console.WriteLine($"AsyncResponse stress — concurrency={concurrency:N0}, count={count:N0}, progressPerFlow={progress}, fanout={fanout}, timeoutCount={timeoutCount:N0}, timeoutMs={timeoutMs}");
        Console.WriteLine($"runtime={Environment.Version}, cores={Environment.ProcessorCount}, serverGC={System.Runtime.GCSettings.IsServerGC}");
        Console.WriteLine(new string('=', 78));

        var failures = 0;
        failures += await WaiterStorm(concurrency, count);
        failures += await ProgressStorm(concurrency, Math.Max(1, count / 10), progress);
        failures += await WorkerStorm(concurrency, count);
        failures += await RaceBurst(concurrency, count);
        failures += await RawIngressStorm(concurrency, count);
        failures += await SharedResponseFanoutStorm(concurrency, Math.Max(1, count / Math.Max(1, fanout)), fanout);
        failures += await ExceptionFanoutStorm(concurrency, Math.Max(1, count / Math.Max(1, fanout)), fanout);
        failures += await TimeoutStorm(Math.Min(concurrency, timeoutCount), timeoutCount, timeoutMs);
        failures += await DisposeCleanupStorm(concurrency, Math.Max(1, count / 10));
        failures += await ContextIsolationStorm(concurrency, Math.Max(1, count / 10));
        failures += await WatchdogScanStorm(Math.Min(count, 10_000), Math.Min(concurrency, 256));

        if (jsonPrefix is not null)
            WriteGitHubBenchmarkJson(jsonPrefix);

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
        metrics.Emit("waiter-storm");
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
        metrics.Emit("progress-storm");
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

            Series.Add(new GhMetric("worker-storm throughput", "jobs/s", count / sw.Elapsed.TotalSeconds, BiggerIsBetter: true));
            Series.Add(new GhMetric("worker-storm allocations", "B/op", alloc / (double)count, BiggerIsBetter: false));

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
        metrics.Emit("race-burst");
        return Check("race-burst", ("timeouts/faults", faults));
    }

    // 5) Raw JSON ingress storm. This exercises the transport-facing broker/webhook path:
    //    raw JSON -> untyped payload -> typed waiter conversion -> completion predicate.
    private static async Task<int> RawIngressStorm(int concurrency, int count)
    {
        using var provider = BuildProvider();
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();
        var crosstalk = 0;
        var faults = 0;

        var metrics = await Measure("raw-ingress-storm (JSON broker ingress -> typed waiter)", count, concurrency, async i =>
        {
            var expected = $"raw-{i}";
            var json = JsonSerializer.Serialize(new BenchPayload { Status = BenchStatus.Completed, Message = expected });

            try
            {
                var result = await builder.For<BenchPayload>()
                    .WithTimeout(TimeSpan.FromSeconds(30))
                    .WaitAsync(ctx => ingress.HandleResponseMessageAsync(json, ctx.CorrelationId));
                if (result.Message != expected) Interlocked.Increment(ref crosstalk);
            }
            catch
            {
                Interlocked.Increment(ref faults);
            }
        });

        metrics.Print();
        metrics.Emit("raw-ingress-storm");
        return Check("raw-ingress-storm", ("crosstalk", crosstalk), ("faults", faults));
    }

    // 6) Multiple active waiters attached to the same correlation id should all receive the same
    //    terminal response, then all subscriptions/recovery entries must clean up.
    private static async Task<int> SharedResponseFanoutStorm(int concurrency, int count, int fanout)
    {
        using var provider = BuildProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var missed = 0;
        var cleanupLeaks = 0;
        var faults = 0;

        var metrics = await Measure($"shared-response-fanout ({fanout} waiters per correlation id)", count, concurrency, async i =>
        {
            var correlationId = $"fanout-{i}-{Guid.NewGuid():N}";
            var expected = $"shared-{i}";
            var waiters = new IAsyncResponseWaiter<BenchPayload>[fanout];
            var tasks = new Task<BenchPayload>[fanout];

            try
            {
                for (var n = 0; n < fanout; n++)
                {
                    waiters[n] = await subscriber.CreateResponseWaiter<BenchPayload>(
                        correlationId,
                        timeout: TimeSpan.FromSeconds(30)).ConfigureAwait(false);
                    tasks[n] = waiters[n].ResponseTask;
                }

                await publisher.SetResponse(new BenchPayload { Status = BenchStatus.Completed, Message = expected }, correlationId)
                    .ConfigureAwait(false);

                var responses = await Task.WhenAll(tasks).ConfigureAwait(false);
                if (responses.Count(r => r.Message == expected) != fanout)
                    Interlocked.Increment(ref missed);
            }
            catch
            {
                Interlocked.Increment(ref faults);
            }
            finally
            {
                foreach (var waiter in waiters)
                {
                    if (waiter is not null)
                        await waiter.DisposeAsync().ConfigureAwait(false);
                }
            }

            if (await probe.CountActiveSubscribersAsync(correlationId).ConfigureAwait(false) != 0
                || await store.GetAsync(correlationId).ConfigureAwait(false) is not null)
            {
                Interlocked.Increment(ref cleanupLeaks);
            }
        });

        metrics.Print();
        metrics.Emit("shared-response-fanout");
        return Check("shared-response-fanout", ("missed", missed), ("cleanupLeaks", cleanupLeaks), ("faults", faults));
    }

    // 7) One technical failure must fan out to every active waiter on a shared correlation id.
    private static async Task<int> ExceptionFanoutStorm(int concurrency, int count, int fanout)
    {
        using var provider = BuildProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var missed = 0;
        var wrongException = 0;
        var faults = 0;

        var metrics = await Measure($"exception-fanout ({fanout} waiters faulted per correlation id)", count, concurrency, async i =>
        {
            var correlationId = $"fault-{i}-{Guid.NewGuid():N}";
            var waiters = new IAsyncResponseWaiter<BenchPayload>[fanout];
            var captures = new Task<string>[fanout];

            try
            {
                for (var n = 0; n < fanout; n++)
                {
                    waiters[n] = await subscriber.CreateResponseWaiter<BenchPayload>(
                        correlationId,
                        timeout: TimeSpan.FromSeconds(30)).ConfigureAwait(false);
                    captures[n] = CaptureFailureNameAsync(waiters[n].ResponseTask);
                }

                await publisher.SetException(new InvalidOperationException("shared technical failure"), correlationId)
                    .ConfigureAwait(false);

                var failures = await Task.WhenAll(captures).ConfigureAwait(false);
                if (failures.Count(static name => name == nameof(InvalidOperationException)) != fanout)
                    Interlocked.Increment(ref wrongException);
            }
            catch
            {
                Interlocked.Increment(ref faults);
            }
            finally
            {
                foreach (var waiter in waiters)
                {
                    if (waiter is not null)
                        await waiter.DisposeAsync().ConfigureAwait(false);
                }
            }
        });

        metrics.Print();
        metrics.Emit("exception-fanout");
        return Check("exception-fanout", ("missed", missed), ("wrongException", wrongException), ("faults", faults));
    }

    // 8) Short-timeout waiters must fault promptly and leave no active subscribers or recovery
    //    entries behind. This is intentionally a smaller scenario because timers dominate it.
    private static async Task<int> TimeoutStorm(int concurrency, int count, int timeoutMs)
    {
        using var provider = BuildProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var completed = 0;
        var wrongException = 0;
        var cleanupLeaks = 0;

        var metrics = await Measure($"timeout-storm ({timeoutMs}ms waiter timeout + cleanup)", count, concurrency, async i =>
        {
            var correlationId = $"timeout-{i}-{Guid.NewGuid():N}";
            var waiter = await subscriber.CreateResponseWaiter<BenchPayload>(
                correlationId,
                timeout: TimeSpan.FromMilliseconds(timeoutMs)).ConfigureAwait(false);

            try
            {
                await waiter.ResponseTask.ConfigureAwait(false);
                Interlocked.Increment(ref completed);
            }
            catch (TimeoutException)
            {
                // Expected.
            }
            catch
            {
                Interlocked.Increment(ref wrongException);
            }
            finally
            {
                await waiter.DisposeAsync().ConfigureAwait(false);
            }

            if (await probe.CountActiveSubscribersAsync(correlationId).ConfigureAwait(false) != 0
                || await store.GetAsync(correlationId).ConfigureAwait(false) is not null)
            {
                Interlocked.Increment(ref cleanupLeaks);
            }
        });

        metrics.Print();
        metrics.Emit("timeout-storm");
        return Check("timeout-storm", ("completed", completed), ("wrongException", wrongException), ("cleanupLeaks", cleanupLeaks));
    }

    // 9) Explicitly disposing an outstanding waiter should remove both the active subscription and
    //    the persisted recovery breadcrumb, even when no response ever arrives.
    private static async Task<int> DisposeCleanupStorm(int concurrency, int count)
    {
        using var provider = BuildProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var cleanupLeaks = 0;
        var faults = 0;

        var metrics = await Measure("dispose-cleanup-storm (manual waiter disposal)", count, concurrency, async i =>
        {
            var correlationId = $"dispose-{i}-{Guid.NewGuid():N}";
            try
            {
                var waiter = await subscriber.CreateResponseWaiter<BenchPayload>(
                    correlationId,
                    timeout: TimeSpan.FromMinutes(5)).ConfigureAwait(false);
                await waiter.DisposeAsync().ConfigureAwait(false);

                if (await probe.CountActiveSubscribersAsync(correlationId).ConfigureAwait(false) != 0
                    || await store.GetAsync(correlationId).ConfigureAwait(false) is not null)
                {
                    Interlocked.Increment(ref cleanupLeaks);
                }
            }
            catch
            {
                Interlocked.Increment(ref faults);
            }
        });

        metrics.Print();
        metrics.Emit("dispose-cleanup-storm");
        return Check("dispose-cleanup-storm", ("cleanupLeaks", cleanupLeaks), ("faults", faults));
    }

    // 10) Completion predicates must run under the waiter's captured ExecutionContext even when
    //     the publisher runs without the caller's AsyncLocal state.
    private static async Task<int> ContextIsolationStorm(int concurrency, int count)
    {
        using var provider = BuildProvider();
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var contextLeaks = 0;
        var faults = 0;

        var metrics = await Measure("context-isolation-storm (captured waiter ExecutionContext)", count, concurrency, async i =>
        {
            var expectedTrace = $"trace-{i}";
            StressAmbient.Trace.Value = expectedTrace;
            try
            {
                var result = await builder.For<BenchPayload>()
                    .WithTimeout(TimeSpan.FromSeconds(30))
                    .Until(payload =>
                    {
                        if (StressAmbient.Trace.Value != expectedTrace)
                            Interlocked.Increment(ref contextLeaks);
                        return payload.Status == BenchStatus.Completed;
                    })
                    .WaitAsync(ctx => PublishWithoutExecutionContextAsync(
                        publisher,
                        new BenchPayload { Status = BenchStatus.Completed, Message = expectedTrace },
                        ctx.CorrelationId));

                if (result.Message != expectedTrace)
                    Interlocked.Increment(ref contextLeaks);
            }
            catch
            {
                Interlocked.Increment(ref faults);
            }
            finally
            {
                StressAmbient.Trace.Value = null;
            }
        });

        metrics.Print();
        metrics.Emit("context-isolation-storm");
        return Check("context-isolation-storm", ("contextLeaks", contextLeaks), ("faults", faults));
    }

    // 11) Full recovery-state scan path: scanner enumeration, active-subscriber probing, watchdog
    //     report evaluation, and cleanup of both active and stale entries.
    private static async Task<int> WatchdogScanStorm(int entries, int activeWaiters)
    {
        using var provider = BuildProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var scanner = provider.GetRequiredService<IRecoveryStateScanner>();
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var waiters = new List<(string CorrelationId, IAsyncResponseWaiter<BenchPayload> Waiter)>(activeWaiters);
        var old = DateTime.UtcNow.AddHours(-2);
        var expectedStale = Math.Max(0, entries - activeWaiters);

        try
        {
            for (var i = 0; i < activeWaiters; i++)
            {
                var correlationId = $"watch-active-{i}-{Guid.NewGuid():N}";
                var waiter = await subscriber.CreateResponseWaiter<BenchPayload>(
                    correlationId,
                    timeout: TimeSpan.FromMinutes(5)).ConfigureAwait(false);
                waiters.Add((correlationId, waiter));
            }

            for (var i = activeWaiters; i < entries; i++)
            {
                var correlationId = $"watch-stale-{i}-{Guid.NewGuid():N}";
                await store.SaveAsync(
                    correlationId,
                    new RecoveryState
                    {
                        CorrelationId = correlationId,
                        PayloadTypeFullName = typeof(BenchPayload).FullName,
                        RegisteredAtUtc = old
                    },
                    TimeSpan.FromMinutes(30)).ConfigureAwait(false);
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var allocBefore = GC.GetTotalAllocatedBytes();
            var sw = Stopwatch.StartNew();

            var totalEntries = 0;
            var entriesWithActiveWaiter = 0;
            var unknownAgeEntries = 0;
            var staleEntries = new List<RecoveryStateObservation>(expectedStale);
            var utcNow = DateTime.UtcNow;
            await foreach (var state in scanner.ScanAsync().ConfigureAwait(false))
            {
                var active = string.IsNullOrWhiteSpace(state.CorrelationId)
                    ? -1
                    : await probe.CountActiveSubscribersAsync(state.CorrelationId).ConfigureAwait(false);

                totalEntries++;
                if (active > 0)
                {
                    entriesWithActiveWaiter++;
                    continue;
                }

                if (active != 0)
                    continue;

                if (state.RegisteredAtUtc is null)
                {
                    unknownAgeEntries++;
                    continue;
                }

                if (utcNow - state.RegisteredAtUtc.Value < TimeSpan.FromMinutes(30))
                    continue;

                staleEntries.Add(new RecoveryStateObservation(
                    state.CorrelationId,
                    state.RegisteredAtUtc,
                    active,
                    state.PayloadTypeFullName));
            }

            var report = new AsyncResponseWatchdogReport(
                totalEntries,
                entriesWithActiveWaiter,
                staleEntries,
                unknownAgeEntries);
            sw.Stop();
            var allocated = GC.GetTotalAllocatedBytes() - allocBefore;

            Console.WriteLine("  watchdog-scan-storm (scanner + active probe + stale evaluation)");
            Console.WriteLine($"    entries={entries:N0}  elapsed={sw.Elapsed.TotalMilliseconds:N0}ms  throughput={entries / sw.Elapsed.TotalSeconds:N0} entries/s");
            Console.WriteLine($"    total={report.TotalEntries:N0}  active={report.EntriesWithActiveWaiter:N0}  stale={report.StaleEntries.Count:N0}  alloc={allocated / 1024.0 / 1024.0:N1}MB ({allocated / (double)Math.Max(1, entries):N0} B/entry)");

            Series.Add(new GhMetric("watchdog-scan-storm throughput", "entries/s", entries / sw.Elapsed.TotalSeconds, BiggerIsBetter: true));
            Series.Add(new GhMetric("watchdog-scan-storm elapsed", "ms", sw.Elapsed.TotalMilliseconds, BiggerIsBetter: false));
            Series.Add(new GhMetric("watchdog-scan-storm allocations", "B/entry", allocated / (double)Math.Max(1, entries), BiggerIsBetter: false));

            return Check(
                "watchdog-scan-storm",
                ("wrongTotal", report.TotalEntries == entries ? 0 : 1),
                ("wrongActive", report.EntriesWithActiveWaiter == activeWaiters ? 0 : 1),
                ("wrongStale", report.StaleEntries.Count == expectedStale ? 0 : 1));
        }
        finally
        {
            foreach (var (_, waiter) in waiters)
                await waiter.DisposeAsync().ConfigureAwait(false);

            await foreach (var state in scanner.ScanAsync().ConfigureAwait(false))
            {
                if (!string.IsNullOrWhiteSpace(state.CorrelationId))
                    await store.TryDeleteAsync(state.CorrelationId).ConfigureAwait(false);
            }
        }
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
    // Keep the harness overhead bounded: one worker task per lane instead of one Task per operation.
    private static async Task ForEachAsync(int count, int concurrency, Func<int, Task> operation)
    {
        if (count <= 0)
            return;

        var workerCount = Math.Min(Math.Max(1, concurrency), count);
        var next = -1;
        var tasks = new Task[workerCount];
        for (var worker = 0; worker < tasks.Length; worker++)
            tasks[worker] = Task.Run(WorkerAsync);

        await Task.WhenAll(tasks).ConfigureAwait(false);

        async Task WorkerAsync()
        {
            while (true)
            {
                var index = Interlocked.Increment(ref next);
                if (index >= count)
                    return;

                await operation(index).ConfigureAwait(false);
            }
        }
    }

    private static async Task<string> CaptureFailureNameAsync(Task<BenchPayload> task)
    {
        try
        {
            await task.ConfigureAwait(false);
            return "completed";
        }
        catch (Exception ex)
        {
            return ex.GetType().Name;
        }
    }

    private static Task PublishWithoutExecutionContextAsync(
        IAsyncResponsePublisher publisher,
        BenchPayload payload,
        string correlationId)
    {
        Task publishTask;
        using (ExecutionContext.SuppressFlow())
        {
            publishTask = Task.Run(() => publisher.SetResponse(payload, correlationId));
        }

        return publishTask;
    }

    private static void WriteGitHubBenchmarkJson(string prefix)
    {
        var options = new JsonSerializerOptions { WriteIndented = true };
        var bigger = Series.Where(m => m.BiggerIsBetter).Select(m => new { name = m.Name, unit = m.Unit, value = m.Value }).ToArray();
        var smaller = Series.Where(m => !m.BiggerIsBetter).Select(m => new { name = m.Name, unit = m.Unit, value = m.Value }).ToArray();

        File.WriteAllText($"{prefix}.bigger.json", JsonSerializer.Serialize(bigger, options));
        File.WriteAllText($"{prefix}.smaller.json", JsonSerializer.Serialize(smaller, options));
        Console.WriteLine($"Wrote {prefix}.bigger.json ({bigger.Length} throughput metrics) and {prefix}.smaller.json ({smaller.Length} latency/alloc metrics).");
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

    private static string? GetString(string[] args, string name)
    {
        var index = Array.IndexOf(args, name);
        return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
    }

    /// <summary>One metric in github-action-benchmark's custom format, tagged with its better-direction.</summary>
    private sealed record GhMetric(string Name, string Unit, double Value, bool BiggerIsBetter);

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

        public double Throughput => Count / Elapsed.TotalSeconds;
        public double AllocatedPerOp => AllocatedBytes / (double)Count;

        public double P99
        {
            get
            {
                if (Latencies.Length == 0) return 0;
                var sorted = (double[])Latencies.Clone();
                Array.Sort(sorted);
                return Percentile(sorted, 99);
            }
        }

        public void Print()
        {
            Console.WriteLine($"  {Name}");
            Console.WriteLine($"    ops={Count:N0}  elapsed={Elapsed.TotalMilliseconds:N0}ms  throughput={Throughput:N0} ops/s");
            if (Latencies.Length > 0)
            {
                var sorted = (double[])Latencies.Clone();
                Array.Sort(sorted);
                Console.WriteLine($"    latency ms: p50={Percentile(sorted, 50):F3}  p95={Percentile(sorted, 95):F3}  p99={Percentile(sorted, 99):F3}  max={sorted[^1]:F3}");
            }

            Console.WriteLine($"    alloc={AllocatedBytes / 1024.0 / 1024.0:N1}MB ({AllocatedPerOp:N0} B/op)  GC g0/g1/g2={Gen0}/{Gen1}/{Gen2}  workingSet={WorkingSetBytes / 1024.0 / 1024.0:N0}MB");
        }

        // Appends this scenario's headline series to the github-action-benchmark output set.
        public void Emit(string scenario)
        {
            Series.Add(new GhMetric($"{scenario} throughput", "ops/s", Throughput, BiggerIsBetter: true));
            Series.Add(new GhMetric($"{scenario} p99 latency", "ms", P99, BiggerIsBetter: false));
            Series.Add(new GhMetric($"{scenario} allocations", "B/op", AllocatedPerOp, BiggerIsBetter: false));
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

    private static class StressAmbient
    {
        public static readonly AsyncLocal<string?> Trace = new();
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
