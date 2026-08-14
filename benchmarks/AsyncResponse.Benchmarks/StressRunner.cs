using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using AsyncResponse.Transports.AzureServiceBus;
using AsyncResponse.Transports.GooglePubSub;
using AsyncResponse.Transports.Kafka;
using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.NATS;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.RabbitMQ;
using AsyncResponse.Transports.Redis;
using AsyncResponse.Transports.SQS;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using StackExchange.Redis;

namespace AsyncResponse.Benchmarks;

/// <summary>Worker target for the worker-storm scenario.</summary>
public interface ICountingWorker
{
    Task CountAsync(int id);
}

/// <summary>
/// Console logger for scenarios where any error is a scenario failure: echoes <c>Error</c> and
/// above (capped), drops the rest. The stress harness otherwise runs on <see cref="NullLogger{T}"/>,
/// which hides exactly the diagnostics a broken scenario needs.
/// </summary>
internal static class StressErrorLogger
{
    internal const int MaxEchoed = 5;
    private static int _echoed;

    public static void Reset() => Interlocked.Exchange(ref _echoed, 0);

    public static void Echo(string category, LogLevel level, string message, Exception? exception)
    {
        var index = Interlocked.Increment(ref _echoed);
        if (index > MaxEchoed)
            return;

        Console.WriteLine($"    [{level.ToString().ToLowerInvariant()}] {category}: {message}");
        if (exception is not null)
            Console.WriteLine($"      {exception.GetType().Name}: {exception.Message}");
        if (index == MaxEchoed)
            Console.WriteLine($"    [warn] further errors suppressed (cap {MaxEchoed}).");
    }
}

/// <inheritdoc cref="StressErrorLogger" />
internal sealed class StressErrorLogger<T> : ILogger<T>
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Error;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (logLevel >= LogLevel.Error)
            StressErrorLogger.Echo(typeof(T).Name, logLevel, formatter(state, exception), exception);
    }
}

/// <summary>Awaited-step payload for the durable-flow storm.</summary>
internal sealed class FlowStormPayload : IAsyncResponsePayload
{
    public bool Done { get; set; }

    /// <summary>
    /// Required on every payload a durable flow awaits — flows register lost-subscriber recovery
    /// callbacks on every channel, and waiter creation fails fast without this override. The
    /// mapping mirrors the flow's <c>until</c> predicate's other axis: a not-done payload is a
    /// progress checkpoint that must NOT consume the registration the terminal one still needs.
    /// </summary>
    public RecoveryAction OnRecovery() => Done ? RecoveryAction.Resume : RecoveryAction.KeepWaiting;
}

/// <summary>Input for the durable-flow storm flow (persisted with the flow state).</summary>
internal sealed record FlowStormInput(int Id);

/// <summary>Shared counters and trigger queue for the durable-flow storm.</summary>
internal sealed class FlowStormRig
{
    private readonly Channel<string> _triggers = Channel.CreateUnbounded<string>();

    public StressRunner.StressCounter Prepare { get; } = new();
    public StressRunner.StressCounter Transform { get; } = new();
    public StressRunner.StressCounter Finalize { get; } = new();
    public ChannelReader<string> Triggers => _triggers.Reader;

    public Task EnqueueTriggerAsync(string correlationId)
    {
        _triggers.Writer.TryWrite(correlationId);
        return Task.CompletedTask;
    }
}

/// <summary>Five-step checkpointed flow (local → awaited → local → awaited → local) for the storm.</summary>
internal sealed class StressFlow(FlowStormRig _rig) : IDurableFlow<FlowStormInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, FlowStormInput input)
    {
        await flow.StepAsync("prepare", () =>
        {
            _rig.Prepare.Record(input.Id);
            return Task.CompletedTask;
        });

        await flow.AwaitStepAsync<FlowStormPayload>(
            "remote-a",
            trigger: _rig.EnqueueTriggerAsync,
            until: payload => payload.Done,
            timeout: TimeSpan.FromSeconds(120));

        await flow.StepAsync("transform", () =>
        {
            _rig.Transform.Record(input.Id);
            return Task.CompletedTask;
        });

        await flow.AwaitStepAsync<FlowStormPayload>(
            "remote-b",
            trigger: _rig.EnqueueTriggerAsync,
            until: payload => payload.Done,
            timeout: TimeSpan.FromSeconds(120));

        await flow.StepAsync("finalize", () =>
        {
            _rig.Finalize.Record(input.Id);
            return Task.CompletedTask;
        });
    }
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
        failures += await GooglePubSubAckAfterEnqueueDispatchStorm(concurrency, Math.Max(1, count / 10));
        failures += await RabbitMqAckAfterEnqueueDispatchStorm(concurrency, Math.Max(1, count / 10));
        failures += await RedisAckAfterEnqueueDispatchStorm(concurrency, Math.Max(1, count / 10));
        failures += await NatsAckAfterEnqueueDispatchStorm(concurrency, Math.Max(1, count / 10));
        failures += await PostgreSqlAckAfterEnqueueDispatchStorm(concurrency, Math.Max(1, count / 10));
        failures += await SqlServerAckAfterEnqueueDispatchStorm(concurrency, Math.Max(1, count / 10));
        failures += await MongoDbAckAfterEnqueueDispatchStorm(concurrency, Math.Max(1, count / 10));
        failures += await AzureServiceBusAckAfterEnqueueDispatchStorm(concurrency, Math.Max(1, count / 10));
        failures += await SqsAckAfterEnqueueDispatchStorm(concurrency, Math.Max(1, count / 10));
        failures += await KafkaAckAfterEnqueueDispatchStorm(concurrency, Math.Max(1, count / 10));
        failures += await RaceBurst(concurrency, count);
        failures += await RawIngressStorm(concurrency, count);
        failures += await SharedResponseFanoutStorm(concurrency, Math.Max(1, count / Math.Max(1, fanout)), fanout);
        failures += await ExceptionFanoutStorm(concurrency, Math.Max(1, count / Math.Max(1, fanout)), fanout);
        failures += await TimeoutStorm(Math.Min(concurrency, timeoutCount), timeoutCount, timeoutMs);
        failures += await DisposeCleanupStorm(concurrency, Math.Max(1, count / 10));
        failures += await ContextIsolationStorm(concurrency, Math.Max(1, count / 10));
        failures += await WatchdogScanStorm(Math.Min(count, 10_000), Math.Min(concurrency, 256));
        failures += await DurableFlowStorm(concurrency, Math.Max(1, count / 20));

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
            await StopBoundedAsync(hosted);
        }
    }

    /// <summary>
    /// Stops hosted services with a drain budget. The in-memory worker host drains what it
    /// accepted, retrying failures with backoff — correct in production, but when a scenario is
    /// broken every job burns its full retry budget on a single worker, and an unbounded stop turns
    /// a failing scenario into an hours-long hang instead of a report. Past the budget the scenario
    /// has already recorded its verdict, so the drain is abandoned loudly.
    /// </summary>
    private static async Task StopBoundedAsync(IEnumerable<IHostedService> hosted)
    {
        using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        foreach (var hostedService in hosted)
        {
            try
            {
                await hostedService.StopAsync(stopCts.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"    [warn] {hostedService.GetType().Name} did not stop within 30s; abandoning the drain.");
            }
        }
    }

    // 3b) Google Pub/Sub ACK-after-enqueue dispatch storm. This bypasses the emulator and hammers the
    //     subscriber callback dispatcher directly: every ACKed message must be processed exactly once.
    private static async Task<int> GooglePubSubAckAfterEnqueueDispatchStorm(int concurrency, int count)
    {
        var workerCount = Math.Clamp(Environment.ProcessorCount, 1, 16);
        var processed = 0;
        var nacks = 0;
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = GooglePubSubMessageDispatcher.Create(
            (_, _) =>
            {
                if (Interlocked.Increment(ref processed) == count)
                    allProcessed.TrySetResult();
                return Task.CompletedTask;
            },
            new GooglePubSubAsyncResponseOptions
            {
                HostShutdownTimeout = TimeSpan.FromSeconds(90)
            },
            new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: workerCount,
                backgroundQueueCapacity: Math.Max(count, concurrency),
                backgroundDrainTimeout: TimeSpan.FromSeconds(60)),
            NullLogger.Instance,
            "stress-worker-sub",
            GooglePubSubSubscriberRole.Worker);

        var metrics = await Measure("google-pubsub-ack-after-enqueue-dispatch-storm", count, concurrency, async i =>
        {
            var reply = await dispatcher.HandleAsync(new PubsubMessage
            {
                MessageId = $"stress-{i}",
                Data = ByteString.CopyFromUtf8("{}")
            }, CancellationToken.None).ConfigureAwait(false);

            if (reply is not SubscriberClient.Reply.Ack)
                Interlocked.Increment(ref nacks);
        });

        var drained = false;
        try
        {
            await allProcessed.Task.WaitAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
            drained = true;
        }
        catch (TimeoutException)
        {
        }

        metrics.Print();
        metrics.Emit("google-pubsub-ack-after-enqueue-dispatch-storm");
        return Check(
            "google-pubsub-ack-after-enqueue-dispatch-storm",
            ("nacks", nacks),
            ("notDrained", drained ? 0 : 1),
            ("missing", count - Volatile.Read(ref processed)));
    }

    // 3c) RabbitMQ ACK-after-enqueue dispatch storm. This bypasses the broker and hammers the
    //     subscriber callback dispatcher directly: every ACKed delivery must be processed exactly once.
    private static async Task<int> RabbitMqAckAfterEnqueueDispatchStorm(int concurrency, int count)
    {
        var workerCount = Math.Clamp(Environment.ProcessorCount, 1, 16);
        var seen = new int[count];
        var processed = 0;
        var duplicates = 0;
        var outOfRange = 0;
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var channel = new StressRabbitMqChannel();
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            (delivery, _) =>
            {
                var index = (long)delivery.DeliveryTag - 1;
                if ((ulong)index >= (ulong)seen.Length)
                {
                    Interlocked.Increment(ref outOfRange);
                }
                else if (Interlocked.Exchange(ref seen[(int)index], 1) == 1)
                {
                    Interlocked.Increment(ref duplicates);
                }
                else if (Interlocked.Increment(ref processed) == count)
                {
                    allProcessed.TrySetResult();
                }

                return Task.CompletedTask;
            },
            new RabbitMqAsyncResponseOptions
            {
                HostShutdownTimeout = TimeSpan.FromSeconds(90)
            },
            new RabbitMqSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: workerCount,
                backgroundQueueCapacity: Math.Max(count, concurrency),
                backgroundDrainTimeout: TimeSpan.FromSeconds(60)),
            NullLogger.Instance,
            "stress-worker-queue",
            RabbitMqSubscriberRole.Worker);

        var metrics = await Measure("rabbitmq-ack-after-enqueue-dispatch-storm", count, concurrency, async i =>
        {
            var delivery = new RabbitMqDelivery(
                ConsumerTag: "stress-consumer",
                DeliveryTag: (ulong)i + 1,
                Redelivered: false,
                Exchange: "stress-exchange",
                RoutingKey: "stress-worker-route",
                BasicProperties: new BasicProperties
                {
                    MessageId = $"stress-{i}",
                    ContentType = "application/json"
                },
                Body: System.Text.Encoding.UTF8.GetBytes("{}"),
                CancellationToken: CancellationToken.None);

            await dispatcher.HandleAsync(delivery, channel, CancellationToken.None).ConfigureAwait(false);
        });

        var drained = false;
        try
        {
            await allProcessed.Task.WaitAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
            drained = true;
        }
        catch (TimeoutException)
        {
        }

        metrics.Print();
        metrics.Emit("rabbitmq-ack-after-enqueue-dispatch-storm");
        return Check(
            "rabbitmq-ack-after-enqueue-dispatch-storm",
            ("nacks", channel.Nacks),
            ("ackMismatch", Math.Abs(count - channel.Acks)),
            ("duplicates", duplicates),
            ("outOfRange", outOfRange),
            ("notDrained", drained ? 0 : 1),
            ("missing", count - Volatile.Read(ref processed)));
    }

    // 3d) Redis Streams ACK-after-enqueue dispatch storm. This bypasses a live Redis server and
    //     hammers the subscriber callback dispatcher directly: every ACKed stream entry must be
    //     processed exactly once.
    private static async Task<int> RedisAckAfterEnqueueDispatchStorm(int concurrency, int count)
    {
        var workerCount = Math.Clamp(Environment.ProcessorCount, 1, 16);
        var database = new BenchmarkRedisStreamDatabase();
        var seen = new int[count];
        var processed = 0;
        var duplicates = 0;
        var outOfRange = 0;
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = RedisMessageDispatcher.Create(
            (delivery, _) =>
            {
                var text = delivery.MessageId.ToString();
                var separator = text.IndexOf('-', StringComparison.Ordinal);
                var index = separator > 0 && int.TryParse(text[..separator], out var parsed)
                    ? parsed - 1
                    : -1;
                if ((uint)index >= (uint)seen.Length)
                {
                    Interlocked.Increment(ref outOfRange);
                }
                else if (Interlocked.Exchange(ref seen[index], 1) == 1)
                {
                    Interlocked.Increment(ref duplicates);
                }
                else if (Interlocked.Increment(ref processed) == count)
                {
                    allProcessed.TrySetResult();
                }

                return Task.CompletedTask;
            },
            database,
            new RedisAsyncResponseTransportOptions
            {
                HostShutdownTimeout = TimeSpan.FromSeconds(90)
            },
            new RedisSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: workerCount,
                backgroundQueueCapacity: Math.Max(count, concurrency),
                backgroundDrainTimeout: TimeSpan.FromSeconds(60)),
            NullLogger.Instance,
            "stress-worker-stream",
            "stress-worker-group",
            RedisSubscriberRole.Worker);

        var metrics = await Measure("redis-ack-after-enqueue-dispatch-storm", count, concurrency, async i =>
        {
            var id = $"{i + 1}-0";
            await dispatcher.HandleAsync(new RedisStreamDelivery(
                "stress-worker-stream",
                "stress-worker-group",
                id,
                "{}",
                $"stress-{i}",
                Attempt: 1,
                new StreamEntry(id, [new NameValueEntry("payload", "{}"), new NameValueEntry("correlationId", $"stress-{i}")])),
                CancellationToken.None).ConfigureAwait(false);
        });

        var drained = false;
        try
        {
            await allProcessed.Task.WaitAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
            drained = true;
        }
        catch (TimeoutException)
        {
        }

        metrics.Print();
        metrics.Emit("redis-ack-after-enqueue-dispatch-storm");
        return Check(
            "redis-ack-after-enqueue-dispatch-storm",
            ("ackMismatch", Math.Abs(count - database.Acks)),
            ("duplicates", duplicates),
            ("outOfRange", outOfRange),
            ("notDrained", drained ? 0 : 1),
            ("missing", count - Volatile.Read(ref processed)));
    }

    // 3e) NATS JetStream ACK-after-enqueue dispatch storm. This bypasses a live NATS server and
    //     hammers the subscriber callback dispatcher directly: every ACKed delivery must be processed
    //     exactly once.
    private static async Task<int> NatsAckAfterEnqueueDispatchStorm(int concurrency, int count)
    {
        var workerCount = Math.Clamp(Environment.ProcessorCount, 1, 16);
        var jetStream = new BenchmarkNatsJetStreamTransport();
        var options = new NatsAsyncResponseTransportOptions
        {
            HostShutdownTimeout = TimeSpan.FromSeconds(90)
        };
        var schema = new NatsTransportSubjectSchema(options);
        var seen = new int[count];
        var processed = 0;
        var duplicates = 0;
        var outOfRange = 0;
        var acks = 0;
        var naks = 0;
        var terms = 0;
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var dispatcher = new NatsMessageDispatcher(
            (delivery, _) =>
            {
                var separator = delivery.Payload.IndexOf(':', StringComparison.Ordinal);
                var index = separator >= 0 && int.TryParse(delivery.Payload[..separator], out var parsed)
                    ? parsed
                    : -1;
                if ((uint)index >= (uint)seen.Length)
                {
                    Interlocked.Increment(ref outOfRange);
                }
                else if (Interlocked.Exchange(ref seen[index], 1) == 1)
                {
                    Interlocked.Increment(ref duplicates);
                }
                else if (Interlocked.Increment(ref processed) == count)
                {
                    allProcessed.TrySetResult();
                }

                return Task.CompletedTask;
            },
            jetStream,
            options,
            new NatsSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: workerCount,
                backgroundQueueCapacity: Math.Max(count, concurrency),
                backgroundDrainTimeout: TimeSpan.FromSeconds(60)),
            schema,
            NullLogger.Instance,
            NatsSubscriberRole.Worker,
            "stress-worker-consumer");

        var metrics = await Measure("nats-ack-after-receive-dispatch-storm", count, concurrency, async i =>
        {
            await dispatcher.HandleAsync(new NatsJobDelivery(
                "asyncresponse.transport.worker",
                $"{i}:{{}}",
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["AR-Correlation-Id"] = $"stress-{i}"
                },
                NumDelivered: 1,
                () => { Interlocked.Increment(ref acks); return ValueTask.CompletedTask; },
                _ => { Interlocked.Increment(ref naks); return ValueTask.CompletedTask; },
                () => { Interlocked.Increment(ref terms); return ValueTask.CompletedTask; }),
                CancellationToken.None).ConfigureAwait(false);
        });

        var drained = false;
        try
        {
            await allProcessed.Task.WaitAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
            drained = true;
        }
        catch (TimeoutException)
        {
        }

        metrics.Print();
        metrics.Emit("nats-ack-after-receive-dispatch-storm");
        return Check(
            "nats-ack-after-receive-dispatch-storm",
            ("ackMismatch", Math.Abs(count - Volatile.Read(ref acks))),
            ("naks", Volatile.Read(ref naks)),
            ("terms", Volatile.Read(ref terms)),
            ("duplicates", duplicates),
            ("outOfRange", outOfRange),
            ("notDrained", drained ? 0 : 1),
            ("missing", count - Volatile.Read(ref processed)));
    }

    // 3f) PostgreSQL ACK-after-enqueue dispatch storm. This bypasses a live PostgreSQL server and
    //     hammers the queue-row callback dispatcher directly: every ACKed row must be processed once.
    private static async Task<int> PostgreSqlAckAfterEnqueueDispatchStorm(int concurrency, int count)
    {
        var workerCount = Math.Clamp(Environment.ProcessorCount, 1, 16);
        var options = new PostgreSqlAsyncResponseTransportOptions
        {
            HostShutdownTimeout = TimeSpan.FromSeconds(90)
        };
        var seen = new int[count];
        var processed = 0;
        var duplicates = 0;
        var outOfRange = 0;
        var acks = 0;
        var naks = 0;
        var deadLetters = 0;
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var dispatcher = new PostgreSqlMessageDispatcher(
            (delivery, _) =>
            {
                var separator = delivery.Payload.IndexOf(':', StringComparison.Ordinal);
                var index = separator >= 0 && int.TryParse(delivery.Payload[..separator], out var parsed)
                    ? parsed
                    : -1;
                if ((uint)index >= (uint)seen.Length)
                {
                    Interlocked.Increment(ref outOfRange);
                }
                else if (Interlocked.Exchange(ref seen[index], 1) == 1)
                {
                    Interlocked.Increment(ref duplicates);
                }
                else if (Interlocked.Increment(ref processed) == count)
                {
                    allProcessed.TrySetResult();
                }

                return Task.CompletedTask;
            },
            options,
            new PostgreSqlSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: workerCount,
                backgroundQueueCapacity: Math.Max(count, concurrency),
                backgroundDrainTimeout: TimeSpan.FromSeconds(60)),
            NullLogger.Instance,
            PostgreSqlSubscriberRole.Worker);

        var metrics = await Measure("postgresql-ack-after-receive-dispatch-storm", count, concurrency, async i =>
        {
            await dispatcher.HandleAsync(new PostgreSqlTransportDelivery(
                Guid.NewGuid(),
                "stress-worker-queue",
                $"{i}:{{}}",
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    [options.CorrelationIdHeader] = $"stress-{i}"
                },
                Attempt: 1,
                () => { Interlocked.Increment(ref acks); return ValueTask.CompletedTask; },
                _ => { Interlocked.Increment(ref naks); return ValueTask.CompletedTask; },
                (_, _, _) => { Interlocked.Increment(ref deadLetters); return ValueTask.FromResult(true); },
                () => ValueTask.FromResult(true)),
                CancellationToken.None).ConfigureAwait(false);
        });

        var drained = false;
        try
        {
            await allProcessed.Task.WaitAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
            drained = true;
        }
        catch (TimeoutException)
        {
        }

        metrics.Print();
        metrics.Emit("postgresql-ack-after-receive-dispatch-storm");
        return Check(
            "postgresql-ack-after-receive-dispatch-storm",
            ("ackMismatch", Math.Abs(count - Volatile.Read(ref acks))),
            ("naks", Volatile.Read(ref naks)),
            ("deadLetters", Volatile.Read(ref deadLetters)),
            ("duplicates", duplicates),
            ("outOfRange", outOfRange),
            ("notDrained", drained ? 0 : 1),
            ("missing", count - Volatile.Read(ref processed)));
    }

    // 3f2) SQL Server ACK-after-enqueue dispatch storm. This bypasses a live SQL Server and
    //      hammers the queue-row callback dispatcher directly: every ACKed row must be processed once.
    private static async Task<int> SqlServerAckAfterEnqueueDispatchStorm(int concurrency, int count)
    {
        var workerCount = Math.Clamp(Environment.ProcessorCount, 1, 16);
        var options = new AsyncResponse.Transports.SqlServer.SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = "Server=localhost;Database=asyncresponse_stress;User ID=sa;Password=unused;TrustServerCertificate=True",
            HostShutdownTimeout = TimeSpan.FromSeconds(90)
        };
        var seen = new int[count];
        var processed = 0;
        var duplicates = 0;
        var outOfRange = 0;
        var acks = 0;
        var naks = 0;
        var deadLetters = 0;
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var dispatcher = new AsyncResponse.Transports.SqlServer.SqlServerMessageDispatcher(
            (delivery, _) =>
            {
                var separator = delivery.Payload.IndexOf(':', StringComparison.Ordinal);
                var index = separator >= 0 && int.TryParse(delivery.Payload[..separator], out var parsed)
                    ? parsed
                    : -1;
                if ((uint)index >= (uint)seen.Length)
                {
                    Interlocked.Increment(ref outOfRange);
                }
                else if (Interlocked.Exchange(ref seen[index], 1) == 1)
                {
                    Interlocked.Increment(ref duplicates);
                }
                else if (Interlocked.Increment(ref processed) == count)
                {
                    allProcessed.TrySetResult();
                }

                return Task.CompletedTask;
            },
            options,
            new AsyncResponse.Transports.SqlServer.SqlServerSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: workerCount,
                backgroundQueueCapacity: Math.Max(count, concurrency),
                backgroundDrainTimeout: TimeSpan.FromSeconds(60)),
            NullLogger.Instance,
            AsyncResponse.Transports.SqlServer.SqlServerSubscriberRole.Worker);

        var metrics = await Measure("sqlserver-ack-after-enqueue-dispatch-storm", count, concurrency, async i =>
        {
            await dispatcher.HandleAsync(new AsyncResponse.Transports.SqlServer.SqlServerTransportDelivery(
                Guid.NewGuid(),
                "stress-worker-queue",
                $"{i}:{{}}",
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    [options.CorrelationIdHeader] = $"stress-{i}"
                },
                Attempt: 1,
                () => { Interlocked.Increment(ref acks); return ValueTask.CompletedTask; },
                _ => { Interlocked.Increment(ref naks); return ValueTask.CompletedTask; },
                (_, _, _) => { Interlocked.Increment(ref deadLetters); return ValueTask.FromResult(true); },
                () => ValueTask.FromResult(true)),
                CancellationToken.None).ConfigureAwait(false);
        });

        var drained = false;
        try
        {
            await allProcessed.Task.WaitAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
            drained = true;
        }
        catch (TimeoutException)
        {
        }

        metrics.Print();
        metrics.Emit("sqlserver-ack-after-enqueue-dispatch-storm");
        return Check(
            "sqlserver-ack-after-enqueue-dispatch-storm",
            ("ackMismatch", Math.Abs(count - Volatile.Read(ref acks))),
            ("naks", Volatile.Read(ref naks)),
            ("deadLetters", Volatile.Read(ref deadLetters)),
            ("duplicates", duplicates),
            ("outOfRange", outOfRange),
            ("notDrained", drained ? 0 : 1),
            ("missing", count - Volatile.Read(ref processed)));
    }

    // 3f3) MongoDB ACK-after-enqueue dispatch storm. This bypasses a live MongoDB server and
    //      hammers the queue-document callback dispatcher directly: every ACKed document must be
    //      processed once.
    private static async Task<int> MongoDbAckAfterEnqueueDispatchStorm(int concurrency, int count)
    {
        var workerCount = Math.Clamp(Environment.ProcessorCount, 1, 16);
        var options = new MongoDbAsyncResponseTransportOptions
        {
            HostShutdownTimeout = TimeSpan.FromSeconds(90)
        };
        var seen = new int[count];
        var processed = 0;
        var duplicates = 0;
        var outOfRange = 0;
        var acks = 0;
        var naks = 0;
        var deadLetters = 0;
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var dispatcher = new MongoDbMessageDispatcher(
            (delivery, _) =>
            {
                var separator = delivery.Payload.IndexOf(':', StringComparison.Ordinal);
                var index = separator >= 0 && int.TryParse(delivery.Payload[..separator], out var parsed)
                    ? parsed
                    : -1;
                if ((uint)index >= (uint)seen.Length)
                {
                    Interlocked.Increment(ref outOfRange);
                }
                else if (Interlocked.Exchange(ref seen[index], 1) == 1)
                {
                    Interlocked.Increment(ref duplicates);
                }
                else if (Interlocked.Increment(ref processed) == count)
                {
                    allProcessed.TrySetResult();
                }

                return Task.CompletedTask;
            },
            options,
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: workerCount,
                backgroundQueueCapacity: Math.Max(count, concurrency),
                backgroundDrainTimeout: TimeSpan.FromSeconds(60)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        var metrics = await Measure("mongodb-ack-after-enqueue-dispatch-storm", count, concurrency, async i =>
        {
            await dispatcher.HandleAsync(new MongoDbTransportDelivery(
                Guid.NewGuid(),
                "stress-worker-queue",
                $"{i}:{{}}",
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    [options.CorrelationIdHeader] = $"stress-{i}"
                },
                Attempt: 1,
                () => { Interlocked.Increment(ref acks); return ValueTask.CompletedTask; },
                _ => { Interlocked.Increment(ref naks); return ValueTask.CompletedTask; },
                (_, _, _) => { Interlocked.Increment(ref deadLetters); return ValueTask.FromResult(true); },
                () => ValueTask.FromResult(true)),
                CancellationToken.None).ConfigureAwait(false);
        });

        var drained = false;
        try
        {
            await allProcessed.Task.WaitAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
            drained = true;
        }
        catch (TimeoutException)
        {
        }

        metrics.Print();
        metrics.Emit("mongodb-ack-after-enqueue-dispatch-storm");
        return Check(
            "mongodb-ack-after-enqueue-dispatch-storm",
            ("ackMismatch", Math.Abs(count - Volatile.Read(ref acks))),
            ("naks", Volatile.Read(ref naks)),
            ("deadLetters", Volatile.Read(ref deadLetters)),
            ("duplicates", duplicates),
            ("outOfRange", outOfRange),
            ("notDrained", drained ? 0 : 1),
            ("missing", count - Volatile.Read(ref processed)));
    }

    // 3g) Azure Service Bus ACK-after-enqueue dispatch storm. This bypasses a live Service Bus
    //     namespace and hammers the callback dispatcher directly: every completed message must be
    //     processed once.
    private static async Task<int> AzureServiceBusAckAfterEnqueueDispatchStorm(int concurrency, int count)
    {
        var workerCount = Math.Clamp(Environment.ProcessorCount, 1, 16);
        var options = new AzureServiceBusAsyncResponseOptions
        {
            HostShutdownTimeout = TimeSpan.FromSeconds(90)
        };
        var seen = new int[count];
        var processed = 0;
        var duplicates = 0;
        var outOfRange = 0;
        var completes = 0;
        var abandons = 0;
        var deadLetters = 0;
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (delivery, _) =>
            {
                var separator = delivery.Body.IndexOf(':', StringComparison.Ordinal);
                var index = separator >= 0 && int.TryParse(delivery.Body[..separator], out var parsed)
                    ? parsed
                    : -1;
                if ((uint)index >= (uint)seen.Length)
                {
                    Interlocked.Increment(ref outOfRange);
                }
                else if (Interlocked.Exchange(ref seen[index], 1) == 1)
                {
                    Interlocked.Increment(ref duplicates);
                }
                else if (Interlocked.Increment(ref processed) == count)
                {
                    allProcessed.TrySetResult();
                }

                return Task.CompletedTask;
            },
            options,
            new AzureServiceBusSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: workerCount,
                backgroundQueueCapacity: Math.Max(count, concurrency),
                backgroundDrainTimeout: TimeSpan.FromSeconds(60)),
            NullLogger.Instance,
            "stress-worker-queue",
            AzureServiceBusSubscriberRole.Worker);

        var metrics = await Measure("azure-servicebus-ack-after-receive-dispatch-storm", count, concurrency, async i =>
        {
            await dispatcher.HandleAsync(new AzureServiceBusTransportDelivery(
                "stress-worker-queue",
                $"{i}:{{}}",
                Guid.NewGuid().ToString("N"),
                $"stress-{i}",
                SequenceNumber: i,
                DeliveryCount: 1,
                new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                {
                    [options.CorrelationIdProperty] = $"stress-{i}"
                },
                () => { Interlocked.Increment(ref completes); return ValueTask.CompletedTask; },
                () => { Interlocked.Increment(ref abandons); return ValueTask.CompletedTask; },
                (_, _) => { Interlocked.Increment(ref deadLetters); return ValueTask.CompletedTask; },
                _ => ValueTask.CompletedTask),
                CancellationToken.None).ConfigureAwait(false);
        });

        var drained = false;
        try
        {
            await allProcessed.Task.WaitAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
            drained = true;
        }
        catch (TimeoutException)
        {
        }

        metrics.Print();
        metrics.Emit("azure-servicebus-ack-after-receive-dispatch-storm");
        return Check(
            "azure-servicebus-ack-after-receive-dispatch-storm",
            ("completeMismatch", Math.Abs(count - Volatile.Read(ref completes))),
            ("abandons", Volatile.Read(ref abandons)),
            ("deadLetters", Volatile.Read(ref deadLetters)),
            ("duplicates", duplicates),
            ("outOfRange", outOfRange),
            ("notDrained", drained ? 0 : 1),
            ("missing", count - Volatile.Read(ref processed)));
    }

    // 3h) Kafka ACK-after-enqueue dispatch storm. This bypasses a live broker and hammers the
    //     subscriber callback dispatcher directly: every offset-stored message must be processed
    //     exactly once and nothing may dead-letter.
    private static async Task<int> KafkaAckAfterEnqueueDispatchStorm(int concurrency, int count)
    {
        var workerCount = Math.Clamp(Environment.ProcessorCount, 1, 16);
        var consumer = new BenchmarkKafkaConsumerClient();
        var producer = new BenchmarkKafkaProducerClient();
        var seen = new int[count];
        var processed = 0;
        var duplicates = 0;
        var outOfRange = 0;
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = KafkaMessageDispatcher.Create(
            (delivery, _) =>
            {
                var index = (int)delivery.Offset;
                if ((uint)index >= (uint)seen.Length)
                {
                    Interlocked.Increment(ref outOfRange);
                }
                else if (Interlocked.Exchange(ref seen[index], 1) == 1)
                {
                    Interlocked.Increment(ref duplicates);
                }
                else if (Interlocked.Increment(ref processed) == count)
                {
                    allProcessed.TrySetResult();
                }

                return Task.CompletedTask;
            },
            consumer,
            producer,
            new KafkaAsyncResponseTransportOptions
            {
                BootstrapServers = "localhost:9092",
                HostShutdownTimeout = TimeSpan.FromSeconds(90)
            },
            new KafkaSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: workerCount,
                backgroundQueueCapacity: Math.Max(count, concurrency),
                backgroundDrainTimeout: TimeSpan.FromSeconds(60)),
            NullLogger.Instance,
            "stress-worker-topic",
            "stress-worker-group",
            KafkaSubscriberRole.Worker);

        var metrics = await Measure("kafka-ack-after-enqueue-dispatch-storm", count, concurrency, async i =>
        {
            await dispatcher.HandleAsync(new KafkaDelivery(
                "stress-worker-topic",
                Partition: 0,
                Offset: i,
                "{}",
                $"stress-{i}",
                [KafkaTransportHeader.Utf8("correlationId", $"stress-{i}")]),
                CancellationToken.None).ConfigureAwait(false);
        });

        var drained = false;
        try
        {
            await allProcessed.Task.WaitAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
            drained = true;
        }
        catch (TimeoutException)
        {
        }

        metrics.Print();
        metrics.Emit("kafka-ack-after-enqueue-dispatch-storm");
        return Check(
            "kafka-ack-after-enqueue-dispatch-storm",
            ("offsetStoreMismatch", Math.Abs(count - consumer.StoredOffsets)),
            ("deadLetters", (int)producer.Publishes),
            ("duplicates", duplicates),
            ("outOfRange", outOfRange),
            ("notDrained", drained ? 0 : 1),
            ("missing", count - Volatile.Read(ref processed)));
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
    //    raw JSON -> typed waiter materialization -> completion predicate.
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
                || (await store.GetAllAsync(correlationId).ConfigureAwait(false)).Count != 0)
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
                || (await store.GetAllAsync(correlationId).ConfigureAwait(false)).Count != 0)
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
                    || (await store.GetAllAsync(correlationId).ConfigureAwait(false)).Count != 0)
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
                    await store.TryDeleteAsync(state.CorrelationId, state.RegistrationId).ConfigureAwait(false);
            }
        }
    }

    // SQS ACK-after-enqueue dispatch storm: every delivery must be deleted exactly once (the early
    // ACK), never released back via ChangeMessageVisibility, and every handler must run exactly
    // once with no duplicates — delete/visibility drift is an at-least-once accounting bug.
    private static async Task<int> SqsAckAfterEnqueueDispatchStorm(int concurrency, int count)
    {
        var workerCount = Math.Clamp(Environment.ProcessorCount, 1, 16);
        var options = new SqsAsyncResponseOptions
        {
            HostShutdownTimeout = TimeSpan.FromSeconds(90)
        };
        var seen = new int[count];
        var processed = 0;
        var duplicates = 0;
        var outOfRange = 0;
        var deletes = 0;
        var visibilityReleases = 0;
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var dispatcher = SqsMessageDispatcher.Create(
            (delivery, _) =>
            {
                var separator = delivery.Body.IndexOf(':', StringComparison.Ordinal);
                var index = separator >= 0 && int.TryParse(delivery.Body[..separator], out var parsed)
                    ? parsed
                    : -1;
                if ((uint)index >= (uint)seen.Length)
                {
                    Interlocked.Increment(ref outOfRange);
                }
                else if (Interlocked.Exchange(ref seen[index], 1) == 1)
                {
                    Interlocked.Increment(ref duplicates);
                }
                else if (Interlocked.Increment(ref processed) == count)
                {
                    allProcessed.TrySetResult();
                }

                return Task.CompletedTask;
            },
            options,
            new SqsSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: workerCount,
                backgroundQueueCapacity: Math.Max(count, concurrency),
                backgroundDrainTimeout: TimeSpan.FromSeconds(60)),
            NullLogger.Instance,
            "stress-worker-queue",
            SqsSubscriberRole.Worker);

        var metrics = await Measure("sqs-ack-after-enqueue-dispatch-storm", count, concurrency, async i =>
        {
            await dispatcher.HandleAsync(new SqsTransportDelivery(
                "https://sqs.stress.local/000000000000/stress-worker-queue",
                $"{i}:{{}}",
                $"stress-{i}",
                $"receipt-{i}",
                ReceiveCount: 1,
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    [options.CorrelationIdAttribute] = $"stress-{i}"
                },
                () => { Interlocked.Increment(ref deletes); return ValueTask.CompletedTask; },
                _ => { Interlocked.Increment(ref visibilityReleases); return ValueTask.CompletedTask; }),
                CancellationToken.None).ConfigureAwait(false);
        });

        var drained = false;
        try
        {
            await allProcessed.Task.WaitAsync(TimeSpan.FromSeconds(60)).ConfigureAwait(false);
            drained = true;
        }
        catch (TimeoutException)
        {
        }

        metrics.Print();
        metrics.Emit("sqs-ack-after-enqueue-dispatch-storm");
        return Check(
            "sqs-ack-after-enqueue-dispatch-storm",
            ("deleteMismatch", Math.Abs(count - Volatile.Read(ref deletes))),
            ("visibilityReleases", Volatile.Read(ref visibilityReleases)),
            ("duplicates", duplicates),
            ("outOfRange", outOfRange),
            ("notDrained", drained ? 0 : 1),
            ("missing", count - Volatile.Read(ref processed)));
    }

    // -- infrastructure --------------------------------------------------------------------------

    private static ServiceProvider BuildProvider(bool withWorker = false)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        var builder = services.AddAsyncResponse(options => options.Watchdog.Enabled = false)
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows();
        if (withWorker)
        {
            builder.WithInMemoryTransport();
            services.AddSingleton<StressCounter>();
            services.AddSingleton<ICountingWorker, CountingWorker>();
        }

        return services.BuildServiceProvider();
    }

    // 18) Durable-flow storm: N concurrent checkpointed flows (local → awaited → local → awaited →
    //     local) started through IDurableFlows and executed by the real worker transport, with
    //     responder loops answering every awaited step (progress + terminal). Every flow must end
    //     Succeeded and every local step must run exactly once — lost or duplicated step executions
    //     are checkpointing bugs.
    private static async Task<int> DurableFlowStorm(int concurrency, int count)
    {
        var services = new ServiceCollection();
        // Not NullLogger here: a flow that fails every attempt (a payload contract violation, a
        // store rejection) is otherwise completely silent, and the scenario only surfaces it as a
        // drain timeout minutes later. Errors — including the transport's terminal "dropping it"
        // log — are echoed, capped, so the cause is on screen the moment it happens.
        StressErrorLogger.Reset();
        services.AddSingleton(typeof(ILogger<>), typeof(StressErrorLogger<>));
        services.AddSingleton<FlowStormRig>();
        services.AddScoped<StressFlow>();
        services.AddAsyncResponse(options => options.Watchdog.Enabled = false)
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows();

        using var provider = services.BuildServiceProvider();

        var flows = provider.GetRequiredService<IDurableFlows>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var rig = provider.GetRequiredService<FlowStormRig>();
        rig.Prepare.Reset(count);
        rig.Transform.Reset(count);
        rig.Finalize.Reset(count);

        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var hostedService in hosted)
            await hostedService.StartAsync(CancellationToken.None);

        using var responderCts = new CancellationTokenSource();
        var responders = Enumerable.Range(0, Math.Max(2, Environment.ProcessorCount / 2)).Select(_ => Task.Run(async () =>
        {
            await foreach (var correlationId in rig.Triggers.ReadAllAsync(responderCts.Token))
            {
                await publisher.SetResponse(new FlowStormPayload { Done = false }, correlationId); // progress
                await publisher.SetResponse(new FlowStormPayload { Done = true }, correlationId);  // terminal
            }
        })).ToArray();

        try
        {
            var allocBefore = GC.GetTotalAllocatedBytes();
            var sw = Stopwatch.StartNew();

            var flowIds = new string[count];
            await ForEachAsync(count, concurrency, async i =>
                flowIds[i] = await flows.StartAsync<StressFlow, FlowStormInput>(new FlowStormInput(i)));
            var drained = await rig.Finalize.WaitAllAsync(TimeSpan.FromSeconds(240));

            sw.Stop();
            var alloc = GC.GetTotalAllocatedBytes() - allocBefore;

            // Finalize-step execution is necessary but not sufficient — sweep the terminal states.
            var notSucceeded = 0;
            for (var i = 0; i < count; i++)
            {
                var state = flowIds[i] is null ? null : await flows.GetStateAsync(flowIds[i]);
                if (state?.Status != FlowRunStatus.Succeeded)
                    notSucceeded++;
            }

            var duplicates = rig.Prepare.Duplicates + rig.Transform.Duplicates + rig.Finalize.Duplicates;

            Console.WriteLine("  durable-flow-storm (5-step checkpointed flows, exactly-once steps)");
            Console.WriteLine($"    flows={count:N0}  elapsed={sw.Elapsed.TotalMilliseconds:N0}ms  throughput={count / sw.Elapsed.TotalSeconds:N0} flows/s");
            Console.WriteLine($"    finalized={rig.Finalize.Executed:N0}  duplicates={duplicates}  notSucceeded={notSucceeded}  alloc={alloc / 1024.0 / 1024.0:N1}MB ({alloc / (double)count:N0} B/flow)");

            Series.Add(new GhMetric("durable-flow-storm throughput", "flows/s", count / sw.Elapsed.TotalSeconds, BiggerIsBetter: true));
            Series.Add(new GhMetric("durable-flow-storm allocations", "B/flow", alloc / (double)count, BiggerIsBetter: false));

            return Check("durable-flow-storm",
                ("notDrained", drained ? 0 : 1),
                ("missingPrepare", count - rig.Prepare.Executed),
                ("missingTransform", count - rig.Transform.Executed),
                ("missingFinalize", count - rig.Finalize.Executed),
                ("duplicates", duplicates),
                ("notSucceeded", notSucceeded));
        }
        finally
        {
            responderCts.Cancel();
            try
            {
                await Task.WhenAll(responders);
            }
            catch (OperationCanceledException)
            {
            }

            await StopBoundedAsync(hosted);
        }
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

    private sealed class StressRabbitMqChannel : IRabbitMqChannel
    {
        private int _acks;
        private int _nacks;

        public int Acks => Volatile.Read(ref _acks);
        public int Nacks => Volatile.Read(ref _nacks);

        public bool IsOpen => true;

        public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object?>? arguments = null, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task QueueBindAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public ValueTask BasicPublishAsync(string exchange, string routingKey, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
            => ValueTask.CompletedTask;

        public Task<RabbitMqConsumer> BasicConsumeAsync(string queue, Func<RabbitMqDelivery, Task> handler, CancellationToken cancellationToken = default)
            => Task.FromResult(new RabbitMqConsumer("stress-consumer", new TaskCompletionSource<string>().Task));

        public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public ValueTask BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _acks);
            return ValueTask.CompletedTask;
        }

        public ValueTask BasicNackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _nacks);
            return ValueTask.CompletedTask;
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public ValueTask DisposeAsync()
            => ValueTask.CompletedTask;
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
