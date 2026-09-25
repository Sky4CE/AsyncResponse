using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>A worker job target that records the ambient context observed while it runs.</summary>
public interface IWorkerProbe
{
    Task RunAsync();
}

public sealed class WorkerProbe : IWorkerProbe
{
    public string? SeenCorrelationId { get; private set; }
    public AsyncResponseReplyTarget? SeenReplyTarget { get; private set; }

    public Task RunAsync()
    {
        SeenCorrelationId = AsyncResponseContext.CorrelationId;
        SeenReplyTarget = AsyncResponseContext.ReplyTarget;
        return Task.CompletedTask;
    }
}

/// <summary>
/// The shared worker-job executor restores the captured correlation id and reply target into the
/// ambient context for the duration of the job (so downstream publishes correlate), then unwinds
/// that scope so one job never leaks its context to the next.
/// </summary>
public class WorkerJobExecutorTests
{
    [Fact]
    public async Task ExecutesJob_RestoringCorrelationAndReplyTargetContext()
    {
        var probe = new WorkerProbe();
        var provider = new ServiceCollection().AddSingleton<IWorkerProbe>(probe).BuildServiceProvider();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);

        await executor.ExecuteAsync(new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IWorkerProbe).FullName!,
                MethodName = nameof(IWorkerProbe.RunAsync),
                Params = []
            },
            CorrelationId = "cid-1",
            ReplyTarget = new AsyncResponseReplyTarget { Name = "default", Transport = "test", Address = "test://reply" }
        });

        Assert.Equal("cid-1", probe.SeenCorrelationId);
        Assert.Equal("default", probe.SeenReplyTarget?.Name);

        // The restored context is scoped to the job and must not leak back to the caller.
        Assert.Null(AsyncResponseContext.CorrelationId);
        Assert.Null(AsyncResponseContext.ReplyTarget);
    }

    [Fact]
    public async Task NullJob_Throws()
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);

        await Assert.ThrowsAsync<ArgumentNullException>(() => executor.ExecuteAsync(null!));
    }

    [Fact]
    public async Task MissingService_FaultsThroughExecutorErrorPath()
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);

        var ex = await Assert.ThrowsAnyAsync<InvalidOperationException>(() => executor.ExecuteAsync(new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IWorkerProbe).FullName!,
                MethodName = nameof(IWorkerProbe.RunAsync),
                Params = []
            },
            CorrelationId = "cid-missing"
        }));

        Assert.Contains(nameof(IWorkerProbe), ex.Message);
    }

    [Fact]
    public async Task InMemoryWorkerTransport_CanceledPublish_FaultsThroughProducerErrorPath()
    {
        var transport = new InMemoryWorkerTransport();
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => transport.PublishAsync(new WorkerJobEnvelope
        {
            CorrelationId = "cid-canceled",
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IWorkerProbe).FullName!,
                MethodName = nameof(IWorkerProbe.RunAsync),
                Params = []
            }
        }, canceled.Token));
    }

    [Fact]
    public async Task InMemoryWorkerTransport_BoundedQueueBackpressuresPublishers()
    {
        var transport = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions
        {
            QueueCapacity = 1,
            WorkerCount = 1
        }));
        var first = CreateJob("first");
        var second = CreateJob("second");

        await transport.PublishAsync(first);
        var blocked = transport.PublishAsync(second);
        await Task.Delay(30);
        Assert.False(blocked.IsCompleted);

        Assert.Equal("first", (await transport.Reader.ReadAsync()).Job.CorrelationId);
        await blocked.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("second", (await transport.Reader.ReadAsync()).Job.CorrelationId);
    }

    [Theory]
    [InlineData(nameof(InMemoryWorkerTransportOptions.QueueCapacity))]
    [InlineData(nameof(InMemoryWorkerTransportOptions.WorkerCount))]
    public void InMemoryWorkerTransport_InvalidOptionsAreRejected(string propertyName)
    {
        var options = new InMemoryWorkerTransportOptions();
        if (propertyName == nameof(InMemoryWorkerTransportOptions.QueueCapacity))
            options.QueueCapacity = 0;
        else
            options.WorkerCount = 0;

        var exception = Assert.Throws<InvalidOperationException>(
            () => new InMemoryWorkerTransport(Options.Create(options)));

        Assert.Contains(propertyName, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InMemoryWorkerHost_ContinuesAfterFailedJobAndRunsWithoutCapturedExecutionContext()
    {
        var probe = new WorkerProbe();
        var provider = new ServiceCollection().AddSingleton<IWorkerProbe>(probe).BuildServiceProvider();

        // The failed job rides the whole default retry ladder (5 attempts, 100/200/400/800 ms of
        // backoff) before the job behind it can run. On the system clock that was ~1.5 s of REAL
        // sleep inside a 2 s real-time window, so the test failed whenever the machine was loaded.
        // The backoff now runs on a virtual clock the test moves itself: it elapses exactly when
        // advanced, whatever the load.
        var clock = new VirtualTimeProvider();
        var logger = new CollectingLogger();
        var transport = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions()), clock);
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);
        var host = new InMemoryWorkerHost(
            transport,
            executor,
            logger.For<InMemoryWorkerHost>(),
            clock);

        var flow = ExecutionContext.SuppressFlow();
        try
        {
            await transport.PublishAsync(new WorkerJobEnvelope
            {
                CorrelationId = "cid-missing-service",
                Call = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = "AsyncResponse.Tests.IMissingWorker",
                    MethodName = nameof(IWorkerProbe.RunAsync),
                    Params = []
                }
            });

            await transport.PublishAsync(new WorkerJobEnvelope
            {
                CorrelationId = "cid-direct",
                Call = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IWorkerProbe).FullName!,
                    MethodName = nameof(IWorkerProbe.RunAsync),
                    Params = []
                }
            });
        }
        finally
        {
            flow.Undo();
        }

        var start = clock.GetUtcNow();
        await host.StartAsync(CancellationToken.None);
        try
        {
            // Step the virtual clock to each backoff timer as the ladder arms it. The real-time
            // bound is only a hang guard: no amount of load can make a backoff elapse early or
            // late, so nothing here races the wall clock.
            var guard = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (probe.SeenCorrelationId != "cid-direct")
            {
                Assert.True(DateTime.UtcNow < guard, $"the job behind the failed one never ran. Logged: {string.Join(" | ", logger.Messages)}");
                if (clock.NextTimerDueAt is { } due)
                    clock.AdvanceTo(due);
                else
                    await Task.Delay(TimeSpan.FromMilliseconds(1));
            }
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        Assert.Equal("cid-direct", probe.SeenCorrelationId);

        // The failed job was retried to the end of its budget and dropped — the worker loop kept
        // serving — and every backoff was spent on the virtual clock: exactly 100+200+400+800 ms.
        Assert.Contains(logger.Messages, message => message.Contains("failed after 5 attempts; dropping it", StringComparison.Ordinal));
        Assert.Equal(TimeSpan.FromMilliseconds(1500), clock.GetUtcNow() - start);
    }

    public interface IExecutorEdgeProbe
    {
        Task InterruptedAsync();

        Task ObserveSkewMarkerAsync();
    }

    private sealed class ExecutorEdgeProbe : IExecutorEdgeProbe
    {
        public bool? SawForcedEarly { get; private set; }

        public Task InterruptedAsync()
            => throw new DurableFlowInterruptedException("Host is stopping; the delivery is handed back.");

        public Task ObserveSkewMarkerAsync()
        {
            SawForcedEarly = WorkerJobSkewScope.IsForcedEarlyExecution;
            return Task.CompletedTask;
        }
    }

    private static WorkerJobEnvelope EdgeJob(string method, string correlationId) => new()
    {
        CorrelationId = correlationId,
        Call = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IExecutorEdgeProbe).FullName!,
            MethodName = method,
            Params = []
        }
    };

    [Fact]
    public async Task DurableFlowInterruption_PropagatesWithoutAFailedOutcomeOrAnErrorSpan()
    {
        // Regression (fixpoint r1): the flow engine's host-stop hand-back is a cancellation by
        // contract, yet the shared executor counted it as a `failed` job and marked its span as
        // an error — on every parked flow of every deploy, on every transport.
        var probe = new ExecutorEdgeProbe();
        await using var provider = new ServiceCollection().AddSingleton<IExecutorEdgeProbe>(probe).BuildServiceProvider();
        var executor = new WorkerJobExecutor(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<WorkerJobExecutor>.Instance);

        using var activities = new AsyncResponseActivityCollector();
        var traceId = System.Diagnostics.Activity.Current!.TraceId;
        var outcomes = new List<string?>();
        using var listener = new System.Diagnostics.Metrics.MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == AsyncResponseDiagnostics.MeterName && instrument.Name == "asyncresponse.worker.jobs")
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            // This test's own trace only: the meter is process-wide and other tests run alongside.
            if (System.Diagnostics.Activity.Current?.TraceId != traceId)
                return;

            foreach (var tag in tags)
            {
                if (tag.Key == "outcome")
                    lock (outcomes) outcomes.Add(tag.Value as string);
            }
        });
        listener.Start();

        await Assert.ThrowsAsync<DurableFlowInterruptedException>(() =>
            executor.ExecuteAsync(EdgeJob(nameof(IExecutorEdgeProbe.InterruptedAsync), "cid-interrupted")));

        lock (outcomes) Assert.DoesNotContain("failed", outcomes);
        var span = activities.Single("asyncresponse.worker.execute", "asyncresponse.correlation_id", "cid-interrupted");
        Assert.NotEqual(System.Diagnostics.ActivityStatusCode.Error, span.Status);
    }

    [Fact]
    public async Task EveryJob_StartsWithoutTheSkewMarkerItsEnqueuerHeld()
    {
        // Regression (fixpoint r1): the in-memory transport runs a job under its ENQUEUER's
        // captured execution context, and unlike WorkerJobScope the skew marker was not cleared
        // per job — a follow-up published by a job the stall guard released early inherited the
        // unconsumed marker, and a durable timer in it spent the one-shot proof.
        var probe = new ExecutorEdgeProbe();
        await using var provider = new ServiceCollection().AddSingleton<IExecutorEdgeProbe>(probe).BuildServiceProvider();
        var executor = new WorkerJobExecutor(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<WorkerJobExecutor>.Instance);

        using (WorkerJobSkewScope.Enter())
        {
            Assert.True(WorkerJobSkewScope.IsForcedEarlyExecution);
            await executor.ExecuteAsync(EdgeJob(nameof(IExecutorEdgeProbe.ObserveSkewMarkerAsync), "cid-unmarked"));
            // The caller's own marker is untouched.
            Assert.True(WorkerJobSkewScope.IsForcedEarlyExecution);
        }

        Assert.False(probe.SawForcedEarly);
    }

    [Fact]
    public async Task UntrustedNamesAndIds_AreEscapedInTheExecutorsLogLines()
    {
        // Regression (fixpoint r1): the executor quoted wire text raw — the correlation id of a
        // schema-rejected job (logged at Warning before the portability check ever ran) and the
        // still-unresolved target type and method of a job about to execute — so a CR/LF in any
        // of them ended the real log line and started a forged one.
        var logger = new CollectingLogger();
        await using var provider = new ServiceCollection().BuildServiceProvider();
        var executor = new WorkerJobExecutor(provider.GetRequiredService<IServiceScopeFactory>(), logger.For<WorkerJobExecutor>());

        await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync(new WorkerJobEnvelope
        {
            SchemaVersion = WorkerJobEnvelopeSchema.Current + 1,
            Call = new ReflectionCallDto { ServiceInterfaceFullName = "Svc", MethodName = "M", Params = [] },
            CorrelationId = "cid\r\nFORGED schema entry"
        }));

        await Assert.ThrowsAnyAsync<Exception>(() => executor.ExecuteAsync(new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "My.IService\r\nFORGED type entry",
                MethodName = "Run\nFORGED method entry",
                Params = []
            },
            CorrelationId = "cid-escaped",
            ReplyTarget = new AsyncResponseReplyTarget { Name = "reply\r\nFORGED target entry", Transport = "test", Address = "test://reply" }
        }));

        Assert.Contains(logger.Messages, message => message.Contains("unsupported schema version", StringComparison.Ordinal));
        Assert.Contains(logger.Messages, message => message.Contains("Executing worker job", StringComparison.Ordinal));
        Assert.All(logger.Messages, message =>
        {
            Assert.DoesNotContain('\r', message);
            Assert.DoesNotContain('\n', message);
        });
    }

    private static WorkerJobEnvelope CreateJob(string correlationId)
        => new()
        {
            CorrelationId = correlationId,
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IWorkerProbe).FullName!,
                MethodName = nameof(IWorkerProbe.RunAsync),
                Params = []
            }
        };
}
