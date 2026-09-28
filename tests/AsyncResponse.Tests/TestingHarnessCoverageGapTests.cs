using System.Reflection;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>A worker target that blocks until the test releases it.</summary>
public interface IGapBlockingWork
{
    Task RunAsync();
}

public sealed class GapBlockingWork : IGapBlockingWork
{
    public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public async Task RunAsync()
    {
        Entered.TrySetResult();
        await Release.Task;
    }
}

/// <summary>
/// Coverage-gap pins for AsyncResponse.Testing: the harness's teardown (hosted services whose stop
/// fails or is cut short, double disposal, diagnostics after disposal), its bounded idle wait, the
/// carry-over of a retained delayed job that is already due, the quiescence probe ignoring a dead
/// incarnation's events, and the flow probe's attempt bookkeeping.
/// </summary>
public sealed class TestingHarnessCoverageGapTests
{
    private sealed class StopBehavior(Func<Task> stop) : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => stop();
    }

    [Fact]
    public async Task Teardown_StopsEveryService_SwallowsACutShortStop_AndAggregatesSeveralFailures()
    {
        var stopped = 0;
        var harness = await AsyncResponseTestHarness.StartAsync(options => options.ConfigureServices = services =>
        {
            services.AddSingleton<IHostedService>(new StopBehavior(async () =>
            {
                Interlocked.Increment(ref stopped);
                // Slow enough for the park watcher to be mid-poll when the stop ends.
                await Task.Delay(50);
                throw new OperationCanceledException("did not stop in time");
            }));
            services.AddSingleton<IHostedService>(new StopBehavior(() =>
            {
                Interlocked.Increment(ref stopped);
                return Task.FromException(new InvalidOperationException("first stop failure"));
            }));
            services.AddSingleton<IHostedService>(new StopBehavior(() =>
            {
                Interlocked.Increment(ref stopped);
                return Task.FromException(new InvalidOperationException("second stop failure"));
            }));
        });

        var ex = await Assert.ThrowsAsync<AggregateException>(() => harness.DisposeAsync().AsTask());

        Assert.Equal(3, stopped);
        Assert.Equal(["first stop failure", "second stop failure"], ex.InnerExceptions.Select(inner => inner.Message).Order());

        // Disposed already: a second disposal is a no-op, and the diagnostic executor read that a
        // failed teardown would consult answers null instead of throwing over the real failure.
        await harness.DisposeAsync();
        var executor = typeof(AsyncResponseTestHarness).GetProperty("FlowExecutorInternal", BindingFlags.Instance | BindingFlags.NonPublic)!;
        Assert.Null(executor.GetValue(harness));
    }

    [Fact]
    public async Task WaitForWorkerIdle_WithAJobStillRunning_FailsAfterTheRealTimeGuard_NamingTheCount()
    {
        var work = new GapBlockingWork();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
        {
            options.RealTimeGuard = TimeSpan.FromMilliseconds(200);
            options.ConfigureServices = services => services.AddSingleton<IGapBlockingWork>(work);
        });
        await harness.Builder.EnqueueWorkerAsync<IGapBlockingWork>(w => w.RunAsync());
        await work.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));

        var ex = await Assert.ThrowsAsync<TimeoutException>(harness.WaitForWorkerIdleAsync);
        Assert.Contains("still has 1 outstanding job(s)", ex.Message, StringComparison.Ordinal);

        work.Release.SetResult();
        await harness.WaitForWorkerIdleAsync();
    }

    [Fact]
    public async Task ARetainedDelayedJobWithNoDueStamp_RunsAtOnceInTheNextIncarnation()
    {
        var audit = new RecordingDeferredWorkAudit();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IDeferredWorkAudit>(audit));
        var transport = harness.Services.GetRequiredService<InMemoryWorkerTransport>();

        // Published straight to the transport with a delay but no NotBeforeUtc stamp: nothing says
        // it is still early, so the restart re-admits it for immediate execution.
        await transport.PublishAsync(
            new WorkerJobEnvelope { Call = CallbackExpressionConverter.ToReflectionCall<IDeferredWorkAudit>(a => a.RanAsync("carried-over")) },
            TimeSpan.FromHours(1));
        Assert.Empty(audit.Ran);

        await harness.SimulateRestartAsync();
        await harness.WaitForWorkerIdleAsync();

        Assert.Equal(["carried-over"], audit.Ran);
    }

    [Fact]
    public async Task TheChannelOptionsHook_ConfiguresEveryIncarnationsChannel()
    {
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.Channel = channel => channel.DefaultTimeout = TimeSpan.FromMinutes(7));

        Assert.Equal(TimeSpan.FromMinutes(7), harness.Services.GetRequiredService<IOptions<InMemoryAsyncResponseOptions>>().Value.DefaultTimeout);
        await harness.SimulateRestartAsync();
        Assert.Equal(TimeSpan.FromMinutes(7), harness.Services.GetRequiredService<IOptions<InMemoryAsyncResponseOptions>>().Value.DefaultTimeout);
    }

    [Fact]
    public async Task TheQuiesceProbe_IgnoresADeadIncarnationsStepEvents()
    {
        await using var harness = await AsyncResponseTestHarness.StartAsync();
        var quiesce = typeof(AsyncResponseTestHarness).GetField("_quiesce", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(harness)!;
        var probeType = quiesce.GetType();
        var generation = (int)probeType.GetField("_generation", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(quiesce)!;
        var waiting = probeType.GetMethod("OnStepWaiting", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var completed = probeType.GetMethod("OnStepCompleted", BindingFlags.Instance | BindingFlags.NonPublic)!;
        int Parked() => (int)probeType.GetProperty("ParkedCount")!.GetValue(quiesce)!;
        var live = new DurableFlowStepEvent("gap-flow", "live", DurableFlowStepKind.Awaited, "cid-live");
        var zombie = new DurableFlowStepEvent("gap-flow", "zombie", DurableFlowStepKind.Awaited, "cid-zombie");

        waiting.Invoke(quiesce, [live, generation]);
        Assert.Equal(1, Parked());

        // A zombie of an earlier incarnation neither parks a step nor completes one.
        waiting.Invoke(quiesce, [zombie, generation - 1]);
        completed.Invoke(quiesce, [live, generation - 1]);
        Assert.Equal(1, Parked());

        completed.Invoke(quiesce, [live, generation]);
        Assert.Equal(0, Parked());
    }

    [Fact]
    public async Task TheFlowProbe_ReleasesAnAwaitedParkOnAFailedAttempt_AndKnowsNothingOfUnseenRuns()
    {
        var probe = new AsyncResponse.Testing.FlowProbe();
        IDurableFlowExecutionObserver observer = probe;

        await observer.OnStepWaitingAsync(new DurableFlowStepEvent("gap-run", "remote", DurableFlowStepKind.Awaited, "cid-gap-probe"));
        Assert.Equal("cid-gap-probe", probe.LatestAwaitedCorrelationId("gap-run", "remote"));

        // The attempt failed: the waiter and its registration are gone, so the id is dead.
        await observer.OnRunAttemptFailedAsync(new DurableFlowRunEvent("gap-run", FlowRunStatus.Running, "attempt failed"));
        Assert.Null(probe.LatestAwaitedCorrelationId("gap-run", "remote"));
        Assert.Equal(1, probe.CountEvents("gap-run", "remote", AsyncResponse.Testing.FlowProbe.EventKind.Waiting));

        Assert.Equal(0, probe.CountEvents("gap-unseen", "remote", AsyncResponse.Testing.FlowProbe.EventKind.Waiting));
        Assert.Empty(probe.EventsFor("gap-unseen"));
    }
}
