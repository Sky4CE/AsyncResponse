using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// Delayed worker delivery mechanics: the builder's capability check, the due-time stamp, the
// executor's NotBeforeUtc early-delivery guard (the chunk chain every capped transport rides),
// and the in-memory transport's timer wheel.
// ---------------------------------------------------------------------------------------------

public class DelayedWorkerTransportTests
{
    private sealed class NonDelayedTransport : IWorkerTransport
    {
        public List<WorkerJobEnvelope> Published { get; } = [];

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            Published.Add(job);
            return Task.CompletedTask;
        }
    }

    private static ReflectionCallDto Work()
        => CallbackExpressionConverter.ToReflectionCall<IDeferredWorkAudit>(target => target.RanAsync("job"));

    [Fact]
    public async Task DelayedEnqueue_OnANonDelayedTransport_FailsWithGuidance()
    {
        var transport = new NonDelayedTransport();
        var builder = new AsyncResponseBuilder(
            new InMemoryAsyncResponseChannel(
                new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
                new InMemoryRecoveryStateStore(),
                Options.Create(new InMemoryAsyncResponseOptions()),
                new AsyncResponseContextPropagation([]),
                NullLogger<InMemoryAsyncResponseChannel>.Instance),
            transport,
            propagation: new AsyncResponseContextPropagation([]));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => builder.EnqueueWorkerAsync(Work(), TimeSpan.FromMinutes(5)));

        Assert.Contains(nameof(IDelayedWorkerTransport), ex.Message, StringComparison.Ordinal);
        Assert.Contains("DelayAsync", ex.Message, StringComparison.Ordinal);
        Assert.Empty(transport.Published);
    }

    [Fact]
    public async Task NonPositiveDelay_PublishesImmediately_EvenOnANonDelayedTransport()
    {
        var transport = new NonDelayedTransport();
        var builder = new AsyncResponseBuilder(
            new InMemoryAsyncResponseChannel(
                new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
                new InMemoryRecoveryStateStore(),
                Options.Create(new InMemoryAsyncResponseOptions()),
                new AsyncResponseContextPropagation([]),
                NullLogger<InMemoryAsyncResponseChannel>.Instance),
            transport,
            propagation: new AsyncResponseContextPropagation([]));

        await builder.EnqueueWorkerAsync(Work(), TimeSpan.Zero);
        await builder.EnqueueWorkerAsync(Work(), TimeSpan.FromSeconds(-5));

        Assert.Equal(2, transport.Published.Count);
        Assert.All(transport.Published, job => Assert.Null(job.NotBeforeUtc));
    }

    [Fact]
    public async Task DelayedEnqueue_StampsTheAbsoluteDueTime()
    {
        var audit = new RecordingDeferredWorkAudit();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IDeferredWorkAudit>(audit));

        await harness.Builder.EnqueueWorkerAsync<IDeferredWorkAudit>(
            worker => worker.RanAsync("stamped"),
            TimeSpan.FromMinutes(30));

        var transport = (InMemoryWorkerTransport)harness.Services.GetRequiredService<IWorkerTransport>();
        var pending = Assert.Single(transport.SnapshotDelayedJobs());
        Assert.Equal(
            harness.Clock.GetUtcNow().UtcDateTime + TimeSpan.FromMinutes(30),
            pending.NotBeforeUtc);
    }

    [Fact]
    public async Task EarlyDeliveredJob_IsRedelayedByTheExecutor_NotExecuted()
    {
        var audit = new RecordingDeferredWorkAudit();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IDeferredWorkAudit>(audit));

        // Simulate a chunk hop arriving early: an IMMEDIATE publish whose envelope says "due in
        // an hour" (what an SQS 15-minute hop looks like at minute 15 of a 75-minute delay).
        var transport = harness.Services.GetRequiredService<IWorkerTransport>();
        await transport.PublishAsync(new WorkerJobEnvelope
        {
            Call = Work(),
            NotBeforeUtc = harness.Clock.GetUtcNow().UtcDateTime.AddHours(1)
        });

        await harness.WaitForWorkerIdleAsync();
        Assert.Empty(audit.Ran);

        // The executor re-scheduled the remainder on the transport's timer wheel; crossing the
        // due time runs it exactly once.
        await harness.AdvanceAsync(TimeSpan.FromHours(1));
        await harness.WaitForWorkerIdleAsync();
        Assert.Equal(["job"], audit.Ran);
    }

    [Fact]
    public async Task EarlyDeliveredJob_HostileLastRedelayRemaining_IsRedelayedInsteadOfCrashing()
    {
        // Regression (round 31): LastRedelayRemaining is a wire value a foreign producer controls,
        // and TimeSpan arithmetic is always overflow-checked — TimeSpan.MinValue made the stall
        // comparison throw OverflowException, a type outside the ingress drop-and-ack filter, so
        // the envelope redelivered forever with the value never becoming valid. The library only
        // ever stamps a strictly positive remainder, so a negative hint is discarded and the hop
        // re-publishes normally.
        var audit = new RecordingDeferredWorkAudit();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IDeferredWorkAudit>(audit));

        var transport = harness.Services.GetRequiredService<IWorkerTransport>();
        await transport.PublishAsync(new WorkerJobEnvelope
        {
            Call = Work(),
            NotBeforeUtc = harness.Clock.GetUtcNow().UtcDateTime.AddHours(1),
            LastRedelayRemaining = TimeSpan.MinValue
        });

        await harness.WaitForWorkerIdleAsync();
        Assert.Empty(audit.Ran);

        // The hostile hint was discarded and the hop parked on the timer wheel like a fresh one.
        var inMemory = (InMemoryWorkerTransport)transport;
        Assert.Single(inMemory.SnapshotDelayedJobs());
        await harness.AdvanceAsync(TimeSpan.FromHours(1));
        await harness.WaitForWorkerIdleAsync();
        Assert.Equal(["job"], audit.Ran);
    }

    [Fact]
    public async Task EarlyDeliveredJob_HostileStallCount_CannotDisarmTheStallFallback()
    {
        // Regression (round 31): RedelayStallCount is an unvalidated wire int incremented in an
        // unchecked context. int.MaxValue wrapped to int.MinValue, so the `>= threshold` stall
        // check could never fire again and the anti-livelock fallback was permanently disarmed —
        // an unbounded re-publish loop, each hop a fresh message id no delivery counter ever
        // catches. The counter is now clamped, so a hostile value on a proven stall executes the
        // job instead of re-publishing forever.
        var audit = new RecordingDeferredWorkAudit();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IDeferredWorkAudit>(audit));

        var transport = harness.Services.GetRequiredService<IWorkerTransport>();
        var notBefore = harness.Clock.GetUtcNow().UtcDateTime.AddHours(1);
        await transport.PublishAsync(new WorkerJobEnvelope
        {
            Call = Work(),
            NotBeforeUtc = notBefore,
            // A stalled hop (the remainder has not shrunk) carrying a hostile counter.
            LastRedelayRemaining = notBefore - harness.Clock.GetUtcNow().UtcDateTime,
            RedelayStallCount = int.MaxValue
        });

        await harness.WaitForWorkerIdleAsync();

        // The stall was proven and the clamped counter crossed the threshold: the job executed
        // early (in-process skew handling) instead of re-publishing with a wrapped counter.
        Assert.Equal(["job"], audit.Ran);
    }

    [Fact]
    public async Task SkewProvenTimer_WaitsInProcess_InsteadOfMintingAWakeThatForgetsTheProof()
    {
        // Persistent clock skew: the transport keeps handing the wake-up back before its due time.
        // The executor tolerates one anomaly and executes on the SECOND consecutive stall — but
        // that proof lives in the ENVELOPE, so a timer step that suspends again mints a fresh
        // wake-up with the counters back at zero and the whole cycle repeats forever: the run
        // re-enters its timer step over and over and never finishes. While the skew marker is set,
        // the step must wait out the remainder in process instead.
        var recorder = new StepRecorder();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(recorder);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ConfigurableSleepFlow, ReminderInput>();
        });

        ConfigurableSleepFlow.Delay = TimeSpan.FromMinutes(30);
        var run = await harness.StartFlowAsync<ConfigurableSleepFlow, ReminderInput>(new ReminderInput("acme"));
        await run.WaitForTimerStepAsync("nap");
        await harness.Engine.WaitForWorkerIdleAsync();

        // Re-deliver the parked wake-up early, twice: the first is treated as an anomaly and
        // re-published, the second proves the stall and releases the job to the flow.
        var transport = (InMemoryWorkerTransport)harness.Engine.Services.GetRequiredService<IWorkerTransport>();
        var parked = Assert.Single(transport.SnapshotDelayedJobs());
        for (var attempt = 0; attempt < 2; attempt++)
        {
            await transport.PublishAsync(new WorkerJobEnvelope
            {
                Call = parked.Call,
                CorrelationId = parked.CorrelationId,
                NotBeforeUtc = parked.NotBeforeUtc,
                LastRedelayRemaining = parked.NotBeforeUtc - harness.Clock.GetUtcNow().UtcDateTime,
                RedelayStallCount = attempt
            });

            // Only the first hop settles back onto the timer wheel. The second proves the stall,
            // so the flow takes the job and holds it while it waits out the remainder in process —
            // "not idle" is the fix working.
            if (attempt == 0)
                await harness.Engine.WaitForWorkerIdleAsync();
        }

        await harness.AdvanceAsync(TimeSpan.FromMinutes(30));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
        Assert.Equal(1, recorder.Count("after-nap"));

        // The load-bearing assertion. The timer step is entered exactly twice: once to park it,
        // once when the skew-proven delivery releases it — and that second entry waits out the
        // remainder in process, so it completes the step itself. Pre-fix that entry re-suspended
        // and minted a fresh wake-up with the stall counters reset, adding a THIRD entry here and
        // one more for every skewed lap in production, which is the loop that never converged.
        Assert.Equal(2, run.StepExecutions("nap"));
    }

    [Fact]
    public async Task DelayBeyondThePersistenceCeiling_IsRejected()
    {
        await using var harness = await AsyncResponseTestHarness.StartAsync();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            harness.Builder.EnqueueWorkerAsync<IDeferredWorkAudit>(
                worker => worker.RanAsync("never"),
                TimeSpan.FromDays(5000)));
    }

    [Fact]
    public async Task DelayedTransports_AdvertiseTheirPerHopCaps()
    {
        await using var harness = await AsyncResponseTestHarness.StartAsync();
        var inMemory = Assert.IsAssignableFrom<IDelayedWorkerTransport>(harness.Services.GetRequiredService<IWorkerTransport>());
        Assert.True(inMemory.MaxPublishDelay > TimeSpan.FromDays(1));

        // The SQS transport chunks at the DelaySeconds ceiling.
        Assert.Equal(TimeSpan.FromSeconds(900), AsyncResponse.Transports.SQS.SqsWorkerTransport.SqsMaxDelay);
    }
}
