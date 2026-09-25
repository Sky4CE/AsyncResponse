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

    /// <summary>Records every delayed hop; optionally fails each one (delayed capacity exhausted, host draining).</summary>
    private sealed class RecordingDelayedTransport : IDelayedWorkerTransport
    {
        private readonly List<(WorkerJobEnvelope Job, TimeSpan Delay)> _hops = [];

        public bool FailHops { get; init; }

        public TimeSpan MaxPublishDelay { get; init; } = TimeSpan.FromMinutes(15);

        public IReadOnlyList<(WorkerJobEnvelope Job, TimeSpan Delay)> Hops
        {
            get { lock (_hops) return [.. _hops]; }
        }

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default)
        {
            lock (_hops) _hops.Add((job, delay));
            return FailHops
                ? Task.FromException(new InvalidOperationException("delayed publish rejected (capacity exhausted)"))
                : Task.CompletedTask;
        }
    }

    [Fact]
    public async Task FailedRedelayHop_RetriedOnTheSameEnvelope_NeverReadsAsAStall()
    {
        // Regression (fixpoint r1): the executor stamped the next hop's stall counters on the
        // DELIVERED envelope before publishing it. The in-memory transport retries that very
        // instance when the in-job publish throws (delayed capacity exhausted, host draining), so
        // each retry 100 ms later saw its own stamped remainder barely shrunk — "no progress" —
        // and the second retry "proved" clock skew and ran the job early: a durable timer would
        // spend the forced-early marker on it and fail terminally with more than the timer
        // ceiling left. The hop is now a copy; the delivered envelope is never modified.
        var audit = new RecordingDeferredWorkAudit();
        await using var provider = new ServiceCollection().AddSingleton<IDeferredWorkAudit>(audit).BuildServiceProvider();
        var clock = new VirtualTimeProvider();
        var transport = new RecordingDelayedTransport { FailHops = true };
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance,
            transport,
            clock);

        var delivered = new WorkerJobEnvelope
        {
            Call = Work(),
            JobId = "job-1",
            NotBeforeUtc = clock.GetUtcNow().UtcDateTime.AddDays(60)
        };

        for (var attempt = 0; attempt < 4; attempt++)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync(delivered));
            clock.Advance(TimeSpan.FromMilliseconds(100)); // the in-memory ladder's retry backoff
        }

        // Never executed early, and the transport's envelope carries no stall evidence of its own.
        Assert.Empty(audit.Ran);
        Assert.Null(delivered.LastRedelayRemaining);
        Assert.Equal(0, delivered.RedelayStallCount);

        // Every attempt published a fresh copy of the same job with a first hop's counters.
        Assert.Equal(4, transport.Hops.Count);
        Assert.All(transport.Hops, hop =>
        {
            Assert.NotSame(delivered, hop.Job);
            Assert.Equal("job-1", hop.Job.JobId);
            Assert.Equal(delivered.NotBeforeUtc, hop.Job.NotBeforeUtc);
            Assert.NotNull(hop.Job.LastRedelayRemaining);
            Assert.Equal(0, hop.Job.RedelayStallCount);
            Assert.Equal(transport.MaxPublishDelay, hop.Delay);
        });
    }

    [Fact]
    public async Task OffsetStampedDueTime_IsComparedAsAUtcInstant()
    {
        // Regression (fixpoint r1): NotBeforeUtc is wire data, and System.Text.Json reads a
        // timestamp carrying an offset ("+00:00", what most non-.NET producers write) as LOCAL
        // time, while DateTime subtraction ignores Kind — so on any host whose zone is not UTC the
        // due time moved by the host's offset (re-delayed hours too long east of Greenwich, run
        // early west of it). The delay assertion discriminates only on a non-UTC host (CI runners
        // are UTC); the Kind assertion on every host.
        var audit = new RecordingDeferredWorkAudit();
        await using var provider = new ServiceCollection().AddSingleton<IDeferredWorkAudit>(audit).BuildServiceProvider();
        var clock = new VirtualTimeProvider();
        var transport = new RecordingDelayedTransport { MaxPublishDelay = TimeSpan.FromDays(1) };
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance,
            transport,
            clock);

        var dueUtc = clock.GetUtcNow().UtcDateTime.AddHours(2);
        // Invariant, with literal separators: a culture-formatted timestamp is not ISO 8601 (':'
        // is the culture's time separator — '.' in fi-FI — and th-TH counts Buddhist-era years).
        var dueText = dueUtc.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss", System.Globalization.CultureInfo.InvariantCulture);
        var json = $$"""
            {
              "SchemaVersion": {{WorkerJobEnvelopeSchema.Current}},
              "Call": {{AsyncResponseJson.Serialize(Work())}},
              "NotBeforeUtc": "{{dueText}}+00:00"
            }
            """;
        var job = AsyncResponseJson.DeserializeCaseInsensitive<WorkerJobEnvelope>(System.Text.Encoding.UTF8.GetBytes(json))!;

        await executor.ExecuteAsync(job);

        Assert.Empty(audit.Ran);
        var hop = Assert.Single(transport.Hops);
        Assert.Equal(TimeSpan.FromHours(2), hop.Delay);
        Assert.Equal(dueUtc, hop.Job.NotBeforeUtc);
        Assert.Equal(DateTimeKind.Utc, hop.Job.NotBeforeUtc!.Value.Kind);
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
