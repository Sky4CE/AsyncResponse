using AsyncResponse.Testing;
using AsyncResponse.Transports.NATS;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Text;
using Xunit;
using static AsyncResponse.Tests.Round40FlowLeaseTestSupport;

namespace AsyncResponse.Tests;

/// <summary>
/// Review of 2026-09-21 (whole repository at dc1a2569) — the regressions that can be written
/// against the pre-existing API, so this file compiles, and fails, on that commit. The pins that
/// live next to their harnesses: the database-channel clock step in
/// <see cref="DbChannelSharedCoverageTests"/>, the Redis consumer name in
/// <see cref="RedisSubscriberTests"/>, the Redis cluster scan in
/// <see cref="RedisRecoveryStateStoreTests"/>, and the dead-letter text cuts in
/// <see cref="AzureServiceBusDispatcherTests"/> and <see cref="RabbitMqDispatcherTests"/>. The API
/// this round added is pinned in <see cref="Round41NewApiTests"/>.
/// </summary>
public sealed class Round41RegressionTests
{
    // ---------------------------------------------------------------------------------------
    // F1 (HIGH): round 40 moved the contended wake-up's deadline to the lease's PERSISTED expiry
    // plus a window — with no ceiling. The expiry is data the waiting host does not control: a
    // store clock ahead of this host's, or an expiry column read back shifted, moved the deadline
    // as far out as the value said (saturating at DateTime.MaxValue, i.e. never). The delivery
    // then polled the store for good, pinning its worker slot, and the contention exception —
    // whose message says "check for clock skew" — was unreachable in exactly that case.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task ContendedWakeUp_BehindAnUnreachablyFarPersistedExpiry_IsHandedBackToTheTransport_NotPolledForever()
    {
        var time = new VirtualTimeProvider();
        var store = await UnacquirableStore.CreateAsync("r41-far-expiry", new FlowLeaseObservation("skewed-owner", DateTime.MaxValue));
        await using var harness = CreateHarness(store, new DurableFlowOptions(), time);

        var execution = harness.Executor.ExecuteAsync("r41-far-expiry");

        // Three virtual hours, one poll per advance: three times the default budget.
        for (var step = 0; step < 36 && !execution.IsCompleted; step++)
        {
            await WaitForArmedTimerOrCompletionAsync(time, execution);
            if (!execution.IsCompleted)
                time.Advance(TimeSpan.FromMinutes(5));
        }

        Assert.True(execution.IsCompleted, "The wake-up was still polling the store three virtual hours in: nothing bounds a wait that follows a persisted expiry.");
        await Assert.ThrowsAsync<DurableFlowLeaseContendedException>(() => execution);
        Assert.Equal(0, harness.Flow.Executions);
        Assert.Equal(FlowRunStatus.Running, (await store.LoadAsync("r41-far-expiry"))!.Status);
    }

    private static async Task WaitForArmedTimerOrCompletionAsync(VirtualTimeProvider time, Task execution)
    {
        var deadline = Stopwatch.StartNew();
        while (time.NextTimerDueAt is null && !execution.IsCompleted)
        {
            if (deadline.Elapsed > TimeSpan.FromSeconds(10))
                throw new TimeoutException("The contention poll armed no delay on the virtual clock.");
            await Task.Delay(1);
        }
    }

    private static ExecutorHarness CreateHarness(IFlowStateStore store, DurableFlowOptions options, TimeProvider timeProvider)
    {
        var services = new ServiceCollection();
        services.AddSingleton(store);
        services.AddSingleton<CountingFlow>();
        var provider = services.BuildServiceProvider();
        var builder = new Mock<IAsyncResponseBuilder>();
        builder.Setup(instance => instance.EnqueueWorkerAsync(
                It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var executor = new DurableFlowExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            builder.Object,
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            new AsyncResponseContextPropagation([]),
            options,
            NullLogger<DurableFlowExecutor>.Instance,
            timeProvider: timeProvider);
        return new ExecutorHarness(provider, executor);
    }

    /// <summary>A Running ledger whose lease this delivery can never win, reporting one fixed observation.</summary>
    internal sealed class UnacquirableStore(InMemoryFlowStateStore inner, FlowLeaseObservation observation) : IFlowStateStore
    {
        public static async Task<UnacquirableStore> CreateAsync(string flowId, FlowLeaseObservation observation)
        {
            var inner = new InMemoryFlowStateStore();
            await inner.TryCreateAsync(flowId, RunnableState(flowId), TimeSpan.FromMinutes(5));
            return new UnacquirableStore(inner, observation);
        }

        public Task<FlowLeaseObservation?> ObserveLeaseAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult<FlowLeaseObservation?>(observation);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => Task.FromResult(false);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);
    }

    // ---------------------------------------------------------------------------------------
    // F4: the scheduler's startup probe keeps the 64 most recent occurrences inside
    // StartupRedriveWindow, but found them by walking the WHOLE window forward from its far end —
    // and the window has no ceiling. A per-minute schedule with a long window evaluated the cron
    // expression millions of times at every startup, synchronously, ahead of the schedule's first
    // occurrence. The walk is now proportional to what the probe keeps.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task ScheduledFlow_StartupProbe_WithAWindowReachingTheEpoch_StillRedrivesPromptly()
    {
        var time = new VirtualTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 30, TimeSpan.Zero));
        var flows = new FakeFlows();

        // Eight per-minute schedules, each with sixty years of minutes (31.5 million occurrences)
        // between the epoch clamp and "now". Their probes run back to back — the fake store
        // answers synchronously — and only the LAST schedule has anything to re-drive, so the
        // re-drive is observed once every probe has run: tens of seconds of cron evaluation on
        // the old walk, a few hundred evaluations on the bounded one.
        const int schedules = 8;
        var undispatched = $"sched:every-minute-{schedules - 1}:20291231T235900Z";
        flows.States[undispatched] = new FlowState { FlowId = undispatched, Status = FlowRunStatus.Running, Attempts = 0 };
        var registrations = Enumerable.Range(0, schedules).Select(index => new ScheduledFlowRegistration
        {
            Name = $"every-minute-{index}",
            CronExpression = "* * * * *",
            Options = new ScheduledFlowOptions { StartupRedriveWindow = TimeSpan.MaxValue, TimeZone = NewYork() },
            StartOccurrenceAsync = static (durableFlows, flowId, occurrence, cancellationToken) =>
                durableFlows.StartAsync<ProbeFlow, DateTimeOffset>(occurrence, flowId, cancellationToken)
        }).ToArray();

        using var scheduler = new ScheduledFlowService(flows, registrations, NullLogger<ScheduledFlowService>.Instance, time);
        // StartAsync may run the probes inline (the loops only yield at their first real wait).
        var starting = Task.Run(() => scheduler.StartAsync(CancellationToken.None));
        try
        {
            await flows.WaitForStartsAsync(1, TimeSpan.FromSeconds(5));
            Assert.Equal(undispatched, flows.Starts.First());
        }
        finally
        {
            await starting;
            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    private static TimeZoneInfo NewYork() => TimeZoneInfo.FindSystemTimeZoneById("America/New_York");

    private sealed class ProbeFlow : IDurableFlow<DateTimeOffset>
    {
        public Task ExecuteAsync(IDurableFlowContext context, DateTimeOffset input) => Task.CompletedTask;
    }

    private sealed class FakeFlows : IDurableFlows
    {
        public ConcurrentDictionary<string, FlowState> States { get; } = new(StringComparer.Ordinal);

        public ConcurrentQueue<string> Starts { get; } = new();

        public Task<string> StartAsync<TFlow, TInput>(TInput input, string? flowId = null, CancellationToken cancellationToken = default)
            where TFlow : class, IDurableFlow<TInput>
        {
            Starts.Enqueue(flowId!);
            return Task.FromResult(flowId!);
        }

        public Task ResumeAsync(string flowId, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<FlowState?> GetStateAsync(string flowId, CancellationToken cancellationToken = default)
            => Task.FromResult(States.TryGetValue(flowId, out var state) ? state : null);

        public async Task WaitForStartsAsync(int count, TimeSpan budget)
        {
            var waited = Stopwatch.StartNew();
            while (Starts.Count < count)
            {
                if (waited.Elapsed > budget)
                    throw new TimeoutException($"Expected {count} start(s) within {budget}; saw [{string.Join(", ", Starts)}]. The startup probe is still walking its window.");
                await Task.Delay(10);
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // F6: a delayed publish reserves one of DelayedJobCapacity slots BEFORE its timer is armed,
    // and only the shutdown branch gave the slot back. A timer that could not be armed (a time
    // provider disposed by a finished fixture, one that rejects the delay) left the slot held by
    // a job that nothing would ever fire or drain — for the life of the transport. Enough of
    // those and every delayed publish was rejected (inside a job) or blocked forever (outside).
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task DelayedPublish_WhoseTimerCannotBeArmed_GivesItsCapacitySlotBack()
    {
        var clock = new FailingTimerClock();
        var transport = new InMemoryWorkerTransport(
            Options.Create(new InMemoryWorkerTransportOptions { DelayedJobCapacity = 1 }),
            clock);

        clock.FailNextTimer = true;
        await Assert.ThrowsAsync<ObjectDisposedException>(() => transport.PublishAsync(DelayedJob(), TimeSpan.FromMinutes(5)));
        Assert.Equal(0, transport.DelayedJobsHeld);

        // The ONE slot is free again: the next delayed publish is accepted instead of waiting on a
        // slot held by a job that does not exist.
        await transport.PublishAsync(DelayedJob(), TimeSpan.FromMinutes(5)).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, transport.DelayedJobsHeld);
        Assert.Single(transport.SnapshotDelayedJobs());
    }

    private static WorkerJobEnvelope DelayedJob() => new()
    {
        Call = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "AsyncResponse.Tests.IRound41Probe",
            MethodName = "RunAsync",
            Params = []
        }
    };

    private sealed class FailingTimerClock : TimeProvider
    {
        public bool FailNextTimer { get; set; }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            if (!FailNextTimer)
                return base.CreateTimer(callback, state, dueTime, period);

            FailNextTimer = false;
            throw new ObjectDisposedException(nameof(FailingTimerClock));
        }
    }

    // ---------------------------------------------------------------------------------------
    // F7: the id excerpt cut an offending id at a fixed UTF-16 index. With a non-BMP character
    // straddling unit 40 the cut kept the high surrogate and dropped its low half — so the helper
    // that quotes an id inside the "unpaired surrogate" rejection could mint an unpaired
    // surrogate of its own, in a message that is logged and persisted as UTF-8. (Fixpoint r2
    // S3#9: the unescaped PortableText.Excerpt was dead code and is gone; ids are quoted through
    // DiagnosticText.EscapedExcerpt, which cuts the same way.)
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void IdExcerpt_NeverEndsInsideASurrogatePair()
    {
        var id = new string('a', 39) + "\U0001F600" + new string('b', 20);

        var excerpt = DiagnosticText.EscapedExcerpt(id);

        Assert.Equal(-1, PortableText.IndexOfIllFormedUtf16(excerpt));
        Assert.Equal(new string('a', 39) + "…", excerpt);
    }

    [Fact]
    public void FlowIdRejection_QuotingAnIdCutAtASurrogatePair_IsWellFormedText()
    {
        // Over the 400-unit limit, so the rejection quotes it — with the pair on the excerpt's edge.
        var flowId = new string('a', 39) + "\U0001F600" + new string('b', 400);

        var rejection = FlowStateConcurrency.FlowIdNotPortable(flowId);

        Assert.NotNull(rejection);
        Assert.Equal(-1, PortableText.IndexOfIllFormedUtf16(rejection));
        // Round-trips through UTF-8 unchanged: nothing was replaced with U+FFFD on the way.
        Assert.Equal(rejection, Encoding.UTF8.GetString(Encoding.UTF8.GetBytes(rejection)));
    }

    // ---------------------------------------------------------------------------------------
    // F10: the correlation-id walker's duplicate-key scan allocated a fresh HashSet — and every
    // growth step on the way to the object's width — for every segment of every configured path
    // of every delivered message, and read JsonProperty.Name (a new string per READ) three times
    // per property. The walk is synchronous, so one set per thread serves every segment, and the
    // name is read once. Measured on this body: ~50 KB per message before, ~10 KB after.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void CorrelationIdWalk_DoesNotAllocateANameSetPerSegment()
    {
        // Three levels, a hundred properties each, the id at the bottom of the last one.
        static string Level(string tail) => "{" + string.Join(",", Enumerable.Range(0, 100).Select(i => $"\"p{i}\":{i}")) + "," + tail + "}";
        var body = Level("\"a\":" + Level("\"b\":" + Level("\"correlationId\":\"cid-41\"")));
        var options = new NatsAsyncResponseTransportOptions { CorrelationIdJsonPaths = ["a.b.correlationId"] };

        // Warm up: path split cache, JIT, the pooled JSON buffer, the thread's name set.
        for (var i = 0; i < 20; i++)
            Assert.Equal("cid-41", NatsCorrelationIdExtractor.Extract(headers: null, body, options));

        const int iterations = 200;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < iterations; i++)
            NatsCorrelationIdExtractor.Extract(headers: null, body, options);
        var perMessage = (GC.GetAllocatedBytesForCurrentThread() - before) / iterations;

        // What is left is one name string per property scanned (3 x 101 short strings, ~10 KB) and
        // the result. Either waste alone — two more reads of every name (~19 KB), or three sets
        // grown to a hundred names each (~21 KB) — lands well above this bound.
        Assert.True(perMessage < 18_000, $"{perMessage} bytes allocated per message.");
    }
}
