using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regressions for round 36 (external holistic review of 145aa8c): behavior pins that compile
/// against the pre-fix tree and fail there. Pins over API this round introduced
/// (<c>InMemoryWorkerTransportOptions.InJobOverflowCapacity</c>, the overflow metrics, the
/// ancestor-depth constant) live in <see cref="Round36NewApiTests"/>; the per-channel
/// body-free-reader pins live next to each channel's existing malformed-envelope tests, and the
/// Cosmos lease-patch pins in <c>CosmosDurableFlowStateStoreTests</c>.
/// </summary>
public sealed class Round36RegressionTests
{
    public sealed record R36Input(DateTimeOffset Occurrence);

    public sealed class R36NoopFlow : IDurableFlow<R36Input>
    {
        public Task ExecuteAsync(IDurableFlowContext flow, R36Input input) => Task.CompletedTask;
    }

    /// <summary>
    /// A payload whose dictionary keys come straight off the wire: a value that fails to convert
    /// makes System.Text.Json report <c>Path: $.Payload.Values['&lt;key&gt;']</c> — the key IS
    /// body content, and it used to reach the application log and the waiter verbatim.
    /// </summary>
    public sealed class LeakProbePayload : IAsyncResponsePayload
    {
        public Dictionary<string, int>? Values { get; set; }

        public RecoveryAction OnRecovery() => RecoveryAction.Resume;
    }

    /// <summary>The marker no log line or exception chain may carry (a synthetic private identifier).</summary>
    public const string Marker = "private_customer_42@example.invalid";

    /// <summary>A success envelope whose only defect is a string where <see cref="LeakProbePayload.Values"/> wants an int.</summary>
    public const string LeakingEnvelope =
        """{"SchemaVersion":1,"Success":true,"Payload":{"Values":{"private_customer_42@example.invalid":"not-a-number"}}}""";

    /// <summary>Asserts neither the exception chain nor anything the channel logged carries the marker.</summary>
    internal static void AssertNoMarker(Exception exception, CollectingLogger logger)
    {
        Assert.DoesNotContain(Marker, exception.ToString(), StringComparison.Ordinal);
        foreach (var (message, logged) in logger.Entries)
        {
            Assert.DoesNotContain(Marker, message, StringComparison.Ordinal);
            if (logged is not null)
                Assert.DoesNotContain(Marker, logged.ToString(), StringComparison.Ordinal);
        }

        // The diagnosis is still there: the failure is located by size and position.
        Assert.Contains("Failed to parse JSON payload", exception.ToString(), StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------------------------------------
    // F1 — the scheduler's re-drive settled an occurrence whose start had never been published.
    //      Round 35 made a start publish-first (a failed publish persists NOTHING), but the re-drive
    //      still read "no ledger" as "expired or deleted; give up" — so every occurrence that fell
    //      due during a broker outage was lost for good once the outage outlasted the start's own
    //      retry ladder, and the startup probe could not find a run that was never persisted either.

    /// <summary>A worker transport that can be told to refuse, and records what it accepted.</summary>
    private sealed class FailableWorkerTransport : IWorkerTransport
    {
        private readonly List<WorkerJobEnvelope> _published = [];
        private int _attempts;

        public volatile bool Fail;

        public int Attempts => Volatile.Read(ref _attempts);

        public int PublishedCount
        {
            get { lock (_published) return _published.Count; }
        }

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _attempts);
            if (Fail)
                throw new TimeoutException("simulated broker outage");

            lock (_published)
                _published.Add(job);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Fires virtual timers one at a time — never two due instants in one advance, so a re-drive
    /// timer and the next occurrence cannot collapse into a single pass — until the condition holds.
    /// </summary>
    private static async Task AdvanceUntilAsync(VirtualTimeProvider clock, Func<bool> done, string what)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        while (!done())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Timed out waiting for {what}.");

            if (clock.NextTimerDueAt is { } due)
                clock.Advance(due - clock.GetUtcNow() + TimeSpan.FromMilliseconds(1));
            else
                await Task.Delay(5);
        }
    }

    /// <summary>
    /// Pre-fix failure: after the outage the re-drive made zero further publish attempts and
    /// settled the entry — the transport never sees a second job, and no ledger ever appears.
    /// Proven with the REAL starter (<c>DurableFlowService</c>) and the real scheduler loop; the
    /// round-34 pin passed because its fake created a ledger before failing, the pre-round-35 order.
    /// </summary>
    [Fact]
    public async Task ScheduledFlow_OccurrenceWhosePublishFailed_IsRedrivenByTheRealStarterUntilPublished()
    {
        var clock = new VirtualTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 30, TimeSpan.Zero));
        var transport = new FailableWorkerTransport { Fail = true };
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<TimeProvider>(clock);
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<R36NoopFlow, R36Input>();
        services.AddSingleton<IWorkerTransport>(transport);
        await using var provider = services.BuildServiceProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();
        var store = provider.GetRequiredService<InMemoryFlowStateStore>();

        var registration = new ScheduledFlowRegistration
        {
            Name = "r36-hourly",
            CronExpression = "0 * * * *",
            Options = new ScheduledFlowOptions(),
            StartOccurrenceAsync = static (durableFlows, flowId, occurrence, cancellationToken) =>
                durableFlows.StartAsync<R36NoopFlow, R36Input>(new R36Input(occurrence), flowId, cancellationToken)
        };
        var occurrenceId = ScheduledFlowService.OccurrenceFlowId("r36-hourly", new DateTimeOffset(2030, 1, 1, 1, 0, 0, TimeSpan.Zero));

        using var scheduler = new ScheduledFlowService(flows, [registration], NullLogger<ScheduledFlowService>.Instance, clock);
        await scheduler.StartAsync(CancellationToken.None);
        try
        {
            // 01:00 falls due inside the outage: the start's whole retry ladder (4 attempts) fails.
            await AdvanceUntilAsync(clock, () => transport.Attempts >= 4, "the start's retry ladder to exhaust");
            await Task.Delay(50);
            Assert.Equal(4, transport.Attempts);
            Assert.Equal(0, transport.PublishedCount);
            // Publish-first: the failed start persisted nothing — exactly the shape the old
            // re-drive misread as "expired".
            Assert.Null(await store.LoadAsync(occurrenceId));

            // The broker is back. The re-drive (RedriveInterval later) must START the occurrence
            // again; pre-fix it loaded the never-created ledger, read the null as gone, and settled.
            transport.Fail = false;
            await AdvanceUntilAsync(clock, () => transport.PublishedCount >= 1, "the re-drive to publish the start job");
            Assert.Equal(1, transport.PublishedCount);
            Assert.Equal(5, transport.Attempts);
            // The starter's own create ran after its publish, so the run now exists.
            Assert.NotNull(await store.LoadAsync(occurrenceId));

            // Settled: nothing else is published before the next occurrence (02:00 is far away).
            clock.Advance(TimeSpan.FromMinutes(5));
            await Task.Delay(100);
            Assert.Equal(1, transport.PublishedCount);
        }
        finally
        {
            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // F2 — AwaitChildFlowAsync handed the first completion the FULL loaded child (ambient Context,
    //      grandchild results and all) but every replay the reduced snapshot read back from the memo,
    //      so a parent could branch differently after a restart on a step it had already completed.

    private static FlowState State(string id, string? parent = null, FlowRunStatus status = FlowRunStatus.Running) => new()
    {
        FlowId = id,
        ParentFlowId = parent,
        Status = status,
        FlowTypeName = typeof(R36NoopFlow).FullName,
        InputTypeName = typeof(R36Input).FullName,
        InputJson = JsonSerializer.Serialize(new R36Input(DateTimeOffset.UnixEpoch)),
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    private static ServiceProvider BuildContextProvider(IWorkerTransport transport, TimeProvider clock)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(clock);
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<R36NoopFlow, R36Input>();
        services.AddSingleton(transport);
        return services.BuildServiceProvider();
    }

    private static DurableFlowContext CreateContext(
        ServiceProvider provider,
        FlowState state,
        IFlowStateStore store,
        FlowExecutionLease lease,
        DurableFlowOptions options,
        TimeProvider clock,
        IWorkerTransport transport,
        ILogger? logger = null)
        => new(
            state,
            store,
            provider.GetRequiredService<IAsyncResponseBuilder>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>(),
            options,
            provider.GetRequiredService<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            logger ?? NullLogger.Instance,
            lease,
            clock,
            workerTransport: transport);

    /// <summary>
    /// Pre-fix failure: the first call returns the child with its Context (1 entry) and the
    /// grandchild step's <c>ResultJson</c>; the replay returns neither — two different objects for
    /// one completed step.
    /// </summary>
    [Theory]
    [InlineData(FlowRunStatus.Succeeded)]
    [InlineData(FlowRunStatus.Failed)]
    public async Task AwaitChildFlow_FirstCompletionAndReplay_ReturnTheSameMemoizedSnapshot(FlowRunStatus childStatus)
    {
        var clock = new VirtualTimeProvider();
        var transport = new FailableWorkerTransport();
        await using var provider = BuildContextProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var parentId = $"r36-snapshot-{childStatus}";
        var parent = State(parentId);
        var child = State($"{parentId}:child", parentId, childStatus);
        child.ParentStepName = "child";
        child.LastMessage = childStatus == FlowRunStatus.Failed ? "child failed" : "child done";
        child.Context = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "demo" };
        child.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal)
        {
            ["local"] = new() { Completed = true, ResultJson = """{"kept":true}""" },
            ["grandchild"] = new() { Completed = true, ChildFlowId = $"{parentId}:child:grandchild", ResultJson = """{"Status":1}""" }
        };
        Assert.True(await store.TryCreateAsync(parentId, parent, TimeSpan.FromDays(1)));
        Assert.True(await store.TryCreateAsync(child.FlowId!, child, TimeSpan.FromDays(1)));

        var options = new DurableFlowOptions();
        await using var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, parentId, options, NullLogger.Instance, clock))!;
        var context = CreateContext(provider, parent, store, lease, options, clock, transport);
        var input = new R36Input(DateTimeOffset.UnixEpoch);

        var first = await context.AwaitChildFlowAsync<R36NoopFlow, R36Input>("child", input, failOnChildFailure: false);
        var replay = await context.AwaitChildFlowAsync<R36NoopFlow, R36Input>("child", input, failOnChildFailure: false);

        // Both are the memoized snapshot: no ambient context, grandchild result elided, the rest kept.
        Assert.Null(first.Context);
        Assert.Null(first.Steps!["grandchild"].ResultJson);
        Assert.Equal($"{parentId}:child:grandchild", first.Steps["grandchild"].ChildFlowId);
        Assert.Equal("""{"kept":true}""", first.Steps["local"].ResultJson);
        Assert.Equal(childStatus, first.Status);
        Assert.Equal(child.LastMessage, first.LastMessage);
        Assert.Equal(FlowStateJson.Serialize(first), FlowStateJson.Serialize(replay));
        // And the stored child itself is untouched by the memoization.
        var stored = await store.LoadAsync(child.FlowId!);
        Assert.Equal("demo", stored!.Context!["tenant"]);
        Assert.Equal("""{"Status":1}""", stored.Steps!["grandchild"].ResultJson);
    }

    // ---------------------------------------------------------------------------------------------
    // F3 — the ledger reader chained the raw System.Text.Json exception, whose message carries
    //      `Path: $.Values['<key>']` built from the stored dictionary keys, into
    //      FlowStateUnreadableException — which the worker ingress logs in full. The channel
    //      readers' pins live next to each channel's existing malformed-envelope tests.

    private const string LeakingLedger =
        """{"SchemaVersion":1,"FlowId":"r36-leak","Values":{"private_customer_42@example.invalid":123}}""";

    /// <summary>Pre-fix failure: the inner JsonException's message quotes the dictionary key.</summary>
    [Fact]
    public void FlowStateJson_MalformedLedger_DoesNotEchoStoredKeysIntoTheExceptionChain()
    {
        var ex = Assert.Throws<FlowStateUnreadableException>(() => FlowStateJson.Deserialize(LeakingLedger, "r36-leak"));

        Assert.Equal("r36-leak", ex.FlowId);
        Assert.Contains("malformed", ex.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain(Marker, ex.ToString(), StringComparison.Ordinal);
        Assert.Contains("Failed to parse JSON payload", ex.ToString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// The same reader, reached the way an attacker (or a schema mismatch) reaches it: a start
    /// job whose carrier is malformed, delivered through the worker ingress. Pre-fix failure: the
    /// key is in the exception the ingress throws and in the error it logs.
    /// </summary>
    [Fact]
    public async Task StartCarrier_MalformedInitialState_DoesNotEchoItsKeysIntoIngressLogsOrTheException()
    {
        var logger = new CollectingLogger();
        var transport = new FailableWorkerTransport();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<R36NoopFlow, R36Input>();
        services.AddSingleton<IWorkerTransport>(transport);
        // After the open-generic NullLogger, so the closed registration wins for the ingress.
        services.AddSingleton<ILogger<AsyncResponseIngress>>(logger.For<AsyncResponseIngress>());
        await using var provider = services.BuildServiceProvider();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        var envelope = new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IDurableFlowExecutor).FullName!,
                MethodName = nameof(IDurableFlowExecutor.CreateAndExecuteAsync),
                Params = [CallbackParam.ForValue("r36-leak"), CallbackParam.ForValue(LeakingLedger)]
            }
        };

        var ex = await Assert.ThrowsAsync<FlowStateUnreadableException>(
            () => ingress.HandleWorkerMessageAsync(JsonSerializer.Serialize(envelope)));

        AssertNoMarker(ex, logger);
        Assert.Contains(logger.Entries, entry => entry.Exception is FlowStateUnreadableException);
    }

    // ---------------------------------------------------------------------------------------------
    // F3 follow-up — the first cut of the body-free reader scrubbed EVERY JsonException, including
    //      the six the envelope converter authors itself. Those name only the wire contract's own
    //      properties (SchemaVersion, Success, Payload) and never a byte of the body, and they are
    //      the primary operator diagnosis for the commonest malformed-envelope cause in production:
    //      a foreign or mismatched producer writing to the response channel. Replacing
    //      "SchemaVersion is required." with "failed at line 0, byte position 2" cost the diagnosis
    //      and protected nothing. (Caught by the integration suite, which pins these messages.)

    public static TheoryData<string, string> WireContractViolations() => new()
    {
        { "{}", "SchemaVersion is required." },
        { """{"SchemaVersion":1,"Success":true}""", "Payload is null or absent" },
        { """{"SchemaVersion":1,"Success":true,"Payload":null}""", "Payload is null or absent" },
        { """{"SchemaVersion":"one","Success":true}""", "SchemaVersion must be an integer." },
        { """{"SchemaVersion":1,"Success":"yes"}""", "Success must be a boolean." },
        { """{"SchemaVersion":1,"Success":false,"ExceptionMessage":7}""", "ExceptionMessage must be a string or null." },
        { "[]", "must be a JSON object" },
    };

    /// <summary>
    /// Pre-fix failure (of the round-36 F3 fix itself): every one of these came back as
    /// "Failed to parse JSON payload (N UTF-16 code units) at line …", with the reason gone.
    /// </summary>
    [Theory]
    [MemberData(nameof(WireContractViolations))]
    public void EnvelopeContractViolation_KeepsItsDiagnosis_BecauseItNamesNoBody(string envelopeJson, string expectedReason)
    {
        var ex = Assert.ThrowsAny<JsonException>(
            () => JsonSafety.SafeDeserialize(envelopeJson, AsyncResponseEnvelopeJson.TypeInfo<OperationResult>()));

        Assert.Contains(expectedReason, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Failed to parse JSON payload", ex.Message, StringComparison.Ordinal);
        // Still a JsonException, so the ingress keeps classifying it as permanent (no retry burn)
        // and application code catching JsonException still catches it.
        Assert.IsAssignableFrom<JsonException>(ex);
    }

    /// <summary>
    /// The other half of the same contract: a failure the READER authored is still scrubbed, even
    /// though it arrives through the very same call. The discriminator is who wrote the message,
    /// not which reader threw it.
    /// </summary>
    [Fact]
    public void PayloadConversionFailure_IsStillScrubbed_EvenThoughTheConverterRanFirst()
    {
        var ex = Assert.Throws<InvalidDataException>(
            () => JsonSafety.SafeDeserialize(LeakingEnvelope, AsyncResponseEnvelopeJson.TypeInfo<LeakProbePayload>()));

        Assert.DoesNotContain(Marker, ex.ToString(), StringComparison.Ordinal);
        Assert.Contains("Failed to parse JSON payload", ex.Message, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------------------------------------
    // F4 — an in-job publish that found the queue full spilled into an UNBOUNDED overflow: with
    //      QueueCapacity = 1 a fan-out handler could park ten thousand envelopes (each with its
    //      captured ExecutionContext) with the configured capacity giving no signal at all.

    private static WorkerJobEnvelope Job() => new()
    {
        Call = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "AsyncResponse.Tests.IRound36Probe",
            MethodName = "RunAsync",
            Params = []
        }
    };

    /// <summary>
    /// Pre-fix failure: every one of the 6 000 follow-up publishes is accepted (outstanding =
    /// 6 000, overflow = 5 999). Now the queue slot plus the default in-job overflow (4 096) are
    /// accepted and the next publish is rejected with the count undone.
    /// </summary>
    [Fact]
    public async Task InMemoryTransport_InJobPublishes_AreRejectedPastTheDefaultOverflowBound()
    {
        var transport = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions { QueueCapacity = 1 }));
        var accepted = 0;
        InvalidOperationException? rejection = null;

        await Task.Run(async () =>
        {
            InMemoryWorkerTransport.InJobScope.MarkActive();
            for (var i = 0; i < 6_000; i++)
            {
                try
                {
                    await transport.PublishAsync(Job());
                    accepted++;
                }
                catch (InvalidOperationException ex)
                {
                    rejection = ex;
                    break;
                }
            }
        });

        Assert.NotNull(rejection);
        Assert.Equal(1 + 4_096, accepted);
        Assert.Equal(1 + 4_096, transport.OutstandingJobs);
        Assert.Contains("InJobOverflowCapacity", rejection.Message, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------------------------------------
    // F7 — a child's long park extended its ancestors' ledgers best-effort: a failed ancestor write
    //      was logged and swallowed, and the walk stopped silently after 16 levels. The child then
    //      published its wake-up and parked "successfully" while the parent it would complete into
    //      expired underneath it.

    /// <summary>A delayed-capable transport that only records what it was asked to publish.</summary>
    private sealed class CapturingDelayedTransport : IDelayedWorkerTransport
    {
        public readonly List<WorkerJobEnvelope> Jobs = [];

        public TimeSpan MaxPublishDelay => TimeSpan.FromDays(30);

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            lock (Jobs)
                Jobs.Add(job);
            return Task.CompletedTask;
        }

        public Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default)
            => PublishAsync(job, cancellationToken);

        public int Count
        {
            get { lock (Jobs) return Jobs.Count; }
        }
    }

    /// <summary>Fails the lease-less (ancestor TTL) writes of one flow id, counting them, and can hang a lease release.</summary>
    private sealed class FaultingFlowStateStore(IFlowStateStore inner) : IFlowStateStore
    {
        private int _failedAncestorUpdates;

        public volatile string? FailAncestorUpdates;
        public bool HangRelease;
        public CancellationToken ReleaseToken { get; private set; }
        public TaskCompletionSource ReleaseEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ReleaseGate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int FailedAncestorUpdates => Volatile.Read(ref _failedAncestorUpdates);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
        {
            if (leaseId is null && string.Equals(flowId, FailAncestorUpdates, StringComparison.Ordinal))
            {
                Interlocked.Increment(ref _failedAncestorUpdates);
                throw new TimeoutException("simulated ancestor write outage");
            }

            return inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);
        }

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
        {
            ReleaseToken = cancellationToken;
            ReleaseEntered.TrySetResult();
            return HangRelease ? ReleaseGate.Task : inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);
        }

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);
    }

    private static readonly DurableFlowOptions ShortLedgerOptions = new()
    {
        StateExpiry = TimeSpan.FromMinutes(1),
        TimerInProcessThreshold = TimeSpan.Zero
    };

    /// <summary>
    /// Pre-fix failure: the first attempt throws <c>DurableFlowSuspendedException</c> with the
    /// wake-up already published and the root's TTL untouched, so two virtual minutes later the
    /// root is gone while the child's park (and its wake-up) live on. Now the park fails with the
    /// store's own exception and nothing published; the redelivered execution replays the parked
    /// timer, extends the chain, and only then publishes.
    /// </summary>
    [Fact]
    public async Task AncestorExtension_StoreOutage_FailsTheParkWithNothingPublished_AndTheRedeliveryExtendsTheChain()
    {
        var clock = new VirtualTimeProvider();
        var transport = new CapturingDelayedTransport();
        await using var provider = BuildContextProvider(transport, clock);
        var inner = provider.GetRequiredService<IFlowStateStore>();
        var store = new FaultingFlowStateStore(inner) { FailAncestorUpdates = "r36-root" };
        var root = State("r36-root");
        root.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal) { ["child"] = new() { ChildFlowId = "r36-root:child" } };
        var child = State("r36-root:child", "r36-root");
        child.ParentStepName = "child";
        Assert.True(await inner.TryCreateAsync("r36-root", root, TimeSpan.FromMinutes(1)));
        Assert.True(await inner.TryCreateAsync("r36-root:child", child, TimeSpan.FromMinutes(1)));

        // Attempt 1: the ancestor write fails. The park must fail too, before any wake-up exists.
        await using (var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, child.FlowId!, ShortLedgerOptions, NullLogger.Instance, clock))!)
        {
            var context = CreateContext(provider, child, store, lease, ShortLedgerOptions, clock, transport);
            await Assert.ThrowsAsync<TimeoutException>(() => context.DelayAsync("long-wait", TimeSpan.FromHours(1)));
        }

        Assert.Equal(0, transport.Count);
        Assert.Equal(1, store.FailedAncestorUpdates);

        // The store recovers; the redelivered execution replays the parked timer.
        store.FailAncestorUpdates = null;
        var replayed = (await inner.LoadAsync(child.FlowId!))!;
        Assert.NotNull(replayed.Steps!["long-wait"].WakeAtUtc);
        await using (var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, child.FlowId!, ShortLedgerOptions, NullLogger.Instance, clock))!)
        {
            var context = CreateContext(provider, replayed, store, lease, ShortLedgerOptions, clock, transport);
            await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => context.DelayAsync("long-wait", TimeSpan.FromHours(1)));
        }

        Assert.Equal(1, transport.Count);

        // The park's window is an hour; the parent outlives its own one-minute expiry.
        clock.Advance(TimeSpan.FromMinutes(2));
        Assert.NotNull(await inner.LoadAsync("r36-root"));
        Assert.NotNull(await inner.LoadAsync("r36-root:child"));
    }

    /// <summary>
    /// Pre-fix failure: the walk stopped after 16 ancestors, so the root of a 20-deep chain kept
    /// its one-minute expiry and was gone two virtual minutes into the leaf's hour-long park.
    /// </summary>
    [Fact]
    public async Task AncestorExtension_ReachesTheRootOfAChainDeeperThanSixteen()
    {
        var clock = new VirtualTimeProvider();
        var transport = new CapturingDelayedTransport();
        await using var provider = BuildContextProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();

        const int depth = 20;
        var ids = new List<string>();
        string? parent = null;
        for (var level = 0; level <= depth; level++)
        {
            var id = parent is null ? "r36-deep-root" : $"{parent}:c";
            var state = State(id, parent);
            if (parent is not null)
                state.ParentStepName = "c";
            if (level < depth)
                state.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal) { ["c"] = new() { ChildFlowId = $"{id}:c" } };
            Assert.True(await store.TryCreateAsync(id, state, TimeSpan.FromMinutes(1)));
            ids.Add(id);
            parent = id;
        }

        var leaf = (await store.LoadAsync(ids[^1]))!;
        await using (var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, leaf.FlowId!, ShortLedgerOptions, NullLogger.Instance, clock))!)
        {
            var context = CreateContext(provider, leaf, store, lease, ShortLedgerOptions, clock, transport);
            await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => context.DelayAsync("long-wait", TimeSpan.FromHours(1)));
        }

        clock.Advance(TimeSpan.FromMinutes(2));
        foreach (var id in ids)
            Assert.True(await store.LoadAsync(id) is not null, $"{id} expired under the leaf's park");
    }

    /// <summary>
    /// A ParentFlowId cycle is corrupted state. Pre-fix it was walked 16 times and the leaf
    /// parked anyway; now the run fails terminally and deterministically, naming the cycle.
    /// </summary>
    [Fact]
    public async Task AncestorExtension_CycleInTheParentChain_FailsTheRunTerminally()
    {
        var clock = new VirtualTimeProvider();
        var transport = new CapturingDelayedTransport();
        await using var provider = BuildContextProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        Assert.True(await store.TryCreateAsync("r36-cycle-a", State("r36-cycle-a", "r36-cycle-b"), TimeSpan.FromMinutes(1)));
        Assert.True(await store.TryCreateAsync("r36-cycle-b", State("r36-cycle-b", "r36-cycle-a"), TimeSpan.FromMinutes(1)));
        var leaf = State("r36-cycle-leaf", "r36-cycle-a");
        Assert.True(await store.TryCreateAsync("r36-cycle-leaf", leaf, TimeSpan.FromMinutes(1)));

        await using var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, "r36-cycle-leaf", ShortLedgerOptions, NullLogger.Instance, clock))!;
        var context = CreateContext(provider, leaf, store, lease, ShortLedgerOptions, clock, transport);

        var ex = await Assert.ThrowsAsync<DurableFlowFailedException>(() => context.DelayAsync("long-wait", TimeSpan.FromHours(1)));

        Assert.Contains("cycle", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, transport.Count);
    }

    // ---------------------------------------------------------------------------------------------
    // F8 — the lease holder's disposal released the lease with an unbounded, uncancelable call: a
    //      store whose release never answered kept a FINISHED execution's disposal — the executor's
    //      `await using`, the job's scope, the worker slot, the acknowledgement — pending forever.

    /// <summary>
    /// Pre-fix failure: disposal is still pending after eleven virtual seconds (and after ten
    /// virtual minutes), and the release received <c>CancellationToken.None</c>.
    /// </summary>
    [Fact]
    public async Task LeaseDisposal_HangingRelease_IsAbandonedAfterItsBudget_WithACancelableToken()
    {
        var clock = new VirtualTimeProvider();
        var inner = new InMemoryFlowStateStore(clock);
        var store = new FaultingFlowStateStore(inner) { HangRelease = true };
        var logger = new CollectingLogger();
        Assert.True(await inner.TryCreateAsync("r36-release", State("r36-release"), TimeSpan.FromDays(1)));
        var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, "r36-release", new DurableFlowOptions(), logger, clock))!;

        var dispose = lease.DisposeAsync().AsTask();
        await store.ReleaseEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(store.ReleaseToken.CanBeCanceled, "the release must receive a token the store can honor");
        Assert.False(dispose.IsCompleted);

        clock.Advance(TimeSpan.FromSeconds(11));
        await dispose.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(store.ReleaseToken.IsCancellationRequested);
        Assert.Contains(logger.Messages, message => message.Contains("release did not complete within", StringComparison.Ordinal));

        // The abandoned call eventually completing is observed, not fatal.
        store.ReleaseGate.SetResult();
        await Task.Delay(20);
    }
}
