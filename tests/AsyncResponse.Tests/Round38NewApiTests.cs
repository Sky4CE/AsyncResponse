using AsyncResponse.Testing;
using AsyncResponse.Transports.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Diagnostics.Metrics;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round-38 pins over API the round introduced — they do not compile against the pre-fix tree, so
/// they live apart from the behavior pins in <see cref="Round38RegressionTests"/>.
/// </summary>
public sealed class Round38NewApiTests
{
    // ---------- F1: FlowState.RetainUntilUtc, the retention floor every ledger write honors ----------

    [Fact]
    public void RetainUntilUtc_IsOmittedFromTheLedgerWhenUnset_AndRoundTripsWhenSet()
    {
        var state = new FlowState { FlowId = "r38-wire", Status = FlowRunStatus.Running };
        Assert.DoesNotContain("RetainUntilUtc", FlowStateJson.Serialize(state), StringComparison.Ordinal);

        var floor = new DateTime(2026, 9, 11, 12, 0, 0, DateTimeKind.Utc);
        state.RetainUntilUtc = floor;
        var json = FlowStateJson.Serialize(state);
        Assert.Contains("\"RetainUntilUtc\":\"2026-09-11T12:00:00Z\"", json, StringComparison.Ordinal);
        Assert.Equal(floor, FlowStateJson.Deserialize(json, "r38-wire").RetainUntilUtc);
    }

    [Theory]
    [InlineData(FlowRunStatus.Running, true)]
    [InlineData(FlowRunStatus.Suspended, true)]
    [InlineData(FlowRunStatus.Succeeded, false)]
    [InlineData(FlowRunStatus.Failed, false)]
    public void EffectiveTtl_RaisesTheRequestedTtlToTheFloor_ForLiveRunsOnly(FlowRunStatus status, bool honored)
    {
        var now = new DateTime(2026, 9, 11, 12, 0, 0, DateTimeKind.Utc);
        var state = new FlowState { Status = status, RetainUntilUtc = now.AddHours(1) };

        var ttl = FlowStateRetention.EffectiveTtl(state, TimeSpan.FromMinutes(1), now);

        Assert.Equal(honored ? TimeSpan.FromHours(1) : TimeSpan.FromMinutes(1), ttl);
    }

    [Fact]
    public void EffectiveTtl_KeepsARequestedTtlAlreadyPastTheFloor_AndSaturatesAtThePersistenceCeiling()
    {
        var now = new DateTime(2026, 9, 11, 12, 0, 0, DateTimeKind.Utc);
        Assert.Equal(TimeSpan.FromHours(2), FlowStateRetention.EffectiveTtl(new FlowState { RetainUntilUtc = now.AddHours(1) }, TimeSpan.FromHours(2), now));
        Assert.Equal(TimeSpan.FromMinutes(1), FlowStateRetention.EffectiveTtl(new FlowState { RetainUntilUtc = now.AddHours(-1) }, TimeSpan.FromMinutes(1), now));
        Assert.Equal(TimeSpan.FromMinutes(1), FlowStateRetention.EffectiveTtl(new FlowState(), TimeSpan.FromMinutes(1), now));
        Assert.Equal(
            AsyncResponseChannelOptions.MaxPersistenceTtl,
            FlowStateRetention.EffectiveTtl(new FlowState { RetainUntilUtc = DateTime.MaxValue }, TimeSpan.FromMinutes(1), now));
    }

    [Fact]
    public void RaiseFloor_NeverLowersAnExistingFloor()
    {
        var now = new DateTime(2026, 9, 11, 12, 0, 0, DateTimeKind.Utc);
        var state = new FlowState();
        Assert.Equal(now.AddHours(2), FlowStateRetention.RaiseFloor(state, now, TimeSpan.FromHours(2)));
        Assert.Equal(now.AddHours(2), FlowStateRetention.RaiseFloor(state, now, TimeSpan.FromHours(1)));
        Assert.Equal(now.AddHours(3), FlowStateRetention.RaiseFloor(state, now, TimeSpan.FromHours(3)));
        Assert.True(FlowStateRetention.Covers(state, now.AddHours(3)));
        Assert.False(FlowStateRetention.Covers(state, now.AddHours(3).AddTicks(1)));
    }

    private static readonly DurableFlowOptions ShortLedgerOptions = new()
    {
        StateExpiry = TimeSpan.FromMinutes(1),
        DefaultStepTimeout = TimeSpan.FromSeconds(10),
        TimerInProcessThreshold = TimeSpan.Zero
    };

    private static FlowState State(string id, string? parent = null) => new()
    {
        FlowId = id,
        ParentFlowId = parent,
        Status = FlowRunStatus.Running,
        FlowTypeName = typeof(Round38RegressionTests.R38NoopFlow).FullName,
        InputTypeName = typeof(Round38RegressionTests.R38Input).FullName,
        InputJson = JsonSerializer.Serialize(new Round38RegressionTests.R38Input("x")),
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    /// <summary>
    /// The executor's per-attempt save and every checkpoint go through the lease: a plain
    /// one-minute save of a run whose floor is an hour out keeps the ledger for the hour. A
    /// terminal save does not — a failed run is not retained for the sleep it never finished.
    /// </summary>
    [Fact]
    public async Task LeaseSave_HonorsTheRetentionFloorForALiveRun_AndIgnoresItOnceTerminal()
    {
        var clock = new VirtualTimeProvider();
        var store = new InMemoryFlowStateStore(clock);
        var now = clock.GetUtcNow().UtcDateTime;
        var live = State("r38-live");
        var terminal = State("r38-terminal");
        Assert.True(await store.TryCreateAsync("r38-live", live, TimeSpan.FromMinutes(1)));
        Assert.True(await store.TryCreateAsync("r38-terminal", terminal, TimeSpan.FromMinutes(1)));

        await using (var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, "r38-live", ShortLedgerOptions, NullLogger.Instance, clock))!)
        {
            live.RetainUntilUtc = now.AddHours(1);
            await lease.SaveAsync(live, TimeSpan.FromMinutes(1));
        }

        await using (var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, "r38-terminal", ShortLedgerOptions, NullLogger.Instance, clock))!)
        {
            terminal.RetainUntilUtc = now.AddHours(1);
            terminal.Status = FlowRunStatus.Failed;
            await lease.SaveAsync(terminal, TimeSpan.FromMinutes(1));
        }

        clock.Advance(TimeSpan.FromMinutes(30));
        Assert.NotNull(await store.LoadAsync("r38-live"));
        Assert.Null(await store.LoadAsync("r38-terminal"));
    }

    [Fact]
    public async Task MutateAsync_HonorsTheRetentionFloor()
    {
        var clock = new VirtualTimeProvider();
        var store = new InMemoryFlowStateStore(clock);
        var state = State("r38-mutate");
        state.RetainUntilUtc = clock.GetUtcNow().UtcDateTime.AddHours(1);
        Assert.True(await store.TryCreateAsync("r38-mutate", state, TimeSpan.FromMinutes(1)));

        Assert.True(await FlowStateConcurrency.MutateAsync(store, "r38-mutate", TimeSpan.FromMinutes(1), clock, s =>
        {
            s.LastMessage = "recovered";
            return true;
        }));

        clock.Advance(TimeSpan.FromMinutes(30));
        Assert.Equal("recovered", (await store.LoadAsync("r38-mutate"))?.LastMessage);
    }

    private static ServiceProvider BuildProvider(IWorkerTransport transport, TimeProvider clock)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(Microsoft.Extensions.Logging.ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(clock);
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<Round38RegressionTests.R38NoopFlow, Round38RegressionTests.R38Input>();
        services.AddSingleton(transport);
        return services.BuildServiceProvider();
    }

    private static DurableFlowContext CreateContext(ServiceProvider provider, FlowState state, IFlowStateStore store, FlowExecutionLease lease, TimeProvider clock, IWorkerTransport transport)
        => new(
            state,
            store,
            provider.GetRequiredService<IAsyncResponseBuilder>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>(),
            ShortLedgerOptions,
            provider.GetRequiredService<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            NullLogger.Instance,
            lease,
            clock,
            workerTransport: transport);

    /// <summary>A leaf's park stamps the floor on itself and on every Running ancestor up to the root.</summary>
    [Fact]
    public async Task AncestorExtension_StampsTheRetentionFloorOnTheRunAndEveryAncestor()
    {
        var clock = new VirtualTimeProvider();
        var transport = new Round38RegressionTests.CapturingDelayedTransport();
        await using var provider = BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        Assert.True(await store.TryCreateAsync("r38-g", State("r38-g"), TimeSpan.FromMinutes(1)));
        Assert.True(await store.TryCreateAsync("r38-g:p", State("r38-g:p", "r38-g"), TimeSpan.FromMinutes(1)));
        var leaf = State("r38-g:p:c", "r38-g:p");
        Assert.True(await store.TryCreateAsync("r38-g:p:c", leaf, TimeSpan.FromMinutes(1)));

        await using (var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, "r38-g:p:c", ShortLedgerOptions, NullLogger.Instance, clock))!)
        {
            var context = CreateContext(provider, leaf, store, lease, clock, transport);
            await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => context.DelayAsync("long-wait", TimeSpan.FromHours(1)));
        }

        var wake = clock.GetUtcNow().UtcDateTime.AddHours(1);
        foreach (var id in new[] { "r38-g", "r38-g:p", "r38-g:p:c" })
        {
            var floor = (await store.LoadAsync(id))!.RetainUntilUtc;
            Assert.True(floor is { } f && f >= wake + ShortLedgerOptions.StateExpiry, $"{id} carries no floor covering the park (got {floor})");
        }

        // The floor is what an unrelated write of the parent honors: a lease-less recovery-style
        // mutation with the plain one-minute TTL leaves the parent alive for the hour.
        Assert.True(await FlowStateConcurrency.MutateAsync(store, "r38-g:p", TimeSpan.FromMinutes(1), clock, s => true));
        clock.Advance(TimeSpan.FromMinutes(30));
        Assert.NotNull(await store.LoadAsync("r38-g:p"));
        Assert.NotNull(await store.LoadAsync("r38-g"));
    }

    /// <summary>Wraps a store so every lease-less write of one id loses its revision race, and counts the attempts.</summary>
    private sealed class AlwaysLosingFlowStateStore(IFlowStateStore inner, string loseOn, DateTime? carryFloor) : IFlowStateStore
    {
        public int AncestorWriteAttempts { get; private set; }

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public async Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
        {
            if (leaseId is null && string.Equals(flowId, loseOn, StringComparison.Ordinal))
            {
                AncestorWriteAttempts++;
                var current = (await inner.LoadAsync(flowId, cancellationToken))!;
                var old = current.Revision;
                current.Revision++;
                if (carryFloor is { } floor)
                    current.RetainUntilUtc = floor;
                Assert.True(await inner.TryUpdateAsync(flowId, current, old, TimeSpan.FromMinutes(1), null, cancellationToken));
            }

            return await inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);
        }

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);
    }

    /// <summary>
    /// A competing write that already carries a floor reaching the park proves the retention: the
    /// extension re-reads, sees it, and moves on without a second write.
    /// </summary>
    [Fact]
    public async Task AncestorExtension_AConcurrentWriteThatCarriesTheFloor_EndsTheRetryWithoutAnotherWrite()
    {
        var clock = new VirtualTimeProvider();
        var transport = new Round38RegressionTests.CapturingDelayedTransport();
        await using var provider = BuildProvider(transport, clock);
        var inner = provider.GetRequiredService<IFlowStateStore>();
        var store = new AlwaysLosingFlowStateStore(inner, "r38-floored-root", carryFloor: clock.GetUtcNow().UtcDateTime.AddDays(2));
        Assert.True(await inner.TryCreateAsync("r38-floored-root", State("r38-floored-root"), TimeSpan.FromMinutes(1)));
        var child = State("r38-floored-root:c", "r38-floored-root");
        Assert.True(await inner.TryCreateAsync("r38-floored-root:c", child, TimeSpan.FromMinutes(1)));

        await using (var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, child.FlowId!, ShortLedgerOptions, NullLogger.Instance, clock))!)
        {
            var context = CreateContext(provider, child, store, lease, clock, transport);
            await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => context.DelayAsync("long-wait", TimeSpan.FromHours(1)));
        }

        Assert.Equal(1, store.AncestorWriteAttempts);
        Assert.Equal(1, transport.Count);
    }

    /// <summary>
    /// Losing every bounded attempt abandons the park with nothing published — the delivery
    /// retries it later — instead of parking on an ancestor whose retention is unproven, and
    /// instead of fighting a live ancestor without end.
    /// </summary>
    [Fact]
    public async Task AncestorExtension_LosingEveryAttempt_AbandonsTheParkWithNothingPublished()
    {
        var clock = new VirtualTimeProvider();
        var transport = new Round38RegressionTests.CapturingDelayedTransport();
        await using var provider = BuildProvider(transport, clock);
        var inner = provider.GetRequiredService<IFlowStateStore>();
        var store = new AlwaysLosingFlowStateStore(inner, "r38-busy-root", carryFloor: null);
        Assert.True(await inner.TryCreateAsync("r38-busy-root", State("r38-busy-root"), TimeSpan.FromMinutes(1)));
        var child = State("r38-busy-root:c", "r38-busy-root");
        Assert.True(await inner.TryCreateAsync("r38-busy-root:c", child, TimeSpan.FromMinutes(1)));

        await using (var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, child.FlowId!, ShortLedgerOptions, NullLogger.Instance, clock))!)
        {
            var context = CreateContext(provider, child, store, lease, clock, transport);
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => context.DelayAsync("long-wait", TimeSpan.FromHours(1)));
            Assert.Contains("r38-busy-root", ex.Message, StringComparison.Ordinal);
            Assert.Contains("abandoned", ex.Message, StringComparison.Ordinal);
        }

        Assert.Equal(DurableFlowContext.MaxAncestorExtensionAttempts, store.AncestorWriteAttempts);
        Assert.Equal(0, transport.Count);
    }

    // ---------- F3: InMemoryWorkerTransportOptions.DelayedJobCapacity ----------

    private static WorkerJobEnvelope DelayedJob() => new()
    {
        Call = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "AsyncResponse.Tests.IRound38Probe",
            MethodName = "RunAsync",
            Params = []
        }
    };

    [Fact]
    public async Task DelayedJobCapacity_RejectsAnInJobDelayedPublishAtTheBound_AndCountsIt()
    {
        var measurements = new List<(string Instrument, long Value)>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == AsyncResponseDiagnostics.MeterName)
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            lock (measurements)
                measurements.Add((instrument.Name, value));
        });
        listener.Start();

        var clock = new VirtualTimeProvider();
        var transport = new InMemoryWorkerTransport(
            Options.Create(new InMemoryWorkerTransportOptions { QueueCapacity = 1, DelayedJobCapacity = 2 }),
            clock);

        var rejection = await Task.Run(async () =>
        {
            InMemoryWorkerTransport.InJobScope.MarkActive();
            await transport.PublishAsync(DelayedJob(), TimeSpan.FromMinutes(5));
            await transport.PublishAsync(DelayedJob(), TimeSpan.FromMinutes(5));
            return await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(DelayedJob(), TimeSpan.FromMinutes(5)));
        });

        Assert.Contains(nameof(InMemoryWorkerTransportOptions.DelayedJobCapacity), rejection.Message, StringComparison.Ordinal);
        Assert.Equal(2, transport.DelayedJobsHeld);
        Assert.Equal(2, transport.SnapshotDelayedJobs().Count);
        listener.RecordObservableInstruments();
        lock (measurements)
        {
            Assert.Contains(measurements, m => m.Instrument == "asyncresponse.worker.inmemory_delayed_rejections" && m.Value == 1);
            Assert.Contains(measurements, m => m.Instrument == "asyncresponse.worker.inmemory_delayed_jobs" && m.Value >= 2);
        }

        // The slot is held through the pending write: both fire into a one-slot queue, one lands
        // and frees its slot, the other pends and keeps its slot.
        clock.Advance(TimeSpan.FromMinutes(5));
        await Task.Delay(50);
        Assert.Equal(1, transport.Reader.Count);
        Assert.Equal(1, transport.DelayedJobsHeld);
        Assert.True(transport.Reader.TryRead(out _));
        await Task.Delay(50);
        Assert.Equal(0, transport.DelayedJobsHeld);
        GC.KeepAlive(transport);
    }

    [Fact]
    public async Task DelayedJobCapacity_TheShutdownDrainFreesEverySlot_DroppedOrRetained()
    {
        var clock = new VirtualTimeProvider();
        var dropping = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions { DelayedJobCapacity = 2 }), clock);
        await dropping.PublishAsync(DelayedJob(), TimeSpan.FromHours(1));
        await dropping.PublishAsync(DelayedJob(), TimeSpan.FromHours(1));
        Assert.Equal(2, dropping.DelayedJobsHeld);
        dropping.BeginShutdownDrain();
        Assert.Equal(0, dropping.DelayedJobsHeld);

        var retaining = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions { DelayedJobCapacity = 2 }), clock);
        await retaining.PublishAsync(DelayedJob(), TimeSpan.FromHours(1));
        var retained = retaining.BeginRetainingDelayedJobs();
        retaining.BeginShutdownDrain();
        Assert.Single(retained);
        Assert.Equal(0, retaining.DelayedJobsHeld);

        // A delayed publish that arrives mid-drain is retained without holding a slot either.
        await retaining.PublishAsync(DelayedJob(), TimeSpan.FromHours(1));
        Assert.Equal(2, retained.Count);
        Assert.Equal(0, retaining.DelayedJobsHeld);
    }

    [Fact]
    public void DelayedJobCapacity_MustBePositive()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new InMemoryWorkerTransport(
            Options.Create(new InMemoryWorkerTransportOptions { DelayedJobCapacity = 0 })));

        Assert.Contains(nameof(InMemoryWorkerTransportOptions.DelayedJobCapacity), ex.Message, StringComparison.Ordinal);
    }

    // ---------- F6: KafkaSubscriberOptions.FaultDrainTimeout ----------

    private sealed class GateIngress : IAsyncResponseIngress
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int SlowRuns;

        public Task HandleResponseMessageAsync(string json, string? correlationId) => Task.CompletedTask;

        public Task HandleWorkerMessageAsync(string json)
        {
            if (json != "slow-job")
                return Task.CompletedTask;

            Interlocked.Increment(ref SlowRuns);
            Entered.TrySetResult();
            return Release.Task;
        }
    }

    private static KafkaAsyncResponseTransportOptions KafkaOptions(TimeSpan faultDrainTimeout)
    {
        var options = KafkaTestData.NewOptions();
        options.WorkerTopic = "workers";
        options.CreateTopics = false;
        options.SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1);
        options.SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(2);
        options.WorkerSubscriber.DetachHandlerAfter = TimeSpan.FromMilliseconds(10);
        options.WorkerSubscriber.FaultDrainTimeout = faultDrainTimeout;
        return options;
    }

    /// <summary>
    /// Pre-fix failure: with the handler held open, the failed consumer is never closed and no
    /// second consumer is created — the reconnect policy (2 ms) cannot run until the handler
    /// finishes. Now the fault teardown waits FaultDrainTimeout, abandons the handler (offset
    /// unstored, outcome logged when it settles), closes the consumer, and the supervisor
    /// rebuilds it at once.
    /// </summary>
    [Fact]
    public async Task KafkaWorker_APollLoopFailure_ReconnectsWithinFaultDrainTimeout_WhileAnUnrelatedHandlerRuns()
    {
        var first = new FakeKafkaConsumerClient();
        first.Enqueue(KafkaTestData.Message("workers", offset: 7, payload: "slow-job"));
        var second = new FakeKafkaConsumerClient();
        var factory = new FakeKafkaConsumerClientFactory(first, second);
        var ingress = new GateIngress();
        var logger = new CollectingLogger();
        using var subscriber = new KafkaWorkerSubscriber(
            Options.Create(KafkaOptions(TimeSpan.FromMilliseconds(50))),
            factory,
            new FakeKafkaProducerClient(),
            new FakeKafkaAdminClient(),
            ingress,
            logger.For<KafkaWorkerSubscriber>());

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await KafkaTestData.WaitUntilAsync(() => first.IsPartitionPaused(0));

            first.NextConsumeException = new IOException("simulated broker connection dropped");
            await KafkaTestData.WaitUntilAsync(() => factory.CreatedRoles.Count == 2, TimeSpan.FromSeconds(5));

            Assert.True(first.Closed);
            Assert.Empty(first.StoredOffsets);
            Assert.False(ingress.Release.Task.IsCompleted);
            await logger.WaitForAsync("Abandoning detached Kafka handler");

            // The abandoned handler settles later: observed and logged, nothing stored on either consumer.
            ingress.Release.SetResult();
            await logger.WaitForAsync("completed after the consumer it was consumed on was rebuilt");
            Assert.Empty(first.StoredOffsets);
            Assert.Empty(second.StoredOffsets);
        }
        finally
        {
            ingress.Release.TrySetResult();
            await subscriber.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>A handler that settles inside the budget has its offset stored before the failed consumer closes, exactly as after a stop.</summary>
    [Fact]
    public async Task KafkaWorker_AHandlerSettlingWithinFaultDrainTimeout_HasItsOffsetStoredBeforeTheClose()
    {
        var first = new FakeKafkaConsumerClient();
        first.Enqueue(KafkaTestData.Message("workers", offset: 7, payload: "slow-job"));
        var factory = new FakeKafkaConsumerClientFactory(first, new FakeKafkaConsumerClient());
        var ingress = new GateIngress();
        using var subscriber = new KafkaWorkerSubscriber(
            Options.Create(KafkaOptions(TimeSpan.FromSeconds(5))),
            factory,
            new FakeKafkaProducerClient(),
            new FakeKafkaAdminClient(),
            ingress,
            NullLogger<KafkaWorkerSubscriber>.Instance);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await ingress.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await KafkaTestData.WaitUntilAsync(() => first.IsPartitionPaused(0));

            first.NextConsumeException = new IOException("simulated broker connection dropped");
            await KafkaTestData.WaitUntilAsync(() => first.NextConsumeException is null);
            await Task.Delay(200);
            Assert.False(first.Closed);
            Assert.Single(factory.CreatedRoles);

            ingress.Release.SetResult();
            await KafkaTestData.WaitUntilAsync(() => factory.CreatedRoles.Count == 2, TimeSpan.FromSeconds(5));
            Assert.Equal(new FakeKafkaConsumerClient.StoredOffset("workers", 0, 7), Assert.Single(first.StoredOffsets));
            Assert.True(first.Closed);
        }
        finally
        {
            ingress.Release.TrySetResult();
            await subscriber.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public void FaultDrainTimeout_MustBeNonNegative_AndTimerBacked()
    {
        var options = KafkaTestData.NewOptions();
        options.WorkerSubscriber.FaultDrainTimeout = TimeSpan.FromMilliseconds(-1);

        var ex = Assert.Throws<InvalidOperationException>(
            () => KafkaMessageDispatcher.ValidateOptions(options, options.WorkerSubscriber, KafkaSubscriberRole.Worker));

        Assert.Contains(nameof(KafkaSubscriberOptions.FaultDrainTimeout), ex.Message, StringComparison.Ordinal);
        Assert.Equal(TimeSpan.FromSeconds(5), new KafkaSubscriberOptions().FaultDrainTimeout);
    }
}
