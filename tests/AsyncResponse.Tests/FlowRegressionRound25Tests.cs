using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Collections;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// Round-25 review regressions in the durable-flow engine: lease-deadline side-effect fencing,
// per-step scoping of the clock-skew marker, the persisted await deadline, ancestor ledger TTLs
// under long child parks, replay-time sleep re-validation, the typed-path input-type check, and
// the startup validator's store-lifetime and ledger-coverage audits.
// ---------------------------------------------------------------------------------------------

public sealed record R25Input(string Name);

/// <summary>Two timers, so a skew-released wake-up for the first can meet the second in one replay.</summary>
public sealed class TwoNapsFlow : IDurableFlow<R25Input>
{
    public static readonly TimeSpan FirstNap = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan SecondNap = TimeSpan.FromDays(60);

    public async Task ExecuteAsync(IDurableFlowContext flow, R25Input input)
    {
        await flow.DelayAsync("nap1", FirstNap);
        await flow.DelayAsync("nap2", SecondNap);
    }
}

/// <summary>Registered for the statically-typed executor path; records that its body ran.</summary>
public sealed class InputEvolvedFlow : IDurableFlow<R25Input>
{
    public static int Executions;

    public Task ExecuteAsync(IDurableFlowContext flow, R25Input input)
    {
        Interlocked.Increment(ref Executions);
        return Task.CompletedTask;
    }
}

/// <summary>Awaits one step with a short real-time timeout; its trigger records every send.</summary>
public sealed class ShortWaitFlow : IDurableFlow<R25Input>
{
    public static readonly List<string> TriggeredCids = [];

    public Task ExecuteAsync(IDurableFlowContext flow, R25Input input)
        => flow.AwaitStepAsync<OperationResult>(
            "slow",
            trigger: cid =>
            {
                lock (TriggeredCids)
                    TriggeredCids.Add(cid);
                return Task.CompletedTask;
            },
            timeout: TimeSpan.FromSeconds(1));
}

/// <summary>30-day awaited step with an entered-execution latch, for mid-window re-attach tests.</summary>
public sealed class DeadlineReattachFlow : IDurableFlow<R25Input>
{
    public static TaskCompletionSource EnteredExecution = NewSource();

    public async Task ExecuteAsync(IDurableFlowContext flow, R25Input input)
    {
        EnteredExecution.TrySetResult();
        await flow.AwaitStepAsync<OperationResult>(
            "slow",
            trigger: _ => Task.CompletedTask,
            timeout: TimeSpan.FromDays(30));
    }

    public static TaskCompletionSource NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

/// <summary>Parks a two-day timer at the bottom of a three-level parent chain.</summary>
public sealed class R25GrandchildFlow : IDurableFlow<R25Input>
{
    public static readonly TimeSpan Nap = TimeSpan.FromDays(2);

    public Task ExecuteAsync(IDurableFlowContext flow, R25Input input)
        => flow.DelayAsync("gc-nap", Nap);
}

public sealed class R25MidFlow : IDurableFlow<R25Input>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, R25Input input)
        => await flow.AwaitChildFlowAsync<R25GrandchildFlow, R25Input>("mid-child", input);
}

public sealed class R25RootFlow(StepRecorder _recorder) : IDurableFlow<R25Input>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, R25Input input)
    {
        await flow.AwaitChildFlowAsync<R25MidFlow, R25Input>("root-child", input);
        await flow.StepAsync("root-done", () =>
        {
            _recorder.Record("root-done");
            return Task.CompletedTask;
        });
    }
}

/// <summary>Sleeps almost the full persistence ceiling, so a raised StateExpiry shrinks the recomputed bound below it.</summary>
public sealed class R25LongSleepFlow : IDurableFlow<R25Input>
{
    public Task ExecuteAsync(IDurableFlowContext flow, R25Input input)
        => flow.DelayAsync("nap", TimeSpan.FromDays(3600));
}

public sealed class FlowRegressionRound25Tests
{
    /// <summary>Settable clock with REAL timers, so renewal loops park forever like a paused process.</summary>
    private sealed class ManualClock(DateTimeOffset start) : TimeProvider
    {
        private long _ticks = start.UtcTicks;

        public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);

        public void Advance(TimeSpan by) => Interlocked.Add(ref _ticks, by.Ticks);
    }

    private static ServiceProvider BuildFlowProvider(Action<AsyncResponseRegistrationBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        configure(services.AddAsyncResponse().WithInMemoryChannel().WithInMemoryTransport().WithInMemoryDurableFlows());
        return services.BuildServiceProvider();
    }

    private static AsyncResponseStartupValidator CreateValidator(IServiceProvider provider)
        => new(
            provider.GetServices<AsyncResponseChannelMarker>(),
            provider.GetServices<AsyncResponseTransportMarker>(),
            provider.GetServices<AsyncResponseDurableFlowStoreMarker>(),
            provider.GetRequiredService<IOptions<AsyncResponseOptions>>(),
            provider.GetServices<DurableFlowOptions>());

    [Fact]
    public async Task StoreLifetimeChangedAfterTheFluentChain_FailsFastAtStartup()
    {
        // Regression (r25): the IFlowStateStore forward mirrors the concrete registration's
        // lifetime as a REGISTRATION-TIME snapshot. Registering the store after the fluent chain
        // left a Scoped forward to a root singleton, so the first flow-execution scope's disposal
        // disposed the store (and its connections) for the whole process — the r24 P1 re-opened
        // through a different door. The startup validator now re-checks the snapshot against the
        // final collection and rejects the ordering with the fix spelled out.
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithDurableFlows<StoreLifetimeForwardTests.DisposableStubStore>();
        services.AddSingleton<StoreLifetimeForwardTests.DisposableStubStore>();
        await using var provider = services.BuildServiceProvider();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateValidator(provider).StartAsync(CancellationToken.None));

        Assert.Contains("after the fluent chain", ex.Message, StringComparison.Ordinal);
        Assert.Contains("dispose", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(nameof(StoreLifetimeForwardTests.DisposableStubStore), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StoreRegisteredBeforeTheFluentChain_PassesTheLifetimeAudit()
    {
        // The documented provider-package ordering (concrete registration first) keeps the
        // snapshot and the final lifetime identical, so the audit stays silent.
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<StoreLifetimeForwardTests.DisposableStubStore>();
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithDurableFlows<StoreLifetimeForwardTests.DisposableStubStore>();
        await using var provider = services.BuildServiceProvider();

        var validator = CreateValidator(provider);
        await validator.StartAsync(CancellationToken.None);
        await validator.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ThrowIfLost_AfterTheLeaseWindowElapsesWithoutARenewal_Throws()
    {
        // Regression (r25): ThrowIfLost only consulted the renewal loop's loss signal, which needs
        // a failed store round-trip to fire. A stop-the-world pause longer than the lease let
        // another worker take over while the thawed holder's next step guard still passed — the
        // step body ran concurrently on two hosts (only the checkpoint was lease-fenced). The
        // guard now also treats a passed lease deadline as lost.
        var clock = new ManualClock(DateTimeOffset.UnixEpoch);
        var store = new InMemoryFlowStateStore(clock);
        var state = new FlowState { FlowId = "paused-flow" };
        Assert.True(await store.TryCreateAsync("paused-flow", state, TimeSpan.FromDays(1)));

        var options = new DurableFlowOptions();
        var lease = await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(
            store, "paused-flow", options, NullLogger.Instance, clock);
        Assert.NotNull(lease);
        await using var _ = lease;

        lease!.ThrowIfLost();

        // The pause: the clock leaps past the lease window with no renewal landing (the renewal
        // loop is parked on a real-time delay, like a frozen process).
        clock.Advance(options.ExecutionLeaseDuration + TimeSpan.FromSeconds(1));

        var ex = Assert.Throws<InvalidOperationException>(() => lease.ThrowIfLost());
        Assert.Contains("lost its execution lease", ex.Message, StringComparison.Ordinal);
        Assert.True(lease.LostToken.IsCancellationRequested);
    }

    [Fact]
    public async Task TypedPath_RunPersistedWithADifferentInputType_FailsClosedInsteadOfMisparsing()
    {
        // Regression (r25): the statically-typed (AOT) execution path deserialized InputJson keyed
        // on FlowTypeName alone. Re-registering a flow with a new TInput silently parsed the old
        // payload as the new type — renamed/added members became defaults — and ran the remaining
        // steps against wrong input. The reflection fallback always failed closed; now both do.
        InputEvolvedFlow.Executions = 0;
        await using var provider = BuildFlowProvider(builder => builder.WithDurableFlow<InputEvolvedFlow, R25Input>());

        var store = provider.GetRequiredService<InMemoryFlowStateStore>();
        var state = new FlowState
        {
            FlowId = "evolved",
            FlowTypeName = typeof(InputEvolvedFlow).FullName,
            InputTypeName = "Company.Orders.OrderV1",
            InputJson = """{"Name":"acme"}""",
            Status = FlowRunStatus.Running
        };
        Assert.True(await store.TryCreateAsync("evolved", state, TimeSpan.FromDays(1)));

        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync("evolved"));

        Assert.Contains("incompatible flow definition", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Company.Orders.OrderV1", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, InputEvolvedFlow.Executions);
    }

    [Fact]
    public async Task SkewReleasedWake_ForcesOnlyItsOwnTimer_ALaterTimerStillSuspends()
    {
        // Regression (r25): the skew marker was an AsyncLocal over the WHOLE replay, so after a
        // forced-early wake-up for nap1 an unrelated later nap2 also skipped suspension — pinning
        // a worker in process for its full remainder, or (beyond the 49.7-day ceiling) failing
        // the run terminally. The marker is now claimed once, by the replayed timer the wake-up
        // actually targeted.
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<TwoNapsFlow, R25Input>());
        await using var _ = harness;

        var run = await harness.StartFlowAsync<TwoNapsFlow, R25Input>(new R25Input("acme"));
        await run.WaitForTimerStepAsync("nap1");
        await harness.Engine.WaitForWorkerIdleAsync();

        // Re-deliver the parked wake-up early, twice: the first hop is treated as an anomaly and
        // re-published, the second proves the stall and releases the job under the skew marker.
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

            if (attempt == 0)
                await harness.Engine.WaitForWorkerIdleAsync();
        }

        // Crossing nap1's due time lets the skew delivery finish it in process and reach nap2 in
        // the SAME replay; the extra advance drains the duplicate wheel wake-up's lease poll.
        await harness.AdvanceAsync(TwoNapsFlow.FirstNap);
        await harness.AdvanceAsync(TimeSpan.FromMinutes(2));

        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
        FlowState? mid;
        while (true)
        {
            mid = await run.GetStateAsync();
            if (mid is { Status: FlowRunStatus.Failed })
                break;
            if (mid?.Steps is not null && mid.Steps.TryGetValue("nap2", out var nap2) && nap2.WakeAtUtc is not null)
                break;

            Assert.True(TimeProvider.System.GetUtcNow() < guard, "nap2 never parked");
            await Task.Delay(TimeSpan.FromMilliseconds(5));
        }

        // The load-bearing assertion: nap2 suspended normally. Pre-fix it inherited the skew
        // exemption and its 60-day remainder hit the timer ceiling — a terminal failure.
        Assert.Equal(FlowRunStatus.Running, mid!.Status);
        Assert.Equal(2, run.StepExecutions("nap1"));

        await harness.AdvanceAsync(TwoNapsFlow.SecondNap + TimeSpan.FromMinutes(5));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    [Fact]
    public async Task TimeoutLessAwaits_LedgerTtlNotAboveTheChannelDefault_FailFastAtStartup()
    {
        // Regression (r25): with no per-call timeout and no DefaultStepTimeout, the channel
        // resolves the wait to its own default — and nothing cross-validated that window against
        // StateExpiry. A 30-day channel default under a 14-day ledger TTL pruned the row (and the
        // lease anchor) mid-wait: the run was unrecoverable and no step-timeout fault ever fired.
        await using var provider = BuildValidatorProvider(
            channelDefaultTimeout: TimeSpan.FromDays(30),
            stateExpiry: TimeSpan.FromDays(14),
            defaultStepTimeout: null);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateValidator(provider).StartAsync(CancellationToken.None));

        Assert.Contains(nameof(DurableFlowOptions.StateExpiry), ex.Message, StringComparison.Ordinal);
        Assert.Contains("default waiter timeout", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LedgerCoverage_PassesWhenTheTtlExceedsTheDefault_OrAStepTimeoutIsConfigured()
    {
        await using (var covered = BuildValidatorProvider(TimeSpan.FromDays(30), TimeSpan.FromDays(31), defaultStepTimeout: null))
            await CreateValidator(covered).StartAsync(CancellationToken.None);

        // A configured DefaultStepTimeout makes the channel default unreachable for awaited
        // steps, so the combination is legitimate.
        await using (var stepBound = BuildValidatorProvider(TimeSpan.FromDays(30), TimeSpan.FromDays(14), defaultStepTimeout: TimeSpan.FromDays(7)))
            await CreateValidator(stepBound).StartAsync(CancellationToken.None);
    }

    private static ServiceProvider BuildValidatorProvider(
        TimeSpan channelDefaultTimeout,
        TimeSpan stateExpiry,
        TimeSpan? defaultStepTimeout)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse()
            .WithInMemoryChannel(channel => channel.DefaultTimeout = channelDefaultTimeout)
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows(flows =>
            {
                flows.StateExpiry = stateExpiry;
                flows.DefaultStepTimeout = defaultStepTimeout;
            });
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task ReattachAfterThePersistedDeadlineElapsed_FaultsTheStep_InsteadOfArmingAFreshWindow()
    {
        // Regression (r25): awaited steps had no persisted deadline — every replay recomputed the
        // step timeout and armed a FRESH full window, so with any redelivery cadence shorter than
        // the timeout a remote system that never answers produced a run that neither completed
        // nor alarmed, forever. The deadline is persisted at the first arm; a re-attach that
        // finds it elapsed settles like the live timeout would have.
        lock (ShortWaitFlow.TriggeredCids)
            ShortWaitFlow.TriggeredCids.Clear();
        await using var provider = BuildFlowProvider(builder => builder.WithDurableFlow<ShortWaitFlow, R25Input>());
        var store = provider.GetRequiredService<InMemoryFlowStateStore>();

        // A parked wait whose window elapsed while no execution was live, written as raw ledger
        // JSON: the deadline property is additive, so builds without it ignore the field.
        var json =
            $$"""
            {"SchemaVersion":1,"Revision":0,"FlowId":"expired-wait","FlowTypeName":"{{typeof(ShortWaitFlow).FullName}}","InputTypeName":"{{typeof(R25Input).FullName}}","InputJson":"{\"Name\":\"acme\"}","Status":0,"Steps":{"slow":{"Completed":false,"PendingCorrelationId":"cid-r25-expired","AwaitDeadlineUtc":"2020-01-01T00:00:00Z","Faulted":false} } }
            """;
        var state = FlowStateJson.Deserialize(json);
        Assert.NotNull(state);
        Assert.True(await store.TryCreateAsync("expired-wait", state!, TimeSpan.FromDays(1)));

        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var ex = await Assert.ThrowsAsync<TimeoutException>(() => executor.ExecuteAsync("expired-wait"));

        Assert.Contains("await deadline", ex.Message, StringComparison.Ordinal);
        var persisted = await store.LoadAsync("expired-wait");
        Assert.True(persisted!.Steps!["slow"].Faulted);
        lock (ShortWaitFlow.TriggeredCids)
            Assert.Empty(ShortWaitFlow.TriggeredCids);
    }

    [Fact]
    public async Task ReattachMidWindow_ArmsTheRemainder_NotAFreshFullWindow()
    {
        // Regression (r25), companion to the elapsed-deadline test: a redelivery 10 days into a
        // 30-day wait must re-extend the ledger by the 20-day REMAINDER (plus the idle margin) —
        // re-arming the full window restarted the fault clock and the TTL from zero on every
        // redelivery.
        DeadlineReattachFlow.EnteredExecution = DeadlineReattachFlow.NewSource();
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<DeadlineReattachFlow, R25Input>();
            options.DurableFlows = flows =>
            {
                // Test-sized lease cadence: the stepwise advance walks every renewal beat, and
                // the awaited step holds the lease for the whole 10 virtual days.
                flows.ExecutionLeaseDuration = TimeSpan.FromDays(40);
                flows.ExecutionLeaseRenewInterval = TimeSpan.FromDays(1);
            };
        });
        await using var _ = harness;

        var run = await harness.StartFlowAsync<DeadlineReattachFlow, R25Input>(new R25Input("acme"));
        await DeadlineReattachFlow.EnteredExecution.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await run.WaitForAwaitingStepAsync("slow");

        // Ten days into the window the parked execution dies and the run is redelivered.
        await harness.AdvanceAsync(TimeSpan.FromDays(10));
        await harness.Engine.SimulateRestartAsync();
        DeadlineReattachFlow.EnteredExecution = DeadlineReattachFlow.NewSource();
        await run.ResumeAsync();
        await DeadlineReattachFlow.EnteredExecution.Task.WaitAsync(TimeSpan.FromSeconds(10));

        using var scope = harness.Engine.Services.CreateScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();
        var entriesField = store.GetType().GetField("_entries", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(entriesField);
        var entries = (IDictionary)entriesField!.GetValue(store)!;

        // The re-attach save races this read: poll past the per-attempt plain-StateExpiry stamp
        // (14 days), then pin the window. Remainder (20d) + StateExpiry (14d) = 34d; the old
        // full-window re-arm stamped 30d + 14d = 44d.
        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
        TimeSpan ttl;
        while (true)
        {
            var entry = entries[run.FlowId];
            Assert.NotNull(entry);
            var expires = (DateTime)entry!.GetType().GetProperty("ExpiresAtUtc")!.GetValue(entry)!;
            ttl = expires - harness.Clock.GetUtcNow().UtcDateTime;
            if (ttl > TimeSpan.FromDays(15))
                break;

            Assert.True(TimeProvider.System.GetUtcNow() < guard, $"the re-attached ledger was never re-extended (TTL {ttl})");
            await Task.Delay(TimeSpan.FromMilliseconds(10));
        }

        Assert.InRange(ttl, TimeSpan.FromDays(33), TimeSpan.FromDays(35));
    }

    [Fact]
    public async Task ChildParkedBeyondStateExpiry_KeepsTheWholeAncestorChainAlive()
    {
        // Regression (r25): a parent parked on a child kept its plain StateExpiry TTL, and
        // nothing refreshed it while the child ran — a child legitimately parking beyond that
        // TTL outlived the parent's row, and the completion wake-up found "no state ... nothing
        // to execute". A long child park now extends every Running ancestor's ledger to cover it.
        var recorder = new StepRecorder();
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(recorder);
            options.ConfigureAsyncResponse = builder => builder
                .WithDurableFlow<R25RootFlow, R25Input>()
                .WithDurableFlow<R25MidFlow, R25Input>()
                .WithDurableFlow<R25GrandchildFlow, R25Input>();
            options.DurableFlows = flows => flows.StateExpiry = TimeSpan.FromHours(1);
        });
        await using var _ = harness;

        var run = await harness.StartFlowAsync<R25RootFlow, R25Input>(new R25Input("acme"));

        // Let the chain settle: root suspends on mid, mid on the grandchild, the grandchild on
        // its two-day timer — all parked, worker idle.
        await harness.Engine.WaitForWorkerIdleAsync();

        await harness.AdvanceAsync(R25GrandchildFlow.Nap + TimeSpan.FromMinutes(1));

        // Pre-fix both ancestors' rows expired at +1h, so the chain of completion wake-ups died
        // at the first "no state" and the root was silently lost.
        Assert.NotNull(await run.GetStateAsync());
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
        Assert.Equal(1, recorder.Count("root-done"));
    }

    [Fact]
    public async Task CheckpointedSleep_ReplayedUnderARaisedStateExpiry_IsNotReValidated()
    {
        // Regression (r25): DelayCoreAsync re-validated the CHECKPOINTED sleep against the
        // current options on every replay. Raising StateExpiry mid-sleep shrinks the recomputed
        // bound (ceiling − StateExpiry), terminally failing a parked timer that was valid when it
        // was armed — the exact failure mode the replay path's argument-ignoring contract rules out.
        await using var provider = BuildFlowProvider(builder => builder.WithDurableFlow<R25LongSleepFlow, R25Input>());

        var flows = provider.GetRequiredService<IDurableFlows>();
        await flows.StartAsync<R25LongSleepFlow, R25Input>(new R25Input("acme"), "long-nap");

        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        await executor.ExecuteAsync("long-nap");

        var store = provider.GetRequiredService<InMemoryFlowStateStore>();
        var parked = await store.LoadAsync("long-nap");
        Assert.Equal(FlowRunStatus.Running, parked!.Status);
        Assert.NotNull(parked.Steps!["nap"].WakeAtUtc);

        // The redeploy: StateExpiry raised mid-sleep. The engine consumes the shared options
        // instance, so mutating it is exactly a restart with new configuration. The recomputed
        // bound (3650 − 100 = 3550 days) is now below the parked 3600-day sleep.
        provider.GetRequiredService<DurableFlowOptions>().StateExpiry = TimeSpan.FromDays(100);

        await executor.ExecuteAsync("long-nap");

        var replayed = await store.LoadAsync("long-nap");
        Assert.Equal(FlowRunStatus.Running, replayed!.Status);
    }
}
