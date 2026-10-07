using System.Reflection;
using System.Text.Json;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using static AsyncResponse.Tests.DurableFlowContextTestSupport;

namespace AsyncResponse.Tests;

/// <summary>
/// Coverage-gap pins for <see cref="DurableFlowContext"/>, one execution at a time on the virtual
/// clock: timer replays and the skew-forced sleep past the timer ceiling, caller cancellation of an
/// in-process wait, a swallowed host-stop interruption, re-attaching awaited steps (recovered,
/// unreadable, mid-window), duplicate step names, the lease-less checkpoint of a claimed response,
/// and the ledger-size warning bands.
/// </summary>
public sealed class DurableFlowContextCoverageGapTests
{
    private sealed record Setup(
        ServiceProvider Provider,
        IFlowStateStore Store,
        FlowExecutionLease Lease,
        DurableFlowContext Context,
        FlowState State,
        VirtualTimeProvider Clock,
        RecordingTransport Transport) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Lease.DisposeAsync();
            await Provider.DisposeAsync();
        }
    }

    private static async Task<Setup> StartAsync(
        string flowId,
        Action<FlowState>? shape = null,
        Func<IFlowStateStore, IFlowStateStore>? wrapStore = null,
        RecordingTransport? transport = null,
        Action<DurableFlowOptions>? configure = null,
        Microsoft.Extensions.Logging.ILogger? logger = null,
        CancellationToken hostStopping = default)
    {
        var clock = new VirtualTimeProvider();
        transport ??= new RecordingTransport();
        var provider = BuildProvider(transport, clock);
        var inner = provider.GetRequiredService<IFlowStateStore>();
        var store = wrapStore?.Invoke(inner) ?? inner;
        var options = Options(configure);
        var state = State(flowId);
        shape?.Invoke(state);
        Assert.True(await inner.TryCreateAsync(flowId, state, options.StateExpiry));
        var lease = await AcquireAsync(store, flowId, options, clock);
        var context = CreateContext(provider, state, store, lease, options, clock, transport, logger, hostStopping);
        return new Setup(provider, store, lease, context, state, clock, transport);
    }

    private static DateTime Now(VirtualTimeProvider clock) => clock.GetUtcNow().UtcDateTime;

    // ---------------------------------------------------------------------------------------
    // Timers
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task DelayUntil_OnAReplayedCompletedTimer_ReturnsWithoutWritingOrPublishing()
    {
        await using var setup = await StartAsync("gap-delay-until-done", state => state.Steps = new()
        {
            ["nap"] = new FlowStepState { Completed = true, WakeAtUtc = VirtualTimeProvider.DefaultStartTime.UtcDateTime }
        });
        var revision = setup.State.Revision;

        await setup.Context.DelayUntilAsync("nap", DateTimeOffset.MaxValue);

        Assert.Equal(revision, setup.State.Revision);
        Assert.Equal(0, setup.Transport.Count);
    }

    [Fact]
    public async Task ASkewForcedWakeUp_WithMoreThanTheTimerCeilingLeft_FailsTheRunTerminally()
    {
        // A replayed timer whose wake-up the executor released early on proven clock skew: it may
        // neither re-suspend (the proof would be lost and the run would loop) nor hand over, and a
        // 60-day remainder is past what one in-process wait can arm.
        var wakeAt = VirtualTimeProvider.DefaultStartTime.UtcDateTime.AddDays(60);
        await using var setup = await StartAsync(
            "gap-skew-ceiling",
            state => state.Steps = new() { ["long-nap"] = new FlowStepState { WakeAtUtc = wakeAt } },
            transport: new RecordingDelayedTransport(),
            configure: options => options.StateExpiry = TimeSpan.FromDays(90));

        using (WorkerJobSkewScope.Enter())
        {
            var ex = await Assert.ThrowsAsync<DurableFlowFailedException>(() => setup.Context.DelayUntilAsync("long-nap", wakeAt));

            Assert.Contains("clock skew", ex.Message, StringComparison.Ordinal);
            Assert.Contains("long-nap", ex.Message, StringComparison.Ordinal);
        }

        Assert.Equal(0, setup.Transport.Count);
    }

    [Fact]
    public async Task AnInProcessWait_CancelledByTheCaller_IsACancellation_NotAHandOver()
    {
        await using var setup = await StartAsync("gap-caller-cancel");
        using var cancel = new CancellationTokenSource();

        var sleeping = setup.Context.DelayAsync("short-nap", TimeSpan.FromMinutes(1), cancel.Token);
        await WaitForArmedTimerAsync(setup.Clock, TimeSpan.FromMinutes(1));
        await cancel.CancelAsync();

        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sleeping);
        Assert.IsNotType<DurableFlowInterruptedException>(ex);
        Assert.Equal(0, setup.Transport.Count);
        Assert.False(setup.State.Steps!["short-nap"].Completed);
    }

    [Fact]
    public async Task AHostStopInterruptionTheFlowSwallowed_StillFailsTheBodysCompletion()
    {
        using var hostStopping = new CancellationTokenSource();
        await hostStopping.CancelAsync();
        await using var setup = await StartAsync("gap-swallowed-interrupt", hostStopping: hostStopping.Token);

        try
        {
            await setup.Context.DelayAsync("nap", TimeSpan.FromMinutes(1));
        }
        catch (DurableFlowInterruptedException)
        {
            // Flow code swallowing the hand-back.
        }

        await Assert.ThrowsAsync<DurableFlowInterruptedException>(() => setup.Context.FlushProgressAsync());
    }

    [Fact]
    public async Task AParkWhosePublishFailed_AndTheFlowSwallowed_StillFailsTheBodysCompletion()
    {
        var transport = new RecordingDelayedTransport { FailPublishesWith = new TimeoutException("broker down") };
        await using var setup = await StartAsync("gap-swallowed-park", transport: transport);

        try
        {
            // Long enough to suspend on a delayed wake-up, whose publish fails.
            await setup.Context.DelayAsync("long-nap", TimeSpan.FromHours(1));
        }
        catch (TimeoutException)
        {
            // Flow code swallowing the failed park.
        }

        // Neither finished nor parked: the executor must not mark the run Succeeded.
        var ex = await Assert.ThrowsAsync<TimeoutException>(() => setup.Context.FlushProgressAsync());
        Assert.Equal("broker down", ex.Message);
    }

    // ---------------------------------------------------------------------------------------
    // Awaited steps re-attaching to an in-flight wait
    // ---------------------------------------------------------------------------------------

    private sealed class CurrentReadOverride(IFlowStateStore inner, string flowId, Func<FlowState?, Task<FlowState?>> read)
        : DelegatingFlowStateStore(inner)
    {
        public override async Task<FlowState?> LoadCurrentAsync(string id, CancellationToken cancellationToken = default)
            => id == flowId ? await read(await Inner.LoadCurrentAsync(id, cancellationToken)) : await Inner.LoadCurrentAsync(id, cancellationToken);
    }

    private static void PendingOn(FlowState state, string correlationId, DateTime deadline)
        => state.Steps = new()
        {
            ["remote"] = new FlowStepState
            {
                PendingCorrelationId = correlationId,
                PendingPayloadTypeFullName = typeof(OperationResult).FullName,
                AwaitDeadlineUtc = deadline
            }
        };

    [Fact]
    public async Task AReattachPastItsDeadline_TakesTheResultRecoveryAlreadyCheckpointed()
    {
        var deadline = VirtualTimeProvider.DefaultStartTime.UtcDateTime.AddHours(-1);
        var recovered = JsonSerializer.Serialize(new OperationResult { Status = OperationStatus.Completed, Message = "recovered" });
        var triggered = 0;
        await using var setup = await StartAsync(
            "gap-reattach-recovered",
            state => PendingOn(state, "cid-gap-recovered", deadline),
            inner => new CurrentReadOverride(inner, "gap-reattach-recovered", current =>
            {
                current!.Steps!["remote"] = new FlowStepState { Completed = true, ResultJson = recovered };
                return Task.FromResult<FlowState?>(current);
            }));

        var result = await setup.Context.AwaitStepAsync<OperationResult>("remote", _ =>
        {
            triggered++;
            return Task.CompletedTask;
        });

        Assert.Equal("recovered", result.Message);
        Assert.Equal(0, triggered);
        Assert.True(setup.State.Steps!["remote"].Completed);
    }

    [Fact]
    public async Task AReattachPastItsDeadline_WhoseCheckpointCannotBeReread_FaultsLikeTheLiveTimeout()
    {
        var logger = new CollectingLogger();
        await using var setup = await StartAsync(
            "gap-reattach-unreadable",
            state => PendingOn(state, "cid-gap-unreadable", VirtualTimeProvider.DefaultStartTime.UtcDateTime.AddHours(-1)),
            inner => new CurrentReadOverride(inner, "gap-reattach-unreadable", _ => throw new TimeoutException("store down")),
            logger: logger);

        var ex = await Assert.ThrowsAsync<TimeoutException>(() => setup.Context.AwaitStepAsync<OperationResult>("remote", _ => Task.CompletedTask));

        Assert.Contains("await deadline", ex.Message, StringComparison.Ordinal);
        Assert.True(setup.State.Steps!["remote"].Faulted);
        var warning = Assert.Single(logger.Entries, entry => entry.Message.Contains("could not re-read its checkpoint", StringComparison.Ordinal));
        Assert.IsType<TimeoutException>(warning.Exception);
    }

    [Fact]
    public async Task AReattachInsideItsWindow_LogsTheReattach_AndTakesTheResponseWhenItArrives()
    {
        var logger = new CollectingLogger();
        await using var setup = await StartAsync(
            "gap-reattach-mid",
            state => PendingOn(state, "cid-gap-mid", VirtualTimeProvider.DefaultStartTime.UtcDateTime.AddHours(1)),
            logger: logger);

        var awaiting = setup.Context.AwaitStepAsync<OperationResult>("remote", _ => throw new InvalidOperationException("a re-attach never re-sends"));
        await logger.WaitForAsync("re-attaching to in-flight correlationId cid-gap-mid");
        await setup.Provider.GetRequiredService<IAsyncResponsePublisher>()
            .SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "late but live" }, "cid-gap-mid");

        var result = await awaiting.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal("late but live", result.Message);
        Assert.True(setup.State.Steps!["remote"].Completed);
    }

    /// <summary>A transport that advertises a broker in-flight ceiling.</summary>
    private sealed class CeilingTransport : RecordingTransport, IWorkerTransportInFlightLimit
    {
        public TimeSpan? MaxInFlightDuration => TimeSpan.FromMinutes(10);
    }

    [Fact]
    public async Task AReattachInsideItsWindow_StillTakesTheResponse_WhenTheReattachDebugLogThrows()
    {
        var logger = new RecordingThrowingLogger<DurableFlowContext> { ThrowOnMessageContaining = "re-attaching to in-flight" };
        await using var setup = await StartAsync(
            "gap-reattach-logger-throws",
            state => PendingOn(state, "cid-gap-logger", VirtualTimeProvider.DefaultStartTime.UtcDateTime.AddHours(1)),
            logger: logger);

        var awaiting = setup.Context.AwaitStepAsync<OperationResult>("remote", _ => throw new InvalidOperationException("a re-attach never re-sends"));
        await setup.Provider.GetRequiredService<IAsyncResponsePublisher>()
            .SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "late but live" }, "cid-gap-logger");

        var result = await awaiting.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal("late but live", result.Message);
    }

    [Fact]
    public async Task AnAwaitedStep_WaitingPastTheInFlightCeiling_StillWaits_WhenTheCeilingWarningThrows()
    {
        var logger = new RecordingThrowingLogger<DurableFlowContext> { ThrowOnMessageContaining = "waits in process for up to" };
        await using var setup = await StartAsync("gap-ceiling-logger-throws", transport: new CeilingTransport(), logger: logger);
        var publisher = setup.Provider.GetRequiredService<IAsyncResponsePublisher>();

        string? sent = null;
        var awaiting = setup.Context.AwaitStepAsync<OperationResult>(
            "remote", correlationId => { sent = correlationId; return Task.CompletedTask; }, TimeSpan.FromHours(1));
        // The waiter is registered before the trigger runs, so the response cannot be missed.
        while (Volatile.Read(ref sent) is null && !awaiting.IsCompleted)
            await Task.Yield();
        if (awaiting.IsCompleted)
            await awaiting; // surfaces a fault before the trigger instead of spinning forever
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "answered" }, sent!);
        var result = await awaiting.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal("answered", result.Message);
    }

    // ---------------------------------------------------------------------------------------
    // Step names, ledger warning bands
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task ReusingAStepNameInOneExecution_IsRejected_WithoutAWarningBandConfigured()
    {
        await using var setup = await StartAsync("gap-duplicate-step", configure: options => options.LedgerSizeWarningBytes = null);

        Assert.Equal(1, await setup.Context.StepAsync("send", () => Task.FromResult(1)));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => setup.Context.StepAsync("send", () => Task.FromResult(2)));
        Assert.Contains("already used in this execution", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void LedgerWarningBands_SaturateAtTheTop_AndAnUnsetThresholdWarnsNothing()
    {
        var band = typeof(DurableFlowContext).GetMethod("NextLedgerWarningBand", BindingFlags.Static | BindingFlags.NonPublic)!;
        Assert.Equal(long.MaxValue - 1, band.Invoke(null, [long.MaxValue / 2 + 1, long.MaxValue]));
        Assert.Equal(2048L, band.Invoke(null, [1024L, 1500L]));

        var logger = new CollectingLogger();
        DurableFlowContext.WarnIfWriteCrossedLedgerWarning(logger, new DurableFlowOptions { LedgerSizeWarningBytes = null }, State("gap-no-band"), 0);
        Assert.Empty(logger.Messages);
    }

    [Fact]
    public async Task AChildsParkEnd_IsItsFurthestPendingTimerOrAwaitDeadline()
    {
        await using var setup = await StartAsync("gap-child-park");
        var now = Now(setup.Clock);
        var child = State("gap-child-park:child");
        child.Steps = new()
        {
            ["done"] = new FlowStepState { Completed = true, WakeAtUtc = now.AddDays(9) },
            ["nap"] = new FlowStepState { WakeAtUtc = now.AddHours(1) },
            ["remote"] = new FlowStepState { AwaitDeadlineUtc = now.AddHours(2) }
        };

        var ends = typeof(DurableFlowContext).GetMethod("ChildParkEndsUtc", BindingFlags.Instance | BindingFlags.NonPublic)!;

        Assert.Equal(now.AddHours(2), ends.Invoke(setup.Context, [child]));
    }

    // ---------------------------------------------------------------------------------------
    // The lease-less checkpoint of a claimed response
    // ---------------------------------------------------------------------------------------

    private static Task CheckpointWithoutLeaseAsync(DurableFlowContext context, string name, FlowStepState step, string correlationId)
        => (Task)typeof(DurableFlowContext)
            .GetMethod("CheckpointReceivedWithoutLeaseAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .MakeGenericMethod(typeof(OperationResult))
            .Invoke(context, [name, step, new OperationResult { Status = OperationStatus.Completed, Message = "claimed" }, correlationId])!;

    private sealed class FailingLoads(IFlowStateStore inner, string flowId) : DelegatingFlowStateStore(inner)
    {
        public bool Fail { get; set; }

        public override Task<FlowState?> LoadAsync(string id, CancellationToken cancellationToken = default)
            => Fail && id == flowId ? Task.FromException<FlowState?>(new TimeoutException("store down")) : base.LoadAsync(id, cancellationToken);

        public override Task<FlowState?> LoadCurrentAsync(string id, CancellationToken cancellationToken = default)
            => Fail && id == flowId ? Task.FromException<FlowState?>(new TimeoutException("store down")) : base.LoadCurrentAsync(id, cancellationToken);
    }

    [Theory]
    [InlineData("missing", "the step no longer exists in the ledger")]
    [InlineData("completed", "the step is already completed")]
    [InlineData("unreadable", "could not checkpoint the claimed response")]
    public async Task AClaimedResponseTheLedgerCannotTake_IsDiscardedWithAReason_AndTheStepStaysOpen(string ledger, string expected)
    {
        var flowId = $"gap-claimed-{ledger}";
        var logger = new CollectingLogger();
        FailingLoads? failing = null;
        await using var setup = await StartAsync(
            flowId,
            state => state.Steps = ledger switch
            {
                "missing" => new(),
                "completed" => new() { ["remote"] = new FlowStepState { Completed = true } },
                _ => new() { ["remote"] = new FlowStepState { PendingCorrelationId = "cid-gap-claimed" } }
            },
            inner => failing = new FailingLoads(inner, flowId),
            logger: logger);
        failing!.Fail = ledger == "unreadable";
        var step = new FlowStepState { PendingCorrelationId = "cid-gap-claimed" };

        await CheckpointWithoutLeaseAsync(setup.Context, "remote", step, "cid-gap-claimed");

        Assert.False(step.Completed);
        Assert.Contains(logger.Messages, message => message.Contains(expected, StringComparison.Ordinal));
    }
}
