using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;
using static AsyncResponse.Tests.DurableFlowContextTestSupport;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// In-process parks — a durable timer on a transport without delayed delivery, and an awaited
// step's wait — hold the broker delivery for as long as they last. Two things bound that:
//  * host stop ends the park and hands the delivery back, without touching the checkpoint;
//  * on a broker with an in-flight ceiling a timer waits in hops, each under a fresh delivery.
// ---------------------------------------------------------------------------------------------
public class DurableFlowInProcessParkTests
{
    private static readonly TimeSpan SixHours = TimeSpan.FromHours(6);

    [Fact]
    public async Task InProcessTimer_HostStop_AbandonsTheDelivery_AndLeavesTheTimerCheckpointUntouched()
    {
        var clock = new VirtualTimeProvider();
        var transport = new RecordingTransport();
        await using var provider = BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var options = Options();
        var state = State("park-host-stop-timer");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));
        using var hostStopping = new CancellationTokenSource();

        await using var lease = await AcquireAsync(store, state.FlowId!, options, clock);
        var context = CreateContext(provider, state, store, lease, options, clock, transport, hostStopping: hostStopping.Token);
        var sleeping = context.DelayAsync("nap", SixHours);
        await WaitForArmedTimerAsync(clock, SixHours);
        Assert.False(sleeping.IsCompleted);

        hostStopping.Cancel();

        // A cancellation, so the worker transport treats the job as not executed and redelivers
        // it after the restart — and the public "interrupted, not failed" type flow code filters.
        var interrupted = await Assert.ThrowsAsync<DurableFlowInterruptedException>(() => sleeping);
        Assert.IsAssignableFrom<OperationCanceledException>(interrupted);
        Assert.Contains("Host is stopping", interrupted.Message, StringComparison.Ordinal);

        // The due time is the breadcrumb: persisted, not faulted, not completed, lease intact.
        var persisted = (await store.LoadAsync(state.FlowId!))!.Steps!["nap"];
        Assert.Equal(clock.GetUtcNow().UtcDateTime + SixHours, persisted.WakeAtUtc);
        Assert.False(persisted.Faulted);
        Assert.False(persisted.Completed);
        Assert.False(lease.IsLost);
        Assert.False(context.IsSuspended);
        Assert.Equal(0, transport.Count);
    }

    [Fact]
    public async Task AwaitedStep_HostStop_AbandonsTheDelivery_AndKeepsTheBreadcrumbForReattach()
    {
        var pending = new TaskCompletionSource<OperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var waiter = new Mock<IAsyncResponseWaiter<OperationResult>>();
        waiter.SetupGet(instance => instance.ResponseTask).Returns(pending.Task);
        // The channel contract: disposing a waiter cancels its still-pending response task.
        waiter.Setup(instance => instance.DisposeAsync()).Returns(() =>
        {
            pending.TrySetCanceled();
            return ValueTask.CompletedTask;
        });
        var subscriber = new Mock<IAsyncResponseSubscriber>();
        subscriber
            .Setup(instance => instance.CreateResponseWaiter(
                It.IsAny<string>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(),
                It.IsAny<TimeSpan?>()))
            .ReturnsAsync(waiter.Object);

        var clock = new VirtualTimeProvider();
        var store = new InMemoryFlowStateStore(clock);
        var options = Options();
        var state = State("park-host-stop-await");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));
        using var hostStopping = new CancellationTokenSource();
        await using var lease = await AcquireAsync(store, state.FlowId!, options, clock);
        var context = new DurableFlowContext(
            state,
            store,
            Mock.Of<IAsyncResponseBuilder>(),
            new AsyncResponseContextPropagation([]),
            options,
            subscriber.Object,
            recoverableSubscriber: null,
            NullLogger.Instance,
            lease,
            clock,
            hostStopping: hostStopping.Token);

        var triggered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var awaiting = context.AwaitStepAsync<OperationResult>(
            "remote",
            _ =>
            {
                triggered.TrySetResult();
                return Task.CompletedTask;
            },
            timeout: TimeSpan.FromHours(6));
        await triggered.Task;
        Assert.False(awaiting.IsCompleted);

        hostStopping.Cancel();

        var interrupted = await Assert.ThrowsAsync<DurableFlowInterruptedException>(() => awaiting);
        Assert.Contains("Host is stopping", interrupted.Message, StringComparison.Ordinal);

        // Wait-side cancellation is not a step verdict: the request is in flight, so the
        // redelivered execution must re-attach to the SAME correlation id, never re-send.
        var persisted = (await store.LoadAsync(state.FlowId!))!.Steps!["remote"];
        Assert.NotNull(persisted.PendingCorrelationId);
        Assert.False(persisted.Faulted);
        Assert.False(persisted.Completed);
        Assert.False(lease.IsLost);
    }

    [Fact]
    public async Task InProcessTimer_OnATransportWithAnInFlightCeiling_WaitsOneHop_ThenHandsOverToAFreshDelivery()
    {
        var clock = new VirtualTimeProvider();
        // A 30-minute ceiling (RabbitMQ's default consumer_timeout): the hop is half of it.
        var transport = new CeilingTransport(TimeSpan.FromMinutes(30));
        var hop = TimeSpan.FromMinutes(15);
        await using var provider = BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var options = Options();
        var state = State("park-hop");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));
        var dueAt = clock.GetUtcNow().UtcDateTime + SixHours;

        await using (var lease = await AcquireAsync(store, state.FlowId!, options, clock))
        {
            var context = CreateContext(provider, state, store, lease, options, clock, transport);
            var sleeping = context.DelayAsync("nap", SixHours);
            await WaitForArmedTimerAsync(clock, hop);

            // The hop is WAITED first: an immediate wake-up published up front would spin
            // deliveries for six hours instead of sleeping.
            Assert.Equal(0, transport.Count);
            clock.Advance(hop - TimeSpan.FromSeconds(1));
            Assert.False(sleeping.IsCompleted);
            Assert.Equal(0, transport.Count);

            clock.Advance(TimeSpan.FromSeconds(1));
            await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => sleeping);
            Assert.True(context.IsSuspended);
        }

        // One IMMEDIATE wake-up for this run, published after the checkpoint.
        var wakeUp = Assert.Single(transport.Jobs);
        Assert.Null(wakeUp.NotBeforeUtc);
        Assert.Equal(nameof(IDurableFlowExecutor.ExecuteAsync), wakeUp.Call.MethodName);
        var parked = (await store.LoadAsync(state.FlowId!))!;
        Assert.Equal(dueAt, parked.Steps!["nap"].WakeAtUtc);
        Assert.False(parked.Steps["nap"].Completed);
        // The ledger still covers the WHOLE remaining sleep, not just the hop.
        Assert.True(parked.RetainUntilUtc >= dueAt, $"retention floor {parked.RetainUntilUtc:O} does not reach the due time {dueAt:O}");
    }

    [Fact]
    public async Task InProcessTimer_HopsUnderFreshDeliveries_UntilTheCheckpointedDueTime()
    {
        var clock = new VirtualTimeProvider();
        var transport = new CeilingTransport(TimeSpan.FromHours(2));
        var hop = TimeSpan.FromHours(1);
        await using var provider = BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var options = Options();
        var state = State("park-hop-chain");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));
        var startedAt = clock.GetUtcNow();

        var deliveries = 0;
        while (true)
        {
            deliveries++;
            Assert.True(deliveries <= 10, "the timer never completed");
            var replayed = (await store.LoadAsync(state.FlowId!))!;
            await using var lease = await AcquireAsync(store, state.FlowId!, options, clock);
            var context = CreateContext(provider, replayed, store, lease, options, clock, transport);
            // The argument is ignored on a replay: the checkpointed due time is the contract.
            var sleeping = context.DelayAsync("nap", deliveries == 1 ? SixHours : TimeSpan.FromDays(9));
            await WaitForArmedTimerAsync(clock, hop);
            var heldSince = clock.GetUtcNow();
            clock.Advance(hop);

            try
            {
                await sleeping;
                break;
            }
            catch (DurableFlowSuspendedException)
            {
                // No single delivery was held past the hop.
                Assert.Equal(hop, clock.GetUtcNow() - heldSince);
            }
        }

        // Six one-hour hops: five hand-overs, and the sixth delivery completes the timer on time.
        Assert.Equal(6, deliveries);
        Assert.Equal(5, transport.Count);
        Assert.Equal(SixHours, clock.GetUtcNow() - startedAt);
        Assert.True((await store.LoadAsync(state.FlowId!))!.Steps!["nap"].Completed);
    }

    [Fact]
    public async Task MaxInProcessParkDuration_ShortensTheTransportDerivedHop_ButNeverLengthensIt()
    {
        // Shorter than half the ceiling: the option wins.
        Assert.Equal(TimeSpan.FromMinutes(5), await FirstHopAsync(new CeilingTransport(TimeSpan.FromMinutes(30)), TimeSpan.FromMinutes(5)));
        // Longer than half the ceiling: the transport-derived hop stands.
        Assert.Equal(TimeSpan.FromMinutes(15), await FirstHopAsync(new CeilingTransport(TimeSpan.FromMinutes(30)), TimeSpan.FromHours(3)));
        // No advertised ceiling: the option supplies the hop.
        Assert.Equal(TimeSpan.FromMinutes(20), await FirstHopAsync(new RecordingTransport(), TimeSpan.FromMinutes(20)));
        Assert.Equal(TimeSpan.FromMinutes(20), await FirstHopAsync(new CeilingTransport(null), TimeSpan.FromMinutes(20)));
    }

    [Fact]
    public async Task NoCeilingAndNoOption_WaitsTheWholeRemainderInOneDelivery()
    {
        var clock = new VirtualTimeProvider();
        var transport = new RecordingTransport();
        await using var provider = BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var options = Options();
        var state = State("park-unbounded");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));

        await using var lease = await AcquireAsync(store, state.FlowId!, options, clock);
        var context = CreateContext(provider, state, store, lease, options, clock, transport);
        var sleeping = context.DelayAsync("nap", SixHours);
        await WaitForArmedTimerAsync(clock, SixHours);
        clock.Advance(SixHours);
        await sleeping;

        Assert.True(state.Steps!["nap"].Completed);
        Assert.Equal(0, transport.Count);
    }

    [Fact]
    public async Task RemainderWithinTheHop_CompletesInProcess_WithoutAHandOver()
    {
        var clock = new VirtualTimeProvider();
        var transport = new CeilingTransport(TimeSpan.FromHours(24));
        await using var provider = BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var options = Options();
        var state = State("park-within-hop");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));

        await using var lease = await AcquireAsync(store, state.FlowId!, options, clock);
        var context = CreateContext(provider, state, store, lease, options, clock, transport);
        var sleeping = context.DelayAsync("nap", SixHours);
        await WaitForArmedTimerAsync(clock, SixHours);
        clock.Advance(SixHours);
        await sleeping;

        Assert.True(state.Steps!["nap"].Completed);
        Assert.Equal(0, transport.Count);
    }

    [Fact]
    public async Task ParkWhoseWakeUpPublishFails_DoesNotCountAsSuspended_AndResurfacesAfterFlowCodeSwallowsIt()
    {
        var clock = new VirtualTimeProvider();
        var transport = new CeilingTransport(TimeSpan.FromMinutes(30));
        var hop = TimeSpan.FromMinutes(15);
        await using var provider = BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var options = Options();
        var state = State("park-publish-fails");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));

        // The broker is unreachable exactly when the hop hands over: the checkpoint is written,
        // the wake-up never is.
        var outage = new InvalidOperationException("broker unreachable");
        transport.FailPublishesWith = outage;

        await using var lease = await AcquireAsync(store, state.FlowId!, options, clock);
        var context = CreateContext(provider, state, store, lease, options, clock, transport);
        var sleeping = context.DelayAsync("nap", SixHours);
        await WaitForArmedTimerAsync(clock, hop);
        clock.Advance(hop);

        // The park FAILED, so the step fails with the publish failure — not as a clean suspension,
        // which the executor acknowledges.
        Assert.Same(outage, await Assert.ThrowsAsync<InvalidOperationException>(() => sleeping));
        Assert.False(context.IsSuspended);
        Assert.Equal(0, transport.Count);

        // Flow code that catches Exception around its steps — the documented compensation pattern —
        // swallowed that throw and carried on. The next context call must re-surface the park
        // failure, so the attempt still ends as the retriable failure it is: reporting "suspended"
        // here is what acknowledged a delivery for a run nothing would ever wake.
        var resurfaced = await Assert.ThrowsAsync<InvalidOperationException>(
            () => context.StepAsync("after-the-swallow", () => Task.CompletedTask));
        Assert.Same(outage, resurfaced);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(50L * 24 * 60 * 60)]
    public void MaxInProcessParkDuration_MustBePositiveAndTimerBacked(long seconds)
    {
        var options = new DurableFlowOptions { MaxInProcessParkDuration = TimeSpan.FromSeconds(seconds) };

        var ex = Assert.Throws<InvalidOperationException>(options.ValidateInProcessPark);
        Assert.Contains(nameof(DurableFlowOptions.MaxInProcessParkDuration), ex.Message, StringComparison.Ordinal);

        // The starter validates at construction, like every other durable-flow knob.
        Assert.Throws<InvalidOperationException>(() => new DurableFlowService(
            Mock.Of<IServiceScopeFactory>(),
            Mock.Of<IAsyncResponseBuilder>(),
            new AsyncResponseContextPropagation([]),
            options,
            NullLogger<DurableFlowService>.Instance));
    }

    [Fact]
    public void MaxInProcessParkDuration_NullIsTheDefault_AndValid()
    {
        var options = new DurableFlowOptions();
        Assert.Null(options.MaxInProcessParkDuration);
        options.ValidateInProcessPark();
    }

    [Fact]
    public async Task AwaitedStep_LongerThanTheBudget_OnATransportWithACeiling_LogsOneWarning_AndIsNotHopped()
    {
        var logger = new CollectingLogger();
        var (context, awaiting, release) = await ParkAwaitedStepAsync(new CeilingTransport(TimeSpan.FromMinutes(30)), TimeSpan.FromHours(6), logger);

        var warning = Assert.Single(logger.Messages, message => message.Contains("in-process budget", StringComparison.Ordinal));
        Assert.Contains("'remote'", warning, StringComparison.Ordinal);
        Assert.Contains("00:30:00", warning, StringComparison.Ordinal);
        Assert.Contains("00:15:00", warning, StringComparison.Ordinal);

        // Still ONE wait for the whole window — the response completes it in this delivery.
        release(new OperationResult { Status = OperationStatus.Completed });
        Assert.Equal(OperationStatus.Completed, (await awaiting).Status);
        Assert.False(context.IsSuspended);
    }

    [Fact]
    public async Task AwaitedStep_WithinTheBudget_OrWithoutACeiling_LogsNoWarning()
    {
        var withinBudget = new CollectingLogger();
        var (_, first, releaseFirst) = await ParkAwaitedStepAsync(new CeilingTransport(TimeSpan.FromMinutes(30)), TimeSpan.FromMinutes(10), withinBudget);
        releaseFirst(new OperationResult { Status = OperationStatus.Completed });
        await first;
        Assert.DoesNotContain(withinBudget.Messages, message => message.Contains("in-process budget", StringComparison.Ordinal));

        var noCeiling = new CollectingLogger();
        var (_, second, releaseSecond) = await ParkAwaitedStepAsync(new RecordingTransport(), TimeSpan.FromHours(6), noCeiling);
        releaseSecond(new OperationResult { Status = OperationStatus.Completed });
        await second;
        Assert.DoesNotContain(noCeiling.Messages, message => message.Contains("in-process budget", StringComparison.Ordinal));
    }

    /// <summary>How long the first delivery of a six-hour timer is held before it hands over.</summary>
    private static async Task<TimeSpan> FirstHopAsync(RecordingTransport transport, TimeSpan? maxInProcessPark)
    {
        var clock = new VirtualTimeProvider();
        await using var provider = BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var options = Options(o => o.MaxInProcessParkDuration = maxInProcessPark);
        var state = State("park-first-hop");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));

        await using var lease = await AcquireAsync(store, state.FlowId!, options, clock);
        var context = CreateContext(provider, state, store, lease, options, clock, transport);
        var startedAt = clock.GetUtcNow();
        var sleeping = context.DelayAsync("nap", SixHours);
        await WaitForArmedTimerAsync(clock, SixHours);
        var due = clock.NextTimerDueAt!.Value;
        clock.AdvanceTo(due);
        await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => sleeping);
        return due - startedAt;
    }

    private static async Task<(DurableFlowContext Context, Task<OperationResult> Awaiting, Action<OperationResult> Release)> ParkAwaitedStepAsync(
        IWorkerTransport transport,
        TimeSpan timeout,
        CollectingLogger logger)
    {
        var pending = new TaskCompletionSource<OperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var waiter = new Mock<IAsyncResponseWaiter<OperationResult>>();
        waiter.SetupGet(instance => instance.ResponseTask).Returns(pending.Task);
        waiter.Setup(instance => instance.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var subscriber = new Mock<IAsyncResponseSubscriber>();
        subscriber
            .Setup(instance => instance.CreateResponseWaiter(
                It.IsAny<string>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(),
                It.IsAny<TimeSpan?>()))
            .ReturnsAsync(waiter.Object);

        var clock = new VirtualTimeProvider();
        var store = new InMemoryFlowStateStore(clock);
        var options = Options();
        var state = State($"park-await-{Guid.NewGuid():N}");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));
        var lease = await AcquireAsync(store, state.FlowId!, options, clock);
        var context = new DurableFlowContext(
            state,
            store,
            Mock.Of<IAsyncResponseBuilder>(),
            new AsyncResponseContextPropagation([]),
            options,
            subscriber.Object,
            recoverableSubscriber: null,
            logger,
            lease,
            clock,
            workerTransport: transport);

        var triggered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var awaiting = context.AwaitStepAsync<OperationResult>(
            "remote",
            _ =>
            {
                triggered.TrySetResult();
                return Task.CompletedTask;
            },
            timeout: timeout);
        await triggered.Task;
        return (context, awaiting, result => pending.TrySetResult(result));
    }
}
