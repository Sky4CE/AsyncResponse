using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;
using static AsyncResponse.Tests.DurableFlowContextTestSupport;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// In-process parks — a durable timer on a transport without delayed delivery, and an awaited
// step's wait — hold the broker delivery for as long as they last. Three things bound that:
//  * host stop hands an in-process TIMER over to a fresh delivery (or hands the delivery back),
//    without touching the checkpointed due time;
//  * host stop does NOT end an awaited step's wait: the channel's own shutdown does, keeping the
//    step's recovery registration;
//  * on a broker with an in-flight ceiling a timer waits in hops, each under a fresh delivery.
// ---------------------------------------------------------------------------------------------
public class DurableFlowInProcessParkTests
{
    private static readonly TimeSpan SixHours = TimeSpan.FromHours(6);

    [Fact]
    public async Task InProcessTimer_HostStopDuringTheWait_HandsOverToAFreshDelivery_AndLeavesTheDueTimeUntouched()
    {
        var clock = new VirtualTimeProvider();
        var transport = new RecordingTransport();
        await using var provider = BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var options = Options();
        var state = State("park-host-stop-timer");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));
        using var hostStopping = new CancellationTokenSource();
        var dueAt = clock.GetUtcNow().UtcDateTime + SixHours;

        await using var lease = await AcquireAsync(store, state.FlowId!, options, clock);
        var context = CreateContext(provider, state, store, lease, options, clock, transport, hostStopping: hostStopping.Token);
        var sleeping = context.DelayAsync("nap", SixHours);
        await WaitForArmedTimerAsync(clock, SixHours);
        Assert.False(sleeping.IsCompleted);

        hostStopping.Cancel();

        // Handed OVER, like a hop: the executor acknowledges this delivery, and the immediate
        // wake-up starts a fresh one whose attempt count is zero — handing the delivery back
        // unsettled cost it one broker delivery attempt per deploy until the cap buried it.
        await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => sleeping);
        Assert.True(context.IsSuspended);
        var wakeUp = Assert.Single(transport.Jobs);
        Assert.Null(wakeUp.NotBeforeUtc);
        Assert.Equal(nameof(IDurableFlowExecutor.ExecuteAsync), wakeUp.Call.MethodName);

        // The due time is the breadcrumb: persisted, not faulted, not completed.
        var persisted = (await store.LoadAsync(state.FlowId!))!;
        Assert.Equal(dueAt, persisted.Steps!["nap"].WakeAtUtc);
        Assert.False(persisted.Steps["nap"].Faulted);
        Assert.False(persisted.Steps["nap"].Completed);
        Assert.True(persisted.RetainUntilUtc >= dueAt);
    }

    [Fact]
    public async Task InProcessTimer_HostStop_WhoseHandOverCannotPublish_HandsTheDeliveryBack_AndStaysInterrupted()
    {
        var clock = new VirtualTimeProvider();
        var transport = new RecordingTransport();
        await using var provider = BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var options = Options();
        var state = State("park-host-stop-publish-fails");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));
        using var hostStopping = new CancellationTokenSource();

        await using var lease = await AcquireAsync(store, state.FlowId!, options, clock);
        var context = CreateContext(provider, state, store, lease, options, clock, transport, hostStopping: hostStopping.Token);
        var sleeping = context.DelayAsync("nap", SixHours);
        await WaitForArmedTimerAsync(clock, SixHours);

        transport.FailPublishesWith = new InvalidOperationException("broker unreachable");
        hostStopping.Cancel();

        // No wake-up exists, so the delivery must not be acknowledged: it is handed back as the
        // interruption it is — a cancellation, never the publish failure's retriable fault.
        var interrupted = await Assert.ThrowsAsync<DurableFlowInterruptedException>(() => sleeping);
        Assert.IsAssignableFrom<OperationCanceledException>(interrupted);
        Assert.Contains("Host is stopping", interrupted.Message, StringComparison.Ordinal);
        Assert.False(context.IsSuspended);
        Assert.Equal(0, transport.Count);
        Assert.False(lease.IsLost);

        // Sticky: flow code that swallowed it gets it again, never "run the next step".
        Assert.Same(interrupted, await Assert.ThrowsAsync<DurableFlowInterruptedException>(
            () => context.StepAsync("after-the-swallow", () => Task.CompletedTask)));
        Assert.Same(interrupted, await Assert.ThrowsAsync<DurableFlowInterruptedException>(context.FlushProgressAsync));
    }

    [Fact]
    public async Task InProcessTimer_HostStopDuringTheWait_ThroughTheExecutor_AcknowledgesTheDelivery_WithAFreshWakeUpQueued()
    {
        var clock = new VirtualTimeProvider();
        var transport = new RecordingTransport();
        using var host = new StoppingHost();
        await using var provider = BuildHost(transport, clock, host);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var state = HostState<SleepyFlow>("host-stop-executor");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromDays(1)));
        var dueAt = clock.GetUtcNow().UtcDateTime + SixHours;

        var execution = provider.GetRequiredService<IDurableFlowExecutor>().ExecuteAsync(state.FlowId!);
        await WaitForArmedTimerAsync(clock, SixHours);
        Assert.False(execution.IsCompleted);

        host.StopApplication();

        // Returns normally, so the worker transport acknowledges the delivery.
        await execution.WaitAsync(TimeSpan.FromSeconds(30));
        var wakeUp = Assert.Single(transport.Jobs);
        Assert.Null(wakeUp.NotBeforeUtc);
        var persisted = (await store.LoadAsync(state.FlowId!))!;
        Assert.Equal(FlowRunStatus.Running, persisted.Status);
        Assert.Equal(dueAt, persisted.Steps!["nap"].WakeAtUtc);
        Assert.False(persisted.Steps["nap"].Completed);
        Assert.False(persisted.Steps.ContainsKey("after"));
        Assert.Null((await store.ObserveLeaseAsync(state.FlowId!))!.LeaseId);
    }

    [Fact]
    public async Task InProcessTimer_ReachedOnAHostAlreadyStopping_HandsTheDeliveryBack_InsteadOfHandingOver()
    {
        // The wake-up a hand-over publishes can come straight back to this host's subscriber, which
        // keeps consuming until its own StopAsync: handing THAT over again would loop for the whole
        // shutdown. A wait that starts on a stopping host is handed back instead.
        var clock = new VirtualTimeProvider();
        var transport = new RecordingTransport();
        using var host = new StoppingHost();
        await using var provider = BuildHost(transport, clock, host);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var state = HostState<SleepyFlow>("host-stopping-at-entry");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromDays(1)));
        host.StopApplication();

        var interrupted = await Assert.ThrowsAsync<DurableFlowInterruptedException>(
            () => provider.GetRequiredService<IDurableFlowExecutor>().ExecuteAsync(state.FlowId!).WaitAsync(TimeSpan.FromSeconds(30)));

        Assert.Contains("Host is stopping", interrupted.Message, StringComparison.Ordinal);
        Assert.Equal(0, transport.Count);
        var persisted = (await store.LoadAsync(state.FlowId!))!;
        Assert.Equal(FlowRunStatus.Running, persisted.Status);
        Assert.NotNull(persisted.Steps!["nap"].WakeAtUtc);
        Assert.False(persisted.Steps["nap"].Completed);
        Assert.False(persisted.Steps["nap"].Faulted);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AHostStopInterruption_SwallowedByFlowCode_IsNotRunPast_NorMarkedSucceeded(bool stepAfterTheSwallow)
    {
        var clock = new VirtualTimeProvider();
        var transport = new RecordingTransport();
        using var host = new StoppingHost();
        await using var provider = BuildHost(transport, clock, host);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var flow = provider.GetRequiredService<SwallowingFlow>();
        flow.StepAfterTheSwallow = stepAfterTheSwallow;
        var state = HostState<SwallowingFlow>($"host-stop-swallowed-{stepAfterTheSwallow}");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromDays(1)));
        host.StopApplication();

        // The catch-all around the timer swallowed the interruption; the step after it must not
        // run early, and a body that simply returns must not have the run marked Succeeded with
        // its timer unfinished.
        await Assert.ThrowsAsync<DurableFlowInterruptedException>(
            () => provider.GetRequiredService<IDurableFlowExecutor>().ExecuteAsync(state.FlowId!).WaitAsync(TimeSpan.FromSeconds(30)));

        Assert.Equal(1, flow.Swallowed);
        Assert.Equal(0, flow.Charged);
        var persisted = (await store.LoadAsync(state.FlowId!))!;
        Assert.Equal(FlowRunStatus.Running, persisted.Status);
        Assert.False(persisted.Steps!["nap"].Completed);
        Assert.False(persisted.Steps.ContainsKey("charge"));
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task AHostStopInterruption_ConvertedByFlowCode_IsStillHandedBack_NotFailedNorRetriedAsAFault(bool intoFailure, bool reflective)
    {
        // Precommit review (A1): the executor overruled flow code that converted a committed PARK
        // into another exception, but not the host-stop interruption. Converted into a
        // DurableFlowFailedException it failed the run terminally on a deploy; converted into
        // anything else it read as a handler failure — a NAK, one broker attempt gone, and in the
        // end a dead-lettered wake-up. Both the registered and the reflection path hand it back.
        var clock = new VirtualTimeProvider();
        var transport = new RecordingTransport();
        using var host = new StoppingHost();
        await using var provider = BuildHost(transport, clock, host);
        var store = provider.GetRequiredService<IFlowStateStore>();
        ConvertingFlowBase flow = reflective
            ? provider.GetRequiredService<UnregisteredConvertingFlow>()
            : provider.GetRequiredService<ConvertingFlow>();
        flow.IntoFailure = intoFailure;
        var state = reflective
            ? HostState<UnregisteredConvertingFlow>($"host-stop-converted-reflective-{intoFailure}")
            : HostState<ConvertingFlow>($"host-stop-converted-{intoFailure}");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromDays(1)));
        host.StopApplication();

        var handedBack = await Assert.ThrowsAsync<DurableFlowInterruptedException>(
            () => provider.GetRequiredService<IDurableFlowExecutor>().ExecuteAsync(state.FlowId!).WaitAsync(TimeSpan.FromSeconds(30)));

        Assert.Contains("Host is stopping", handedBack.Message, StringComparison.Ordinal);
        Assert.Equal(1, flow.Converted);
        Assert.Equal(0, transport.Count);
        var persisted = (await store.LoadAsync(state.FlowId!))!;
        Assert.Equal(FlowRunStatus.Running, persisted.Status);
        Assert.False(persisted.Steps!["nap"].Completed);
        Assert.False(persisted.Steps["nap"].Faulted);
        Assert.False(persisted.Steps.ContainsKey("after"));
    }

    [Fact]
    public async Task AwaitedStep_HostStop_DoesNotEndTheWait_SoTheWaiterAndItsRecoveryRegistrationSurvive()
    {
        var pending = new TaskCompletionSource<OperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var waiter = new Mock<IAsyncResponseWaiter<OperationResult>>();
        waiter.SetupGet(instance => instance.ResponseTask).Returns(pending.Task);
        // The channel contract: disposing a waiter cancels its still-pending response task — and
        // deletes its lost-subscriber recovery registration.
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

        // ApplicationStopping fires long before the channel's own shutdown, which ends its waiters
        // while KEEPING their registrations. Ending the wait here instead disposed the waiter —
        // deleting the registration a response landing during the deploy needed to be recovered.
        // So the wait goes on, and the response still completes it in this delivery.
        pending.TrySetResult(new OperationResult { Status = OperationStatus.Completed });
        Assert.Equal(OperationStatus.Completed, (await awaiting).Status);

        // Disposed once, by the step's own finally after the response — never on the host-stop path.
        waiter.Verify(instance => instance.DisposeAsync(), Times.Once);
        var persisted = (await store.LoadAsync(state.FlowId!))!.Steps!["remote"];
        Assert.True(persisted.Completed);
        Assert.Null(persisted.PendingCorrelationId);
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
    public async Task ACommittedPark_ReleasesItsLeaseAtOnce_NotWhenTheBodyHasUnwound()
    {
        var clock = new VirtualTimeProvider();
        var transport = new CeilingTransport(TimeSpan.FromMinutes(30));
        var hop = TimeSpan.FromMinutes(15);
        await using var provider = BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var options = Options();
        var state = State("park-releases-lease");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));

        await using var lease = await AcquireAsync(store, state.FlowId!, options, clock);
        var context = CreateContext(provider, state, store, lease, options, clock, transport);
        var sleeping = context.DelayAsync("nap", SixHours);
        await WaitForArmedTimerAsync(clock, hop);
        clock.Advance(hop);
        await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => sleeping);
        Assert.Single(transport.Jobs);

        // Still inside the execution — the executor disposes the lease only once the body has
        // unwound — yet the store already shows no holder: the wake-up just published takes the
        // run on its next poll instead of judging a lease that is still being renewed.
        var observed = await store.ObserveLeaseAsync(state.FlowId!);
        Assert.Null(observed!.LeaseId);

        // Every later context call still reports the park, not a lost lease...
        await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => context.StepAsync("after", () => Task.CompletedTask));
        // ...and nothing is checkpointed through the released lease.
        Assert.Throws<InvalidOperationException>(() => lease.ThrowIfLost());
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

        // ...and so does the executor: a worker-only host never builds the starter, and it is the
        // executor that runs the timers.
        Assert.Throws<InvalidOperationException>(() => new DurableFlowExecutor(
            Mock.Of<IServiceScopeFactory>(),
            Mock.Of<IAsyncResponseBuilder>(),
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            new AsyncResponseContextPropagation([]),
            options,
            NullLogger<DurableFlowExecutor>.Instance));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task DurableFlowOptions_AreValidatedAtHostStartup_NotFirstInsideAFlowJob(long parkSeconds)
    {
        // Both the executor and the starter are built lazily — the executor inside the first flow
        // job, where a bad knob threw on every delivery inside the transport's retry loop.
        var services = new ServiceCollection();
        services.AddSingleton(typeof(Microsoft.Extensions.Logging.ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows(options => options.MaxInProcessParkDuration = TimeSpan.FromSeconds(parkSeconds));
        await using var provider = services.BuildServiceProvider();
        var validator = provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains(nameof(DurableFlowOptions.MaxInProcessParkDuration), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ALeaseRenewIntervalNotShorterThanTheLease_FailsHostStartup()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(Microsoft.Extensions.Logging.ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithInMemoryDurableFlows(options => options.ExecutionLeaseRenewInterval = options.ExecutionLeaseDuration);
        await using var provider = services.BuildServiceProvider();
        var validator = provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains(nameof(DurableFlowOptions.ExecutionLeaseRenewInterval), ex.Message, StringComparison.Ordinal);
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

        // MaxInProcessParkDuration shortens TIMER hops only: a 10-minute step under a 30-minute
        // ceiling cannot reach it, however short the timer hop is configured — the warning used to
        // compare against the shortened hop and name a broker ceiling no such step could reach.
        var shortTimerHop = new CollectingLogger();
        var (_, third, releaseThird) = await ParkAwaitedStepAsync(
            new CeilingTransport(TimeSpan.FromMinutes(30)),
            TimeSpan.FromMinutes(10),
            shortTimerHop,
            Options(o => o.MaxInProcessParkDuration = TimeSpan.FromMinutes(1)));
        releaseThird(new OperationResult { Status = OperationStatus.Completed });
        await third;
        Assert.DoesNotContain(shortTimerHop.Messages, message => message.Contains("in-process budget", StringComparison.Ordinal));
    }

    internal sealed class SleepyFlow : IDurableFlow<ParkInput>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, ParkInput input)
        {
            await flow.DelayAsync("nap", SixHours);
            await flow.StepAsync("after", () => Task.CompletedTask);
        }
    }

    /// <summary>The virtual clock, recording the due time of every timer armed on it.</summary>
    private sealed class TimerSpy(VirtualTimeProvider inner) : TimeProvider
    {
        private readonly List<TimeSpan> _armed = [];

        public override DateTimeOffset GetUtcNow() => inner.GetUtcNow();

        public override TimeZoneInfo LocalTimeZone => inner.LocalTimeZone;

        public override long TimestampFrequency => inner.TimestampFrequency;

        public override long GetTimestamp() => inner.GetTimestamp();

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            lock (_armed)
                _armed.Add(dueTime);
            return inner.CreateTimer(callback, state, dueTime, period);
        }

        /// <summary>Waits (real time, bounded) until a timer due exactly <paramref name="dueTime"/> out has been armed.</summary>
        public async Task WaitForTimerAsync(TimeSpan dueTime)
        {
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (true)
            {
                lock (_armed)
                {
                    if (_armed.Contains(dueTime))
                        return;
                }

                if (DateTime.UtcNow > deadline)
                {
                    lock (_armed)
                        throw new TimeoutException($"No {dueTime} timer was armed (armed: {string.Join(", ", _armed)}).");
                }

                await Task.Delay(5);
            }
        }
    }

    /// <summary>A step that takes <see cref="StepTakes"/> of virtual time, then a six-hour timer.</summary>
    internal sealed class SlowStepThenSleepFlow(TimeProvider clock) : IDurableFlow<ParkInput>
    {
        public TimeSpan StepTakes { get; set; }

        public async Task ExecuteAsync(IDurableFlowContext flow, ParkInput input)
        {
            await flow.StepAsync("slow", () =>
            {
                ((VirtualTimeProvider)clock).Advance(StepTakes);
                return Task.CompletedTask;
            });
            await flow.DelayAsync("nap", SixHours);
        }
    }

    /// <summary>The compensation pattern done wrong: a catch-all around the timer.</summary>
    internal sealed class SwallowingFlow : IDurableFlow<ParkInput>
    {
        private int _swallowed;
        private int _charged;

        public bool StepAfterTheSwallow { get; set; }

        public int Swallowed => Volatile.Read(ref _swallowed);

        public int Charged => Volatile.Read(ref _charged);

        public async Task ExecuteAsync(IDurableFlowContext flow, ParkInput input)
        {
            try
            {
                await flow.DelayAsync("nap", SixHours);
            }
            catch (Exception)
            {
                Interlocked.Increment(ref _swallowed);
            }

            if (StepAfterTheSwallow)
            {
                await flow.StepAsync("charge", () =>
                {
                    Interlocked.Increment(ref _charged);
                    return Task.CompletedTask;
                });
            }
        }
    }

    /// <summary>
    /// The compensation pattern done wrong the other way: the timer's cancellation turned into a
    /// terminal failure (<see cref="IntoFailure"/>) or into some other exception.
    /// </summary>
    internal abstract class ConvertingFlowBase : IDurableFlow<ParkInput>
    {
        private int _converted;

        public bool IntoFailure { get; set; }

        public int Converted => Volatile.Read(ref _converted);

        public async Task ExecuteAsync(IDurableFlowContext flow, ParkInput input)
        {
            try
            {
                await flow.DelayAsync("nap", SixHours);
            }
            catch (OperationCanceledException ex)
            {
                Interlocked.Increment(ref _converted);
                if (IntoFailure)
                    throw new DurableFlowFailedException("The nap was cut short.", ex);
                throw new InvalidOperationException("The nap was cut short.", ex);
            }

            await flow.StepAsync("after", () => Task.CompletedTask);
        }
    }

    /// <summary>Registered with <c>WithDurableFlow</c>: executed on the typed path.</summary>
    internal sealed class ConvertingFlow : ConvertingFlowBase { }

    /// <summary>In DI only, never registered with <c>WithDurableFlow</c>: executed on the reflection path.</summary>
    internal sealed class UnregisteredConvertingFlow : ConvertingFlowBase { }

    /// <summary>A whole host: the real executor, resolved from DI with a host lifetime the test stops.</summary>
    private static ServiceProvider BuildHost(IWorkerTransport transport, TimeProvider clock, StoppingHost host)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(Microsoft.Extensions.Logging.ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(clock);
        services.AddSingleton<Microsoft.Extensions.Hosting.IHostApplicationLifetime>(host);
        services.AddSingleton<SwallowingFlow>();
        services.AddSingleton<SlowStepThenSleepFlow>();
        services.AddSingleton<ConvertingFlow>();
        services.AddSingleton<UnregisteredConvertingFlow>();
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows(options =>
            {
                options.ExecutionLeaseDuration = TimeSpan.FromDays(30);
                options.ExecutionLeaseRenewInterval = TimeSpan.FromDays(10);
            })
            .WithDurableFlow<SleepyFlow, ParkInput>()
            .WithDurableFlow<SwallowingFlow, ParkInput>()
            .WithDurableFlow<SlowStepThenSleepFlow, ParkInput>()
            .WithDurableFlow<ConvertingFlow, ParkInput>();
        services.AddSingleton(transport);
        return services.BuildServiceProvider();
    }

    private static FlowState HostState<TFlow>(string id)
    {
        var state = State(id);
        state.FlowTypeName = typeof(TFlow).FullName;
        return state;
    }

    [Fact]
    public async Task InProcessTimer_AfterStepsThatSpentMostOfTheCeiling_HopsOnlyWhatTheDeliveryHasLeft()
    {
        // A 30-minute ceiling, and a delivery that spent 20 minutes in a step before reaching the
        // timer. Half the ceiling (15 min) would hold it 35 minutes — past the ceiling, so the broker
        // redelivers it under its live handler. The hop is what the delivery has left, less a tenth
        // of the ceiling as headroom for the hand-over: 30 - 20 - 3 = 7 minutes.
        var clock = new VirtualTimeProvider();
        var transport = new CeilingTransport(TimeSpan.FromMinutes(30));
        using var host = new StoppingHost();
        await using var provider = BuildHost(transport, clock, host);
        var store = provider.GetRequiredService<IFlowStateStore>();
        provider.GetRequiredService<SlowStepThenSleepFlow>().StepTakes = TimeSpan.FromMinutes(20);
        var state = HostState<SlowStepThenSleepFlow>("hop-what-is-left");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromDays(1)));
        var deliveryStarted = clock.GetUtcNow();

        var execution = provider.GetRequiredService<IDurableFlowExecutor>().ExecuteAsync(state.FlowId!);
        await WaitForArmedTimerAsync(clock, TimeSpan.FromHours(1));
        Assert.Equal(TimeSpan.FromMinutes(7), clock.NextTimerDueAt!.Value - clock.GetUtcNow());

        clock.AdvanceTo(clock.NextTimerDueAt!.Value);
        await execution.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Single(transport.Jobs);
        Assert.Equal(TimeSpan.FromMinutes(27), clock.GetUtcNow() - deliveryStarted);
    }

    [Fact]
    public async Task InProcessTimer_OnADeliveryWithNothingLeftOfItsCeiling_HandsOverAtOnce()
    {
        var clock = new VirtualTimeProvider();
        var transport = new CeilingTransport(TimeSpan.FromMinutes(30));
        using var host = new StoppingHost();
        await using var provider = BuildHost(transport, clock, host);
        var store = provider.GetRequiredService<IFlowStateStore>();
        provider.GetRequiredService<SlowStepThenSleepFlow>().StepTakes = TimeSpan.FromMinutes(28);
        var state = HostState<SlowStepThenSleepFlow>("hop-nothing-left");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromDays(1)));
        var deliveryStarted = clock.GetUtcNow();

        // No virtual time passes after the step: the wake-up is published straight away, and the
        // next delivery — whose in-flight clock starts from zero — waits the next hop. (The lease's
        // own timers are days out, and its release arms only seconds-long budgets; a timer due in
        // minutes would be a hop this delivery waits.)
        var execution = provider.GetRequiredService<IDurableFlowExecutor>().ExecuteAsync(state.FlowId!);
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (!execution.IsCompleted && !(clock.NextTimerDueAt is { } due && due - clock.GetUtcNow() is var hop && hop >= TimeSpan.FromMinutes(5) && hop <= TimeSpan.FromHours(1)))
        {
            Assert.True(DateTime.UtcNow < deadline, "The execution neither finished nor armed a hop.");
            await Task.Delay(5);
        }

        Assert.True(execution.IsCompleted, $"The delivery parked for another {clock.NextTimerDueAt - clock.GetUtcNow()} instead of handing over at once.");
        await execution;
        var wakeUp = Assert.Single(transport.Jobs);
        Assert.Null(wakeUp.NotBeforeUtc);
        Assert.Equal(TimeSpan.FromMinutes(28), clock.GetUtcNow() - deliveryStarted);
        Assert.False((await store.LoadAsync(state.FlowId!))!.Steps!["nap"].Completed);
    }

    [Fact]
    public async Task TimerLongerThanTheBclTimerCeiling_OnATransportWithNeitherDelayNorACeiling_IsWaitedInHops_NotFailed()
    {
        // Kafka, NATS and Redis Streams advertise neither delayed delivery nor an in-flight
        // ceiling. A 60-day sleep used to fail the run terminally on the ~49.7-day .NET timer
        // ceiling; the hand-over that waits any remainder in hops now takes it.
        var virtualClock = new VirtualTimeProvider();
        var clock = new TimerSpy(virtualClock);
        var transport = new RecordingTransport();
        await using var provider = BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var options = Options(o =>
        {
            o.ExecutionLeaseDuration = TimeSpan.FromDays(100);
            o.ExecutionLeaseRenewInterval = TimeSpan.FromDays(40);
        });
        var state = State("park-beyond-timer-ceiling");
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));
        var sixtyDays = TimeSpan.FromDays(60);

        await using var lease = await AcquireAsync(store, state.FlowId!, options, clock);
        var context = CreateContext(provider, state, store, lease, options, clock, transport);
        var sleeping = context.DelayAsync("nap", sixtyDays);

        // One hop of the whole timer ceiling (the lease's own timers are armed next to it) — not the
        // terminal failure the sleep used to end with at once.
        var armed = clock.WaitForTimerAsync(AsyncResponseChannelOptions.MaxTimerBackedTimeout);
        await Task.WhenAny(armed, sleeping);
        Assert.False(sleeping.IsCompleted, $"The sleep ended before waiting a hop: {sleeping.Exception?.GetBaseException().Message}");
        await armed;
        virtualClock.Advance(AsyncResponseChannelOptions.MaxTimerBackedTimeout);
        await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => sleeping);
        var wakeUp = Assert.Single(transport.Jobs);
        Assert.Null(wakeUp.NotBeforeUtc);
        Assert.False((await store.LoadAsync(state.FlowId!))!.Steps!["nap"].Completed);
    }

    [Fact]
    public async Task AFirstPassTimerPark_ExtendsEachAncestorOnce_EvenAsTheClockMovesBetweenItsTwoSaves()
    {
        // A first-pass park on a delayed transport checkpoints twice for the same wait (the
        // breadcrumb, then the park itself). The ancestor floor used to be "now + a remainder
        // measured before both saves", so it crept forward with the clock and the second save
        // never found the parent covered: every Running ancestor was rewritten — and its revision
        // bumped — twice per park. A frozen virtual clock hid it; here every read of the clock
        // moves it by a millisecond, as a real one does.
        var clock = new CreepingClock();
        var transport = new RecordingDelayedTransport();
        await using var provider = BuildProvider(transport, clock);
        var store = new AncestorWriteCountingStore(new InMemoryFlowStateStore(clock), "creep-parent");
        var options = Options();
        var parent = State("creep-parent");
        var child = State("creep-parent:child", parent: "creep-parent");
        Assert.True(await store.TryCreateAsync(parent.FlowId!, parent, options.StateExpiry));
        Assert.True(await store.TryCreateAsync(child.FlowId!, child, options.StateExpiry));

        await using var lease = await AcquireAsync(store, child.FlowId!, options, clock);
        var context = CreateContext(provider, child, store, lease, options, clock, transport);
        await Assert.ThrowsAsync<DurableFlowSuspendedException>(() => context.DelayAsync("nap", TimeSpan.FromDays(30)));

        Assert.Single(transport.Jobs);
        Assert.Equal(1, store.AncestorWrites);
        Assert.True((await store.LoadAsync(parent.FlowId!))!.RetainUntilUtc >= child.Steps!["nap"].WakeAtUtc!.Value + options.StateExpiry);
    }

    /// <summary>A clock that moves forward a millisecond on every read; its timers never fire.</summary>
    private sealed class CreepingClock : TimeProvider
    {
        private readonly VirtualTimeProvider _timers = new();
        private long _reads;

        public override DateTimeOffset GetUtcNow()
            => VirtualTimeProvider.DefaultStartTime + TimeSpan.FromMilliseconds(Interlocked.Increment(ref _reads));

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
            => _timers.CreateTimer(callback, state, dueTime, period);
    }

    /// <summary>Counts the updates of one ancestor's ledger.</summary>
    private sealed class AncestorWriteCountingStore(InMemoryFlowStateStore inner, string ancestorId) : IFlowStateStore
    {
        private int _ancestorWrites;

        public int AncestorWrites => Volatile.Read(ref _ancestorWrites);

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
        {
            if (string.Equals(flowId, ancestorId, StringComparison.Ordinal))
                Interlocked.Increment(ref _ancestorWrites);
            return inner.TryUpdateAsync(flowId, state, expectedRevision, ttl, leaseId, cancellationToken);
        }

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => inner.TryCreateAsync(flowId, state, ttl, cancellationToken);

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.LoadAsync(flowId, cancellationToken);

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryAcquireLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => inner.TryRenewLeaseAsync(flowId, leaseId, leaseDuration, cancellationToken);

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => inner.ReleaseLeaseAsync(flowId, leaseId, cancellationToken);

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => inner.TryDeleteAsync(flowId, cancellationToken);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ATimerStartedNextToARunningStep_IsRefusedBeforeItTouchesTheLedger(bool alreadyCompleted, bool until)
    {
        // Every step entry point takes the concurrency guard before it looks its step up — except
        // the timers did: GetStep inserted an empty entry into the ledger's (non-thread-safe) step
        // dictionary, next to a sibling step running concurrently, and only then did the guard
        // throw. The orphan was persisted with the sibling's next save; a completed timer skipped
        // the guard altogether.
        var clock = new VirtualTimeProvider();
        var transport = new RecordingTransport();
        await using var provider = BuildProvider(transport, clock);
        var store = provider.GetRequiredService<IFlowStateStore>();
        var options = Options();
        var state = State($"timer-guard-{alreadyCompleted}-{until}");
        if (alreadyCompleted)
            state.Steps = new Dictionary<string, FlowStepState>(StringComparer.Ordinal) { ["a"] = new() { Completed = true } };
        Assert.True(await store.TryCreateAsync(state.FlowId!, state, options.StateExpiry));

        await using var lease = await AcquireAsync(store, state.FlowId!, options, clock);
        var context = CreateContext(provider, state, store, lease, options, clock, transport);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = context.StepAsync("b", async () => await gate.Task);

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => until
            ? context.DelayUntilAsync("a", clock.GetUtcNow() + TimeSpan.FromHours(1))
            : context.DelayAsync("a", TimeSpan.FromHours(1)));
        Assert.Contains("while another step", refused.Message, StringComparison.Ordinal);
        Assert.Equal(alreadyCompleted, state.Steps!.ContainsKey("a"));

        gate.SetResult();
        await running;
        Assert.Equal(alreadyCompleted, (await store.LoadAsync(state.FlowId!))!.Steps!.ContainsKey("a"));
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
        CollectingLogger logger,
        DurableFlowOptions? options = null)
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
        options ??= Options();
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
