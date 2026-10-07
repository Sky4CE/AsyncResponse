using System.Reflection;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Coverage-gap pins for the in-memory worker transport, its worker host and the in-memory
/// response channel: the restart/readmission plumbing the Testing kit drives, the delayed-job
/// branches that only a timer firing after the drain (or after the writer completed) reaches, and
/// the channel's guarded log calls when a logging provider throws.
/// </summary>
public sealed class InMemoryCoreCoverageGapTests
{
    private static WorkerJobEnvelope Job(string method = "Run") => new()
    {
        Call = new ReflectionCallDto { ServiceInterfaceFullName = "Gap.IUnregisteredWork", MethodName = method, Params = [] }
    };

    private static InMemoryWorkerTransport Transport(int queueCapacity = 1024, TimeProvider? clock = null)
        => new(Options.Create(new InMemoryWorkerTransportOptions { QueueCapacity = queueCapacity }), clock);

    /// <summary>Hands every timer's callback to the test, which fires it when it chooses.</summary>
    private sealed class ManualTimers : TimeProvider
    {
        public List<(TimerCallback Callback, object? State)> Created { get; } = [];

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            lock (Created)
                Created.Add((callback, state));
            return new InertTimer();
        }

        public void FireLast()
        {
            (TimerCallback Callback, object? State) last;
            lock (Created)
                last = Created[^1];
            last.Callback(last.State);
        }
    }

    private sealed class InertTimer : ITimer
    {
        public bool Disposed { get; private set; }

        public bool Change(TimeSpan dueTime, TimeSpan period) => true;

        public void Dispose() => Disposed = true;

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    // ---------------------------------------------------------------------------------------
    // InMemoryWorkerTransport
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void AnIdleTransport_HasNoOverflowToTake_AndNoDelayedJobsToSnapshot()
    {
        var transport = Transport();

        Assert.False(transport.TryTakeOverflow(out _));
        Assert.Empty(transport.SnapshotDelayedJobs());
    }

    [Fact]
    public void ScheduleRetained_RejectsANonPositiveOrUnarmableDelay()
    {
        var transport = Transport();
        var queued = new InMemoryWorkerTransport.QueuedJob(Job(), null);

        Assert.Throws<ArgumentOutOfRangeException>(() => transport.ScheduleRetained(queued, TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => transport.ScheduleRetained(queued, transport.MaxPublishDelay + TimeSpan.FromMilliseconds(1)));
        Assert.Empty(transport.SnapshotDelayedJobs());
    }

    [Fact]
    public void Readmit_PastTheQueueCapacity_JoinsTheOverflow_AndTakeUnstartedJobsReturnsBoth()
    {
        var transport = Transport(queueCapacity: 1);
        var first = new InMemoryWorkerTransport.QueuedJob(Job("First"), null);
        var second = new InMemoryWorkerTransport.QueuedJob(Job("Second"), null);

        transport.Readmit(first);
        transport.Readmit(second);

        // No worker runs: the second one waits in the overflow, counted as outstanding.
        Assert.Equal(1, transport.OverflowDepth);
        Assert.Equal(2, transport.OutstandingJobs);

        var taken = transport.TakeUnstartedJobs(reopen: true);

        Assert.Equal(["First", "Second"], taken.Select(queued => queued.Job.Call.MethodName));
        Assert.Equal(0, transport.OverflowDepth);
        Assert.Equal(0, transport.OutstandingJobs);
    }

    [Fact]
    public async Task DelayedPublish_WithNoDelay_IsAnImmediatePublish_AndPastTheCeilingIsRejected()
    {
        var transport = Transport();

        await transport.PublishAsync(Job("Now"), TimeSpan.Zero);
        Assert.True(transport.Reader.TryRead(out var queued));
        Assert.Equal("Now", queued.Job.Call.MethodName);

        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = transport.PublishAsync(Job(), transport.MaxPublishDelay + TimeSpan.FromMilliseconds(1)); });
        Assert.Empty(transport.SnapshotDelayedJobs());
    }

    [Fact]
    public async Task DelayedPublish_DuringShutdown_OutsideAJob_IsRejectedLoudly()
    {
        var transport = Transport();
        var logger = new CollectingLogger();
        transport.DrainLogger = logger;
        transport.BeginShutdownDrain();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => transport.PublishAsync(Job("Late"), TimeSpan.FromMinutes(1)));

        Assert.Contains("shutting down", ex.Message, StringComparison.Ordinal);
        Assert.Contains(logger.Messages, message => message.Contains("Rejecting delayed in-memory worker job Gap.IUnregisteredWork.Late", StringComparison.Ordinal));
        Assert.Equal(0, transport.DelayedJobsHeld);
    }

    [Fact]
    public async Task ADelayedTimerFiringAfterTheDrainClaimedIt_DoesNothing()
    {
        var timers = new ManualTimers();
        var transport = Transport(clock: timers);
        await transport.PublishAsync(Job("Claimed"), TimeSpan.FromMinutes(5));
        Assert.Single(transport.SnapshotDelayedJobs());

        transport.BeginShutdownDrain();
        timers.FireLast();

        // The drain dropped it: firing afterwards neither queues nor counts it.
        Assert.False(transport.Reader.TryRead(out _));
        Assert.Equal(0, transport.OutstandingJobs);
        Assert.Equal(0, transport.DelayedJobsHeld);
    }

    [Fact]
    public async Task ADelayedJobFiringIntoACompletedQueue_IsDroppedWithAWarning_AndReleasesItsSlot()
    {
        var timers = new ManualTimers();
        var transport = Transport(clock: timers);
        var logger = new CollectingLogger();
        transport.DrainLogger = logger;
        await transport.PublishAsync(Job("TooLate"), TimeSpan.FromMinutes(5));
        typeof(InMemoryWorkerTransport).GetMethod("CompleteWriter", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(transport, null);

        timers.FireLast();

        await WaitUntilAsync(() => transport.DelayedJobsHeld == 0);
        Assert.Equal(0, transport.OutstandingJobs);
        Assert.Contains(logger.Messages, message => message.Contains("fired after the transport completed its shutdown drain", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ADelayedJobWhoseQueueWriteFails_IsDroppedWithAnError_AndReleasesItsSlot()
    {
        var timers = new ManualTimers();
        var transport = Transport(clock: timers);
        var logger = new CollectingLogger();
        transport.DrainLogger = logger;
        await transport.PublishAsync(Job("Unwritable"), TimeSpan.FromMinutes(5));
        QueueOf(transport).Writer.TryComplete(new OperationCanceledException("queue torn down"));

        timers.FireLast();

        await WaitUntilAsync(() => transport.DelayedJobsHeld == 0);
        Assert.Equal(0, transport.OutstandingJobs);
        var failure = Assert.Single(logger.Entries, entry => entry.Message.Contains("Failed to enqueue fired delayed in-memory worker job", StringComparison.Ordinal));
        Assert.IsType<OperationCanceledException>(failure.Exception);
    }

    private static Channel<InMemoryWorkerTransport.QueuedJob> QueueOf(InMemoryWorkerTransport transport)
        => (Channel<InMemoryWorkerTransport.QueuedJob>)typeof(InMemoryWorkerTransport)
            .GetField("_queue", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(transport)!;

    // ---------------------------------------------------------------------------------------
    // InMemoryWorkerHost
    // ---------------------------------------------------------------------------------------

    private static WorkerJobExecutor Executor(ServiceProvider provider)
        => new(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<WorkerJobExecutor>.Instance);

    [Fact]
    public async Task AHostThatNeverStarted_StopsAsANoOp()
    {
        await using var provider = new ServiceCollection().BuildServiceProvider();
        using var host = new InMemoryWorkerHost(Transport(), Executor(provider), NullLogger<InMemoryWorkerHost>.Instance);

        await host.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task WorkerLoops_EndedByACancelledQueue_EndTheHostQuietly()
    {
        await using var provider = new ServiceCollection().BuildServiceProvider();
        var transport = Transport();
        using var host = new InMemoryWorkerHost(transport, Executor(provider), NullLogger<InMemoryWorkerHost>.Instance);
        await host.StartAsync(CancellationToken.None);

        QueueOf(transport).Writer.TryComplete(new OperationCanceledException("queue torn down"));

        var execution = (Task)typeof(InMemoryWorkerHost).GetField("_execution", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(host)!;
        await execution.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(execution.IsCompletedSuccessfully);
        await host.StopAsync(CancellationToken.None);
    }

    /// <summary>A clock that cannot arm a timer: the redelivery backoff's Task.Delay throws.</summary>
    private sealed class UnarmableClock : TimeProvider
    {
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
            => throw new NotSupportedException("no timers on this clock");
    }

    [Fact]
    public async Task AFailureEscapingTheRedeliveryLoop_IsCaughtByTheBackstop_AndTheWorkerLivesOn()
    {
        await using var provider = new ServiceCollection().BuildServiceProvider();
        var transport = Transport();
        var logger = new CollectingLogger();
        using var host = new InMemoryWorkerHost(transport, Executor(provider), logger.For<InMemoryWorkerHost>(), new UnarmableClock());
        await host.StartAsync(CancellationToken.None);
        try
        {
            // The target is not registered: the first attempt fails, and the backoff cannot be armed.
            await transport.PublishAsync(Job("Backstopped"));

            await logger.WaitForAsync("Gap.IUnregisteredWork.Backstopped failed.");
            var backstop = Assert.Single(logger.Entries, entry => entry.Message.EndsWith("Backstopped failed.", StringComparison.Ordinal));
            Assert.IsType<NotSupportedException>(backstop.Exception);
            await WaitUntilAsync(() => transport.OutstandingJobs == 0);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    // ---------------------------------------------------------------------------------------
    // InMemoryAsyncResponseChannel
    // ---------------------------------------------------------------------------------------

    private static InMemoryAsyncResponseChannel NewChannel(IRecoveryStateStore store, Microsoft.Extensions.Logging.ILogger<InMemoryAsyncResponseChannel>? logger = null, TimeProvider? clock = null)
        => new(
            new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            store,
            Options.Create(new InMemoryAsyncResponseOptions
            {
                DefaultTimeout = TimeSpan.FromMinutes(5),
                RecoveryStateExpiry = TimeSpan.FromMinutes(10)
            }),
            new AsyncResponseContextPropagation([]),
            logger ?? NullLogger<InMemoryAsyncResponseChannel>.Instance,
            clock);

    private sealed class ScriptedRecoveryStore : IRecoveryStateStore
    {
        public Func<Task>? OnSave { get; set; }

        public Func<CancellationToken, Task<IReadOnlyList<RecoveryState>>>? OnGetAll { get; set; }

        public Task SaveAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => OnSave?.Invoke() ?? Task.CompletedTask;

        public Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default)
            => OnGetAll?.Invoke(cancellationToken) ?? Task.FromResult<IReadOnlyList<RecoveryState>>([]);

        public int Deletes => Volatile.Read(ref _deletes);

        private int _deletes;

        public Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _deletes);
            return Task.FromResult(true);
        }
    }

    [Fact]
    public async Task ASaveFailingAfterADeliverySettledTheWait_ReturnsTheCompletedWaiter_EvenWhenTheWarningThrows()
    {
        var saveGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new ScriptedRecoveryStore
        {
            OnSave = async () =>
            {
                await saveGate.Task;
                throw new TimeoutException("recovery store timed out after the response landed");
            }
        };
        var logger = new CollectingLogger { ThrowOnMessageContaining = "Registration step failed after a delivery settled" };
        var channel = NewChannel(store, logger.For<InMemoryAsyncResponseChannel>());

        var creating = channel.CreateResponseWaiter<OperationResult>("gap-settled-then-save-failed");
        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "delivered" }, "gap-settled-then-save-failed");
        saveGate.SetResult();

        await using var waiter = await creating.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal("delivered", (await waiter.ResponseTask).Message);
        Assert.Contains(logger.Messages, message => message.Contains("Registration step failed after a delivery settled", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ARawPublishOrExceptionCancelledByItsCaller_WhileReadingRecoveryState_IsACancellation()
    {
        using var cancel = new CancellationTokenSource();
        var store = new ScriptedRecoveryStore
        {
            OnGetAll = token =>
            {
                cancel.Cancel();
                return Task.FromException<IReadOnlyList<RecoveryState>>(new OperationCanceledException(token));
            }
        };
        var channel = NewChannel(store);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ((IRawAsyncResponsePublisher)channel).SetRawResponseJson("""{"Status":2}""", "gap-raw-cancelled", cancel.Token));

        using var cancelAgain = new CancellationTokenSource();
        store.OnGetAll = token =>
        {
            cancelAgain.Cancel();
            return Task.FromException<IReadOnlyList<RecoveryState>>(new OperationCanceledException(token));
        };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => channel.SetException(new TimeoutException("remote gave up"), "gap-exception-cancelled", cancelAgain.Token));
    }

    [Fact]
    public async Task AbandonAll_DrainsAGroupOfSeveralWaiters_WithoutCompletingThem()
    {
        var store = new ScriptedRecoveryStore();
        var channel = NewChannel(store);
        await using var first = await channel.CreateResponseWaiter<OperationResult>("gap-abandon");
        await using var second = await channel.CreateResponseWaiter<OperationResult>("gap-abandon");
        Assert.Equal(2, await channel.CountActiveSubscribersAsync("gap-abandon"));

        await channel.AbandonAllAsync();

        // A crash's semantics: nobody listens any more, neither wait received anything, and both
        // recovery registrations are left behind for the next incarnation.
        Assert.Equal(0, await channel.CountActiveSubscribersAsync("gap-abandon"));
        Assert.True(first.ResponseTask.IsCanceled);
        Assert.True(second.ResponseTask.IsCanceled);
        Assert.Equal(0, store.Deletes);

        // An empty group drains to nothing.
        var groupType = typeof(InMemoryAsyncResponseChannel).GetNestedType("SubscriptionGroup", BindingFlags.NonPublic)!;
        var empty = Activator.CreateInstance(groupType, nonPublic: true)!;
        var drained = (System.Collections.ICollection)groupType.GetMethod("DrainForAbandon")!.Invoke(empty, null)!;
        Assert.Empty(drained);
    }

    /// <summary>Cleans the subscription up from inside timer creation — the race ArmTimeout guards.</summary>
    private sealed class CleanupWhileArmingClock : TimeProvider
    {
        public InertTimer? Armed { get; private set; }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            if (Armed is null && state is not null && state.GetType().GetMethod("CleanupOnceAsync") is { } cleanup)
            {
                Armed = new InertTimer();
                ((ValueTask)cleanup.Invoke(state, null)!).AsTask().GetAwaiter().GetResult();
                return Armed;
            }

            return System.CreateTimer(callback, state, dueTime, period);
        }
    }

    [Fact]
    public async Task ACleanupThatRacesTimerArming_DisposesTheTimerItArmed()
    {
        var clock = new CleanupWhileArmingClock();
        var channel = NewChannel(new ScriptedRecoveryStore(), clock: clock);

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("gap-arm-race");

        Assert.NotNull(clock.Armed);
        Assert.True(clock.Armed!.Disposed);
        Assert.True(waiter.ResponseTask.IsCanceled);
    }

    [Theory]
    [InlineData("for correlationId gap-timeout-logger")] // the timeout warning AND the error report throw
    [InlineData("Timed out waiting for response for correlationId gap-timeout-logger")] // only the warning throws
    public async Task AWaiterTimeout_WhoseLoggingThrows_StillTimesTheWaitOut_AndReportsOrSwallowsTheFailure(string throwingFragment)
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        var logger = new CollectingLogger { ThrowOnMessageContaining = throwingFragment };
        var channel = NewChannel(new ScriptedRecoveryStore(), logger.For<InMemoryAsyncResponseChannel>(), clock);
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("gap-timeout-logger", timeout: TimeSpan.FromSeconds(10));

        clock.Advance(TimeSpan.FromSeconds(11));

        await logger.WaitForAsync("Timed out waiting for response for correlationId gap-timeout-logger");
        await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(30)));

        // Round 65: the timeout warning is guarded (and logged after the metric and span), so
        // its throw no longer escapes to the timer body's error report.
        await waiter.DisposeAsync();
        Assert.DoesNotContain(logger.Messages, message => message.StartsWith("Error handling waiter timeout", StringComparison.Ordinal));
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Timed out waiting for the condition.");
            await Task.Delay(5);
        }
    }
}
