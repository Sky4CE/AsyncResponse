using System.Collections;
using System.Reflection;
using AsyncResponse.Testing;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Coverage-gap pins for <see cref="ScheduledFlowService"/>: the defensive backstops of the
/// scheduler loop (hand-registered duplicates, a mutated cron expression, an unsatisfiable
/// schedule) and every settlement branch of the undispatched-occurrence re-drive and startup probe.
/// The re-drive and probe are driven directly (reflection) so each branch is one deterministic call.
/// </summary>
public sealed class ScheduledFlowsCoverageGapTests
{
    private static readonly Type UndispatchedType =
        typeof(ScheduledFlowService).GetNestedType("UndispatchedOccurrence", BindingFlags.NonPublic)!;

    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 30, TimeSpan.Zero);

    private static ScheduledFlowRegistration Registration(
        string name = "gap",
        string cron = "0 * * * *",
        Func<IDurableFlows, string, DateTimeOffset, CancellationToken, Task>? start = null,
        ScheduledFlowOptions? options = null) => new()
    {
        Name = name,
        CronExpression = cron,
        Options = options ?? new ScheduledFlowOptions(),
        StartOccurrenceAsync = start ?? ((_, _, _, _) => Task.CompletedTask)
    };

    private static Task InvokeExecuteAsync(ScheduledFlowService service, CancellationToken token)
        => (Task)typeof(ScheduledFlowService)
            .GetMethod("ExecuteAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(service, [token])!;

    private static object Entry(string flowId, bool awaitingFirstPublish)
    {
        var entry = Activator.CreateInstance(UndispatchedType, nonPublic: true)!;
        UndispatchedType.GetProperty("FlowId")!.SetValue(entry, flowId);
        UndispatchedType.GetProperty("Occurrence")!.SetValue(entry, Start);
        UndispatchedType.GetProperty("DueUtc")!.SetValue(entry, Start);
        UndispatchedType.GetProperty("AwaitingFirstPublish")!.SetValue(entry, awaitingFirstPublish);
        return entry;
    }

    private static IList NewQueue() => (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(UndispatchedType))!;

    private static string FlowIdOf(object entry) => (string)UndispatchedType.GetProperty("FlowId")!.GetValue(entry)!;

    private static async Task<string> RedriveAsync(ScheduledFlowService service, ScheduledFlowRegistration registration, object entry, CancellationToken token = default)
    {
        var task = (Task)typeof(ScheduledFlowService)
            .GetMethod("RedriveAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(service, [registration, entry, token])!;
        await task;
        return task.GetType().GetProperty("Result")!.GetValue(task)!.ToString()!;
    }

    private static Task ProbeAsync(ScheduledFlowService service, ScheduledFlowRegistration registration, IList queue, TimeProvider clock, CancellationToken token)
        => (Task)typeof(ScheduledFlowService)
            .GetMethod("ProbeUndispatchedAtStartupAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(service, [registration, CronSchedule.Parse(registration.CronExpression), queue, clock, token])!;

    // ---------------------------------------------------------------------------------------
    // The loop's backstops.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task NoRegistrations_EndTheSchedulerAtOnce()
    {
        var flows = new Mock<IDurableFlows>(MockBehavior.Strict);
        using var service = new ScheduledFlowService(flows.Object, [], new CollectingLogger().For<ScheduledFlowService>());

        await InvokeExecuteAsync(service, CancellationToken.None);
    }

    [Fact]
    public async Task HandRegisteredDuplicateNames_ScheduleNothing_AndSaySo()
    {
        var logger = new CollectingLogger();
        var flows = new Mock<IDurableFlows>(MockBehavior.Strict);
        using var service = new ScheduledFlowService(
            flows.Object,
            [Registration("twin"), Registration("twin", "30 * * * *")],
            logger.For<ScheduledFlowService>(),
            new VirtualTimeProvider(Start));

        await InvokeExecuteAsync(service, CancellationToken.None);

        var error = Assert.Single(logger.Messages);
        Assert.Contains("share the name 'twin'", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MutatedCronExpressions_FailTheScheduler_AndTheSecondFaultIsLoggedNotLost()
    {
        var logger = new CollectingLogger();
        using var service = new ScheduledFlowService(
            Mock.Of<IDurableFlows>(),
            [Registration("first", "not a cron"), Registration("second", "still not a cron")],
            logger.For<ScheduledFlowService>(),
            new VirtualTimeProvider(Start));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeExecuteAsync(service, CancellationToken.None));

        Assert.Contains("invalid cron expression", ex.Message, StringComparison.Ordinal);
        Assert.IsType<FormatException>(ex.InnerException);
        await logger.WaitForAsync("faulted while the scheduler was already failing");
    }

    [Fact]
    public async Task AnUnsatisfiableHandRegisteredSchedule_StopsItsLoop_WithAWarning()
    {
        var logger = new CollectingLogger();
        using var service = new ScheduledFlowService(
            Mock.Of<IDurableFlows>(),
            [Registration("impossible", "0 0 30 2 *", options: new ScheduledFlowOptions { StartupRedriveWindow = TimeSpan.Zero })],
            logger.For<ScheduledFlowService>(),
            new VirtualTimeProvider(Start));

        await InvokeExecuteAsync(service, CancellationToken.None);

        Assert.Contains(logger.Messages, message => message.Contains("has no future occurrence", StringComparison.Ordinal));
    }

    [Fact]
    public async Task HostShutdown_WhileTheLoopSleeps_EndsItQuietly()
    {
        var clock = new VirtualTimeProvider(Start);
        var flows = new Mock<IDurableFlows>(MockBehavior.Strict);
        using var service = new ScheduledFlowService(
            flows.Object,
            [Registration("yearly", "0 0 1 1 *", options: new ScheduledFlowOptions { StartupRedriveWindow = TimeSpan.Zero })],
            new CollectingLogger().For<ScheduledFlowService>(),
            clock);
        using var stopping = new CancellationTokenSource();

        var loop = InvokeExecuteAsync(service, stopping.Token);
        await DurableFlowContextTestSupport.WaitForArmedTimerAsync(clock, TimeSpan.FromDays(400));
        await stopping.CancelAsync();

        await loop.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(loop.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task AStopRequestedWhileAnOccurrenceStarts_EndsTheLoopAtItsNextTurn()
    {
        var clock = new VirtualTimeProvider(Start);
        using var stopping = new CancellationTokenSource();
        var started = new List<string>();
        using var service = new ScheduledFlowService(
            Mock.Of<IDurableFlows>(),
            [Registration(
                "hourly",
                start: (_, flowId, _, _) =>
                {
                    // The start itself completes; the stop lands while it runs.
                    started.Add(flowId);
                    stopping.Cancel();
                    return Task.CompletedTask;
                },
                options: new ScheduledFlowOptions { StartupRedriveWindow = TimeSpan.Zero })],
            new CollectingLogger().For<ScheduledFlowService>(),
            clock);

        var loop = InvokeExecuteAsync(service, stopping.Token);
        await DurableFlowContextTestSupport.WaitForArmedTimerAsync(clock, TimeSpan.FromHours(1));
        clock.Advance(TimeSpan.FromHours(1));

        await loop.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(loop.IsCompletedSuccessfully);
        Assert.Equal(["sched:hourly:20300101T010000Z"], started);
    }

    // ---------------------------------------------------------------------------------------
    // The in-process re-drive queue.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void Enqueue_SkipsAnOccurrenceAlreadyQueued_AndDropsTheOldestPastTheBound()
    {
        var logger = new CollectingLogger();
        using var service = new ScheduledFlowService(Mock.Of<IDurableFlows>(), [], logger.For<ScheduledFlowService>());
        var registration = Registration("bounded");
        var enqueue = typeof(ScheduledFlowService).GetMethod("Enqueue", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var queue = NewQueue();

        for (var i = 0; i < ScheduledFlowService.MaxUndispatchedOccurrences; i++)
            enqueue.Invoke(service, [queue, registration, Start.AddHours(i), Start]);
        var oldest = FlowIdOf(queue[0]!);

        // The same occurrence again: already queued, nothing changes.
        enqueue.Invoke(service, [queue, registration, Start, Start]);
        Assert.Equal(ScheduledFlowService.MaxUndispatchedOccurrences, queue.Count);
        Assert.Empty(logger.Messages);

        // One more distinct occurrence: the oldest is dropped, loudly, naming its id.
        enqueue.Invoke(service, [queue, registration, Start.AddHours(ScheduledFlowService.MaxUndispatchedOccurrences), Start]);
        Assert.Equal(ScheduledFlowService.MaxUndispatchedOccurrences, queue.Count);
        Assert.NotEqual(oldest, FlowIdOf(queue[0]!));
        var dropped = Assert.Single(logger.Messages);
        Assert.Contains(oldest, dropped, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Redrive_AStateReadFailure_Retries_AndACancelledStopPropagates()
    {
        var flows = new Mock<IDurableFlows>();
        using var stopping = new CancellationTokenSource();
        flows.Setup(f => f.GetStateAsync("read-fails", It.IsAny<CancellationToken>())).ThrowsAsync(new TimeoutException("store down"));
        flows.Setup(f => f.GetStateAsync("read-cancelled", It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                stopping.Cancel();
                return Task.FromException<FlowState?>(new OperationCanceledException(stopping.Token));
            });
        var logger = new CollectingLogger();
        using var service = new ScheduledFlowService(flows.Object, [], logger.For<ScheduledFlowService>());
        var registration = Registration();

        Assert.Equal("Retry", await RedriveAsync(service, registration, Entry("read-fails", awaitingFirstPublish: true)));
        Assert.Contains(logger.Messages, message => message.Contains("could not load occurrence read-fails", StringComparison.Ordinal));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => RedriveAsync(service, registration, Entry("read-cancelled", awaitingFirstPublish: true), stopping.Token));
    }

    [Fact]
    public async Task Redrive_ALedgerThatVanished_OrThatWasPickedUp_IsSettled_WithoutAStart()
    {
        var starts = 0;
        var flows = new Mock<IDurableFlows>();
        flows.Setup(f => f.GetStateAsync("expired", It.IsAny<CancellationToken>())).ReturnsAsync((FlowState?)null);
        flows.Setup(f => f.GetStateAsync("picked-up", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new FlowState { FlowId = "picked-up", Status = FlowRunStatus.Running, Attempts = 1 });
        var logger = new CollectingLogger();
        using var service = new ScheduledFlowService(flows.Object, [], logger.For<ScheduledFlowService>());
        var registration = Registration(start: (_, _, _, _) =>
        {
            starts++;
            return Task.CompletedTask;
        });

        // Queued by the startup probe from an EXISTING ledger: absence now means expired or deleted.
        Assert.Equal("Settled", await RedriveAsync(service, registration, Entry("expired", awaitingFirstPublish: false)));
        Assert.Equal("Settled", await RedriveAsync(service, registration, Entry("picked-up", awaitingFirstPublish: false)));

        Assert.Equal(0, starts);
        Assert.Contains(logger.Messages, message => message.Contains("no longer has a ledger", StringComparison.Ordinal));
        Assert.Contains(logger.Messages, message => message.Contains("has been picked up", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Redrive_StartOutcomes_ConflictSettles_FailureRetries_CancelledStopPropagates()
    {
        var flows = new Mock<IDurableFlows>();
        flows.Setup(f => f.GetStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync((FlowState?)null);
        var logger = new CollectingLogger();
        using var service = new ScheduledFlowService(flows.Object, [], logger.For<ScheduledFlowService>());
        using var stopping = new CancellationTokenSource();
        Exception? next = null;
        var registration = Registration(start: (_, _, _, _) =>
        {
            if (next is OperationCanceledException)
                stopping.Cancel();
            return Task.FromException(next!);
        });

        next = new DurableFlowIdConflictException("different input");
        Assert.Equal("Settled", await RedriveAsync(service, registration, Entry("conflict", awaitingFirstPublish: true)));
        Assert.Contains(logger.Messages, message => message.Contains("cannot be re-driven", StringComparison.Ordinal));

        next = new InvalidOperationException("store rejected the start");
        Assert.Equal("Retry", await RedriveAsync(service, registration, Entry("failing", awaitingFirstPublish: true)));
        Assert.Contains(logger.Messages, message => message.Contains("failed to re-drive occurrence failing", StringComparison.Ordinal));

        next = new OperationCanceledException(stopping.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => RedriveAsync(service, registration, Entry("stopping", awaitingFirstPublish: true), stopping.Token));
    }

    // ---------------------------------------------------------------------------------------
    // The startup probe.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task StartupProbe_AFailedRead_EndsTheProbe_AndACancelledStopPropagates()
    {
        var clock = new VirtualTimeProvider(Start);
        var reads = 0;
        var flows = new Mock<IDurableFlows>();
        flows.Setup(f => f.GetStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                reads++;
                return Task.FromException<FlowState?>(new TimeoutException("store down"));
            });
        var logger = new CollectingLogger();
        using var service = new ScheduledFlowService(flows.Object, [], logger.For<ScheduledFlowService>(), clock);
        var registration = Registration("per-minute", "* * * * *");
        var queue = NewQueue();

        await ProbeAsync(service, registration, queue, clock, CancellationToken.None);

        // The first failure ends the whole probe: best-effort, the loop starts regardless.
        Assert.Equal(1, reads);
        Assert.Empty(queue);
        Assert.Contains(logger.Messages, message => message.Contains("skipping the rest of the probe", StringComparison.Ordinal));

        using var stopping = new CancellationTokenSource();
        var cancelling = new Mock<IDurableFlows>();
        cancelling.Setup(f => f.GetStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                stopping.Cancel();
                return Task.FromException<FlowState?>(new OperationCanceledException(stopping.Token));
            });
        using var stopped = new ScheduledFlowService(cancelling.Object, [], logger.For<ScheduledFlowService>(), clock);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ProbeAsync(stopped, registration, queue, clock, stopping.Token));
    }
}
